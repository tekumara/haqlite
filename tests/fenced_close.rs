//! Acknowledged WAL writes must survive a failed close and its retry.
//! The storage seam enforces a real lease fence, as fenced HTTP backends do.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use hadb_lease::AtomicFence;
use hadb_storage::{CasResult, StorageBackend};
use hadb_storage_local::LocalStorage;
use haqlite::{
    Coordinator, CoordinatorConfig, HaQLite, InMemoryLeaseStore, LeaseConfig,
    SqliteFollowerBehavior, SqliteReplicator,
};
use tempfile::TempDir;

struct FencedStorage {
    inner: Arc<LocalStorage>,
    fence: AtomicFence,
    unavailable: Arc<AtomicBool>,
}

impl FencedStorage {
    fn require_write(&self) -> Result<()> {
        hadb_lease::FenceSource::require(&self.fence)?;
        if self.unavailable.load(Ordering::SeqCst) {
            anyhow::bail!("injected storage outage");
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for FencedStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.require_write()?;
        self.inner.put(key, data).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.require_write()?;
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.inner.list(prefix, after).await
    }

    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.require_write()?;
        self.inner.put_if_absent(key, data).await
    }

    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.require_write()?;
        self.inner.put_if_match(key, data, etag).await
    }
}

async fn open_node(
    path: &std::path::Path,
    storage: Arc<LocalStorage>,
    lease_store: Arc<InMemoryLeaseStore>,
    unavailable: Arc<AtomicBool>,
    instance: &str,
) -> Result<HaQLite> {
    let (fence, writer) = AtomicFence::new();
    let storage: Arc<dyn StorageBackend> = Arc::new(FencedStorage {
        inner: storage,
        fence: fence.clone(),
        unavailable,
    });
    let config = CoordinatorConfig {
        lease: Some(LeaseConfig::new(
            lease_store,
            instance.into(),
            String::new(),
        )),
        fence_writer: Some(Arc::new(writer)),
        durability: hadb::Durability::Replicated(Duration::from_secs(3600)),
        ..Default::default()
    };
    let replicator = Arc::new(
        SqliteReplicator::new(
            storage.clone(),
            "close/",
            walrust::ReplicationConfig {
                sync_interval: Duration::from_secs(3600),
                snapshot_interval: Duration::from_secs(3600),
                ..Default::default()
            },
        )
        .with_fence(fence),
    );
    let follower = Arc::new(SqliteFollowerBehavior::new(storage));
    let coordinator = Coordinator::new(replicator, None, None, follower, "close/", config);
    Ok(HaQLite::from_coordinator(
        coordinator,
        path.to_str().unwrap(),
        "CREATE TABLE IF NOT EXISTS entries (value TEXT NOT NULL)",
        0,
        Duration::from_secs(5),
    )
    .await?)
}

#[tokio::test]
async fn close_retries_pending_wal_under_the_held_lease() {
    let remote = TempDir::new().unwrap();
    let original = TempDir::new().unwrap();
    let restored = TempDir::new().unwrap();
    let storage = Arc::new(LocalStorage::new(remote.path()));
    let lease_store = Arc::new(InMemoryLeaseStore::new());
    let unavailable = Arc::new(AtomicBool::new(false));
    let mut first = open_node(
        &original.path().join("db.db"),
        storage.clone(),
        lease_store.clone(),
        unavailable.clone(),
        "first",
    )
    .await
    .unwrap();

    // Replicated durability acknowledges the local WAL. Keep remote writes
    // unavailable before insertion so these bytes must be shipped by close.
    unavailable.store(true, Ordering::SeqCst);
    first
        .execute_async("INSERT INTO entries VALUES ('acknowledged')", &[])
        .await
        .unwrap();
    first
        .close()
        .await
        .expect_err("failed final upload must fail close");

    // A failed final upload must retain both the pending WAL and its ownership.
    // An early fence revocation would make this retry fail with NoActiveLease.
    unavailable.store(false, Ordering::SeqCst);
    first
        .close()
        .await
        .expect("retry final sync under the held lease");

    let mut second = open_node(
        &restored.path().join("db.db"),
        storage,
        lease_store,
        unavailable,
        "second",
    )
    .await
    .expect("restore from the replicated bytes, not the first node's cache");
    let value: String = second
        .query_row_local("SELECT value FROM entries", &[], |row| row.get(0))
        .unwrap();
    assert_eq!(value, "acknowledged");
    second.close().await.unwrap();
}
