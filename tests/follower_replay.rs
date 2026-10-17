//! Native regressions for the snapshot-to-incremental replay handoff.

mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use changesets::ChangesetKind;
use common::InMemoryStorage;
use hadb_storage::StorageBackend;
use haqlite::{Durability, HaQLite, InMemoryLeaseStore, Role, SqlValue};
use tempfile::TempDir;
use walrust::hadb_changeset::{physical, storage as changesets};

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS entries (value TEXT NOT NULL);";
const PREFIX: &str = "replay/";
const POLL: Duration = Duration::from_millis(20);
const TIMEOUT: Duration = Duration::from_secs(2);

fn entries(db: &HaQLite) -> String {
    db.query_row_local(
        "SELECT COALESCE(group_concat(value, ','), '') FROM entries",
        &[],
        |row| row.get(0),
    )
    .expect("read entries")
}

async fn wait_for_rows(db: &HaQLite, expected: &str) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(TIMEOUT, async {
        while entries(db) != expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
}

struct Cluster {
    tmp: TempDir,
    storage: Arc<InMemoryStorage>,
    leases: Arc<InMemoryLeaseStore>,
    leader: HaQLite,
    snapshot: changesets::DiscoveredChangeset,
    seq: u64,
}

impl Cluster {
    async fn new() -> Self {
        Self::with_snapshot_interval(Duration::from_secs(3600)).await
    }

    async fn with_snapshot_interval(snapshot_interval: Duration) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let tmp = TempDir::new().expect("tempdir");
        let storage = Arc::new(InMemoryStorage::new());
        let leases = Arc::new(InMemoryLeaseStore::new());
        let leader = HaQLite::builder()
            .instance_id("leader")
            .lease_store(leases.clone())
            .walrust_storage(storage.clone())
            .prefix(PREFIX)
            .disable_forwarding()
            .durability(Durability::Replicated(POLL))
            .coordinator_config(haqlite::CoordinatorConfig {
                snapshot_interval,
                ..Default::default()
            })
            .open(
                tmp.path().join("leader/entries.db").to_str().unwrap(),
                SCHEMA,
            )
            .await
            .expect("open leader");
        assert_eq!(leader.role(), Some(Role::Leader));
        let snapshot = changesets::discover_latest_snapshot(
            storage.as_ref(),
            PREFIX,
            "entries",
            ChangesetKind::Physical,
        )
        .await
        .expect("discover snapshot")
        .expect("leader published a snapshot");
        assert!(snapshot.seq > 1, "exercise a nonzero restore cursor");
        let seq = snapshot.seq;
        Self {
            tmp,
            storage,
            leases,
            leader,
            snapshot,
            seq,
        }
    }

    async fn follower(&self, interval: Duration) -> HaQLite {
        let db = self
            .try_follower(interval)
            .await
            .expect("open follower and restore");
        assert_eq!(db.role(), Some(Role::Follower));
        db
    }

    async fn try_follower(&self, interval: Duration) -> anyhow::Result<HaQLite> {
        HaQLite::builder()
            .instance_id("follower")
            .lease_store(self.leases.clone())
            .walrust_storage(self.storage.clone())
            .prefix(PREFIX)
            .disable_forwarding()
            .follower_pull_interval(interval)
            .durability(Durability::Replicated(POLL))
            .lease_follower_poll_interval(POLL)
            .open(
                self.tmp
                    .path()
                    .join("follower/entries.db")
                    .to_str()
                    .unwrap(),
                SCHEMA,
            )
            .await
    }

    async fn publish(&mut self, value: &str) -> (changesets::DiscoveredChangeset, Vec<u8>) {
        self.leader
            .execute_async(
                "INSERT INTO entries VALUES (?1)",
                &[SqlValue::Text(value.into())],
            )
            .await
            .expect("write leader");
        let file = tokio::time::timeout(TIMEOUT, async {
            loop {
                let files = changesets::discover_after(
                    self.storage.as_ref(),
                    PREFIX,
                    "entries",
                    self.seq,
                    ChangesetKind::Physical,
                )
                .await
                .expect("discover published incrementals");
                if let Some(file) = files.into_iter().next() {
                    break file;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("leader must publish before checking replay");
        assert_eq!(file.seq, self.seq + 1);
        let data = self.storage.get(&file.key).await.unwrap().unwrap();
        physical::decode(&data).expect("published incremental is readable and decodable");
        self.seq = file.seq;
        (file, data)
    }
}

// Boundary adapter for an external base owner: materialize the real leader's
// snapshot, but return its replay cursor rather than a storage manifest version.
struct ExternalBase {
    data: Vec<u8>,
    seq: u64,
}

#[async_trait::async_trait]
impl walrust::SnapshotSource for ExternalBase {
    async fn materialize(&self, output: &std::path::Path) -> anyhow::Result<u64> {
        walrust::ltx::decode_to_db(&self.data, output)?;
        Ok(self.seq)
    }

    async fn checkpoint_version(&self) -> anyhow::Result<u64> {
        Ok(self.seq)
    }
}

#[tokio::test]
async fn external_base_follower_replays_from_materialized_cursor() {
    let mut cluster = Cluster::new().await;
    let source = Arc::new(ExternalBase {
        data: cluster
            .storage
            .get(&cluster.snapshot.key)
            .await
            .unwrap()
            .unwrap(),
        seq: cluster.snapshot.seq,
    });
    let replicator = Arc::new(
        haqlite::ExternalSnapshotSqliteReplicator::new(
            cluster.storage.clone(),
            PREFIX,
            walrust::ReplicationConfig {
                autonomous_snapshots: false,
                ..Default::default()
            },
            source,
        )
        .unwrap(),
    );
    let coordinator = haqlite::Coordinator::new(
        replicator,
        None,
        None,
        Arc::new(haqlite::SqliteFollowerBehavior::new()),
        PREFIX,
        haqlite::CoordinatorConfig {
            lease: Some(haqlite::LeaseConfig::new(
                cluster.leases.clone(),
                "external-follower".into(),
                "http://localhost:0".into(),
            )),
            follower_pull_interval: POLL,
            ..Default::default()
        },
    );
    let mut follower = HaQLite::from_coordinator(
        coordinator,
        cluster
            .tmp
            .path()
            .join("external/entries.db")
            .to_str()
            .unwrap(),
        SCHEMA,
        0,
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(follower.role(), Some(Role::Follower));
    assert_eq!(follower.replay_position(), cluster.snapshot.seq);
    let (file, _) = cluster.publish("after-external-base").await;
    wait_for_rows(&follower, "after-external-base")
        .await
        .expect("replay from the actual external image checksum");
    assert_eq!(follower.replay_position(), file.seq);
    follower.close().await.unwrap();
    cluster.leader.close().await.unwrap();
}

#[tokio::test]
async fn follower_replays_incremental_after_restoring_snapshot() {
    let mut cluster = Cluster::new().await;
    let mut follower = cluster.follower(POLL).await;
    let restored_position = follower.replay_position();
    assert_eq!(entries(&follower), "");
    let (incremental, _) = cluster.publish("after-restore").await;
    assert_eq!(entries(&cluster.leader), "after-restore");
    let replayed = wait_for_rows(&follower, "after-restore").await;
    let position = follower.replay_position();
    let metrics = follower.prometheus_metrics().unwrap();
    assert!(replayed.is_ok(),
        "follower restored snapshot {} but did not replay readable incremental {}; replay position={position}\n{metrics}",
        cluster.snapshot.seq, incremental.seq);
    assert_eq!(restored_position, cluster.snapshot.seq);
    assert_eq!(position, incremental.seq);
    let (next, _) = cluster.publish("second-pull").await;
    wait_for_rows(&follower, "after-restore,second-pull")
        .await
        .expect("advance the retained cursor again");
    assert_eq!(follower.replay_position(), next.seq);
    follower.close().await.unwrap();
    cluster.leader.close().await.unwrap();
}

#[tokio::test]
async fn follower_replays_across_a_new_snapshot_generation() {
    let mut cluster = Cluster::with_snapshot_interval(Duration::from_millis(100)).await;
    let mut follower = cluster.follower(POLL).await;
    let newer = tokio::time::timeout(TIMEOUT, async {
        loop {
            let snapshot = changesets::discover_latest_snapshot(
                cluster.storage.as_ref(),
                PREFIX,
                "entries",
                ChangesetKind::Physical,
            )
            .await
            .unwrap()
            .unwrap();
            if snapshot.seq > cluster.snapshot.seq {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("leader must publish a later snapshot");
    cluster
        .leader
        .execute_async("INSERT INTO entries VALUES ('after-new-snapshot')", &[])
        .await
        .unwrap();
    let replayed = wait_for_rows(&follower, "after-new-snapshot").await;
    let position = follower.replay_position();
    follower.close().await.unwrap();
    cluster.leader.close().await.unwrap();
    assert!(
        replayed.is_ok(),
        "follower must cross snapshot seq {} before replaying its tail; position={position}",
        newer.seq
    );
    assert!(position > newer.seq);
}

#[tokio::test]
async fn follower_rejects_incrementals_without_a_restorable_base() {
    let mut cluster = Cluster::new().await;
    cluster.publish("orphaned").await;
    cluster.storage.delete(&cluster.snapshot.key).await.unwrap();
    let result = cluster.try_follower(POLL).await;
    let error = match result {
        Ok(mut follower) => {
            follower.close().await.unwrap();
            None
        }
        Err(error) => Some(error),
    };
    cluster.leader.close().await.unwrap();
    let error = error.expect("must not join with an unanchored incremental tail");
    assert!(
        error.to_string().contains("no snapshot"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn follower_can_restore_when_first_snapshot_arrives_after_join() {
    let mut cluster = Cluster::new().await;
    let snapshot_data = cluster
        .storage
        .get(&cluster.snapshot.key)
        .await
        .unwrap()
        .unwrap();
    cluster.storage.delete(&cluster.snapshot.key).await.unwrap();
    let mut follower = cluster.follower(POLL).await;
    assert_eq!(follower.replay_position(), 0);
    cluster
        .storage
        .put(&cluster.snapshot.key, &snapshot_data)
        .await
        .unwrap();
    let (file, _) = cluster.publish("after-late-snapshot").await;
    wait_for_rows(&follower, "after-late-snapshot")
        .await
        .expect("restore arriving snapshot and replay");
    assert_eq!(follower.replay_position(), file.seq);
    follower.close().await.unwrap();
    cluster.leader.close().await.unwrap();
}

#[tokio::test]
async fn promotion_catches_up_from_restored_cursor_before_any_periodic_replay() {
    let mut cluster = Cluster::new().await;
    let mut follower = cluster.follower(Duration::from_secs(3600)).await;
    // Consume the immediate interval tick before publishing. The next periodic
    // replay is an hour away; only promotion catch-up can deliver this row.
    tokio::time::timeout(TIMEOUT, async {
        while follower
            .coordinator()
            .unwrap()
            .metrics()
            .follower_pulls_no_new_data
            .load(Ordering::Relaxed)
            == 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    cluster.publish("before-promotion").await;
    assert_eq!(entries(&follower), "");
    cluster.leader.close().await.unwrap();
    tokio::time::timeout(TIMEOUT, async {
        while follower.role() != Some(Role::Leader) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("warm promotion must finish");
    assert_eq!(entries(&follower), "before-promotion");
    follower
        .execute_async("INSERT INTO entries VALUES ('new-leader')", &[])
        .await
        .unwrap();
    assert_eq!(entries(&follower), "before-promotion,new-leader");
    follower.close().await.unwrap();
}

#[derive(Clone, Copy, Debug)]
enum BadTail {
    Gap,
    Fork,
    SnapshotFork,
    StaleEpoch,
}

async fn rejects_bad_tail(kind: BadTail) {
    // Exercise both anchor contracts: snapshot image checksum and incremental
    // chain checksum. The remote anchor is deleted after restore, so re-reading
    // a prior object (or accepting an unanchored first delta) cannot pass.
    for restored_tail in [false, true] {
        let mut cluster = Cluster::new().await;
        let anchor_key = if restored_tail {
            cluster.publish("restored").await.0.key
        } else {
            cluster.snapshot.key.clone()
        };
        let restored_seq = cluster.seq;
        let (file, good_data) = cluster.publish("next").await;
        cluster.storage.delete(&file.key).await.unwrap();
        cluster
            .storage
            .insert("replay/entries/_epoch", b"10".to_vec())
            .await;
        let mut follower = cluster.follower(POLL).await;
        let initial_rows = if restored_tail { "restored" } else { "" };
        assert_eq!(entries(&follower), initial_rows);
        assert_eq!(follower.replay_position(), restored_seq);
        cluster.storage.delete(&anchor_key).await.unwrap();

        let mut bad = physical::decode(&good_data).unwrap();
        let bad_key = match kind {
            BadTail::Gap => {
                bad.header.seq += 1;
                changesets::format_key(
                    PREFIX,
                    "entries",
                    changesets::GENERATION_INCREMENTAL,
                    bad.header.seq,
                    ChangesetKind::Physical,
                )
            }
            BadTail::Fork => {
                // Re-encode a valid object on a different lineage, not a corrupt
                // object that the decoder would reject for an unrelated reason.
                bad = physical::PhysicalChangeset::new(
                    bad.header.seq,
                    bad.header.prev_checksum ^ 1,
                    bad.header.page_id_size,
                    bad.header.page_size,
                    bad.pages,
                );
                file.key.clone()
            }
            BadTail::SnapshotFork => {
                let snapshot_path = cluster.tmp.path().join("fork.db");
                cluster
                    .leader
                    .execute_async(
                        "VACUUM INTO ?1",
                        &[SqlValue::Text(snapshot_path.to_string_lossy().into_owned())],
                    )
                    .await
                    .unwrap();
                let data = walrust::ltx::encode_snapshot(
                    &snapshot_path,
                    bad.header.page_size,
                    file.seq,
                    bad.header.prev_checksum ^ 1,
                )
                .unwrap();
                bad = physical::decode(&data).unwrap();
                changesets::format_key(
                    PREFIX,
                    "entries",
                    changesets::GENERATION_SNAPSHOT,
                    file.seq,
                    ChangesetKind::Physical,
                )
            }
            BadTail::StaleEpoch => {
                cluster
                    .storage
                    .insert("replay/entries/_epoch", b"9".to_vec())
                    .await;
                file.key.clone()
            }
        };
        let bad_data = physical::encode(&bad);
        physical::decode(&bad_data).expect("negative control must be a valid HADBP object");
        cluster.storage.put(&bad_key, &bad_data).await.unwrap();
        tokio::time::timeout(TIMEOUT, async {
            while follower
                .coordinator()
                .unwrap()
                .metrics()
                .follower_pulls_failed
                .load(Ordering::Relaxed)
                == 0
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("follower must attempt and reject the bad tail");
        assert_eq!(
            entries(&follower),
            initial_rows,
            "{kind:?} must not apply pages"
        );
        assert_eq!(follower.replay_position(), restored_seq);
        assert!(!follower.is_caught_up());

        // Repair only the invalid input. Successful replay then proves the
        // earlier rejection was not a setup, network, or missing-anchor error.
        cluster.storage.delete(&bad_key).await.unwrap();
        cluster
            .storage
            .insert("replay/entries/_epoch", b"10".to_vec())
            .await;
        cluster.storage.put(&file.key, &good_data).await.unwrap();
        let expected = if restored_tail {
            "restored,next"
        } else {
            "next"
        };
        wait_for_rows(&follower, expected)
            .await
            .expect("replay repaired tail");
        assert_eq!(follower.replay_position(), file.seq);
        follower.close().await.unwrap();
        cluster.leader.close().await.unwrap();
    }
}

#[tokio::test]
async fn follower_rejects_gaps_after_restore() {
    rejects_bad_tail(BadTail::Gap).await;
}

#[tokio::test]
async fn follower_rejects_forks_using_retained_restore_anchor() {
    rejects_bad_tail(BadTail::Fork).await;
}

#[tokio::test]
async fn follower_rejects_forked_snapshots_after_restore() {
    rejects_bad_tail(BadTail::SnapshotFork).await;
}

#[tokio::test]
async fn follower_rejects_epochs_older_than_its_restore() {
    rejects_bad_tail(BadTail::StaleEpoch).await;
}
