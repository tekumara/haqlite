mod common;

use std::sync::Arc;

use common::InMemoryStorage;
use hadb::Replicator;
use hadb_storage::StorageBackend;
use haqlite::SqliteReplicator;
use tempfile::TempDir;

#[tokio::test]
async fn plain_sqlite_replicator_add_still_uploads_initial_snapshot() {
    let tmp = TempDir::new().expect("temp dir");
    let db_path = tmp.path().join("plain.db");

    let conn = rusqlite::Connection::open(&db_path).expect("open sqlite db");
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE demo (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO demo (value) VALUES ('hello');",
    )
    .expect("seed database");
    drop(conn);

    let storage = Arc::new(InMemoryStorage::new());
    let replicator = SqliteReplicator::new(
        storage.clone(),
        "plain/",
        walrust::ReplicationConfig::default(),
    );

    replicator
        .add("plain", &db_path)
        .await
        .expect("register database");

    let keys = storage.keys().await;
    let snapshot_keys: Vec<_> = keys
        .iter()
        .filter(|key| {
            key.starts_with("plain/plain/") && key.contains("/0001/") && key.ends_with(".hadbp")
        })
        .collect();
    assert_eq!(
        snapshot_keys.len(),
        1,
        "expected exactly one initial snapshot upload, got keys: {:?}",
        keys
    );

    let snapshot_bytes = storage
        .get(snapshot_keys[0])
        .await
        .expect("read uploaded snapshot")
        .expect("snapshot key exists");
    assert!(
        snapshot_bytes.starts_with(b"HADBP"),
        "snapshot should be encoded as HADBP"
    );
    let files = haqlite::ops::discover_ltx_files(storage.as_ref(), "plain/", "plain")
        .await
        .expect("discover native backup layout");
    assert_eq!(files.iter().filter(|f| f.is_snapshot).count(), 1);
    assert!(
        haqlite::ops::verify_database(storage.as_ref(), "plain/", "plain")
            .await
            .expect("verify native backup layout")
            .is_valid()
    );
    replicator.remove("plain").await.expect("stop replication");
}

#[tokio::test]
async fn restored_leader_reanchors_before_shipping_new_writes() {
    let tmp = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let original = tmp.path().join("original.db");
    let conn = rusqlite::Connection::open(&original).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
        CREATE TABLE t (value TEXT); INSERT INTO t VALUES ('before');",
    )
    .unwrap();
    drop(conn);
    let first = SqliteReplicator::new(storage.clone(), "resume/", Default::default());
    first.add("db", &original).await.unwrap();
    first.remove("db").await.unwrap();

    let restored = tmp.path().join("restored.db");
    let (fence, writer) = hadb_lease::AtomicFence::new();
    writer.set(7);
    let next =
        SqliteReplicator::new(storage.clone(), "resume/", Default::default()).with_fence(fence);
    next.pull("db", &restored).await.unwrap();
    writer.clear();
    let err = next
        .add("db", &restored)
        .await
        .expect_err("lost lease must fence restore adoption");
    assert!(err.to_string().contains("active lease"), "{err:#}");
    writer.set(8);
    next.add("db", &restored)
        .await
        .expect("resume a restored database, not a saved WAL offset");
    let conn = rusqlite::Connection::open(&restored).unwrap();
    conn.execute("INSERT INTO t VALUES ('after')", []).unwrap();
    next.sync("db").await.unwrap();
    let follower = tmp.path().join("follower.db");
    next.pull("db", &follower).await.unwrap();
    let reader = rusqlite::Connection::open(follower).unwrap();
    let values = reader
        .prepare("SELECT value FROM t ORDER BY rowid")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(values, ["before", "after"]);
    next.remove("db").await.unwrap();
}

#[tokio::test]
async fn follower_cursor_checks_lineage_and_rejects_unproven_or_forked_tails() {
    use hadb::FollowerBehavior;
    let tmp = TempDir::new().unwrap();
    let storage = Arc::new(InMemoryStorage::new());
    let source = tmp.path().join("source.db");
    let conn = rusqlite::Connection::open(&source).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
        CREATE TABLE t (value TEXT); INSERT INTO t VALUES ('before');",
    )
    .unwrap();
    drop(conn);
    let leader = SqliteReplicator::new(storage.clone(), "cursor/", Default::default());
    leader.add("db", &source).await.unwrap();
    let follower_path = tmp.path().join("follower.db");
    let base = leader.restore("db", &follower_path).await.unwrap().unwrap();
    let conn = rusqlite::Connection::open(&source).unwrap();
    conn.execute("INSERT INTO t VALUES ('after')", []).unwrap();
    leader.sync("db").await.unwrap();
    let follower = haqlite::SqliteFollowerBehavior::new(storage.clone());

    // Issue #12 is still present: hadb starts at zero after restore. The new
    // API must reject that unproven cursor, not bridge the missing sequence.
    let err = follower
        .catchup_on_promotion("cursor/", "db", &follower_path, 0)
        .await
        .expect_err("zero is not the restored cursor");
    assert!(err.to_string().contains("incremental gap"), "{err:#}");
    follower
        .catchup_on_promotion("cursor/", "db", &follower_path, base)
        .await
        .expect("the actual restored cursor follows the owned lineage");
    let count = || {
        rusqlite::Connection::open(&follower_path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap()
    };
    assert_eq!(count(), 2);

    let head = leader.inner().current_seq("db").await.unwrap();
    let head_key = storage
        .keys()
        .await
        .into_iter()
        .find(|key| key.contains("/0000/") && key.ends_with(&format!("{head:016x}.hadbp")))
        .unwrap();
    let bad_key = format!(
        "{}/{:016x}.hadbp",
        head_key.rsplit_once('/').unwrap().0,
        head + 1
    );
    let (bad_delta, _) = walrust::ltx::encode_wal_changes_with_end_page_count(
        &[(1, vec![0; 4096])],
        4096,
        head + 1,
        0,
        2,
    )
    .unwrap();
    storage.put(&bad_key, &bad_delta).await.unwrap();
    let err = follower
        .catchup_on_promotion("cursor/", "db", &follower_path, head)
        .await
        .expect_err("a fork must fail before applying any pages");
    assert!(err.to_string().contains("checksum chain broken"), "{err:#}");
    assert_eq!(count(), 2);
    leader.remove("db").await.unwrap();
}
