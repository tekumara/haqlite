//! Current walrust snapshot contract over Turbolite's published page groups.

use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use turbolite::tiered::SharedTurboliteVfs;
use walrust::{SnapshotCheckpoint, SnapshotSource};

pub(crate) struct TurboliteSnapshotSource(pub SharedTurboliteVfs);

#[async_trait]
impl SnapshotSource for TurboliteSnapshotSource {
    async fn materialize(&self, output: &Path) -> Result<SnapshotCheckpoint> {
        let vfs = self.0.clone();
        let manifest = vfs.manifest();
        let output = output.to_path_buf();
        tokio::task::spawn_blocking(move || {
            vfs.shared_state()
                .materialize_manifest_to_file(&manifest, &output)
                .context("materialize Turbolite checkpoint")?;
            Ok(SnapshotCheckpoint {
                seq: if manifest.cursor.epoch > 0 {
                    manifest.cursor.last_applied_seq
                } else {
                    manifest.change_counter
                },
                checksum: walrust::ltx::compute_checksum_from_file(&output)?,
            })
        })
        .await
        .context("Turbolite checkpoint task panicked")?
    }

    async fn checkpoint(&self) -> Result<SnapshotCheckpoint> {
        let tmp = tempfile::NamedTempFile::new()?;
        self.materialize(tmp.path()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use turbolite::tiered::{TurboliteConfig, TurboliteVfs};
    use walrust::hadb_changeset::{physical, storage};

    // The replay floor is independent of SQLite's change counter. Exercise the
    // SnapshotSource via a real restore, not just a field-value assertion.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_restore_replays_from_manifest_cursor_with_checksum() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let db = tmp.path().join("seed.db");
        let conn = rusqlite::Connection::open(&db)?;
        conn.execute_batch("CREATE TABLE t (value TEXT); INSERT INTO t VALUES ('base');")?;
        drop(conn);
        let backend = Arc::new(hadb_storage_local::LocalStorage::new(
            tmp.path().join("objects"),
        ));
        let vfs = TurboliteVfs::with_backend(
            TurboliteConfig {
                cache_dir: tmp.path().join("cache"),
                ..Default::default()
            },
            Arc::new(crate::TurboliteStorage(backend.clone())),
            tokio::runtime::Handle::current(),
        )?;
        vfs.import_sqlite_file(&db)?;
        vfs.flush_to_storage()?;
        vfs.manifest_bytes_with_replay_cursor_anchor(40, 7, "leader")?;
        let mut manifest = vfs.manifest();
        manifest.version += 1;
        manifest.change_counter = 900; // Not a replication sequence.
        vfs.set_manifest(manifest);
        let source = TurboliteSnapshotSource(SharedTurboliteVfs::new(vfs));
        let base_path = tmp.path().join("base.db");
        let base = source.materialize(&base_path).await?;
        assert_eq!(source.checkpoint().await?, base);

        // A legitimate next delta carries the checksum of the materialized
        // snapshot, not a guessed or zero checksum. Change data through SQLite.
        let conn = rusqlite::Connection::open(&db)?;
        conn.execute("INSERT INTO t VALUES ('delta')", [])?;
        drop(conn);
        let encoded = walrust::ltx::encode_snapshot(&db, 4096, 41, base.checksum)?;
        let mut delta = physical::decode(&encoded)?;
        delta = physical::PhysicalChangeset::new(
            41,
            base.checksum,
            delta.header.page_id_size,
            delta.header.page_size,
            delta.pages,
        );
        storage::upload_physical(backend.as_ref(), "", "db", &delta).await?;
        let output = tmp.path().join("restored.db");
        let seq =
            walrust::restore_with_snapshot_source(backend.as_ref(), "", "db", &output, &source)
                .await?;
        assert_eq!(seq, 41);
        let conn = rusqlite::Connection::open(output)?;
        let values = conn
            .prepare("SELECT value FROM t ORDER BY rowid")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(values, ["base", "delta"]);
        Ok(())
    }
}
