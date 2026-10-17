//! SQLite replicator implementation using walrust.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use hadb_lease::AtomicFence;
use hadb_storage::StorageBackend;
use walrust::{ReplicationConfig, SnapshotOwnership, SnapshotSource};

use hadb::Replicator;

use crate::epoch_fence::{self, AppliedEpoch};

#[derive(Default)]
struct ReplaySession {
    cursor: Option<walrust::ReplayCursor>,
    epoch: AppliedEpoch,
}

/// One owner for restore, live replay and promotion catch-up. Scope receipts
/// (including epoch high-water marks) to the local database, not the remote head.
#[derive(Default)]
struct ReplaySessions(tokio::sync::Mutex<HashMap<(String, PathBuf), ReplaySession>>);

impl ReplaySession {
    async fn gate_epoch(&self, inner: &walrust::Replicator, name: &str) -> Result<()> {
        if let Some(epoch) =
            epoch_fence::read_leader_epoch(inner.storage().as_ref(), inner.prefix(), name).await?
        {
            self.epoch.admit(name, epoch)?;
        }
        Ok(())
    }

    async fn replay(
        &mut self,
        inner: &walrust::Replicator,
        name: &str,
        path: &Path,
        allow_fork_bridge: bool,
    ) -> Result<u64> {
        self.gate_epoch(inner, name).await?;
        let Some(cursor) = self.cursor else {
            let tail = walrust::hadb_changeset::storage::discover_after(
                inner.storage().as_ref(),
                inner.prefix(),
                name,
                0,
                walrust::hadb_changeset::storage::ChangesetKind::Physical,
            )
            .await?;
            if !tail.is_empty() {
                anyhow::bail!("no snapshot for '{name}': refusing an unanchored incremental tail");
            }
            return Ok(0);
        };
        let policy = if allow_fork_bridge {
            walrust::ReplayPolicy::AllowForkBridge
        } else {
            walrust::ReplayPolicy::Strict
        };
        let cursor = walrust::pull_incremental_with_cursor(
            inner.storage().as_ref(),
            inner.prefix(),
            name,
            path,
            cursor,
            policy,
        )
        .await?;
        self.cursor = Some(cursor);
        Ok(cursor.seq)
    }
}

/// SQLite replicator wrapping walrust.
///
/// Handles WAL replication via HADBP changesets (SQLite-specific format).
pub struct SqliteReplicator {
    inner: Arc<walrust::Replicator>,
    /// When true, `add()` skips the initial snapshot upload.
    /// Used for SingleWriter+Synchronous where remote storage handles durability
    /// and the local SQLite file may be empty (data is in S3 page groups).
    skip_snapshot_on_add: bool,
    /// When true, `pull()` is allowed to bridge a chain break by applying
    /// forked changesets by page content (the old behavior). Default false:
    /// a chain break that `restore()` refused is a hard error rather than a
    /// silent fork merge (F5). Only enable with an explicit fork-ack policy.
    allow_fork_bridge: bool,
    /// Reader side of the lease fence. When present, the leader stamps its
    /// current lease epoch into a per-database `_epoch` marker next to the
    /// changesets it publishes (F11). The follower reads that marker and
    /// refuses a strictly-lower epoch (a former leader's stale write). `None`
    /// in single-node / no-lease deployments: the marker is never written and
    /// the follower never gates, preserving the legacy behavior.
    fence: Option<AtomicFence>,
    replay: ReplaySessions,
}

impl SqliteReplicator {
    /// Create a new SqliteReplicator.
    ///
    /// `storage` is the walrust S3 backend for WAL data.
    /// `prefix` is the S3 key prefix for all databases (e.g. "wal/" or "ha/").
    /// `config` is the walrust replication configuration.
    pub fn new(storage: Arc<dyn StorageBackend>, prefix: &str, config: ReplicationConfig) -> Self {
        Self {
            inner: walrust::Replicator::new(storage, prefix, config),
            skip_snapshot_on_add: false,
            allow_fork_bridge: false,
            fence: None,
            replay: ReplaySessions::default(),
        }
    }

    /// Attach the lease fence so each `sync()` stamps the current leader epoch
    /// into the per-database `_epoch` marker (F11). Restore and replay retain
    /// the highest accepted marker so a former leader's stale changesets are
    /// rejected on apply.
    pub fn with_fence(mut self, fence: AtomicFence) -> Self {
        self.fence = Some(fence);
        self
    }

    /// Skip snapshot upload on `add()`. For SingleWriter+Synchronous where
    /// remote storage handles durability and the local SQLite file has no data.
    pub fn with_skip_snapshot(mut self, skip: bool) -> Self {
        self.skip_snapshot_on_add = skip;
        self
    }

    /// Allow `pull()` to bridge a chain break by applying forked changesets
    /// by page content. Default is false (fork = hard error). Only enable
    /// behind an explicit fork-acknowledgement policy (F5).
    pub fn with_allow_fork_bridge(mut self, allow: bool) -> Self {
        self.allow_fork_bridge = allow;
        self
    }

    /// Get a reference to the inner walrust Replicator.
    ///
    /// Useful for operations not exposed by the hadb Replicator trait.
    pub fn inner(&self) -> &Arc<walrust::Replicator> {
        &self.inner
    }

    /// Restore a database from S3, returning the final seq.
    ///
    /// This is SQLite-specific and not part of the generic Replicator trait.
    pub async fn restore(&self, name: &str, output_path: &Path) -> Result<Option<u64>> {
        let mut sessions = self.replay.0.lock().await;
        let session = sessions
            .entry((name.to_string(), output_path.to_path_buf()))
            .or_default();
        session.gate_epoch(&self.inner, name).await?;
        session.cursor = None;
        let cursor = self.inner.restore_with_cursor(name, output_path).await?;
        session.cursor = cursor;
        Ok(cursor.map(|cursor| cursor.seq))
    }

    /// Register a database without taking a snapshot.
    /// Use after restore() to avoid uploading a redundant snapshot.
    pub async fn add_without_snapshot(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.add_without_snapshot(name, path).await
    }

    /// Register a database with an explicit WAL path.
    pub async fn add_with_wal_path(&self, name: &str, path: &Path, wal_path: &Path) -> Result<()> {
        self.inner.add_with_wal_path(name, path, wal_path).await
    }

    /// Register a database without taking a snapshot, with an explicit WAL path.
    pub async fn add_without_snapshot_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
    ) -> Result<()> {
        self.inner
            .add_without_snapshot_with_wal_path(name, path, wal_path)
            .await
    }

    pub async fn add_external_base_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
        base: walrust::ExternalBaseCursor,
    ) -> Result<()> {
        self.inner
            .add_external_base_with_wal_path(name, path, wal_path, base)
            .await
    }
}

/// SQLite replicator for external-base-state mode.
///
/// Use this when another layer owns checkpointed base state and walrust should
/// only ship / replay WAL deltas after that checkpoint.
pub struct ExternalSnapshotSqliteReplicator {
    inner: Arc<walrust::Replicator>,
    snapshot_source: Arc<dyn SnapshotSource>,
    /// See `SqliteReplicator::allow_fork_bridge` (F5). Default false.
    allow_fork_bridge: bool,
    replay: ReplaySessions,
}

impl ExternalSnapshotSqliteReplicator {
    /// Create a new external-base-state SQLite replicator.
    ///
    /// The wrapper forces walrust into external snapshot ownership. The caller
    /// must still pass `autonomous_snapshots = false`; enabling periodic
    /// snapshots in this mode is a real bug and is rejected.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        prefix: &str,
        mut config: ReplicationConfig,
        snapshot_source: Arc<dyn SnapshotSource>,
    ) -> Result<Self> {
        if config.autonomous_snapshots {
            anyhow::bail!(
                "external-base-state SQLite replication requires autonomous_snapshots = false"
            );
        }
        config.snapshot_ownership = SnapshotOwnership::External;

        Ok(Self {
            inner: walrust::Replicator::try_new(storage, prefix, config)?,
            snapshot_source,
            allow_fork_bridge: false,
            replay: ReplaySessions::default(),
        })
    }

    /// Allow `pull()` to bridge a chain break by applying forked changesets
    /// by page content. Default is false (fork = hard error). Only enable
    /// behind an explicit fork-acknowledgement policy (F5).
    pub fn with_allow_fork_bridge(mut self, allow: bool) -> Self {
        self.allow_fork_bridge = allow;
        self
    }

    pub fn inner(&self) -> &Arc<walrust::Replicator> {
        &self.inner
    }

    pub async fn add_with_wal_path(&self, name: &str, path: &Path, wal_path: &Path) -> Result<()> {
        self.inner.add_with_wal_path(name, path, wal_path).await
    }

    pub async fn add_without_snapshot_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
    ) -> Result<()> {
        self.inner
            .add_without_snapshot_with_wal_path(name, path, wal_path)
            .await
    }

    pub async fn add_external_base_with_wal_path(
        &self,
        name: &str,
        path: &Path,
        wal_path: &Path,
        base: walrust::ExternalBaseCursor,
    ) -> Result<()> {
        self.inner
            .add_external_base_with_wal_path(name, path, wal_path, base)
            .await
    }
}

#[async_trait]
impl Replicator for SqliteReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        if self.skip_snapshot_on_add {
            self.inner.add_without_snapshot(name, path).await
        } else {
            self.inner.add(name, path).await
        }
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        // Serialize against any replay finishing as promotion stops the loop.
        let _sessions = self.replay.0.lock().await;
        self.inner.add_without_snapshot(name, path).await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        self.pull_with_position(name, path).await.map(|_| ())
    }

    async fn pull_with_position(&self, name: &str, path: &Path) -> Result<u64> {
        let mut sessions = self.replay.0.lock().await;
        let session = sessions
            .entry((name.to_string(), path.to_path_buf()))
            .or_default();
        session.gate_epoch(&self.inner, name).await?;
        // A failed/partial restore must never leave an old cursor usable.
        session.cursor = None;
        session.cursor = self.inner.restore_with_cursor(name, path).await?;
        // A valid tail published during restore is safe to replay; a fork that
        // restore stopped at still fails against the retained anchor (F5).
        session
            .replay(&self.inner, name, path, self.allow_fork_bridge)
            .await
    }

    async fn pull_incremental(&self, name: &str, path: &Path) -> Result<u64> {
        let mut sessions = self.replay.0.lock().await;
        let session = sessions
            .entry((name.to_string(), path.to_path_buf()))
            .or_default();
        if session.cursor.is_none() {
            // The follower may have joined before the first snapshot existed.
            // Only an actual restore may establish its starting cursor.
            session.gate_epoch(&self.inner, name).await?;
            session.cursor = self.inner.restore_with_cursor(name, path).await?;
        }
        session
            .replay(&self.inner, name, path, self.allow_fork_bridge)
            .await
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.remove(name).await;
        Ok(())
    }

    async fn sync(&self, name: &str) -> Result<()> {
        let frames = self.inner.flush(name).await?;
        if frames > 0 {
            tracing::info!(
                "SqliteReplicator::sync('{}') flushed {} frames",
                name,
                frames
            );
        }
        // F11: stamp the leader's current lease epoch into the per-database
        // marker so the follower can fence a former leader's stale changesets.
        // Read the epoch at publish time and ship it next to the changesets —
        // a demoted leader carries its own (now-superseded) epoch, never the
        // live one. No fence configured (single-node) = no marker, no gate.
        if let Some(fence) = &self.fence {
            if let Some(epoch) = fence.current() {
                epoch_fence::stamp_leader_epoch(
                    self.inner.storage().as_ref(),
                    self.inner.prefix(),
                    name,
                    epoch,
                )
                .await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Replicator for ExternalSnapshotSqliteReplicator {
    async fn add(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.add(name, path).await
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        let _sessions = self.replay.0.lock().await;
        self.inner.add_without_snapshot(name, path).await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        self.pull_with_position(name, path).await.map(|_| ())
    }

    async fn pull_with_position(&self, name: &str, path: &Path) -> Result<u64> {
        let mut sessions = self.replay.0.lock().await;
        let session = sessions
            .entry((name.to_string(), path.to_path_buf()))
            .or_default();
        session.gate_epoch(&self.inner, name).await?;
        session.cursor = None;
        let seq = self.snapshot_source.materialize(path).await?;
        session.cursor = Some(walrust::ReplayCursor {
            seq,
            checksum: walrust::ltx::compute_checksum_from_file(path)?,
            generation: walrust::ReplayGeneration::ExternalBase,
        });
        session
            .replay(&self.inner, name, path, self.allow_fork_bridge)
            .await
    }

    async fn pull_incremental(&self, name: &str, path: &Path) -> Result<u64> {
        let mut sessions = self.replay.0.lock().await;
        let session = sessions
            .get_mut(&(name.to_string(), path.to_path_buf()))
            .ok_or_else(|| anyhow::anyhow!("no restored replay cursor for '{name}'"))?;
        session
            .replay(&self.inner, name, path, self.allow_fork_bridge)
            .await
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.remove(name).await;
        Ok(())
    }

    async fn sync(&self, name: &str) -> Result<()> {
        let frames = self.inner.flush(name).await?;
        if frames > 0 {
            tracing::info!(
                "ExternalSnapshotSqliteReplicator::sync('{}') flushed {} frames",
                name,
                frames,
            );
        }
        Ok(())
    }
}
