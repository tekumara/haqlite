//! SQLite replicator implementation using walrust.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use hadb_lease::AtomicFence;
use hadb_storage::StorageBackend;
use walrust::{ReplicationConfig, SnapshotOwnership, SnapshotSource};

use hadb::Replicator;

use crate::epoch_fence;

/// SQLite replicator wrapping walrust.
///
/// Handles WAL replication via HADBP changesets (SQLite-specific format).
pub struct SqliteReplicator {
    inner: Arc<walrust::Replicator>,
    /// When true, `add()` skips the initial snapshot upload.
    /// Used for SingleWriter+Synchronous where remote storage handles durability
    /// and the local SQLite file may be empty (data is in S3 page groups).
    skip_snapshot_on_add: bool,
    /// Legacy acknowledgement of a leftover tail (F5). Current walrust still
    /// rejects sequence/checksum breaks; this never disables native checks.
    allow_fork_bridge: bool,
    /// Reader side of the lease fence. When present, the leader stamps its
    /// current lease epoch into a per-database `_epoch` marker next to the
    /// changesets it publishes (F11). The follower reads that marker and
    /// refuses a strictly-lower epoch (a former leader's stale write). `None`
    /// in single-node / no-lease deployments: the marker is never written and
    /// the follower never gates, preserving the legacy behavior.
    fence: Option<AtomicFence>,
    retry_policy: walrust::RetryPolicy,
}

/// Retain the explicit leftover-tail guard (F5). Current walrust also rejects
/// forks during restore and preflights every incremental before writing pages.
/// A legacy acknowledgement bypasses only this extra guard, never those checks.
async fn ensure_no_fork_after_restore(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    restored_seq: u64,
    allow_fork_bridge: bool,
) -> Result<()> {
    if allow_fork_bridge {
        return Ok(());
    }
    let namespace = crate::replication_cursor::database_namespace(storage, prefix, name).await?;
    let leftover = walrust::hadb_changeset::storage::discover_after(
        storage,
        prefix,
        &namespace,
        restored_seq,
        walrust::hadb_changeset::storage::ChangesetKind::Physical,
    )
    .await?;
    if let Some(first) = leftover.first() {
        anyhow::bail!(
            "refusing to bridge a chain break for '{}': restore stopped at seq {} but {} changeset(s) remain (next at seq {}); these are a forked lineage restore refused. Enable explicit fork-ack to bridge.",
            name,
            restored_seq,
            leftover.len(),
            first.seq,
        );
    }
    Ok(())
}

impl SqliteReplicator {
    /// Create a new SqliteReplicator.
    ///
    /// `storage` is the walrust S3 backend for WAL data.
    /// `prefix` is the S3 key prefix for all databases (e.g. "wal/" or "ha/").
    /// `config` is the walrust replication configuration.
    pub fn new(storage: Arc<dyn StorageBackend>, prefix: &str, config: ReplicationConfig) -> Self {
        Self {
            retry_policy: config.retry_policy.clone(),
            inner: walrust::Replicator::new(storage, prefix, config),
            skip_snapshot_on_add: false,
            allow_fork_bridge: false,
            fence: None,
        }
    }

    async fn resume_or_add(&self, name: &str, path: &Path) -> Result<()> {
        // Saved WAL offsets belong to the old local files, not this restored
        // database. Re-anchor under a held lease before registering a sender.
        let namespace = crate::replication_cursor::database_namespace(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
        )
        .await?;
        if walrust::hadb_changeset::storage::discover_latest_snapshot(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            &namespace,
            walrust::hadb_changeset::storage::ChangesetKind::Physical,
        )
        .await?
        .is_none()
        {
            return self.inner.add(name, path).await;
        }
        let fence = self
            .fence
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("resuming a restored leader requires a lease fence"))?;
        let lease = ResumeLease {
            fence,
            floor: fence.current().ok_or_else(|| {
                anyhow::anyhow!("cannot resume restored leader without an active lease")
            })?,
        };
        // Check ownership before restore can replace local files as well as
        // before publishing the new base.
        lease.ensure_active()?;
        let restored = walrust::sync::restore(
            self.inner.storage().clone(),
            self.inner.prefix(),
            name,
            path,
            None,
        )
        .await?;
        let mut state = walrust::SyncState::new(path.to_path_buf())?;
        state.name = name.to_owned();
        walrust::sync::resume_owned_after_restore(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            &mut state,
            &restored,
            &lease,
            &self.retry_policy,
        )
        .await?;
        lease.ensure_active()?;
        self.inner.add_without_snapshot(name, path).await
    }

    /// Attach the lease fence so each `sync()` stamps the current leader epoch
    /// into the per-database `_epoch` marker (F11). Pair this with the same
    /// fence handed to the follower behavior so a former leader's stale
    /// changesets are rejected on apply.
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

    /// Acknowledge a leftover tail for the legacy extra guard (F5).
    /// Native walrust sequence and checksum verification always remains on.
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
        self.inner.restore(name, output_path).await
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

// The coordinator owns renewal and clears this fence on lease loss. Require
// it throughout walrust's retryable resume; a cleared or regressed revision
// must not publish a resumed stream. Renewals may advance the revision.
struct ResumeLease<'a> {
    fence: &'a AtomicFence,
    floor: u64,
}

impl ResumeLease<'_> {
    fn ensure_active(&self) -> Result<()> {
        match self.fence.current() {
            Some(rev) if rev >= self.floor => Ok(()),
            _ => anyhow::bail!("lease lost while resuming restored leader"),
        }
    }
}

impl walrust::OwnedResumeLease for ResumeLease<'_> {
    fn ensure_held(&self) -> Result<()> {
        self.ensure_active()
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
        })
    }

    /// Acknowledge a leftover tail for the legacy extra guard (F5).
    /// Native walrust sequence and checksum verification always remains on.
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
            self.resume_or_add(name, path).await
        }
    }

    async fn add_continuing(&self, name: &str, path: &Path) -> Result<()> {
        self.resume_or_add(name, path).await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        // Restore from latest snapshot + chained incrementals.
        let restored_seq = self.inner.restore(name, path).await?;
        let seq = restored_seq.unwrap_or(0);

        // F5: a changeset still present after `restored_seq` is one restore
        // refused for a checksum-chain break (a forked lineage). Bridging it
        // by page content silently merges divergent history, so hard-error
        // unless fork-bridging is explicitly acknowledged.
        ensure_no_fork_after_restore(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            seq,
            self.allow_fork_bridge,
        )
        .await?;

        if restored_seq.is_none() {
            return Ok(());
        }

        // Apply any newly published incrementals with native sequence and
        // checksum verification, even when the legacy guard was acknowledged.
        let final_seq = walrust::sync::pull_incremental(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            path,
            crate::replication_cursor::cursor_at(
                self.inner.storage().as_ref(),
                self.inner.prefix(),
                name,
                path,
                seq,
            )
            .await?,
        )
        .await?
        .seq;

        if final_seq > seq {
            tracing::info!(
                "SqliteReplicator::pull('{}') bridged chain gap: restore seq {} -> pull seq {}",
                name,
                seq,
                final_seq,
            );
        }
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.remove(name).await
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
        self.inner.add_without_snapshot(name, path).await
    }

    async fn pull(&self, name: &str, path: &Path) -> Result<()> {
        let restored_seq = walrust::restore_with_snapshot_source(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            path,
            self.snapshot_source.as_ref(),
        )
        .await?;

        // F5: hard-error on a forked tail restore refused, unless fork-bridging
        // is explicitly acknowledged.
        ensure_no_fork_after_restore(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            restored_seq,
            self.allow_fork_bridge,
        )
        .await?;

        let final_seq = walrust::sync::pull_incremental(
            self.inner.storage().as_ref(),
            self.inner.prefix(),
            name,
            path,
            crate::replication_cursor::cursor_at(
                self.inner.storage().as_ref(),
                self.inner.prefix(),
                name,
                path,
                restored_seq,
            )
            .await?,
        )
        .await?
        .seq;

        if final_seq > restored_seq {
            tracing::info!(
                "ExternalSnapshotSqliteReplicator::pull('{}') advanced after restore: {} -> {}",
                name,
                restored_seq,
                final_seq,
            );
        }
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        self.inner.remove(name).await
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
