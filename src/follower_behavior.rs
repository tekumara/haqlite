//! SQLite-specific follower behavior implementation.
//!
//! WAL-based (walrust): for Replicated and Eventual durability

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::{FollowerBehavior, HaMetrics, Replicator};
use hadb_storage::StorageBackend;

use crate::epoch_fence::{self, AppliedEpoch};

/// SQLite-specific follower behavior.
///
/// For WAL-based durability (Replicated/Eventual), uses walrust pull_incremental.
pub struct SqliteFollowerBehavior {
    /// walrust storage backend for WAL-based replication.
    walrust_storage: Arc<dyn StorageBackend>,
    /// Optional wakeup signal for fast-path manifest catch-up.
    /// When the coordinator receives a ManifestChanged event, it calls notify_one()
    /// to immediately wake the follower loop instead of waiting for poll interval.
    wakeup: Option<Arc<tokio::sync::Notify>>,
    /// Highest leader epoch this follower has accepted (F11). Seeded empty;
    /// advanced as higher-epoch batches apply. A changeset batch stamped with
    /// an epoch strictly below this mark is a former leader's stale write and
    /// is refused before `pull_incremental` applies it. Shared as `Arc` so all
    /// apply paths (`run_follower_loop`, `catchup_on_promotion`) gate on the
    /// same high-water mark.
    applied_epoch: Arc<AppliedEpoch>,
}

// walrust-core now verifies the complete sequence/checksum chain before writing
// any pages. Retain its full PullCursor between polls; hadb exposes only seq.

impl SqliteFollowerBehavior {
    pub fn new(walrust_storage: Arc<dyn StorageBackend>) -> Self {
        Self {
            walrust_storage,
            wakeup: None,
            applied_epoch: Arc::new(AppliedEpoch::new()),
        }
    }

    /// Enable fast-path wakeup on ManifestChanged events.
    /// The Notify is shared with the role listener which calls notify_one()
    /// when the coordinator receives ManifestChanged.
    pub fn with_wakeup(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.wakeup = Some(notify);
        self
    }

    /// F11 leader-epoch gate. Reads the per-database `_epoch` marker the leader
    /// stamped at publish time and refuses to apply when the stamped epoch is
    /// strictly below the highest epoch this follower has already accepted —
    /// a former leader's stale changeset. An accepted epoch advances the
    /// high-water mark. No marker (legacy database / no lease fence) = no gate.
    ///
    /// Reads the stamped marker, never the live lease: a demoted leader's
    /// published batch carries its own superseded epoch, so the follower must
    /// compare against what travelled with the changesets.
    async fn gate_leader_epoch(&self, prefix: &str, db_name: &str) -> Result<()> {
        match epoch_fence::read_leader_epoch(self.walrust_storage.as_ref(), prefix, db_name).await?
        {
            Some(epoch) => self.applied_epoch.admit(db_name, epoch),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl FollowerBehavior for SqliteFollowerBehavior {
    async fn run_follower_loop(
        &self,
        _replicator: Arc<dyn Replicator>,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        poll_interval: Duration,
        position: Arc<AtomicU64>,
        caught_up: Arc<AtomicBool>,
        mut cancel_rx: watch::Receiver<bool>,
        metrics: Arc<HaMetrics>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(poll_interval);
        let mut cursor: Option<walrust::PullCursor> = None;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let current_seq = position.load(Ordering::SeqCst);
                    let current = match cursor {
                        Some(current) if current.seq == current_seq => current,
                        _ => match crate::replication_cursor::cursor_at(
                            self.walrust_storage.as_ref(), prefix, db_name, db_path, current_seq,
                        ).await {
                            Ok(current) => current,
                            Err(e) => {
                                tracing::error!("Follower '{}': cannot establish cursor: {}", db_name, e);
                                caught_up.store(false, Ordering::SeqCst);
                                metrics.follower_caught_up.store(0, Ordering::Relaxed);
                                metrics.inc(&metrics.follower_pulls_failed);
                                continue;
                            }
                        },
                    };
                    // F11: refuse a former leader's stale-epoch changeset before
                    // applying it. Fail closed (stall, visible) on rejection.
                    if let Err(e) = self.gate_leader_epoch(prefix, db_name).await {
                        tracing::error!("Follower '{}': leader-epoch gate rejected: {}", db_name, e);
                        caught_up.store(false, Ordering::SeqCst);
                        metrics.follower_caught_up.store(0, Ordering::Relaxed);
                        metrics.inc(&metrics.follower_pulls_failed);
                        continue;
                    }
                    match walrust::sync::pull_incremental(
                        self.walrust_storage.as_ref(),
                        prefix,
                        db_name,
                        db_path,
                        current,
                    ).await {
                        Ok(next) => {
                            cursor = Some(next);
                            let new_seq = next.seq;
                            if new_seq > current_seq {
                                tracing::debug!(
                                    "Follower '{}': pulled seq {} -> {}",
                                    db_name, current_seq, new_seq
                                );
                                position.store(new_seq, Ordering::SeqCst);
                                metrics.follower_replay_position.store(new_seq, Ordering::Relaxed);
                                metrics.inc(&metrics.follower_pulls_succeeded);
                                caught_up.store(true, Ordering::SeqCst);
                                metrics.follower_caught_up.store(1, Ordering::Relaxed);
                            } else {
                                caught_up.store(true, Ordering::SeqCst);
                                metrics.follower_caught_up.store(1, Ordering::Relaxed);
                                metrics.inc(&metrics.follower_pulls_no_new_data);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Follower '{}': pull failed: {}", db_name, e);
                            caught_up.store(false, Ordering::SeqCst);
                            metrics.follower_caught_up.store(0, Ordering::Relaxed);
                            metrics.inc(&metrics.follower_pulls_failed);
                        }
                    }
                }
                _ = cancel_rx.changed() => {
                    return Ok(());
                }
            }
        }
    }

    async fn catchup_on_promotion(
        &self,
        prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        position: u64,
    ) -> Result<()> {
        let cursor = crate::replication_cursor::cursor_at(
            self.walrust_storage.as_ref(),
            prefix,
            db_name,
            db_path,
            position,
        )
        .await?;
        // F11: refuse a former leader's stale-epoch changeset before applying.
        self.gate_leader_epoch(prefix, db_name).await?;
        let new_seq = walrust::sync::pull_incremental(
            self.walrust_storage.as_ref(),
            prefix,
            db_name,
            db_path,
            cursor,
        )
        .await?;
        tracing::info!(
            "SqliteFollowerBehavior '{}': caught up seq {} -> {}",
            db_name,
            position,
            new_seq.seq
        );
        Ok(())
    }
}
