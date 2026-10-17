//! SQLite-specific scheduling for cursor-aware restore/replay.
//!
//! The replicator owns the applied cursor, checksum anchor and epoch gate so
//! normal replay and promotion catch-up use the same verification contract.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::watch;

use hadb::{FollowerBehavior, HaMetrics, Replicator};

#[derive(Default)]
pub struct SqliteFollowerBehavior {
    wakeup: Option<Arc<tokio::sync::Notify>>,
}

impl SqliteFollowerBehavior {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable fast-path wakeup on ManifestChanged events.
    pub fn with_wakeup(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.wakeup = Some(notify);
        self
    }
}

#[async_trait]
impl FollowerBehavior for SqliteFollowerBehavior {
    async fn run_follower_loop(
        &self,
        replicator: Arc<dyn Replicator>,
        _prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        poll_interval: Duration,
        position: Arc<AtomicU64>,
        caught_up: Arc<AtomicBool>,
        mut cancel_rx: watch::Receiver<bool>,
        metrics: Arc<HaMetrics>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(poll_interval);
        loop {
            tokio::select! {
                biased;
                _ = cancel_rx.changed() => return Ok(()),
                _ = async {
                    if let Some(wakeup) = &self.wakeup {
                        tokio::select! {
                            _ = interval.tick() => {},
                            _ = wakeup.notified() => {},
                        }
                    } else {
                        interval.tick().await;
                    }
                } => {
                    if *cancel_rx.borrow() {
                        return Ok(());
                    }
                    let previous = position.load(Ordering::SeqCst);
                    match replicator.pull_incremental(db_name, db_path).await {
                        Ok(seq) => {
                            position.store(seq, Ordering::SeqCst);
                            metrics.follower_replay_position.store(seq, Ordering::Relaxed);
                            caught_up.store(true, Ordering::SeqCst);
                            metrics.follower_caught_up.store(1, Ordering::Relaxed);
                            if seq > previous {
                                tracing::debug!("Follower '{}': pulled seq {} -> {}", db_name, previous, seq);
                                metrics.inc(&metrics.follower_pulls_succeeded);
                            } else {
                                metrics.inc(&metrics.follower_pulls_no_new_data);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Follower '{}': replay failed: {}", db_name, e);
                            caught_up.store(false, Ordering::SeqCst);
                            metrics.follower_caught_up.store(0, Ordering::Relaxed);
                            metrics.inc(&metrics.follower_pulls_failed);
                        }
                    }
                }
            }
        }
    }

    async fn catchup_on_promotion(
        &self,
        replicator: Arc<dyn Replicator>,
        _prefix: &str,
        db_name: &str,
        db_path: &PathBuf,
        position: u64,
    ) -> Result<()> {
        let seq = replicator.pull_incremental(db_name, db_path).await?;
        tracing::info!(
            "SqliteFollowerBehavior '{}': caught up seq {} -> {}",
            db_name,
            position,
            seq
        );
        Ok(())
    }
}
