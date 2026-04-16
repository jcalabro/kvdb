//! Background worker for pruning old pub/sub messages from FDB.
//!
//! Messages accumulate in `pubsub/msg/<channel, versionstamp>`.
//! This worker periodically scans and removes messages older than
//! the configured retention period.
//!
//! The retention period is generous (default 60 seconds) because
//! messages only need to survive long enough for watch-based
//! subscribers to read them (typically <100ms). The extra headroom
//! handles slow subscribers and network hiccups.

use foundationdb::RangeOption;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::PubSubDirectories;
use crate::storage::Database;

/// Configuration for the pub/sub cleanup worker.
#[derive(Debug, Clone)]
pub struct PubSubCleanupConfig {
    /// How often to run the cleanup scan (seconds).
    pub scan_interval_secs: u64,
    /// Maximum age of messages to retain (seconds).
    pub retention_secs: u64,
    /// Maximum keys to delete per cleanup transaction.
    pub batch_size: usize,
}

impl Default for PubSubCleanupConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 10,
            retention_secs: 60,
            batch_size: 1000,
        }
    }
}

/// Run the pub/sub message cleanup loop.
///
/// Periodically scans `pubsub/msg/` and removes messages that have
/// been around longer than the retention period. Uses FDB's committed
/// version to estimate message age.
pub async fn run(db: Database, dirs: PubSubDirectories, config: PubSubCleanupConfig, cancel: CancellationToken) {
    let interval = tokio::time::Duration::from_secs(config.scan_interval_secs);
    info!(
        interval_secs = config.scan_interval_secs,
        retention_secs = config.retention_secs,
        "pub/sub cleanup worker started"
    );

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                if let Err(e) = cleanup_old_messages(&db, &dirs, &config).await {
                    warn!(error = %e, "pub/sub cleanup error");
                }
            }
            _ = cancel.cancelled() => {
                info!("pub/sub cleanup worker shutting down");
                return;
            }
        }
    }
}

/// One pass of the cleanup scan.
///
/// Strategy: read the oldest batch of messages from `pubsub/msg/`,
/// check if they're older than retention, and delete them. We use
/// the FDB committed version as a proxy for age — messages with
/// versionstamps well below the current version are old.
///
/// This is simpler than tracking wall-clock time per message and
/// works because FDB versionstamps are monotonically increasing
/// with ~1M versions per second.
async fn cleanup_old_messages(
    db: &Database,
    dirs: &PubSubDirectories,
    config: &PubSubCleanupConfig,
) -> Result<(), foundationdb::FdbError> {
    // Get current FDB version to estimate message age.
    let version_trx = db.inner().create_trx()?;
    let current_version = version_trx.get_read_version().await?;

    // FDB increments versions at ~1M/sec. Retention of 60 seconds ~ 60M versions.
    let cutoff_version = current_version - (config.retention_secs as i64 * 1_000_000);
    if cutoff_version <= 0 {
        // Server just started or retention exceeds uptime — nothing to clean.
        return Ok(());
    }

    // Scan the msg/ subspace for old messages. Set read version to the
    // cutoff so we only see messages older than the retention period.
    // Messages are keyed by versionstamp (monotonically increasing with
    // transaction version), so reading at cutoff_version returns only
    // messages committed before that point.
    let trx = db.inner().create_trx()?;
    trx.set_read_version(cutoff_version);
    let (begin, end) = dirs.msg.range();
    let range = RangeOption {
        limit: Some(config.batch_size),
        reverse: false,
        ..RangeOption::from((begin.as_slice(), end.as_slice()))
    };

    let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
    {
        let mut stream = trx.get_ranges_keyvalues(range, true);
        while let Some(result) = stream.next().await {
            let kv = result?;
            keys_to_delete.push(kv.key().to_vec());
        }
    }
    // Stream is dropped here, releasing the borrow on `trx`.

    if !keys_to_delete.is_empty() {
        let del_trx = db.inner().create_trx()?;
        for key in &keys_to_delete {
            del_trx.clear(key);
        }
        del_trx.commit().await?;
        debug!(deleted = keys_to_delete.len(), "cleaned up old pub/sub messages");
    }

    Ok(())
}
