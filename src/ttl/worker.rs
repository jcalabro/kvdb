//! Background expiry worker.
//!
//! Scans the `expire/` directory periodically and deletes keys whose
//! TTL has passed. Complements the lazy expiry check that runs on
//! every read.

use foundationdb::RangeOption;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::observability::metrics;
use crate::storage::helpers;
use crate::storage::{Database, Directories, NamespaceCache};
use crate::ttl::ExpiryConfig;

/// Run the background expiry worker until cancelled.
pub async fn run(ns_cache: NamespaceCache, config: ExpiryConfig, cancel: CancellationToken) {
    info!(
        scan_interval_ms = config.scan_interval_ms,
        batch_size = config.batch_size,
        "background expiry worker started"
    );

    let interval = tokio::time::Duration::from_millis(config.scan_interval_ms);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = cancel.cancelled() => {
                info!("background expiry worker shutting down");
                return;
            }
        }

        let namespaces = ns_cache.cached_namespaces().await;
        for dirs in &namespaces {
            if cancel.is_cancelled() {
                return;
            }
            if let Err(e) = scan_one_namespace(ns_cache.db(), dirs, config.batch_size).await {
                error!(namespace = dirs.namespace, error = %e, "expiry scan failed");
            }
        }
    }
}

async fn scan_one_namespace(
    db: &Database,
    dirs: &Directories,
    batch_size: usize,
) -> Result<(), crate::error::StorageError> {
    let timer = std::time::Instant::now();
    let now = helpers::now_ms();

    // Step 1: Snapshot read to find expired entries.
    let expired_keys: Vec<Vec<u8>> = {
        let trx = db.inner().create_trx().map_err(crate::error::StorageError::Fdb)?;
        let (begin, end) = dirs.expire.range();
        let range_opt = RangeOption::from((begin.as_slice(), end.as_slice()));
        let kvs = trx
            .get_range(&range_opt, batch_size, true)
            .await
            .map_err(crate::error::StorageError::Fdb)?;

        let mut keys = Vec::new();
        for kv in kvs.iter() {
            if kv.value().len() == 8 {
                let ts = u64::from_be_bytes(kv.value().try_into().unwrap_or([0; 8]));
                if now >= ts {
                    // Unpack the Redis key from the FDB expire key.
                    match dirs.expire.unpack::<(Vec<u8>,)>(kv.key()) {
                        Ok((redis_key,)) => keys.push(redis_key),
                        Err(e) => {
                            error!("failed to unpack expire key: {e:?}");
                        }
                    }
                }
            }
        }
        keys
    };

    if expired_keys.is_empty() {
        metrics::EXPIRY_SCAN_DURATION_SECONDS.observe(timer.elapsed().as_secs_f64());
        return Ok(());
    }

    let count = expired_keys.len();
    debug!(namespace = dirs.namespace, count, "cleaning expired keys");

    // Step 2: Delete expired keys in a write transaction.
    let dirs_clone = dirs.clone();
    crate::storage::run_transact(db, "EXPIRE_CLEANUP", |tr| {
        let dirs = dirs_clone.clone();
        let keys = expired_keys.clone();
        async move {
            for key in &keys {
                helpers::delete_object(&tr, &dirs, key, 0)
                    .await
                    .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))?;
            }
            Ok(())
        }
    })
    .await?;

    metrics::EXPIRED_KEYS_TOTAL
        .with_label_values(&["active"])
        .inc_by(count as u64);
    metrics::EXPIRY_SCAN_DURATION_SECONDS.observe(timer.elapsed().as_secs_f64());

    Ok(())
}
