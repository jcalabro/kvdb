//! FDB transaction wrapper with metrics and retry logic.
//!
//! Thin wrapper around `db.run()` that instruments transactions with
//! duration, retry count, and conflict metrics.

use foundationdb::options::TransactionOption;
use foundationdb::{FdbBindingError, RetryableTransaction};
use tracing::info_span;

use crate::error::{CommandError, StorageError};
use crate::observability::metrics::FDB_TRANSACTION_DURATION_SECONDS;

/// Transaction isolation mode.
#[derive(Debug, Clone, Copy)]
pub enum IsolationMode {
    /// Standard read-write transaction with full conflict detection.
    ReadWrite,
    /// Snapshot reads that don't add to the conflict set.
    /// Use for non-critical reads or large scans.
    Snapshot,
}

/// Run `closure` inside an FDB transaction with retry logic, metrics,
/// and a 5-second timeout.
///
/// The closure receives a `RetryableTransaction` (which derefs to
/// `Transaction`) and should perform all reads/writes on it. The
/// transaction is automatically committed and retried on transient
/// errors (conflicts, network issues).
///
/// # Idempotency
///
/// **The closure may be called multiple times** due to FDB's automatic
/// retry on transient errors. Callers MUST ensure the closure is
/// idempotent — it should produce the same result regardless of how
/// many times it runs. In practice this means: don't perform
/// side effects outside the transaction (e.g., incrementing an
/// in-memory counter, sending a network request) inside the closure.
///
/// `operation` is a label used for Prometheus metrics and tracing
/// spans (e.g. `"GET"`, `"SET"`, `"HSET"`).
pub async fn run_transact<F, Fut, T>(
    db: &super::database::Database,
    operation: &str,
    closure: F,
) -> Result<T, StorageError>
where
    F: Fn(RetryableTransaction) -> Fut,
    Fut: std::future::Future<Output = Result<T, FdbBindingError>>,
{
    let span = info_span!("fdb_transact", op = operation);
    let _enter = span.enter();

    let timer = std::time::Instant::now();

    let op_str = operation.to_owned();
    let result: Result<T, FdbBindingError> = db
        .inner()
        .run(|tr, _maybe_committed| {
            // Set 5-second timeout on each attempt.
            let _ = tr.set_option(TransactionOption::Timeout(5000));
            closure(tr)
        })
        .await;

    let elapsed = timer.elapsed().as_secs_f64();

    match &result {
        Ok(_) => {
            FDB_TRANSACTION_DURATION_SECONDS
                .with_label_values(&[op_str.as_str(), "committed"])
                .observe(elapsed);
        }
        Err(e) => {
            let status = if e.get_fdb_error().is_some_and(|f| f.is_retryable()) {
                "conflict"
            } else {
                "error"
            };
            FDB_TRANSACTION_DURATION_SECONDS
                .with_label_values(&[op_str.as_str(), status])
                .observe(elapsed);
        }
    }

    result.map_err(|e| match e {
        FdbBindingError::NonRetryableFdbError(fdb_err) => StorageError::Fdb(fdb_err),
        FdbBindingError::CustomError(boxed) => {
            // Try to extract a CommandError (e.g. WrongType) to preserve
            // the original Redis error prefix on the wire.
            match boxed.downcast::<CommandError>() {
                Ok(cmd_err) => StorageError::Command(*cmd_err),
                Err(other) => StorageError::FdbBinding(format!("{other}")),
            }
        }
        other => StorageError::FdbBinding(format!("{other}")),
    })
}
