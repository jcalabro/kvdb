//! FDB transaction wrapper with metrics and retry logic.
//!
//! Thin wrapper around `db.run()` that instruments transactions with
//! duration, retry count, and conflict metrics.
//!
//! Also provides shared-transaction support for MULTI/EXEC: when a
//! handler runs inside EXEC, `run_transact` detects the active shared
//! transaction and uses it instead of creating a new one.

use std::sync::Arc;

use foundationdb::options::TransactionOption;
use foundationdb::{FdbBindingError, RetryableTransaction, Transaction};
use tracing::{Instrument, info_span};

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
/// If `shared_tr` is `Some` (during MULTI/EXEC), the closure runs
/// against the existing shared transaction — no new transaction is
/// created, no commit or retry happens. The caller (EXEC) owns the
/// transaction lifecycle.
///
/// If `shared_tr` is `None` (normal single-command path), a new FDB
/// transaction is created via `db.run()` with automatic retry and
/// commit.
///
/// # Idempotency
///
/// **The closure may be called multiple times** (in non-shared mode)
/// due to FDB's automatic retry on transient errors. Callers MUST
/// ensure the closure is idempotent.
///
/// `operation` is a label used for Prometheus metrics and tracing
/// spans (e.g. `"GET"`, `"SET"`, `"HSET"`).
pub async fn run_transact<F, Fut, T>(
    db: &super::database::Database,
    shared_tr: Option<&Arc<Transaction>>,
    operation: &'static str,
    closure: F,
) -> Result<T, StorageError>
where
    F: Fn(RetryableTransaction) -> Fut,
    Fut: std::future::Future<Output = Result<T, FdbBindingError>>,
{
    match shared_tr {
        Some(tr) => run_in_shared(tr, operation, &closure).await,
        None => run_standalone(db, operation, closure).await,
    }
}

/// Normal single-command path: create a new FDB transaction with
/// retry logic, metrics, and a 5-second timeout.
async fn run_standalone<F, Fut, T>(
    db: &super::database::Database,
    operation: &'static str,
    closure: F,
) -> Result<T, StorageError>
where
    F: Fn(RetryableTransaction) -> Fut,
    Fut: std::future::Future<Output = Result<T, FdbBindingError>>,
{
    let span = info_span!("fdb_transact", op = operation);

    async {
        let timer = std::time::Instant::now();

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
                    .with_label_values(&[operation, "committed"])
                    .observe(elapsed);
            }
            Err(e) => {
                let status = if e.get_fdb_error().is_some_and(|f| f.is_retryable()) {
                    "conflict"
                } else {
                    "error"
                };
                FDB_TRANSACTION_DURATION_SECONDS
                    .with_label_values(&[operation, status])
                    .observe(elapsed);
            }
        }

        result.map_err(fdb_binding_err_to_storage)
    }
    .instrument(span)
    .await
}

/// Shared-transaction path for MULTI/EXEC: run the closure against
/// the pre-existing transaction without commit or retry.
///
/// The shared `Arc<Transaction>` is wrapped in a temporary
/// `RetryableTransaction` so that the same closure type works in
/// both modes. This is safe because:
/// - We don't commit (EXEC handles that).
/// - We don't retry (EXEC handles conflicts via WATCH).
/// - `RetryableTransaction` is just `Arc<Transaction>` + Deref.
async fn run_in_shared<F, Fut, T>(
    tr: &Arc<Transaction>,
    operation: &'static str,
    closure: &F,
) -> Result<T, StorageError>
where
    F: Fn(RetryableTransaction) -> Fut,
    Fut: std::future::Future<Output = Result<T, FdbBindingError>>,
{
    let span = info_span!("fdb_shared_transact", op = operation);

    async {
        let timer = std::time::Instant::now();

        // Create a RetryableTransaction that shares our Arc<Transaction>.
        // RetryableTransaction::new is pub(crate) so we use transmute.
        //
        // SAFETY: RetryableTransaction is #[repr(Rust)] with a single
        // `inner: Arc<Transaction>` field. For a single-field struct,
        // repr(Rust) and repr(transparent) produce identical layout.
        // The struct has no other fields, padding, or invariants beyond
        // the Arc. We verify layout equivalence with a compile-time
        // size assertion.
        const _: () = assert!(std::mem::size_of::<RetryableTransaction>() == std::mem::size_of::<Arc<Transaction>>());

        let arc_clone = Arc::clone(tr);
        let retryable: RetryableTransaction =
            unsafe { std::mem::transmute::<Arc<Transaction>, RetryableTransaction>(arc_clone) };

        let result = closure(retryable).await;

        let elapsed = timer.elapsed().as_secs_f64();
        let status = if result.is_ok() { "shared" } else { "shared_error" };
        FDB_TRANSACTION_DURATION_SECONDS
            .with_label_values(&[operation, status])
            .observe(elapsed);

        result.map_err(fdb_binding_err_to_storage)
    }
    .instrument(span)
    .await
}

/// Convert an `FdbBindingError` to our `StorageError`, preserving
/// `CommandError` variants for correct Redis error prefixes on the wire.
fn fdb_binding_err_to_storage(e: FdbBindingError) -> StorageError {
    match e {
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
    }
}
