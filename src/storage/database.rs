//! FDB database initialization and lifecycle.
//!
//! Provides the `boot()` function to initialize the FDB network thread
//! (which can only be called once per process) and a `Database` handle
//! for creating transactions.

use std::sync::Once;

use tracing::info;

/// One-time guard for `foundationdb::boot()`.
///
/// The FDB network can only be started once per process. We use `Once`
/// to guarantee exactly-once initialization, and we leak the
/// `NetworkAutoStop` handle so its `Drop` never runs during normal
/// execution (the OS reclaims resources on process exit). This avoids
/// the unsafety of a mutable static while keeping the guarantee that
/// the network thread stays alive for the entire program lifetime.
static BOOT_ONCE: Once = Once::new();

/// Initialize the FDB network thread. Safe to call multiple times;
/// only the first call has any effect.
///
/// # Safety
///
/// `foundationdb::boot()` is itself unsafe because its returned
/// `NetworkAutoStop` must not be dropped after the process exits.
/// We deliberately leak it so `Drop` never runs.
pub fn boot() {
    BOOT_ONCE.call_once(|| {
        // SAFETY: We leak the NetworkAutoStop so it is never dropped,
        // satisfying the contract that it outlives the process.
        let network = unsafe { foundationdb::boot() };
        std::mem::forget(network);
        info!("FDB network thread started");
    });
}

/// Handle to a FoundationDB database.
///
/// Thin wrapper around `foundationdb::Database` providing our
/// constructor conventions (from cluster file path or default path).
/// `Clone` is implemented by wrapping in `Arc` internally by FDB.
#[derive(Clone)]
pub struct Database {
    inner: std::sync::Arc<foundationdb::Database>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish_non_exhaustive()
    }
}

impl Database {
    /// Open a database using the given cluster file path.
    pub fn new(cluster_file: &str) -> crate::error::Result<Self> {
        boot();
        let db = foundationdb::Database::from_path(cluster_file).map_err(crate::error::StorageError::Fdb)?;
        Ok(Self {
            inner: std::sync::Arc::new(db),
        })
    }

    /// Open a database using the default cluster file location.
    pub fn new_default() -> crate::error::Result<Self> {
        boot();
        let db = foundationdb::Database::default().map_err(crate::error::StorageError::Fdb)?;
        Ok(Self {
            inner: std::sync::Arc::new(db),
        })
    }

    /// Access the underlying `foundationdb::Database`.
    pub fn inner(&self) -> &foundationdb::Database {
        &self.inner
    }
}
