//! FDB database initialization and lifecycle.
//!
//! Provides the `boot()` function to initialize the FDB network thread
//! (which can only be called once per process) and a `Database` handle
//! for creating transactions.
//!
//! M3 will implement real FDB initialization. This stub defines the
//! interface so downstream code can compile.

/// Placeholder for the FDB database handle.
///
/// In M3 this will wrap `foundationdb::Database` and provide
/// `run()` / `create_trx()` methods with retry logic and metrics.
#[derive(Clone)]
pub struct Database;
