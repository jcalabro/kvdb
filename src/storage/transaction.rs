//! FDB transaction wrapper with metrics and retry logic.
//!
//! Thin wrapper around `db.run()` that instruments transactions with
//! duration, retry count, and conflict metrics. M3 will implement
//! the real FDB integration.

/// Transaction isolation mode.
#[derive(Debug, Clone, Copy)]
pub enum IsolationMode {
    /// Standard read-write transaction with full conflict detection.
    ReadWrite,
    /// Snapshot reads that don't add to the conflict set.
    /// Use for non-critical reads or large scans.
    Snapshot,
}
