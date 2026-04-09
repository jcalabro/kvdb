//! TTL and expiration management.
//!
//! Implements a hybrid lazy + active expiration strategy:
//!
//! - **Lazy**: every read checks `ObjectMeta.expires_at_ms` and treats
//!   expired keys as non-existent.
//! - **Active**: a background worker scans the `expire/` directory every
//!   250ms and cleans up expired keys in batches.
//!
//! M5 will implement the full expiry worker. This module defines the
//! interface.

/// Configuration for the background expiry worker.
#[derive(Debug, Clone)]
pub struct ExpiryConfig {
    /// How often the worker scans for expired keys.
    pub scan_interval_ms: u64,
    /// Maximum keys to process per scan batch.
    pub batch_size: usize,
}

impl Default for ExpiryConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 250,
            batch_size: 1000,
        }
    }
}
