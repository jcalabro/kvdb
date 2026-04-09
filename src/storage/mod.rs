//! FoundationDB storage layer.
//!
//! Manages the FDB connection, directory layout, ObjectMeta, value
//! chunking, and transaction wrappers. M3 will flesh this out with
//! real FDB integration.

pub mod chunking;
pub mod database;
pub mod directories;
pub mod meta;
pub mod transaction;
