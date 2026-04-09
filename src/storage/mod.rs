//! FoundationDB storage layer.
//!
//! Manages the FDB connection, directory layout, ObjectMeta, value
//! chunking, and transaction wrappers.

pub mod chunking;
pub mod database;
pub mod directories;
pub mod meta;
pub mod transaction;

pub use database::Database;
pub use directories::Directories;
pub use meta::{KeyType, ObjectMeta};
pub use transaction::{run_transact, IsolationMode};
