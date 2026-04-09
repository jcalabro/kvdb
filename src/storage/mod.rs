//! FoundationDB storage layer.
//!
//! Manages the FDB connection, directory layout, ObjectMeta, value
//! chunking, and transaction wrappers.

pub mod chunking;
pub mod database;
pub mod directories;
pub mod helpers;
pub mod meta;
pub mod namespace;
pub mod transaction;

pub use database::Database;
pub use directories::{Directories, MAX_KEY_SIZE, validate_key_size};
pub use meta::{KeyType, ObjectMeta};
pub use namespace::NamespaceCache;
pub use transaction::{IsolationMode, run_transact};
