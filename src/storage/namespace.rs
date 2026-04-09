//! Shared directory cache for multi-namespace support.
//!
//! The `NamespaceCache` holds opened `Directories` handles for each
//! namespace (0-15). Directories are opened lazily on first access
//! (via SELECT) and cached for reuse across all connections.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use super::database::Database;
use super::directories::Directories;
use crate::error::StorageError;

/// Shared cache of opened FDB directory handles, one per namespace.
///
/// Thread-safe via `Arc<RwLock<...>>`. The `RwLock` allows concurrent
/// reads (the common case — most connections use the same namespaces)
/// with exclusive access only when opening a new namespace.
#[derive(Clone)]
pub struct NamespaceCache {
    db: Database,
    root_prefix: String,
    cache: Arc<RwLock<HashMap<u8, Directories>>>,
}

impl NamespaceCache {
    /// Create a new cache, seeding it with the given initial namespace.
    pub fn new(db: Database, root_prefix: String, initial_dirs: Directories) -> Self {
        let mut map = HashMap::new();
        map.insert(initial_dirs.namespace, initial_dirs);
        Self {
            db,
            root_prefix,
            cache: Arc::new(RwLock::new(map)),
        }
    }

    /// Get the `Directories` for a namespace, opening them if not cached.
    pub async fn get(&self, namespace: u8) -> Result<Directories, StorageError> {
        // Fast path: read lock.
        {
            let cache = self.cache.read().await;
            if let Some(dirs) = cache.get(&namespace) {
                return Ok(dirs.clone());
            }
        }

        // Slow path: open directories and insert under write lock.
        let mut cache = self.cache.write().await;
        // Double-check after acquiring write lock (another task may have opened it).
        if let Some(dirs) = cache.get(&namespace) {
            return Ok(dirs.clone());
        }

        let dirs = Directories::open(&self.db, namespace, &self.root_prefix).await?;
        cache.insert(namespace, dirs.clone());
        Ok(dirs)
    }

    /// Return all currently cached namespaces (for FLUSHALL and expiry worker).
    pub async fn cached_namespaces(&self) -> Vec<Directories> {
        let cache = self.cache.read().await;
        cache.values().cloned().collect()
    }

    /// Return a reference to the underlying database handle.
    pub fn db(&self) -> &Database {
        &self.db
    }
}
