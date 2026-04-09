//! FDB directory layout.
//!
//! Each namespace (Redis databases 0-15) gets its own directory tree:
//!
//! ```text
//! kvdb/<ns>/meta/<key>
//! kvdb/<ns>/obj/<key, chunk>
//! kvdb/<ns>/hash/<key, field>
//! kvdb/<ns>/set/<key, member>
//! kvdb/<ns>/zset/<key, score, member>
//! kvdb/<ns>/zset_idx/<key, member>
//! kvdb/<ns>/list/<key, index>
//! kvdb/<ns>/expire/<key>
//! ```

use foundationdb::directory::DirectorySubspace;
use foundationdb::directory::{Directory, DirectoryLayer};
use tracing::info_span;

use crate::error::StorageError;

/// The eight subspace names under each namespace.
const SUBSPACE_NAMES: [&str; 8] = ["meta", "obj", "hash", "set", "zset", "zset_idx", "list", "expire"];

/// Holds FDB subspace handles for a single namespace.
///
/// All eight subspaces are opened in a single transaction when the
/// namespace is first accessed. The `DirectorySubspace` handles are
/// cheap to clone (inner `Arc`).
#[derive(Clone)]
pub struct Directories {
    /// The namespace index (0-15).
    pub namespace: u8,

    /// `kvdb/<ns>/meta` — per-key ObjectMeta
    pub meta: DirectorySubspace,
    /// `kvdb/<ns>/obj` — string value chunks
    pub obj: DirectorySubspace,
    /// `kvdb/<ns>/hash` — hash field values
    pub hash: DirectorySubspace,
    /// `kvdb/<ns>/set` — set membership
    pub set: DirectorySubspace,
    /// `kvdb/<ns>/zset` — sorted set (score, member) entries
    pub zset: DirectorySubspace,
    /// `kvdb/<ns>/zset_idx` — sorted set member -> score index
    pub zset_idx: DirectorySubspace,
    /// `kvdb/<ns>/list` — list elements by index
    pub list: DirectorySubspace,
    /// `kvdb/<ns>/expire` — expiry timestamps
    pub expire: DirectorySubspace,
}

impl Directories {
    /// Open (or create) all 8 directory subspaces for `namespace`
    /// inside a single FDB transaction.
    pub async fn open(db: &super::database::Database, namespace: u8) -> Result<Self, StorageError> {
        let span = info_span!("dir_open", ns = namespace);
        let _enter = span.enter();

        let ns_str = namespace.to_string();

        let trx = db.inner().create_trx().map_err(|e| StorageError::Fdb(e))?;

        let dir_layer = DirectoryLayer::default();

        let mut subspaces: Vec<DirectorySubspace> = Vec::with_capacity(8);

        for name in &SUBSPACE_NAMES {
            let path = vec!["kvdb".to_string(), ns_str.clone(), (*name).to_string()];

            let output = dir_layer
                .create_or_open(&trx, &path, None, None)
                .await
                .map_err(|e| StorageError::Directory(format!("{e:?}")))?;

            match output {
                foundationdb::directory::DirectoryOutput::DirectorySubspace(ds) => {
                    subspaces.push(ds);
                }
                _ => {
                    return Err(StorageError::Directory(
                        "expected DirectorySubspace, got DirectoryPartition".to_string(),
                    ));
                }
            }
        }

        trx.commit().await.map_err(|e| StorageError::Fdb(e.into()))?;

        // SUBSPACE_NAMES order: meta, obj, hash, set, zset, zset_idx, list, expire
        Ok(Self {
            namespace,
            meta: subspaces.remove(0),
            obj: subspaces.remove(0),
            hash: subspaces.remove(0),
            set: subspaces.remove(0),
            zset: subspaces.remove(0),
            zset_idx: subspaces.remove(0),
            list: subspaces.remove(0),
            expire: subspaces.remove(0),
        })
    }

    /// Build the FDB key for a meta entry: `<meta_prefix> + pack(key)`.
    pub fn meta_key(&self, key: &[u8]) -> Vec<u8> {
        self.meta.pack(&(key,))
    }

    /// Build the FDB key for a specific object chunk:
    /// `<obj_prefix> + pack(key, chunk_index)`.
    pub fn obj_chunk_key(&self, key: &[u8], chunk_index: u32) -> Vec<u8> {
        self.obj.pack(&(key, chunk_index))
    }

    /// Build the FDB key for an expire entry: `<expire_prefix> + pack(key)`.
    pub fn expire_key(&self, key: &[u8]) -> Vec<u8> {
        self.expire.pack(&(key,))
    }

    /// Return (begin, end) byte ranges covering all obj chunks for `key`.
    ///
    /// Useful for `clear_range` when deleting a key's data.
    pub fn obj_key_range(&self, key: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let sub = self.obj.subspace(&(key,));
        sub.range()
    }
}
