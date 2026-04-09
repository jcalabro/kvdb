//! ObjectMeta — per-key metadata stored in the `meta/` directory.
//!
//! Every Redis key has a corresponding ObjectMeta entry that tracks:
//! - **Type**: String, Hash, Set, SortedSet, List, Stream
//! - **Chunk count / size**: for transparent value chunking
//! - **TTL**: expiry timestamp in milliseconds (0 = no expiry)
//! - **Cardinality**: element count for collections
//! - **List indices**: head/tail for O(1) push/pop
//!
//! This metadata is read on every command to enforce type safety,
//! check expiration, and locate data.

use bincode::Options;
use foundationdb::Transaction;
use serde::{Deserialize, Serialize};
use tracing::trace_span;

use super::directories::Directories;
use crate::error::StorageError;

/// Maximum serialized size of an ObjectMeta in bytes.
///
/// ObjectMeta is a fixed-size struct (~60 bytes with bincode). We set a
/// generous limit of 1 KB to guard against memory exhaustion if FDB
/// returns corrupted data (e.g., a different value type was written to
/// the meta key due to a bug).
const MAX_META_SIZE: u64 = 1024;

/// Shared bincode configuration.
///
/// Uses fixed-integer encoding (matching bincode 1.x default) with a
/// size limit for deserialization safety. Both `serialize` and
/// `deserialize` must use the same encoding, so we centralize it here.
fn bincode_config() -> impl bincode::Options {
    bincode::options().with_fixint_encoding().with_limit(MAX_META_SIZE)
}

/// The type of value stored under a Redis key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyType {
    String,
    Hash,
    Set,
    SortedSet,
    List,
    Stream,
}

impl KeyType {
    /// Return the Redis type string (as used by the `TYPE` command).
    pub fn as_redis_type_str(&self) -> &'static str {
        match self {
            KeyType::String => "string",
            KeyType::Hash => "hash",
            KeyType::Set => "set",
            KeyType::SortedSet => "zset",
            KeyType::List => "list",
            KeyType::Stream => "stream",
        }
    }
}

/// Per-key metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub key_type: KeyType,
    pub num_chunks: u32,
    pub size_bytes: u64,
    pub expires_at_ms: u64,
    pub cardinality: u64,
    pub last_accessed_ms: u64,
    // List-specific fields
    pub list_head: i64,
    pub list_tail: i64,
    pub list_length: u64,
}

impl ObjectMeta {
    /// Create metadata for a new string key.
    pub fn new_string(num_chunks: u32, size_bytes: u64) -> Self {
        Self {
            key_type: KeyType::String,
            num_chunks,
            size_bytes,
            expires_at_ms: 0,
            cardinality: 0,
            last_accessed_ms: 0,
            list_head: 0,
            list_tail: 0,
            list_length: 0,
        }
    }

    /// Returns `true` if this key has expired relative to `now_ms`.
    ///
    /// Redis semantics: a key expires AT the specified timestamp, meaning
    /// `now_ms >= expires_at_ms` is expired (not strictly greater-than).
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expires_at_ms > 0 && now_ms >= self.expires_at_ms
    }

    /// Serialize to bincode bytes.
    pub fn serialize(&self) -> Result<Vec<u8>, StorageError> {
        bincode_config()
            .serialize(self)
            .map_err(|e| StorageError::Serialization(e.to_string()))
    }

    /// Deserialize from bincode bytes.
    ///
    /// Enforces a size limit to prevent memory exhaustion from corrupted data.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, StorageError> {
        bincode_config()
            .deserialize(bytes)
            .map_err(|e| StorageError::Serialization(e.to_string()))
    }

    /// Read ObjectMeta for `key` from FDB. Returns `None` if the key
    /// does not exist or has expired (lazy expiry check).
    ///
    /// `now_ms` is the current time in milliseconds for the lazy expiry
    /// check. Pass 0 to skip the expiry check (useful in contexts where
    /// you want to read expired metadata, e.g. background cleanup).
    ///
    /// Uses a snapshot read when `snapshot` is true (avoids adding to
    /// the transaction's conflict set).
    pub async fn read(
        tr: &Transaction,
        dirs: &Directories,
        key: &[u8],
        now_ms: u64,
        snapshot: bool,
    ) -> Result<Option<Self>, StorageError> {
        let _span = trace_span!("meta_read").entered();

        let fdb_key = dirs.meta_key(key);
        let maybe_val = tr.get(&fdb_key, snapshot).await.map_err(StorageError::Fdb)?;

        match maybe_val {
            None => Ok(None),
            Some(slice) => {
                let meta = Self::deserialize(&slice)?;
                if now_ms > 0 && meta.is_expired(now_ms) {
                    Ok(None)
                } else {
                    Ok(Some(meta))
                }
            }
        }
    }

    /// Write this ObjectMeta for `key` into the transaction.
    ///
    /// This is a synchronous operation — it only buffers the write
    /// in the transaction; the actual write happens at commit time.
    pub fn write(&self, tr: &Transaction, dirs: &Directories, key: &[u8]) -> Result<(), StorageError> {
        let _span = trace_span!("meta_write").entered();

        let fdb_key = dirs.meta_key(key);
        let value = self.serialize()?;
        tr.set(&fdb_key, &value);
        Ok(())
    }

    /// Delete the ObjectMeta for `key` from the transaction.
    pub fn delete(tr: &Transaction, dirs: &Directories, key: &[u8]) {
        let _span = trace_span!("meta_delete").entered();

        let fdb_key = dirs.meta_key(key);
        tr.clear(&fdb_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_string() {
        let meta = ObjectMeta::new_string(3, 250_000);
        let bytes = meta.serialize().unwrap();
        let decoded = ObjectMeta::deserialize(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn serde_roundtrip_hash() {
        let meta = ObjectMeta {
            key_type: KeyType::Hash,
            num_chunks: 0,
            size_bytes: 0,
            expires_at_ms: 0,
            cardinality: 42,
            last_accessed_ms: 1_000_000,
            list_head: 0,
            list_tail: 0,
            list_length: 0,
        };
        let bytes = meta.serialize().unwrap();
        let decoded = ObjectMeta::deserialize(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn serde_roundtrip_list() {
        let meta = ObjectMeta {
            key_type: KeyType::List,
            num_chunks: 0,
            size_bytes: 0,
            expires_at_ms: 5_000,
            cardinality: 100,
            last_accessed_ms: 0,
            list_head: -50,
            list_tail: 49,
            list_length: 100,
        };
        let bytes = meta.serialize().unwrap();
        let decoded = ObjectMeta::deserialize(&bytes).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn serde_all_key_types() {
        let types = [
            KeyType::String,
            KeyType::Hash,
            KeyType::Set,
            KeyType::SortedSet,
            KeyType::List,
            KeyType::Stream,
        ];
        for kt in types {
            let meta = ObjectMeta {
                key_type: kt,
                num_chunks: 1,
                size_bytes: 100,
                expires_at_ms: 0,
                cardinality: 0,
                last_accessed_ms: 0,
                list_head: 0,
                list_tail: 0,
                list_length: 0,
            };
            let bytes = meta.serialize().unwrap();
            let decoded = ObjectMeta::deserialize(&bytes).unwrap();
            assert_eq!(meta.key_type, decoded.key_type);
        }
    }

    #[test]
    fn expiry_not_set() {
        let meta = ObjectMeta::new_string(1, 10);
        assert!(!meta.is_expired(1_000_000));
    }

    #[test]
    fn expiry_not_yet() {
        let mut meta = ObjectMeta::new_string(1, 10);
        meta.expires_at_ms = 5_000;
        assert!(!meta.is_expired(4_999));
    }

    #[test]
    fn expiry_at_boundary() {
        // Redis semantics: key expires AT the timestamp (>=, not >).
        let mut meta = ObjectMeta::new_string(1, 10);
        meta.expires_at_ms = 5_000;
        assert!(meta.is_expired(5_000), "key should be expired at exactly expires_at_ms");
    }

    #[test]
    fn expiry_past() {
        let mut meta = ObjectMeta::new_string(1, 10);
        meta.expires_at_ms = 5_000;
        assert!(meta.is_expired(5_001));
        assert!(meta.is_expired(1_000_000));
    }

    #[test]
    fn deserialize_garbage_returns_error() {
        let result = ObjectMeta::deserialize(b"not valid bincode data");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("serialization"));
    }

    #[test]
    fn deserialize_empty_returns_error() {
        let result = ObjectMeta::deserialize(b"");
        assert!(result.is_err());
    }

    #[test]
    fn redis_type_strings() {
        assert_eq!(KeyType::String.as_redis_type_str(), "string");
        assert_eq!(KeyType::Hash.as_redis_type_str(), "hash");
        assert_eq!(KeyType::Set.as_redis_type_str(), "set");
        assert_eq!(KeyType::SortedSet.as_redis_type_str(), "zset");
        assert_eq!(KeyType::List.as_redis_type_str(), "list");
        assert_eq!(KeyType::Stream.as_redis_type_str(), "stream");
    }
}
