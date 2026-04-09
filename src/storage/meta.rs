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

/// The type of value stored under a Redis key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    String,
    Hash,
    Set,
    SortedSet,
    List,
    Stream,
}

/// Per-key metadata.
#[derive(Debug, Clone)]
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
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expires_at_ms > 0 && now_ms > self.expires_at_ms
    }
}
