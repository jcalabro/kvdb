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
//!
//! M3 will implement real directory/subspace creation.

/// Holds FDB subspace handles for a single namespace.
pub struct Directories {
    /// The namespace index (0-15).
    pub namespace: u8,
}
