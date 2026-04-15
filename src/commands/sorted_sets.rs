//! Sorted set command handlers.
//!
//! Sorted sets use a dual-index FDB layout:
//!
//! - **Score index:** `zset/<key, score: f64, member: bytes> → b""`
//!   Entries ordered by (score, member) for range queries.
//!
//! - **Reverse index:** `zset_idx/<key, member: bytes> → score_bytes`
//!   Point lookups for ZSCORE, member existence, and ZADD updates.
//!
//! `ObjectMeta` tracks cardinality (member count). Both indexes and
//! cardinality are kept in sync within each FDB transaction.
//! Empty sorted sets are deleted per Redis semantics.
