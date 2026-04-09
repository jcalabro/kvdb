//! Transparent value chunking.
//!
//! Values exceeding 100,000 bytes (FDB's hard limit) are automatically
//! split into chunks stored at sequential sub-keys. The chunk boundary
//! is invisible to the command layer.
//!
//! M3 will implement the real read/write/delete operations against FDB.

/// Maximum size of a single FDB value, in bytes.
/// This is an exact limit (100,000 bytes, not 100 KiB).
pub const MAX_VALUE_SIZE: usize = 100_000;

/// Maximum size of a single chunk. We use the full FDB value limit.
pub const CHUNK_SIZE: usize = MAX_VALUE_SIZE;

/// Compute the number of chunks needed for `data_len` bytes.
pub fn chunk_count(data_len: usize) -> u32 {
    if data_len == 0 {
        return 0;
    }
    data_len.div_ceil(CHUNK_SIZE) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_count_zero() {
        assert_eq!(chunk_count(0), 0);
    }

    #[test]
    fn chunk_count_single() {
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(CHUNK_SIZE), 1);
    }

    #[test]
    fn chunk_count_boundary() {
        assert_eq!(chunk_count(CHUNK_SIZE + 1), 2);
        assert_eq!(chunk_count(CHUNK_SIZE * 5), 5);
        assert_eq!(chunk_count(CHUNK_SIZE * 5 + 1), 6);
    }
}
