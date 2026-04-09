//! Transparent value chunking for the `obj/` subspace.
//!
//! Values exceeding 100,000 bytes (FDB's hard limit) are automatically
//! split into chunks stored at sequential sub-keys. The chunk boundary
//! is invisible to the command layer.
//!
//! Currently hardcoded to the `obj/` subspace via `Directories`. When
//! hash field values or list elements need chunking (M6+), this module
//! should be generalized to accept a `DirectorySubspace` parameter.

use foundationdb::Transaction;
use tracing::debug_span;

use super::directories::Directories;
use crate::error::StorageError;

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

/// Build the FDB key for a specific chunk of `key`.
fn chunk_key(dirs: &Directories, key: &[u8], chunk_index: u32) -> Vec<u8> {
    dirs.obj_chunk_key(key, chunk_index)
}

/// Write `data` as chunks into the transaction.
///
/// Splits `data` at `CHUNK_SIZE` boundaries and writes each chunk at
/// `obj/<key, chunk_index>`. This is a synchronous operation — it
/// buffers the writes in the transaction.
///
/// Returns the number of chunks written.
pub fn write_chunks(tr: &Transaction, dirs: &Directories, key: &[u8], data: &[u8]) -> u32 {
    let _span = debug_span!("write_chunks", key_len = key.len(), data_len = data.len()).entered();

    if data.is_empty() {
        return 0;
    }

    let num_chunks = chunk_count(data.len());
    for i in 0..num_chunks {
        let start = (i as usize) * CHUNK_SIZE;
        let end = std::cmp::min(start + CHUNK_SIZE, data.len());
        let fdb_key = chunk_key(dirs, key, i);
        tr.set(&fdb_key, &data[start..end]);
    }

    num_chunks
}

/// Read all chunks for `key` and reassemble into a single `Vec<u8>`.
///
/// Issues parallel gets for all `num_chunks` chunks and concatenates
/// the results. Returns `DataCorruption` if any chunk is missing.
pub async fn read_chunks(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    num_chunks: u32,
    snapshot: bool,
) -> Result<Vec<u8>, StorageError> {
    let _span = debug_span!("read_chunks", key_len = key.len(), num_chunks).entered();

    if num_chunks == 0 {
        return Ok(Vec::new());
    }

    // Issue all gets in parallel by collecting the futures first.
    let futures: Vec<_> = (0..num_chunks)
        .map(|i| {
            let fdb_key = chunk_key(dirs, key, i);
            tr.get(&fdb_key, snapshot)
        })
        .collect();

    // Pre-allocate for the common case (last chunk may be smaller,
    // but the over-allocation is bounded by CHUNK_SIZE).
    let mut result = Vec::with_capacity((num_chunks as usize) * CHUNK_SIZE);

    for (i, fut) in futures.into_iter().enumerate() {
        let maybe_val = fut.await.map_err(StorageError::Fdb)?;
        match maybe_val {
            Some(slice) => result.extend_from_slice(&slice),
            None => {
                return Err(StorageError::DataCorruption(format!(
                    "missing chunk {i} of {num_chunks} for key"
                )));
            }
        }
    }

    Ok(result)
}

/// Delete all chunks for `key` using a range clear.
///
/// This is a synchronous operation — it buffers the clear in the
/// transaction.
pub fn delete_chunks(tr: &Transaction, dirs: &Directories, key: &[u8]) {
    let _span = debug_span!("delete_chunks", key_len = key.len()).entered();

    let (begin, end) = dirs.obj_key_range(key);
    tr.clear_range(&begin, &end);
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
