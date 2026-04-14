//! High-level storage helpers for command implementations.
//!
//! These functions encapsulate the common patterns that every string
//! command (and later, other data-type commands) needs:
//!
//! - Read a string value (with meta check, type enforcement, lazy expiry)
//! - Write a string value (with chunking, meta upsert, optional TTL)
//! - Delete any object (clear type-specific data + meta + expire)
//!
//! They work at the `Transaction` level — callers wrap them inside
//! `run_transact()` to get retry logic and metrics.

use std::time::{SystemTime, UNIX_EPOCH};

use foundationdb::{FdbBindingError, Transaction};

use super::chunking::{self, delete_chunks, read_chunks, write_chunks};
use super::directories::Directories;
use super::meta::{KeyType, ObjectMeta};
use crate::error::{CommandError, StorageError};
use crate::protocol::types::RespValue;

/// Read a string value for `key`.
///
/// Returns `Ok(None)` if the key does not exist or has expired.
/// Returns `Err(CommandError::WrongType)` if the key exists but is
/// not a string.
pub async fn get_string(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    now_ms: u64,
) -> Result<Option<Vec<u8>>, CommandError> {
    let meta = ObjectMeta::read(tr, dirs, key, now_ms, false)
        .await
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    let meta = match meta {
        Some(m) => m,
        None => return Ok(None),
    };

    if meta.key_type != KeyType::String {
        return Err(CommandError::WrongType);
    }

    let data = read_chunks(tr, &dirs.obj, key, meta.num_chunks, meta.size_bytes, false)
        .await
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    Ok(Some(data))
}

/// Write a string value for `key`.
///
/// If `old_meta` is `Some`, the existing data chunks are cleared first
/// (handles overwrite of a key that previously had more chunks).
/// If `expires_at_ms` is 0, no expiry is set; otherwise the expire
/// entry is written.
///
/// This is a synchronous operation — it buffers writes in the
/// transaction. The actual commit happens in the caller.
pub fn write_string(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    data: &[u8],
    expires_at_ms: u64,
    old_meta: Option<&ObjectMeta>,
) -> Result<(), CommandError> {
    // If overwriting, clear old type-specific data first.
    // This handles type changes (e.g. SET on a key that was previously a hash)
    // by delegating to delete_data_for_meta which knows how to clean up each type.
    if let Some(old) = old_meta {
        delete_data_for_meta(tr, dirs, key, old);
    }

    let num_chunks = write_chunks(tr, &dirs.obj, key, data);
    let mut meta = ObjectMeta::new_string(num_chunks, data.len() as u64);
    meta.expires_at_ms = expires_at_ms;

    meta.write(tr, dirs, key)
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    // Write or clear the expire entry.
    if expires_at_ms > 0 {
        let expire_key = dirs.expire_key(key);
        tr.set(&expire_key, &expires_at_ms.to_be_bytes());
    } else if old_meta.is_some_and(|m| m.expires_at_ms > 0) {
        // Old key had an expiry, new one doesn't — clear it.
        let expire_key = dirs.expire_key(key);
        tr.clear(&expire_key);
    }

    Ok(())
}

/// Delete an object of any type for `key`.
///
/// Reads the meta (without expiry check) to determine the type, then
/// clears all type-specific data, the meta entry, and the expire entry.
/// This ensures orphaned expired data is cleaned up. Returns `true` if
/// the key was live (not expired), `false` otherwise.
pub async fn delete_object(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    now_ms: u64,
) -> Result<bool, CommandError> {
    // Read WITHOUT expiry check so we can clean up orphaned data.
    let meta = ObjectMeta::read(tr, dirs, key, 0, false)
        .await
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    let meta = match meta {
        Some(m) => m,
        None => return Ok(false), // truly doesn't exist
    };

    // Determine if the key was "live" (for the return value).
    let was_live = now_ms == 0 || !meta.is_expired(now_ms);

    // Always clean up data, meta, and expire entry.
    delete_data_for_meta(tr, dirs, key, &meta);

    ObjectMeta::delete(tr, dirs, key).map_err(|e| CommandError::Generic(e.to_string()))?;

    if meta.expires_at_ms > 0 {
        let expire_key = dirs.expire_key(key);
        tr.clear(&expire_key);
    }

    Ok(was_live)
}

/// Clear all type-specific data for a key given its metadata.
///
/// This is an internal helper used by `delete_object` and (later)
/// type-change overwrites (e.g. SET on a key that was previously a hash).
pub(crate) fn delete_data_for_meta(tr: &Transaction, dirs: &Directories, key: &[u8], meta: &ObjectMeta) {
    match meta.key_type {
        KeyType::String => {
            delete_chunks(tr, &dirs.obj, key);
        }
        KeyType::Hash => {
            let sub = dirs.hash.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::Set => {
            let sub = dirs.set.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::SortedSet => {
            let sub = dirs.zset.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);

            let idx_sub = dirs.zset_idx.subspace(&(key,));
            let (begin, end) = idx_sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::List => {
            let sub = dirs.list.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::Stream => {
            // Streams are not yet implemented — nothing to clear.
        }
    }
}

/// Delete all data, metadata, and expire entry for a key.
///
/// This is the consolidated cleanup helper for type-change overwrites
/// and expired key reclamation. It clears type-specific data, the
/// ObjectMeta entry, and any expire entry in a single sweep.
pub fn delete_all_data_and_meta(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    meta: &ObjectMeta,
) -> Result<(), CommandError> {
    delete_data_for_meta(tr, dirs, key, meta);
    ObjectMeta::delete(tr, dirs, key).map_err(|e| CommandError::Generic(e.to_string()))?;
    if meta.expires_at_ms > 0 {
        let expire_key = dirs.expire_key(key);
        tr.clear(&expire_key);
    }
    Ok(())
}

/// Get the current time in milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as u64
}

/// Convert a `CommandError` into an `FdbBindingError` for use inside
/// `run_transact` closures.
pub fn cmd_err(e: CommandError) -> FdbBindingError {
    FdbBindingError::CustomError(Box::new(e))
}

/// Convert a `StorageError` into an `FdbBindingError` for use inside
/// `run_transact` closures.
pub fn storage_err(e: StorageError) -> FdbBindingError {
    FdbBindingError::CustomError(Box::new(e))
}

/// Convert a `StorageError` into a RESP error response.
///
/// `CommandError` variants (e.g. `WrongType`) already include the correct
/// Redis error prefix (e.g. `WRONGTYPE ...`), so they are passed through
/// directly. All other storage errors get wrapped with `ERR`.
pub fn storage_err_to_resp(e: StorageError) -> RespValue {
    match e {
        StorageError::Command(cmd_err) => RespValue::err(cmd_err.to_string()),
        other => RespValue::err(format!("ERR {other}")),
    }
}

/// Compute the number of chunks needed for a given data length.
///
/// Re-exported from `chunking` for convenience in command handlers.
pub fn chunk_count(data_len: usize) -> u32 {
    chunking::chunk_count(data_len)
}
