//! List command handlers: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX,
//! LRANGE, LSET, LTRIM, LREM, LPUSHX, RPUSHX, LINSERT, LPOS, LMOVE,
//! LMPOP.
//!
//! # Design: index-based storage
//!
//! Lists are stored as individual FDB key-value pairs keyed by an
//! integer index:
//!
//! ```text
//! list/<redis_key, i64_index>  ->  element_value
//! ```
//!
//! `ObjectMeta` tracks `list_head` (inclusive lowest index), `list_tail`
//! (inclusive highest index), and `list_length`. `list_head` and
//! `list_tail` are both signed — head goes negative as LPUSH prepends,
//! tail increases as RPUSH appends. This gives O(1) push/pop/LINDEX
//! and O(k) LRANGE for k elements.
//!
//! ## Conventions
//!
//! - Fresh list, first element: `head = 0, tail = 0, length = 1`.
//! - LPUSH: first element on empty list goes at index 0 (sets head=tail=0),
//!   subsequent elements decrement head.
//! - RPUSH: first element on empty list goes at index 0 (sets head=tail=0),
//!   subsequent elements increment tail.
//! - Empty list (length == 0): the key is deleted entirely (Redis semantics).
//! - Logical index `i >= 0` maps to FDB index `head + i`.
//! - Logical index `i < 0` maps to FDB index `head + length + i`.
//! - Invariant: `length == tail - head + 1` when length > 0.
//!
//! ## LINSERT/LREM strategy
//!
//! Operations that modify the middle of the list use a compact-rewrite:
//! read all elements via range read, splice in memory, clear the old
//! range, rewrite at contiguous indices [0, N-1]. This is O(n) — same
//! as Redis. For very large lists this may hit FDB's 10MB transaction
//! limit; callers get a clear error in that case.

use bytes::Bytes;
use foundationdb::RangeOption;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::chunking::MAX_VALUE_SIZE;
use crate::storage::directories::Directories;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::{helpers, run_transact};

use super::util::parse_i64_arg;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Result of reading list meta for a write-path command.
///
/// When the key is live and is a list: `Some(meta)` — use its head/tail/length.
/// When the key doesn't exist OR was cleaned up (expired or wrong type and
/// we cleaned it up): `None` — treat the list as empty.
///
/// Wrong-type and expired keys are cleaned up inside the transaction before
/// returning. A live wrong-type key causes a `WrongType` error.
async fn read_list_meta_for_write(
    tr: &foundationdb::Transaction,
    dirs: &Directories,
    key: &[u8],
) -> Result<Option<ObjectMeta>, foundationdb::FdbBindingError> {
    let now = helpers::now_ms();

    // Read raw meta (no expiry filter) so we can clean up stale data.
    let raw_meta = ObjectMeta::read(tr, dirs, key, 0, false)
        .await
        .map_err(helpers::storage_err)?;

    let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

    // Live but wrong type — reject.
    if let Some(m) = live_meta
        && m.key_type != KeyType::List
    {
        return Err(helpers::cmd_err(CommandError::WrongType));
    }

    // Stale meta (expired or wrong type) — clean it up before proceeding.
    if let Some(old_meta) = &raw_meta
        && (old_meta.is_expired(now) || old_meta.key_type != KeyType::List)
    {
        helpers::delete_all_data_and_meta(tr, dirs, key, old_meta).map_err(helpers::cmd_err)?;
    }

    Ok(live_meta.cloned())
}

/// Read live list meta for a read-only command. Returns `None` if the
/// key doesn't exist or has expired. Returns `Err(WrongType)` if the
/// key is live but not a list.
async fn read_list_meta_for_read(
    tr: &foundationdb::Transaction,
    dirs: &Directories,
    key: &[u8],
) -> Result<Option<ObjectMeta>, foundationdb::FdbBindingError> {
    let now = helpers::now_ms();

    let meta = ObjectMeta::read(tr, dirs, key, now, false)
        .await
        .map_err(helpers::storage_err)?;

    match meta {
        None => Ok(None),
        Some(m) => {
            if m.key_type != KeyType::List {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }
            Ok(Some(m))
        }
    }
}

/// Resolve a Redis logical index (possibly negative) to an FDB index.
///
/// Returns `None` if the index is out of bounds (including the empty-list
/// case — callers should treat `None` as "no such element").
fn resolve_index(head: i64, length: u64, index: i64) -> Option<i64> {
    if length == 0 {
        return None;
    }
    let len_i64 = length as i64;
    let logical = if index < 0 {
        // Negative: -1 = last, -2 = second-to-last, etc.
        let adjusted = len_i64 + index;
        if adjusted < 0 {
            return None;
        }
        adjusted
    } else {
        if index >= len_i64 {
            return None;
        }
        index
    };
    Some(head + logical)
}

/// Normalize a `start..stop` pair into logical offsets `(start, stop)`
/// with Redis LRANGE/LTRIM clamping semantics.
///
/// Returns `None` if the resulting range is empty (e.g. `start > stop`
/// after clamping, or length is 0).
fn normalize_range(length: u64, start: i64, stop: i64) -> Option<(u64, u64)> {
    if length == 0 {
        return None;
    }
    let len_i64 = length as i64;

    // Convert negative indices by adding length.
    let mut s = if start < 0 { start + len_i64 } else { start };
    let mut e = if stop < 0 { stop + len_i64 } else { stop };

    // Clamp: start >= 0, stop <= length - 1.
    if s < 0 {
        s = 0;
    }
    if e >= len_i64 {
        e = len_i64 - 1;
    }

    if s > e || s >= len_i64 {
        return None;
    }
    Some((s as u64, e as u64))
}

/// Read a contiguous range of list elements from FDB.
///
/// `fdb_start` and `fdb_end_inclusive` are the FDB (not logical) indices.
/// Paginates through the range so large ranges are read completely.
/// Elements are returned in FDB key order (ascending by index).
async fn read_element_range(
    tr: &foundationdb::Transaction,
    dirs: &Directories,
    key: &[u8],
    fdb_start: i64,
    fdb_end_inclusive: i64,
) -> Result<Vec<Vec<u8>>, foundationdb::FdbBindingError> {
    if fdb_start > fdb_end_inclusive {
        return Ok(Vec::new());
    }
    let begin = dirs.list.pack(&(key, fdb_start));
    // FDB get_range end is exclusive — pack end + 1.
    let end = dirs.list.pack(&(key, fdb_end_inclusive + 1));
    let mut maybe_range: Option<RangeOption<'_>> = Some(RangeOption::from((begin.as_slice(), end.as_slice())));
    let mut elements = Vec::new();
    let mut iteration = 1;

    while let Some(range_opt) = maybe_range.take() {
        let kvs = tr
            .get_range(&range_opt, iteration, false)
            .await
            .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

        for kv in kvs.iter() {
            elements.push(kv.value().to_vec());
        }

        maybe_range = range_opt.next_range(&kvs);
        iteration += 1;
    }

    Ok(elements)
}

/// Write updated list meta after a push/pop, preserving expiry.
/// If `new_length == 0`, deletes the key entirely instead.
fn write_or_delete_list_meta(
    tr: &foundationdb::Transaction,
    dirs: &Directories,
    key: &[u8],
    old_meta: Option<&ObjectMeta>,
    new_head: i64,
    new_tail: i64,
    new_length: u64,
) -> Result<(), foundationdb::FdbBindingError> {
    if new_length == 0 {
        // Empty list — remove the key entirely (Redis semantics).
        if let Some(m) = old_meta {
            helpers::delete_all_data_and_meta(tr, dirs, key, m).map_err(helpers::cmd_err)?;
        } else {
            // No old meta, nothing to delete — just clear the meta entry defensively.
            ObjectMeta::delete(tr, dirs, key).map_err(helpers::storage_err)?;
        }
        return Ok(());
    }

    let mut meta = ObjectMeta::new_list(new_head, new_tail, new_length);
    meta.expires_at_ms = old_meta.map_or(0, |m| m.expires_at_ms);
    meta.write(tr, dirs, key).map_err(helpers::storage_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// LPUSH key element [element ...]
// ---------------------------------------------------------------------------

/// LPUSH key element [element ...] — Prepend element(s) to a list.
///
/// Elements are pushed one by one, so `LPUSH k a b c` results in
/// `[c, b, a]` (each arg becomes the new head in turn). Creates the
/// list if the key doesn't exist. Returns the new list length.
pub async fn handle_lpush(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "LPUSH".into() }.to_string());
    }
    handle_push(
        args, state, "LPUSH", /* left = */ true, /* only_if_exists = */ false,
    )
    .await
}

// ---------------------------------------------------------------------------
// RPUSH key element [element ...]
// ---------------------------------------------------------------------------

/// RPUSH key element [element ...] — Append element(s) to a list.
pub async fn handle_rpush(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "RPUSH".into() }.to_string());
    }
    handle_push(
        args, state, "RPUSH", /* left = */ false, /* only_if_exists = */ false,
    )
    .await
}

// ---------------------------------------------------------------------------
// LPUSHX key element [element ...]
// ---------------------------------------------------------------------------

/// LPUSHX key element [element ...] — Prepend only if list exists.
///
/// Returns 0 without creating the list if the key doesn't exist.
pub async fn handle_lpushx(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "LPUSHX".into() }.to_string());
    }
    handle_push(
        args, state, "LPUSHX", /* left = */ true, /* only_if_exists = */ true,
    )
    .await
}

// ---------------------------------------------------------------------------
// RPUSHX key element [element ...]
// ---------------------------------------------------------------------------

/// RPUSHX key element [element ...] — Append only if list exists.
pub async fn handle_rpushx(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "RPUSHX".into() }.to_string());
    }
    handle_push(
        args, state, "RPUSHX", /* left = */ false, /* only_if_exists = */ true,
    )
    .await
}

/// Shared push implementation for LPUSH/RPUSH/LPUSHX/RPUSHX.
///
/// `left = true` prepends (LPUSH/LPUSHX); `false` appends (RPUSH/RPUSHX).
/// `only_if_exists = true` returns 0 if the key doesn't exist (LPUSHX/RPUSHX
/// semantics); `false` creates the key on demand.
async fn handle_push(
    args: &[Bytes],
    state: &ConnectionState,
    op: &'static str,
    left: bool,
    only_if_exists: bool,
) -> RespValue {
    match run_transact(&state.db, op, |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let elements = &args[1..];

            // Validate element sizes before any writes. FDB has a hard
            // 100,000-byte value limit; list elements are stored directly
            // (not chunked) so we must reject oversized elements upfront.
            for element in elements {
                if element.len() > MAX_VALUE_SIZE {
                    return Err(helpers::cmd_err(CommandError::Generic(format!(
                        "ERR list element exceeds maximum size ({} bytes > {} byte limit)",
                        element.len(),
                        MAX_VALUE_SIZE,
                    ))));
                }
            }

            let meta = read_list_meta_for_write(&tr, &dirs, key).await?;

            // LPUSHX/RPUSHX: bail out without side effects if the key doesn't exist.
            if only_if_exists && meta.is_none() {
                return Ok(0i64);
            }

            let (mut head, mut tail, mut length) = match &meta {
                Some(m) => (m.list_head, m.list_tail, m.list_length),
                None => {
                    // Sentinel for "list doesn't exist yet". The first element
                    // below will initialize head=tail=0, length=1.
                    (0, 0, 0)
                }
            };

            for element in elements {
                if length == 0 {
                    // First element — seed head and tail at 0.
                    head = 0;
                    tail = 0;
                    length = 1;
                    tr.set(&dirs.list.pack(&(key.as_ref(), 0i64)), element.as_ref());
                } else if left {
                    head -= 1;
                    length += 1;
                    tr.set(&dirs.list.pack(&(key.as_ref(), head)), element.as_ref());
                } else {
                    tail += 1;
                    length += 1;
                    tr.set(&dirs.list.pack(&(key.as_ref(), tail)), element.as_ref());
                }
            }

            write_or_delete_list_meta(&tr, &dirs, key, meta.as_ref(), head, tail, length)?;
            Ok(length as i64)
        }
    })
    .await
    {
        Ok(len) => RespValue::Integer(len),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LPOP key [count]
// ---------------------------------------------------------------------------

/// LPOP key [count] — Remove and return element(s) from the head.
///
/// Without `count`: returns the single removed bulk string, or Nil if
/// the list is empty/missing.
///
/// With `count`: returns an array of up to `count` bulk strings. Returns
/// Nil if the key doesn't exist. Returns an empty array if count is 0.
/// Returns WRONGTYPE if the key exists but isn't a list.
pub async fn handle_lpop(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_pop(args, state, "LPOP", /* from_head = */ true).await
}

// ---------------------------------------------------------------------------
// RPOP key [count]
// ---------------------------------------------------------------------------

/// RPOP key [count] — Remove and return element(s) from the tail.
pub async fn handle_rpop(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_pop(args, state, "RPOP", /* from_head = */ false).await
}

async fn handle_pop(args: &[Bytes], state: &ConnectionState, op: &'static str, from_head: bool) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::err(CommandError::WrongArity { name: op.into() }.to_string());
    }

    // The count argument changes the return type (bulk string vs array),
    // so we track whether it was provided independently of its value.
    let (count, has_count) = if args.len() == 2 {
        match parse_i64_arg(&args[1]) {
            Ok(c) if c < 0 => return RespValue::err("ERR value is out of range, must be positive"),
            Ok(c) => (c as u64, true),
            Err(resp) => return resp,
        }
    } else {
        (1, false)
    };

    match run_transact(&state.db, op, |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            // Use write-path reader: pop mutates the list, and we need to
            // clean up orphaned data from expired/wrong-type keys.
            let meta = read_list_meta_for_write(&tr, &dirs, key).await?;

            let meta = match meta {
                None => return Ok(None), // missing key: caller returns Nil or empty array
                Some(m) => m,
            };

            // Cap pop count at the list's length.
            let to_pop = count.min(meta.list_length);
            let mut popped: Vec<Vec<u8>> = Vec::with_capacity(to_pop as usize);

            let mut head = meta.list_head;
            let mut tail = meta.list_tail;
            let mut length = meta.list_length;

            // Fire reads, then clears. Reads first so we can return the
            // values; clears are buffered inside the transaction.
            for _ in 0..to_pop {
                let fdb_idx = if from_head { head } else { tail };
                let fdb_key = dirs.list.pack(&(key.as_ref(), fdb_idx));
                let val = tr
                    .get(&fdb_key, false)
                    .await
                    .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                let bytes = match val {
                    Some(v) => v.to_vec(),
                    None => {
                        // Shouldn't happen if meta is consistent, but guard against it.
                        return Err(helpers::cmd_err(CommandError::Generic(format!(
                            "list index {fdb_idx} missing but meta claims length {length}"
                        ))));
                    }
                };
                tr.clear(&fdb_key);
                popped.push(bytes);

                if from_head {
                    head += 1;
                } else {
                    tail -= 1;
                }
                length -= 1;
            }

            write_or_delete_list_meta(&tr, &dirs, key, Some(&meta), head, tail, length)?;
            Ok(Some(popped))
        }
    })
    .await
    {
        Ok(None) => {
            // Key didn't exist.
            if has_count {
                // Redis: LPOP missing-key count returns Nil array.
                RespValue::Array(None)
            } else {
                RespValue::BulkString(None)
            }
        }
        Ok(Some(popped)) => {
            if !has_count {
                // No count: return a single bulk string (or Nil if the list was empty,
                // though meta.is_some() should prevent empty results here).
                match popped.into_iter().next() {
                    Some(v) => RespValue::BulkString(Some(Bytes::from(v))),
                    None => RespValue::BulkString(None),
                }
            } else {
                RespValue::Array(Some(
                    popped
                        .into_iter()
                        .map(|v| RespValue::BulkString(Some(Bytes::from(v))))
                        .collect(),
                ))
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LLEN key
// ---------------------------------------------------------------------------

/// LLEN key — Return the length of the list, or 0 if the key doesn't exist.
pub async fn handle_llen(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "LLEN".into() }.to_string());
    }

    match run_transact(&state.db, "LLEN", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let meta = read_list_meta_for_read(&tr, &dirs, key).await?;
            Ok(meta.map_or(0i64, |m| m.list_length as i64))
        }
    })
    .await
    {
        Ok(len) => RespValue::Integer(len),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LINDEX key index
// ---------------------------------------------------------------------------

/// LINDEX key index — Return the element at `index`, or Nil if out of range.
///
/// Supports negative indices (-1 = last, -2 = second-to-last, etc.).
pub async fn handle_lindex(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "LINDEX".into() }.to_string());
    }

    let index = match parse_i64_arg(&args[1]) {
        Ok(i) => i,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, "LINDEX", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let meta = match read_list_meta_for_read(&tr, &dirs, key).await? {
                None => return Ok(None),
                Some(m) => m,
            };

            let fdb_idx = match resolve_index(meta.list_head, meta.list_length, index) {
                None => return Ok(None),
                Some(idx) => idx,
            };

            let fdb_key = dirs.list.pack(&(key.as_ref(), fdb_idx));
            let val = tr
                .get(&fdb_key, false)
                .await
                .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;
            Ok(val.map(|v| v.to_vec()))
        }
    })
    .await
    {
        Ok(None) => RespValue::BulkString(None),
        Ok(Some(bytes)) => RespValue::BulkString(Some(Bytes::from(bytes))),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LRANGE key start stop
// ---------------------------------------------------------------------------

/// LRANGE key start stop — Return the specified range of elements.
///
/// Start/stop are logical indices (0-based, negative = from end).
/// Redis clamping: oversized bounds are clamped to the list range; if
/// the resulting range is empty, an empty array is returned.
pub async fn handle_lrange(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "LRANGE".into() }.to_string());
    }

    let start = match parse_i64_arg(&args[1]) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let stop = match parse_i64_arg(&args[2]) {
        Ok(i) => i,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, "LRANGE", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let meta = match read_list_meta_for_read(&tr, &dirs, key).await? {
                None => return Ok(Vec::new()),
                Some(m) => m,
            };

            let (s, e) = match normalize_range(meta.list_length, start, stop) {
                None => return Ok(Vec::new()),
                Some(r) => r,
            };

            let fdb_start = meta.list_head + s as i64;
            let fdb_end_inclusive = meta.list_head + e as i64;
            read_element_range(&tr, &dirs, key, fdb_start, fdb_end_inclusive).await
        }
    })
    .await
    {
        Ok(elements) => RespValue::Array(Some(
            elements
                .into_iter()
                .map(|v| RespValue::BulkString(Some(Bytes::from(v))))
                .collect(),
        )),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LSET key index value
// ---------------------------------------------------------------------------

/// LSET key index value — Set the element at `index` to `value`.
///
/// Returns OK on success. Returns "no such key" if the key doesn't exist
/// (Redis quirk: LSET distinguishes missing key from out-of-range index).
/// Returns "index out of range" if the index is out of bounds.
pub async fn handle_lset(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "LSET".into() }.to_string());
    }

    let index = match parse_i64_arg(&args[1]) {
        Ok(i) => i,
        Err(resp) => return resp,
    };

    // LSET outcomes beyond success need distinct error messages, so we
    // model them as a small enum inside the transaction closure.
    enum LsetResult {
        Ok,
        NoSuchKey,
        OutOfRange,
    }

    match run_transact(&state.db, "LSET", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let value = &args[2];

            if value.len() > MAX_VALUE_SIZE {
                return Err(helpers::cmd_err(CommandError::Generic(format!(
                    "ERR list element exceeds maximum size ({} bytes > {} byte limit)",
                    value.len(),
                    MAX_VALUE_SIZE,
                ))));
            }

            let meta = match read_list_meta_for_read(&tr, &dirs, key).await? {
                None => return Ok(LsetResult::NoSuchKey),
                Some(m) => m,
            };

            let fdb_idx = match resolve_index(meta.list_head, meta.list_length, index) {
                None => return Ok(LsetResult::OutOfRange),
                Some(idx) => idx,
            };

            tr.set(&dirs.list.pack(&(key.as_ref(), fdb_idx)), value.as_ref());
            Ok(LsetResult::Ok)
        }
    })
    .await
    {
        Ok(LsetResult::Ok) => RespValue::ok(),
        Ok(LsetResult::NoSuchKey) => RespValue::err("ERR no such key"),
        Ok(LsetResult::OutOfRange) => RespValue::err("ERR index out of range"),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LTRIM key start stop
// ---------------------------------------------------------------------------

/// LTRIM key start stop — Trim the list to the specified range.
///
/// Elements outside [start, stop] (after clamping) are removed. If the
/// range is empty, the list is deleted entirely. Always returns OK.
pub async fn handle_ltrim(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "LTRIM".into() }.to_string());
    }

    let start = match parse_i64_arg(&args[1]) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    let stop = match parse_i64_arg(&args[2]) {
        Ok(i) => i,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, "LTRIM", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let meta = match read_list_meta_for_read(&tr, &dirs, key).await? {
                None => return Ok(()),
                Some(m) => m,
            };

            let (s, e) = match normalize_range(meta.list_length, start, stop) {
                None => {
                    // Empty result range -> delete the list entirely.
                    helpers::delete_all_data_and_meta(&tr, &dirs, key, &meta).map_err(helpers::cmd_err)?;
                    return Ok(());
                }
                Some(r) => r,
            };

            // If the trim is a no-op (keeps everything), we can skip the work.
            if s == 0 && e == meta.list_length - 1 {
                return Ok(());
            }

            let new_fdb_head = meta.list_head + s as i64;
            let new_fdb_tail = meta.list_head + e as i64;

            // Clear elements before the new head (head .. new_fdb_head) — exclusive upper.
            if meta.list_head < new_fdb_head {
                let begin = dirs.list.pack(&(key.as_ref(), meta.list_head));
                let end = dirs.list.pack(&(key.as_ref(), new_fdb_head));
                tr.clear_range(&begin, &end);
            }
            // Clear elements after the new tail (new_fdb_tail + 1 ..= tail) — exclusive upper.
            if new_fdb_tail < meta.list_tail {
                let begin = dirs.list.pack(&(key.as_ref(), new_fdb_tail + 1));
                let end = dirs.list.pack(&(key.as_ref(), meta.list_tail + 1));
                tr.clear_range(&begin, &end);
            }

            let new_length = e - s + 1;
            write_or_delete_list_meta(&tr, &dirs, key, Some(&meta), new_fdb_head, new_fdb_tail, new_length)?;
            Ok(())
        }
    })
    .await
    {
        Ok(()) => RespValue::ok(),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// Compact-rewrite helper (shared by LREM, LINSERT)
// ---------------------------------------------------------------------------

/// Clear the existing list range and rewrite `new_elements` starting at index 0.
///
/// After this call, the list's FDB indices are contiguous [0, N-1], and
/// meta is updated accordingly. If `new_elements` is empty, the list is
/// deleted entirely.
///
/// This is used by LREM and LINSERT, which compute a new element sequence
/// in memory and then replace the whole list. It's O(n) in FDB writes
/// and can hit FDB's 10MB transaction limit for very large lists; the
/// error will propagate with a clear message.
fn compact_rewrite(
    tr: &foundationdb::Transaction,
    dirs: &Directories,
    key: &[u8],
    old_meta: &ObjectMeta,
    new_elements: Vec<Vec<u8>>,
) -> Result<u64, foundationdb::FdbBindingError> {
    // Clear the entire old range [head, tail] inclusive.
    let begin = dirs.list.pack(&(key, old_meta.list_head));
    let end = dirs.list.pack(&(key, old_meta.list_tail + 1));
    tr.clear_range(&begin, &end);

    let new_length = new_elements.len() as u64;

    if new_length == 0 {
        helpers::delete_all_data_and_meta(tr, dirs, key, old_meta).map_err(helpers::cmd_err)?;
        return Ok(0);
    }

    // Write compacted elements at indices [0, N-1].
    for (i, elem) in new_elements.into_iter().enumerate() {
        tr.set(&dirs.list.pack(&(key, i as i64)), &elem);
    }

    let new_tail = new_length as i64 - 1;
    write_or_delete_list_meta(tr, dirs, key, Some(old_meta), 0, new_tail, new_length)?;
    Ok(new_length)
}

// ---------------------------------------------------------------------------
// LREM key count element
// ---------------------------------------------------------------------------

/// LREM key count element — Remove occurrences of `element`.
///
/// `count > 0`: remove the first `count` matches, head-to-tail.
/// `count < 0`: remove the first `|count|` matches, tail-to-head.
/// `count == 0`: remove all matches.
///
/// Returns the number of elements removed (0 if key doesn't exist).
pub async fn handle_lrem(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "LREM".into() }.to_string());
    }

    let count = match parse_i64_arg(&args[1]) {
        Ok(c) => c,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, "LREM", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let target = &args[2];

            let meta = match read_list_meta_for_read(&tr, &dirs, key).await? {
                None => return Ok(0i64),
                Some(m) => m,
            };

            // Read the entire list. This is O(n) — same as Redis LREM.
            let elements = read_element_range(&tr, &dirs, key, meta.list_head, meta.list_tail).await?;

            // Build a keep/remove mask. For count > 0 we scan forward and
            // remove up to `count` matches; for count < 0 we scan backward.
            // count == 0 removes all.
            let limit: Option<usize> = if count == 0 {
                None
            } else {
                Some(count.unsigned_abs() as usize)
            };
            let mut to_remove: Vec<bool> = vec![false; elements.len()];
            let mut removed: usize = 0;

            if count >= 0 {
                for (i, elem) in elements.iter().enumerate() {
                    if elem.as_slice() == target.as_ref() {
                        to_remove[i] = true;
                        removed += 1;
                        if let Some(lim) = limit
                            && removed >= lim
                        {
                            break;
                        }
                    }
                }
            } else {
                for (i, elem) in elements.iter().enumerate().rev() {
                    if elem.as_slice() == target.as_ref() {
                        to_remove[i] = true;
                        removed += 1;
                        if let Some(lim) = limit
                            && removed >= lim
                        {
                            break;
                        }
                    }
                }
            }

            if removed == 0 {
                return Ok(0);
            }

            let kept: Vec<Vec<u8>> = elements
                .into_iter()
                .zip(to_remove.into_iter())
                .filter_map(|(e, rm)| if rm { None } else { Some(e) })
                .collect();

            compact_rewrite(&tr, &dirs, key, &meta, kept)?;
            Ok(removed as i64)
        }
    })
    .await
    {
        Ok(n) => RespValue::Integer(n),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LINSERT key BEFORE|AFTER pivot element
// ---------------------------------------------------------------------------

/// LINSERT key BEFORE|AFTER pivot element — Insert `element` next to `pivot`.
///
/// Returns the new list length on success, -1 if the pivot was not found,
/// or 0 if the key doesn't exist.
pub async fn handle_linsert(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 4 {
        return RespValue::err(CommandError::WrongArity { name: "LINSERT".into() }.to_string());
    }

    let where_arg = args[1].to_ascii_uppercase();
    let insert_before = match where_arg.as_slice() {
        b"BEFORE" => true,
        b"AFTER" => false,
        _ => return RespValue::err("ERR syntax error"),
    };

    match run_transact(&state.db, "LINSERT", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let pivot = &args[2];
            let new_elem = &args[3];

            if new_elem.len() > MAX_VALUE_SIZE {
                return Err(helpers::cmd_err(CommandError::Generic(format!(
                    "ERR list element exceeds maximum size ({} bytes > {} byte limit)",
                    new_elem.len(),
                    MAX_VALUE_SIZE,
                ))));
            }

            let meta = match read_list_meta_for_read(&tr, &dirs, key).await? {
                None => return Ok(0i64),
                Some(m) => m,
            };

            let elements = read_element_range(&tr, &dirs, key, meta.list_head, meta.list_tail).await?;

            // Find the first occurrence of `pivot`.
            let pivot_idx = elements.iter().position(|e| e.as_slice() == pivot.as_ref());
            let pivot_idx = match pivot_idx {
                None => return Ok(-1i64),
                Some(i) => i,
            };

            // Splice the new element into the vec.
            let mut new_elements = elements;
            let insert_at = if insert_before { pivot_idx } else { pivot_idx + 1 };
            new_elements.insert(insert_at, new_elem.to_vec());

            let new_len = compact_rewrite(&tr, &dirs, key, &meta, new_elements)?;
            Ok(new_len as i64)
        }
    })
    .await
    {
        Ok(n) => RespValue::Integer(n),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
// ---------------------------------------------------------------------------

/// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
///
/// Returns the logical index of the Nth match of `element`.
/// - `RANK rank`: which match to return (1=first, -1=first from tail).
/// - `COUNT count`: if provided, return up to `count` indices as an array
///   (0 = return all matches). Without COUNT, return a single integer or Nil.
/// - `MAXLEN maxlen`: limit scan to the first `maxlen` elements (0 = no limit).
pub async fn handle_lpos(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "LPOS".into() }.to_string());
    }

    // Parse optional args. Defaults: RANK=1, COUNT absent (single result),
    // MAXLEN=0 (no scan limit).
    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: i64 = 0;

    let mut i = 2;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        if i + 1 >= args.len() {
            return RespValue::err("ERR syntax error");
        }
        let val = match parse_i64_arg(&args[i + 1]) {
            Ok(v) => v,
            Err(resp) => return resp,
        };
        match opt.as_slice() {
            b"RANK" => {
                // Redis disallows RANK = 0 explicitly.
                if val == 0 {
                    return RespValue::err(
                        "ERR RANK can't be zero: use 1 to start from the first match going forward, or -1 from the last match going backward.",
                    );
                }
                rank = val;
            }
            b"COUNT" => {
                if val < 0 {
                    return RespValue::err("ERR COUNT can't be negative");
                }
                count = Some(val);
            }
            b"MAXLEN" => {
                if val < 0 {
                    return RespValue::err("ERR MAXLEN can't be negative");
                }
                maxlen = val;
            }
            _ => return RespValue::err("ERR syntax error"),
        }
        i += 2;
    }

    // Two return shapes: single-value (no COUNT) or array (with COUNT).
    // We compute the array-of-matches internally, then convert.
    match run_transact(&state.db, "LPOS", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let target = &args[1];

            let meta = match read_list_meta_for_read(&tr, &dirs, key).await? {
                None => return Ok(Vec::<i64>::new()),
                Some(m) => m,
            };

            let forward = rank > 0;
            let skip = (rank.unsigned_abs() as usize).saturating_sub(1);
            // count: None = want 1, Some(0) = want all, Some(n>0) = want n.
            let want: Option<usize> = match count {
                None => Some(1),
                Some(0) => None, // all
                Some(n) => Some(n as usize),
            };

            // Optimization: when MAXLEN is specified, only read the
            // relevant portion of the list from FDB rather than the
            // entire thing.
            let scan_len = if maxlen == 0 {
                meta.list_length as usize
            } else {
                (maxlen as usize).min(meta.list_length as usize)
            };

            // When scanning forward, read the first `scan_len` elements;
            // when scanning backward, read the last `scan_len` elements.
            // We also track the logical offset so returned indices are
            // still relative to the full list head.
            let (fdb_start, fdb_end_inclusive, logical_offset) = if forward {
                let fdb_start = meta.list_head;
                let fdb_end = meta.list_head + scan_len as i64 - 1;
                (fdb_start, fdb_end.min(meta.list_tail), 0usize)
            } else {
                let fdb_end = meta.list_tail;
                let fdb_start = meta.list_tail - scan_len as i64 + 1;
                let offset = (meta.list_length as usize).saturating_sub(scan_len);
                (fdb_start.max(meta.list_head), fdb_end, offset)
            };

            let elements = read_element_range(&tr, &dirs, key, fdb_start, fdb_end_inclusive).await?;

            let mut matches = Vec::new();
            let mut skipped = 0;

            let iter: Box<dyn Iterator<Item = (usize, &Vec<u8>)>> = if forward {
                Box::new(elements.iter().enumerate())
            } else {
                // Reverse scan: iterate tail-to-head. The logical index
                // accounts for the offset into the full list.
                Box::new(elements.iter().enumerate().rev())
            };

            for (local_idx, elem) in iter {
                if elem.as_slice() == target.as_ref() {
                    if skipped < skip {
                        skipped += 1;
                        continue;
                    }
                    // Translate the index within our (possibly partial)
                    // read back to the full-list logical index.
                    matches.push((local_idx + logical_offset) as i64);
                    if let Some(w) = want
                        && matches.len() >= w
                    {
                        break;
                    }
                }
            }

            Ok(matches)
        }
    })
    .await
    {
        Ok(matches) => {
            if count.is_none() {
                // No COUNT: return single integer (first match) or Nil.
                match matches.first() {
                    Some(&idx) => RespValue::Integer(idx),
                    None => RespValue::BulkString(None),
                }
            } else {
                // With COUNT: return array (possibly empty).
                RespValue::Array(Some(matches.into_iter().map(RespValue::Integer).collect()))
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// Direction helper (shared by LMOVE, LMPOP)
// ---------------------------------------------------------------------------

/// Parse a direction argument: LEFT or RIGHT (case-insensitive).
///
/// Returns `Ok(true)` for LEFT, `Ok(false)` for RIGHT, or an `Err(RespValue)`
/// syntax error for anything else.
pub(crate) fn parse_direction(arg: &Bytes) -> Result<bool, RespValue> {
    let upper = arg.to_ascii_uppercase();
    match upper.as_slice() {
        b"LEFT" => Ok(true),
        b"RIGHT" => Ok(false),
        _ => Err(RespValue::err("ERR syntax error")),
    }
}

// ---------------------------------------------------------------------------
// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
// ---------------------------------------------------------------------------

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT — Atomically move an
/// element from one list to another.
///
/// Pops from the specified end of `source` and pushes to the specified end
/// of `destination`. Returns the moved element as a bulk string, or nil if
/// the source list is empty or doesn't exist. Source and destination may be
/// the same key (rotation).
pub async fn handle_lmove(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 4 {
        return RespValue::err(CommandError::WrongArity { name: "LMOVE".into() }.to_string());
    }

    let src_left = match parse_direction(&args[2]) {
        Ok(left) => left,
        Err(resp) => return resp,
    };
    let dst_left = match parse_direction(&args[3]) {
        Ok(left) => left,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, "LMOVE", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let src_key = &args[0];
            let dst_key = &args[1];
            let same_key = src_key == dst_key;

            // --- Read source meta ---
            let src_meta = read_list_meta_for_write(&tr, &dirs, src_key).await?;
            let src_meta = match src_meta {
                None => return Ok(None),
                Some(m) => {
                    if m.list_length == 0 {
                        return Ok(None);
                    }
                    m
                }
            };

            let mut src_head = src_meta.list_head;
            let mut src_tail = src_meta.list_tail;
            let mut src_length = src_meta.list_length;

            // --- Pop from source ---
            let pop_idx = if src_left { src_head } else { src_tail };
            let fdb_key = dirs.list.pack(&(src_key.as_ref(), pop_idx));
            let val = tr
                .get(&fdb_key, false)
                .await
                .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

            let element = match val {
                Some(v) => v.to_vec(),
                None => {
                    return Err(helpers::cmd_err(CommandError::Generic(format!(
                        "list index {pop_idx} missing but meta claims length {src_length}"
                    ))));
                }
            };
            tr.clear(&fdb_key);

            if src_left {
                src_head += 1;
            } else {
                src_tail -= 1;
            }
            src_length -= 1;

            // --- Push to destination ---
            if same_key {
                // Source and destination are the same key. Use post-pop state.
                if src_length == 0 {
                    // Popped the only element — push it back as the sole element.
                    src_head = 0;
                    src_tail = 0;
                    src_length = 1;
                    tr.set(&dirs.list.pack(&(src_key.as_ref(), 0i64)), element.as_slice());
                } else if dst_left {
                    src_head -= 1;
                    src_length += 1;
                    tr.set(&dirs.list.pack(&(src_key.as_ref(), src_head)), element.as_slice());
                } else {
                    src_tail += 1;
                    src_length += 1;
                    tr.set(&dirs.list.pack(&(src_key.as_ref(), src_tail)), element.as_slice());
                }

                write_or_delete_list_meta(&tr, &dirs, src_key, Some(&src_meta), src_head, src_tail, src_length)?;
            } else {
                // Different keys — update source, then read/push destination.

                // Update source meta (may delete if now empty).
                write_or_delete_list_meta(&tr, &dirs, src_key, Some(&src_meta), src_head, src_tail, src_length)?;

                // Read destination meta.
                let dst_meta = read_list_meta_for_write(&tr, &dirs, dst_key).await?;

                let (mut dst_head, mut dst_tail, mut dst_length) = match &dst_meta {
                    Some(m) => (m.list_head, m.list_tail, m.list_length),
                    None => (0, 0, 0),
                };

                if dst_length == 0 {
                    // Destination doesn't exist or is empty — create it.
                    dst_head = 0;
                    dst_tail = 0;
                    dst_length = 1;
                    tr.set(&dirs.list.pack(&(dst_key.as_ref(), 0i64)), element.as_slice());
                } else if dst_left {
                    dst_head -= 1;
                    dst_length += 1;
                    tr.set(&dirs.list.pack(&(dst_key.as_ref(), dst_head)), element.as_slice());
                } else {
                    dst_tail += 1;
                    dst_length += 1;
                    tr.set(&dirs.list.pack(&(dst_key.as_ref(), dst_tail)), element.as_slice());
                }

                write_or_delete_list_meta(&tr, &dirs, dst_key, dst_meta.as_ref(), dst_head, dst_tail, dst_length)?;
            }

            Ok(Some(element))
        }
    })
    .await
    {
        Ok(Some(element)) => RespValue::BulkString(Some(Bytes::from(element))),
        Ok(None) => RespValue::BulkString(None),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
// ---------------------------------------------------------------------------

/// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
///
/// Scans the given keys in order and pops up to COUNT (default 1) elements
/// from the first non-empty list found. Only one list is affected per call.
///
/// Returns a two-element array `[key_name, [elements...]]`, or nil if all
/// lists are empty or missing.
pub async fn handle_lmpop(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Minimum args: numkeys + at least 1 key + direction = 3
    if args.len() < 3 {
        return RespValue::err(CommandError::WrongArity { name: "LMPOP".into() }.to_string());
    }

    // Parse numkeys.
    let numkeys = match parse_i64_arg(&args[0]) {
        Ok(n) => n,
        Err(resp) => return resp,
    };
    if numkeys < 1 {
        return RespValue::err("ERR numkeys can't be non-positive value");
    }
    let numkeys = numkeys as usize;

    // Validate we have enough arguments: numkeys-arg + numkeys keys + direction.
    let dir_pos = 1 + numkeys;
    if dir_pos >= args.len() {
        return RespValue::err(CommandError::WrongArity { name: "LMPOP".into() }.to_string());
    }

    // Parse direction (LEFT|RIGHT).
    let from_head = match parse_direction(&args[dir_pos]) {
        Ok(left) => left,
        Err(resp) => return resp,
    };

    // Parse optional COUNT (default 1).
    // After direction, remaining args should be nothing or "COUNT <n>".
    let remaining = &args[dir_pos + 1..];
    let count: u64 = match remaining.len() {
        0 => 1,
        2 => {
            let kw = remaining[0].to_ascii_uppercase();
            if kw.as_slice() != b"COUNT" {
                return RespValue::err("ERR syntax error");
            }
            match parse_i64_arg(&remaining[1]) {
                Ok(c) if c < 1 => {
                    return RespValue::err("ERR COUNT value of 0 is not allowed");
                }
                Ok(c) => c as u64,
                Err(resp) => return resp,
            }
        }
        _ => return RespValue::err("ERR syntax error"),
    };

    match run_transact(&state.db, "LMPOP", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let keys = &args[1..1 + numkeys];

            for key in keys {
                let meta = read_list_meta_for_write(&tr, &dirs, key).await?;

                let meta = match meta {
                    None => continue,
                    Some(m) => {
                        if m.list_length == 0 {
                            continue;
                        }
                        m
                    }
                };

                // Found a non-empty list — pop up to `count` elements.
                let to_pop = count.min(meta.list_length);
                let mut popped: Vec<Vec<u8>> = Vec::with_capacity(to_pop as usize);

                let mut head = meta.list_head;
                let mut tail = meta.list_tail;
                let mut length = meta.list_length;

                for _ in 0..to_pop {
                    let fdb_idx = if from_head { head } else { tail };
                    let fdb_key = dirs.list.pack(&(key.as_ref(), fdb_idx));
                    let val = tr
                        .get(&fdb_key, false)
                        .await
                        .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                    let bytes = match val {
                        Some(v) => v.to_vec(),
                        None => {
                            return Err(helpers::cmd_err(CommandError::Generic(format!(
                                "list index {fdb_idx} missing but meta claims length {length}"
                            ))));
                        }
                    };
                    tr.clear(&fdb_key);
                    popped.push(bytes);

                    if from_head {
                        head += 1;
                    } else {
                        tail -= 1;
                    }
                    length -= 1;
                }

                write_or_delete_list_meta(&tr, &dirs, key, Some(&meta), head, tail, length)?;

                return Ok(Some((key.clone(), popped)));
            }

            // No non-empty list found.
            Ok(None)
        }
    })
    .await
    {
        Ok(None) => RespValue::Array(None),
        Ok(Some((key, elements))) => RespValue::Array(Some(vec![
            RespValue::BulkString(Some(key)),
            RespValue::Array(Some(
                elements
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(Bytes::from(v))))
                    .collect(),
            )),
        ])),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_index_positive_in_range() {
        // head=5, length=3 -> logical 0..2 maps to FDB 5..7.
        assert_eq!(resolve_index(5, 3, 0), Some(5));
        assert_eq!(resolve_index(5, 3, 1), Some(6));
        assert_eq!(resolve_index(5, 3, 2), Some(7));
    }

    #[test]
    fn resolve_index_positive_out_of_range() {
        assert_eq!(resolve_index(0, 3, 3), None);
        assert_eq!(resolve_index(0, 3, 100), None);
    }

    #[test]
    fn resolve_index_negative_in_range() {
        // head=-2, length=3 -> logical -1 is last (FDB index 0),
        // -2 is middle (-1), -3 is first (-2).
        assert_eq!(resolve_index(-2, 3, -1), Some(0));
        assert_eq!(resolve_index(-2, 3, -2), Some(-1));
        assert_eq!(resolve_index(-2, 3, -3), Some(-2));
    }

    #[test]
    fn resolve_index_negative_out_of_range() {
        assert_eq!(resolve_index(0, 3, -4), None);
        assert_eq!(resolve_index(0, 3, -100), None);
    }

    #[test]
    fn resolve_index_empty_list() {
        assert_eq!(resolve_index(0, 0, 0), None);
        assert_eq!(resolve_index(0, 0, -1), None);
    }

    #[test]
    fn normalize_range_basic() {
        assert_eq!(normalize_range(5, 0, 2), Some((0, 2)));
        assert_eq!(normalize_range(5, 1, 3), Some((1, 3)));
        assert_eq!(normalize_range(5, 0, -1), Some((0, 4)));
    }

    #[test]
    fn normalize_range_clamping() {
        // stop past end clamps to length-1.
        assert_eq!(normalize_range(5, 0, 100), Some((0, 4)));
        // start before 0 clamps to 0.
        assert_eq!(normalize_range(5, -100, 2), Some((0, 2)));
    }

    #[test]
    fn normalize_range_empty_range() {
        // start > stop -> empty.
        assert_eq!(normalize_range(5, 3, 1), None);
        // start past end -> empty.
        assert_eq!(normalize_range(5, 10, 20), None);
    }

    #[test]
    fn normalize_range_empty_list() {
        assert_eq!(normalize_range(0, 0, 0), None);
        assert_eq!(normalize_range(0, 0, -1), None);
    }

    #[test]
    fn normalize_range_negative_indices() {
        // length=5: -1 = 4, -2 = 3.
        assert_eq!(normalize_range(5, -3, -1), Some((2, 4)));
        assert_eq!(normalize_range(5, -5, -5), Some((0, 0)));
    }
}
