//! Hash command handlers: HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS,
//! HGETALL, HMGET, HMSET, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD.
//!
//! Hash fields are stored as individual FDB key-value pairs:
//!   hash/<redis_key, field_name> -> field_value
//!
//! ObjectMeta tracks the key type (Hash) and cardinality (field count).
//! All reads/writes happen within a single FDB transaction per command.

use bytes::Bytes;
use foundationdb::RangeOption;
use futures::future::join_all;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::{helpers, run_transact};

use super::util::{format_redis_float, parse_i64_arg};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Read all (field, value) pairs for a hash key via FDB range read.
///
/// Paginates through the full range so hashes with more fields than a
/// single FDB response batch are read completely. Returns an empty vec
/// if the hash has no fields.
async fn read_hash_fields(
    tr: &foundationdb::Transaction,
    dirs: &crate::storage::directories::Directories,
    key: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CommandError> {
    let sub = dirs.hash.subspace(&(key,));
    let (begin, end) = sub.range();
    let mut maybe_range: Option<RangeOption<'_>> = Some(RangeOption::from((begin.as_slice(), end.as_slice())));
    let mut pairs = Vec::new();
    let mut iteration = 1;

    while let Some(range_opt) = maybe_range.take() {
        let kvs = tr
            .get_range(&range_opt, iteration, false)
            .await
            .map_err(|e| CommandError::Generic(e.to_string()))?;

        for kv in kvs.iter() {
            let (_, field): (Vec<u8>, Vec<u8>) = dirs
                .hash
                .unpack(kv.key())
                .map_err(|e| CommandError::Generic(format!("hash key unpack: {e:?}")))?;
            pairs.push((field, kv.value().to_vec()));
        }

        maybe_range = range_opt.next_range(&kvs);
        iteration += 1;
    }

    Ok(pairs)
}

// ---------------------------------------------------------------------------
// HSET key field value [field value ...]
// ---------------------------------------------------------------------------

/// HSET key field value [field value ...] — Set field(s) in a hash.
///
/// Returns the number of fields that were **added** (not updated).
/// Creates the hash if the key doesn't exist. Returns WRONGTYPE if
/// the key exists but is not a hash.
pub async fn handle_hset(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Minimum: key + one field-value pair = 3 args, and must be odd (key + pairs).
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return RespValue::err(CommandError::WrongArity { name: "HSET".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HSET", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let now = helpers::now_ms();

            // Read meta WITHOUT expiry filtering so we can clean up expired data.
            let raw_meta = ObjectMeta::read(&tr, &dirs, key, 0, false)
                .await
                .map_err(helpers::storage_err)?;

            // Determine if the key is live (exists and not expired).
            let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

            // Type check: if the key is live and not a hash, reject.
            if let Some(m) = live_meta
                && m.key_type != KeyType::Hash
            {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            // If old meta exists and is either the wrong type or expired, clean up.
            if let Some(old_meta) = &raw_meta
                && (old_meta.is_expired(now) || old_meta.key_type != KeyType::Hash)
            {
                helpers::delete_all_data_and_meta(&tr, &dirs, key, old_meta).map_err(helpers::cmd_err)?;
            }

            let old_cardinality = live_meta.map_or(0, |m| m.cardinality);
            let old_expires_at_ms = live_meta.map_or(0, |m| m.expires_at_ms);
            let mut added: i64 = 0;

            // Process field-value pairs. Pre-pack FDB keys so we can fire
            // all existence checks in parallel (same pattern as MGET).
            let pairs = &args[1..];
            let fdb_keys: Vec<Vec<u8>> = pairs
                .chunks(2)
                .map(|chunk| dirs.hash.pack(&(key.as_ref(), chunk[0].as_ref())))
                .collect();

            let get_futures: Vec<_> = fdb_keys.iter().map(|fk| tr.get(fk, false)).collect();
            let results = join_all(get_futures).await;

            for (i, result) in results.into_iter().enumerate() {
                let existing = result.map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;
                if existing.is_none() {
                    added += 1;
                }
                tr.set(&fdb_keys[i], &pairs[i * 2 + 1]);
            }

            // Write updated ObjectMeta.
            let new_cardinality = (old_cardinality as i64 + added) as u64;
            let mut meta = ObjectMeta::new_hash(new_cardinality);
            meta.expires_at_ms = old_expires_at_ms;
            meta.write(&tr, &dirs, key).map_err(helpers::storage_err)?;

            Ok(added)
        }
    })
    .await
    {
        Ok(added) => RespValue::Integer(added),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HGET key field
// ---------------------------------------------------------------------------

/// HGET key field — Returns the value of a hash field, or nil.
///
/// Returns WRONGTYPE if the key exists but is not a hash.
pub async fn handle_hget(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "HGET".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HGET", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let field = args[1].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(None),
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }

                    let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));
                    let val = tr
                        .get(&fdb_key, false)
                        .await
                        .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                    Ok(val.map(|v| v.to_vec()))
                }
            }
        }
    })
    .await
    {
        Ok(Some(data)) => RespValue::BulkString(Some(Bytes::from(data))),
        Ok(None) => RespValue::BulkString(None),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HDEL key field [field ...]
// ---------------------------------------------------------------------------

/// HDEL key field [field ...] — Remove field(s) from a hash.
///
/// Returns the number of fields actually removed. If all fields are
/// removed, the key itself is deleted (ObjectMeta + expire entry).
/// Returns 0 if the key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hdel(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "HDEL".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HDEL", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            let meta = match meta {
                None => return Ok(0i64),
                Some(m) => m,
            };

            if meta.key_type != KeyType::Hash {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            let mut removed: i64 = 0;

            for field in &args[1..] {
                let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));
                let existing = tr
                    .get(&fdb_key, false)
                    .await
                    .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                if existing.is_some() {
                    tr.clear(&fdb_key);
                    removed += 1;
                }
            }

            let new_cardinality = meta.cardinality as i64 - removed;

            if new_cardinality <= 0 {
                // All fields removed — delete the key entirely.
                helpers::delete_all_data_and_meta(&tr, &dirs, key, &meta).map_err(helpers::cmd_err)?;
            } else {
                let mut new_meta = ObjectMeta::new_hash(new_cardinality as u64);
                new_meta.expires_at_ms = meta.expires_at_ms;
                new_meta.write(&tr, &dirs, key).map_err(helpers::storage_err)?;
            }

            Ok(removed)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HEXISTS key field
// ---------------------------------------------------------------------------

/// HEXISTS key field — Return 1 if field exists, 0 otherwise.
///
/// WRONGTYPE if key exists but is not a hash.
pub async fn handle_hexists(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "HEXISTS".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HEXISTS", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let field = args[1].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(0i64),
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }

                    let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));
                    let val = tr
                        .get(&fdb_key, false)
                        .await
                        .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                    Ok(if val.is_some() { 1 } else { 0 })
                }
            }
        }
    })
    .await
    {
        Ok(exists) => RespValue::Integer(exists),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HLEN key
// ---------------------------------------------------------------------------

/// HLEN key — Return the number of fields in a hash.
///
/// Returns 0 if the key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hlen(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "HLEN".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HLEN", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(0i64),
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    Ok(m.cardinality as i64)
                }
            }
        }
    })
    .await
    {
        Ok(len) => RespValue::Integer(len),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HGETALL key
// ---------------------------------------------------------------------------

/// HGETALL key — Return all fields and values as a flat array.
///
/// Returns an empty array if the key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hgetall(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "HGETALL".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HGETALL", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(vec![]),
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let pairs = read_hash_fields(&tr, &dirs, &key).await.map_err(helpers::cmd_err)?;
                    // Flatten to [field1, val1, field2, val2, ...]
                    let mut flat = Vec::with_capacity(pairs.len() * 2);
                    for (f, v) in pairs {
                        flat.push(f);
                        flat.push(v);
                    }
                    Ok(flat)
                }
            }
        }
    })
    .await
    {
        Ok(items) => {
            let elements: Vec<RespValue> = items
                .into_iter()
                .map(|b| RespValue::BulkString(Some(Bytes::from(b))))
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HKEYS key
// ---------------------------------------------------------------------------

/// HKEYS key — Return all field names as an array.
///
/// Returns an empty array if the key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hkeys(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "HKEYS".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HKEYS", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(vec![]),
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let pairs = read_hash_fields(&tr, &dirs, &key).await.map_err(helpers::cmd_err)?;
                    Ok(pairs.into_iter().map(|(f, _)| f).collect())
                }
            }
        }
    })
    .await
    {
        Ok(fields) => {
            let elements: Vec<RespValue> = fields
                .into_iter()
                .map(|b| RespValue::BulkString(Some(Bytes::from(b))))
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HVALS key
// ---------------------------------------------------------------------------

/// HVALS key — Return all field values as an array.
///
/// Returns an empty array if the key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hvals(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "HVALS".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HVALS", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(vec![]),
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let pairs = read_hash_fields(&tr, &dirs, &key).await.map_err(helpers::cmd_err)?;
                    Ok(pairs.into_iter().map(|(_, v)| v).collect())
                }
            }
        }
    })
    .await
    {
        Ok(values) => {
            let elements: Vec<RespValue> = values
                .into_iter()
                .map(|b| RespValue::BulkString(Some(Bytes::from(b))))
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HMGET key field [field ...]
// ---------------------------------------------------------------------------

/// HMGET key field [field ...] — Return values for multiple fields.
///
/// Returns nil for fields that don't exist. Returns array of nils if
/// the key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hmget(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "HMGET".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HMGET", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            let field_count = args.len() - 1;

            match meta {
                None => {
                    // Key doesn't exist — return array of nils.
                    Ok(vec![None; field_count])
                }
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }

                    // Fire all field lookups in parallel.
                    let fdb_keys: Vec<Vec<u8>> = args[1..]
                        .iter()
                        .map(|field| dirs.hash.pack(&(key.as_ref(), field.as_ref())))
                        .collect();
                    let get_futures: Vec<_> = fdb_keys.iter().map(|fk| tr.get(fk, false)).collect();
                    let raw_results = join_all(get_futures).await;

                    let mut results = Vec::with_capacity(field_count);
                    for result in raw_results {
                        let val = result.map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;
                        results.push(val.map(|v| v.to_vec()));
                    }
                    Ok(results)
                }
            }
        }
    })
    .await
    {
        Ok(values) => {
            let elements: Vec<RespValue> = values
                .into_iter()
                .map(|v| match v {
                    Some(data) => RespValue::BulkString(Some(Bytes::from(data))),
                    None => RespValue::BulkString(None),
                })
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HMSET key field value [field value ...]
// ---------------------------------------------------------------------------

/// HMSET key field value [field value ...] — Deprecated alias for HSET.
///
/// Delegates to handle_hset but returns OK instead of the added count.
pub async fn handle_hmset(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Same arity check as HSET.
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return RespValue::err(CommandError::WrongArity { name: "HMSET".into() }.to_string());
    }

    match handle_hset(args, state).await {
        RespValue::Integer(_) => RespValue::ok(),
        err => err, // propagate WRONGTYPE or other errors
    }
}

// ---------------------------------------------------------------------------
// HINCRBY key field increment
// ---------------------------------------------------------------------------

/// HINCRBY key field increment — Increment a hash field by an integer.
///
/// Creates the field with value 0 if it doesn't exist. Creates the hash
/// if the key doesn't exist. Returns the new value as Integer.
/// Error if field value is not an integer. Overflow protection.
pub async fn handle_hincrby(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "HINCRBY".into() }.to_string());
    }

    let delta = match parse_i64_arg(&args[2]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, state.shared_txn(), "HINCRBY", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let field = args[1].clone();
        let inc = delta;
        async move {
            let now = helpers::now_ms();

            // Read raw meta (even if expired) for cleanup.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &key, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

            // Type check.
            if let Some(m) = live_meta
                && m.key_type != KeyType::Hash
            {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            // Clean up expired/wrong-type data.
            if let Some(old_meta) = &raw_meta
                && (old_meta.is_expired(now) || old_meta.key_type != KeyType::Hash)
            {
                helpers::delete_all_data_and_meta(&tr, &dirs, &key, old_meta).map_err(helpers::cmd_err)?;
            }

            let old_cardinality = live_meta.map_or(0, |m| m.cardinality);
            let old_expires_at_ms = live_meta.map_or(0, |m| m.expires_at_ms);

            let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));
            let existing = tr
                .get(&fdb_key, false)
                .await
                .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

            let (current_val, is_new_field) = match &existing {
                Some(data) => {
                    let s = std::str::from_utf8(data).map_err(|_| {
                        helpers::cmd_err(CommandError::Generic(
                            "hash value is not an integer or out of range".into(),
                        ))
                    })?;
                    let v: i64 = s.parse().map_err(|_| {
                        helpers::cmd_err(CommandError::Generic(
                            "hash value is not an integer or out of range".into(),
                        ))
                    })?;
                    (v, false)
                }
                None => (0i64, true),
            };

            let new_val = current_val.checked_add(inc).ok_or_else(|| {
                helpers::cmd_err(CommandError::Generic("increment or decrement would overflow".into()))
            })?;

            let mut buf = itoa::Buffer::new();
            let new_str = buf.format(new_val);
            tr.set(&fdb_key, new_str.as_bytes());

            // Update meta.
            let new_cardinality = if is_new_field {
                old_cardinality + 1
            } else {
                old_cardinality
            };
            let mut meta = ObjectMeta::new_hash(new_cardinality);
            meta.expires_at_ms = old_expires_at_ms;
            meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;

            Ok(new_val)
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HINCRBYFLOAT key field increment
// ---------------------------------------------------------------------------

/// HINCRBYFLOAT key field increment — Increment a hash field by a float.
///
/// Creates the field with value 0 if it doesn't exist. Creates the hash
/// if the key doesn't exist. Returns the new value as BulkString.
/// Error if field value is not a valid float. Rejects NaN/Inf.
pub async fn handle_hincrbyfloat(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "HINCRBYFLOAT".into(),
            }
            .to_string(),
        );
    }

    let incr_str = match std::str::from_utf8(&args[2]) {
        Ok(s) => s.to_owned(),
        Err(_) => return RespValue::err("ERR value is not a valid float"),
    };

    let increment: f64 = match incr_str.parse() {
        Ok(v) => v,
        Err(_) => return RespValue::err("ERR value is not a valid float"),
    };

    if increment.is_nan() || increment.is_infinite() {
        return RespValue::err("ERR increment would produce NaN or Infinity");
    }

    match run_transact(&state.db, state.shared_txn(), "HINCRBYFLOAT", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let field = args[1].clone();
        let inc = increment;
        async move {
            let now = helpers::now_ms();

            // Read raw meta (even if expired) for cleanup.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &key, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

            // Type check.
            if let Some(m) = live_meta
                && m.key_type != KeyType::Hash
            {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            // Clean up expired/wrong-type data.
            if let Some(old_meta) = &raw_meta
                && (old_meta.is_expired(now) || old_meta.key_type != KeyType::Hash)
            {
                helpers::delete_all_data_and_meta(&tr, &dirs, &key, old_meta).map_err(helpers::cmd_err)?;
            }

            let old_cardinality = live_meta.map_or(0, |m| m.cardinality);
            let old_expires_at_ms = live_meta.map_or(0, |m| m.expires_at_ms);

            let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));
            let existing = tr
                .get(&fdb_key, false)
                .await
                .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

            let (current_val, is_new_field) = match &existing {
                Some(data) => {
                    let s = std::str::from_utf8(data).map_err(|_| {
                        helpers::cmd_err(CommandError::Generic("hash value is not a valid float".into()))
                    })?;
                    let v: f64 = s.parse().map_err(|_| {
                        helpers::cmd_err(CommandError::Generic("hash value is not a valid float".into()))
                    })?;
                    (v, false)
                }
                None => (0.0f64, true),
            };

            let result = current_val + inc;
            if result.is_nan() || result.is_infinite() {
                return Err(helpers::cmd_err(CommandError::Generic(
                    "increment would produce NaN or Infinity".into(),
                )));
            }

            let formatted = format_redis_float(result);
            tr.set(&fdb_key, formatted.as_bytes());

            // Update meta.
            let new_cardinality = if is_new_field {
                old_cardinality + 1
            } else {
                old_cardinality
            };
            let mut meta = ObjectMeta::new_hash(new_cardinality);
            meta.expires_at_ms = old_expires_at_ms;
            meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;

            Ok(formatted)
        }
    })
    .await
    {
        Ok(s) => RespValue::BulkString(Some(Bytes::from(s))),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HSETNX key field value
// ---------------------------------------------------------------------------

/// HSETNX key field value — Set field only if it doesn't already exist.
///
/// Returns 1 if the field was set, 0 if it already existed.
/// Creates the hash if the key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hsetnx(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "HSETNX".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HSETNX", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let field = args[1].clone();
        let value = args[2].clone();
        async move {
            let now = helpers::now_ms();

            // Read raw meta (even if expired) for cleanup.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &key, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

            // Type check.
            if let Some(m) = live_meta
                && m.key_type != KeyType::Hash
            {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            // Clean up expired/wrong-type data.
            if let Some(old_meta) = &raw_meta
                && (old_meta.is_expired(now) || old_meta.key_type != KeyType::Hash)
            {
                helpers::delete_all_data_and_meta(&tr, &dirs, &key, old_meta).map_err(helpers::cmd_err)?;
            }

            let old_cardinality = live_meta.map_or(0, |m| m.cardinality);
            let old_expires_at_ms = live_meta.map_or(0, |m| m.expires_at_ms);

            let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));
            let existing = tr
                .get(&fdb_key, false)
                .await
                .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

            if existing.is_some() {
                // Field already exists — don't set.
                return Ok(0i64);
            }

            tr.set(&fdb_key, &value);

            // Update meta (added one field).
            let mut meta = ObjectMeta::new_hash(old_cardinality + 1);
            meta.expires_at_ms = old_expires_at_ms;
            meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;

            Ok(1i64)
        }
    })
    .await
    {
        Ok(result) => RespValue::Integer(result),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HSTRLEN key field
// ---------------------------------------------------------------------------

/// HSTRLEN key field — Return the string length of a hash field value.
///
/// Returns 0 if the field or key doesn't exist. WRONGTYPE if not a hash.
pub async fn handle_hstrlen(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "HSTRLEN".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "HSTRLEN", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let field = args[1].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(0i64),
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }

                    let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));
                    let val = tr
                        .get(&fdb_key, false)
                        .await
                        .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                    Ok(val.map_or(0, |v| v.len() as i64))
                }
            }
        }
    })
    .await
    {
        Ok(len) => RespValue::Integer(len),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// HRANDFIELD key [count [WITHVALUES]]
// ---------------------------------------------------------------------------

/// HRANDFIELD key [count [WITHVALUES]] — Return random field(s).
///
/// - No count arg: return single field name (BulkString), nil if key doesn't exist.
/// - count > 0: return up to `count` distinct fields.
/// - count < 0: return `|count|` fields allowing duplicates.
/// - count == 0: return empty array.
/// - WITHVALUES flag: interleave values in the result array.
pub async fn handle_hrandfield(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() || args.len() > 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "HRANDFIELD".into(),
            }
            .to_string(),
        );
    }

    // Parse optional count and WITHVALUES.
    let has_count = args.len() >= 2;
    let count: i64 = if has_count {
        match parse_i64_arg(&args[1]) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    } else {
        0 // unused when has_count is false
    };

    let with_values = if args.len() == 3 {
        let flag = args[2].to_ascii_uppercase();
        if flag.as_slice() == b"WITHVALUES" {
            true
        } else {
            return RespValue::err("ERR syntax error");
        }
    } else {
        false
    };

    // WITHVALUES without count is a syntax error.
    if with_values && !has_count {
        return RespValue::err("ERR syntax error");
    }

    match run_transact(&state.db, state.shared_txn(), "HRANDFIELD", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();

            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => {
                    // Key doesn't exist — return empty for count mode, nil for no-count.
                    Ok(None)
                }
                Some(m) => {
                    if m.key_type != KeyType::Hash {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let pairs = read_hash_fields(&tr, &dirs, &key).await.map_err(helpers::cmd_err)?;
                    Ok(Some(pairs))
                }
            }
        }
    })
    .await
    {
        Ok(None) => {
            if !has_count {
                RespValue::BulkString(None)
            } else {
                RespValue::Array(Some(vec![]))
            }
        }
        Ok(Some(pairs)) => {
            if pairs.is_empty() {
                if !has_count {
                    return RespValue::BulkString(None);
                } else {
                    return RespValue::Array(Some(vec![]));
                }
            }

            if !has_count {
                // Return a single random field name.
                use rand::Rng;
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..pairs.len());
                return RespValue::BulkString(Some(Bytes::from(pairs[idx].0.clone())));
            }

            if count == 0 {
                return RespValue::Array(Some(vec![]));
            }

            use rand::Rng;
            let mut rng = rand::thread_rng();

            if count > 0 {
                // Distinct selection: up to `count` unique fields.
                let n = (count as usize).min(pairs.len());
                // Fisher-Yates partial shuffle.
                let mut indices: Vec<usize> = (0..pairs.len()).collect();
                for i in 0..n {
                    let j = rng.gen_range(i..indices.len());
                    indices.swap(i, j);
                }
                let selected = &indices[..n];

                let mut result = Vec::with_capacity(if with_values { n * 2 } else { n });
                for &idx in selected {
                    result.push(RespValue::BulkString(Some(Bytes::from(pairs[idx].0.clone()))));
                    if with_values {
                        result.push(RespValue::BulkString(Some(Bytes::from(pairs[idx].1.clone()))));
                    }
                }
                RespValue::Array(Some(result))
            } else {
                // count < 0: allow duplicates, return |count| fields.
                let n = count.unsigned_abs() as usize;
                let mut result = Vec::with_capacity(if with_values { n * 2 } else { n });
                for _ in 0..n {
                    let idx = rng.gen_range(0..pairs.len());
                    result.push(RespValue::BulkString(Some(Bytes::from(pairs[idx].0.clone()))));
                    if with_values {
                        result.push(RespValue::BulkString(Some(Bytes::from(pairs[idx].1.clone()))));
                    }
                }
                RespValue::Array(Some(result))
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
