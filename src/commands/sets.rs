//! Set command handlers: SADD, SREM, SISMEMBER, SMISMEMBER, SCARD,
//! SMEMBERS, SPOP, SRANDMEMBER, SMOVE, SINTER, SUNION, SDIFF,
//! SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SINTERCARD.
//!
//! Set members are stored as individual FDB keys with empty values:
//!   set/<redis_key, member> -> b""
//!
//! Membership is determined by key existence — no value payload needed.
//! ObjectMeta tracks the key type (Set) and cardinality (member count).
//! All reads/writes happen within a single FDB transaction per command.

use std::collections::HashSet;

use bytes::Bytes;
use foundationdb::RangeOption;
use futures::future::join_all;
use rand::Rng;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::{helpers, run_transact};

use super::util::parse_i64_arg;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Read all members of a set key via paginated FDB range read.
///
/// Returns an empty vec if the set has no members (or doesn't exist).
/// Members are returned in FDB key order (lexicographic by their
/// tuple-encoded representation).
async fn read_set_members(
    tr: &foundationdb::Transaction,
    dirs: &crate::storage::directories::Directories,
    key: &[u8],
) -> Result<Vec<Vec<u8>>, CommandError> {
    let sub = dirs.set.subspace(&(key,));
    let (begin, end) = sub.range();
    let mut maybe_range: Option<RangeOption<'_>> = Some(RangeOption::from((begin.as_slice(), end.as_slice())));
    let mut members = Vec::new();
    let mut iteration = 1;

    while let Some(range_opt) = maybe_range.take() {
        let kvs = tr
            .get_range(&range_opt, iteration, false)
            .await
            .map_err(|e| CommandError::Generic(e.to_string()))?;

        for kv in kvs.iter() {
            let (_, member): (Vec<u8>, Vec<u8>) = dirs
                .set
                .unpack(kv.key())
                .map_err(|e| CommandError::Generic(format!("set key unpack: {e:?}")))?;
            members.push(member);
        }

        maybe_range = range_opt.next_range(&kvs);
        iteration += 1;
    }

    Ok(members)
}

/// Read the live ObjectMeta for a set key, performing type checking.
///
/// Returns `Ok((None, 0, 0))` if the key doesn't exist or is expired.
/// Returns `Err` with WRONGTYPE if the key exists but is not a set.
/// Also cleans up expired or wrong-type data when encountered.
///
/// Returns `(live_meta, old_cardinality, old_expires_at_ms)`.
async fn read_set_meta_for_write(
    tr: &foundationdb::Transaction,
    dirs: &crate::storage::directories::Directories,
    key: &[u8],
) -> Result<(Option<ObjectMeta>, u64, u64), foundationdb::FdbBindingError> {
    let now = helpers::now_ms();

    // Read meta WITHOUT expiry filtering so we can clean up expired data.
    let raw_meta = ObjectMeta::read(tr, dirs, key, 0, false)
        .await
        .map_err(helpers::storage_err)?;

    // Determine if the key is live (exists and not expired).
    let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

    // Type check: if the key is live and not a set, reject.
    if let Some(m) = live_meta
        && m.key_type != KeyType::Set
    {
        return Err(helpers::cmd_err(CommandError::WrongType));
    }

    // If old meta exists and is either the wrong type or expired, clean up.
    if let Some(old_meta) = &raw_meta
        && (old_meta.is_expired(now) || old_meta.key_type != KeyType::Set)
    {
        helpers::delete_all_data_and_meta(tr, dirs, key, old_meta).map_err(helpers::cmd_err)?;
    }

    let old_cardinality = live_meta.map_or(0, |m| m.cardinality);
    let old_expires_at_ms = live_meta.map_or(0, |m| m.expires_at_ms);

    Ok((live_meta.cloned(), old_cardinality, old_expires_at_ms))
}

/// Read the live ObjectMeta for a set key (read-only path).
///
/// Returns `Ok(None)` if the key doesn't exist or is expired.
/// Returns `Err` with WRONGTYPE if the key exists but is not a set.
async fn read_set_meta_for_read(
    tr: &foundationdb::Transaction,
    dirs: &crate::storage::directories::Directories,
    key: &[u8],
) -> Result<Option<ObjectMeta>, foundationdb::FdbBindingError> {
    let now = helpers::now_ms();

    let meta = ObjectMeta::read(tr, dirs, key, now, false)
        .await
        .map_err(helpers::storage_err)?;

    match meta {
        None => Ok(None),
        Some(m) => {
            if m.key_type != KeyType::Set {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }
            Ok(Some(m))
        }
    }
}

// ---------------------------------------------------------------------------
// SADD key member [member ...]
// ---------------------------------------------------------------------------

/// SADD key member [member ...] — Add member(s) to a set.
///
/// Returns the number of members that were **added** (not already present).
/// Creates the set if the key doesn't exist. Returns WRONGTYPE if
/// the key exists but is not a set.
pub async fn handle_sadd(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "SADD".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "SADD", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let (_, old_cardinality, old_expires_at_ms) = read_set_meta_for_write(&tr, &dirs, key).await?;

            // Deduplicate members within the call — SADD k a a a returns 1,
            // not 3. We must deduplicate before the parallel existence checks
            // because all checks fire before any writes, so duplicates would
            // all see the member as absent and be over-counted.
            let mut seen = HashSet::new();
            let unique_members: Vec<&Bytes> = args[1..].iter().filter(|m| seen.insert(m.as_ref().to_vec())).collect();

            // Pre-pack FDB keys and fire all existence checks in parallel.
            let fdb_keys: Vec<Vec<u8>> = unique_members
                .iter()
                .map(|member| dirs.set.pack(&(key.as_ref(), member.as_ref())))
                .collect();

            let get_futures: Vec<_> = fdb_keys.iter().map(|fk| tr.get(fk, false)).collect();
            let results = join_all(get_futures).await;

            let mut added: i64 = 0;
            for (i, result) in results.into_iter().enumerate() {
                let existing = result.map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;
                if existing.is_none() {
                    added += 1;
                }
                // Always write — idempotent for existing members.
                tr.set(&fdb_keys[i], b"");
            }

            // Write updated ObjectMeta.
            let new_cardinality = (old_cardinality as i64 + added) as u64;
            let mut meta = ObjectMeta::new_set(new_cardinality);
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
// SREM key member [member ...]
// ---------------------------------------------------------------------------

/// SREM key member [member ...] — Remove member(s) from a set.
///
/// Returns the number of members actually removed. If all members are
/// removed, the key itself is deleted (ObjectMeta + expire entry).
/// Returns 0 if the key doesn't exist. WRONGTYPE if not a set.
pub async fn handle_srem(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "SREM".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "SREM", |tr| {
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

            if meta.key_type != KeyType::Set {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            // Deduplicate members within the call — SREM k a a should only
            // remove "a" once and return 1, not 2. Without deduplication the
            // parallel existence checks (join_all) would both see the member
            // as present and over-count removals, corrupting the cardinality.
            let mut seen = HashSet::new();
            let unique_members: Vec<&Bytes> = args[1..].iter().filter(|m| seen.insert(m.as_ref().to_vec())).collect();

            let fdb_keys: Vec<Vec<u8>> = unique_members
                .iter()
                .map(|member| dirs.set.pack(&(key.as_ref(), member.as_ref())))
                .collect();

            let get_futures: Vec<_> = fdb_keys.iter().map(|fk| tr.get(fk, false)).collect();
            let results = join_all(get_futures).await;

            let mut removed: i64 = 0;
            for (i, result) in results.into_iter().enumerate() {
                let existing = result.map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;
                if existing.is_some() {
                    tr.clear(&fdb_keys[i]);
                    removed += 1;
                }
            }

            let new_cardinality = meta.cardinality as i64 - removed;

            if new_cardinality <= 0 {
                // All members removed — delete the key entirely.
                helpers::delete_all_data_and_meta(&tr, &dirs, key, &meta).map_err(helpers::cmd_err)?;
            } else {
                let mut new_meta = ObjectMeta::new_set(new_cardinality as u64);
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
// SISMEMBER key member
// ---------------------------------------------------------------------------

/// SISMEMBER key member — Return 1 if member is in the set, 0 otherwise.
///
/// WRONGTYPE if key exists but is not a set.
pub async fn handle_sismember(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "SISMEMBER".into(),
            }
            .to_string(),
        );
    }

    match run_transact(&state.db, state.shared_txn(), "SISMEMBER", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let member = args[1].clone();
        async move {
            let meta = read_set_meta_for_read(&tr, &dirs, &key).await?;

            match meta {
                None => Ok(0i64),
                Some(_) => {
                    let fdb_key = dirs.set.pack(&(key.as_ref(), member.as_ref()));
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
// SMISMEMBER key member [member ...]
// ---------------------------------------------------------------------------

/// SMISMEMBER key member [member ...] — Check membership for multiple members.
///
/// Returns an array of 0/1 integers corresponding to each member.
/// WRONGTYPE if key exists but is not a set.
pub async fn handle_smismember(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "SMISMEMBER".into(),
            }
            .to_string(),
        );
    }

    match run_transact(&state.db, state.shared_txn(), "SMISMEMBER", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let meta = read_set_meta_for_read(&tr, &dirs, key).await?;

            let member_count = args.len() - 1;

            match meta {
                None => Ok(vec![0i64; member_count]),
                Some(_) => {
                    let fdb_keys: Vec<Vec<u8>> = args[1..]
                        .iter()
                        .map(|member| dirs.set.pack(&(key.as_ref(), member.as_ref())))
                        .collect();
                    let get_futures: Vec<_> = fdb_keys.iter().map(|fk| tr.get(fk, false)).collect();
                    let raw_results = join_all(get_futures).await;

                    let mut results = Vec::with_capacity(member_count);
                    for result in raw_results {
                        let val = result.map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;
                        results.push(if val.is_some() { 1i64 } else { 0i64 });
                    }
                    Ok(results)
                }
            }
        }
    })
    .await
    {
        Ok(values) => {
            let elements: Vec<RespValue> = values.into_iter().map(RespValue::Integer).collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SCARD key
// ---------------------------------------------------------------------------

/// SCARD key — Return the number of members in a set.
///
/// Returns 0 if the key doesn't exist. WRONGTYPE if not a set.
pub async fn handle_scard(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "SCARD".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "SCARD", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let meta = read_set_meta_for_read(&tr, &dirs, &key).await?;

            match meta {
                None => Ok(0i64),
                Some(m) => Ok(m.cardinality as i64),
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
// SMEMBERS key
// ---------------------------------------------------------------------------

/// SMEMBERS key — Return all members of a set as an array.
///
/// Returns an empty array if the key doesn't exist. WRONGTYPE if not a set.
pub async fn handle_smembers(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "SMEMBERS".into(),
            }
            .to_string(),
        );
    }

    match run_transact(&state.db, state.shared_txn(), "SMEMBERS", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let meta = read_set_meta_for_read(&tr, &dirs, &key).await?;

            match meta {
                None => Ok(vec![]),
                Some(_) => {
                    let members = read_set_members(&tr, &dirs, &key).await.map_err(helpers::cmd_err)?;
                    Ok(members)
                }
            }
        }
    })
    .await
    {
        Ok(members) => {
            let elements: Vec<RespValue> = members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SPOP key [count]
// ---------------------------------------------------------------------------

/// SPOP key \[count\] — Remove and return random member(s).
///
/// Without count: returns a single member (BulkString), nil if empty.
/// With count: returns an array of up to `count` distinct members.
/// Removed members are deleted from the set and cardinality is updated.
pub async fn handle_spop(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::err(CommandError::WrongArity { name: "SPOP".into() }.to_string());
    }

    let has_count = args.len() == 2;
    let count: i64 = if has_count {
        match parse_i64_arg(&args[1]) {
            Ok(v) if v < 0 => {
                return RespValue::err("ERR value is out of range, must be positive");
            }
            Ok(v) => v,
            Err(resp) => return resp,
        }
    } else {
        1
    };

    // Short-circuit: SPOP key 0 returns an empty array without touching FDB.
    if has_count && count == 0 {
        return RespValue::Array(Some(vec![]));
    }

    match run_transact(&state.db, state.shared_txn(), "SPOP", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();

            // Read raw meta for cleanup path.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &key, 0, false)
                .await
                .map_err(helpers::storage_err)?;

            let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

            if let Some(m) = live_meta
                && m.key_type != KeyType::Set
            {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            if let Some(old_meta) = &raw_meta
                && (old_meta.is_expired(now) || old_meta.key_type != KeyType::Set)
            {
                helpers::delete_all_data_and_meta(&tr, &dirs, &key, old_meta).map_err(helpers::cmd_err)?;
            }

            let meta = match live_meta {
                None => return Ok(None),
                Some(m) => m,
            };

            let members = read_set_members(&tr, &dirs, &key).await.map_err(helpers::cmd_err)?;

            if members.is_empty() {
                return Ok(None);
            }

            // Select up to `count` random distinct members.
            let n = (count as usize).min(members.len());
            let mut rng = rand::thread_rng();
            let mut indices: Vec<usize> = (0..members.len()).collect();
            for i in 0..n {
                let j = rng.gen_range(i..indices.len());
                indices.swap(i, j);
            }
            let selected_indices = &indices[..n];

            // Remove selected members from FDB.
            let mut popped = Vec::with_capacity(n);
            for &idx in selected_indices {
                let fdb_key = dirs.set.pack(&(key.as_ref(), members[idx].as_slice()));
                tr.clear(&fdb_key);
                popped.push(members[idx].clone());
            }

            let new_cardinality = meta.cardinality.saturating_sub(n as u64);

            if new_cardinality == 0 {
                helpers::delete_all_data_and_meta(&tr, &dirs, &key, meta).map_err(helpers::cmd_err)?;
            } else {
                let mut new_meta = ObjectMeta::new_set(new_cardinality);
                new_meta.expires_at_ms = meta.expires_at_ms;
                new_meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;
            }

            Ok(Some(popped))
        }
    })
    .await
    {
        Ok(None) => {
            if has_count {
                RespValue::Array(Some(vec![]))
            } else {
                RespValue::BulkString(None)
            }
        }
        Ok(Some(popped)) => {
            if !has_count {
                // Single-member mode: return as BulkString.
                RespValue::BulkString(Some(Bytes::from(popped.into_iter().next().unwrap())))
            } else {
                let elements: Vec<RespValue> = popped
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                    .collect();
                RespValue::Array(Some(elements))
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SRANDMEMBER key [count]
// ---------------------------------------------------------------------------

/// SRANDMEMBER key \[count\] — Return random member(s) without removing.
///
/// Without count: returns a single member (BulkString), nil if empty.
/// count > 0: return up to `count` distinct members.
/// count < 0: return `|count|` members allowing duplicates.
/// count == 0: return empty array.
pub async fn handle_srandmember(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "SRANDMEMBER".into(),
            }
            .to_string(),
        );
    }

    let has_count = args.len() == 2;
    let count: i64 = if has_count {
        match parse_i64_arg(&args[1]) {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    } else {
        0 // unused when has_count is false
    };

    match run_transact(&state.db, state.shared_txn(), "SRANDMEMBER", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let meta = read_set_meta_for_read(&tr, &dirs, &key).await?;

            match meta {
                None => Ok(None),
                Some(_) => {
                    let members = read_set_members(&tr, &dirs, &key).await.map_err(helpers::cmd_err)?;
                    Ok(Some(members))
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
        Ok(Some(members)) => {
            if members.is_empty() {
                if !has_count {
                    return RespValue::BulkString(None);
                } else {
                    return RespValue::Array(Some(vec![]));
                }
            }

            if !has_count {
                // Single random member.
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..members.len());
                return RespValue::BulkString(Some(Bytes::from(members[idx].clone())));
            }

            if count == 0 {
                return RespValue::Array(Some(vec![]));
            }

            let mut rng = rand::thread_rng();

            if count > 0 {
                // Distinct selection: up to `count` unique members.
                let n = (count as usize).min(members.len());
                let mut indices: Vec<usize> = (0..members.len()).collect();
                for i in 0..n {
                    let j = rng.gen_range(i..indices.len());
                    indices.swap(i, j);
                }
                let result: Vec<RespValue> = indices[..n]
                    .iter()
                    .map(|&idx| RespValue::BulkString(Some(Bytes::from(members[idx].clone()))))
                    .collect();
                RespValue::Array(Some(result))
            } else {
                // count < 0: allow duplicates, return |count| members.
                let n = count.unsigned_abs() as usize;
                let result: Vec<RespValue> = (0..n)
                    .map(|_| {
                        let idx = rng.gen_range(0..members.len());
                        RespValue::BulkString(Some(Bytes::from(members[idx].clone())))
                    })
                    .collect();
                RespValue::Array(Some(result))
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SMOVE source destination member
// ---------------------------------------------------------------------------

/// SMOVE source destination member — Move a member between sets.
///
/// Returns 1 if the member was moved, 0 if it wasn't in the source set.
/// Creates the destination set if it doesn't exist.
/// WRONGTYPE if either key exists but is not a set.
pub async fn handle_smove(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "SMOVE".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "SMOVE", |tr| {
        let dirs = state.dirs.clone();
        let source = args[0].clone();
        let destination = args[1].clone();
        let member = args[2].clone();
        async move {
            let now = helpers::now_ms();

            // --- Source key ---
            let src_meta = ObjectMeta::read(&tr, &dirs, &source, now, false)
                .await
                .map_err(helpers::storage_err)?;

            let src_meta = match src_meta {
                None => return Ok(0i64), // source doesn't exist
                Some(m) => {
                    if m.key_type != KeyType::Set {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    m
                }
            };

            // Check if member exists in source.
            let src_fdb_key = dirs.set.pack(&(source.as_ref(), member.as_ref()));
            let exists_in_source = tr
                .get(&src_fdb_key, false)
                .await
                .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

            if exists_in_source.is_none() {
                return Ok(0i64); // member not in source
            }

            // Fast path: source == destination. The member already exists
            // in the "destination" (same set), so this is a no-op that
            // returns 1. Without this check, the clear-then-read-your-writes
            // sequence would corrupt the cardinality.
            if source == destination {
                return Ok(1i64);
            }

            // --- Destination key ---
            let raw_dst_meta = ObjectMeta::read(&tr, &dirs, &destination, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let live_dst_meta = raw_dst_meta.as_ref().filter(|m| !m.is_expired(now));

            if let Some(m) = live_dst_meta
                && m.key_type != KeyType::Set
            {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }

            // Clean up expired/wrong-type destination data.
            if let Some(old_meta) = &raw_dst_meta
                && (old_meta.is_expired(now) || old_meta.key_type != KeyType::Set)
            {
                helpers::delete_all_data_and_meta(&tr, &dirs, &destination, old_meta).map_err(helpers::cmd_err)?;
            }

            let dst_cardinality = live_dst_meta.map_or(0, |m| m.cardinality);
            let dst_expires_at_ms = live_dst_meta.map_or(0, |m| m.expires_at_ms);

            // Remove from source.
            tr.clear(&src_fdb_key);
            let new_src_cardinality = src_meta.cardinality.saturating_sub(1);

            if new_src_cardinality == 0 {
                helpers::delete_all_data_and_meta(&tr, &dirs, &source, &src_meta).map_err(helpers::cmd_err)?;
            } else {
                let mut new_src_meta = ObjectMeta::new_set(new_src_cardinality);
                new_src_meta.expires_at_ms = src_meta.expires_at_ms;
                new_src_meta.write(&tr, &dirs, &source).map_err(helpers::storage_err)?;
            }

            // Add to destination.
            let dst_fdb_key = dirs.set.pack(&(destination.as_ref(), member.as_ref()));
            let already_in_dst = tr
                .get(&dst_fdb_key, false)
                .await
                .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

            tr.set(&dst_fdb_key, b"");

            let new_dst_cardinality = if already_in_dst.is_none() {
                dst_cardinality + 1
            } else {
                dst_cardinality
            };

            let mut new_dst_meta = ObjectMeta::new_set(new_dst_cardinality);
            new_dst_meta.expires_at_ms = dst_expires_at_ms;
            new_dst_meta
                .write(&tr, &dirs, &destination)
                .map_err(helpers::storage_err)?;

            Ok(1i64)
        }
    })
    .await
    {
        Ok(moved) => RespValue::Integer(moved),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// Multi-set operations: SINTER, SUNION, SDIFF
// ---------------------------------------------------------------------------

/// Compute a multi-set operation by loading all sets into memory and
/// applying `op`. Returns the resulting member list.
///
/// All sets are read within the same FDB transaction for consistency.
async fn multi_set_op<F>(
    args: &[Bytes],
    state: &ConnectionState,
    operation: &'static str,
    op: F,
) -> Result<Vec<Vec<u8>>, crate::error::StorageError>
where
    F: Fn(Vec<HashSet<Vec<u8>>>) -> Vec<Vec<u8>> + Send + Clone + 'static,
{
    run_transact(&state.db, state.shared_txn(), operation, |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        let op = op.clone();
        async move {
            let now = helpers::now_ms();
            let mut sets: Vec<HashSet<Vec<u8>>> = Vec::with_capacity(args.len());

            for set_key in &args {
                let meta = ObjectMeta::read(&tr, &dirs, set_key, now, false)
                    .await
                    .map_err(helpers::storage_err)?;

                match meta {
                    None => {
                        sets.push(HashSet::new());
                    }
                    Some(m) => {
                        if m.key_type != KeyType::Set {
                            return Err(helpers::cmd_err(CommandError::WrongType));
                        }
                        let members = read_set_members(&tr, &dirs, set_key).await.map_err(helpers::cmd_err)?;
                        sets.push(members.into_iter().collect());
                    }
                }
            }

            Ok(op(sets))
        }
    })
    .await
}

/// Set intersection: members present in ALL sets.
fn intersect_sets(sets: Vec<HashSet<Vec<u8>>>) -> Vec<Vec<u8>> {
    if sets.is_empty() {
        return vec![];
    }

    // Start with the smallest set for efficiency.
    let min_idx = sets
        .iter()
        .enumerate()
        .min_by_key(|(_, s)| s.len())
        .map(|(i, _)| i)
        .unwrap();

    let mut result: HashSet<Vec<u8>> = sets[min_idx].clone();

    for (i, set) in sets.iter().enumerate() {
        if i == min_idx {
            continue;
        }
        result.retain(|m| set.contains(m));
        if result.is_empty() {
            break; // short-circuit
        }
    }

    result.into_iter().collect()
}

/// Set union: members present in ANY set.
fn union_sets(sets: Vec<HashSet<Vec<u8>>>) -> Vec<Vec<u8>> {
    let mut result = HashSet::new();
    for set in sets {
        result.extend(set);
    }
    result.into_iter().collect()
}

/// Set difference: members in first set but not in any other set.
fn diff_sets(sets: Vec<HashSet<Vec<u8>>>) -> Vec<Vec<u8>> {
    if sets.is_empty() {
        return vec![];
    }

    let mut result = sets[0].clone();
    for set in &sets[1..] {
        result.retain(|m| !set.contains(m));
        if result.is_empty() {
            break;
        }
    }

    result.into_iter().collect()
}

// ---------------------------------------------------------------------------
// SINTER key [key ...]
// ---------------------------------------------------------------------------

/// SINTER key [key ...] — Return the intersection of all given sets.
pub async fn handle_sinter(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "SINTER".into() }.to_string());
    }

    match multi_set_op(args, state, "SINTER", intersect_sets).await {
        Ok(members) => {
            let elements: Vec<RespValue> = members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SUNION key [key ...]
// ---------------------------------------------------------------------------

/// SUNION key [key ...] — Return the union of all given sets.
pub async fn handle_sunion(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "SUNION".into() }.to_string());
    }

    match multi_set_op(args, state, "SUNION", union_sets).await {
        Ok(members) => {
            let elements: Vec<RespValue> = members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SDIFF key [key ...]
// ---------------------------------------------------------------------------

/// SDIFF key [key ...] — Return the difference of the first set from others.
pub async fn handle_sdiff(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "SDIFF".into() }.to_string());
    }

    match multi_set_op(args, state, "SDIFF", diff_sets).await {
        Ok(members) => {
            let elements: Vec<RespValue> = members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                .collect();
            RespValue::Array(Some(elements))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// Store variants: SINTERSTORE, SUNIONSTORE, SDIFFSTORE
// ---------------------------------------------------------------------------

/// Common implementation for *STORE commands.
///
/// Computes a set operation on the source keys and writes the result
/// to the destination key. Returns the cardinality of the result set.
async fn store_set_op<F>(
    args: &[Bytes],
    state: &ConnectionState,
    cmd_name: &'static str,
    operation: &'static str,
    op: F,
) -> RespValue
where
    F: Fn(Vec<HashSet<Vec<u8>>>) -> Vec<Vec<u8>> + Send + Clone + 'static,
{
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: cmd_name.into() }.to_string());
    }

    let destination = args[0].clone();
    let source_keys = args[1..].to_vec();

    match run_transact(&state.db, state.shared_txn(), operation, |tr| {
        let dirs = state.dirs.clone();
        let dest = destination.clone();
        let sources = source_keys.clone();
        let op = op.clone();
        async move {
            let now = helpers::now_ms();

            // Load all source sets.
            let mut sets: Vec<HashSet<Vec<u8>>> = Vec::with_capacity(sources.len());
            for set_key in &sources {
                let meta = ObjectMeta::read(&tr, &dirs, set_key, now, false)
                    .await
                    .map_err(helpers::storage_err)?;

                match meta {
                    None => sets.push(HashSet::new()),
                    Some(m) => {
                        if m.key_type != KeyType::Set {
                            return Err(helpers::cmd_err(CommandError::WrongType));
                        }
                        let members = read_set_members(&tr, &dirs, set_key).await.map_err(helpers::cmd_err)?;
                        sets.push(members.into_iter().collect());
                    }
                }
            }

            let result_members = op(sets);
            let result_count = result_members.len() as i64;

            // Delete existing destination key (any type).
            let dst_raw_meta = ObjectMeta::read(&tr, &dirs, &dest, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            if let Some(old_meta) = &dst_raw_meta {
                helpers::delete_all_data_and_meta(&tr, &dirs, &dest, old_meta).map_err(helpers::cmd_err)?;
            }

            // Write result members to destination.
            if !result_members.is_empty() {
                for member in &result_members {
                    let fdb_key = dirs.set.pack(&(dest.as_ref(), member.as_slice()));
                    tr.set(&fdb_key, b"");
                }

                let meta = ObjectMeta::new_set(result_members.len() as u64);
                meta.write(&tr, &dirs, &dest).map_err(helpers::storage_err)?;
            }

            Ok(result_count)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

/// SINTERSTORE destination key [key ...] — Store intersection result.
pub async fn handle_sinterstore(args: &[Bytes], state: &ConnectionState) -> RespValue {
    store_set_op(args, state, "SINTERSTORE", "SINTERSTORE", intersect_sets).await
}

/// SUNIONSTORE destination key [key ...] — Store union result.
pub async fn handle_sunionstore(args: &[Bytes], state: &ConnectionState) -> RespValue {
    store_set_op(args, state, "SUNIONSTORE", "SUNIONSTORE", union_sets).await
}

/// SDIFFSTORE destination key [key ...] — Store difference result.
pub async fn handle_sdiffstore(args: &[Bytes], state: &ConnectionState) -> RespValue {
    store_set_op(args, state, "SDIFFSTORE", "SDIFFSTORE", diff_sets).await
}

// ---------------------------------------------------------------------------
// SINTERCARD numkeys key [key ...] [LIMIT limit]
// ---------------------------------------------------------------------------

/// SINTERCARD numkeys key [key ...] [LIMIT limit] — Return cardinality
/// of the intersection, optionally capped at LIMIT.
///
/// This command can short-circuit when the LIMIT is reached without
/// materializing the full intersection.
pub async fn handle_sintercard(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "SINTERCARD".into(),
            }
            .to_string(),
        );
    }

    // Parse numkeys.
    let numkeys = match parse_i64_arg(&args[0]) {
        Ok(v) if v <= 0 => {
            return RespValue::err("ERR numkeys can't be non-positive value");
        }
        Ok(v) => v as usize,
        Err(resp) => return resp,
    };

    // We need at least numkeys + 1 args (numkeys value + the keys themselves).
    let remaining = &args[1..];
    if remaining.len() < numkeys {
        return RespValue::err(
            CommandError::WrongArity {
                name: "SINTERCARD".into(),
            }
            .to_string(),
        );
    }

    let set_keys = &remaining[..numkeys];
    let extra = &remaining[numkeys..];

    // Parse optional LIMIT.
    let limit: u64 = if extra.is_empty() {
        0 // 0 means no limit
    } else if extra.len() == 2 {
        let flag = extra[0].to_ascii_uppercase();
        if flag.as_slice() != b"LIMIT" {
            return RespValue::err("ERR syntax error");
        }
        match parse_i64_arg(&extra[1]) {
            Ok(v) if v < 0 => {
                return RespValue::err("ERR LIMIT can't be negative");
            }
            Ok(v) => v as u64,
            Err(resp) => return resp,
        }
    } else {
        return RespValue::err("ERR syntax error");
    };

    match run_transact(&state.db, state.shared_txn(), "SINTERCARD", |tr| {
        let dirs = state.dirs.clone();
        let keys: Vec<Bytes> = set_keys.to_vec();
        async move {
            let now = helpers::now_ms();

            // Load all sets and find the smallest for iteration order.
            let mut sets: Vec<HashSet<Vec<u8>>> = Vec::with_capacity(keys.len());
            for set_key in &keys {
                let meta = ObjectMeta::read(&tr, &dirs, set_key, now, false)
                    .await
                    .map_err(helpers::storage_err)?;

                match meta {
                    None => {
                        // One empty set means intersection is empty.
                        return Ok(0i64);
                    }
                    Some(m) => {
                        if m.key_type != KeyType::Set {
                            return Err(helpers::cmd_err(CommandError::WrongType));
                        }
                        // Short-circuit on empty set (avoids reading all members
                        // from FDB when meta already tells us the set is empty).
                        if m.cardinality == 0 {
                            return Ok(0i64);
                        }
                        let members = read_set_members(&tr, &dirs, set_key).await.map_err(helpers::cmd_err)?;
                        if members.is_empty() {
                            return Ok(0i64);
                        }
                        sets.push(members.into_iter().collect());
                    }
                }
            }

            if sets.is_empty() {
                return Ok(0i64);
            }

            // Iterate over the smallest set and check membership in all others.
            let min_idx = sets
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.len())
                .map(|(i, _)| i)
                .unwrap();

            let mut count: u64 = 0;
            for member in &sets[min_idx] {
                let in_all = sets.iter().enumerate().all(|(i, s)| i == min_idx || s.contains(member));
                if in_all {
                    count += 1;
                    if limit > 0 && count >= limit {
                        break;
                    }
                }
            }

            Ok(count as i64)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
