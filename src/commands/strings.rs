//! String command handlers: GET, SET, DEL, EXISTS, SETNX, SETEX, PSETEX, GETDEL,
//! MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN,
//! GETRANGE, SETRANGE.
//!
//! Each handler validates arguments, runs an FDB transaction via
//! `run_transact`, and returns a `RespValue` response.

use bytes::Bytes;
use futures::future::join_all;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::{helpers, run_transact};

// ---------------------------------------------------------------------------
// GET key
// ---------------------------------------------------------------------------

/// GET key -- Returns the string value of `key`, or nil if the key does not exist.
pub async fn handle_get(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "GET".into() }.to_string());
    }
    let key = args[0].to_vec();

    match run_transact(&state.db, "GET", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        async move {
            let now = helpers::now_ms();
            helpers::get_string(&tr, &dirs, &k, now).await.map_err(helpers::cmd_err)
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
// SET key value [NX|XX] [GET] [EX s|PX ms|EXAT s|PXAT ms] [KEEPTTL]
// ---------------------------------------------------------------------------

/// Parsed flags for the SET command.
#[derive(Debug, Clone)]
struct SetFlags {
    /// NX: only set if key does NOT exist.
    nx: bool,
    /// XX: only set if key DOES exist.
    xx: bool,
    /// GET: return the old value.
    get: bool,
    /// KEEPTTL: preserve existing TTL.
    keepttl: bool,
    /// Raw expiry specification — resolved to an absolute timestamp inside the
    /// transaction closure so that retries recompute relative offsets.
    expiry: ExpirySpec,
}

/// Raw expiry specification parsed from SET flags. Absolute timestamps are
/// stored directly; relative durations are stored as-is and resolved inside
/// the transaction closure via `now_ms()`.
#[derive(Debug, Clone)]
enum ExpirySpec {
    /// No expiry requested.
    None,
    /// EX <seconds> — relative.
    ExSeconds(u64),
    /// PX <milliseconds> — relative.
    PxMillis(u64),
    /// EXAT <seconds> — absolute.
    ExatSeconds(u64),
    /// PXAT <milliseconds> — absolute.
    PxatMillis(u64),
}

impl ExpirySpec {
    /// Resolve to an absolute expiry timestamp in milliseconds, calling
    /// `now_ms()` for relative specs. Returns 0 when no expiry is set.
    fn resolve(&self) -> u64 {
        match self {
            ExpirySpec::None => 0,
            ExpirySpec::ExSeconds(s) => helpers::now_ms() + s * 1000,
            ExpirySpec::PxMillis(ms) => helpers::now_ms() + ms,
            ExpirySpec::ExatSeconds(s) => s * 1000,
            ExpirySpec::PxatMillis(ms) => *ms,
        }
    }
}

/// Parse SET flags from args starting at index 2 (after key and value).
fn parse_set_flags(args: &[Bytes]) -> Result<SetFlags, RespValue> {
    let mut flags = SetFlags {
        nx: false,
        xx: false,
        get: false,
        keepttl: false,
        expiry: ExpirySpec::None,
    };
    let mut has_expiry = false;
    let mut i = 2; // skip key and value

    while i < args.len() {
        let flag = args[i].to_ascii_uppercase();
        match flag.as_slice() {
            b"NX" => {
                if flags.xx {
                    return Err(RespValue::err("ERR syntax error"));
                }
                flags.nx = true;
                i += 1;
            }
            b"XX" => {
                if flags.nx {
                    return Err(RespValue::err("ERR syntax error"));
                }
                flags.xx = true;
                i += 1;
            }
            b"GET" => {
                flags.get = true;
                i += 1;
            }
            b"KEEPTTL" => {
                if has_expiry {
                    return Err(RespValue::err("ERR syntax error"));
                }
                flags.keepttl = true;
                i += 1;
            }
            b"EX" => {
                if has_expiry || flags.keepttl {
                    return Err(RespValue::err("ERR syntax error"));
                }
                i += 1;
                if i >= args.len() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                let secs = parse_positive_i64(&args[i], "set")?;
                flags.expiry = ExpirySpec::ExSeconds(secs as u64);
                has_expiry = true;
                i += 1;
            }
            b"PX" => {
                if has_expiry || flags.keepttl {
                    return Err(RespValue::err("ERR syntax error"));
                }
                i += 1;
                if i >= args.len() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                let ms = parse_positive_i64(&args[i], "set")?;
                flags.expiry = ExpirySpec::PxMillis(ms as u64);
                has_expiry = true;
                i += 1;
            }
            b"EXAT" => {
                if has_expiry || flags.keepttl {
                    return Err(RespValue::err("ERR syntax error"));
                }
                i += 1;
                if i >= args.len() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                let secs = parse_positive_i64(&args[i], "set")?;
                flags.expiry = ExpirySpec::ExatSeconds(secs as u64);
                has_expiry = true;
                i += 1;
            }
            b"PXAT" => {
                if has_expiry || flags.keepttl {
                    return Err(RespValue::err("ERR syntax error"));
                }
                i += 1;
                if i >= args.len() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                let ms = parse_positive_i64(&args[i], "set")?;
                flags.expiry = ExpirySpec::PxatMillis(ms as u64);
                has_expiry = true;
                i += 1;
            }
            _ => {
                return Err(RespValue::err("ERR syntax error"));
            }
        }
    }

    Ok(flags)
}

/// Parse a byte slice as a positive i64 (> 0). Returns an error
/// response if the value is not a valid integer or is <= 0.
///
/// Redis distinguishes two error cases:
/// - Non-integer input → `ERR value is not an integer or out of range`
/// - Zero or negative  → `ERR invalid expire time in '<cmd>' command`
fn parse_positive_i64(arg: &Bytes, cmd_name: &str) -> Result<i64, RespValue> {
    let s = std::str::from_utf8(arg).map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
    let val: i64 = s
        .parse()
        .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
    if val <= 0 {
        return Err(RespValue::err(format!(
            "ERR invalid expire time in '{cmd_name}' command"
        )));
    }
    Ok(val)
}

/// SET key value [NX|XX] [GET] [EX s|PX ms|EXAT s|PXAT ms] [KEEPTTL] --
/// Sets the string value of `key`. Supports conditional set, TTL, and old-value return.
pub async fn handle_set(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "SET".into() }.to_string());
    }

    let flags = match parse_set_flags(args) {
        Ok(f) => f,
        Err(resp) => return resp,
    };

    let key = args[0].to_vec();
    let value = args[1].to_vec();

    match run_transact(&state.db, "SET", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let v = value.clone();
        let f = flags.clone();
        async move {
            let now = helpers::now_ms();

            // Read existing meta (pass now_ms=0 to see expired keys for KEEPTTL).
            // We need the raw meta for KEEPTTL even if expired.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;

            // Determine if the key is "live" (not expired).
            let live_meta = match &raw_meta {
                Some(m) if !m.is_expired(now) => Some(m.clone()),
                _ => None,
            };

            // GET: read old value before any NX/XX short-circuit, so that
            // SET k v NX GET returns the existing value even when NX prevents
            // the write. WRONGTYPE check applies regardless.
            let old_value = if f.get {
                match &live_meta {
                    Some(m) if m.key_type != KeyType::String => {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    Some(m) => {
                        let data = crate::storage::chunking::read_chunks(
                            &tr,
                            &dirs.obj,
                            &k,
                            m.num_chunks,
                            m.size_bytes,
                            false,
                        )
                        .await
                        .map_err(helpers::storage_err)?;
                        Some(data)
                    }
                    None => None,
                }
            } else {
                None
            };

            // NX: only set if key does NOT exist (live).
            if f.nx && live_meta.is_some() {
                return Ok(SetResult::WithOldValue(old_value));
            }

            // XX: only set if key DOES exist (live).
            if f.xx && live_meta.is_none() {
                return Ok(SetResult::NotSet);
            }

            // Determine expiry for the new key. Resolve relative specs
            // inside the transaction so retries recompute from current time.
            let expires_at_ms = if f.keepttl {
                // Preserve existing TTL from the live meta.
                live_meta.as_ref().map_or(0, |m| m.expires_at_ms)
            } else {
                f.expiry.resolve()
            };

            // Write the new string value. Pass the raw_meta (even if
            // expired) so write_string can clean up old data.
            helpers::write_string(&tr, &dirs, &k, &v, expires_at_ms, raw_meta.as_ref()).map_err(helpers::cmd_err)?;

            if f.get {
                Ok(SetResult::WithOldValue(old_value))
            } else {
                Ok(SetResult::Ok)
            }
        }
    })
    .await
    {
        Ok(SetResult::Ok) => RespValue::ok(),
        Ok(SetResult::NotSet) => RespValue::BulkString(None),
        Ok(SetResult::WithOldValue(Some(data))) => RespValue::BulkString(Some(Bytes::from(data))),
        Ok(SetResult::WithOldValue(None)) => RespValue::BulkString(None),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

/// Internal result type for SET to distinguish between OK, not-set (NX/XX),
/// and GET-with-old-value responses.
#[derive(Debug)]
enum SetResult {
    Ok,
    NotSet,
    WithOldValue(Option<Vec<u8>>),
}

// ---------------------------------------------------------------------------
// DEL key [key ...]
// ---------------------------------------------------------------------------

/// DEL key [key ...] -- Removes the specified keys. Returns the number of keys deleted.
pub async fn handle_del(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "DEL".into() }.to_string());
    }

    let keys: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();

    match run_transact(&state.db, "DEL", |tr| {
        let dirs = state.dirs.clone();
        let ks = keys.clone();
        async move {
            let now = helpers::now_ms();
            let mut count: i64 = 0;
            for k in &ks {
                let deleted = helpers::delete_object(&tr, &dirs, k, now)
                    .await
                    .map_err(helpers::cmd_err)?;
                if deleted {
                    count += 1;
                }
            }
            Ok(count)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// EXISTS key [key ...]
// ---------------------------------------------------------------------------

/// EXISTS key [key ...] -- Returns the number of specified keys that exist.
pub async fn handle_exists(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "EXISTS".into() }.to_string());
    }

    let keys: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();

    match run_transact(&state.db, "EXISTS", |tr| {
        let dirs = state.dirs.clone();
        let ks = keys.clone();
        async move {
            let now = helpers::now_ms();
            let mut count: i64 = 0;
            for k in &ks {
                let meta = ObjectMeta::read(&tr, &dirs, k, now, false)
                    .await
                    .map_err(helpers::storage_err)?;
                if meta.is_some() {
                    count += 1;
                }
            }
            Ok(count)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SETNX key value
// ---------------------------------------------------------------------------

/// SETNX key value -- Sets `key` only if it does not already exist. Returns 1 if set, 0 otherwise.
pub async fn handle_setnx(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "SETNX".into() }.to_string());
    }

    let key = args[0].to_vec();
    let value = args[1].to_vec();

    match run_transact(&state.db, "SETNX", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let v = value.clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &k, now, false)
                .await
                .map_err(helpers::storage_err)?;

            if meta.is_some() {
                return Ok(0i64);
            }

            helpers::write_string(&tr, &dirs, &k, &v, 0, None).map_err(helpers::cmd_err)?;
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
// SETEX key seconds value
// ---------------------------------------------------------------------------

/// SETEX key seconds value -- Sets the value and expiration (in seconds) of `key`.
pub async fn handle_setex(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "SETEX".into() }.to_string());
    }

    let seconds = match parse_positive_i64(&args[1], "setex") {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let key = args[0].to_vec();
    let value = args[2].to_vec();
    let secs = seconds as u64;

    match run_transact(&state.db, "SETEX", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let v = value.clone();
        async move {
            let expires_at_ms = helpers::now_ms() + secs * 1000;
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;

            helpers::write_string(&tr, &dirs, &k, &v, expires_at_ms, raw_meta.as_ref()).map_err(helpers::cmd_err)?;
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
// PSETEX key milliseconds value
// ---------------------------------------------------------------------------

/// PSETEX key milliseconds value -- Sets the value and expiration (in milliseconds) of `key`.
pub async fn handle_psetex(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "PSETEX".into() }.to_string());
    }

    let ms = match parse_positive_i64(&args[1], "psetex") {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let key = args[0].to_vec();
    let value = args[2].to_vec();
    let millis = ms as u64;

    match run_transact(&state.db, "PSETEX", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let v = value.clone();
        async move {
            let expires_at_ms = helpers::now_ms() + millis;
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;

            helpers::write_string(&tr, &dirs, &k, &v, expires_at_ms, raw_meta.as_ref()).map_err(helpers::cmd_err)?;
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
// GETDEL key
// ---------------------------------------------------------------------------

/// GETDEL key -- Returns the string value of `key` and deletes it. Returns nil if the key does not exist.
pub async fn handle_getdel(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "GETDEL".into() }.to_string());
    }

    let key = args[0].to_vec();

    match run_transact(&state.db, "GETDEL", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        async move {
            let now = helpers::now_ms();

            // Read the string value first.
            let data = helpers::get_string(&tr, &dirs, &k, now)
                .await
                .map_err(helpers::cmd_err)?;

            // If the key existed, delete it.
            if data.is_some() {
                helpers::delete_object(&tr, &dirs, &k, now)
                    .await
                    .map_err(helpers::cmd_err)?;
            }

            Ok(data)
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
// MGET key [key ...]
// ---------------------------------------------------------------------------

/// MGET key [key ...] -- Returns the values of all specified keys (nil for missing/non-string keys).
pub async fn handle_mget(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "MGET".into() }.to_string());
    }

    let keys: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();

    match run_transact(&state.db, "MGET", |tr| {
        let dirs = state.dirs.clone();
        let ks = keys.clone();
        async move {
            let now = helpers::now_ms();

            // Read all metas in parallel.
            let meta_futures: Vec<_> = ks.iter().map(|k| ObjectMeta::read(&tr, &dirs, k, now, false)).collect();
            let metas: Vec<Result<Option<ObjectMeta>, _>> = join_all(meta_futures).await;

            // Collect keys that need chunk reads, fire those in parallel.
            let mut chunk_futures = Vec::new();
            let mut chunk_indices = Vec::new();
            let mut results: Vec<Option<Vec<u8>>> = vec![None; ks.len()];

            for (i, meta_result) in metas.into_iter().enumerate() {
                let meta = meta_result.map_err(helpers::storage_err)?;
                // MGET returns nil for non-string keys (no WRONGTYPE error).
                if let Some(m) = meta
                    && m.key_type == KeyType::String
                {
                    chunk_indices.push(i);
                    chunk_futures.push(crate::storage::chunking::read_chunks(
                        &tr,
                        &dirs.obj,
                        &ks[i],
                        m.num_chunks,
                        m.size_bytes,
                        false,
                    ));
                }
            }

            let chunk_results = join_all(chunk_futures).await;
            for (idx, chunk_result) in chunk_indices.into_iter().zip(chunk_results) {
                let data = chunk_result.map_err(helpers::storage_err)?;
                results[idx] = Some(data);
            }

            Ok(results)
        }
    })
    .await
    {
        Ok(results) => {
            let arr: Vec<RespValue> = results
                .into_iter()
                .map(|opt| match opt {
                    Some(data) => RespValue::BulkString(Some(Bytes::from(data))),
                    None => RespValue::BulkString(None),
                })
                .collect();
            RespValue::Array(Some(arr))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// MSET key value [key value ...]
// ---------------------------------------------------------------------------

/// MSET key value [key value ...] -- Sets multiple keys to their respective values atomically.
pub async fn handle_mset(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return RespValue::err(CommandError::WrongArity { name: "MSET".into() }.to_string());
    }

    let pairs: Vec<(Vec<u8>, Vec<u8>)> = args
        .chunks(2)
        .map(|chunk| (chunk[0].to_vec(), chunk[1].to_vec()))
        .collect();

    match run_transact(&state.db, "MSET", |tr| {
        let dirs = state.dirs.clone();
        let ps = pairs.clone();
        async move {
            for (k, v) in &ps {
                let raw_meta = ObjectMeta::read(&tr, &dirs, k, 0, false)
                    .await
                    .map_err(helpers::storage_err)?;

                helpers::write_string(&tr, &dirs, k, v, 0, raw_meta.as_ref()).map_err(helpers::cmd_err)?;
            }
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
// INCR / DECR / INCRBY / DECRBY — shared helper
// ---------------------------------------------------------------------------

/// Parse a byte slice as an i64.
fn parse_i64_arg(arg: &Bytes) -> Result<i64, RespValue> {
    let s = std::str::from_utf8(arg).map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
    s.parse::<i64>()
        .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))
}

/// Shared implementation for INCR, DECR, INCRBY, DECRBY.
///
/// Reads the current value (defaulting to "0" if the key does not exist),
/// adds `delta`, writes back as a decimal string, and returns the new value.
/// Preserves existing TTL.
async fn incr_by_impl(key: Vec<u8>, delta: i64, state: &ConnectionState, cmd_name: &str) -> RespValue {
    match run_transact(&state.db, cmd_name, |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        async move {
            let now = helpers::now_ms();

            // Single read with now_ms=0 to get raw meta (even if expired),
            // then determine liveness from that.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let meta = match &raw_meta {
                Some(m) if !m.is_expired(now) => Some(m),
                _ => None,
            };

            let (current_bytes, expires_at_ms) = match meta {
                Some(m) => {
                    if m.key_type != KeyType::String {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let data =
                        crate::storage::chunking::read_chunks(&tr, &dirs.obj, &k, m.num_chunks, m.size_bytes, false)
                            .await
                            .map_err(helpers::storage_err)?;
                    (data, m.expires_at_ms)
                }
                None => (b"0".to_vec(), 0u64),
            };

            let current_str = std::str::from_utf8(&current_bytes).map_err(|_| {
                helpers::cmd_err(CommandError::Generic("value is not an integer or out of range".into()))
            })?;

            let current_val: i64 = current_str.parse().map_err(|_| {
                helpers::cmd_err(CommandError::Generic("value is not an integer or out of range".into()))
            })?;

            let new_val = current_val.checked_add(delta).ok_or_else(|| {
                helpers::cmd_err(CommandError::Generic("increment or decrement would overflow".into()))
            })?;

            // Format using itoa for speed.
            let mut buf = itoa::Buffer::new();
            let new_str = buf.format(new_val);

            helpers::write_string(&tr, &dirs, &k, new_str.as_bytes(), expires_at_ms, raw_meta.as_ref())
                .map_err(helpers::cmd_err)?;

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
// INCR key
// ---------------------------------------------------------------------------

/// INCR key -- Increments the integer value of `key` by one. Creates the key with value 0 if absent.
pub async fn handle_incr(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "INCR".into() }.to_string());
    }
    let key = args[0].to_vec();
    incr_by_impl(key, 1, state, "INCR").await
}

// ---------------------------------------------------------------------------
// DECR key
// ---------------------------------------------------------------------------

/// DECR key -- Decrements the integer value of `key` by one. Creates the key with value 0 if absent.
pub async fn handle_decr(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "DECR".into() }.to_string());
    }
    let key = args[0].to_vec();
    incr_by_impl(key, -1, state, "DECR").await
}

// ---------------------------------------------------------------------------
// INCRBY key increment
// ---------------------------------------------------------------------------

/// INCRBY key increment -- Increments the integer value of `key` by `increment`.
pub async fn handle_incrby(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "INCRBY".into() }.to_string());
    }
    let key = args[0].to_vec();
    let delta = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    incr_by_impl(key, delta, state, "INCRBY").await
}

// ---------------------------------------------------------------------------
// DECRBY key decrement
// ---------------------------------------------------------------------------

/// DECRBY key decrement -- Decrements the integer value of `key` by `decrement`.
pub async fn handle_decrby(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "DECRBY".into() }.to_string());
    }
    let key = args[0].to_vec();
    let delta = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    // DECRBY key N == INCRBY key -N
    let neg_delta = match delta.checked_neg() {
        Some(v) => v,
        None => {
            return RespValue::err("ERR increment or decrement would overflow");
        }
    };
    incr_by_impl(key, neg_delta, state, "DECRBY").await
}

// ---------------------------------------------------------------------------
// INCRBYFLOAT key increment
// ---------------------------------------------------------------------------

/// Format a float following Redis INCRBYFLOAT rules:
/// - If the result is an exact integer that fits in i64, format without decimal point.
/// - Otherwise, use minimal precision and strip trailing zeros.
fn format_redis_float(val: f64) -> String {
    if val.fract() == 0.0 && val.is_finite() && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
        // Integer result that fits in i64 — format without decimal.
        format!("{}", val as i64)
    } else {
        // Use ryu for fast float-to-string, then strip trailing zeros.
        let mut s = ryu::Buffer::new().format(val).to_string();
        if s.contains('.') {
            let trimmed = s.trim_end_matches('0');
            let trimmed = trimmed.trim_end_matches('.');
            s.truncate(trimmed.len());
        }
        s
    }
}

/// INCRBYFLOAT key increment -- Increments the float value of `key` by `increment`.
pub async fn handle_incrbyfloat(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "INCRBYFLOAT".into(),
            }
            .to_string(),
        );
    }

    let key = args[0].to_vec();
    let incr_str = match std::str::from_utf8(&args[1]) {
        Ok(s) => s.to_owned(),
        Err(_) => {
            return RespValue::err("ERR value is not a valid float");
        }
    };

    let increment: f64 = match incr_str.parse() {
        Ok(v) => v,
        Err(_) => {
            return RespValue::err("ERR value is not a valid float");
        }
    };

    if increment.is_nan() || increment.is_infinite() {
        return RespValue::err("ERR increment would produce NaN or Infinity");
    }

    match run_transact(&state.db, "INCRBYFLOAT", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let inc = increment;
        async move {
            let now = helpers::now_ms();

            // Single read with now_ms=0 to get raw meta (even if expired),
            // then determine liveness from that.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let meta = match &raw_meta {
                Some(m) if !m.is_expired(now) => Some(m),
                _ => None,
            };

            let (current_bytes, expires_at_ms) = match meta {
                Some(m) => {
                    if m.key_type != KeyType::String {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let data =
                        crate::storage::chunking::read_chunks(&tr, &dirs.obj, &k, m.num_chunks, m.size_bytes, false)
                            .await
                            .map_err(helpers::storage_err)?;
                    (data, m.expires_at_ms)
                }
                None => (b"0".to_vec(), 0u64),
            };

            let current_str = std::str::from_utf8(&current_bytes)
                .map_err(|_| helpers::cmd_err(CommandError::Generic("value is not a valid float".into())))?;

            let current_val: f64 = current_str
                .parse()
                .map_err(|_| helpers::cmd_err(CommandError::Generic("value is not a valid float".into())))?;

            let result = current_val + inc;
            if result.is_nan() || result.is_infinite() {
                return Err(helpers::cmd_err(CommandError::Generic(
                    "increment would produce NaN or Infinity".into(),
                )));
            }

            let formatted = format_redis_float(result);

            helpers::write_string(&tr, &dirs, &k, formatted.as_bytes(), expires_at_ms, raw_meta.as_ref())
                .map_err(helpers::cmd_err)?;

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
// APPEND key value
// ---------------------------------------------------------------------------

/// APPEND key value -- Appends `value` to the string at `key`. Returns the new string length.
pub async fn handle_append(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "APPEND".into() }.to_string());
    }

    let key = args[0].to_vec();
    let append_val = args[1].to_vec();

    match run_transact(&state.db, "APPEND", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let av = append_val.clone();
        async move {
            let now = helpers::now_ms();

            // Single read with now_ms=0 to get raw meta (even if expired),
            // then determine liveness from that.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let meta = match &raw_meta {
                Some(m) if !m.is_expired(now) => Some(m),
                _ => None,
            };

            let (mut current, expires_at_ms) = match meta {
                Some(m) => {
                    if m.key_type != KeyType::String {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let data =
                        crate::storage::chunking::read_chunks(&tr, &dirs.obj, &k, m.num_chunks, m.size_bytes, false)
                            .await
                            .map_err(helpers::storage_err)?;
                    (data, m.expires_at_ms)
                }
                None => (Vec::new(), 0u64),
            };

            current.extend_from_slice(&av);
            let new_len = current.len() as i64;

            helpers::write_string(&tr, &dirs, &k, &current, expires_at_ms, raw_meta.as_ref())
                .map_err(helpers::cmd_err)?;

            Ok(new_len)
        }
    })
    .await
    {
        Ok(len) => RespValue::Integer(len),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// STRLEN key
// ---------------------------------------------------------------------------

/// STRLEN key -- Returns the length of the string value stored at `key`, or 0 if absent.
pub async fn handle_strlen(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "STRLEN".into() }.to_string());
    }

    let key = args[0].to_vec();

    match run_transact(&state.db, "STRLEN", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &k, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                Some(m) => {
                    if m.key_type != KeyType::String {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    Ok(m.size_bytes as i64)
                }
                None => Ok(0i64),
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
// GETRANGE key start end
// ---------------------------------------------------------------------------

/// GETRANGE key start end -- Returns the substring of the string at `key` between `start` and `end` (inclusive).
pub async fn handle_getrange(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "GETRANGE".into(),
            }
            .to_string(),
        );
    }

    let key = args[0].to_vec();

    let start = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let end = match parse_i64_arg(&args[2]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, "GETRANGE", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        async move {
            let now = helpers::now_ms();
            let data = helpers::get_string(&tr, &dirs, &k, now)
                .await
                .map_err(helpers::cmd_err)?;

            let value = data.unwrap_or_default();
            let len = value.len() as i64;

            if len == 0 {
                return Ok(Vec::new());
            }

            // Resolve negative indices.
            let mut s = if start < 0 { len + start } else { start };
            let mut e = if end < 0 { len + end } else { end };

            // Clamp.
            if s < 0 {
                s = 0;
            }
            if e >= len {
                e = len - 1;
            }

            if s > e || s >= len {
                return Ok(Vec::new());
            }

            Ok(value[s as usize..=e as usize].to_vec())
        }
    })
    .await
    {
        Ok(data) => RespValue::BulkString(Some(Bytes::from(data))),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// SETRANGE key offset value
// ---------------------------------------------------------------------------

/// SETRANGE key offset value -- Overwrites part of the string at `key` starting at `offset`. Returns the new length.
pub async fn handle_setrange(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "SETRANGE".into(),
            }
            .to_string(),
        );
    }

    let key = args[0].to_vec();

    let offset = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    if offset < 0 {
        return RespValue::err("ERR offset is out of range");
    }
    let offset = offset as usize;

    let patch = args[2].to_vec();

    // Check 512MB limit.
    if offset + patch.len() > 536_870_912 {
        return RespValue::err("ERR string exceeds maximum allowed size");
    }

    match run_transact(&state.db, "SETRANGE", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let p = patch.clone();
        let off = offset;
        async move {
            let now = helpers::now_ms();

            // Single read with now_ms=0 to get raw meta (even if expired),
            // then determine liveness from that.
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;
            let meta = match &raw_meta {
                Some(m) if !m.is_expired(now) => Some(m),
                _ => None,
            };

            let (mut current, expires_at_ms) = match meta {
                Some(m) => {
                    if m.key_type != KeyType::String {
                        return Err(helpers::cmd_err(CommandError::WrongType));
                    }
                    let data =
                        crate::storage::chunking::read_chunks(&tr, &dirs.obj, &k, m.num_chunks, m.size_bytes, false)
                            .await
                            .map_err(helpers::storage_err)?;
                    (data, m.expires_at_ms)
                }
                None => (Vec::new(), 0u64),
            };

            // Zero-pad if offset > current length.
            let required_len = off + p.len();
            if required_len > current.len() {
                current.resize(required_len, 0u8);
            }

            // Overwrite bytes at [offset..offset+patch.len()].
            current[off..off + p.len()].copy_from_slice(&p);

            let new_len = current.len() as i64;

            helpers::write_string(&tr, &dirs, &k, &current, expires_at_ms, raw_meta.as_ref())
                .map_err(helpers::cmd_err)?;

            Ok(new_len)
        }
    })
    .await
    {
        Ok(len) => RespValue::Integer(len),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
