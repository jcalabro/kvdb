//! String command handlers: GET, SET, DEL, EXISTS, SETNX, SETEX, PSETEX, GETDEL.
//!
//! Each handler validates arguments, runs an FDB transaction via
//! `run_transact`, and returns a `RespValue` response.

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::{helpers, run_transact};

// ---------------------------------------------------------------------------
// GET key
// ---------------------------------------------------------------------------

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
    /// Absolute expiry timestamp in milliseconds, or 0 for none.
    expires_at_ms: u64,
}

/// Parse SET flags from args starting at index 2 (after key and value).
fn parse_set_flags(args: &[Bytes]) -> Result<SetFlags, RespValue> {
    let mut flags = SetFlags {
        nx: false,
        xx: false,
        get: false,
        keepttl: false,
        expires_at_ms: 0,
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
                flags.expires_at_ms = helpers::now_ms() + (secs as u64) * 1000;
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
                flags.expires_at_ms = helpers::now_ms() + ms as u64;
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
                flags.expires_at_ms = (secs as u64) * 1000;
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
                flags.expires_at_ms = ms as u64;
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
fn parse_positive_i64(arg: &Bytes, cmd_name: &str) -> Result<i64, RespValue> {
    let s = std::str::from_utf8(arg)
        .map_err(|_| RespValue::err(format!("ERR invalid expire time in '{cmd_name}' command")))?;
    let val: i64 = s
        .parse()
        .map_err(|_| RespValue::err(format!("ERR invalid expire time in '{cmd_name}' command")))?;
    if val <= 0 {
        return Err(RespValue::err(format!(
            "ERR invalid expire time in '{cmd_name}' command"
        )));
    }
    Ok(val)
}

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

            // NX: only set if key does NOT exist (live).
            if f.nx && live_meta.is_some() {
                return Ok(SetResult::NotSet);
            }

            // XX: only set if key DOES exist (live).
            if f.xx && live_meta.is_none() {
                return Ok(SetResult::NotSet);
            }

            // GET: return old value. WRONGTYPE check on non-String.
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

            // Determine expiry for the new key.
            let expires_at_ms = if f.keepttl {
                // Preserve existing TTL from the live meta.
                live_meta.as_ref().map_or(0, |m| m.expires_at_ms)
            } else {
                f.expires_at_ms
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
    let expires_at_ms = helpers::now_ms() + (seconds as u64) * 1000;

    match run_transact(&state.db, "SETEX", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let v = value.clone();
        let exp = expires_at_ms;
        async move {
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;

            helpers::write_string(&tr, &dirs, &k, &v, exp, raw_meta.as_ref()).map_err(helpers::cmd_err)?;
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
    let expires_at_ms = helpers::now_ms() + ms as u64;

    match run_transact(&state.db, "PSETEX", |tr| {
        let dirs = state.dirs.clone();
        let k = key.clone();
        let v = value.clone();
        let exp = expires_at_ms;
        async move {
            let raw_meta = ObjectMeta::read(&tr, &dirs, &k, 0, false)
                .await
                .map_err(helpers::storage_err)?;

            helpers::write_string(&tr, &dirs, &k, &v, exp, raw_meta.as_ref()).map_err(helpers::cmd_err)?;
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
