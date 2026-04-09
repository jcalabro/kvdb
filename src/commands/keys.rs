//! Key management command handlers.
//!
//! **Read commands**: TTL, PTTL, EXPIRETIME, PEXPIRETIME — query TTL metadata.
//! **Write commands**: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST — modify TTL metadata.

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::ObjectMeta;
use crate::storage::{helpers, run_transact};

// ---------------------------------------------------------------------------
// TTL key
// ---------------------------------------------------------------------------

/// TTL key -- Returns the remaining time to live of a key in seconds.
///
/// Returns:
/// - -2 if the key does not exist
/// - -1 if the key exists but has no expiry
/// - positive integer: remaining seconds until expiry
pub async fn handle_ttl(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "TTL".into() }.to_string());
    }
    match run_transact(&state.db, "TTL", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => {
                    let remaining_ms = m.expires_at_ms.saturating_sub(now);
                    let remaining_secs = (remaining_ms / 1000) as i64;
                    Ok(remaining_secs)
                }
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// PTTL key
// ---------------------------------------------------------------------------

/// PTTL key -- Returns the remaining time to live of a key in milliseconds.
///
/// Returns:
/// - -2 if the key does not exist
/// - -1 if the key exists but has no expiry
/// - positive integer: remaining milliseconds until expiry
pub async fn handle_pttl(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "PTTL".into() }.to_string());
    }
    match run_transact(&state.db, "PTTL", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => {
                    let remaining_ms = m.expires_at_ms.saturating_sub(now);
                    Ok(remaining_ms as i64)
                }
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// EXPIRETIME key
// ---------------------------------------------------------------------------

/// EXPIRETIME key -- Returns the absolute Unix timestamp (in seconds) at
/// which the key will expire.
///
/// Returns:
/// - -2 if the key does not exist
/// - -1 if the key exists but has no expiry
/// - positive integer: Unix timestamp in seconds
pub async fn handle_expiretime(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "EXPIRETIME".into(),
            }
            .to_string(),
        );
    }
    match run_transact(&state.db, "EXPIRETIME", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => Ok((m.expires_at_ms / 1000) as i64),
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// PEXPIRETIME key
// ---------------------------------------------------------------------------

/// PEXPIRETIME key -- Returns the absolute Unix timestamp (in milliseconds)
/// at which the key will expire.
///
/// Returns:
/// - -2 if the key does not exist
/// - -1 if the key exists but has no expiry
/// - positive integer: Unix timestamp in milliseconds
pub async fn handle_pexpiretime(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "PEXPIRETIME".into(),
            }
            .to_string(),
        );
    }
    match run_transact(&state.db, "PEXPIRETIME", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => Ok(m.expires_at_ms as i64),
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// Helper functions for TTL write commands
// ---------------------------------------------------------------------------

/// Parse a byte slice as an i64.
fn parse_i64_arg(arg: &[u8]) -> Result<i64, CommandError> {
    let s = std::str::from_utf8(arg).map_err(|_| CommandError::ValueNotInteger {
        value: String::from_utf8_lossy(arg).into(),
    })?;
    s.parse::<i64>()
        .map_err(|_| CommandError::ValueNotInteger { value: s.to_string() })
}

/// Shared helper for the four EXPIRE variants.
///
/// Reads the key's metadata, sets the absolute expiry timestamp, then
/// updates both the meta entry and the expire directory entry.
///
/// Returns:
/// - 1 if the key exists and the expiry was set
/// - 0 if the key does not exist or has already expired
async fn set_expire(key: &[u8], expires_at_ms: u64, state: &ConnectionState, command_name: &'static str) -> RespValue {
    match run_transact(&state.db, command_name, |tr| {
        let dirs = state.dirs.clone();
        let key = Bytes::copy_from_slice(key);
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            let mut meta = match meta {
                Some(m) => m,
                None => return Ok(0i64), // key doesn't exist or already expired
            };

            // Update meta and write expire entry.
            meta.expires_at_ms = expires_at_ms;
            meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;

            let expire_key = dirs.expire_key(&key);
            tr.set(&expire_key, &expires_at_ms.to_be_bytes());

            Ok(1i64)
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// EXPIRE key seconds
// ---------------------------------------------------------------------------

/// EXPIRE key seconds -- Set a timeout on key in seconds.
///
/// Returns:
/// - 1 if the timeout was set
/// - 0 if the key does not exist
pub async fn handle_expire(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "EXPIRE".into() }.to_string());
    }

    let seconds = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    if seconds < 0 {
        return RespValue::err(
            CommandError::InvalidExpireTime(format!("invalid expire time in 'expire' command: {seconds}")).to_string(),
        );
    }

    let offset_ms = match (seconds as u64).checked_mul(1000) {
        Some(v) => v,
        None => {
            return RespValue::err(CommandError::InvalidExpireTime("expire time overflow".into()).to_string());
        }
    };

    let now_ms = helpers::now_ms();
    let expires_at_ms = now_ms.saturating_add(offset_ms);

    set_expire(&args[0], expires_at_ms, state, "EXPIRE").await
}

// ---------------------------------------------------------------------------
// PEXPIRE key milliseconds
// ---------------------------------------------------------------------------

/// PEXPIRE key milliseconds -- Set a timeout on key in milliseconds.
///
/// Returns:
/// - 1 if the timeout was set
/// - 0 if the key does not exist
pub async fn handle_pexpire(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "PEXPIRE".into() }.to_string());
    }

    let milliseconds = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    if milliseconds < 0 {
        return RespValue::err(
            CommandError::InvalidExpireTime(format!("invalid expire time in 'pexpire' command: {milliseconds}"))
                .to_string(),
        );
    }

    let now_ms = helpers::now_ms();
    let expires_at_ms = now_ms.saturating_add(milliseconds as u64);

    set_expire(&args[0], expires_at_ms, state, "PEXPIRE").await
}

// ---------------------------------------------------------------------------
// EXPIREAT key timestamp
// ---------------------------------------------------------------------------

/// EXPIREAT key timestamp -- Set an expiry time (a Unix timestamp in seconds).
///
/// Returns:
/// - 1 if the timeout was set
/// - 0 if the key does not exist
pub async fn handle_expireat(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "EXPIREAT".into(),
            }
            .to_string(),
        );
    }

    let timestamp_secs = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    if timestamp_secs < 0 {
        return RespValue::err(
            CommandError::InvalidExpireTime(format!("invalid expire time in 'expireat' command: {timestamp_secs}"))
                .to_string(),
        );
    }

    let expires_at_ms = match (timestamp_secs as u64).checked_mul(1000) {
        Some(v) => v,
        None => {
            return RespValue::err(CommandError::InvalidExpireTime("expire time overflow".into()).to_string());
        }
    };

    set_expire(&args[0], expires_at_ms, state, "EXPIREAT").await
}

// ---------------------------------------------------------------------------
// PEXPIREAT key ms-timestamp
// ---------------------------------------------------------------------------

/// PEXPIREAT key ms-timestamp -- Set an expiry time (a Unix timestamp in milliseconds).
///
/// Returns:
/// - 1 if the timeout was set
/// - 0 if the key does not exist
pub async fn handle_pexpireat(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "PEXPIREAT".into(),
            }
            .to_string(),
        );
    }

    let timestamp_ms = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    if timestamp_ms < 0 {
        return RespValue::err(
            CommandError::InvalidExpireTime(format!("invalid expire time in 'pexpireat' command: {timestamp_ms}"))
                .to_string(),
        );
    }

    let expires_at_ms = timestamp_ms as u64;

    set_expire(&args[0], expires_at_ms, state, "PEXPIREAT").await
}

// ---------------------------------------------------------------------------
// PERSIST key
// ---------------------------------------------------------------------------

/// PERSIST key -- Remove the expiry from a key.
///
/// Returns:
/// - 1 if the timeout was removed
/// - 0 if the key does not exist or does not have an associated timeout
pub async fn handle_persist(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "PERSIST".into() }.to_string());
    }

    match run_transact(&state.db, "PERSIST", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            let mut meta = match meta {
                Some(m) => m,
                None => return Ok(0i64), // key doesn't exist or already expired
            };

            // If the key has no expiry, return 0.
            if meta.expires_at_ms == 0 {
                return Ok(0i64);
            }

            // Clear the expiry.
            meta.expires_at_ms = 0;
            meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;

            let expire_key = dirs.expire_key(&key);
            tr.clear(&expire_key);

            Ok(1i64)
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
