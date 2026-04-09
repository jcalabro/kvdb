//! Key management command handlers: TTL, PTTL, EXPIRETIME, PEXPIRETIME.
//!
//! These are read-only commands that query TTL metadata without modifying
//! anything. All four follow the same pattern: read ObjectMeta with lazy
//! expiry, then return the appropriate timestamp or remaining time value.

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
