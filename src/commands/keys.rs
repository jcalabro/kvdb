//! Key management command handlers.
//!
//! **Read commands**: TTL, PTTL, EXPIRETIME, PEXPIRETIME — query TTL metadata.
//! **Write commands**: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST — modify TTL metadata.
//! **Introspection commands**: TYPE, TOUCH, UNLINK, DBSIZE — key inspection and management.

use bytes::Bytes;
use foundationdb::RangeOption;

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
    match run_transact(&state.db, state.shared_txn(), "TTL", |tr| {
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
    match run_transact(&state.db, state.shared_txn(), "PTTL", |tr| {
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
    match run_transact(&state.db, state.shared_txn(), "EXPIRETIME", |tr| {
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
    match run_transact(&state.db, state.shared_txn(), "PEXPIRETIME", |tr| {
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
    match run_transact(&state.db, state.shared_txn(), command_name, |tr| {
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

    match run_transact(&state.db, state.shared_txn(), "PERSIST", |tr| {
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

// ---------------------------------------------------------------------------
// TYPE key
// ---------------------------------------------------------------------------

/// TYPE key -- Returns the string representation of the type stored at key.
///
/// Returns:
/// - "none" if the key does not exist
/// - "string", "hash", "set", "zset", "list", or "stream" for existing keys
pub async fn handle_type(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "TYPE".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "TYPE", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok("none"),
                Some(m) => Ok(m.key_type.as_redis_type_str()),
            }
        }
    })
    .await
    {
        Ok(type_str) => RespValue::SimpleString(Bytes::from(type_str)),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// UNLINK key [key ...]
// ---------------------------------------------------------------------------

/// UNLINK key [key ...] -- Delete keys asynchronously.
///
/// For Phase 1, this is identical to DEL (synchronous deletion).
/// Returns the count of keys that were deleted.
pub async fn handle_unlink(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "UNLINK".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "UNLINK", |tr| {
        let dirs = state.dirs.clone();
        let keys: Vec<Bytes> = args.to_vec();
        async move {
            let now = helpers::now_ms();
            let mut count: i64 = 0;
            for k in &keys {
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
// TOUCH key [key ...]
// ---------------------------------------------------------------------------

/// TOUCH key [key ...] -- Alter the last access time of keys.
///
/// Returns the count of keys that exist (non-expired).
pub async fn handle_touch(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "TOUCH".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "TOUCH", |tr| {
        let dirs = state.dirs.clone();
        let keys: Vec<Bytes> = args.to_vec();
        async move {
            let now = helpers::now_ms();
            let mut count: i64 = 0;
            for k in &keys {
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
// DBSIZE
// ---------------------------------------------------------------------------

/// DBSIZE -- Returns the number of keys in the currently selected database.
///
/// Counts all non-expired keys in the current namespace.
pub async fn handle_dbsize(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if !args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "DBSIZE".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "DBSIZE", |tr| {
        let dirs = state.dirs.clone();
        async move {
            let now = helpers::now_ms();
            let (begin, end) = dirs.meta.range();
            let mut maybe_range: Option<RangeOption<'_>> = Some(RangeOption::from((begin.as_slice(), end.as_slice())));
            let mut iteration = 1;
            let mut count: i64 = 0;

            while let Some(range_opt) = maybe_range.take() {
                let kvs = tr
                    .get_range(&range_opt, iteration, true)
                    .await
                    .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                for kv in kvs.iter() {
                    if let Ok(meta) = ObjectMeta::deserialize(kv.value())
                        && !meta.is_expired(now)
                    {
                        count += 1;
                    }
                }

                maybe_range = range_opt.next_range(&kvs);
                iteration += 1;
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
// RENAME key newkey
// ---------------------------------------------------------------------------

/// RENAME key newkey -- Atomically move a key to a new name.
///
/// Overwrites the destination if it exists.
/// Returns OK on success, error if source doesn't exist.
pub async fn handle_rename(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "RENAME".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "RENAME", |tr| {
        let dirs = state.dirs.clone();
        let src_key = args[0].clone();
        let dst_key = args[1].clone();
        async move {
            let result = rename_impl(&tr, &dirs, &src_key, &dst_key, false).await?;
            match result {
                RenameResult::Ok => Ok(()),
                RenameResult::DestExists => unreachable!("RENAME should always overwrite"),
            }
        }
    })
    .await
    {
        Ok(()) => RespValue::ok(),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// RENAMENX key newkey
// ---------------------------------------------------------------------------

/// RENAMENX key newkey -- Atomically move a key to a new name if destination doesn't exist.
///
/// Returns:
/// - 1 if the key was renamed
/// - 0 if the destination already exists
/// - error if source doesn't exist
pub async fn handle_renamenx(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "RENAMENX".into(),
            }
            .to_string(),
        );
    }

    match run_transact(&state.db, state.shared_txn(), "RENAMENX", |tr| {
        let dirs = state.dirs.clone();
        let src_key = args[0].clone();
        let dst_key = args[1].clone();
        async move {
            let result = rename_impl(&tr, &dirs, &src_key, &dst_key, true).await?;
            match result {
                RenameResult::Ok => Ok(1i64),
                RenameResult::DestExists => Ok(0i64),
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
// Shared RENAME implementation
// ---------------------------------------------------------------------------

/// Result of rename_impl.
enum RenameResult {
    Ok,
    DestExists,
}

/// Shared implementation for RENAME and RENAMENX.
///
/// If `nx` is true, returns `DestExists` if destination exists (live).
/// Otherwise, overwrites the destination.
async fn rename_impl(
    tr: &foundationdb::RetryableTransaction,
    dirs: &crate::storage::Directories,
    src_key: &[u8],
    dst_key: &[u8],
    nx: bool,
) -> Result<RenameResult, foundationdb::FdbBindingError> {
    let now = helpers::now_ms();

    // 1. Read source meta with now_ms=0 to see expired keys too.
    let src_meta = ObjectMeta::read(tr, dirs, src_key, 0, false)
        .await
        .map_err(helpers::storage_err)?;

    let src_meta = match src_meta {
        Some(m) => m,
        None => {
            return Err(helpers::cmd_err(CommandError::Generic("ERR no such key".into())));
        }
    };

    // 2. If source is expired, clean it up and return error.
    if src_meta.is_expired(now) {
        helpers::delete_object(tr, dirs, src_key, now)
            .await
            .map_err(helpers::cmd_err)?;
        return Err(helpers::cmd_err(CommandError::Generic("ERR no such key".into())));
    }

    // 3. If src == dst, return OK (no-op).
    if src_key == dst_key {
        return Ok(RenameResult::Ok);
    }

    // 4. For RENAMENX: check if destination exists (live).
    if nx {
        let dst_meta = ObjectMeta::read(tr, dirs, dst_key, now, false)
            .await
            .map_err(helpers::storage_err)?;
        if dst_meta.is_some() {
            return Ok(RenameResult::DestExists);
        }
    }

    // 5. Only String type is implemented for now.
    if src_meta.key_type != crate::storage::meta::KeyType::String {
        return Err(helpers::cmd_err(CommandError::Generic(
            "ERR RENAME of non-string types not yet supported".into(),
        )));
    }

    // 6. Read source data.
    let src_data = helpers::get_string(tr, dirs, src_key, 0)
        .await
        .map_err(helpers::cmd_err)?;
    let src_data = match src_data {
        Some(d) => d,
        None => {
            return Err(helpers::cmd_err(CommandError::Generic("ERR no such key".into())));
        }
    };

    // 7. Delete destination if it exists (even if expired).
    helpers::delete_object(tr, dirs, dst_key, 0)
        .await
        .map_err(helpers::cmd_err)?;

    // 8. Write source data to destination, preserving expires_at_ms.
    helpers::write_string(tr, dirs, dst_key, &src_data, src_meta.expires_at_ms, None).map_err(helpers::cmd_err)?;

    // 9. Delete source (even if expired).
    helpers::delete_object(tr, dirs, src_key, 0)
        .await
        .map_err(helpers::cmd_err)?;

    Ok(RenameResult::Ok)
}

// ---------------------------------------------------------------------------
// SELECT index
// ---------------------------------------------------------------------------

/// SELECT index -- Switch to the specified database namespace.
///
/// Returns OK on success, error if index is out of range (0-15).
pub async fn handle_select(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "SELECT".into() }.to_string());
    }

    let index = match parse_i64_arg(&args[0]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    if !(0..=15).contains(&index) {
        return RespValue::err("ERR DB index is out of range");
    }

    let namespace = index as u8;

    // Get or open directories for this namespace.
    match state.ns_cache.get(namespace).await {
        Ok(dirs) => {
            state.dirs = dirs;
            state.selected_db = namespace;
            RespValue::ok()
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// FLUSHDB [ASYNC]
// ---------------------------------------------------------------------------

/// FLUSHDB \[ASYNC\] -- Delete all keys in the currently selected database.
///
/// The ASYNC flag is accepted but ignored (all flushes are synchronous).
/// Returns OK on success.
pub async fn handle_flushdb(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Accept 0 or 1 args (ASYNC flag, which we ignore).
    if args.len() > 1 {
        return RespValue::err(CommandError::WrongArity { name: "FLUSHDB".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "FLUSHDB", |tr| {
        let dirs = state.dirs.clone();
        async move {
            flush_namespace(&tr, &dirs);
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
// FLUSHALL [ASYNC]
// ---------------------------------------------------------------------------

/// FLUSHALL \[ASYNC\] -- Delete all keys in all cached databases.
///
/// The ASYNC flag is accepted but ignored (all flushes are synchronous).
/// Returns OK on success.
pub async fn handle_flushall(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Accept 0 or 1 args (ASYNC flag, which we ignore).
    if args.len() > 1 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "FLUSHALL".into(),
            }
            .to_string(),
        );
    }

    // Get all cached namespaces.
    let all_dirs = state.ns_cache.cached_namespaces().await;

    match run_transact(&state.db, state.shared_txn(), "FLUSHALL", |tr| {
        let dirs_list = all_dirs.clone();
        async move {
            for dirs in &dirs_list {
                flush_namespace(&tr, dirs);
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
// Helper: flush all subspaces for a namespace
// ---------------------------------------------------------------------------

/// Clear all 8 subspaces for a single namespace.
fn flush_namespace(tr: &foundationdb::RetryableTransaction, dirs: &crate::storage::Directories) {
    let (begin, end) = dirs.meta.range();
    tr.clear_range(&begin, &end);

    let (begin, end) = dirs.obj.range();
    tr.clear_range(&begin, &end);

    let (begin, end) = dirs.hash.range();
    tr.clear_range(&begin, &end);

    let (begin, end) = dirs.set.range();
    tr.clear_range(&begin, &end);

    let (begin, end) = dirs.zset.range();
    tr.clear_range(&begin, &end);

    let (begin, end) = dirs.zset_idx.range();
    tr.clear_range(&begin, &end);

    let (begin, end) = dirs.list.range();
    tr.clear_range(&begin, &end);

    let (begin, end) = dirs.expire.range();
    tr.clear_range(&begin, &end);
}
