//! Hash command handlers: HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS,
//! HGETALL, HMGET, HMSET, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD.
//!
//! Hash fields are stored as individual FDB key-value pairs:
//!   hash/<redis_key, field_name> -> field_value
//!
//! ObjectMeta tracks the key type (Hash) and cardinality (field count).
//! All reads/writes happen within a single FDB transaction per command.

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::{helpers, run_transact};

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

    match run_transact(&state.db, "HSET", |tr| {
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

            // Process field-value pairs.
            let pairs = &args[1..];
            for chunk in pairs.chunks(2) {
                let field = &chunk[0];
                let value = &chunk[1];

                let fdb_key = dirs.hash.pack(&(key.as_ref(), field.as_ref()));

                // Check if the field already exists to distinguish add vs update.
                let existing = tr
                    .get(&fdb_key, false)
                    .await
                    .map_err(|e| helpers::cmd_err(CommandError::Generic(e.to_string())))?;

                if existing.is_none() {
                    added += 1;
                }

                tr.set(&fdb_key, value);
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

    match run_transact(&state.db, "HGET", |tr| {
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
