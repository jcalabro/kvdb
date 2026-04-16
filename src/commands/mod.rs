//! Command dispatch and implementations.
//!
//! Commands are parsed from RESP arrays of bulk strings, dispatched by
//! name (case-insensitive), and executed against the storage layer.
//! Each command returns a `CommandResponse` that tells the connection
//! handler whether to continue or close the connection.

pub mod hashes;
pub mod keys;
pub mod lists;
pub mod pubsub;
pub mod registry;
pub mod server;
pub mod sets;
pub mod sorted_sets;
pub mod strings;
pub mod transaction;
pub(crate) mod util;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::CommandError;
use crate::protocol::types::{RedisCommand, RespValue};
use crate::pubsub::PubSubMessage;
use crate::server::connection::ConnectionState;

/// Sanitize a byte slice for safe embedding in RESP simple error messages.
///
/// RESP simple errors (`-ERR ...\r\n`) are line-delimited — if the message
/// contains `\r` or `\n`, the parser will see a premature frame boundary
/// and the trailing data becomes a rogue RESP value (protocol injection).
///
/// This function replaces control characters with safe escape sequences
/// so that user-controlled data (command names, subcommands, arguments)
/// can be safely included in error responses.
fn sanitize_for_error(data: &[u8]) -> String {
    let lossy = String::from_utf8_lossy(data);
    lossy.replace('\r', "\\r").replace('\n', "\\n")
}

/// Result of dispatching a command.
#[derive(Debug)]
#[must_use]
pub enum CommandResponse {
    /// Send the response and continue processing commands.
    Reply(RespValue),
    /// Send the response and close the connection.
    Close(RespValue),
    /// Send multiple responses (used by SUBSCRIBE/PSUBSCRIBE which
    /// return one confirmation per channel/pattern).
    MultiReply(Vec<RespValue>),
}

/// Dispatch a parsed command to the appropriate handler.
///
/// The command name is already uppercased by `RedisCommand::from_resp()`.
/// Returns a `CommandResponse` indicating the response to send and
/// whether the connection should be closed.
///
/// When inside a MULTI block (`state.transaction.is_some()`), most
/// commands are queued instead of executed immediately. Only
/// transaction-control commands (MULTI, EXEC, DISCARD, WATCH, UNWATCH)
/// and QUIT execute directly.
pub async fn dispatch(
    cmd: &RedisCommand,
    state: &mut ConnectionState,
    pubsub_rx: &mut Option<mpsc::UnboundedReceiver<PubSubMessage>>,
) -> CommandResponse {
    // If inside a MULTI block, intercept commands for queuing.
    if state.transaction.is_some() {
        match cmd.name.as_ref() {
            // These commands execute immediately even inside MULTI.
            b"EXEC" => return CommandResponse::Reply(transaction::handle_exec(&cmd.args, state).await),
            b"DISCARD" => return CommandResponse::Reply(transaction::handle_discard(&cmd.args, state)),
            b"MULTI" => return CommandResponse::Reply(RespValue::err("ERR MULTI calls can not be nested")),
            b"WATCH" => return CommandResponse::Reply(RespValue::err("ERR WATCH inside MULTI is not allowed")),
            b"UNWATCH" => return CommandResponse::Reply(transaction::handle_unwatch(&cmd.args, state)),
            b"QUIT" => return CommandResponse::Close(RespValue::ok()),
            // Everything else gets queued.
            _ => return CommandResponse::Reply(transaction::queue_command(cmd.clone(), state)),
        }
    }

    // Pub/sub mode enforcement: when a connection has active subscriptions,
    // only (P)SUBSCRIBE, (P)UNSUBSCRIBE, PING, QUIT, and RESET are allowed.
    // Everything else returns an error. This matches Redis behavior.
    if state.subscription_count() > 0 {
        match cmd.name.as_ref() {
            b"SUBSCRIBE" | b"PSUBSCRIBE" | b"UNSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PING" | b"QUIT" | b"RESET" => {
                // Allowed — fall through to normal dispatch.
            }
            _ => {
                let name_str = sanitize_for_error(&cmd.name);
                return CommandResponse::Reply(RespValue::err(format!(
                    "ERR Command not allowed inside a subscription context. Use SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT or RESET. Command was '{name_str}'"
                )));
            }
        }
    }

    match cmd.name.as_ref() {
        b"PING" => CommandResponse::Reply(if state.subscription_count() > 0 {
            // Redis spec: PING in pub/sub mode returns a Push message
            // ["pong", message] so clients can distinguish it from a
            // regular command response while subscribed.
            let msg = cmd.args.first().cloned().unwrap_or_else(|| Bytes::from_static(b""));
            RespValue::Push(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"pong"))),
                RespValue::BulkString(Some(msg)),
            ])
        } else {
            handle_ping(&cmd.args)
        }),
        b"ECHO" => CommandResponse::Reply(handle_echo(&cmd.args)),
        b"HELLO" => CommandResponse::Reply(handle_hello(&cmd.args, state)),
        b"QUIT" => CommandResponse::Close(RespValue::ok()),
        b"COMMAND" => CommandResponse::Reply(server::handle_command(&cmd.args).await),
        b"CLIENT" => CommandResponse::Reply(server::handle_client(&cmd.args, state).await),
        b"INFO" => CommandResponse::Reply(server::handle_info(&cmd.args, state).await),
        b"CONFIG" => CommandResponse::Reply(server::handle_config(&cmd.args, state).await),
        b"TIME" => CommandResponse::Reply(server::handle_time(&cmd.args).await),
        b"RANDOMKEY" => CommandResponse::Reply(server::handle_randomkey(&cmd.args, state).await),
        b"KEYS" => CommandResponse::Reply(server::handle_keys(&cmd.args, state).await),
        b"OBJECT" => CommandResponse::Reply(server::handle_object(&cmd.args, state).await),
        b"SLOWLOG" => CommandResponse::Reply(server::handle_slowlog(&cmd.args, state).await),
        b"MEMORY" => CommandResponse::Reply(server::handle_memory(&cmd.args, state).await),
        b"LATENCY" => CommandResponse::Reply(server::handle_latency(&cmd.args).await),
        b"DEBUG" => CommandResponse::Reply(server::handle_debug(&cmd.args, state).await),
        b"SHUTDOWN" => CommandResponse::Reply(server::handle_shutdown(&cmd.args, state).await),
        b"BGSAVE" => CommandResponse::Reply(server::handle_bgsave(&cmd.args).await),
        b"SAVE" => CommandResponse::Reply(server::handle_save(&cmd.args).await),
        b"BGREWRITEAOF" => CommandResponse::Reply(server::handle_bgrewriteaof(&cmd.args).await),
        b"LASTSAVE" => CommandResponse::Reply(server::handle_lastsave(&cmd.args, state).await),
        b"WAIT" => CommandResponse::Reply(server::handle_wait(&cmd.args).await),
        b"ACL" => CommandResponse::Reply(server::handle_acl(&cmd.args).await),
        b"AUTH" => CommandResponse::Reply(server::handle_auth(&cmd.args, state).await),
        b"GET" => CommandResponse::Reply(strings::handle_get(&cmd.args, state).await),
        b"SET" => CommandResponse::Reply(strings::handle_set(&cmd.args, state).await),
        b"DEL" => CommandResponse::Reply(strings::handle_del(&cmd.args, state).await),
        b"EXISTS" => CommandResponse::Reply(strings::handle_exists(&cmd.args, state).await),
        b"SETNX" => CommandResponse::Reply(strings::handle_setnx(&cmd.args, state).await),
        b"SETEX" => CommandResponse::Reply(strings::handle_setex(&cmd.args, state).await),
        b"PSETEX" => CommandResponse::Reply(strings::handle_psetex(&cmd.args, state).await),
        b"GETDEL" => CommandResponse::Reply(strings::handle_getdel(&cmd.args, state).await),
        b"MGET" => CommandResponse::Reply(strings::handle_mget(&cmd.args, state).await),
        b"MSET" => CommandResponse::Reply(strings::handle_mset(&cmd.args, state).await),
        b"INCR" => CommandResponse::Reply(strings::handle_incr(&cmd.args, state).await),
        b"DECR" => CommandResponse::Reply(strings::handle_decr(&cmd.args, state).await),
        b"INCRBY" => CommandResponse::Reply(strings::handle_incrby(&cmd.args, state).await),
        b"DECRBY" => CommandResponse::Reply(strings::handle_decrby(&cmd.args, state).await),
        b"INCRBYFLOAT" => CommandResponse::Reply(strings::handle_incrbyfloat(&cmd.args, state).await),
        b"APPEND" => CommandResponse::Reply(strings::handle_append(&cmd.args, state).await),
        b"STRLEN" => CommandResponse::Reply(strings::handle_strlen(&cmd.args, state).await),
        b"GETRANGE" => CommandResponse::Reply(strings::handle_getrange(&cmd.args, state).await),
        b"SETRANGE" => CommandResponse::Reply(strings::handle_setrange(&cmd.args, state).await),
        b"TTL" => CommandResponse::Reply(keys::handle_ttl(&cmd.args, state).await),
        b"PTTL" => CommandResponse::Reply(keys::handle_pttl(&cmd.args, state).await),
        b"EXPIRETIME" => CommandResponse::Reply(keys::handle_expiretime(&cmd.args, state).await),
        b"PEXPIRETIME" => CommandResponse::Reply(keys::handle_pexpiretime(&cmd.args, state).await),
        b"EXPIRE" => CommandResponse::Reply(keys::handle_expire(&cmd.args, state).await),
        b"PEXPIRE" => CommandResponse::Reply(keys::handle_pexpire(&cmd.args, state).await),
        b"EXPIREAT" => CommandResponse::Reply(keys::handle_expireat(&cmd.args, state).await),
        b"PEXPIREAT" => CommandResponse::Reply(keys::handle_pexpireat(&cmd.args, state).await),
        b"PERSIST" => CommandResponse::Reply(keys::handle_persist(&cmd.args, state).await),
        b"TYPE" => CommandResponse::Reply(keys::handle_type(&cmd.args, state).await),
        b"UNLINK" => CommandResponse::Reply(keys::handle_unlink(&cmd.args, state).await),
        b"TOUCH" => CommandResponse::Reply(keys::handle_touch(&cmd.args, state).await),
        b"DBSIZE" => CommandResponse::Reply(keys::handle_dbsize(&cmd.args, state).await),
        b"RENAME" => CommandResponse::Reply(keys::handle_rename(&cmd.args, state).await),
        b"RENAMENX" => CommandResponse::Reply(keys::handle_renamenx(&cmd.args, state).await),
        b"SELECT" => CommandResponse::Reply(keys::handle_select(&cmd.args, state).await),
        b"FLUSHDB" => CommandResponse::Reply(keys::handle_flushdb(&cmd.args, state).await),
        b"FLUSHALL" => CommandResponse::Reply(keys::handle_flushall(&cmd.args, state).await),
        b"MULTI" => CommandResponse::Reply(transaction::handle_multi(&cmd.args, state)),
        b"EXEC" => CommandResponse::Reply(transaction::handle_exec(&cmd.args, state).await),
        b"DISCARD" => CommandResponse::Reply(transaction::handle_discard(&cmd.args, state)),
        b"WATCH" => CommandResponse::Reply(transaction::handle_watch(&cmd.args, state).await),
        b"UNWATCH" => CommandResponse::Reply(transaction::handle_unwatch(&cmd.args, state)),
        b"HSET" => CommandResponse::Reply(hashes::handle_hset(&cmd.args, state).await),
        b"HGET" => CommandResponse::Reply(hashes::handle_hget(&cmd.args, state).await),
        b"HDEL" => CommandResponse::Reply(hashes::handle_hdel(&cmd.args, state).await),
        b"HEXISTS" => CommandResponse::Reply(hashes::handle_hexists(&cmd.args, state).await),
        b"HLEN" => CommandResponse::Reply(hashes::handle_hlen(&cmd.args, state).await),
        b"HGETALL" => CommandResponse::Reply(hashes::handle_hgetall(&cmd.args, state).await),
        b"HKEYS" => CommandResponse::Reply(hashes::handle_hkeys(&cmd.args, state).await),
        b"HVALS" => CommandResponse::Reply(hashes::handle_hvals(&cmd.args, state).await),
        b"HMGET" => CommandResponse::Reply(hashes::handle_hmget(&cmd.args, state).await),
        b"HMSET" => CommandResponse::Reply(hashes::handle_hmset(&cmd.args, state).await),
        b"HINCRBY" => CommandResponse::Reply(hashes::handle_hincrby(&cmd.args, state).await),
        b"HINCRBYFLOAT" => CommandResponse::Reply(hashes::handle_hincrbyfloat(&cmd.args, state).await),
        b"HSETNX" => CommandResponse::Reply(hashes::handle_hsetnx(&cmd.args, state).await),
        b"HSTRLEN" => CommandResponse::Reply(hashes::handle_hstrlen(&cmd.args, state).await),
        b"HRANDFIELD" => CommandResponse::Reply(hashes::handle_hrandfield(&cmd.args, state).await),
        b"SADD" => CommandResponse::Reply(sets::handle_sadd(&cmd.args, state).await),
        b"SREM" => CommandResponse::Reply(sets::handle_srem(&cmd.args, state).await),
        b"SISMEMBER" => CommandResponse::Reply(sets::handle_sismember(&cmd.args, state).await),
        b"SMISMEMBER" => CommandResponse::Reply(sets::handle_smismember(&cmd.args, state).await),
        b"SCARD" => CommandResponse::Reply(sets::handle_scard(&cmd.args, state).await),
        b"SMEMBERS" => CommandResponse::Reply(sets::handle_smembers(&cmd.args, state).await),
        b"SPOP" => CommandResponse::Reply(sets::handle_spop(&cmd.args, state).await),
        b"SRANDMEMBER" => CommandResponse::Reply(sets::handle_srandmember(&cmd.args, state).await),
        b"SMOVE" => CommandResponse::Reply(sets::handle_smove(&cmd.args, state).await),
        b"SINTER" => CommandResponse::Reply(sets::handle_sinter(&cmd.args, state).await),
        b"SUNION" => CommandResponse::Reply(sets::handle_sunion(&cmd.args, state).await),
        b"SDIFF" => CommandResponse::Reply(sets::handle_sdiff(&cmd.args, state).await),
        b"SINTERSTORE" => CommandResponse::Reply(sets::handle_sinterstore(&cmd.args, state).await),
        b"SUNIONSTORE" => CommandResponse::Reply(sets::handle_sunionstore(&cmd.args, state).await),
        b"SDIFFSTORE" => CommandResponse::Reply(sets::handle_sdiffstore(&cmd.args, state).await),
        b"SINTERCARD" => CommandResponse::Reply(sets::handle_sintercard(&cmd.args, state).await),
        b"LPUSH" => CommandResponse::Reply(lists::handle_lpush(&cmd.args, state).await),
        b"RPUSH" => CommandResponse::Reply(lists::handle_rpush(&cmd.args, state).await),
        b"LPUSHX" => CommandResponse::Reply(lists::handle_lpushx(&cmd.args, state).await),
        b"RPUSHX" => CommandResponse::Reply(lists::handle_rpushx(&cmd.args, state).await),
        b"LPOP" => CommandResponse::Reply(lists::handle_lpop(&cmd.args, state).await),
        b"RPOP" => CommandResponse::Reply(lists::handle_rpop(&cmd.args, state).await),
        b"LLEN" => CommandResponse::Reply(lists::handle_llen(&cmd.args, state).await),
        b"LINDEX" => CommandResponse::Reply(lists::handle_lindex(&cmd.args, state).await),
        b"LRANGE" => CommandResponse::Reply(lists::handle_lrange(&cmd.args, state).await),
        b"LSET" => CommandResponse::Reply(lists::handle_lset(&cmd.args, state).await),
        b"LTRIM" => CommandResponse::Reply(lists::handle_ltrim(&cmd.args, state).await),
        b"LREM" => CommandResponse::Reply(lists::handle_lrem(&cmd.args, state).await),
        b"LINSERT" => CommandResponse::Reply(lists::handle_linsert(&cmd.args, state).await),
        b"LPOS" => CommandResponse::Reply(lists::handle_lpos(&cmd.args, state).await),
        b"LMOVE" => CommandResponse::Reply(lists::handle_lmove(&cmd.args, state).await),
        b"LMPOP" => CommandResponse::Reply(lists::handle_lmpop(&cmd.args, state).await),
        b"ZADD" => CommandResponse::Reply(sorted_sets::handle_zadd(&cmd.args, state).await),
        b"ZCARD" => CommandResponse::Reply(sorted_sets::handle_zcard(&cmd.args, state).await),
        b"ZSCORE" => CommandResponse::Reply(sorted_sets::handle_zscore(&cmd.args, state).await),
        b"ZREM" => CommandResponse::Reply(sorted_sets::handle_zrem(&cmd.args, state).await),
        b"ZRANK" => CommandResponse::Reply(sorted_sets::handle_zrank(&cmd.args, state).await),
        b"ZREVRANK" => CommandResponse::Reply(sorted_sets::handle_zrevrank(&cmd.args, state).await),
        b"ZINCRBY" => CommandResponse::Reply(sorted_sets::handle_zincrby(&cmd.args, state).await),
        b"ZRANGE" => CommandResponse::Reply(sorted_sets::handle_zrange(&cmd.args, state).await),
        b"ZREVRANGE" => CommandResponse::Reply(sorted_sets::handle_zrevrange(&cmd.args, state).await),
        b"ZCOUNT" => CommandResponse::Reply(sorted_sets::handle_zcount(&cmd.args, state).await),
        b"ZLEXCOUNT" => CommandResponse::Reply(sorted_sets::handle_zlexcount(&cmd.args, state).await),
        b"ZRANGEBYSCORE" => CommandResponse::Reply(sorted_sets::handle_zrangebyscore(&cmd.args, state).await),
        b"ZRANGEBYLEX" => CommandResponse::Reply(sorted_sets::handle_zrangebylex(&cmd.args, state).await),
        b"ZREMRANGEBYRANK" => CommandResponse::Reply(sorted_sets::handle_zremrangebyrank(&cmd.args, state).await),
        b"ZREMRANGEBYSCORE" => CommandResponse::Reply(sorted_sets::handle_zremrangebyscore(&cmd.args, state).await),
        b"ZREMRANGEBYLEX" => CommandResponse::Reply(sorted_sets::handle_zremrangebylex(&cmd.args, state).await),
        b"ZPOPMIN" => CommandResponse::Reply(sorted_sets::handle_zpopmin(&cmd.args, state).await),
        b"ZPOPMAX" => CommandResponse::Reply(sorted_sets::handle_zpopmax(&cmd.args, state).await),
        b"ZRANDMEMBER" => CommandResponse::Reply(sorted_sets::handle_zrandmember(&cmd.args, state).await),
        b"ZMSCORE" => CommandResponse::Reply(sorted_sets::handle_zmscore(&cmd.args, state).await),
        b"SUBSCRIBE" => pubsub::handle_subscribe(&cmd.args, state, pubsub_rx),
        b"UNSUBSCRIBE" => pubsub::handle_unsubscribe(&cmd.args, state),
        b"PSUBSCRIBE" => pubsub::handle_psubscribe(&cmd.args, state, pubsub_rx),
        b"PUNSUBSCRIBE" => pubsub::handle_punsubscribe(&cmd.args, state),
        b"PUBLISH" => CommandResponse::Reply(pubsub::handle_publish(&cmd.args, state).await),
        b"PUBSUB" => CommandResponse::Reply(pubsub::handle_pubsub(&cmd.args, state)),
        b"RESET" => CommandResponse::Reply(handle_reset(state)),
        _ => {
            let name_str = sanitize_for_error(&cmd.name);
            let mut msg = format!("ERR unknown command '{name_str}', with args beginning with:");
            for arg in cmd.args.iter().take(3) {
                msg.push_str(&format!(" '{}'", sanitize_for_error(arg)));
            }
            CommandResponse::Reply(RespValue::Error(Bytes::from(msg)))
        }
    }
}

/// RESET
///
/// Resets the connection to its initial state:
/// - Cancels all pub/sub subscriptions and their watcher tasks
/// - Resets the protocol version to RESP2
///
/// Returns `+RESET` (a SimpleString). Allowed in pub/sub mode so that
/// clients can use RESET to exit pub/sub mode cleanly.
fn handle_reset(state: &mut ConnectionState) -> RespValue {
    // Cancel all channel watcher tasks.
    for (_, cancel) in state.subscribed_channel_cancels.drain() {
        cancel.cancel();
    }
    // Cancel all pattern watcher tasks.
    for (_, cancel) in state.subscribed_pattern_cancels.drain() {
        cancel.cancel();
    }
    // Remove all subscriptions from the shared registry.
    state.pubsub.unsubscribe_all(state.connection_id);
    // Clear connection-local subscription tracking.
    state.subscribed_channels.clear();
    state.subscribed_patterns.clear();
    // Reset protocol version to RESP2.
    state.protocol_version = 2;

    RespValue::SimpleString(Bytes::from_static(b"RESET"))
}

fn handle_ping(args: &[Bytes]) -> RespValue {
    match args.len() {
        0 => RespValue::SimpleString(Bytes::from_static(b"PONG")),
        1 => RespValue::BulkString(Some(args[0].clone())),
        _ => RespValue::Error(Bytes::from(
            CommandError::WrongArity { name: "PING".into() }.to_string(),
        )),
    }
}

fn handle_echo(args: &[Bytes]) -> RespValue {
    if args.len() != 1 {
        return RespValue::Error(Bytes::from(
            CommandError::WrongArity { name: "ECHO".into() }.to_string(),
        ));
    }
    RespValue::BulkString(Some(args[0].clone()))
}

/// HELLO [protover [AUTH username password] [SETNAME clientname]]
///
/// Negotiates protocol version and returns server info. If protover
/// is 3, switches the connection to RESP3 encoding. If omitted or 2,
/// stays on RESP2.
///
/// NOTE: AUTH and SETNAME sub-arguments are silently ignored for now.
/// AUTH support is planned for M12. This is safe because clients that
/// send AUTH in HELLO will still get a successful response — they just
/// won't be authenticated (no auth is required until M12).
fn handle_hello(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    let proto = if args.is_empty() {
        state.protocol_version
    } else {
        match args[0].as_ref() {
            b"2" => {
                state.protocol_version = 2;
                2
            }
            b"3" => {
                state.protocol_version = 3;
                3
            }
            _ => {
                return RespValue::err("NOPROTO unsupported protocol version");
            }
        }
    };

    // HELLO always returns a map of server properties.
    // In RESP2 mode, the encoder will downgrade Map to a flat Array.
    RespValue::Map(vec![
        (
            RespValue::BulkString(Some(Bytes::from_static(b"server"))),
            RespValue::BulkString(Some(Bytes::from_static(b"kvdb"))),
        ),
        (
            RespValue::BulkString(Some(Bytes::from_static(b"version"))),
            RespValue::BulkString(Some(Bytes::from_static(b"0.1.0"))),
        ),
        (
            RespValue::BulkString(Some(Bytes::from_static(b"proto"))),
            RespValue::Integer(proto as i64),
        ),
        (
            RespValue::BulkString(Some(Bytes::from_static(b"id"))),
            RespValue::Integer(0),
        ),
        (
            RespValue::BulkString(Some(Bytes::from_static(b"mode"))),
            RespValue::BulkString(Some(Bytes::from_static(b"standalone"))),
        ),
        (
            RespValue::BulkString(Some(Bytes::from_static(b"role"))),
            RespValue::BulkString(Some(Bytes::from_static(b"master"))),
        ),
        (
            RespValue::BulkString(Some(Bytes::from_static(b"modules"))),
            RespValue::Array(Some(vec![])),
        ),
    ])
}

// COMMAND and CLIENT stubs replaced by full implementations in
// commands/server.rs (dispatched via server::handle_command and
// server::handle_client above).

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cmd(name: &[u8], args: Vec<&[u8]>) -> RedisCommand {
        RedisCommand {
            name: Bytes::from(name.to_vec()),
            args: args.into_iter().map(|a| Bytes::from(a.to_vec())).collect(),
        }
    }

    async fn dispatch_reply(cmd: &RedisCommand) -> RespValue {
        let mut state = ConnectionState::default_for_test();
        let mut pubsub_rx = None;
        match dispatch(cmd, &mut state, &mut pubsub_rx).await {
            CommandResponse::Reply(resp) => resp,
            CommandResponse::Close(resp) => resp,
            CommandResponse::MultiReply(resps) => RespValue::Array(Some(resps)),
        }
    }

    #[tokio::test]
    async fn ping_no_args() {
        let cmd = make_cmd(b"PING", vec![]);
        assert_eq!(
            dispatch_reply(&cmd).await,
            RespValue::SimpleString(Bytes::from_static(b"PONG"))
        );
    }

    #[tokio::test]
    async fn ping_with_message() {
        let cmd = make_cmd(b"PING", vec![b"hello"]);
        assert_eq!(
            dispatch_reply(&cmd).await,
            RespValue::BulkString(Some(Bytes::from_static(b"hello")))
        );
    }

    #[tokio::test]
    async fn ping_wrong_arity() {
        let cmd = make_cmd(b"PING", vec![b"a", b"b"]);
        match dispatch_reply(&cmd).await {
            RespValue::Error(msg) => {
                let s = String::from_utf8_lossy(&msg);
                assert!(s.contains("wrong number of arguments"), "got: {s}");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn echo_returns_argument() {
        let cmd = make_cmd(b"ECHO", vec![b"world"]);
        assert_eq!(
            dispatch_reply(&cmd).await,
            RespValue::BulkString(Some(Bytes::from_static(b"world")))
        );
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let cmd = make_cmd(b"FAKECMD", vec![]);
        match dispatch_reply(&cmd).await {
            RespValue::Error(_) => {} // expected
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_command_with_crlf_in_name_produces_safe_error() {
        // Regression test: command names with \r\n must be sanitized
        // in error messages to prevent RESP protocol injection.
        let cmd = make_cmd(b"FOO\r\nBAR", vec![b"arg\r\n"]);
        match dispatch_reply(&cmd).await {
            RespValue::Error(msg) => {
                let s = String::from_utf8_lossy(&msg);
                // \r and \n must be escaped, not literal
                assert!(!s.contains('\r'), "error contains raw \\r: {s}");
                assert!(!s.contains('\n'), "error contains raw \\n: {s}");
                assert!(s.contains("\\r"), "error should contain escaped \\r: {s}");
                assert!(s.contains("\\n"), "error should contain escaped \\n: {s}");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn command_dispatch_is_case_insensitive() {
        // dispatch() expects uppercase names (from_resp uppercases),
        // but verify it matches correctly by sending lowercase through
        // the full RedisCommand::from_resp path.
        let value = RespValue::Array(Some(vec![RespValue::BulkString(Some(Bytes::from_static(b"ping")))]));
        let cmd = RedisCommand::from_resp(value).unwrap();
        assert_eq!(
            dispatch_reply(&cmd).await,
            RespValue::SimpleString(Bytes::from_static(b"PONG"))
        );
    }

    #[tokio::test]
    async fn hello_defaults_to_resp2() {
        let cmd = make_cmd(b"HELLO", vec![]);
        let mut state = ConnectionState::default_for_test();
        let mut pubsub_rx = None;
        let resp = match dispatch(&cmd, &mut state, &mut pubsub_rx).await {
            CommandResponse::Reply(r) => r,
            other => panic!("expected Reply, got {other:?}"),
        };
        if let RespValue::Map(pairs) = resp {
            let proto_pair = pairs
                .iter()
                .find(|(k, _)| matches!(k, RespValue::BulkString(Some(b)) if b.as_ref() == b"proto"));
            assert_eq!(proto_pair.unwrap().1, RespValue::Integer(2));
        } else {
            panic!("expected Map, got {resp:?}");
        }
    }

    #[tokio::test]
    async fn hello_upgrades_to_resp3() {
        let cmd = make_cmd(b"HELLO", vec![b"3"]);
        let mut state = ConnectionState::default_for_test();
        assert_eq!(state.protocol_version, 2);
        let mut pubsub_rx = None;
        match dispatch(&cmd, &mut state, &mut pubsub_rx).await {
            CommandResponse::Reply(_) => {}
            other => panic!("expected Reply, got {other:?}"),
        }
        assert_eq!(state.protocol_version, 3);
    }

    #[tokio::test]
    async fn hello_rejects_invalid_version() {
        let cmd = make_cmd(b"HELLO", vec![b"4"]);
        let mut state = ConnectionState::default_for_test();
        let mut pubsub_rx = None;
        match dispatch(&cmd, &mut state, &mut pubsub_rx).await {
            CommandResponse::Reply(RespValue::Error(msg)) => {
                assert!(msg.as_ref().starts_with(b"NOPROTO"));
            }
            other => panic!("expected NOPROTO error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn quit_returns_close() {
        let cmd = make_cmd(b"QUIT", vec![]);
        let mut state = ConnectionState::default_for_test();
        let mut pubsub_rx = None;
        match dispatch(&cmd, &mut state, &mut pubsub_rx).await {
            CommandResponse::Close(RespValue::SimpleString(msg)) => {
                assert_eq!(msg.as_ref(), b"OK");
            }
            other => panic!("expected Close(OK), got {other:?}"),
        }
    }
}
