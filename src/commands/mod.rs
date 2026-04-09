//! Command dispatch and implementations.
//!
//! Commands are parsed from RESP arrays of bulk strings, dispatched by
//! name (case-insensitive), and executed against the storage layer.
//! Each command returns a `CommandResponse` that tells the connection
//! handler whether to continue or close the connection.

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::types::{RedisCommand, RespValue};
use crate::server::connection::ConnectionState;

/// Result of dispatching a command.
#[derive(Debug)]
#[must_use]
pub enum CommandResponse {
    /// Send the response and continue processing commands.
    Reply(RespValue),
    /// Send the response and close the connection.
    Close(RespValue),
}

/// Dispatch a parsed command to the appropriate handler.
///
/// The command name is already uppercased by `RedisCommand::from_resp()`.
/// Returns a `CommandResponse` indicating the response to send and
/// whether the connection should be closed.
pub fn dispatch(cmd: &RedisCommand, state: &mut ConnectionState) -> CommandResponse {
    match cmd.name.as_ref() {
        b"PING" => CommandResponse::Reply(handle_ping(&cmd.args)),
        b"ECHO" => CommandResponse::Reply(handle_echo(&cmd.args)),
        b"HELLO" => CommandResponse::Reply(handle_hello(&cmd.args, state)),
        b"QUIT" => CommandResponse::Close(RespValue::ok()),
        b"COMMAND" => CommandResponse::Reply(handle_command(&cmd.args)),
        b"CLIENT" => CommandResponse::Reply(handle_client(&cmd.args)),
        _ => {
            let name_str = String::from_utf8_lossy(&cmd.name);
            let mut msg = format!("ERR unknown command '{name_str}', with args beginning with:");
            for arg in cmd.args.iter().take(3) {
                msg.push_str(&format!(" '{}'", String::from_utf8_lossy(arg)));
            }
            CommandResponse::Reply(RespValue::Error(Bytes::from(msg)))
        }
    }
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

/// COMMAND [subcommand]
///
/// Minimal stubs for client library handshake.
fn handle_command(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::Array(Some(vec![]));
    }

    let subcmd = args[0].to_ascii_uppercase();
    match subcmd.as_slice() {
        b"COUNT" => RespValue::Integer(0),
        b"DOCS" => RespValue::Map(vec![]),
        b"LIST" => RespValue::Array(Some(vec![])),
        b"INFO" => RespValue::Array(Some(vec![])),
        _ => RespValue::err(format!(
            "ERR unknown subcommand or wrong number of arguments for 'COMMAND|{}' command",
            String::from_utf8_lossy(&subcmd)
        )),
    }
}

/// CLIENT [subcommand]
///
/// Minimal stubs for client library handshake.
fn handle_client(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return RespValue::err("ERR wrong number of arguments for 'CLIENT' command");
    }

    let subcmd = args[0].to_ascii_uppercase();
    match subcmd.as_slice() {
        b"SETNAME" => RespValue::ok(),
        b"GETNAME" => RespValue::BulkString(None),
        b"ID" => RespValue::Integer(0),
        b"INFO" => RespValue::BulkString(Some(Bytes::from_static(b""))),
        _ => RespValue::err(format!(
            "ERR unknown subcommand or wrong number of arguments for 'CLIENT|{}' command",
            String::from_utf8_lossy(&subcmd)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cmd(name: &[u8], args: Vec<&[u8]>) -> RedisCommand {
        RedisCommand {
            name: Bytes::from(name.to_vec()),
            args: args.into_iter().map(|a| Bytes::from(a.to_vec())).collect(),
        }
    }

    fn dispatch_reply(cmd: &RedisCommand) -> RespValue {
        let mut state = ConnectionState::default();
        match dispatch(cmd, &mut state) {
            CommandResponse::Reply(resp) => resp,
            CommandResponse::Close(resp) => resp,
        }
    }

    #[test]
    fn ping_no_args() {
        let cmd = make_cmd(b"PING", vec![]);
        assert_eq!(
            dispatch_reply(&cmd),
            RespValue::SimpleString(Bytes::from_static(b"PONG"))
        );
    }

    #[test]
    fn ping_with_message() {
        let cmd = make_cmd(b"PING", vec![b"hello"]);
        assert_eq!(
            dispatch_reply(&cmd),
            RespValue::BulkString(Some(Bytes::from_static(b"hello")))
        );
    }

    #[test]
    fn ping_wrong_arity() {
        let cmd = make_cmd(b"PING", vec![b"a", b"b"]);
        match dispatch_reply(&cmd) {
            RespValue::Error(msg) => {
                let s = String::from_utf8_lossy(&msg);
                assert!(s.contains("wrong number of arguments"), "got: {s}");
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn echo_returns_argument() {
        let cmd = make_cmd(b"ECHO", vec![b"world"]);
        assert_eq!(
            dispatch_reply(&cmd),
            RespValue::BulkString(Some(Bytes::from_static(b"world")))
        );
    }

    #[test]
    fn unknown_command_returns_error() {
        let cmd = make_cmd(b"FAKECMD", vec![]);
        match dispatch_reply(&cmd) {
            RespValue::Error(_) => {} // expected
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn command_dispatch_is_case_insensitive() {
        // dispatch() expects uppercase names (from_resp uppercases),
        // but verify it matches correctly by sending lowercase through
        // the full RedisCommand::from_resp path.
        let value = RespValue::Array(Some(vec![RespValue::BulkString(Some(Bytes::from_static(b"ping")))]));
        let cmd = RedisCommand::from_resp(value).unwrap();
        assert_eq!(
            dispatch_reply(&cmd),
            RespValue::SimpleString(Bytes::from_static(b"PONG"))
        );
    }

    #[test]
    fn hello_defaults_to_resp2() {
        let cmd = make_cmd(b"HELLO", vec![]);
        let mut state = ConnectionState::default();
        let resp = match dispatch(&cmd, &mut state) {
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

    #[test]
    fn hello_upgrades_to_resp3() {
        let cmd = make_cmd(b"HELLO", vec![b"3"]);
        let mut state = ConnectionState::default();
        assert_eq!(state.protocol_version, 2);
        match dispatch(&cmd, &mut state) {
            CommandResponse::Reply(_) => {}
            other => panic!("expected Reply, got {other:?}"),
        }
        assert_eq!(state.protocol_version, 3);
    }

    #[test]
    fn hello_rejects_invalid_version() {
        let cmd = make_cmd(b"HELLO", vec![b"4"]);
        let mut state = ConnectionState::default();
        match dispatch(&cmd, &mut state) {
            CommandResponse::Reply(RespValue::Error(msg)) => {
                assert!(msg.as_ref().starts_with(b"NOPROTO"));
            }
            other => panic!("expected NOPROTO error, got {other:?}"),
        }
    }

    #[test]
    fn quit_returns_close() {
        let cmd = make_cmd(b"QUIT", vec![]);
        let mut state = ConnectionState::default();
        match dispatch(&cmd, &mut state) {
            CommandResponse::Close(RespValue::SimpleString(msg)) => {
                assert_eq!(msg.as_ref(), b"OK");
            }
            other => panic!("expected Close(OK), got {other:?}"),
        }
    }
}
