//! Command dispatch and implementations.
//!
//! Commands are parsed from RESP arrays of bulk strings, dispatched by
//! name (case-insensitive), and executed against the storage layer.
//! Each command module (string, hash, set, etc.) implements the
//! individual command handlers.
//!
//! M2 will implement the dispatch table and stub commands (PING, ECHO).
//! M4+ will add real data structure commands.

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::types::{RedisCommand, RespValue};

/// Dispatch a parsed command to the appropriate handler.
///
/// Returns the RESP response value. For unknown commands, returns
/// an error response.
pub fn dispatch(cmd: &RedisCommand) -> RespValue {
    // Uppercase the command name for case-insensitive matching.
    let name = cmd.name.to_ascii_uppercase();

    match name.as_slice() {
        b"PING" => handle_ping(&cmd.args),
        b"ECHO" => handle_echo(&cmd.args),
        _ => {
            let name_str = String::from_utf8_lossy(&cmd.name).to_string();
            RespValue::Error(Bytes::from(format!(
                "ERR unknown command '{}', with args beginning with: ",
                name_str
            )))
        }
    }
}

fn handle_ping(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        RespValue::SimpleString(Bytes::from_static(b"PONG"))
    } else {
        RespValue::BulkString(Some(args[0].clone()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_no_args() {
        let cmd = RedisCommand {
            name: Bytes::from_static(b"PING"),
            args: vec![],
        };
        assert_eq!(dispatch(&cmd), RespValue::SimpleString(Bytes::from_static(b"PONG")));
    }

    #[test]
    fn ping_with_message() {
        let cmd = RedisCommand {
            name: Bytes::from_static(b"PING"),
            args: vec![Bytes::from_static(b"hello")],
        };
        assert_eq!(
            dispatch(&cmd),
            RespValue::BulkString(Some(Bytes::from_static(b"hello")))
        );
    }

    #[test]
    fn echo_returns_argument() {
        let cmd = RedisCommand {
            name: Bytes::from_static(b"ECHO"),
            args: vec![Bytes::from_static(b"world")],
        };
        assert_eq!(
            dispatch(&cmd),
            RespValue::BulkString(Some(Bytes::from_static(b"world")))
        );
    }

    #[test]
    fn unknown_command_returns_error() {
        let cmd = RedisCommand {
            name: Bytes::from_static(b"FAKECMD"),
            args: vec![],
        };
        match dispatch(&cmd) {
            RespValue::Error(_) => {} // expected
            other => panic!("expected error, got {:?}", other),
        }
    }

    #[test]
    fn command_dispatch_is_case_insensitive() {
        let cmd = RedisCommand {
            name: Bytes::from_static(b"ping"),
            args: vec![],
        };
        assert_eq!(dispatch(&cmd), RespValue::SimpleString(Bytes::from_static(b"PONG")));
    }
}
