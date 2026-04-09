//! RESP value types and command representation.
//!
//! Covers all RESP2 and RESP3 types. The `RespValue` enum is the core
//! wire-level representation; `RedisCommand` is the parsed command form
//! used by the dispatch layer.

use bytes::Bytes;

use crate::error::CommandError;

/// A value in the RESP protocol (covers both RESP2 and RESP3 types).
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    // --- RESP2 types ---
    /// Simple string: `+OK\r\n`
    SimpleString(Bytes),
    /// Error: `-ERR message\r\n`
    Error(Bytes),
    /// Integer: `:42\r\n`
    Integer(i64),
    /// Bulk string: `$5\r\nhello\r\n` or null bulk `$-1\r\n`
    BulkString(Option<Bytes>),
    /// Array: `*2\r\n...` or null array `*-1\r\n`
    Array(Option<Vec<RespValue>>),

    // --- RESP3 types ---
    /// Null: `_\r\n`
    Null,
    /// Boolean: `#t\r\n` or `#f\r\n`
    Boolean(bool),
    /// Double: `,3.14\r\n`
    Double(f64),
    /// Big number: `(3492890328409238509324850943850943825024385\r\n`
    BigNumber(Bytes),
    /// Bulk error: `!<len>\r\n<error>\r\n`
    BulkError(Bytes),
    /// Verbatim string: `=<len>\r\n<encoding>:<data>\r\n`
    VerbatimString { encoding: [u8; 3], data: Bytes },
    /// Map: `%<count>\r\n<key><value>...`
    Map(Vec<(RespValue, RespValue)>),
    /// Set: `~<count>\r\n<element>...`
    Set(Vec<RespValue>),
    /// Push: `><count>\r\n<element>...`
    Push(Vec<RespValue>),
    /// Attribute: `|<count>\r\n<key><value>...` (metadata attached to next value)
    Attribute(Vec<(RespValue, RespValue)>),
}

/// A parsed Redis command: the command name plus its arguments.
///
/// Both name and args are kept as raw `Bytes` to support binary-safe
/// keys and values without unnecessary UTF-8 validation.
#[derive(Debug, Clone)]
pub struct RedisCommand {
    /// Uppercased command name (e.g. `GET`, `SET`, `HGETALL`).
    pub name: Bytes,
    /// Command arguments, in order.
    pub args: Vec<Bytes>,
}

impl RespValue {
    /// Returns the value as an integer, if it is one.
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Returns the value as bytes, for simple strings and bulk strings.
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            RespValue::SimpleString(b) | RespValue::BulkString(Some(b)) => Some(b),
            _ => None,
        }
    }

    /// Returns `true` if this is a null value (RESP2 null bulk/array or RESP3 null).
    pub fn is_null(&self) -> bool {
        matches!(
            self,
            RespValue::Null | RespValue::BulkString(None) | RespValue::Array(None)
        )
    }

    /// Convenience constructor for an OK simple string.
    pub fn ok() -> Self {
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    }

    /// Convenience constructor for an error response.
    pub fn err(msg: impl Into<String>) -> Self {
        RespValue::Error(Bytes::from(msg.into()))
    }
}

impl RedisCommand {
    /// Parse a `RespValue` (expected to be an Array of BulkStrings) into
    /// a `RedisCommand`. This is the standard way Redis commands arrive
    /// on the wire.
    ///
    /// Returns `Err(CommandError)` if the value is not a valid command
    /// (e.g. empty array, non-bulk-string elements).
    pub fn from_resp(value: RespValue) -> Result<Self, CommandError> {
        let elements = match value {
            RespValue::Array(Some(elems)) => elems,
            _ => {
                return Err(CommandError::Generic("expected array for command".into()));
            }
        };

        if elements.is_empty() {
            return Err(CommandError::Generic("empty command array".into()));
        }

        let mut args = Vec::with_capacity(elements.len() - 1);
        let mut name = None;

        for (i, elem) in elements.into_iter().enumerate() {
            let data = match elem {
                RespValue::BulkString(Some(b)) => b,
                _ => {
                    return Err(CommandError::Generic("command elements must be bulk strings".into()));
                }
            };

            if i == 0 {
                // Uppercase the command name for case-insensitive dispatch
                name = Some(Bytes::from(data.to_ascii_uppercase()));
            } else {
                args.push(data);
            }
        }

        Ok(RedisCommand {
            name: name.expect("non-empty array guarantees first element"),
            args,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp_value_as_integer() {
        assert_eq!(RespValue::Integer(42).as_integer(), Some(42));
        assert_eq!(RespValue::SimpleString(Bytes::new()).as_integer(), None);
    }

    #[test]
    fn resp_value_as_bytes() {
        let bs = Bytes::from_static(b"hello");
        assert_eq!(RespValue::BulkString(Some(bs.clone())).as_bytes(), Some(&bs));
        assert_eq!(RespValue::SimpleString(bs.clone()).as_bytes(), Some(&bs));
        assert_eq!(RespValue::Integer(1).as_bytes(), None);
    }

    #[test]
    fn resp_value_is_null() {
        assert!(RespValue::Null.is_null());
        assert!(RespValue::BulkString(None).is_null());
        assert!(RespValue::Array(None).is_null());
        assert!(!RespValue::Integer(0).is_null());
    }

    #[test]
    fn redis_command_from_resp_basic() {
        let value = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"set"))),
            RespValue::BulkString(Some(Bytes::from_static(b"foo"))),
            RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
        ]));

        let cmd = RedisCommand::from_resp(value).unwrap();
        assert_eq!(&cmd.name[..], b"SET"); // uppercased
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(&cmd.args[0][..], b"foo");
        assert_eq!(&cmd.args[1][..], b"bar");
    }

    #[test]
    fn redis_command_from_resp_empty_array() {
        let value = RespValue::Array(Some(vec![]));
        assert!(RedisCommand::from_resp(value).is_err());
    }

    #[test]
    fn redis_command_from_resp_not_array() {
        let value = RespValue::SimpleString(Bytes::from_static(b"PING"));
        assert!(RedisCommand::from_resp(value).is_err());
    }

    #[test]
    fn redis_command_from_resp_non_bulk_elements() {
        let value = RespValue::Array(Some(vec![RespValue::Integer(42)]));
        assert!(RedisCommand::from_resp(value).is_err());
    }

    #[test]
    fn redis_command_case_insensitive() {
        let value = RespValue::Array(Some(vec![RespValue::BulkString(Some(Bytes::from_static(b"PiNg")))]));
        let cmd = RedisCommand::from_resp(value).unwrap();
        assert_eq!(&cmd.name[..], b"PING");
    }
}
