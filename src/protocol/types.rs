//! RESP value types and command representation.
//!
//! Covers all RESP2 and RESP3 types. The `RespValue` enum is the core
//! wire-level representation; `RedisCommand` is the parsed command form
//! used by the dispatch layer.

use bytes::Bytes;

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
}
