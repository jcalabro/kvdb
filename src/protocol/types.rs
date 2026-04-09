//! RESP value types and command representation.
//!
//! Covers all RESP2 and RESP3 types. The `RespValue` enum is the core
//! wire-level representation; `RedisCommand` is the parsed command form
//! used by the dispatch layer.

use bytes::Bytes;

use crate::error::CommandError;

/// A value in the RESP protocol (covers both RESP2 and RESP3 types).
///
/// Note: `Eq` is intentionally not derived because `Double(f64)` contains
/// `f64` which does not implement `Eq` (NaN != NaN). Use [`resp3_eq`] for
/// equality comparison that accounts for RESP3 null normalization.
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    // --- RESP2 types ---
    /// Simple string: `+OK\r\n`
    ///
    /// Per RESP spec, simple strings cannot contain CR or LF. The parser
    /// enforces CRLF termination but does not reject other control characters
    /// or non-UTF-8 bytes — downstream code should validate if needed.
    SimpleString(Bytes),
    /// Error: `-ERR message\r\n`
    ///
    /// Same content constraints as `SimpleString`.
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
                // Uppercase the command name for case-insensitive dispatch.
                // Skip allocation if already uppercase (common case: redis-cli
                // and most client libraries send uppercase commands).
                if data.iter().all(|b| !b.is_ascii_lowercase()) {
                    name = Some(data);
                } else {
                    name = Some(Bytes::from(data.to_ascii_uppercase()));
                }
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

/// Compare two `RespValue`s for equivalence under RESP3 null normalization.
///
/// In RESP3, `BulkString(None)` and `Array(None)` both encode to `_\r\n`
/// (the native null type), which parses back as `Null`. This function
/// treats those three representations as equivalent. It also handles
/// `Double(NaN) == Double(NaN)` which standard `PartialEq` does not.
pub fn resp3_eq(a: &RespValue, b: &RespValue) -> bool {
    match (a, b) {
        // Null equivalences (RESP3 encoder normalizes these)
        (RespValue::BulkString(None), RespValue::Null)
        | (RespValue::Null, RespValue::BulkString(None))
        | (RespValue::Array(None), RespValue::Null)
        | (RespValue::Null, RespValue::Array(None))
        | (RespValue::Null, RespValue::Null)
        | (RespValue::BulkString(None), RespValue::BulkString(None))
        | (RespValue::Array(None), RespValue::Array(None)) => true,

        (RespValue::SimpleString(a), RespValue::SimpleString(b)) => a == b,
        (RespValue::Error(a), RespValue::Error(b)) => a == b,
        (RespValue::Integer(a), RespValue::Integer(b)) => a == b,
        (RespValue::BulkString(Some(a)), RespValue::BulkString(Some(b))) => a == b,
        (RespValue::Boolean(a), RespValue::Boolean(b)) => a == b,
        (RespValue::Double(a), RespValue::Double(b)) => (a.is_nan() && b.is_nan()) || a == b,
        (RespValue::BigNumber(a), RespValue::BigNumber(b)) => a == b,
        (RespValue::BulkError(a), RespValue::BulkError(b)) => a == b,
        (
            RespValue::VerbatimString { encoding: ea, data: da },
            RespValue::VerbatimString { encoding: eb, data: db },
        ) => ea == eb && da == db,
        (RespValue::Array(Some(a)), RespValue::Array(Some(b))) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| resp3_eq(x, y))
        }
        (RespValue::Map(a), RespValue::Map(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((k1, v1), (k2, v2))| resp3_eq(k1, k2) && resp3_eq(v1, v2))
        }
        (RespValue::Set(a), RespValue::Set(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| resp3_eq(x, y))
        }
        (RespValue::Push(a), RespValue::Push(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| resp3_eq(x, y))
        }
        (RespValue::Attribute(a), RespValue::Attribute(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((k1, v1), (k2, v2))| resp3_eq(k1, k2) && resp3_eq(v1, v2))
        }
        _ => false,
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
