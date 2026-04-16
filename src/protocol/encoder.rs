//! RESP encoder.
//!
//! Encodes `RespValue` into wire-format bytes. Supports both RESP2 and
//! RESP3 encoding: when `protocol_version == 2`, RESP3-only types are
//! downgraded to their RESP2 equivalents (e.g. Map -> Array of pairs,
//! Boolean -> Integer 0/1, Null -> BulkString(None)).
//!
//! Uses `itoa` and `ryu` for fast integer and float formatting — these
//! avoid the allocation and UTF-8 overhead of `format!()`.

use bytes::BytesMut;

use crate::protocol::types::RespValue;

/// Encode a RESP value into the wire format, returning a new buffer.
///
/// `protocol_version` controls whether RESP3 types are encoded natively (3)
/// or downgraded to RESP2 equivalents (2).
///
/// Note: RESP3 encoding is lossy for null variants — `BulkString(None)`,
/// `Array(None)`, and `Null` all encode to `_\r\n`, which parses back as
/// `Null`. Use `types::resp3_eq` for round-trip equality checks.
pub fn encode(value: &RespValue, protocol_version: u8) -> BytesMut {
    let mut buf = BytesMut::with_capacity(estimate_size(value));
    encode_into(&mut buf, value, protocol_version);
    buf
}

/// Encode a RESP value into an existing buffer. This avoids allocation when
/// batching multiple responses (e.g. pipelining).
pub fn encode_into(buf: &mut BytesMut, value: &RespValue, protocol_version: u8) {
    if protocol_version <= 2 {
        encode_resp2(buf, value);
    } else {
        encode_resp3(buf, value);
    }
}

// ---------------------------------------------------------------------------
// RESP3 native encoding
// ---------------------------------------------------------------------------

fn encode_resp3(buf: &mut BytesMut, value: &RespValue) {
    match value {
        RespValue::SimpleString(s) => {
            buf.extend_from_slice(b"+");
            buf.extend_from_slice(s);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            buf.extend_from_slice(b"-");
            buf.extend_from_slice(e);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            buf.extend_from_slice(b":");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(*n).as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(None) => {
            // In RESP3, null bulk strings become the native null type
            buf.extend_from_slice(b"_\r\n");
        }
        RespValue::BulkString(Some(data)) => {
            buf.extend_from_slice(b"$");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(data.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Array(None) => {
            // In RESP3, null arrays become the native null type
            buf.extend_from_slice(b"_\r\n");
        }
        RespValue::Array(Some(elements)) => {
            buf.extend_from_slice(b"*");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for elem in elements {
                encode_resp3(buf, elem);
            }
        }
        RespValue::Null => {
            buf.extend_from_slice(b"_\r\n");
        }
        RespValue::Boolean(b) => {
            if *b {
                buf.extend_from_slice(b"#t\r\n");
            } else {
                buf.extend_from_slice(b"#f\r\n");
            }
        }
        RespValue::Double(d) => {
            buf.extend_from_slice(b",");
            encode_double_value(buf, *d);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BigNumber(n) => {
            buf.extend_from_slice(b"(");
            buf.extend_from_slice(n);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkError(e) => {
            buf.extend_from_slice(b"!");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(e.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(e);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::VerbatimString { encoding, data } => {
            let total_len = 4 + data.len(); // 3 encoding bytes + ':' + data
            buf.extend_from_slice(b"=");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(total_len).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(encoding);
            buf.extend_from_slice(b":");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Map(pairs) => {
            buf.extend_from_slice(b"%");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(pairs.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for (key, val) in pairs {
                encode_resp3(buf, key);
                encode_resp3(buf, val);
            }
        }
        RespValue::Set(elements) => {
            buf.extend_from_slice(b"~");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for elem in elements {
                encode_resp3(buf, elem);
            }
        }
        RespValue::Push(elements) => {
            buf.extend_from_slice(b">");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for elem in elements {
                encode_resp3(buf, elem);
            }
        }
        RespValue::Attribute(pairs) => {
            buf.extend_from_slice(b"|");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(pairs.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for (key, val) in pairs {
                encode_resp3(buf, key);
                encode_resp3(buf, val);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RESP2 encoding (with RESP3 → RESP2 downgrades)
// ---------------------------------------------------------------------------

fn encode_resp2(buf: &mut BytesMut, value: &RespValue) {
    match value {
        // --- RESP2-native types: encode identically in both protocols ---
        RespValue::SimpleString(s) => {
            buf.extend_from_slice(b"+");
            buf.extend_from_slice(s);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Error(e) => {
            buf.extend_from_slice(b"-");
            buf.extend_from_slice(e);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(n) => {
            buf.extend_from_slice(b":");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(*n).as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkString(None) => {
            buf.extend_from_slice(b"$-1\r\n");
        }
        RespValue::BulkString(Some(data)) => {
            buf.extend_from_slice(b"$");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(data.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Array(None) => {
            buf.extend_from_slice(b"*-1\r\n");
        }
        RespValue::Array(Some(elements)) => {
            buf.extend_from_slice(b"*");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for elem in elements {
                encode_resp2(buf, elem);
            }
        }

        // --- RESP3 → RESP2 downgrades ---
        RespValue::Null => {
            // Null → null bulk string
            buf.extend_from_slice(b"$-1\r\n");
        }
        RespValue::Boolean(b) => {
            // Boolean → Integer 0/1
            if *b {
                buf.extend_from_slice(b":1\r\n");
            } else {
                buf.extend_from_slice(b":0\r\n");
            }
        }
        RespValue::Double(d) => {
            // Double → BulkString
            encode_resp2_bulk_from_double(buf, *d);
        }
        RespValue::BigNumber(n) => {
            // BigNumber → BulkString
            buf.extend_from_slice(b"$");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(n.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(n);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::BulkError(e) => {
            // BulkError → simple Error if safe, otherwise BulkString with ERR prefix.
            // Simple errors cannot contain \r or \n.
            if e.iter().any(|&b| b == b'\r' || b == b'\n') {
                // Unsafe for simple error — downgrade to bulk string
                buf.extend_from_slice(b"$");
                let mut itoa_buf = itoa::Buffer::new();
                buf.extend_from_slice(itoa_buf.format(e.len()).as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            } else {
                buf.extend_from_slice(b"-");
                buf.extend_from_slice(e);
                buf.extend_from_slice(b"\r\n");
            }
        }
        RespValue::VerbatimString { data, .. } => {
            // VerbatimString → BulkString (drop encoding prefix)
            buf.extend_from_slice(b"$");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(data.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(data);
            buf.extend_from_slice(b"\r\n");
        }
        RespValue::Map(pairs) => {
            // Map → flat Array of alternating key-value pairs
            buf.extend_from_slice(b"*");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(pairs.len() * 2).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for (key, val) in pairs {
                encode_resp2(buf, key);
                encode_resp2(buf, val);
            }
        }
        RespValue::Set(elements) => {
            // Set → Array
            buf.extend_from_slice(b"*");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for elem in elements {
                encode_resp2(buf, elem);
            }
        }
        RespValue::Push(elements) => {
            // Push → Array (RESP2 pub/sub uses arrays)
            buf.extend_from_slice(b"*");
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
            buf.extend_from_slice(b"\r\n");
            for elem in elements {
                encode_resp2(buf, elem);
            }
        }
        RespValue::Attribute(_) => {
            // Attributes are metadata — RESP2 has no equivalent.
            // At the top level, the actual reply follows the attribute and
            // is encoded separately by the caller. When nested inside a
            // container, we must emit a placeholder to keep element counts
            // consistent, so we encode as null bulk string.
            buf.extend_from_slice(b"$-1\r\n");
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write a double's string representation into `buf`.
/// Handles special values (inf, -inf, nan) per RESP3 spec.
fn encode_double_value(buf: &mut BytesMut, d: f64) {
    if d.is_infinite() {
        if d.is_sign_positive() {
            buf.extend_from_slice(b"inf");
        } else {
            buf.extend_from_slice(b"-inf");
        }
    } else if d.is_nan() {
        buf.extend_from_slice(b"nan");
    } else {
        let mut ryu_buf = ryu::Buffer::new();
        buf.extend_from_slice(ryu_buf.format(d).as_bytes());
    }
}

/// Encode a double as a RESP2 bulk string.
fn encode_resp2_bulk_from_double(buf: &mut BytesMut, d: f64) {
    // Format to a stack buffer, then wrap as a RESP2 bulk string.
    // ryu::Buffer is stack-allocated so the common (finite) path is alloc-free.
    let mut ryu_buf = ryu::Buffer::new();
    let formatted: &str = if d.is_infinite() {
        if d.is_sign_positive() { "inf" } else { "-inf" }
    } else if d.is_nan() {
        "nan"
    } else {
        ryu_buf.format(d)
    };

    buf.extend_from_slice(b"$");
    let mut itoa_buf = itoa::Buffer::new();
    buf.extend_from_slice(itoa_buf.format(formatted.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(formatted.as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Rough estimate of encoded size for pre-allocating the output buffer.
/// Does not need to be exact — just avoids excessive reallocation.
fn estimate_size(value: &RespValue) -> usize {
    match value {
        RespValue::SimpleString(s) => 3 + s.len(),
        RespValue::Error(e) => 3 + e.len(),
        RespValue::Integer(_) => 24,
        RespValue::BulkString(None) => 5,
        RespValue::BulkString(Some(data)) => 16 + data.len(),
        RespValue::Array(None) => 5,
        RespValue::Array(Some(elems)) => 16 + elems.iter().map(estimate_size).sum::<usize>(),
        RespValue::Null => 3,
        RespValue::Boolean(_) => 4,
        RespValue::Double(_) => 32,
        RespValue::BigNumber(n) => 3 + n.len(),
        RespValue::BulkError(e) => 16 + e.len(),
        RespValue::VerbatimString { data, .. } => 20 + data.len(),
        RespValue::Map(pairs) => {
            16 + pairs
                .iter()
                .map(|(k, v)| estimate_size(k) + estimate_size(v))
                .sum::<usize>()
        }
        RespValue::Set(elems) => 16 + elems.iter().map(estimate_size).sum::<usize>(),
        RespValue::Push(elems) => 16 + elems.iter().map(estimate_size).sum::<usize>(),
        RespValue::Attribute(pairs) => {
            16 + pairs
                .iter()
                .map(|(k, v)| estimate_size(k) + estimate_size(v))
                .sum::<usize>()
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    // --- RESP3 encoding ---

    #[test]
    fn encode_simple_string() {
        let buf = encode(&RespValue::SimpleString(Bytes::from_static(b"OK")), 3);
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn encode_error() {
        let buf = encode(&RespValue::Error(Bytes::from_static(b"ERR something wrong")), 3);
        assert_eq!(&buf[..], b"-ERR something wrong\r\n");
    }

    #[test]
    fn encode_integer() {
        assert_eq!(&encode(&RespValue::Integer(42), 3)[..], b":42\r\n");
        assert_eq!(&encode(&RespValue::Integer(-17), 3)[..], b":-17\r\n");
        assert_eq!(&encode(&RespValue::Integer(0), 3)[..], b":0\r\n");
    }

    #[test]
    fn encode_bulk_string() {
        let buf = encode(&RespValue::BulkString(Some(Bytes::from_static(b"hello"))), 3);
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn encode_bulk_string_empty() {
        let buf = encode(&RespValue::BulkString(Some(Bytes::from_static(b""))), 3);
        assert_eq!(&buf[..], b"$0\r\n\r\n");
    }

    #[test]
    fn encode_null_bulk_string_resp3() {
        // In RESP3, null bulk string becomes native null
        let buf = encode(&RespValue::BulkString(None), 3);
        assert_eq!(&buf[..], b"_\r\n");
    }

    #[test]
    fn encode_null_bulk_string_resp2() {
        let buf = encode(&RespValue::BulkString(None), 2);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn encode_array() {
        let val = RespValue::Array(Some(vec![
            RespValue::Integer(1),
            RespValue::Integer(2),
            RespValue::Integer(3),
        ]));
        let buf = encode(&val, 3);
        assert_eq!(&buf[..], b"*3\r\n:1\r\n:2\r\n:3\r\n");
    }

    #[test]
    fn encode_array_empty() {
        let buf = encode(&RespValue::Array(Some(vec![])), 3);
        assert_eq!(&buf[..], b"*0\r\n");
    }

    #[test]
    fn encode_null_array_resp3() {
        let buf = encode(&RespValue::Array(None), 3);
        assert_eq!(&buf[..], b"_\r\n");
    }

    #[test]
    fn encode_null_array_resp2() {
        let buf = encode(&RespValue::Array(None), 2);
        assert_eq!(&buf[..], b"*-1\r\n");
    }

    #[test]
    fn encode_null() {
        assert_eq!(&encode(&RespValue::Null, 3)[..], b"_\r\n");
    }

    #[test]
    fn encode_null_resp2() {
        assert_eq!(&encode(&RespValue::Null, 2)[..], b"$-1\r\n");
    }

    #[test]
    fn encode_boolean() {
        assert_eq!(&encode(&RespValue::Boolean(true), 3)[..], b"#t\r\n");
        assert_eq!(&encode(&RespValue::Boolean(false), 3)[..], b"#f\r\n");
    }

    #[test]
    fn encode_boolean_resp2() {
        assert_eq!(&encode(&RespValue::Boolean(true), 2)[..], b":1\r\n");
        assert_eq!(&encode(&RespValue::Boolean(false), 2)[..], b":0\r\n");
    }

    #[test]
    fn encode_double() {
        let buf = encode(&RespValue::Double(1.23), 3);
        let s = std::str::from_utf8(&buf).unwrap();
        assert!(s.starts_with(','));
        assert!(s.ends_with("\r\n"));
        // Parse the value back to verify it's a valid double
        let inner = &s[1..s.len() - 2];
        let parsed: f64 = inner.parse().unwrap();
        assert!((parsed - 1.23).abs() < 1e-10);
    }

    #[test]
    fn encode_double_special() {
        assert_eq!(&encode(&RespValue::Double(f64::INFINITY), 3)[..], b",inf\r\n");
        assert_eq!(&encode(&RespValue::Double(f64::NEG_INFINITY), 3)[..], b",-inf\r\n");
        assert_eq!(&encode(&RespValue::Double(f64::NAN), 3)[..], b",nan\r\n");
    }

    #[test]
    fn encode_double_resp2() {
        // Double → BulkString in RESP2
        let buf = encode(&RespValue::Double(f64::INFINITY), 2);
        assert_eq!(&buf[..], b"$3\r\ninf\r\n");
    }

    #[test]
    fn encode_big_number() {
        let buf = encode(&RespValue::BigNumber(Bytes::from_static(b"12345678901234567890")), 3);
        assert_eq!(&buf[..], b"(12345678901234567890\r\n");
    }

    #[test]
    fn encode_big_number_resp2() {
        let buf = encode(&RespValue::BigNumber(Bytes::from_static(b"12345678901234567890")), 2);
        assert_eq!(&buf[..], b"$20\r\n12345678901234567890\r\n");
    }

    #[test]
    fn encode_bulk_error() {
        let buf = encode(&RespValue::BulkError(Bytes::from_static(b"SYNTAX invalid syntax")), 3);
        assert_eq!(&buf[..], b"!21\r\nSYNTAX invalid syntax\r\n");
    }

    #[test]
    fn encode_bulk_error_resp2() {
        let buf = encode(&RespValue::BulkError(Bytes::from_static(b"SYNTAX invalid syntax")), 2);
        assert_eq!(&buf[..], b"-SYNTAX invalid syntax\r\n");
    }

    #[test]
    fn encode_verbatim_string() {
        let buf = encode(
            &RespValue::VerbatimString {
                encoding: *b"txt",
                data: Bytes::from_static(b"Some string"),
            },
            3,
        );
        assert_eq!(&buf[..], b"=15\r\ntxt:Some string\r\n");
    }

    #[test]
    fn encode_verbatim_string_resp2() {
        // VerbatimString → BulkString (drop encoding)
        let buf = encode(
            &RespValue::VerbatimString {
                encoding: *b"txt",
                data: Bytes::from_static(b"Some string"),
            },
            2,
        );
        assert_eq!(&buf[..], b"$11\r\nSome string\r\n");
    }

    #[test]
    fn encode_map() {
        let val = RespValue::Map(vec![
            (
                RespValue::SimpleString(Bytes::from_static(b"first")),
                RespValue::Integer(1),
            ),
            (
                RespValue::SimpleString(Bytes::from_static(b"second")),
                RespValue::Integer(2),
            ),
        ]);
        let buf = encode(&val, 3);
        assert_eq!(&buf[..], b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
    }

    #[test]
    fn encode_map_resp2() {
        // Map → flat Array of alternating key-value pairs
        let val = RespValue::Map(vec![(
            RespValue::SimpleString(Bytes::from_static(b"k")),
            RespValue::Integer(1),
        )]);
        let buf = encode(&val, 2);
        assert_eq!(&buf[..], b"*2\r\n+k\r\n:1\r\n");
    }

    #[test]
    fn encode_set() {
        let val = RespValue::Set(vec![
            RespValue::SimpleString(Bytes::from_static(b"a")),
            RespValue::SimpleString(Bytes::from_static(b"b")),
        ]);
        assert_eq!(&encode(&val, 3)[..], b"~2\r\n+a\r\n+b\r\n");
    }

    #[test]
    fn encode_set_resp2() {
        let val = RespValue::Set(vec![RespValue::Integer(1)]);
        assert_eq!(&encode(&val, 2)[..], b"*1\r\n:1\r\n");
    }

    #[test]
    fn encode_push() {
        let val = RespValue::Push(vec![
            RespValue::SimpleString(Bytes::from_static(b"subscribe")),
            RespValue::SimpleString(Bytes::from_static(b"ch")),
        ]);
        assert_eq!(&encode(&val, 3)[..], b">2\r\n+subscribe\r\n+ch\r\n");
    }

    #[test]
    fn encode_attribute() {
        let val = RespValue::Attribute(vec![(
            RespValue::SimpleString(Bytes::from_static(b"ttl")),
            RespValue::Integer(3600),
        )]);
        assert_eq!(&encode(&val, 3)[..], b"|1\r\n+ttl\r\n:3600\r\n");
    }

    #[test]
    fn encode_attribute_resp2() {
        // Attributes encode as null bulk string in RESP2
        let val = RespValue::Attribute(vec![(
            RespValue::SimpleString(Bytes::from_static(b"ttl")),
            RespValue::Integer(3600),
        )]);
        assert_eq!(&encode(&val, 2)[..], b"$-1\r\n");
    }
}
