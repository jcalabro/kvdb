//! Incremental RESP parser.
//!
//! Feed bytes into a `BytesMut` buffer, then call `parse()` to extract
//! complete frames. Returns `Ok(None)` when more data is needed (partial
//! frame), `Ok(Some(value))` on success, or `Err` on malformed input.
//!
//! # Design
//!
//! Single-pass parser with zero-copy bulk string extraction. The parser
//! walks `&[u8]` once, building a lightweight `Blueprint` (byte offsets
//! instead of `Bytes` references). If the buffer is incomplete, it returns
//! `Ok(None)` without modifying the `BytesMut`. On success, it calls
//! `split_to().freeze()` once and materializes `Bytes::slice()` references
//! from the blueprint — bulk string payloads are zero-copy.

use bytes::{Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::protocol::types::RespValue;

/// Maximum nesting depth for aggregate types (arrays, maps, sets, etc.).
/// Prevents stack overflow from maliciously crafted deeply-nested inputs.
const MAX_DEPTH: usize = 128;

/// Maximum length for simple (line-delimited) values.
/// Prevents unbounded memory usage from a missing CRLF.
const MAX_LINE_LENGTH: usize = 64 * 1024;

/// Maximum bulk string / bulk error / verbatim string length.
/// Matches Redis default of 512 MB.
const MAX_BULK_LENGTH: i64 = 512 * 1024 * 1024;

/// Parse one complete RESP value from the front of `buf`.
///
/// On success, the consumed bytes are drained from `buf` and the parsed
/// value is returned. Returns `Ok(None)` if the buffer doesn't contain
/// a complete frame yet (caller should read more data and retry).
pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // Single pass over &[u8] — returns blueprint + total bytes consumed,
    // or None if incomplete. Buffer is not modified on None.
    let mut pos = 0;
    let blueprint = match try_parse(&buf[..], &mut pos, 0)? {
        Some(bp) => bp,
        None => return Ok(None),
    };

    // Commit: extract the frame as frozen Bytes (one atomic refcount op)
    let frame = buf.split_to(pos).freeze();

    // Materialize: convert byte-offset blueprint into Bytes-slice RespValue
    Ok(Some(blueprint.materialize(&frame)))
}

// ---------------------------------------------------------------------------
// Blueprint: lightweight parse result using byte offsets
// ---------------------------------------------------------------------------

/// A parsed RESP value represented as byte offsets into the frame buffer.
/// This avoids creating `Bytes` references until we know the frame is
/// complete, which means incomplete reads never modify the buffer.
enum Blueprint {
    SimpleString(usize, usize),
    Error(usize, usize),
    Integer(i64),
    BulkString(Option<(usize, usize)>),
    Array(Option<Vec<Blueprint>>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(usize, usize),
    BulkError(usize, usize),
    VerbatimString {
        encoding: [u8; 3],
        data_start: usize,
        data_end: usize,
    },
    Map(Vec<(Blueprint, Blueprint)>),
    Set(Vec<Blueprint>),
    Push(Vec<Blueprint>),
    Attribute(Vec<(Blueprint, Blueprint)>),
}

impl Blueprint {
    /// Convert byte-offset blueprint into a real `RespValue` with zero-copy
    /// `Bytes::slice()` references into the frozen frame.
    fn materialize(self, frame: &Bytes) -> RespValue {
        match self {
            Blueprint::SimpleString(s, e) => RespValue::SimpleString(frame.slice(s..e)),
            Blueprint::Error(s, e) => RespValue::Error(frame.slice(s..e)),
            Blueprint::Integer(n) => RespValue::Integer(n),
            Blueprint::BulkString(None) => RespValue::BulkString(None),
            Blueprint::BulkString(Some((s, e))) => RespValue::BulkString(Some(frame.slice(s..e))),
            Blueprint::Array(None) => RespValue::Array(None),
            Blueprint::Array(Some(elems)) => {
                RespValue::Array(Some(elems.into_iter().map(|bp| bp.materialize(frame)).collect()))
            }
            Blueprint::Null => RespValue::Null,
            Blueprint::Boolean(b) => RespValue::Boolean(b),
            Blueprint::Double(d) => RespValue::Double(d),
            Blueprint::BigNumber(s, e) => RespValue::BigNumber(frame.slice(s..e)),
            Blueprint::BulkError(s, e) => RespValue::BulkError(frame.slice(s..e)),
            Blueprint::VerbatimString {
                encoding,
                data_start,
                data_end,
            } => RespValue::VerbatimString {
                encoding,
                data: frame.slice(data_start..data_end),
            },
            Blueprint::Map(pairs) => RespValue::Map(
                pairs
                    .into_iter()
                    .map(|(k, v)| (k.materialize(frame), v.materialize(frame)))
                    .collect(),
            ),
            Blueprint::Set(elems) => RespValue::Set(elems.into_iter().map(|bp| bp.materialize(frame)).collect()),
            Blueprint::Push(elems) => RespValue::Push(elems.into_iter().map(|bp| bp.materialize(frame)).collect()),
            Blueprint::Attribute(pairs) => RespValue::Attribute(
                pairs
                    .into_iter()
                    .map(|(k, v)| (k.materialize(frame), v.materialize(frame)))
                    .collect(),
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Single-pass parser
// ---------------------------------------------------------------------------

/// Incomplete sentinel — returned when the buffer doesn't contain enough
/// data for a complete frame. This is distinct from protocol errors.
const INCOMPLETE: Option<Blueprint> = None;

/// Try to parse one RESP value from `buf` starting at `*pos`.
/// Advances `*pos` past the consumed bytes on success.
/// Returns `Ok(None)` if incomplete, `Err` on malformed input.
/// Does NOT modify the buffer — only reads through `&[u8]`.
fn try_parse(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<Blueprint>, ProtocolError> {
    if depth > MAX_DEPTH {
        return Err(ProtocolError::NestingTooDeep { max: MAX_DEPTH });
    }
    if *pos >= buf.len() {
        return Ok(INCOMPLETE);
    }

    let type_byte = buf[*pos];
    *pos += 1;

    match type_byte {
        b'+' => try_parse_simple_string(buf, pos),
        b'-' => try_parse_error(buf, pos),
        b':' => try_parse_integer(buf, pos),
        b'$' => try_parse_bulk_string(buf, pos),
        b'*' => try_parse_array(buf, pos, depth),
        b'_' => try_parse_null(buf, pos),
        b'#' => try_parse_boolean(buf, pos),
        b',' => try_parse_double(buf, pos),
        b'(' => try_parse_big_number(buf, pos),
        b'!' => try_parse_bulk_error(buf, pos),
        b'=' => try_parse_verbatim_string(buf, pos),
        b'%' => try_parse_map(buf, pos, depth),
        b'~' => try_parse_set(buf, pos, depth),
        b'>' => try_parse_push(buf, pos, depth),
        b'|' => try_parse_attribute(buf, pos, depth),
        byte => Err(ProtocolError::InvalidFormat { byte }),
    }
}

/// Try to find \r\n starting at `*pos`. Returns the content range (start, end)
/// where end points to the \r. Advances `*pos` past the \r\n.
/// Returns None if \r\n not found (incomplete).
#[inline]
fn try_read_line(buf: &[u8], pos: &mut usize) -> Result<Option<(usize, usize)>, ProtocolError> {
    let start = *pos;
    let remaining = &buf[start..];

    match find_crlf(remaining) {
        Some(rel_cr) => {
            let content_len = rel_cr;
            if content_len > MAX_LINE_LENGTH {
                return Err(ProtocolError::LineTooLong {
                    len: content_len,
                    max: MAX_LINE_LENGTH,
                });
            }
            let end = start + rel_cr;
            *pos = end + 2; // skip \r\n
            Ok(Some((start, end)))
        }
        None => {
            // Check for obviously-too-long lines even before CRLF arrives
            if remaining.len() > MAX_LINE_LENGTH {
                return Err(ProtocolError::LineTooLong {
                    len: remaining.len(),
                    max: MAX_LINE_LENGTH,
                });
            }
            Ok(None)
        }
    }
}

fn try_parse_simple_string(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };
    Ok(Some(Blueprint::SimpleString(start, end)))
}

fn try_parse_error(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };
    Ok(Some(Blueprint::Error(start, end)))
}

fn try_parse_integer(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };
    let n = parse_int_from_slice(&buf[start..end])?;
    Ok(Some(Blueprint::Integer(n)))
}

fn try_parse_bulk_string(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let len = parse_int_from_slice(&buf[start..end])?;

    if len == -1 {
        return Ok(Some(Blueprint::BulkString(None)));
    }
    if len < 0 {
        return Err(ProtocolError::InvalidLength(len));
    }
    if len > MAX_BULK_LENGTH {
        return Err(ProtocolError::InvalidLength(len));
    }

    let data_len = len as usize;
    let needed = *pos + data_len + 2; // data + \r\n
    if buf.len() < needed {
        return Ok(INCOMPLETE);
    }

    let data_start = *pos;
    let data_end = data_start + data_len;
    *pos = data_end + 2; // skip data + \r\n
    Ok(Some(Blueprint::BulkString(Some((data_start, data_end)))))
}

fn try_parse_array(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let count = parse_int_from_slice(&buf[start..end])?;

    if count == -1 {
        return Ok(Some(Blueprint::Array(None)));
    }
    if count < 0 {
        return Err(ProtocolError::InvalidLength(count));
    }

    let count = count as usize;
    if !plausible(buf, *pos, count) {
        return Ok(INCOMPLETE);
    }
    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        match try_parse(buf, pos, depth + 1)? {
            Some(bp) => elements.push(bp),
            None => return Ok(INCOMPLETE),
        }
    }
    Ok(Some(Blueprint::Array(Some(elements))))
}

fn try_parse_null(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };
    if start != end {
        return Err(ProtocolError::InvalidFormat { byte: buf[start] });
    }
    Ok(Some(Blueprint::Null))
}

fn try_parse_boolean(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };
    if end - start != 1 {
        return Err(ProtocolError::InvalidFormat {
            byte: if start < end { buf[start] } else { b'#' },
        });
    }
    match buf[start] {
        b't' => Ok(Some(Blueprint::Boolean(true))),
        b'f' => Ok(Some(Blueprint::Boolean(false))),
        byte => Err(ProtocolError::InvalidFormat { byte }),
    }
}

fn try_parse_double(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };
    let s = std::str::from_utf8(&buf[start..end]).map_err(|_| ProtocolError::InvalidFormat { byte: buf[start] })?;

    let val = match s {
        "inf" | "+inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        "nan" => f64::NAN,
        _ => s
            .parse::<f64>()
            .map_err(|_| ProtocolError::InvalidFormat { byte: buf[start] })?,
    };
    Ok(Some(Blueprint::Double(val)))
}

fn try_parse_big_number(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };
    Ok(Some(Blueprint::BigNumber(start, end)))
}

fn try_parse_bulk_error(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let len = parse_int_from_slice(&buf[start..end])?;
    if len < 0 {
        return Err(ProtocolError::InvalidLength(len));
    }
    if len > MAX_BULK_LENGTH {
        return Err(ProtocolError::InvalidLength(len));
    }

    let data_len = len as usize;
    let needed = *pos + data_len + 2;
    if buf.len() < needed {
        return Ok(INCOMPLETE);
    }

    let data_start = *pos;
    let data_end = data_start + data_len;
    *pos = data_end + 2;
    Ok(Some(Blueprint::BulkError(data_start, data_end)))
}

fn try_parse_verbatim_string(buf: &[u8], pos: &mut usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let len = parse_int_from_slice(&buf[start..end])?;
    if len < 0 {
        return Err(ProtocolError::InvalidLength(len));
    }
    if len > MAX_BULK_LENGTH {
        return Err(ProtocolError::InvalidLength(len));
    }

    let data_len = len as usize;
    let needed = *pos + data_len + 2;
    if buf.len() < needed {
        return Ok(INCOMPLETE);
    }

    // Format: <3-byte-encoding>:<data>
    if data_len < 4 || buf[*pos + 3] != b':' {
        return Err(ProtocolError::InvalidFormat { byte: b'=' });
    }

    let encoding = [buf[*pos], buf[*pos + 1], buf[*pos + 2]];
    let data_start = *pos + 4;
    let data_end = *pos + data_len;
    *pos += data_len + 2;
    Ok(Some(Blueprint::VerbatimString {
        encoding,
        data_start,
        data_end,
    }))
}

fn try_parse_map(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let count = parse_int_from_slice(&buf[start..end])?;
    if count < 0 {
        return Err(ProtocolError::InvalidLength(count));
    }

    let count = count as usize;
    if !plausible(buf, *pos, count * 2) {
        return Ok(INCOMPLETE);
    }
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let key = match try_parse(buf, pos, depth + 1)? {
            Some(bp) => bp,
            None => return Ok(INCOMPLETE),
        };
        let val = match try_parse(buf, pos, depth + 1)? {
            Some(bp) => bp,
            None => return Ok(INCOMPLETE),
        };
        pairs.push((key, val));
    }
    Ok(Some(Blueprint::Map(pairs)))
}

fn try_parse_set(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let count = parse_int_from_slice(&buf[start..end])?;
    if count < 0 {
        return Err(ProtocolError::InvalidLength(count));
    }

    let count = count as usize;
    if !plausible(buf, *pos, count) {
        return Ok(INCOMPLETE);
    }
    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        match try_parse(buf, pos, depth + 1)? {
            Some(bp) => elements.push(bp),
            None => return Ok(INCOMPLETE),
        }
    }
    Ok(Some(Blueprint::Set(elements)))
}

fn try_parse_push(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let count = parse_int_from_slice(&buf[start..end])?;
    if count < 0 {
        return Err(ProtocolError::InvalidLength(count));
    }

    let count = count as usize;
    if !plausible(buf, *pos, count) {
        return Ok(INCOMPLETE);
    }
    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        match try_parse(buf, pos, depth + 1)? {
            Some(bp) => elements.push(bp),
            None => return Ok(INCOMPLETE),
        }
    }
    Ok(Some(Blueprint::Push(elements)))
}

fn try_parse_attribute(buf: &[u8], pos: &mut usize, depth: usize) -> Result<Option<Blueprint>, ProtocolError> {
    let (start, end) = match try_read_line(buf, pos)? {
        Some(range) => range,
        None => return Ok(INCOMPLETE),
    };

    let count = parse_int_from_slice(&buf[start..end])?;
    if count < 0 {
        return Err(ProtocolError::InvalidLength(count));
    }

    let count = count as usize;
    if !plausible(buf, *pos, count * 2) {
        return Ok(INCOMPLETE);
    }
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let key = match try_parse(buf, pos, depth + 1)? {
            Some(bp) => bp,
            None => return Ok(INCOMPLETE),
        };
        let val = match try_parse(buf, pos, depth + 1)? {
            Some(bp) => bp,
            None => return Ok(INCOMPLETE),
        };
        pairs.push((key, val));
    }
    Ok(Some(Blueprint::Attribute(pairs)))
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/// Minimum bytes per RESP element: type byte + \r\n = 3 (e.g. `_\r\n`).
/// Used for early incomplete detection in aggregate types to avoid
/// allocating huge Vecs for counts that obviously can't be satisfied.
const MIN_ELEMENT_BYTES: usize = 3;

/// Check whether `n_elements` RESP elements could plausibly fit in the
/// remaining buffer. If not, the buffer is definitely incomplete and
/// we can skip the allocation + per-element parsing entirely.
#[inline]
fn plausible(buf: &[u8], pos: usize, n_elements: usize) -> bool {
    let remaining = buf.len().saturating_sub(pos);
    // Use saturating mul to avoid overflow on absurd counts
    n_elements.saturating_mul(MIN_ELEMENT_BYTES) <= remaining
}

/// Find the byte offset of the first `\r\n` in `buf`.
/// Returns the index of `\r`, or `None` if not found.
#[inline]
fn find_crlf(buf: &[u8]) -> Option<usize> {
    let mut start = 0;
    while start < buf.len() {
        match buf[start..].iter().position(|&b| b == b'\n') {
            Some(rel) => {
                let abs = start + rel;
                if abs > 0 && buf[abs - 1] == b'\r' {
                    return Some(abs - 1);
                }
                start = abs + 1;
            }
            None => return None,
        }
    }
    None
}

/// Parse a signed integer from an ASCII byte slice.
#[inline]
fn parse_int_from_slice(buf: &[u8]) -> Result<i64, ProtocolError> {
    if buf.is_empty() {
        return Err(ProtocolError::InvalidInteger(String::new()));
    }

    let (negative, digits) = if buf[0] == b'-' {
        (true, &buf[1..])
    } else if buf[0] == b'+' {
        (false, &buf[1..])
    } else {
        (false, buf)
    };

    if digits.is_empty() {
        return Err(ProtocolError::InvalidInteger(String::from_utf8_lossy(buf).into_owned()));
    }

    // Accumulate as negative to avoid overflow on i64::MIN.
    // i64::MIN (-9223372036854775808) has no positive i64 representation,
    // so we must work in the negative domain and negate at the end for positives.
    let mut n: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return Err(ProtocolError::InvalidInteger(String::from_utf8_lossy(buf).into_owned()));
        }
        n = n
            .checked_mul(10)
            .and_then(|n| n.checked_sub((b - b'0') as i64))
            .ok_or_else(|| ProtocolError::InvalidInteger(String::from_utf8_lossy(buf).into_owned()))?;
    }

    if negative {
        Ok(n)
    } else {
        n.checked_neg()
            .ok_or_else(|| ProtocolError::InvalidInteger(String::from_utf8_lossy(buf).into_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Helper ---
    fn parse_bytes(input: &[u8]) -> Result<Option<RespValue>, ProtocolError> {
        let mut buf = BytesMut::from(input);
        parse(&mut buf)
    }

    fn parse_complete(input: &[u8]) -> RespValue {
        parse_bytes(input).unwrap().expect("expected complete frame")
    }

    // --- Simple String ---

    #[test]
    fn simple_string() {
        assert_eq!(
            parse_complete(b"+OK\r\n"),
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        );
    }

    #[test]
    fn simple_string_empty() {
        assert_eq!(
            parse_complete(b"+\r\n"),
            RespValue::SimpleString(Bytes::from_static(b""))
        );
    }

    // --- Error ---

    #[test]
    fn simple_error() {
        assert_eq!(
            parse_complete(b"-ERR unknown command\r\n"),
            RespValue::Error(Bytes::from_static(b"ERR unknown command"))
        );
    }

    #[test]
    fn wrongtype_error() {
        let input = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        match parse_complete(input) {
            RespValue::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    // --- Integer ---

    #[test]
    fn integer_positive() {
        assert_eq!(parse_complete(b":42\r\n"), RespValue::Integer(42));
    }

    #[test]
    fn integer_negative() {
        assert_eq!(parse_complete(b":-17\r\n"), RespValue::Integer(-17));
    }

    #[test]
    fn integer_zero() {
        assert_eq!(parse_complete(b":0\r\n"), RespValue::Integer(0));
    }

    #[test]
    fn integer_max() {
        let input = format!(":{}\r\n", i64::MAX);
        assert_eq!(parse_complete(input.as_bytes()), RespValue::Integer(i64::MAX));
    }

    #[test]
    fn integer_min() {
        // Regression: i64::MIN (-9223372036854775808) has no positive i64
        // representation. The parser must handle this without overflow.
        let input = format!(":{}\r\n", i64::MIN);
        assert_eq!(parse_complete(input.as_bytes()), RespValue::Integer(i64::MIN));
    }

    // --- Bulk String ---

    #[test]
    fn bulk_string() {
        assert_eq!(
            parse_complete(b"$5\r\nhello\r\n"),
            RespValue::BulkString(Some(Bytes::from_static(b"hello")))
        );
    }

    #[test]
    fn bulk_string_empty() {
        assert_eq!(
            parse_complete(b"$0\r\n\r\n"),
            RespValue::BulkString(Some(Bytes::from_static(b"")))
        );
    }

    #[test]
    fn bulk_string_null() {
        assert_eq!(parse_complete(b"$-1\r\n"), RespValue::BulkString(None));
    }

    #[test]
    fn bulk_string_binary() {
        // Binary data with embedded \r\n
        let mut input = BytesMut::new();
        input.extend_from_slice(b"$6\r\n");
        input.extend_from_slice(b"ab\r\ncd");
        input.extend_from_slice(b"\r\n");
        let result = parse(&mut input).unwrap().unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from_static(b"ab\r\ncd"))));
    }

    // --- Array ---

    #[test]
    fn array_empty() {
        assert_eq!(parse_complete(b"*0\r\n"), RespValue::Array(Some(vec![])));
    }

    #[test]
    fn array_null() {
        assert_eq!(parse_complete(b"*-1\r\n"), RespValue::Array(None));
    }

    #[test]
    fn array_of_integers() {
        assert_eq!(
            parse_complete(b"*3\r\n:1\r\n:2\r\n:3\r\n"),
            RespValue::Array(Some(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ]))
        );
    }

    #[test]
    fn array_mixed_types() {
        let input = b"*2\r\n$5\r\nhello\r\n:42\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"hello"))),
                RespValue::Integer(42),
            ]))
        );
    }

    #[test]
    fn array_nested() {
        let input = b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Array(Some(vec![
                RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(2),])),
                RespValue::Array(Some(vec![RespValue::Integer(3), RespValue::Integer(4),])),
            ]))
        );
    }

    #[test]
    fn array_with_null_element() {
        let input = b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"foo"))),
                RespValue::BulkString(None),
                RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
            ]))
        );
    }

    // --- RESP3: Null ---

    #[test]
    fn resp3_null() {
        assert_eq!(parse_complete(b"_\r\n"), RespValue::Null);
    }

    // --- RESP3: Boolean ---

    #[test]
    fn resp3_boolean_true() {
        assert_eq!(parse_complete(b"#t\r\n"), RespValue::Boolean(true));
    }

    #[test]
    fn resp3_boolean_false() {
        assert_eq!(parse_complete(b"#f\r\n"), RespValue::Boolean(false));
    }

    // --- RESP3: Double ---

    #[test]
    fn resp3_double() {
        assert_eq!(parse_complete(b",1.23\r\n"), RespValue::Double(1.23));
    }

    #[test]
    fn resp3_double_negative() {
        assert_eq!(parse_complete(b",-1.5\r\n"), RespValue::Double(-1.5));
    }

    #[test]
    fn resp3_double_integer_form() {
        assert_eq!(parse_complete(b",10\r\n"), RespValue::Double(10.0));
    }

    #[test]
    fn resp3_double_inf() {
        assert_eq!(parse_complete(b",inf\r\n"), RespValue::Double(f64::INFINITY));
    }

    #[test]
    fn resp3_double_neg_inf() {
        assert_eq!(parse_complete(b",-inf\r\n"), RespValue::Double(f64::NEG_INFINITY));
    }

    #[test]
    fn resp3_double_nan() {
        match parse_complete(b",nan\r\n") {
            RespValue::Double(d) => assert!(d.is_nan()),
            other => panic!("expected Double(NaN), got {:?}", other),
        }
    }

    #[test]
    fn resp3_double_exponent() {
        assert_eq!(parse_complete(b",1.5e2\r\n"), RespValue::Double(150.0));
    }

    // --- RESP3: Big Number ---

    #[test]
    fn resp3_big_number() {
        let input = b"(3492890328409238509324850943850943825024385\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::BigNumber(Bytes::from_static(b"3492890328409238509324850943850943825024385"))
        );
    }

    #[test]
    fn resp3_big_number_negative() {
        assert_eq!(
            parse_complete(b"(-42\r\n"),
            RespValue::BigNumber(Bytes::from_static(b"-42"))
        );
    }

    // --- RESP3: Bulk Error ---

    #[test]
    fn resp3_bulk_error() {
        assert_eq!(
            parse_complete(b"!21\r\nSYNTAX invalid syntax\r\n"),
            RespValue::BulkError(Bytes::from_static(b"SYNTAX invalid syntax"))
        );
    }

    // --- RESP3: Verbatim String ---

    #[test]
    fn resp3_verbatim_string() {
        assert_eq!(
            parse_complete(b"=15\r\ntxt:Some string\r\n"),
            RespValue::VerbatimString {
                encoding: *b"txt",
                data: Bytes::from_static(b"Some string"),
            }
        );
    }

    #[test]
    fn resp3_verbatim_string_mkd() {
        assert_eq!(
            parse_complete(b"=9\r\nmkd:hello\r\n"),
            RespValue::VerbatimString {
                encoding: *b"mkd",
                data: Bytes::from_static(b"hello"),
            }
        );
    }

    // --- RESP3: Map ---

    #[test]
    fn resp3_map() {
        let input = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Map(vec![
                (
                    RespValue::SimpleString(Bytes::from_static(b"first")),
                    RespValue::Integer(1),
                ),
                (
                    RespValue::SimpleString(Bytes::from_static(b"second")),
                    RespValue::Integer(2),
                ),
            ])
        );
    }

    #[test]
    fn resp3_map_empty() {
        assert_eq!(parse_complete(b"%0\r\n"), RespValue::Map(vec![]));
    }

    // --- RESP3: Set ---

    #[test]
    fn resp3_set() {
        let input = b"~3\r\n+a\r\n+b\r\n+c\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Set(vec![
                RespValue::SimpleString(Bytes::from_static(b"a")),
                RespValue::SimpleString(Bytes::from_static(b"b")),
                RespValue::SimpleString(Bytes::from_static(b"c")),
            ])
        );
    }

    // --- RESP3: Push ---

    #[test]
    fn resp3_push() {
        let input = b">2\r\n+subscribe\r\n+channel\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Push(vec![
                RespValue::SimpleString(Bytes::from_static(b"subscribe")),
                RespValue::SimpleString(Bytes::from_static(b"channel")),
            ])
        );
    }

    // --- RESP3: Attribute ---

    #[test]
    fn resp3_attribute() {
        let input = b"|1\r\n+ttl\r\n:3600\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Attribute(vec![(
                RespValue::SimpleString(Bytes::from_static(b"ttl")),
                RespValue::Integer(3600),
            )])
        );
    }

    // --- Incremental parsing ---

    #[test]
    fn incomplete_returns_none() {
        assert_eq!(parse_bytes(b"+OK").unwrap(), None);
        assert_eq!(parse_bytes(b"+OK\r").unwrap(), None);
        assert_eq!(parse_bytes(b"$5\r\nhel").unwrap(), None);
        assert_eq!(parse_bytes(b"*2\r\n:1\r\n").unwrap(), None);
        assert_eq!(parse_bytes(b"").unwrap(), None);
    }

    #[test]
    fn buffer_preserves_unconsumed_bytes() {
        let mut buf = BytesMut::from(&b"+OK\r\n+NEXT\r\n"[..]);
        let first = parse(&mut buf).unwrap().unwrap();
        assert_eq!(first, RespValue::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(&buf[..], b"+NEXT\r\n");

        let second = parse(&mut buf).unwrap().unwrap();
        assert_eq!(second, RespValue::SimpleString(Bytes::from_static(b"NEXT")));
        assert!(buf.is_empty());
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let mut buf = BytesMut::from(&b":1\r\n:2\r\n:3\r\n"[..]);
        assert_eq!(parse(&mut buf).unwrap().unwrap(), RespValue::Integer(1));
        assert_eq!(parse(&mut buf).unwrap().unwrap(), RespValue::Integer(2));
        assert_eq!(parse(&mut buf).unwrap().unwrap(), RespValue::Integer(3));
        assert_eq!(parse(&mut buf).unwrap(), None);
    }

    // --- Error cases ---

    #[test]
    fn invalid_type_byte() {
        assert!(parse_bytes(b"X\r\n").is_err());
    }

    #[test]
    fn invalid_integer() {
        assert!(parse_bytes(b":abc\r\n").is_err());
    }

    #[test]
    fn invalid_bulk_length() {
        assert!(parse_bytes(b"$-2\r\n").is_err());
    }

    #[test]
    fn invalid_boolean() {
        assert!(parse_bytes(b"#x\r\n").is_err());
    }

    #[test]
    fn boolean_with_trailing_garbage_is_rejected() {
        // Regression: fuzzer found that `#t$1%2\r\n` was accepted as
        // Boolean(true) with the decoder advancing only 3 bytes past `#`,
        // leaving `$1%2\r\n` to be misinterpreted as the next frame.
        assert!(parse_bytes(b"#t$1%2\r\n").is_err());
        assert!(parse_bytes(b"#tX\r\n").is_err());
    }

    #[test]
    fn null_with_trailing_garbage_is_rejected() {
        assert!(parse_bytes(b"_X\r\n").is_err());
    }

    // --- Zero-copy verification ---

    #[test]
    fn bulk_string_is_zero_copy() {
        let mut buf = BytesMut::from(&b"$5\r\nhello\r\n"[..]);
        let value = parse(&mut buf).unwrap().unwrap();

        if let RespValue::BulkString(Some(ref data)) = value {
            assert_eq!(&data[..], b"hello");
        } else {
            panic!("expected BulkString");
        }
    }

    // --- Redis command format ---

    #[test]
    fn parse_redis_command() {
        // SET foo bar → *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        assert_eq!(
            parse_complete(input),
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"SET"))),
                RespValue::BulkString(Some(Bytes::from_static(b"foo"))),
                RespValue::BulkString(Some(Bytes::from_static(b"bar"))),
            ]))
        );
    }

    // --- Utility tests ---

    #[test]
    fn find_crlf_basic() {
        assert_eq!(find_crlf(b"hello\r\n"), Some(5));
        assert_eq!(find_crlf(b"\r\n"), Some(0));
        assert_eq!(find_crlf(b"no newline"), None);
        assert_eq!(find_crlf(b"has\nbut\rnot crlf"), None);
        assert_eq!(find_crlf(b"a\rb\r\nc"), Some(3));
    }

    #[test]
    fn parse_int_from_slice_cases() {
        assert_eq!(parse_int_from_slice(b"42").unwrap(), 42);
        assert_eq!(parse_int_from_slice(b"-1").unwrap(), -1);
        assert_eq!(parse_int_from_slice(b"0").unwrap(), 0);
        assert_eq!(parse_int_from_slice(b"+7").unwrap(), 7);
        assert!(parse_int_from_slice(b"").is_err());
        assert!(parse_int_from_slice(b"abc").is_err());
        assert!(parse_int_from_slice(b"-").is_err());
    }
}
