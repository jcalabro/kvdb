//! RESP encoder.
//!
//! Encodes `RespValue` into wire-format bytes. Supports both RESP2 and
//! RESP3 encoding: when `protocol_version == 2`, RESP3-only types are
//! downgraded to their RESP2 equivalents (e.g. Map -> Array of pairs,
//! Boolean -> Integer 0/1, Null -> BulkString(None)).

use bytes::BytesMut;

use crate::protocol::types::RespValue;

/// Encode a RESP value into the wire format.
///
/// `protocol_version` controls whether RESP3 types are encoded natively (3)
/// or downgraded to RESP2 equivalents (2).
pub fn encode(value: &RespValue, protocol_version: u8) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    encode_into(&mut buf, value, protocol_version);
    buf
}

/// Encode a RESP value into an existing buffer (avoids allocation when
/// batching multiple responses).
pub fn encode_into(buf: &mut BytesMut, value: &RespValue, protocol_version: u8) {
    // M1 will implement the full encoder. For now, write a placeholder.
    let _ = (buf, value, protocol_version);
}
