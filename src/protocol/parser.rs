//! Incremental RESP parser.
//!
//! Feed bytes into a `BytesMut` buffer, then call `parse()` to extract
//! complete frames. Returns `Ok(None)` when more data is needed (partial
//! frame), `Ok(Some(value))` on success, or `Err` on malformed input.
//!
//! Zero-copy: bulk string values are `Bytes` slices into the original
//! buffer, not heap copies.

use bytes::BytesMut;

use crate::error::ProtocolError;
use crate::protocol::types::RespValue;

/// Parse one complete RESP value from the front of `buf`.
///
/// On success, the consumed bytes are drained from `buf` and the parsed
/// value is returned. Returns `Ok(None)` if the buffer doesn't contain
/// a complete frame yet (caller should read more data and retry).
pub fn parse(buf: &mut BytesMut) -> Result<Option<RespValue>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // M1 will implement the full parser. For now, return incomplete.
    let _ = buf;
    Ok(None)
}
