//! RESP protocol parser and encoder.
//!
//! Implements the full RESP3 specification with RESP2 backward compatibility.
//! Zero-copy parsing via `bytes::BytesMut` — parsed values reference the
//! original buffer rather than copying data.

pub mod encoder;
pub mod parser;
pub mod types;
