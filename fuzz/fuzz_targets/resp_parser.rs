//! Fuzz target for the RESP protocol parser.
//!
//! Feeds arbitrary bytes into the parser and verifies it never panics.
//! Run with `just fuzz` or `cargo +nightly fuzz run resp_parser`.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    // The parser must handle arbitrary input without panicking.
    let _ = kvdb::protocol::parser::parse(&mut buf);
});
