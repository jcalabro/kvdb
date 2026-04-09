//! Fuzz target: encoder stability and output validity.
//!
//! Generates arbitrary `RespValue` trees via `arbitrary`, encodes them
//! in both RESP2 and RESP3 modes, and verifies:
//! 1. The encoder never panics.
//! 2. The encoded output always parses back without error.
//!
//! This catches encoder bugs that produce malformed RESP output.
//!
//! Run with `cargo +nightly fuzz run resp_encoder`.

#![no_main]

mod fuzz_types;

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: fuzz_types::FuzzRespValue| {
    let value = input.into_resp();

    // RESP3 encoding must produce parseable output
    let resp3 = kvdb::protocol::encoder::encode(&value, 3);
    if !resp3.is_empty() {
        let mut buf = BytesMut::from(&resp3[..]);
        kvdb::protocol::parser::parse(&mut buf)
            .expect("RESP3 encoded output must parse without error")
            .expect("RESP3 encoded output must be a complete frame");
        assert_eq!(buf.len(), 0, "RESP3: unconsumed bytes after parse");
    }

    // RESP2 encoding must produce parseable output (or empty for Attributes)
    let resp2 = kvdb::protocol::encoder::encode(&value, 2);
    if !resp2.is_empty() {
        let mut buf = BytesMut::from(&resp2[..]);
        kvdb::protocol::parser::parse(&mut buf)
            .expect("RESP2 encoded output must parse without error")
            .expect("RESP2 encoded output must be a complete frame");
        assert_eq!(buf.len(), 0, "RESP2: unconsumed bytes after parse");
    }
});
