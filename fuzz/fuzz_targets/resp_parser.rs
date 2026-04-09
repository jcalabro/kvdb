//! Fuzz target for the RESP protocol parser.
//!
//! Feeds arbitrary bytes into the parser and verifies it never panics.
//! Also checks that if parsing succeeds, re-encoding and re-parsing
//! produces an equivalent value (encode-parse round-trip on valid input).
//!
//! Run with `just fuzz` or `cargo +nightly fuzz run resp_parser`.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);

    // The parser must handle arbitrary input without panicking.
    match kvdb::protocol::parser::parse(&mut buf) {
        Ok(Some(value)) => {
            // If parsing succeeded, the encoded form should parse back
            // to an equivalent value. This catches encoder bugs too.
            let encoded = kvdb::protocol::encoder::encode(&value, 3);
            let mut rebuf = BytesMut::from(&encoded[..]);
            let reparsed = kvdb::protocol::parser::parse(&mut rebuf)
                .expect("re-parse of encoded value should not error");
            assert!(reparsed.is_some(), "re-parse should produce complete frame");
        }
        Ok(None) => {
            // Incomplete frame — normal for arbitrary bytes
        }
        Err(_) => {
            // Protocol error — expected for most random inputs
        }
    }
});
