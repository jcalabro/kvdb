//! Fuzz target: structured encode→parse round-trip.
//!
//! Uses `arbitrary` to generate valid `RespValue` trees via coverage-
//! guided mutation, encodes them, parses the result back, and verifies
//! the round-trip produces an equivalent value. This is the coverage-
//! guided counterpart to our proptest round-trip — libfuzzer's mutation
//! engine finds edge cases that random generation misses.
//!
//! Run with `cargo +nightly fuzz run resp_roundtrip`.

#![no_main]

mod fuzz_types;

use bytes::BytesMut;
use kvdb::protocol::types::{RespValue, resp3_eq};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: fuzz_types::FuzzRespValue| {
    let value = input.into_resp();
    let encoded = kvdb::protocol::encoder::encode(&value, 3);

    let mut buf = BytesMut::from(&encoded[..]);
    let parsed = kvdb::protocol::parser::parse(&mut buf)
        .expect("parsing our own encoded output must not error")
        .expect("parsing our own encoded output must produce a complete frame");

    // Buffer fully consumed
    assert_eq!(buf.len(), 0, "unconsumed bytes after parsing encoded output");

    // Round-trip equivalence (accounting for null upgrades in RESP3)
    assert!(
        resp3_eq(&value, &parsed),
        "round-trip mismatch:\n  original: {:?}\n  parsed:   {:?}",
        value,
        parsed
    );
});
