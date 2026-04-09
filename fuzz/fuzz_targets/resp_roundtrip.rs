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
use kvdb::protocol::types::RespValue;
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
        resp_eq(&value, &parsed),
        "round-trip mismatch:\n  original: {:?}\n  parsed:   {:?}",
        value,
        parsed
    );
});

/// Compare two RespValues, accounting for RESP3 null upgrades:
/// BulkString(None) and Array(None) both encode to `_\r\n` in RESP3,
/// which parses back as Null.
fn resp_eq(a: &RespValue, b: &RespValue) -> bool {
    match (a, b) {
        (RespValue::BulkString(None), RespValue::Null)
        | (RespValue::Null, RespValue::BulkString(None))
        | (RespValue::Array(None), RespValue::Null)
        | (RespValue::Null, RespValue::Array(None))
        | (RespValue::Null, RespValue::Null) => true,

        (RespValue::SimpleString(a), RespValue::SimpleString(b)) => a == b,
        (RespValue::Error(a), RespValue::Error(b)) => a == b,
        (RespValue::Integer(a), RespValue::Integer(b)) => a == b,
        (RespValue::BulkString(Some(a)), RespValue::BulkString(Some(b))) => a == b,
        (RespValue::BulkString(None), RespValue::BulkString(None)) => true,
        (RespValue::Boolean(a), RespValue::Boolean(b)) => a == b,
        (RespValue::Double(a), RespValue::Double(b)) => (a.is_nan() && b.is_nan()) || a == b,
        (RespValue::BigNumber(a), RespValue::BigNumber(b)) => a == b,
        (RespValue::BulkError(a), RespValue::BulkError(b)) => a == b,
        (
            RespValue::VerbatimString {
                encoding: ea,
                data: da,
            },
            RespValue::VerbatimString {
                encoding: eb,
                data: db,
            },
        ) => ea == eb && da == db,
        (RespValue::Array(Some(a)), RespValue::Array(Some(b))) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| resp_eq(x, y))
        }
        (RespValue::Map(a), RespValue::Map(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((k1, v1), (k2, v2))| resp_eq(k1, k2) && resp_eq(v1, v2))
        }
        (RespValue::Set(a), RespValue::Set(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| resp_eq(x, y))
        }
        (RespValue::Push(a), RespValue::Push(b)) => {
            a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| resp_eq(x, y))
        }
        (RespValue::Attribute(a), RespValue::Attribute(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((k1, v1), (k2, v2))| resp_eq(k1, k2) && resp_eq(v1, v2))
        }
        _ => false,
    }
}
