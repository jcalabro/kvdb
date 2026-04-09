//! Property-based tests for the RESP protocol parser and encoder.
//!
//! The core invariant: for any valid `RespValue`, encoding it and then
//! parsing the result must produce an equivalent value. We generate
//! arbitrary `RespValue` trees and verify this round-trip property
//! holds across thousands of randomized inputs.

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;

use kvdb::protocol::encoder::encode;
use kvdb::protocol::parser::parse;
use kvdb::protocol::types::RespValue;

/// Maximum nesting depth for generated values.
/// Depth 3 with branching factor 5 still produces trees up to 125 leaves —
/// plenty for finding bugs. Deep nesting (64 levels) is tested separately
/// by `accept_nested_arrays`.
const MAX_GEN_DEPTH: usize = 3;

/// Maximum collection size for generated aggregates.
const MAX_GEN_SIZE: usize = 5;

/// Maximum bulk string length for generated values.
/// Large values (200KB) are tested separately by `accept_large_bulk_string`.
const MAX_GEN_BULK_LEN: usize = 256;

// ---------------------------------------------------------------------------
// Arbitrary RespValue generation
// ---------------------------------------------------------------------------

/// Generate an arbitrary `RespValue` up to the given nesting depth.
fn arb_resp_value(depth: usize) -> BoxedStrategy<RespValue> {
    if depth == 0 {
        // Leaf nodes only — no aggregates
        arb_resp_leaf().boxed()
    } else {
        prop_oneof![
            4 => arb_resp_leaf(),
            1 => arb_array(depth),
            1 => arb_map(depth),
            1 => arb_set(depth),
            1 => arb_push(depth),
        ]
        .boxed()
    }
}

/// Generate a leaf (non-aggregate) `RespValue`.
fn arb_resp_leaf() -> impl Strategy<Value = RespValue> {
    prop_oneof![
        // SimpleString: no \r or \n allowed
        "[^\r\n]{0,64}".prop_map(|s| RespValue::SimpleString(Bytes::from(s))),
        // Error: no \r or \n
        "[^\r\n]{1,64}".prop_map(|s| RespValue::Error(Bytes::from(s))),
        // Integer
        any::<i64>().prop_map(RespValue::Integer),
        // BulkString (non-null, binary-safe)
        prop::collection::vec(any::<u8>(), 0..MAX_GEN_BULK_LEN)
            .prop_map(|v| RespValue::BulkString(Some(Bytes::from(v)))),
        // BulkString null
        Just(RespValue::BulkString(None)),
        // Null
        Just(RespValue::Null),
        // Boolean
        any::<bool>().prop_map(RespValue::Boolean),
        // Double (finite only — NaN breaks equality; tested separately)
        prop_double_finite().prop_map(RespValue::Double),
        // BigNumber
        "-?[0-9]{1,40}".prop_map(|s| RespValue::BigNumber(Bytes::from(s))),
        // BulkError
        prop::collection::vec(any::<u8>(), 0..128).prop_map(|v| RespValue::BulkError(Bytes::from(v))),
        // VerbatimString
        (
            prop::array::uniform3(b'a'..=b'z'),
            prop::collection::vec(any::<u8>(), 0..128),
        )
            .prop_map(|(enc, data)| RespValue::VerbatimString {
                encoding: enc,
                data: Bytes::from(data),
            }),
    ]
}

/// Generate finite f64 values (not NaN, not infinite — those break == comparison).
fn prop_double_finite() -> impl Strategy<Value = f64> {
    // Use a range that avoids precision issues with ryu round-trip
    any::<f64>().prop_filter("finite doubles only", |d| d.is_finite())
}

/// Generate an Array with nested values.
fn arb_array(depth: usize) -> impl Strategy<Value = RespValue> {
    prop_oneof![
        // Non-null array
        prop::collection::vec(arb_resp_value(depth - 1), 0..MAX_GEN_SIZE)
            .prop_map(|elems| RespValue::Array(Some(elems))),
        // Null array
        Just(RespValue::Array(None)),
    ]
}

/// Generate a Map with nested values.
fn arb_map(depth: usize) -> impl Strategy<Value = RespValue> {
    prop::collection::vec((arb_resp_value(depth - 1), arb_resp_value(depth - 1)), 0..MAX_GEN_SIZE)
        .prop_map(RespValue::Map)
}

/// Generate a Set with nested values.
fn arb_set(depth: usize) -> impl Strategy<Value = RespValue> {
    prop::collection::vec(arb_resp_value(depth - 1), 0..MAX_GEN_SIZE).prop_map(RespValue::Set)
}

/// Generate a Push with nested values.
fn arb_push(depth: usize) -> impl Strategy<Value = RespValue> {
    prop::collection::vec(arb_resp_value(depth - 1), 0..MAX_GEN_SIZE).prop_map(RespValue::Push)
}

// ---------------------------------------------------------------------------
// Custom equality: handles RESP3 encoding upgrades
// ---------------------------------------------------------------------------

/// Compare two `RespValue`s, accounting for the fact that encoding in RESP3
/// mode upgrades `BulkString(None)` and `Array(None)` to `Null`.
fn resp_eq(a: &RespValue, b: &RespValue) -> bool {
    match (a, b) {
        // Null equivalences (RESP3 encoder converts these)
        (RespValue::BulkString(None), RespValue::Null)
        | (RespValue::Null, RespValue::BulkString(None))
        | (RespValue::Array(None), RespValue::Null)
        | (RespValue::Null, RespValue::Array(None)) => true,

        // Direct matches
        (RespValue::SimpleString(a), RespValue::SimpleString(b)) => a == b,
        (RespValue::Error(a), RespValue::Error(b)) => a == b,
        (RespValue::Integer(a), RespValue::Integer(b)) => a == b,
        (RespValue::BulkString(Some(a)), RespValue::BulkString(Some(b))) => a == b,
        (RespValue::BulkString(None), RespValue::BulkString(None)) => true,
        (RespValue::Null, RespValue::Null) => true,
        (RespValue::Boolean(a), RespValue::Boolean(b)) => a == b,
        (RespValue::Double(a), RespValue::Double(b)) => (a.is_nan() && b.is_nan()) || a == b,
        (RespValue::BigNumber(a), RespValue::BigNumber(b)) => a == b,
        (RespValue::BulkError(a), RespValue::BulkError(b)) => a == b,
        (
            RespValue::VerbatimString { encoding: ea, data: da },
            RespValue::VerbatimString { encoding: eb, data: db },
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

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    // Exit criteria: property tests pass with 10K cases.
    // 5K cases per property × 3 properties = 15K total randomized cases.
    // Exceeds the 10K exit criteria while staying under the 15s budget.
    #![proptest_config(ProptestConfig::with_cases(5_000))]

    /// Core round-trip: encode in RESP3, parse back, verify equality.
    #[test]
    fn accept_resp3_roundtrip(value in arb_resp_value(MAX_GEN_DEPTH)) {
        let encoded = encode(&value, 3);
        let mut buf = BytesMut::from(&encoded[..]);
        let parsed = parse(&mut buf).expect("parse should not error").expect("frame should be complete");

        prop_assert!(
            resp_eq(&value, &parsed),
            "round-trip failed:\n  original: {:?}\n  encoded:  {:?}\n  parsed:   {:?}",
            value,
            String::from_utf8_lossy(&encoded),
            parsed
        );

        // Buffer should be fully consumed
        prop_assert_eq!(buf.len(), 0, "unconsumed bytes after parse");
    }

    /// RESP2 round-trip: encode in RESP2, parse back.
    /// RESP2 encoding changes some types (Boolean→Integer, Null→BulkString(None), etc.),
    /// so we only verify the parse succeeds and produces a valid value.
    #[test]
    fn accept_resp2_encode_parses(value in arb_resp_value(MAX_GEN_DEPTH)) {
        let encoded = encode(&value, 2);
        // Attributes are silently omitted in RESP2, which may produce empty output
        if !encoded.is_empty() {
            let mut buf = BytesMut::from(&encoded[..]);
            let parsed = parse(&mut buf).expect("parse should not error").expect("frame should be complete");
            // Just verify we got a valid value back
            let _ = parsed;
        }
    }

    /// Incremental parsing: splitting the encoded bytes at an arbitrary position
    /// and feeding them in two chunks must produce the same result.
    #[test]
    fn accept_incremental_parse(
        value in arb_resp_value(MAX_GEN_DEPTH),
        split_frac in 0.0f64..1.0,
    ) {
        let encoded = encode(&value, 3);
        let split_at = (split_frac * encoded.len() as f64) as usize;
        let (first, second) = encoded.split_at(split_at);

        let mut buf = BytesMut::from(first);
        // First parse should return None (incomplete) or Some (if split_at == len)
        let result1 = parse(&mut buf).expect("parse should not error");

        if result1.is_none() {
            // Feed the rest
            buf.extend_from_slice(second);
            let result2 = parse(&mut buf).expect("parse should not error").expect("frame should be complete after full data");
            prop_assert!(resp_eq(&value, &result2));
        } else {
            // Got the full value in the first chunk — that's fine too
            prop_assert!(resp_eq(&value, &result1.unwrap()));
        }
    }

    /// Double special values: inf, -inf round-trip correctly.
    #[test]
    fn accept_double_special_roundtrip(d in prop_oneof![
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
    ]) {
        let value = RespValue::Double(d);
        let encoded = encode(&value, 3);
        let mut buf = BytesMut::from(&encoded[..]);
        let parsed = parse(&mut buf).unwrap().unwrap();
        prop_assert_eq!(parsed, value);
    }
}

// NaN needs a separate non-proptest test since NaN != NaN
#[test]
fn accept_double_nan_roundtrip() {
    let value = RespValue::Double(f64::NAN);
    let encoded = encode(&value, 3);
    let mut buf = BytesMut::from(&encoded[..]);
    let parsed = parse(&mut buf).unwrap().unwrap();
    match parsed {
        RespValue::Double(d) => assert!(d.is_nan(), "expected NaN, got {}", d),
        other => panic!("expected Double, got {:?}", other),
    }
}

/// Verify that encoding then parsing large bulk strings works correctly.
#[test]
fn accept_large_bulk_string() {
    let big = vec![0xABu8; 200_000]; // 200KB
    let value = RespValue::BulkString(Some(Bytes::from(big.clone())));
    let encoded = encode(&value, 3);
    let mut buf = BytesMut::from(&encoded[..]);
    let parsed = parse(&mut buf).unwrap().unwrap();
    match parsed {
        RespValue::BulkString(Some(data)) => assert_eq!(&data[..], &big[..]),
        other => panic!("expected BulkString, got {:?}", other),
    }
}

/// Verify deeply nested structures parse correctly up to the limit.
#[test]
fn accept_nested_arrays() {
    // Build a deeply nested array: [[[[...]]]]
    let mut value = RespValue::Integer(42);
    for _ in 0..64 {
        value = RespValue::Array(Some(vec![value]));
    }
    let encoded = encode(&value, 3);
    let mut buf = BytesMut::from(&encoded[..]);
    let parsed = parse(&mut buf).unwrap().unwrap();
    assert_eq!(parsed, value);
}
