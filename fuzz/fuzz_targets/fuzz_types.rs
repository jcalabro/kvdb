//! Shared types for structured fuzz targets.
//!
//! `FuzzRespValue` is an `Arbitrary`-derivable mirror of `RespValue` that
//! avoids the `Bytes` type (which doesn't implement `Arbitrary`). Each
//! variant stores owned `Vec<u8>` data, and `.into_resp()` converts to
//! the real `RespValue` for use with the parser/encoder.

use arbitrary::Arbitrary;
use bytes::Bytes;
use kvdb::protocol::types::RespValue;

/// Maximum nesting depth — keeps generated values small enough
/// that the fuzzer can explore the space quickly.
const MAX_DEPTH: usize = 4;

/// Maximum collection size per aggregate.
const MAX_SIZE: usize = 6;

/// A fuzz-friendly mirror of `RespValue` that derives `Arbitrary`.
#[derive(Debug, Arbitrary)]
pub enum FuzzRespValue {
    SimpleString(SafeLineString),
    Error(SafeLineString),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<FuzzArray>),
    Null,
    Boolean(bool),
    Double(FuzzDouble),
    BigNumber(SafeDigitString),
    BulkError(Vec<u8>),
    VerbatimString {
        encoding: [u8; 3],
        data: Vec<u8>,
    },
    Map(FuzzPairVec),
    Set(FuzzVec),
    Push(FuzzVec),
}

/// A string that never contains \r or \n (valid for simple strings/errors).
#[derive(Debug)]
pub struct SafeLineString(pub Vec<u8>);

impl<'a> Arbitrary<'a> for SafeLineString {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.int_in_range(0..=64)?;
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            let b: u8 = u.arbitrary()?;
            // Replace \r and \n with safe bytes
            buf.push(match b {
                b'\r' | b'\n' => b'_',
                other => other,
            });
        }
        Ok(SafeLineString(buf))
    }
}

/// A digit string with optional leading minus (valid for big numbers).
#[derive(Debug)]
pub struct SafeDigitString(pub Vec<u8>);

impl<'a> Arbitrary<'a> for SafeDigitString {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let negative: bool = u.arbitrary()?;
        let len = u.int_in_range(1..=40)?;
        let mut buf = Vec::with_capacity(len + 1);
        if negative {
            buf.push(b'-');
        }
        for _ in 0..len {
            buf.push(b'0' + u.int_in_range(0..=9)?);
        }
        Ok(SafeDigitString(buf))
    }
}

/// A finite f64 (no NaN/infinity — those need special handling for equality).
#[derive(Debug)]
pub struct FuzzDouble(pub f64);

impl<'a> Arbitrary<'a> for FuzzDouble {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let bits: u64 = u.arbitrary()?;
        let d = f64::from_bits(bits);
        // Only emit finite values — inf/nan are tested by dedicated tests
        if d.is_finite() {
            Ok(FuzzDouble(d))
        } else {
            Ok(FuzzDouble(0.0))
        }
    }
}

/// Bounded-depth array of fuzz values.
#[derive(Debug)]
pub struct FuzzArray(pub Vec<FuzzRespValue>);

impl<'a> Arbitrary<'a> for FuzzArray {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.int_in_range(0..=MAX_SIZE)?;
        let mut elems = Vec::with_capacity(len);
        for _ in 0..len {
            elems.push(arb_bounded(u, MAX_DEPTH - 1)?);
        }
        Ok(FuzzArray(elems))
    }
}

/// Bounded-depth vec of fuzz values (for Set, Push).
#[derive(Debug)]
pub struct FuzzVec(pub Vec<FuzzRespValue>);

impl<'a> Arbitrary<'a> for FuzzVec {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.int_in_range(0..=MAX_SIZE)?;
        let mut elems = Vec::with_capacity(len);
        for _ in 0..len {
            elems.push(arb_bounded(u, MAX_DEPTH - 1)?);
        }
        Ok(FuzzVec(elems))
    }
}

/// Bounded-depth vec of key-value pairs (for Map).
#[derive(Debug)]
pub struct FuzzPairVec(pub Vec<(FuzzRespValue, FuzzRespValue)>);

impl<'a> Arbitrary<'a> for FuzzPairVec {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.int_in_range(0..=MAX_SIZE)?;
        let mut pairs = Vec::with_capacity(len);
        for _ in 0..len {
            let k = arb_bounded(u, MAX_DEPTH - 1)?;
            let v = arb_bounded(u, MAX_DEPTH - 1)?;
            pairs.push((k, v));
        }
        Ok(FuzzPairVec(pairs))
    }
}

/// Generate a leaf-only `FuzzRespValue` (no aggregates).
fn arb_leaf(u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<FuzzRespValue> {
    let choice: u8 = u.int_in_range(0..=9)?;
    match choice {
        0 => Ok(FuzzRespValue::SimpleString(u.arbitrary()?)),
        1 => Ok(FuzzRespValue::Error(u.arbitrary()?)),
        2 => Ok(FuzzRespValue::Integer(u.arbitrary()?)),
        3 => {
            let data: Option<Vec<u8>> = u.arbitrary()?;
            Ok(FuzzRespValue::BulkString(data.map(|mut v| {
                v.truncate(256);
                v
            })))
        }
        4 => Ok(FuzzRespValue::Null),
        5 => Ok(FuzzRespValue::Boolean(u.arbitrary()?)),
        6 => Ok(FuzzRespValue::Double(u.arbitrary()?)),
        7 => Ok(FuzzRespValue::BigNumber(u.arbitrary()?)),
        8 => {
            let mut data: Vec<u8> = u.arbitrary()?;
            data.truncate(128);
            Ok(FuzzRespValue::BulkError(data))
        }
        _ => {
            let encoding: [u8; 3] = u.arbitrary()?;
            let mut data: Vec<u8> = u.arbitrary()?;
            data.truncate(128);
            Ok(FuzzRespValue::VerbatimString { encoding, data })
        }
    }
}

/// Generate a `FuzzRespValue` with bounded depth.
fn arb_bounded(
    u: &mut arbitrary::Unstructured<'_>,
    depth: usize,
) -> arbitrary::Result<FuzzRespValue> {
    if depth == 0 {
        return arb_leaf(u);
    }
    // Bias toward leaves to keep trees small
    let choice: u8 = u.int_in_range(0..=14)?;
    if choice < 10 {
        arb_leaf(u)
    } else {
        match choice {
            10 => {
                let len = u.int_in_range(0..=MAX_SIZE)?;
                let mut elems = Vec::with_capacity(len);
                for _ in 0..len {
                    elems.push(arb_bounded(u, depth - 1)?);
                }
                Ok(FuzzRespValue::Array(Some(FuzzArray(elems))))
            }
            11 => {
                let len = u.int_in_range(0..=MAX_SIZE)?;
                let mut pairs = Vec::with_capacity(len);
                for _ in 0..len {
                    let k = arb_bounded(u, depth - 1)?;
                    let v = arb_bounded(u, depth - 1)?;
                    pairs.push((k, v));
                }
                Ok(FuzzRespValue::Map(FuzzPairVec(pairs)))
            }
            12 => {
                let len = u.int_in_range(0..=MAX_SIZE)?;
                let mut elems = Vec::with_capacity(len);
                for _ in 0..len {
                    elems.push(arb_bounded(u, depth - 1)?);
                }
                Ok(FuzzRespValue::Set(FuzzVec(elems)))
            }
            13 => {
                let len = u.int_in_range(0..=MAX_SIZE)?;
                let mut elems = Vec::with_capacity(len);
                for _ in 0..len {
                    elems.push(arb_bounded(u, depth - 1)?);
                }
                Ok(FuzzRespValue::Push(FuzzVec(elems)))
            }
            _ => Ok(FuzzRespValue::Array(None)),
        }
    }
}

impl FuzzRespValue {
    /// Convert to the real `RespValue` type.
    pub fn into_resp(self) -> RespValue {
        match self {
            FuzzRespValue::SimpleString(s) => RespValue::SimpleString(Bytes::from(s.0)),
            FuzzRespValue::Error(s) => RespValue::Error(Bytes::from(s.0)),
            FuzzRespValue::Integer(n) => RespValue::Integer(n),
            FuzzRespValue::BulkString(None) => RespValue::BulkString(None),
            FuzzRespValue::BulkString(Some(data)) => {
                RespValue::BulkString(Some(Bytes::from(data)))
            }
            FuzzRespValue::Array(None) => RespValue::Array(None),
            FuzzRespValue::Array(Some(arr)) => {
                RespValue::Array(Some(arr.0.into_iter().map(|v| v.into_resp()).collect()))
            }
            FuzzRespValue::Null => RespValue::Null,
            FuzzRespValue::Boolean(b) => RespValue::Boolean(b),
            FuzzRespValue::Double(d) => RespValue::Double(d.0),
            FuzzRespValue::BigNumber(s) => RespValue::BigNumber(Bytes::from(s.0)),
            FuzzRespValue::BulkError(data) => RespValue::BulkError(Bytes::from(data)),
            FuzzRespValue::VerbatimString { encoding, data } => RespValue::VerbatimString {
                encoding,
                data: Bytes::from(data),
            },
            FuzzRespValue::Map(pairs) => RespValue::Map(
                pairs
                    .0
                    .into_iter()
                    .map(|(k, v)| (k.into_resp(), v.into_resp()))
                    .collect(),
            ),
            FuzzRespValue::Set(elems) => {
                RespValue::Set(elems.0.into_iter().map(|v| v.into_resp()).collect())
            }
            FuzzRespValue::Push(elems) => {
                RespValue::Push(elems.0.into_iter().map(|v| v.into_resp()).collect())
            }
        }
    }
}
