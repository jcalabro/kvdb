//! Shared utility functions for command handlers.
//!
//! Helpers that are used by more than one command module live here
//! to avoid duplication and keep the logic in a single place.

use bytes::Bytes;

use crate::protocol::types::RespValue;

/// Parse a byte argument as i64 with Redis-compatible error message.
pub fn parse_i64_arg(arg: &Bytes) -> Result<i64, RespValue> {
    let s = std::str::from_utf8(arg).map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
    s.parse::<i64>()
        .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))
}

/// Format a float following Redis INCRBYFLOAT / HINCRBYFLOAT rules:
/// - If the result is an exact integer that fits in i64 (round-trip safe),
///   format without decimal point.
/// - Otherwise, use minimal precision via ryu and strip trailing zeros.
pub fn format_redis_float(val: f64) -> String {
    if val.fract() == 0.0 && val.is_finite() {
        // Use integer formatting only when the value truly fits in i64.
        //
        // Subtlety: `i64::MAX as f64` rounds UP to 2^63 (because i64::MAX
        // is not exactly representable in f64). Using `val <= i64::MAX as f64`
        // would incorrectly allow 2^63 through, and `val as i64` would
        // saturate to i64::MAX, producing the wrong decimal string.
        //
        // The strict less-than excludes 2^63. Any f64 that passes this check
        // is guaranteed to be in [i64::MIN, i64::MAX] because the next smaller
        // representable f64 is 2^63 - 1024, well within i64 range.
        //
        // i64::MIN (-2^63) is exactly representable in f64, so >= is correct.
        if val >= i64::MIN as f64 && val < i64::MAX as f64 {
            return format!("{}", val as i64);
        }
    }

    let mut s = ryu::Buffer::new().format(val).to_string();
    if s.contains('.') {
        let trimmed = s.trim_end_matches('0');
        let trimmed = trimmed.trim_end_matches('.');
        s.truncate(trimmed.len());
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_float_integer_result() {
        assert_eq!(format_redis_float(4.0), "4");
        assert_eq!(format_redis_float(0.0), "0");
        assert_eq!(format_redis_float(-3.0), "-3");
    }

    #[test]
    fn format_float_fractional() {
        assert_eq!(format_redis_float(1.5), "1.5");
        assert_eq!(format_redis_float(1.23), "1.23");
    }

    #[test]
    fn format_float_near_i64_boundary() {
        // i64::MAX = 9223372036854775807
        // i64::MAX as f64 rounds to 9223372036854775808.0, which doesn't
        // round-trip through i64. Must NOT use integer formatting.
        let big = i64::MAX as f64;
        let formatted = format_redis_float(big);
        // Should NOT be formatted as i64::MAX (the saturated value).
        assert_ne!(formatted, "9223372036854775807");

        let neg_big = i64::MIN as f64;
        let formatted = format_redis_float(neg_big);
        // i64::MIN is exactly representable in f64, so this SHOULD use
        // integer formatting.
        assert_eq!(formatted, format!("{}", i64::MIN));
    }

    #[test]
    fn parse_i64_valid() {
        assert_eq!(parse_i64_arg(&Bytes::from("42")).unwrap(), 42);
        assert_eq!(parse_i64_arg(&Bytes::from("-1")).unwrap(), -1);
    }

    #[test]
    fn parse_i64_invalid() {
        assert!(parse_i64_arg(&Bytes::from("not_a_number")).is_err());
        assert!(parse_i64_arg(&Bytes::from("3.14")).is_err());
    }
}
