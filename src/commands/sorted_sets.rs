//! Sorted set command handlers.
//!
//! Sorted sets use a dual-index FDB layout:
//!
//! - **Score index:** `zset/<key, score: f64, member: bytes> → b""`
//!   Entries ordered by (score, member) for range queries.
//!
//! - **Reverse index:** `zset_idx/<key, member: bytes> → score_bytes`
//!   Point lookups for ZSCORE, member existence, and ZADD updates.
//!
//! `ObjectMeta` tracks cardinality (member count). Both indexes and
//! cardinality are kept in sync within each FDB transaction.
//! Empty sorted sets are deleted per Redis semantics.

// These helpers are building blocks for command handlers in later tasks.
// They are pub(crate) and will be used once ZADD, ZRANGE, etc. land.
#![allow(dead_code)]

use crate::error::CommandError;

// ---------------------------------------------------------------------------
// Score bound types and parsing
// ---------------------------------------------------------------------------

/// A bound for score-based range queries (ZRANGEBYSCORE, ZCOUNT, etc.).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScoreBound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
}

/// A bound for lexicographic range queries (ZRANGEBYLEX, ZLEXCOUNT, etc.).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LexBound {
    NegInf,
    PosInf,
    Inclusive(Vec<u8>),
    Exclusive(Vec<u8>),
}

/// Parse a score bound argument per Redis conventions.
///
/// - `-inf`, `+inf`, `inf` (case insensitive) → NegInf / PosInf
/// - `(1.5` → Exclusive(1.5)
/// - `1.5` → Inclusive(1.5)
pub(crate) fn parse_score_bound(arg: &[u8]) -> Result<ScoreBound, CommandError> {
    let s = std::str::from_utf8(arg).map_err(|_| CommandError::Generic("invalid score bound".into()))?;

    let lower = s.to_ascii_lowercase();
    match lower.as_str() {
        "-inf" => return Ok(ScoreBound::NegInf),
        "+inf" | "inf" => return Ok(ScoreBound::PosInf),
        _ => {}
    }

    if let Some(rest) = s.strip_prefix('(') {
        let val: f64 = rest
            .parse()
            .map_err(|_| CommandError::Generic("ERR min or max is not a float".into()))?;
        Ok(ScoreBound::Exclusive(val))
    } else {
        let val: f64 = s
            .parse()
            .map_err(|_| CommandError::Generic("ERR min or max is not a float".into()))?;
        Ok(ScoreBound::Inclusive(val))
    }
}

/// Parse a lex bound argument per Redis conventions.
///
/// - `-` → NegInf
/// - `+` → PosInf
/// - `[value` → Inclusive(value)
/// - `(value` → Exclusive(value)
pub(crate) fn parse_lex_bound(arg: &[u8]) -> Result<LexBound, CommandError> {
    if arg.is_empty() {
        return Err(CommandError::Generic(
            "ERR min or max not valid string range item".into(),
        ));
    }

    match arg {
        b"-" => Ok(LexBound::NegInf),
        b"+" => Ok(LexBound::PosInf),
        _ => {
            if arg[0] == b'[' {
                Ok(LexBound::Inclusive(arg[1..].to_vec()))
            } else if arg[0] == b'(' {
                Ok(LexBound::Exclusive(arg[1..].to_vec()))
            } else {
                Err(CommandError::Generic(
                    "ERR min or max not valid string range item".into(),
                ))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Range checking helpers
// ---------------------------------------------------------------------------

/// Returns `true` if `score >= bound` (inclusive) or `score > bound` (exclusive).
pub(crate) fn score_gte_bound(score: f64, bound: &ScoreBound) -> bool {
    match bound {
        ScoreBound::NegInf => true,
        ScoreBound::PosInf => false,
        ScoreBound::Inclusive(b) => score >= *b,
        ScoreBound::Exclusive(b) => score > *b,
    }
}

/// Returns `true` if `score <= bound` (inclusive) or `score < bound` (exclusive).
pub(crate) fn score_lte_bound(score: f64, bound: &ScoreBound) -> bool {
    match bound {
        ScoreBound::NegInf => false,
        ScoreBound::PosInf => true,
        ScoreBound::Inclusive(b) => score <= *b,
        ScoreBound::Exclusive(b) => score < *b,
    }
}

/// Returns `true` if `score` is within the range `[min, max]`.
pub(crate) fn score_in_range(score: f64, min: &ScoreBound, max: &ScoreBound) -> bool {
    score_gte_bound(score, min) && score_lte_bound(score, max)
}

/// Returns `true` if `member >= bound` (inclusive) or `member > bound` (exclusive).
pub(crate) fn member_gte_lex_bound(member: &[u8], bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Inclusive(b) => member >= b.as_slice(),
        LexBound::Exclusive(b) => member > b.as_slice(),
    }
}

/// Returns `true` if `member <= bound` (inclusive) or `member < bound` (exclusive).
pub(crate) fn member_lte_lex_bound(member: &[u8], bound: &LexBound) -> bool {
    match bound {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Inclusive(b) => member <= b.as_slice(),
        LexBound::Exclusive(b) => member < b.as_slice(),
    }
}

/// Returns `true` if `member` is within the lex range `[min, max]`.
pub(crate) fn member_in_lex_range(member: &[u8], min: &LexBound, max: &LexBound) -> bool {
    member_gte_lex_bound(member, min) && member_lte_lex_bound(member, max)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- ScoreBound parsing --

    #[test]
    fn parse_score_neg_inf() {
        assert_eq!(parse_score_bound(b"-inf").unwrap(), ScoreBound::NegInf);
        assert_eq!(parse_score_bound(b"-INF").unwrap(), ScoreBound::NegInf);
        assert_eq!(parse_score_bound(b"-Inf").unwrap(), ScoreBound::NegInf);
    }

    #[test]
    fn parse_score_pos_inf() {
        assert_eq!(parse_score_bound(b"+inf").unwrap(), ScoreBound::PosInf);
        assert_eq!(parse_score_bound(b"+INF").unwrap(), ScoreBound::PosInf);
        assert_eq!(parse_score_bound(b"inf").unwrap(), ScoreBound::PosInf);
        assert_eq!(parse_score_bound(b"INF").unwrap(), ScoreBound::PosInf);
    }

    #[test]
    fn parse_score_inclusive() {
        assert_eq!(parse_score_bound(b"1.5").unwrap(), ScoreBound::Inclusive(1.5));
        assert_eq!(parse_score_bound(b"0").unwrap(), ScoreBound::Inclusive(0.0));
        assert_eq!(parse_score_bound(b"-5").unwrap(), ScoreBound::Inclusive(-5.0));
    }

    #[test]
    fn parse_score_exclusive() {
        assert_eq!(parse_score_bound(b"(1.5").unwrap(), ScoreBound::Exclusive(1.5));
        assert_eq!(parse_score_bound(b"(0").unwrap(), ScoreBound::Exclusive(0.0));
        assert_eq!(parse_score_bound(b"(-5").unwrap(), ScoreBound::Exclusive(-5.0));
    }

    #[test]
    fn parse_score_invalid() {
        assert!(parse_score_bound(b"abc").is_err());
        assert!(parse_score_bound(b"(abc").is_err());
        assert!(parse_score_bound(b"").is_err());
    }

    // -- LexBound parsing --

    #[test]
    fn parse_lex_inf() {
        assert_eq!(parse_lex_bound(b"-").unwrap(), LexBound::NegInf);
        assert_eq!(parse_lex_bound(b"+").unwrap(), LexBound::PosInf);
    }

    #[test]
    fn parse_lex_inclusive() {
        assert_eq!(
            parse_lex_bound(b"[abc").unwrap(),
            LexBound::Inclusive(b"abc".to_vec())
        );
        assert_eq!(
            parse_lex_bound(b"[").unwrap(),
            LexBound::Inclusive(b"".to_vec())
        );
    }

    #[test]
    fn parse_lex_exclusive() {
        assert_eq!(
            parse_lex_bound(b"(abc").unwrap(),
            LexBound::Exclusive(b"abc".to_vec())
        );
    }

    #[test]
    fn parse_lex_invalid() {
        assert!(parse_lex_bound(b"").is_err());
        assert!(parse_lex_bound(b"abc").is_err());
    }

    // -- Score range checks --

    #[test]
    fn score_gte_neg_inf_always_true() {
        assert!(score_gte_bound(f64::NEG_INFINITY, &ScoreBound::NegInf));
        assert!(score_gte_bound(0.0, &ScoreBound::NegInf));
    }

    #[test]
    fn score_gte_pos_inf_always_false() {
        assert!(!score_gte_bound(f64::INFINITY, &ScoreBound::PosInf));
    }

    #[test]
    fn score_gte_inclusive() {
        assert!(score_gte_bound(5.0, &ScoreBound::Inclusive(5.0)));
        assert!(score_gte_bound(6.0, &ScoreBound::Inclusive(5.0)));
        assert!(!score_gte_bound(4.0, &ScoreBound::Inclusive(5.0)));
    }

    #[test]
    fn score_gte_exclusive() {
        assert!(!score_gte_bound(5.0, &ScoreBound::Exclusive(5.0)));
        assert!(score_gte_bound(5.1, &ScoreBound::Exclusive(5.0)));
    }

    #[test]
    fn score_lte_pos_inf_always_true() {
        assert!(score_lte_bound(f64::INFINITY, &ScoreBound::PosInf));
    }

    #[test]
    fn score_lte_neg_inf_always_false() {
        assert!(!score_lte_bound(f64::NEG_INFINITY, &ScoreBound::NegInf));
    }

    #[test]
    fn score_lte_inclusive() {
        assert!(score_lte_bound(5.0, &ScoreBound::Inclusive(5.0)));
        assert!(score_lte_bound(4.0, &ScoreBound::Inclusive(5.0)));
        assert!(!score_lte_bound(6.0, &ScoreBound::Inclusive(5.0)));
    }

    #[test]
    fn score_lte_exclusive() {
        assert!(!score_lte_bound(5.0, &ScoreBound::Exclusive(5.0)));
        assert!(score_lte_bound(4.9, &ScoreBound::Exclusive(5.0)));
    }

    #[test]
    fn score_in_range_inclusive() {
        let min = ScoreBound::Inclusive(1.0);
        let max = ScoreBound::Inclusive(3.0);
        assert!(score_in_range(1.0, &min, &max));
        assert!(score_in_range(2.0, &min, &max));
        assert!(score_in_range(3.0, &min, &max));
        assert!(!score_in_range(0.5, &min, &max));
        assert!(!score_in_range(3.5, &min, &max));
    }

    #[test]
    fn score_in_range_exclusive() {
        let min = ScoreBound::Exclusive(1.0);
        let max = ScoreBound::Exclusive(3.0);
        assert!(!score_in_range(1.0, &min, &max));
        assert!(score_in_range(2.0, &min, &max));
        assert!(!score_in_range(3.0, &min, &max));
    }

    #[test]
    fn score_in_range_inf() {
        let min = ScoreBound::NegInf;
        let max = ScoreBound::PosInf;
        assert!(score_in_range(f64::NEG_INFINITY, &min, &max));
        assert!(score_in_range(0.0, &min, &max));
        assert!(score_in_range(f64::INFINITY, &min, &max));
    }

    // -- Lex range checks --

    #[test]
    fn lex_gte_neg_inf_always_true() {
        assert!(member_gte_lex_bound(b"", &LexBound::NegInf));
        assert!(member_gte_lex_bound(b"abc", &LexBound::NegInf));
    }

    #[test]
    fn lex_gte_pos_inf_always_false() {
        assert!(!member_gte_lex_bound(b"\xff\xff\xff", &LexBound::PosInf));
    }

    #[test]
    fn lex_gte_inclusive() {
        assert!(member_gte_lex_bound(b"b", &LexBound::Inclusive(b"b".to_vec())));
        assert!(member_gte_lex_bound(b"c", &LexBound::Inclusive(b"b".to_vec())));
        assert!(!member_gte_lex_bound(b"a", &LexBound::Inclusive(b"b".to_vec())));
    }

    #[test]
    fn lex_gte_exclusive() {
        assert!(!member_gte_lex_bound(b"b", &LexBound::Exclusive(b"b".to_vec())));
        assert!(member_gte_lex_bound(b"c", &LexBound::Exclusive(b"b".to_vec())));
    }

    #[test]
    fn lex_in_range_full() {
        let min = LexBound::NegInf;
        let max = LexBound::PosInf;
        assert!(member_in_lex_range(b"anything", &min, &max));
    }

    #[test]
    fn lex_in_range_bounded() {
        let min = LexBound::Inclusive(b"b".to_vec());
        let max = LexBound::Exclusive(b"d".to_vec());
        assert!(!member_in_lex_range(b"a", &min, &max));
        assert!(member_in_lex_range(b"b", &min, &max));
        assert!(member_in_lex_range(b"c", &min, &max));
        assert!(!member_in_lex_range(b"d", &min, &max));
    }
}
