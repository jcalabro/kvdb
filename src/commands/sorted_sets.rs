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

use bytes::Bytes;
use foundationdb::{FdbBindingError, RangeOption, Transaction};
use rand::Rng;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::directories::Directories;
use crate::storage::helpers;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::run_transact;

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
// Score byte helpers
// ---------------------------------------------------------------------------

/// Encode a score as big-endian bytes for storage in `zset_idx` values.
pub(crate) fn score_to_bytes(score: f64) -> [u8; 8] {
    score.to_be_bytes()
}

/// Decode a score from big-endian bytes stored in `zset_idx` values.
pub(crate) fn score_from_bytes(bytes: &[u8]) -> Result<f64, CommandError> {
    let arr: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CommandError::Generic(format!("invalid score bytes: expected 8, got {}", bytes.len())))?;
    Ok(f64::from_be_bytes(arr))
}

// ---------------------------------------------------------------------------
// Score parsing / formatting
// ---------------------------------------------------------------------------

/// Parse a byte argument as an f64 score, rejecting NaN.
///
/// Redis rejects NaN scores: `ZADD key NaN member` returns an error.
pub(crate) fn parse_score_arg(arg: &[u8]) -> Result<f64, CommandError> {
    let s = std::str::from_utf8(arg).map_err(|_| CommandError::Generic("ERR value is not a valid float".into()))?;

    let val: f64 = s
        .parse()
        .map_err(|_| CommandError::Generic("ERR value is not a valid float".into()))?;

    if val.is_nan() {
        return Err(CommandError::Generic("ERR value is not a valid float".into()));
    }

    // Normalize -0.0 to 0.0. Redis treats negative zero identically to
    // positive zero, but FDB's tuple layer encodes them differently which
    // would cause incorrect sort ordering in the score index.
    if val == 0.0 {
        return Ok(0.0);
    }

    Ok(val)
}

/// Format an f64 score for Redis RESP output.
///
/// - Integer-valued scores have no decimal: `1.0` → `"1"`
/// - Infinity: `"inf"` / `"-inf"`
/// - Otherwise: minimal decimal representation
pub(crate) fn format_score(score: f64) -> String {
    if score == f64::INFINITY {
        return "inf".to_string();
    }
    if score == f64::NEG_INFINITY {
        return "-inf".to_string();
    }
    if score.fract() == 0.0 && score.is_finite() {
        // Same boundary check as util::format_redis_float.
        if score >= i64::MIN as f64 && score < i64::MAX as f64 {
            return format!("{}", score as i64);
        }
    }

    let mut s = ryu::Buffer::new().format(score).to_string();
    if s.contains('.') {
        let trimmed = s.trim_end_matches('0');
        let trimmed = trimmed.trim_end_matches('.');
        s.truncate(trimmed.len());
    }
    s
}

// ---------------------------------------------------------------------------
// Meta read helpers
// ---------------------------------------------------------------------------

/// Read the live ObjectMeta for a sorted set key (write path).
///
/// Reads WITHOUT expiry filtering so expired/wrong-type data can be
/// cleaned up in the same transaction. Returns `(live_meta,
/// old_cardinality, old_expires_at_ms)`.
///
/// Returns `Err(WRONGTYPE)` if the key is live and not a sorted set.
pub(crate) async fn read_zset_meta_for_write(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
) -> Result<(Option<ObjectMeta>, u64, u64), FdbBindingError> {
    let now = helpers::now_ms();

    // Read meta WITHOUT expiry filtering so we can clean up expired data.
    let raw_meta = ObjectMeta::read(tr, dirs, key, 0, false)
        .await
        .map_err(helpers::storage_err)?;

    // Determine if the key is live (exists and not expired).
    let live_meta = raw_meta.as_ref().filter(|m| !m.is_expired(now));

    // Type check: if the key is live and not a sorted set, reject.
    if let Some(m) = live_meta
        && m.key_type != KeyType::SortedSet
    {
        return Err(helpers::cmd_err(CommandError::WrongType));
    }

    // If old meta exists and is either the wrong type or expired, clean up.
    if let Some(old_meta) = &raw_meta
        && (old_meta.is_expired(now) || old_meta.key_type != KeyType::SortedSet)
    {
        helpers::delete_all_data_and_meta(tr, dirs, key, old_meta).map_err(helpers::cmd_err)?;
    }

    let old_cardinality = live_meta.map_or(0, |m| m.cardinality);
    let old_expires_at_ms = live_meta.map_or(0, |m| m.expires_at_ms);

    Ok((live_meta.cloned(), old_cardinality, old_expires_at_ms))
}

/// Read the live ObjectMeta for a sorted set key (read-only path).
///
/// Returns `Ok(None)` if the key doesn't exist or is expired.
/// Returns `Err(WRONGTYPE)` if the key exists but is not a sorted set.
pub(crate) async fn read_zset_meta_for_read(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
) -> Result<Option<ObjectMeta>, FdbBindingError> {
    let now = helpers::now_ms();

    let meta = ObjectMeta::read(tr, dirs, key, now, false)
        .await
        .map_err(helpers::storage_err)?;

    match meta {
        None => Ok(None),
        Some(m) => {
            if m.key_type != KeyType::SortedSet {
                return Err(helpers::cmd_err(CommandError::WrongType));
            }
            Ok(Some(m))
        }
    }
}

// ---------------------------------------------------------------------------
// Dual-index mutation helpers
// ---------------------------------------------------------------------------

/// Set a member's score in a sorted set, maintaining both indexes.
///
/// If `old_score` is `Some` and differs from `score` (compared via
/// `to_bits()` to distinguish -0.0 from 0.0), the old score index
/// entry is removed before the new one is written.
pub(crate) fn zset_set_member(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    member: &[u8],
    score: f64,
    old_score: Option<f64>,
) {
    // If the score changed, remove the old score index entry.
    if let Some(old) = old_score
        && old.to_bits() != score.to_bits()
    {
        let old_zset_key = dirs.zset.pack(&(key, old, member));
        tr.clear(&old_zset_key);
    }

    // Write the score index entry: zset/<key, score, member> → b""
    let zset_key = dirs.zset.pack(&(key, score, member));
    tr.set(&zset_key, b"");

    // Write the reverse index entry: zset_idx/<key, member> → score_bytes
    let idx_key = dirs.zset_idx.pack(&(key, member));
    tr.set(&idx_key, &score_to_bytes(score));
}

/// Remove a member from a sorted set, clearing both indexes.
pub(crate) fn zset_remove_member(tr: &Transaction, dirs: &Directories, key: &[u8], member: &[u8], score: f64) {
    // Clear score index: zset/<key, score, member>
    let zset_key = dirs.zset.pack(&(key, score, member));
    tr.clear(&zset_key);

    // Clear reverse index: zset_idx/<key, member>
    let idx_key = dirs.zset_idx.pack(&(key, member));
    tr.clear(&idx_key);
}

/// Look up a member's score via the reverse index (`zset_idx`).
///
/// Returns `None` if the member does not exist in the sorted set.
pub(crate) async fn zset_get_score(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    member: &[u8],
    snapshot: bool,
) -> Result<Option<f64>, FdbBindingError> {
    let idx_key = dirs.zset_idx.pack(&(key, member));
    let val = tr.get(&idx_key, snapshot).await?;

    match val {
        None => Ok(None),
        Some(bytes) => {
            let score = score_from_bytes(&bytes).map_err(helpers::cmd_err)?;
            Ok(Some(score))
        }
    }
}

// ---------------------------------------------------------------------------
// Read helper: paginated range read of sorted set members
// ---------------------------------------------------------------------------

/// Read all members of a sorted set in score order via paginated range read.
///
/// Returns `Vec<(score, member)>` ordered by (score, member).
/// Uses the `zset/<key, score, member>` index for natural ordering.
pub(crate) async fn read_zset_members_by_score(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    snapshot: bool,
) -> Result<Vec<(f64, Vec<u8>)>, CommandError> {
    let sub = dirs.zset.subspace(&(key,));
    let (begin, end) = sub.range();
    let mut maybe_range: Option<RangeOption<'_>> = Some(RangeOption::from((begin.as_slice(), end.as_slice())));
    let mut members = Vec::new();
    let mut iteration = 1;

    while let Some(range_opt) = maybe_range.take() {
        let kvs = tr
            .get_range(&range_opt, iteration, snapshot)
            .await
            .map_err(|e| CommandError::Generic(e.to_string()))?;

        for kv in kvs.iter() {
            let (_, score, member): (Vec<u8>, f64, Vec<u8>) = dirs
                .zset
                .unpack(kv.key())
                .map_err(|e| CommandError::Generic(format!("zset key unpack: {e:?}")))?;
            members.push((score, member));
        }

        maybe_range = range_opt.next_range(&kvs);
        iteration += 1;
    }

    Ok(members)
}

// ---------------------------------------------------------------------------
// Write helper: write or delete sorted set meta
// ---------------------------------------------------------------------------

/// Write updated ObjectMeta for a sorted set, or delete the key if empty.
///
/// If `new_cardinality` is 0, the key is deleted entirely (meta + data +
/// expire). Otherwise, meta is written with the new cardinality, preserving
/// the TTL from `old_meta` if it was set.
pub(crate) fn write_or_delete_zset_meta(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    old_meta: Option<&ObjectMeta>,
    new_cardinality: u64,
) -> Result<(), FdbBindingError> {
    if new_cardinality == 0 {
        // Delete the key entirely.
        if let Some(old) = old_meta {
            helpers::delete_all_data_and_meta(tr, dirs, key, old).map_err(helpers::cmd_err)?;
        }
    } else {
        let old_expires = old_meta.map_or(0, |m| m.expires_at_ms);
        let mut meta = ObjectMeta::new_sorted_set(new_cardinality);
        meta.expires_at_ms = old_expires;
        meta.write(tr, dirs, key).map_err(helpers::storage_err)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
// ---------------------------------------------------------------------------

/// ZADD flags parsed from the command arguments.
struct ZaddFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
}

/// Parse ZADD flags from the arguments after the key.
///
/// Flags come before the score-member pairs and are case-insensitive.
/// Returns the parsed flags and the index of the first score argument.
fn parse_zadd_flags(args: &[Bytes]) -> Result<(ZaddFlags, usize), CommandError> {
    let mut flags = ZaddFlags {
        nx: false,
        xx: false,
        gt: false,
        lt: false,
        ch: false,
    };

    let mut i = 0;
    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"NX" => flags.nx = true,
            b"XX" => flags.xx = true,
            b"GT" => flags.gt = true,
            b"LT" => flags.lt = true,
            b"CH" => flags.ch = true,
            _ => break, // First non-flag argument — must be a score.
        }
        i += 1;
    }

    // Validate flag combinations.
    if flags.nx && flags.xx {
        return Err(CommandError::Generic(
            "ERR XX and NX options at the same time are not compatible".into(),
        ));
    }
    if flags.nx && (flags.gt || flags.lt) {
        return Err(CommandError::Generic(
            "ERR GT, LT, and NX options at the same time are not compatible".into(),
        ));
    }

    Ok((flags, i))
}

/// ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
///
/// Add or update members in a sorted set. Returns the number of members
/// added (or added+updated with CH flag).
pub async fn handle_zadd(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Need at least: key score member
    if args.len() < 3 {
        return RespValue::err(CommandError::WrongArity { name: "ZADD".into() }.to_string());
    }

    let key = &args[0];

    // Parse flags from args[1..].
    let (flags, flag_end) = match parse_zadd_flags(&args[1..]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    let score_member_args = &args[1 + flag_end..];

    // Must have at least one score-member pair, and even count.
    if score_member_args.is_empty() || !score_member_args.len().is_multiple_of(2) {
        return RespValue::err(CommandError::WrongArity { name: "ZADD".into() }.to_string());
    }

    // Pre-parse all scores before entering the transaction.
    let mut pairs: Vec<(f64, Bytes)> = Vec::with_capacity(score_member_args.len() / 2);
    for chunk in score_member_args.chunks(2) {
        let score = match parse_score_arg(&chunk[0]) {
            Ok(s) => s,
            Err(e) => return RespValue::err(e.to_string()),
        };
        pairs.push((score, chunk[1].clone()));
    }

    let nx = flags.nx;
    let xx = flags.xx;
    let gt = flags.gt;
    let lt = flags.lt;
    let ch = flags.ch;

    match run_transact(&state.db, state.shared_txn(), "ZADD", |tr| {
        let dirs = state.dirs.clone();
        let key = key.clone();
        let pairs = pairs.clone();
        async move {
            let (live_meta, old_cardinality, _old_expires_at_ms) = read_zset_meta_for_write(&tr, &dirs, &key).await?;

            let mut added: i64 = 0;
            let mut updated: i64 = 0;

            for (score, member) in &pairs {
                let existing_score = zset_get_score(&tr, &dirs, &key, member, false).await?;

                match existing_score {
                    Some(old_score) => {
                        // Member exists. NX means skip updates.
                        if nx {
                            continue;
                        }
                        // GT: only update if new score > old score.
                        if gt && *score <= old_score {
                            continue;
                        }
                        // LT: only update if new score < old score.
                        if lt && *score >= old_score {
                            continue;
                        }
                        // Score unchanged — no update needed.
                        if old_score.to_bits() == score.to_bits() {
                            continue;
                        }
                        zset_set_member(&tr, &dirs, &key, member, *score, Some(old_score));
                        updated += 1;
                    }
                    None => {
                        // Member doesn't exist. XX means skip new members.
                        if xx {
                            continue;
                        }
                        zset_set_member(&tr, &dirs, &key, member, *score, None);
                        added += 1;
                    }
                }
            }

            let new_cardinality = (old_cardinality as i64 + added) as u64;
            write_or_delete_zset_meta(&tr, &dirs, &key, live_meta.as_ref(), new_cardinality)?;

            let result = if ch { added + updated } else { added };
            Ok(result)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZCARD key
// ---------------------------------------------------------------------------

/// ZCARD key — Returns the cardinality of a sorted set.
///
/// Returns 0 if the key doesn't exist.
pub async fn handle_zcard(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "ZCARD".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "ZCARD", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            Ok(meta.map_or(0i64, |m| m.cardinality as i64))
        }
    })
    .await
    {
        Ok(card) => RespValue::Integer(card),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZSCORE key member
// ---------------------------------------------------------------------------

/// ZSCORE key member — Returns the score of a member in a sorted set.
///
/// Returns a bulk string with the score, or Nil if the member or key
/// doesn't exist.
pub async fn handle_zscore(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "ZSCORE".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "ZSCORE", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let member = args[1].clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            if meta.is_none() {
                return Ok(None);
            }
            zset_get_score(&tr, &dirs, &key, &member, false).await
        }
    })
    .await
    {
        Ok(Some(score)) => RespValue::BulkString(Some(Bytes::from(format_score(score)))),
        Ok(None) => RespValue::BulkString(None),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZREM key member [member ...]
// ---------------------------------------------------------------------------

/// ZREM key member [member ...] — Remove members from a sorted set.
///
/// Returns the number of members actually removed. If all members are
/// removed, the key itself is deleted.
pub async fn handle_zrem(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "ZREM".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "ZREM", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let (live_meta, old_cardinality, _old_expires_at_ms) = read_zset_meta_for_write(&tr, &dirs, key).await?;

            if live_meta.is_none() {
                return Ok(0i64);
            }

            let mut removed: i64 = 0;

            for member in &args[1..] {
                let score = zset_get_score(&tr, &dirs, key, member, false).await?;
                if let Some(s) = score {
                    zset_remove_member(&tr, &dirs, key, member, s);
                    removed += 1;
                }
            }

            let new_cardinality = (old_cardinality as i64 - removed) as u64;
            write_or_delete_zset_meta(&tr, &dirs, key, live_meta.as_ref(), new_cardinality)?;

            Ok(removed)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZRANK key member / ZREVRANK key member
// ---------------------------------------------------------------------------

/// Shared implementation for ZRANK and ZREVRANK.
///
/// Returns the 0-based rank of a member ordered by ascending score (ZRANK)
/// or descending score (ZREVRANK). Returns Nil if the key or member doesn't
/// exist.
async fn handle_rank_impl(args: &[Bytes], state: &ConnectionState, cmd_name: &'static str, reverse: bool) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: cmd_name.into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), cmd_name, |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let member = args[1].clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            if meta.is_none() {
                return Ok(None);
            }

            let score = zset_get_score(&tr, &dirs, &key, &member, false).await?;
            let score = match score {
                Some(s) => s,
                None => return Ok(None),
            };

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            // Find the position of (score, member) in the list.
            let pos = members
                .iter()
                .position(|(s, m)| s.to_bits() == score.to_bits() && m.as_slice() == member.as_ref());

            match pos {
                Some(p) => {
                    let rank = if reverse { members.len() - 1 - p } else { p };
                    Ok(Some(rank as i64))
                }
                None => Ok(None),
            }
        }
    })
    .await
    {
        Ok(Some(rank)) => RespValue::Integer(rank),
        Ok(None) => RespValue::BulkString(None),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

/// ZRANK key member — Returns the 0-based rank of a member (ascending score).
pub async fn handle_zrank(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_rank_impl(args, state, "ZRANK", false).await
}

/// ZREVRANK key member — Returns the 0-based rank of a member (descending score).
pub async fn handle_zrevrank(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_rank_impl(args, state, "ZREVRANK", true).await
}

// ---------------------------------------------------------------------------
// ZINCRBY key increment member
// ---------------------------------------------------------------------------

/// ZINCRBY key increment member — Increment a member's score.
///
/// If the member doesn't exist, it's created with the increment as score.
/// Returns the new score as a BulkString.
pub async fn handle_zincrby(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "ZINCRBY".into() }.to_string());
    }

    let increment = match parse_score_arg(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    match run_transact(&state.db, state.shared_txn(), "ZINCRBY", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let member = args[2].clone();
        async move {
            let (live_meta, old_cardinality, _old_expires_at_ms) = read_zset_meta_for_write(&tr, &dirs, &key).await?;

            let existing_score = zset_get_score(&tr, &dirs, &key, &member, false).await?;
            let old_score = existing_score.unwrap_or(0.0);
            let new_score = old_score + increment;

            if new_score.is_nan() {
                return Err(helpers::cmd_err(CommandError::Generic(
                    "ERR resulting score is not a number (NaN)".into(),
                )));
            }

            zset_set_member(&tr, &dirs, &key, &member, new_score, existing_score);

            let new_cardinality = if existing_score.is_none() {
                old_cardinality + 1
            } else {
                old_cardinality
            };

            write_or_delete_zset_meta(&tr, &dirs, &key, live_meta.as_ref(), new_cardinality)?;

            Ok(format_score(new_score))
        }
    })
    .await
    {
        Ok(score_str) => RespValue::BulkString(Some(Bytes::from(score_str))),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZRANGE key start stop [WITHSCORES] / ZREVRANGE key start stop [WITHSCORES]
// ---------------------------------------------------------------------------

/// Shared implementation for ZRANGE and ZREVRANGE.
///
/// Returns members in rank range. Supports negative indices and WITHSCORES.
/// ZRANGE returns ascending score order, ZREVRANGE returns descending.
async fn handle_range_by_rank(
    args: &[Bytes],
    state: &ConnectionState,
    cmd_name: &'static str,
    reverse: bool,
) -> RespValue {
    // Minimum: key start stop. Optional: WITHSCORES.
    if args.len() < 3 || args.len() > 4 {
        return RespValue::err(CommandError::WrongArity { name: cmd_name.into() }.to_string());
    }

    let start = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let stop = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };

    let withscores = if args.len() == 4 {
        if args[3].to_ascii_uppercase() == b"WITHSCORES".as_slice() {
            true
        } else {
            return RespValue::err("ERR syntax error");
        }
    } else {
        false
    };

    match run_transact(&state.db, state.shared_txn(), cmd_name, |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            if meta.is_none() {
                return Ok(Vec::new());
            }

            let mut members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            if reverse {
                members.reverse();
            }

            let len = members.len() as i64;

            // Normalize negative indices.
            let norm_start = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len)
            };
            let norm_stop = if stop < 0 {
                (len + stop).max(-1)
            } else {
                stop.min(len - 1)
            };

            if norm_start > norm_stop || norm_start >= len {
                return Ok(Vec::new());
            }

            let slice = &members[norm_start as usize..=norm_stop as usize];
            Ok(slice.to_vec())
        }
    })
    .await
    {
        Ok(pairs) => {
            let mut result = Vec::new();
            for (score, member) in pairs {
                result.push(RespValue::BulkString(Some(Bytes::from(member))));
                if withscores {
                    result.push(RespValue::BulkString(Some(Bytes::from(format_score(score)))));
                }
            }
            RespValue::Array(Some(result))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

/// ZRANGE key start stop [WITHSCORES] — Return members in ascending rank order.
pub async fn handle_zrange(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_range_by_rank(args, state, "ZRANGE", false).await
}

/// ZREVRANGE key start stop [WITHSCORES] — Return members in descending rank order.
pub async fn handle_zrevrange(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_range_by_rank(args, state, "ZREVRANGE", true).await
}

// ---------------------------------------------------------------------------
// ZCOUNT key min max
// ---------------------------------------------------------------------------

/// ZCOUNT key min max — Count members with scores in [min, max].
///
/// Uses score bound syntax: `-inf`, `+inf`, `(exclusive`, `inclusive`.
pub async fn handle_zcount(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(CommandError::WrongArity { name: "ZCOUNT".into() }.to_string());
    }

    let min = match parse_score_bound(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };
    let max = match parse_score_bound(&args[2]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    match run_transact(&state.db, state.shared_txn(), "ZCOUNT", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let min = min.clone();
        let max = max.clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            if meta.is_none() {
                return Ok(0i64);
            }

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            let count = members.iter().filter(|(s, _)| score_in_range(*s, &min, &max)).count();
            Ok(count as i64)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZLEXCOUNT key min max
// ---------------------------------------------------------------------------

/// ZLEXCOUNT key min max — Count members in a lex range.
///
/// All members must have the same score for meaningful results.
/// Uses lex bound syntax: `-`, `+`, `[inclusive`, `(exclusive`.
pub async fn handle_zlexcount(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "ZLEXCOUNT".into(),
            }
            .to_string(),
        );
    }

    let min = match parse_lex_bound(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };
    let max = match parse_lex_bound(&args[2]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    match run_transact(&state.db, state.shared_txn(), "ZLEXCOUNT", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let min = min.clone();
        let max = max.clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            if meta.is_none() {
                return Ok(0i64);
            }

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            let count = members
                .iter()
                .filter(|(_, m)| member_in_lex_range(m.as_slice(), &min, &max))
                .count();
            Ok(count as i64)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
// ---------------------------------------------------------------------------

/// Parse a LIMIT clause at position `i` within `args`.
///
/// Expects `args[i]` to be "LIMIT", followed by offset and count integers.
/// Returns `(offset, count, next_index)`. Negative count means "all remaining"
/// per Redis behavior.
fn parse_limit_clause(args: &[Bytes], i: usize) -> Result<(usize, usize, usize), CommandError> {
    if i + 2 >= args.len() {
        return Err(CommandError::Generic("ERR syntax error".into()));
    }
    let off_str = std::str::from_utf8(&args[i + 1])
        .map_err(|_| CommandError::Generic("ERR value is not an integer or out of range".into()))?;
    let cnt_str = std::str::from_utf8(&args[i + 2])
        .map_err(|_| CommandError::Generic("ERR value is not an integer or out of range".into()))?;
    let offset = off_str
        .parse::<i64>()
        .map_err(|_| CommandError::Generic("ERR value is not an integer or out of range".into()))?
        as usize;
    let raw_count = cnt_str
        .parse::<i64>()
        .map_err(|_| CommandError::Generic("ERR value is not an integer or out of range".into()))?;
    // Negative count means "all remaining" per Redis behavior.
    let count = if raw_count < 0 { usize::MAX } else { raw_count as usize };
    Ok((offset, count, i + 3))
}

/// Parse optional WITHSCORES and LIMIT flags from trailing arguments.
///
/// Returns `(withscores, offset, count)` where offset/count default to 0/usize::MAX
/// (meaning "no limit").
fn parse_range_options(args: &[Bytes], start_idx: usize) -> Result<(bool, usize, usize), CommandError> {
    let mut withscores = false;
    let mut offset: usize = 0;
    let mut count: usize = usize::MAX;

    let mut i = start_idx;
    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            b"LIMIT" => {
                (offset, count, i) = parse_limit_clause(args, i)?;
            }
            _ => {
                return Err(CommandError::Generic("ERR syntax error".into()));
            }
        }
    }

    Ok((withscores, offset, count))
}

/// Parse optional LIMIT flags (no WITHSCORES) from trailing arguments.
///
/// Returns `(offset, count)` where offset/count default to 0/usize::MAX.
fn parse_limit_only(args: &[Bytes], start_idx: usize) -> Result<(usize, usize), CommandError> {
    let mut offset: usize = 0;
    let mut count: usize = usize::MAX;

    let mut i = start_idx;
    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"LIMIT" => {
                (offset, count, i) = parse_limit_clause(args, i)?;
            }
            _ => {
                return Err(CommandError::Generic("ERR syntax error".into()));
            }
        }
    }

    Ok((offset, count))
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
///
/// Return members with scores in [min, max] range. min/max use score
/// bound syntax: `-inf`, `+inf`, `(exclusive`, `inclusive`.
pub async fn handle_zrangebyscore(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "ZRANGEBYSCORE".into(),
            }
            .to_string(),
        );
    }

    let min = match parse_score_bound(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };
    let max = match parse_score_bound(&args[2]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    let (withscores, offset, count) = match parse_range_options(args, 3) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    match run_transact(&state.db, state.shared_txn(), "ZRANGEBYSCORE", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let min = min.clone();
        let max = max.clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            if meta.is_none() {
                return Ok(Vec::new());
            }

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            let filtered: Vec<(f64, Vec<u8>)> = members
                .into_iter()
                .filter(|(s, _)| score_in_range(*s, &min, &max))
                .skip(offset)
                .take(count)
                .collect();

            Ok(filtered)
        }
    })
    .await
    {
        Ok(pairs) => {
            let mut result = Vec::new();
            for (score, member) in pairs {
                result.push(RespValue::BulkString(Some(Bytes::from(member))));
                if withscores {
                    result.push(RespValue::BulkString(Some(Bytes::from(format_score(score)))));
                }
            }
            RespValue::Array(Some(result))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZRANGEBYLEX key min max [LIMIT offset count]
// ---------------------------------------------------------------------------

/// ZRANGEBYLEX key min max [LIMIT offset count]
///
/// Return members in a lexicographic range. All members should have the
/// same score for meaningful results. min/max use lex bound syntax:
/// `-`, `+`, `[inclusive`, `(exclusive`.
pub async fn handle_zrangebylex(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "ZRANGEBYLEX".into(),
            }
            .to_string(),
        );
    }

    let min = match parse_lex_bound(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };
    let max = match parse_lex_bound(&args[2]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    let (offset, count) = match parse_limit_only(args, 3) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    match run_transact(&state.db, state.shared_txn(), "ZRANGEBYLEX", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let min = min.clone();
        let max = max.clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            if meta.is_none() {
                return Ok(Vec::new());
            }

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            let filtered: Vec<Vec<u8>> = members
                .into_iter()
                .filter(|(_, m)| member_in_lex_range(m.as_slice(), &min, &max))
                .map(|(_, m)| m)
                .skip(offset)
                .take(count)
                .collect();

            Ok(filtered)
        }
    })
    .await
    {
        Ok(members) => {
            let result: Vec<RespValue> = members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(Bytes::from(m))))
                .collect();
            RespValue::Array(Some(result))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZREMRANGEBYRANK key start stop
// ---------------------------------------------------------------------------

/// ZREMRANGEBYRANK key start stop — Remove members by rank range.
///
/// Returns the number of members removed. Supports negative indices.
pub async fn handle_zremrangebyrank(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "ZREMRANGEBYRANK".into(),
            }
            .to_string(),
        );
    }

    let start = match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };
    let stop = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(v) => v,
        None => return RespValue::err("ERR value is not an integer or out of range"),
    };

    match run_transact(&state.db, state.shared_txn(), "ZREMRANGEBYRANK", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let (live_meta, old_cardinality, _) = read_zset_meta_for_write(&tr, &dirs, &key).await?;
            if live_meta.is_none() {
                return Ok(0i64);
            }

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            let len = members.len() as i64;

            // Normalize negative indices.
            let norm_start = if start < 0 {
                (len + start).max(0)
            } else {
                start.min(len)
            };
            let norm_stop = if stop < 0 {
                (len + stop).max(-1)
            } else {
                stop.min(len - 1)
            };

            if norm_start > norm_stop || norm_start >= len {
                return Ok(0i64);
            }

            let to_remove = &members[norm_start as usize..=norm_stop as usize];
            let removed = to_remove.len() as i64;

            for (score, member) in to_remove {
                zset_remove_member(&tr, &dirs, &key, member, *score);
            }

            let new_cardinality = (old_cardinality as i64 - removed) as u64;
            write_or_delete_zset_meta(&tr, &dirs, &key, live_meta.as_ref(), new_cardinality)?;

            Ok(removed)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZREMRANGEBYSCORE key min max
// ---------------------------------------------------------------------------

/// ZREMRANGEBYSCORE key min max — Remove members by score range.
///
/// Returns the number of members removed. Uses score bound syntax.
pub async fn handle_zremrangebyscore(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "ZREMRANGEBYSCORE".into(),
            }
            .to_string(),
        );
    }

    let min = match parse_score_bound(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };
    let max = match parse_score_bound(&args[2]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    match run_transact(&state.db, state.shared_txn(), "ZREMRANGEBYSCORE", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let min = min.clone();
        let max = max.clone();
        async move {
            let (live_meta, old_cardinality, _) = read_zset_meta_for_write(&tr, &dirs, &key).await?;
            if live_meta.is_none() {
                return Ok(0i64);
            }

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            let to_remove: Vec<_> = members.iter().filter(|(s, _)| score_in_range(*s, &min, &max)).collect();
            let removed = to_remove.len() as i64;

            for (score, member) in &to_remove {
                zset_remove_member(&tr, &dirs, &key, member, *score);
            }

            let new_cardinality = (old_cardinality as i64 - removed) as u64;
            write_or_delete_zset_meta(&tr, &dirs, &key, live_meta.as_ref(), new_cardinality)?;

            Ok(removed)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZREMRANGEBYLEX key min max
// ---------------------------------------------------------------------------

/// ZREMRANGEBYLEX key min max — Remove members by lex range.
///
/// Returns the number of members removed. Uses lex bound syntax.
pub async fn handle_zremrangebylex(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "ZREMRANGEBYLEX".into(),
            }
            .to_string(),
        );
    }

    let min = match parse_lex_bound(&args[1]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };
    let max = match parse_lex_bound(&args[2]) {
        Ok(v) => v,
        Err(e) => return RespValue::err(e.to_string()),
    };

    match run_transact(&state.db, state.shared_txn(), "ZREMRANGEBYLEX", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let min = min.clone();
        let max = max.clone();
        async move {
            let (live_meta, old_cardinality, _) = read_zset_meta_for_write(&tr, &dirs, &key).await?;
            if live_meta.is_none() {
                return Ok(0i64);
            }

            let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            let to_remove: Vec<_> = members
                .iter()
                .filter(|(_, m)| member_in_lex_range(m.as_slice(), &min, &max))
                .collect();
            let removed = to_remove.len() as i64;

            for (score, member) in &to_remove {
                zset_remove_member(&tr, &dirs, &key, member, *score);
            }

            let new_cardinality = (old_cardinality as i64 - removed) as u64;
            write_or_delete_zset_meta(&tr, &dirs, &key, live_meta.as_ref(), new_cardinality)?;

            Ok(removed)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZPOPMIN key [count] / ZPOPMAX key [count]
// ---------------------------------------------------------------------------

/// Shared implementation for ZPOPMIN and ZPOPMAX.
///
/// Pops the lowest (pop_min=true) or highest (pop_min=false) scored members.
/// Returns an interleaved array of [member, score, ...] pairs.
async fn handle_zpop_impl(args: &[Bytes], state: &ConnectionState, cmd_name: &'static str, pop_min: bool) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return RespValue::err(CommandError::WrongArity { name: cmd_name.into() }.to_string());
    }

    let count: usize = if args.len() == 2 {
        match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(c) if c >= 0 => c as usize,
            _ => return RespValue::err("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };

    match run_transact(&state.db, state.shared_txn(), cmd_name, |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let (live_meta, old_cardinality, _) = read_zset_meta_for_write(&tr, &dirs, &key).await?;
            if live_meta.is_none() {
                return Ok(Vec::new());
            }

            let mut members = read_zset_members_by_score(&tr, &dirs, &key, false)
                .await
                .map_err(helpers::cmd_err)?;

            if !pop_min {
                members.reverse();
            }

            let n = count.min(members.len());
            let popped: Vec<(f64, Vec<u8>)> = members.into_iter().take(n).collect();

            for (score, member) in &popped {
                zset_remove_member(&tr, &dirs, &key, member, *score);
            }

            let new_cardinality = (old_cardinality as i64 - popped.len() as i64) as u64;
            write_or_delete_zset_meta(&tr, &dirs, &key, live_meta.as_ref(), new_cardinality)?;

            Ok(popped)
        }
    })
    .await
    {
        Ok(pairs) => {
            let mut result = Vec::new();
            for (score, member) in pairs {
                result.push(RespValue::BulkString(Some(Bytes::from(member))));
                result.push(RespValue::BulkString(Some(Bytes::from(format_score(score)))));
            }
            RespValue::Array(Some(result))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

/// ZPOPMIN key [count] — Pop members with the lowest scores.
pub async fn handle_zpopmin(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_zpop_impl(args, state, "ZPOPMIN", true).await
}

/// ZPOPMAX key [count] — Pop members with the highest scores.
pub async fn handle_zpopmax(args: &[Bytes], state: &ConnectionState) -> RespValue {
    handle_zpop_impl(args, state, "ZPOPMAX", false).await
}

// ---------------------------------------------------------------------------
// ZRANDMEMBER key [count [WITHSCORES]]
// ---------------------------------------------------------------------------

/// ZRANDMEMBER key [count [WITHSCORES]]
///
/// Return random members without removing them.
/// - No count: return one random member as BulkString, Nil if empty.
/// - Positive count: up to `count` distinct members (Array).
/// - Negative count: `|count|` members, may repeat (Array).
pub async fn handle_zrandmember(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() || args.len() > 3 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "ZRANDMEMBER".into(),
            }
            .to_string(),
        );
    }

    let has_count = args.len() >= 2;
    let count: i64 = if has_count {
        match std::str::from_utf8(&args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(v) => v,
            None => return RespValue::err("ERR value is not an integer or out of range"),
        }
    } else {
        0 // unused when has_count is false
    };

    let withscores = if args.len() == 3 {
        if args[2].to_ascii_uppercase() == b"WITHSCORES".as_slice() {
            true
        } else {
            return RespValue::err("ERR syntax error");
        }
    } else {
        false
    };

    match run_transact(&state.db, state.shared_txn(), "ZRANDMEMBER", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let meta = read_zset_meta_for_read(&tr, &dirs, &key).await?;
            match meta {
                None => Ok(None),
                Some(_) => {
                    let members = read_zset_members_by_score(&tr, &dirs, &key, false)
                        .await
                        .map_err(helpers::cmd_err)?;
                    Ok(Some(members))
                }
            }
        }
    })
    .await
    {
        Ok(None) => {
            if !has_count {
                RespValue::BulkString(None)
            } else {
                RespValue::Array(Some(vec![]))
            }
        }
        Ok(Some(members)) => {
            if members.is_empty() {
                if !has_count {
                    return RespValue::BulkString(None);
                } else {
                    return RespValue::Array(Some(vec![]));
                }
            }

            if !has_count {
                // Single random member.
                let mut rng = rand::thread_rng();
                let idx = rng.gen_range(0..members.len());
                return RespValue::BulkString(Some(Bytes::from(members[idx].1.clone())));
            }

            if count == 0 {
                return RespValue::Array(Some(vec![]));
            }

            let mut rng = rand::thread_rng();

            if count > 0 {
                // Distinct selection: up to `count` unique members.
                let n = (count as usize).min(members.len());
                let mut indices: Vec<usize> = (0..members.len()).collect();
                for i in 0..n {
                    let j = rng.gen_range(i..indices.len());
                    indices.swap(i, j);
                }
                let result: Vec<RespValue> = indices[..n]
                    .iter()
                    .flat_map(|&idx| {
                        let mut items = vec![RespValue::BulkString(Some(Bytes::from(members[idx].1.clone())))];
                        if withscores {
                            items.push(RespValue::BulkString(Some(Bytes::from(format_score(members[idx].0)))));
                        }
                        items
                    })
                    .collect();
                RespValue::Array(Some(result))
            } else {
                // count < 0: allow duplicates, return |count| members.
                let n = count.unsigned_abs() as usize;
                let result: Vec<RespValue> = (0..n)
                    .flat_map(|_| {
                        let idx = rng.gen_range(0..members.len());
                        let mut items = vec![RespValue::BulkString(Some(Bytes::from(members[idx].1.clone())))];
                        if withscores {
                            items.push(RespValue::BulkString(Some(Bytes::from(format_score(members[idx].0)))));
                        }
                        items
                    })
                    .collect();
                RespValue::Array(Some(result))
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// ZMSCORE key member [member ...]
// ---------------------------------------------------------------------------

/// ZMSCORE key member [member ...] — Return scores for multiple members.
///
/// Returns an array where each element is the score (BulkString) or Nil.
pub async fn handle_zmscore(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(CommandError::WrongArity { name: "ZMSCORE".into() }.to_string());
    }

    match run_transact(&state.db, state.shared_txn(), "ZMSCORE", |tr| {
        let dirs = state.dirs.clone();
        let args = args.to_vec();
        async move {
            let key = &args[0];
            let meta = read_zset_meta_for_read(&tr, &dirs, key).await?;

            let mut scores: Vec<Option<f64>> = Vec::with_capacity(args.len() - 1);

            if meta.is_none() {
                // Key doesn't exist — all Nils.
                for _ in 1..args.len() {
                    scores.push(None);
                }
            } else {
                for member in &args[1..] {
                    let score = zset_get_score(&tr, &dirs, key, member, false).await?;
                    scores.push(score);
                }
            }

            Ok(scores)
        }
    })
    .await
    {
        Ok(scores) => {
            let result: Vec<RespValue> = scores
                .into_iter()
                .map(|s| match s {
                    Some(score) => RespValue::BulkString(Some(Bytes::from(format_score(score)))),
                    None => RespValue::BulkString(None),
                })
                .collect();
            RespValue::Array(Some(result))
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
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
        assert_eq!(parse_lex_bound(b"[abc").unwrap(), LexBound::Inclusive(b"abc".to_vec()));
        assert_eq!(parse_lex_bound(b"[").unwrap(), LexBound::Inclusive(b"".to_vec()));
    }

    #[test]
    fn parse_lex_exclusive() {
        assert_eq!(parse_lex_bound(b"(abc").unwrap(), LexBound::Exclusive(b"abc".to_vec()));
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

    // -- Score bytes roundtrip --

    #[test]
    fn score_bytes_roundtrip() {
        let cases = [
            0.0,
            -0.0,
            1.0,
            -1.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            1e100,
            -1e100,
            0.1,
        ];
        for score in cases {
            let bytes = score_to_bytes(score);
            let decoded = score_from_bytes(&bytes).unwrap();
            assert_eq!(score.to_bits(), decoded.to_bits(), "roundtrip failed for {score}");
        }
    }

    #[test]
    fn score_from_bytes_wrong_length() {
        assert!(score_from_bytes(&[1, 2, 3]).is_err());
        assert!(score_from_bytes(&[]).is_err());
        assert!(score_from_bytes(&[0; 9]).is_err());
    }

    // -- format_score --

    #[test]
    fn format_score_integers() {
        assert_eq!(format_score(0.0), "0");
        assert_eq!(format_score(1.0), "1");
        assert_eq!(format_score(-42.0), "-42");
    }

    #[test]
    fn format_score_fractions() {
        assert_eq!(format_score(1.5), "1.5");
        assert_eq!(format_score(-0.5), "-0.5");
    }

    #[test]
    fn format_score_infinity() {
        assert_eq!(format_score(f64::INFINITY), "inf");
        assert_eq!(format_score(f64::NEG_INFINITY), "-inf");
    }

    // -- parse_score_arg --

    #[test]
    fn parse_score_arg_valid() {
        assert_eq!(parse_score_arg(b"0").unwrap(), 0.0);
        assert_eq!(parse_score_arg(b"1.5").unwrap(), 1.5);
        assert_eq!(parse_score_arg(b"-42").unwrap(), -42.0);
        assert_eq!(parse_score_arg(b"inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_score_arg(b"-inf").unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn parse_score_arg_rejects_nan() {
        assert!(parse_score_arg(b"NaN").is_err());
        assert!(parse_score_arg(b"nan").is_err());
    }

    #[test]
    fn parse_score_arg_rejects_garbage() {
        assert!(parse_score_arg(b"abc").is_err());
        assert!(parse_score_arg(b"").is_err());
    }

    #[test]
    fn parse_score_arg_normalizes_negative_zero() {
        let val = parse_score_arg(b"-0").unwrap();
        // Must be positive zero (bit pattern), not negative zero.
        assert_eq!(val.to_bits(), 0.0_f64.to_bits());
        assert_ne!(val.to_bits(), (-0.0_f64).to_bits());
    }
}
