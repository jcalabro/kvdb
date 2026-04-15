// tests/sorted_sets.rs
//! Integration tests for sorted set commands.
//!
//! Covers: ZADD (NX/XX/GT/LT/CH), ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD,
//! ZCOUNT, ZLEXCOUNT, ZINCRBY, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANGEBYLEX,
//! ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZPOPMIN, ZPOPMAX,
//! ZRANDMEMBER, ZMSCORE.
//!
//! Tests the full path: TCP connect -> RESP -> dispatch -> FDB -> response.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

// ---------------------------------------------------------------------------
// ZADD + ZSCORE round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_single_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.5)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 1);

    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 1.5).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zadd_multiple_members() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .arg(3.0)
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 3);

    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 3);
}

#[tokio::test]
async fn zadd_update_existing_score() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // Update alice's score — returns 0 (updated, not added).
    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(5.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 0);

    // Score should be updated.
    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 5.0).abs() < f64::EPSILON);

    // Cardinality should still be 1.
    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);
}

// ---------------------------------------------------------------------------
// ZADD flags: NX, XX, GT, LT, CH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_nx_flag() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Add alice with score 1.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // NX: existing member should NOT be updated, new member should be added.
    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("NX")
        .arg(99.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 1); // Only bob added.

    // alice's score should be unchanged.
    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 1.0).abs() < f64::EPSILON);

    // bob should exist with score 2.
    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 2.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zadd_xx_flag() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Add alice with score 1.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // XX: new member should NOT be added, existing member should be updated.
    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("XX")
        .arg(99.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 0); // No new members added.

    // alice's score should be updated.
    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 99.0).abs() < f64::EPSILON);

    // bob should NOT exist.
    let score: Option<f64> = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(score, None);

    // Cardinality should still be 1.
    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);
}

#[tokio::test]
async fn zadd_gt_flag() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(10.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // GT: only update if new score > old score. 5 < 10, should not update.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("GT")
        .arg(5.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 10.0).abs() < f64::EPSILON); // Unchanged.

    // GT: 20 > 10, should update.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("GT")
        .arg(20.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 20.0).abs() < f64::EPSILON); // Updated.
}

#[tokio::test]
async fn zadd_ch_flag() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // CH: return added+updated count. Update alice (score changes), add bob.
    let changed: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("CH")
        .arg(99.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(changed, 2); // 1 added (bob) + 1 updated (alice) = 2
}

// ---------------------------------------------------------------------------
// ZSCORE edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zscore_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let score: Option<f64> = redis::cmd("ZSCORE")
        .arg("nosuchkey")
        .arg("member")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(score, None);
}

#[tokio::test]
async fn zscore_nonexistent_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let score: Option<f64> = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("nosuchmember")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(score, None);
}

// ---------------------------------------------------------------------------
// ZCARD edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zcard_nonexistent() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let card: i64 = redis::cmd("ZCARD")
        .arg("nosuchkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(card, 0);
}

// ---------------------------------------------------------------------------
// ZREM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrem_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .arg(3.0)
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("ZREM")
        .arg("myzset")
        .arg("alice")
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);

    // Removed members should return None.
    let score: Option<f64> = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(score, None);

    // Remaining member should still be there.
    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 3.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zrem_last_member_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("ZREM")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // Key should be deleted — TYPE returns "none".
    let key_type: String = redis::cmd("TYPE").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(key_type, "none");
}

// ---------------------------------------------------------------------------
// Negative scores
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_negative_scores() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(-10.0)
        .arg("a")
        .arg(0.0)
        .arg("b")
        .arg(5.0)
        .arg("c")
        .arg(-3.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 4);

    let score_a: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score_a - (-10.0)).abs() < f64::EPSILON);

    let score_b: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(score_b.abs() < f64::EPSILON);

    let score_c: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score_c - 5.0).abs() < f64::EPSILON);

    let score_d: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score_d - (-3.0)).abs() < f64::EPSILON);
}

// ---------------------------------------------------------------------------
// WRONGTYPE enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrongtype_zadd_on_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let err = redis::cmd("ZADD")
        .arg("mykey")
        .arg(1.0)
        .arg("member")
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
}

#[tokio::test]
async fn wrongtype_zcard_on_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let err = redis::cmd("ZCARD")
        .arg("mykey")
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
}

// ---------------------------------------------------------------------------
// Arity errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_arity_errors() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // No args at all.
    let err = redis::cmd("ZADD").query_async::<i64>(&mut con).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("wrong number of arguments") || msg.contains("ERR"),
        "expected arity error, got: {msg}"
    );

    // Key only, no score/member.
    let err = redis::cmd("ZADD")
        .arg("myzset")
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("wrong number of arguments") || msg.contains("ERR"),
        "expected arity error, got: {msg}"
    );

    // Odd score/member count (key + 1 score, no member).
    let err = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("wrong number of arguments") || msg.contains("ERR"),
        "expected arity error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// ZRANK / ZREVRANK
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrank_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .arg(3.0)
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();

    let rank: i64 = redis::cmd("ZRANK")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, 0);

    let rank: i64 = redis::cmd("ZRANK")
        .arg("myzset")
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, 1);

    let rank: i64 = redis::cmd("ZRANK")
        .arg("myzset")
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, 2);
}

#[tokio::test]
async fn zrevrank_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .arg(3.0)
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();

    let rank: i64 = redis::cmd("ZREVRANK")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, 2);

    let rank: i64 = redis::cmd("ZREVRANK")
        .arg("myzset")
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, 0);
}

#[tokio::test]
async fn zrank_nonexistent_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let rank: Option<i64> = redis::cmd("ZRANK")
        .arg("myzset")
        .arg("nosuchmember")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, None);
}

#[tokio::test]
async fn zrank_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let rank: Option<i64> = redis::cmd("ZRANK")
        .arg("nosuchkey")
        .arg("member")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rank, None);
}

// ---------------------------------------------------------------------------
// ZINCRBY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zincrby_new_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let score: f64 = redis::cmd("ZINCRBY")
        .arg("myzset")
        .arg(5.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 5.0).abs() < f64::EPSILON);

    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);
}

#[tokio::test]
async fn zincrby_existing_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(10.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let score: f64 = redis::cmd("ZINCRBY")
        .arg("myzset")
        .arg(5.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 15.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zincrby_negative_increment() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(10.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let score: f64 = redis::cmd("ZINCRBY")
        .arg("myzset")
        .arg(-3.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 7.0).abs() < f64::EPSILON);
}

// ---------------------------------------------------------------------------
// ZRANGE / ZREVRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrange_ascending_order() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(3.0)
        .arg("carol")
        .arg(1.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGE")
        .arg("myzset")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["alice", "bob", "carol"]);
}

#[tokio::test]
async fn zrange_with_scores() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<String> = redis::cmd("ZRANGE")
        .arg("myzset")
        .arg(0)
        .arg(-1)
        .arg("WITHSCORES")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, vec!["alice", "1", "bob", "2"]);
}

#[tokio::test]
async fn zrange_subset() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGE")
        .arg("myzset")
        .arg(1)
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["b", "c"]);
}

#[tokio::test]
async fn zrange_negative_indices() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGE")
        .arg("myzset")
        .arg(-2)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["c", "d"]);
}

#[tokio::test]
async fn zrange_empty() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let members: Vec<String> = redis::cmd("ZRANGE")
        .arg("nosuchkey")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(members.is_empty());
}

#[tokio::test]
async fn zrevrange_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .arg(2.0)
        .arg("bob")
        .arg(3.0)
        .arg("carol")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZREVRANGE")
        .arg("myzset")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["carol", "bob", "alice"]);
}

// ---------------------------------------------------------------------------
// ZCOUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zcount_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("ZCOUNT")
        .arg("myzset")
        .arg(2)
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn zcount_inf() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("ZCOUNT")
        .arg("myzset")
        .arg("-inf")
        .arg("+inf")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn zcount_exclusive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("ZCOUNT")
        .arg("myzset")
        .arg("(1")
        .arg("(3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 1); // Only score 2.
}

#[tokio::test]
async fn zcount_empty_range() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("ZCOUNT")
        .arg("myzset")
        .arg(10)
        .arg(20)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

// ---------------------------------------------------------------------------
// ZLEXCOUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zlexcount_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // All same score for meaningful lex ordering.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(0.0)
        .arg("a")
        .arg(0.0)
        .arg("b")
        .arg(0.0)
        .arg("c")
        .arg(0.0)
        .arg("d")
        .arg(0.0)
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("ZLEXCOUNT")
        .arg("myzset")
        .arg("[b")
        .arg("[d")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 3); // b, c, d
}

// ---------------------------------------------------------------------------
// ZRANGEBYSCORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrangebyscore_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg("myzset")
        .arg(2)
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["b", "c"]);
}

#[tokio::test]
async fn zrangebyscore_inf() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg("myzset")
        .arg("-inf")
        .arg("+inf")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn zrangebyscore_exclusive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg("myzset")
        .arg("(1")
        .arg("(3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["b"]); // Only score 2.
}

#[tokio::test]
async fn zrangebyscore_withscores() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg("myzset")
        .arg(1)
        .arg(2)
        .arg("WITHSCORES")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, vec!["a", "1", "b", "2"]);
}

#[tokio::test]
async fn zrangebyscore_limit() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg("myzset")
        .arg("-inf")
        .arg("+inf")
        .arg("LIMIT")
        .arg(1)
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["b", "c"]); // Skip 1, take 2.
}

// ---------------------------------------------------------------------------
// ZRANGEBYLEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrangebylex_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // All same score for meaningful lex ordering.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(0.0)
        .arg("a")
        .arg(0.0)
        .arg("b")
        .arg(0.0)
        .arg("c")
        .arg(0.0)
        .arg("d")
        .arg(0.0)
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYLEX")
        .arg("myzset")
        .arg("[b")
        .arg("[d")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["b", "c", "d"]);
}

#[tokio::test]
async fn zrangebylex_exclusive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(0.0)
        .arg("a")
        .arg(0.0)
        .arg("b")
        .arg(0.0)
        .arg("c")
        .arg(0.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYLEX")
        .arg("myzset")
        .arg("(a")
        .arg("(d")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["b", "c"]);
}

#[tokio::test]
async fn zrangebylex_all() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(0.0)
        .arg("a")
        .arg(0.0)
        .arg("b")
        .arg(0.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYLEX")
        .arg("myzset")
        .arg("-")
        .arg("+")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn zrangebylex_limit() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(0.0)
        .arg("a")
        .arg(0.0)
        .arg("b")
        .arg(0.0)
        .arg("c")
        .arg(0.0)
        .arg("d")
        .arg(0.0)
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("ZRANGEBYLEX")
        .arg("myzset")
        .arg("-")
        .arg("+")
        .arg("LIMIT")
        .arg(1)
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members, vec!["b", "c"]); // Skip 1, take 2.
}

// ---------------------------------------------------------------------------
// ZREMRANGEBYRANK
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zremrangebyrank_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    // Remove middle 2 (ranks 1 and 2: b, c).
    let removed: i64 = redis::cmd("ZREMRANGEBYRANK")
        .arg("myzset")
        .arg(1)
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let remaining: Vec<String> = redis::cmd("ZRANGE")
        .arg("myzset")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(remaining, vec!["a", "d"]);
}

#[tokio::test]
async fn zremrangebyrank_all() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("ZREMRANGEBYRANK")
        .arg("myzset")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    // Key should be deleted.
    let key_type: String = redis::cmd("TYPE").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(key_type, "none");
}

// ---------------------------------------------------------------------------
// ZREMRANGEBYSCORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zremrangebyscore_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("ZREMRANGEBYSCORE")
        .arg("myzset")
        .arg(2)
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let remaining: Vec<String> = redis::cmd("ZRANGE")
        .arg("myzset")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(remaining, vec!["a", "d"]);
}

#[tokio::test]
async fn zremrangebyscore_exclusive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("ZREMRANGEBYSCORE")
        .arg("myzset")
        .arg("(1")
        .arg("(3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 1); // Only score 2.
}

#[tokio::test]
async fn zremrangebyscore_inf() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("ZREMRANGEBYSCORE")
        .arg("myzset")
        .arg("-inf")
        .arg("+inf")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let key_type: String = redis::cmd("TYPE").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(key_type, "none");
}

// ---------------------------------------------------------------------------
// ZREMRANGEBYLEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zremrangebylex_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // All same score for meaningful lex ordering.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(0.0)
        .arg("a")
        .arg(0.0)
        .arg("b")
        .arg(0.0)
        .arg("c")
        .arg(0.0)
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("ZREMRANGEBYLEX")
        .arg("myzset")
        .arg("[b")
        .arg("[c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2); // b, c removed.

    let remaining: Vec<String> = redis::cmd("ZRANGE")
        .arg("myzset")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(remaining, vec!["a", "d"]);
}

// ---------------------------------------------------------------------------
// ZPOPMIN / ZPOPMAX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zpopmin_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<String> = redis::cmd("ZPOPMIN").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(result, vec!["a", "1"]);

    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);
}

#[tokio::test]
async fn zpopmin_with_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<String> = redis::cmd("ZPOPMIN")
        .arg("myzset")
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, vec!["a", "1", "b", "2"]);

    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);
}

#[tokio::test]
async fn zpopmax_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<String> = redis::cmd("ZPOPMAX").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(result, vec!["c", "3"]);
}

#[tokio::test]
async fn zpopmin_empty() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: Vec<String> = redis::cmd("ZPOPMIN")
        .arg("nosuchkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn zpopmin_last_member_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: Vec<String> = redis::cmd("ZPOPMIN").arg("myzset").query_async(&mut con).await.unwrap();

    let key_type: String = redis::cmd("TYPE").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(key_type, "none");
}

// ---------------------------------------------------------------------------
// ZRANDMEMBER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrandmember_returns_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let member: String = redis::cmd("ZRANDMEMBER")
        .arg("myzset")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        member == "a" || member == "b" || member == "c",
        "unexpected member: {member}"
    );
}

#[tokio::test]
async fn zrandmember_positive_count_distinct() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    // count > cardinality should return all distinct members.
    let members: Vec<String> = redis::cmd("ZRANDMEMBER")
        .arg("myzset")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members.len(), 2);
    assert!(members.contains(&"a".to_string()));
    assert!(members.contains(&"b".to_string()));
}

#[tokio::test]
async fn zrandmember_negative_count_duplicates() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // Negative count on single-member set returns all duplicates.
    let members: Vec<String> = redis::cmd("ZRANDMEMBER")
        .arg("myzset")
        .arg(-5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members.len(), 5);
    for m in &members {
        assert_eq!(m, "a");
    }
}

#[tokio::test]
async fn zrandmember_nonexistent() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let member: Option<String> = redis::cmd("ZRANDMEMBER")
        .arg("nosuchkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(member, None);
}

// ---------------------------------------------------------------------------
// ZMSCORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zmscore_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.5)
        .arg("a")
        .arg(2.5)
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let scores: Vec<Option<f64>> = redis::cmd("ZMSCORE")
        .arg("myzset")
        .arg("a")
        .arg("nonexistent")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(scores.len(), 3);
    assert!((scores[0].unwrap() - 1.5).abs() < f64::EPSILON);
    assert_eq!(scores[1], None);
    assert!((scores[2].unwrap() - 2.5).abs() < f64::EPSILON);
}

#[tokio::test]
async fn zmscore_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let scores: Vec<Option<f64>> = redis::cmd("ZMSCORE")
        .arg("nosuchkey")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(scores.len(), 2);
    assert_eq!(scores[0], None);
    assert_eq!(scores[1], None);
}

// ---------------------------------------------------------------------------
// ZADD LT flag
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_lt_flag() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(10.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // LT: only update if new score < old score. 20 > 10, should NOT update.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("LT")
        .arg(20.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 10.0).abs() < f64::EPSILON); // Unchanged.

    // LT: 5 < 10, should update.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("LT")
        .arg(5.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 5.0).abs() < f64::EPSILON); // Updated.

    // LT still allows new members to be added.
    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("LT")
        .arg(99.0)
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 1);
}

// ---------------------------------------------------------------------------
// ZADD flag mutual exclusion errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_flag_nx_xx_mutual_exclusion() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // NX + XX should be rejected.
    let err = redis::cmd("ZADD")
        .arg("myzset")
        .arg("NX")
        .arg("XX")
        .arg(1.0)
        .arg("alice")
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("XX and NX"),
        "expected NX/XX incompatibility error, got: {msg}"
    );
}

#[tokio::test]
async fn zadd_flag_nx_gt_mutual_exclusion() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // NX + GT should be rejected.
    let err = redis::cmd("ZADD")
        .arg("myzset")
        .arg("NX")
        .arg("GT")
        .arg(1.0)
        .arg("alice")
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("GT, LT, and NX"),
        "expected NX/GT incompatibility error, got: {msg}"
    );
}

#[tokio::test]
async fn zadd_flag_nx_lt_mutual_exclusion() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // NX + LT should be rejected.
    let err = redis::cmd("ZADD")
        .arg("myzset")
        .arg("NX")
        .arg("LT")
        .arg(1.0)
        .arg("alice")
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("GT, LT, and NX"),
        "expected NX/LT incompatibility error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// ZADD with duplicate members in a single call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zadd_duplicate_members_in_single_call() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Redis processes left-to-right: first pair adds alice at 1, second
    // pair updates alice to 5. Only 1 member added total.
    let added: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .arg(5.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 1);

    // Score should be the last value (5.0).
    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 5.0).abs() < f64::EPSILON);

    // Cardinality should be 1, not 2.
    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);
}

// ---------------------------------------------------------------------------
// ZINCRBY inf + -inf = NaN rejection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zincrby_nan_result_rejected() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Set score to +inf.
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg("inf")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // Incrementing by -inf should produce NaN and be rejected.
    let err = redis::cmd("ZINCRBY")
        .arg("myzset")
        .arg("-inf")
        .arg("alice")
        .query_async::<String>(&mut con)
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("NaN"), "expected NaN error, got: {msg}");

    // Score should be unchanged (still inf).
    let score: String = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(score, "inf");
}

// ---------------------------------------------------------------------------
// ZRANDMEMBER WITHSCORES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn zrandmember_positive_count_withscores() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    // Positive count + WITHSCORES: returns up to count distinct members with scores.
    let result: Vec<String> = redis::cmd("ZRANDMEMBER")
        .arg("myzset")
        .arg(10)
        .arg("WITHSCORES")
        .query_async(&mut con)
        .await
        .unwrap();
    // Should be 4 elements: 2 members * (member + score).
    assert_eq!(result.len(), 4);
    // Verify that scores are present (every odd index is a parseable number).
    assert!(result[1].parse::<f64>().is_ok());
    assert!(result[3].parse::<f64>().is_ok());
}

#[tokio::test]
async fn zrandmember_negative_count_withscores() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // Negative count + WITHSCORES: returns |count| entries, may repeat.
    let result: Vec<String> = redis::cmd("ZRANDMEMBER")
        .arg("myzset")
        .arg(-3)
        .arg("WITHSCORES")
        .query_async(&mut con)
        .await
        .unwrap();
    // 3 entries, each with score: 6 total elements.
    assert_eq!(result.len(), 6);
    for i in (0..6).step_by(2) {
        assert_eq!(result[i], "a");
        assert_eq!(result[i + 1], "1");
    }
}
