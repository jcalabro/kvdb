// tests/sorted_sets.rs
//! Integration tests for sorted set commands: ZADD, ZCARD, ZSCORE, ZREM.
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
