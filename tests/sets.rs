// tests/sets.rs
//! Integration tests for all 16 set commands.
//!
//! Tests the full path: TCP connect -> RESP -> dispatch -> FDB -> response.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use std::collections::HashSet;

use harness::TestContext;

// ---------------------------------------------------------------------------
// SADD / SISMEMBER / SREM round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sadd_and_sismember() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let added: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 3);

    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("myset")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(is_member, 1);

    let not_member: i64 = redis::cmd("SISMEMBER")
        .arg("myset")
        .arg("z")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(not_member, 0);
}

#[tokio::test]
async fn sadd_returns_only_new_members() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    // Add overlapping members: only "c" is new.
    let added: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 1);
}

#[tokio::test]
async fn sadd_duplicate_members_in_single_call() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Redis deduplicates within a single SADD call.
    let added: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("a")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 1);

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);
}

#[tokio::test]
async fn srem_removes_members() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("SREM")
        .arg("myset")
        .arg("a")
        .arg("z") // doesn't exist
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);
}

#[tokio::test]
async fn srem_all_members_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SREM")
        .arg("myset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // Key should no longer exist.
    let exists: i64 = redis::cmd("EXISTS").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn srem_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let removed: i64 = redis::cmd("SREM")
        .arg("nosuchkey")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 0);
}

// ---------------------------------------------------------------------------
// SMISMEMBER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn smismember_mixed() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let results: Vec<i64> = redis::cmd("SMISMEMBER")
        .arg("myset")
        .arg("a")
        .arg("z")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results, vec![1, 0, 1]);
}

#[tokio::test]
async fn smismember_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let results: Vec<i64> = redis::cmd("SMISMEMBER")
        .arg("nosuchkey")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results, vec![0, 0]);
}

// ---------------------------------------------------------------------------
// SCARD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scard_after_add_remove() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 0);

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 3);

    let _: i64 = redis::cmd("SREM")
        .arg("myset")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);
}

// ---------------------------------------------------------------------------
// SMEMBERS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn smembers_returns_all() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("c")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("SMEMBERS").arg("myset").query_async(&mut con).await.unwrap();
    let member_set: HashSet<String> = members.into_iter().collect();
    assert_eq!(
        member_set,
        HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()])
    );
}

#[tokio::test]
async fn smembers_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let members: Vec<String> = redis::cmd("SMEMBERS")
        .arg("nosuchkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(members.is_empty());
}

// ---------------------------------------------------------------------------
// SPOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spop_removes_and_returns() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let popped: String = redis::cmd("SPOP").arg("myset").query_async(&mut con).await.unwrap();

    // The popped member should be one of a, b, c.
    assert!(["a", "b", "c"].contains(&popped.as_str()));

    // Cardinality should be 2 now.
    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);

    // Popped member should no longer be in the set.
    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("myset")
        .arg(&popped)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(is_member, 0);
}

#[tokio::test]
async fn spop_with_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let popped: Vec<String> = redis::cmd("SPOP")
        .arg("myset")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(popped.len(), 3);

    // All returned members should be distinct.
    let popped_set: HashSet<&str> = popped.iter().map(|s| s.as_str()).collect();
    assert_eq!(popped_set.len(), 3);

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);
}

#[tokio::test]
async fn spop_count_larger_than_set() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let popped: Vec<String> = redis::cmd("SPOP")
        .arg("myset")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(popped.len(), 2);

    // Key should be deleted.
    let exists: i64 = redis::cmd("EXISTS").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn spop_empty_set() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: Option<String> = redis::cmd("SPOP").arg("nosuchkey").query_async(&mut con).await.unwrap();
    assert_eq!(result, None);
}

// ---------------------------------------------------------------------------
// SRANDMEMBER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn srandmember_no_removal() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let member: String = redis::cmd("SRANDMEMBER")
        .arg("myset")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(["a", "b", "c"].contains(&member.as_str()));

    // Set should still have 3 members.
    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 3);
}

#[tokio::test]
async fn srandmember_positive_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("SRANDMEMBER")
        .arg("myset")
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members.len(), 2);

    // All distinct.
    let member_set: HashSet<&str> = members.iter().map(|s| s.as_str()).collect();
    assert_eq!(member_set.len(), 2);
}

#[tokio::test]
async fn srandmember_negative_count_allows_duplicates() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("only")
        .query_async(&mut con)
        .await
        .unwrap();

    // Negative count with single-element set should return duplicates.
    let members: Vec<String> = redis::cmd("SRANDMEMBER")
        .arg("myset")
        .arg(-5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(members.len(), 5);

    for m in &members {
        assert_eq!(m, "only");
    }
}

#[tokio::test]
async fn srandmember_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: Option<String> = redis::cmd("SRANDMEMBER")
        .arg("nosuchkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, None);
}

// ---------------------------------------------------------------------------
// SMOVE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn smove_between_sets() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("src")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("dst")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: i64 = redis::cmd("SMOVE")
        .arg("src")
        .arg("dst")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, 1);

    // "a" should be in dst, not in src.
    let in_src: i64 = redis::cmd("SISMEMBER")
        .arg("src")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(in_src, 0);

    let in_dst: i64 = redis::cmd("SISMEMBER")
        .arg("dst")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(in_dst, 1);

    let src_card: i64 = redis::cmd("SCARD").arg("src").query_async(&mut con).await.unwrap();
    assert_eq!(src_card, 1);

    let dst_card: i64 = redis::cmd("SCARD").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(dst_card, 2);
}

#[tokio::test]
async fn smove_nonexistent_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("src")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: i64 = redis::cmd("SMOVE")
        .arg("src")
        .arg("dst")
        .arg("z")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, 0);
}

#[tokio::test]
async fn smove_creates_destination() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("src")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SMOVE")
        .arg("src")
        .arg("dst")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("SMEMBERS").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(members, vec!["a".to_string()]);
}

// ---------------------------------------------------------------------------
// SINTER
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sinter_overlapping() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let inter: Vec<String> = redis::cmd("SINTER")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    let inter_set: HashSet<String> = inter.into_iter().collect();
    assert_eq!(inter_set, HashSet::from(["b".to_string(), "c".to_string()]));
}

#[tokio::test]
async fn sinter_with_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    // Intersection with a non-existent key is always empty.
    let inter: Vec<String> = redis::cmd("SINTER")
        .arg("s1")
        .arg("nosuchkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(inter.is_empty());
}

// ---------------------------------------------------------------------------
// SUNION
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sunion_disjoint() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let union: Vec<String> = redis::cmd("SUNION")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    let union_set: HashSet<String> = union.into_iter().collect();
    assert_eq!(
        union_set,
        HashSet::from(["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()])
    );
}

// ---------------------------------------------------------------------------
// SDIFF
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sdiff_asymmetric() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    // s1 - s2 = {a}
    let diff: Vec<String> = redis::cmd("SDIFF")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(diff, vec!["a".to_string()]);

    // s2 - s1 = {d}
    let diff2: Vec<String> = redis::cmd("SDIFF")
        .arg("s2")
        .arg("s1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(diff2, vec!["d".to_string()]);
}

// ---------------------------------------------------------------------------
// SINTERSTORE / SUNIONSTORE / SDIFFSTORE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sinterstore_writes_result() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("SINTERSTORE")
        .arg("dest")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let members: Vec<String> = redis::cmd("SMEMBERS").arg("dest").query_async(&mut con).await.unwrap();
    let member_set: HashSet<String> = members.into_iter().collect();
    assert_eq!(member_set, HashSet::from(["b".to_string(), "c".to_string()]));
}

#[tokio::test]
async fn sunionstore_writes_result() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("SUNIONSTORE")
        .arg("dest")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn sdiffstore_writes_result() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("SDIFFSTORE")
        .arg("dest")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let members: Vec<String> = redis::cmd("SMEMBERS").arg("dest").query_async(&mut con).await.unwrap();
    let member_set: HashSet<String> = members.into_iter().collect();
    assert_eq!(member_set, HashSet::from(["a".to_string(), "c".to_string()]));
}

#[tokio::test]
async fn store_overwrites_existing_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Create a string key at "dest".
    let _: () = redis::cmd("SET")
        .arg("dest")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // SUNIONSTORE should overwrite the string key.
    let count: i64 = redis::cmd("SUNIONSTORE")
        .arg("dest")
        .arg("s1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // dest should now be a set.
    let key_type: String = redis::cmd("TYPE").arg("dest").query_async(&mut con).await.unwrap();
    assert_eq!(key_type, "set");
}

// ---------------------------------------------------------------------------
// SINTERCARD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sintercard_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("SINTERCARD")
        .arg(2)
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn sintercard_with_limit() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    // Full intersection has 3 members; LIMIT 1 should return 1.
    let count: i64 = redis::cmd("SINTERCARD")
        .arg(2)
        .arg("s1")
        .arg("s2")
        .arg("LIMIT")
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

// ---------------------------------------------------------------------------
// WRONGTYPE enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrongtype_sadd_on_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let err = redis::cmd("SADD")
        .arg("mykey")
        .arg("member")
        .query_async::<i64>(&mut con)
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
}

#[tokio::test]
async fn wrongtype_sinter_with_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("SET")
        .arg("mystr")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let err = redis::cmd("SINTER")
        .arg("myset")
        .arg("mystr")
        .query_async::<Vec<String>>(&mut con)
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
}

// ---------------------------------------------------------------------------
// TTL preservation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sadd_preserves_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("EXPIRE")
        .arg("myset")
        .arg(3600)
        .query_async(&mut con)
        .await
        .unwrap();

    // SADD should preserve TTL.
    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let ttl: i64 = redis::cmd("TTL").arg("myset").query_async(&mut con).await.unwrap();
    assert!(ttl > 3500, "TTL should be preserved, got: {ttl}");
}

// ---------------------------------------------------------------------------
// Operations on non-existent keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sismember_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: i64 = redis::cmd("SISMEMBER")
        .arg("nosuchkey")
        .arg("member")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn scard_nonexistent_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let card: i64 = redis::cmd("SCARD")
        .arg("nosuchkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(card, 0);
}

// ---------------------------------------------------------------------------
// SREM with duplicate members in a single call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn srem_duplicate_members_in_single_call() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    // SREM with "a" repeated: should only remove "a" once and return 1.
    let removed: i64 = redis::cmd("SREM")
        .arg("myset")
        .arg("a")
        .arg("a")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // Cardinality should be 2, not 0 (which would happen without dedup).
    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);

    // Verify the right members remain.
    let members: Vec<String> = redis::cmd("SMEMBERS").arg("myset").query_async(&mut con).await.unwrap();
    let member_set: HashSet<String> = members.into_iter().collect();
    assert_eq!(member_set, HashSet::from(["b".to_string(), "c".to_string()]));
}

// ---------------------------------------------------------------------------
// SMOVE source == destination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn smove_same_source_and_destination() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    // SMOVE to the same key should return 1 (member exists) but not change cardinality.
    let moved: i64 = redis::cmd("SMOVE")
        .arg("myset")
        .arg("myset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, 1);

    // Cardinality should still be 3.
    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 3);

    // "a" should still be a member.
    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("myset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(is_member, 1);
}

#[tokio::test]
async fn smove_same_key_nonexistent_member() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // Member "z" doesn't exist — should return 0.
    let moved: i64 = redis::cmd("SMOVE")
        .arg("myset")
        .arg("myset")
        .arg("z")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, 0);

    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 1);
}

// ---------------------------------------------------------------------------
// SPOP / SRANDMEMBER count=0
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spop_count_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let popped: Vec<String> = redis::cmd("SPOP")
        .arg("myset")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(popped.is_empty());

    // Set should be unchanged.
    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);
}

#[tokio::test]
async fn srandmember_count_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let members: Vec<String> = redis::cmd("SRANDMEMBER")
        .arg("myset")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(members.is_empty());

    // Set should be unchanged.
    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);
}

// ---------------------------------------------------------------------------
// Multi-set operations with 3+ sets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sinter_three_sets() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s3")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    // Intersection of all three: only "c".
    let inter: Vec<String> = redis::cmd("SINTER")
        .arg("s1")
        .arg("s2")
        .arg("s3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(inter, vec!["c".to_string()]);
}

#[tokio::test]
async fn sunion_three_sets() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s3")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let union: Vec<String> = redis::cmd("SUNION")
        .arg("s1")
        .arg("s2")
        .arg("s3")
        .query_async(&mut con)
        .await
        .unwrap();
    let union_set: HashSet<String> = union.into_iter().collect();
    assert_eq!(
        union_set,
        HashSet::from(["a".to_string(), "b".to_string(), "c".to_string()])
    );
}

#[tokio::test]
async fn sdiff_three_sets() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s3")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    // s1 - s2 - s3 = {a, d}
    let diff: Vec<String> = redis::cmd("SDIFF")
        .arg("s1")
        .arg("s2")
        .arg("s3")
        .query_async(&mut con)
        .await
        .unwrap();
    let diff_set: HashSet<String> = diff.into_iter().collect();
    assert_eq!(diff_set, HashSet::from(["a".to_string(), "d".to_string()]));
}

// ---------------------------------------------------------------------------
// Store operations where destination is one of the source keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sinterstore_dest_is_source() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    // Store intersection into s1 (overwriting it).
    let count: i64 = redis::cmd("SINTERSTORE")
        .arg("s1")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let members: Vec<String> = redis::cmd("SMEMBERS").arg("s1").query_async(&mut con).await.unwrap();
    let member_set: HashSet<String> = members.into_iter().collect();
    assert_eq!(member_set, HashSet::from(["b".to_string(), "c".to_string()]));
}

#[tokio::test]
async fn sdiffstore_empty_result_deletes_dest() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Pre-populate destination.
    let _: i64 = redis::cmd("SADD")
        .arg("dest")
        .arg("x")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s1")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: i64 = redis::cmd("SADD")
        .arg("s2")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // s1 - s2 = {} — destination should be deleted.
    let count: i64 = redis::cmd("SDIFFSTORE")
        .arg("dest")
        .arg("s1")
        .arg("s2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 0);

    let exists: i64 = redis::cmd("EXISTS").arg("dest").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}
