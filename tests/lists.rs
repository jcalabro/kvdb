// tests/lists.rs
//! Integration tests for the 14 M9 list commands.
//!
//! Tests the full path: TCP connect -> RESP -> dispatch -> FDB -> response.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

// ---------------------------------------------------------------------------
// LPUSH / RPUSH basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lpush_rpush_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let len: i64 = redis::cmd("LPUSH")
        .arg("mylist")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 1);

    let len: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 2);

    let llen: i64 = redis::cmd("LLEN").arg("mylist").query_async(&mut con).await.unwrap();
    assert_eq!(llen, 2);
}

#[tokio::test]
async fn lpush_ordering_reverses_input() {
    // LPUSH key a b c → list should be [c, b, a].
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("LPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["c", "b", "a"]);
}

#[tokio::test]
async fn rpush_ordering_preserves_input() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn interleaved_lpush_rpush() {
    // Demonstrate that head grows negative and tail grows positive
    // without overlapping.
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("LPUSH")
        .arg("l")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("LPUSH")
        .arg("l")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["a", "b", "c", "d"]);
}

// ---------------------------------------------------------------------------
// LPOP / RPOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lpop_rpop_single() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let head: String = redis::cmd("LPOP").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(head, "a");
    let tail: String = redis::cmd("RPOP").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(tail, "c");
    let mid: String = redis::cmd("LPOP").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(mid, "b");

    // List is now empty — the key should be gone (Redis semantics).
    let ttl: i64 = redis::cmd("EXISTS").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(ttl, 0);
}

#[tokio::test]
async fn lpop_rpop_with_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LPOP").arg("l").arg(2).query_async(&mut con).await.unwrap();
    assert_eq!(got, vec!["a", "b"]);

    let got: Vec<String> = redis::cmd("RPOP").arg("l").arg(2).query_async(&mut con).await.unwrap();
    assert_eq!(got, vec!["e", "d"]);

    let llen: i64 = redis::cmd("LLEN").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(llen, 1);
}

#[tokio::test]
async fn lpop_rpop_count_exceeds_length() {
    // LPOP with count > length should return all elements.
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    let got: Vec<String> = redis::cmd("LPOP")
        .arg("l")
        .arg(100)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["a", "b"]);

    // Key should no longer exist.
    let exists: i64 = redis::cmd("EXISTS").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn lpop_missing_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Without count: returns Nil.
    let res: Option<String> = redis::cmd("LPOP").arg("nope").query_async(&mut con).await.unwrap();
    assert_eq!(res, None);

    // With count: returns Nil array (redis crate represents as None).
    let res: Option<Vec<String>> = redis::cmd("LPOP")
        .arg("nope")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(res, None);
}

// ---------------------------------------------------------------------------
// LLEN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn llen_nonexistent_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let llen: i64 = redis::cmd("LLEN").arg("nope").query_async(&mut con).await.unwrap();
    assert_eq!(llen, 0);
}

#[tokio::test]
async fn llen_wrongtype_on_string_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("s")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();
    let result: redis::RedisResult<i64> = redis::cmd("LLEN").arg("s").query_async(&mut con).await;
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("WRONGTYPE"), "expected WRONGTYPE, got: {err}");
}

// ---------------------------------------------------------------------------
// LINDEX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lindex_positive_and_negative() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async(&mut con)
        .await
        .unwrap();

    let v: String = redis::cmd("LINDEX")
        .arg("l")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, "a");
    let v: String = redis::cmd("LINDEX")
        .arg("l")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, "d");
    let v: String = redis::cmd("LINDEX")
        .arg("l")
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, "d");
    let v: String = redis::cmd("LINDEX")
        .arg("l")
        .arg(-4)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, "a");
}

#[tokio::test]
async fn lindex_out_of_bounds_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    let v: Option<String> = redis::cmd("LINDEX")
        .arg("l")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, None);
    let v: Option<String> = redis::cmd("LINDEX")
        .arg("l")
        .arg(-5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, None);
}

#[tokio::test]
async fn lindex_nonexistent_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let v: Option<String> = redis::cmd("LINDEX")
        .arg("nope")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, None);
}

// ---------------------------------------------------------------------------
// LRANGE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lrange_full_list() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn lrange_clamps_bounds() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    // Out-of-range bounds should clamp, not error.
    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(-100)
        .arg(100)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["a", "b", "c"]);

    // start > stop returns empty.
    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(5)
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(got.is_empty());
}

#[tokio::test]
async fn lrange_subset() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(1)
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["b", "c", "d"]);

    // Negative range.
    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(-2)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["d", "e"]);
}

// ---------------------------------------------------------------------------
// LSET
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lset_updates_element() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("LSET")
        .arg("l")
        .arg(1)
        .arg("X")
        .query_async(&mut con)
        .await
        .unwrap();

    let v: String = redis::cmd("LINDEX")
        .arg("l")
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, "X");

    // Negative index.
    let _: () = redis::cmd("LSET")
        .arg("l")
        .arg(-1)
        .arg("Z")
        .query_async(&mut con)
        .await
        .unwrap();
    let v: String = redis::cmd("LINDEX")
        .arg("l")
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v, "Z");
}

#[tokio::test]
async fn lset_out_of_range() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    let result: redis::RedisResult<String> = redis::cmd("LSET").arg("l").arg(5).arg("X").query_async(&mut con).await;
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("out of range"), "got: {err}");
}

#[tokio::test]
async fn lset_no_such_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<String> = redis::cmd("LSET")
        .arg("nope")
        .arg(0)
        .arg("X")
        .query_async(&mut con)
        .await;
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("no such key"), "got: {err}");
}

// ---------------------------------------------------------------------------
// LTRIM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ltrim_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("LTRIM")
        .arg("l")
        .arg(1)
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["b", "c", "d"]);

    let llen: i64 = redis::cmd("LLEN").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(llen, 3);
}

#[tokio::test]
async fn ltrim_empty_range_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    // start > stop trims to nothing.
    let _: () = redis::cmd("LTRIM")
        .arg("l")
        .arg(5)
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();

    let exists: i64 = redis::cmd("EXISTS").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn ltrim_single_element_keeps_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("LTRIM")
        .arg("l")
        .arg(1)
        .arg(1)
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["b"]);
}

// ---------------------------------------------------------------------------
// LPUSHX / RPUSHX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lpushx_rpushx_existing_list() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("LPUSHX")
        .arg("l")
        .arg("x")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 2);
    let len: i64 = redis::cmd("RPUSHX")
        .arg("l")
        .arg("y")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 3);

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["x", "a", "y"]);
}

#[tokio::test]
async fn lpushx_rpushx_nonexistent_noop() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let len: i64 = redis::cmd("LPUSHX")
        .arg("nope")
        .arg("x")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 0);
    let exists: i64 = redis::cmd("EXISTS").arg("nope").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);

    let len: i64 = redis::cmd("RPUSHX")
        .arg("nope2")
        .arg("y")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 0);
    let exists: i64 = redis::cmd("EXISTS").arg("nope2").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

// ---------------------------------------------------------------------------
// LREM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lrem_positive_count_head_to_tail() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("b")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("LREM")
        .arg("l")
        .arg(2)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["b", "b", "a"]);
}

#[tokio::test]
async fn lrem_negative_count_tail_to_head() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("b")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("LREM")
        .arg("l")
        .arg(-2)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    // Removed last two 'a's, first one stays.
    assert_eq!(got, vec!["a", "b", "b"]);
}

#[tokio::test]
async fn lrem_zero_count_removes_all() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("LREM")
        .arg("l")
        .arg(0)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 3);

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["b"]);
}

#[tokio::test]
async fn lrem_removes_all_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("LREM")
        .arg("l")
        .arg(0)
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    let exists: i64 = redis::cmd("EXISTS").arg("l").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn lrem_no_match_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    let removed: i64 = redis::cmd("LREM")
        .arg("l")
        .arg(0)
        .arg("z")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 0);
}

// ---------------------------------------------------------------------------
// LINSERT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn linsert_before_and_after() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("LINSERT")
        .arg("l")
        .arg("BEFORE")
        .arg("b")
        .arg("X")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 4);
    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["a", "X", "b", "c"]);

    let len: i64 = redis::cmd("LINSERT")
        .arg("l")
        .arg("AFTER")
        .arg("b")
        .arg("Y")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 5);
    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("l")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["a", "X", "b", "Y", "c"]);
}

#[tokio::test]
async fn linsert_missing_pivot_returns_negative_one() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    let res: i64 = redis::cmd("LINSERT")
        .arg("l")
        .arg("BEFORE")
        .arg("nope")
        .arg("X")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(res, -1);
}

#[tokio::test]
async fn linsert_missing_key_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let res: i64 = redis::cmd("LINSERT")
        .arg("nope")
        .arg("BEFORE")
        .arg("p")
        .arg("x")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(res, 0);
}

// ---------------------------------------------------------------------------
// LPOS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lpos_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("b")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let pos: i64 = redis::cmd("LPOS")
        .arg("l")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pos, 1);

    // Missing element: Nil.
    let res: Option<i64> = redis::cmd("LPOS")
        .arg("l")
        .arg("z")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(res, None);
}

#[tokio::test]
async fn lpos_with_rank() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("b")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    // Second match head-to-tail.
    let pos: i64 = redis::cmd("LPOS")
        .arg("l")
        .arg("b")
        .arg("RANK")
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pos, 3);

    // First match tail-to-head — 'a' at index 4.
    let pos: i64 = redis::cmd("LPOS")
        .arg("l")
        .arg("a")
        .arg("RANK")
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pos, 4);
}

#[tokio::test]
async fn lpos_with_count_returns_array() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("c")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();

    let got: Vec<i64> = redis::cmd("LPOS")
        .arg("l")
        .arg("a")
        .arg("COUNT")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec![0, 2, 4]);

    let got: Vec<i64> = redis::cmd("LPOS")
        .arg("l")
        .arg("a")
        .arg("COUNT")
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec![0, 2]);
}

#[tokio::test]
async fn lpos_rank_zero_rejected() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("l")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    let result: redis::RedisResult<Option<i64>> = redis::cmd("LPOS")
        .arg("l")
        .arg("a")
        .arg("RANK")
        .arg(0)
        .query_async(&mut con)
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// LMOVE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lmove_left_left() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("src")
        .arg("dst")
        .arg("LEFT")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "a");

    let src: Vec<String> = redis::cmd("LRANGE")
        .arg("src")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(src, vec!["b", "c"]);

    let dst: Vec<String> = redis::cmd("LRANGE")
        .arg("dst")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(dst, vec!["a"]);
}

#[tokio::test]
async fn lmove_right_left() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("src")
        .arg("dst")
        .arg("RIGHT")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "c");

    let src: Vec<String> = redis::cmd("LRANGE")
        .arg("src")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(src, vec!["a", "b"]);

    let dst: Vec<String> = redis::cmd("LRANGE")
        .arg("dst")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(dst, vec!["c"]);
}

#[tokio::test]
async fn lmove_right_right() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("src")
        .arg("dst")
        .arg("RIGHT")
        .arg("RIGHT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "c");

    let src: Vec<String> = redis::cmd("LRANGE")
        .arg("src")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(src, vec!["a", "b"]);

    let dst: Vec<String> = redis::cmd("LRANGE")
        .arg("dst")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(dst, vec!["c"]);
}

#[tokio::test]
async fn lmove_left_right() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("src")
        .arg("dst")
        .arg("LEFT")
        .arg("RIGHT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "a");

    let src: Vec<String> = redis::cmd("LRANGE")
        .arg("src")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(src, vec!["b", "c"]);

    let dst: Vec<String> = redis::cmd("LRANGE")
        .arg("dst")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(dst, vec!["a"]);
}

#[tokio::test]
async fn lmove_same_key_rotation() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("mylist")
        .arg("mylist")
        .arg("LEFT")
        .arg("RIGHT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "a");

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["b", "c", "a"]);
}

#[tokio::test]
async fn lmove_same_key_rotation_right_to_left() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("mylist")
        .arg("mylist")
        .arg("RIGHT")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "c");

    let got: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got, vec!["c", "a", "b"]);
}

#[tokio::test]
async fn lmove_nonexistent_source_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: Option<String> = redis::cmd("LMOVE")
        .arg("nosuch")
        .arg("dst")
        .arg("LEFT")
        .arg("RIGHT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, None);

    // dst should not have been created.
    let exists: i64 = redis::cmd("EXISTS").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn lmove_empties_source_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("only")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("src")
        .arg("dst")
        .arg("LEFT")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "only");

    let exists: i64 = redis::cmd("EXISTS").arg("src").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);

    let dst: Vec<String> = redis::cmd("LRANGE")
        .arg("dst")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(dst, vec!["only"]);
}

#[tokio::test]
async fn lmove_pushes_to_existing_destination() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("src")
        .arg("x")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("dst")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let moved: String = redis::cmd("LMOVE")
        .arg("src")
        .arg("dst")
        .arg("LEFT")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(moved, "x");

    let dst: Vec<String> = redis::cmd("LRANGE")
        .arg("dst")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(dst, vec!["x", "a", "b"]);
}

// ---------------------------------------------------------------------------
// LMPOP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lmpop_single_key_left() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<redis::Value> = redis::cmd("LMPOP")
        .arg(1)
        .arg("mylist")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    match &result[0] {
        redis::Value::BulkString(b) => assert_eq!(b, b"mylist"),
        other => panic!("expected bulk string, got: {other:?}"),
    }
    match &result[1] {
        redis::Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                redis::Value::BulkString(b) => assert_eq!(b, b"a"),
                other => panic!("expected bulk string, got: {other:?}"),
            }
        }
        other => panic!("expected array, got: {other:?}"),
    }

    // Remaining list should be [b, c]
    let remaining: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(remaining, vec!["b", "c"]);
}

#[tokio::test]
async fn lmpop_single_key_right_with_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<redis::Value> = redis::cmd("LMPOP")
        .arg(1)
        .arg("mylist")
        .arg("RIGHT")
        .arg("COUNT")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    match &result[0] {
        redis::Value::BulkString(b) => assert_eq!(b, b"mylist"),
        other => panic!("expected bulk string, got: {other:?}"),
    }
    match &result[1] {
        redis::Value::Array(arr) => {
            assert_eq!(arr.len(), 3);
            let popped: Vec<&[u8]> = arr
                .iter()
                .map(|v| match v {
                    redis::Value::BulkString(b) => b.as_slice(),
                    other => panic!("expected bulk string, got: {other:?}"),
                })
                .collect();
            assert_eq!(popped, vec![b"e", b"d", b"c"]);
        }
        other => panic!("expected array, got: {other:?}"),
    }

    // Remaining list should be [a, b]
    let remaining: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(remaining, vec!["a", "b"]);
}

#[tokio::test]
async fn lmpop_picks_first_nonempty() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // key1 does not exist. key2 has [x, y]. key3 has [z].
    let _: i64 = redis::cmd("RPUSH")
        .arg("key2")
        .arg("x")
        .arg("y")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("RPUSH")
        .arg("key3")
        .arg("z")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<redis::Value> = redis::cmd("LMPOP")
        .arg(3)
        .arg("key1")
        .arg("key2")
        .arg("key3")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    match &result[0] {
        redis::Value::BulkString(b) => assert_eq!(b, b"key2"),
        other => panic!("expected bulk string for key name, got: {other:?}"),
    }
    match &result[1] {
        redis::Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                redis::Value::BulkString(b) => assert_eq!(b, b"x"),
                other => panic!("expected bulk string, got: {other:?}"),
            }
        }
        other => panic!("expected array, got: {other:?}"),
    }

    // key3 should be untouched
    let key3: Vec<String> = redis::cmd("LRANGE")
        .arg("key3")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(key3, vec!["z"]);
}

#[tokio::test]
async fn lmpop_all_empty_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::Value = redis::cmd("LMPOP")
        .arg(2)
        .arg("empty1")
        .arg("empty2")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, redis::Value::Nil);
}

#[tokio::test]
async fn lmpop_count_exceeds_length() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("short")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<redis::Value> = redis::cmd("LMPOP")
        .arg(1)
        .arg("short")
        .arg("LEFT")
        .arg("COUNT")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    match &result[1] {
        redis::Value::Array(arr) => {
            assert_eq!(arr.len(), 2);
            let popped: Vec<&[u8]> = arr
                .iter()
                .map(|v| match v {
                    redis::Value::BulkString(b) => b.as_slice(),
                    other => panic!("expected bulk string, got: {other:?}"),
                })
                .collect();
            assert_eq!(popped, vec![b"a", b"b"]);
        }
        other => panic!("expected array, got: {other:?}"),
    }

    // List should be auto-deleted
    let exists: i64 = redis::cmd("EXISTS").arg("short").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

#[tokio::test]
async fn lmpop_empties_list_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("only")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<redis::Value> = redis::cmd("LMPOP")
        .arg(1)
        .arg("mylist")
        .arg("LEFT")
        .query_async(&mut con)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    match &result[1] {
        redis::Value::Array(arr) => {
            assert_eq!(arr.len(), 1);
            match &arr[0] {
                redis::Value::BulkString(b) => assert_eq!(b, b"only"),
                other => panic!("expected bulk string, got: {other:?}"),
            }
        }
        other => panic!("expected array, got: {other:?}"),
    }

    let exists: i64 = redis::cmd("EXISTS").arg("mylist").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

// ---------------------------------------------------------------------------
// Cross-type WRONGTYPE enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrongtype_list_commands_on_string_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("s")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    for cmd_args in [
        vec!["LPUSH", "s", "x"],
        vec!["RPUSH", "s", "x"],
        vec!["LPOP", "s"],
        vec!["RPOP", "s"],
        vec!["LINDEX", "s", "0"],
        vec!["LRANGE", "s", "0", "-1"],
        vec!["LSET", "s", "0", "x"],
        vec!["LTRIM", "s", "0", "0"],
        vec!["LREM", "s", "0", "x"],
        vec!["LINSERT", "s", "BEFORE", "a", "x"],
        vec!["LPOS", "s", "x"],
        vec!["LMOVE", "s", "other", "LEFT", "RIGHT"],
        vec!["LMPOP", "1", "s", "LEFT"],
    ] {
        let mut cmd = redis::cmd(cmd_args[0]);
        for arg in &cmd_args[1..] {
            cmd.arg(*arg);
        }
        let result: redis::RedisResult<redis::Value> = cmd.query_async(&mut con).await;
        assert!(result.is_err(), "{} on string key should fail", cmd_args[0]);
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("WRONGTYPE"),
            "{}: expected WRONGTYPE, got {err}",
            cmd_args[0]
        );
    }
}

// ---------------------------------------------------------------------------
// Arity errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn arity_errors() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    for cmd_args in [
        vec!["LPUSH"],                       // missing key + elements
        vec!["LPUSH", "k"],                  // missing elements
        vec!["LPOP"],                        // missing key
        vec!["LPOP", "k", "a", "b"],         // too many args
        vec!["LLEN"],                        // missing key
        vec!["LINDEX", "k"],                 // missing index
        vec!["LRANGE", "k", "0"],            // missing stop
        vec!["LSET", "k", "0"],              // missing value
        vec!["LINSERT", "k", "BEFORE"],      // missing pivot + element
        vec!["LMOVE"],                       // missing all args
        vec!["LMOVE", "src"],                // missing 3 args
        vec!["LMOVE", "src", "dst", "LEFT"], // missing whereto
        vec!["LMPOP"],                       // missing all args
        vec!["LMPOP", "1"],                  // missing key + direction
        vec!["LMPOP", "1", "k"],             // missing direction
    ] {
        let mut cmd = redis::cmd(cmd_args[0]);
        for arg in &cmd_args[1..] {
            cmd.arg(*arg);
        }
        let result: redis::RedisResult<redis::Value> = cmd.query_async(&mut con).await;
        assert!(result.is_err(), "{:?} should error", cmd_args);
    }
}
