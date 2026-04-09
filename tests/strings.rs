// tests/strings.rs
//! Integration tests for string commands (M4 Tasks 4-6).
//!
//! Tests the full path: TCP connect -> RESP -> dispatch -> FDB -> response.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

// ---------------------------------------------------------------------------
// GET tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_nonexistent_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: Option<String> = redis::cmd("GET")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn get_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // 0 args
    let result: redis::RedisResult<String> = redis::cmd("GET").query_async(&mut con).await;
    assert!(result.is_err(), "GET with 0 args should error");

    // 2 args
    let result: redis::RedisResult<String> = redis::cmd("GET").arg("a").arg("b").query_async(&mut con).await;
    assert!(result.is_err(), "GET with 2 args should error");
}

// ---------------------------------------------------------------------------
// SET/GET tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_and_get() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("myvalue")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: String = redis::cmd("GET").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, "myvalue");
}

#[tokio::test]
async fn set_overwrites_existing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("key")
        .arg("first")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("SET")
        .arg("key")
        .arg("second")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: String = redis::cmd("GET").arg("key").query_async(&mut con).await.unwrap();
    assert_eq!(result, "second");
}

#[tokio::test]
async fn set_nx_respects_condition() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // NX on non-existent: succeeds
    let result: Option<String> = redis::cmd("SET")
        .arg("nxkey")
        .arg("value1")
        .arg("NX")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, Some("OK".to_string()));

    let val: String = redis::cmd("GET").arg("nxkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "value1");

    // NX on existing: returns nil, does not overwrite
    let result: Option<String> = redis::cmd("SET")
        .arg("nxkey")
        .arg("value2")
        .arg("NX")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, None);

    let val: String = redis::cmd("GET").arg("nxkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "value1"); // unchanged
}

#[tokio::test]
async fn set_xx_respects_condition() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // XX on non-existent: returns nil
    let result: Option<String> = redis::cmd("SET")
        .arg("xxkey")
        .arg("value1")
        .arg("XX")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, None);

    let val: Option<String> = redis::cmd("GET").arg("xxkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, None);

    // Create the key first
    let _: () = redis::cmd("SET")
        .arg("xxkey")
        .arg("value1")
        .query_async(&mut con)
        .await
        .unwrap();

    // XX on existing: succeeds
    let result: Option<String> = redis::cmd("SET")
        .arg("xxkey")
        .arg("value2")
        .arg("XX")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, Some("OK".to_string()));

    let val: String = redis::cmd("GET").arg("xxkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "value2");
}

#[tokio::test]
async fn set_get_returns_old_value() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // GET on non-existent key
    let result: Option<String> = redis::cmd("SET")
        .arg("gkey")
        .arg("val1")
        .arg("GET")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, None);

    // GET on existing key returns old value
    let result: Option<String> = redis::cmd("SET")
        .arg("gkey")
        .arg("val2")
        .arg("GET")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, Some("val1".to_string()));

    // Confirm new value was set
    let val: String = redis::cmd("GET").arg("gkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "val2");
}

#[tokio::test]
async fn set_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // 0 args
    let result: redis::RedisResult<String> = redis::cmd("SET").query_async(&mut con).await;
    assert!(result.is_err(), "SET with 0 args should error");

    // 1 arg
    let result: redis::RedisResult<String> = redis::cmd("SET").arg("key").query_async(&mut con).await;
    assert!(result.is_err(), "SET with 1 arg should error");
}

// ---------------------------------------------------------------------------
// SET flag error tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_nx_xx_mutually_exclusive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("NX")
        .arg("XX")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "SET NX XX should error");
    let err = result.unwrap_err();
    let detail = format!("{err}");
    assert!(
        detail.to_lowercase().contains("syntax"),
        "expected syntax error, got: {detail}"
    );
}

#[tokio::test]
async fn set_ex_zero_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("EX")
        .arg("0")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "SET EX 0 should error");
    let err = result.unwrap_err();
    let detail = format!("{err}");
    assert!(
        detail.to_lowercase().contains("invalid expire"),
        "expected expire error, got: {detail}"
    );
}

#[tokio::test]
async fn set_ex_negative_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("EX")
        .arg("-5")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "SET EX -5 should error");
    let err = result.unwrap_err();
    let detail = format!("{err}");
    assert!(
        detail.to_lowercase().contains("invalid expire"),
        "expected expire error, got: {detail}"
    );
}

#[tokio::test]
async fn set_ex_px_mutually_exclusive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("EX")
        .arg("10")
        .arg("PX")
        .arg("10000")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "SET EX PX should error");
    let err = result.unwrap_err();
    let detail = format!("{err}");
    assert!(
        detail.to_lowercase().contains("syntax"),
        "expected syntax error, got: {detail}"
    );
}

#[tokio::test]
async fn set_unknown_flag_returns_syntax_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("BOGUS")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "SET with unknown flag should error");
    let err = result.unwrap_err();
    let detail = format!("{err}");
    assert!(
        detail.to_lowercase().contains("syntax"),
        "expected syntax error, got: {detail}"
    );
}

// ---------------------------------------------------------------------------
// DEL/EXISTS tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn del_removes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("delkey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("DEL").arg("delkey").query_async(&mut con).await.unwrap();
    assert_eq!(count, 1);

    let val: Option<String> = redis::cmd("GET").arg("delkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn del_multiple_keys_returns_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("dk1")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("dk2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();

    // Delete dk1, dk2, and a nonexistent key
    let count: i64 = redis::cmd("DEL")
        .arg("dk1")
        .arg("dk2")
        .arg("dk_nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn del_nonexistent_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let count: i64 = redis::cmd("DEL")
        .arg("no_such_key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn exists_returns_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("ekey1")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("ekey2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();

    let count: i64 = redis::cmd("EXISTS")
        .arg("ekey1")
        .arg("ekey2")
        .arg("ekey_nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn exists_counts_duplicates() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("dupkey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    // EXISTS dupkey dupkey dupkey -> 3
    let count: i64 = redis::cmd("EXISTS")
        .arg("dupkey")
        .arg("dupkey")
        .arg("dupkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 3);
}

// ---------------------------------------------------------------------------
// Sugar command tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn setnx_returns_integer() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SETNX on non-existent: returns 1
    let result: i64 = redis::cmd("SETNX")
        .arg("snxkey")
        .arg("value1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    // SETNX on existing: returns 0
    let result: i64 = redis::cmd("SETNX")
        .arg("snxkey")
        .arg("value2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 0);

    // Value should still be the original
    let val: String = redis::cmd("GET").arg("snxkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "value1");
}

#[tokio::test]
async fn setex_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SETEX")
        .arg("sxkey")
        .arg("3600")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("GET").arg("sxkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "value");
}

#[tokio::test]
async fn psetex_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("PSETEX")
        .arg("psxkey")
        .arg("3600000")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("GET").arg("psxkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "value");
}

#[tokio::test]
async fn getdel_returns_and_deletes() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("gdkey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: String = redis::cmd("GETDEL").arg("gdkey").query_async(&mut con).await.unwrap();
    assert_eq!(result, "value");

    // Key should be gone
    let val: Option<String> = redis::cmd("GET").arg("gdkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn getdel_nonexistent_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: Option<String> = redis::cmd("GETDEL").arg("nokey").query_async(&mut con).await.unwrap();
    assert_eq!(result, None);
}

// ---------------------------------------------------------------------------
// Chunking test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn large_value_chunking() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // 500KB value — exceeds the 100KB FDB value limit, triggers chunking
    let large_value = "x".repeat(500_000);

    let _: () = redis::cmd("SET")
        .arg("bigkey")
        .arg(&large_value)
        .query_async(&mut con)
        .await
        .unwrap();

    let result: String = redis::cmd("GET").arg("bigkey").query_async(&mut con).await.unwrap();
    assert_eq!(result, large_value);
}
