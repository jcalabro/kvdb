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

// ---------------------------------------------------------------------------
// MGET / MSET tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mset_and_mget_roundtrip() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MSET")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<Option<String>> = redis::cmd("MGET")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        result,
        vec![Some("1".to_string()), Some("2".to_string()), Some("3".to_string())]
    );
}

#[tokio::test]
async fn mget_returns_nil_for_missing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("existing")
        .arg("val")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: Vec<Option<String>> = redis::cmd("MGET")
        .arg("existing")
        .arg("missing")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, vec![Some("val".to_string()), None]);
}

#[tokio::test]
async fn mset_overwrites_existing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("old")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("MSET")
        .arg("k")
        .arg("new")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("GET").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(val, "new");
}

#[tokio::test]
async fn mset_odd_args_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<()> = redis::cmd("MSET")
        .arg("a")
        .arg("1")
        .arg("b")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "MSET with odd args should error");
}

// ---------------------------------------------------------------------------
// INCR / DECR tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn incr_creates_key_from_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let val: i64 = redis::cmd("INCR").arg("counter").query_async(&mut con).await.unwrap();
    assert_eq!(val, 1);
}

#[tokio::test]
async fn incr_increments_existing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("10")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: i64 = redis::cmd("INCR").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(val, 11);
}

#[tokio::test]
async fn incr_returns_error_on_non_integer() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("notnum")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: redis::RedisResult<i64> = redis::cmd("INCR").arg("k").query_async(&mut con).await;
    assert!(result.is_err(), "INCR on non-integer should error");
}

#[tokio::test]
async fn incr_returns_error_on_float_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("3.14")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: redis::RedisResult<i64> = redis::cmd("INCR").arg("k").query_async(&mut con).await;
    assert!(result.is_err(), "INCR on float string should error");
}

#[tokio::test]
async fn incr_overflow_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("9223372036854775807")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: redis::RedisResult<i64> = redis::cmd("INCR").arg("k").query_async(&mut con).await;
    assert!(result.is_err(), "INCR on i64::MAX should error");
}

#[tokio::test]
async fn decr_underflow_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("-9223372036854775808")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: redis::RedisResult<i64> = redis::cmd("DECR").arg("k").query_async(&mut con).await;
    assert!(result.is_err(), "DECR on i64::MIN should error");
}

#[tokio::test]
async fn decrby_with_large_delta() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET").arg("k").arg("0").query_async(&mut con).await.unwrap();

    let val: i64 = redis::cmd("DECRBY")
        .arg("k")
        .arg("1000000")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, -1_000_000);
}

#[tokio::test]
async fn incrby_preserves_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("0")
        .arg("EX")
        .arg("100")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: i64 = redis::cmd("INCRBY")
        .arg("k")
        .arg("5")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, 5);

    // Key should still exist (TTL preserved).
    let result: Option<String> = redis::cmd("GET").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(result, Some("5".to_string()));
}

// ---------------------------------------------------------------------------
// INCRBYFLOAT tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn incrbyfloat_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let val: String = redis::cmd("INCRBYFLOAT")
        .arg("k")
        .arg("3.14")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "3.14");
}

#[tokio::test]
async fn incrbyfloat_on_integer_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("10")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("INCRBYFLOAT")
        .arg("k")
        .arg("0.5")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "10.5");
}

#[tokio::test]
async fn incrbyfloat_integer_result() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: String = redis::cmd("INCRBYFLOAT")
        .arg("k")
        .arg("3.14")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("INCRBYFLOAT")
        .arg("k")
        .arg("0.86")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "4");
}

#[tokio::test]
async fn incrbyfloat_nan_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<String> = redis::cmd("INCRBYFLOAT")
        .arg("k")
        .arg("NaN")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "INCRBYFLOAT NaN should error");
}

#[tokio::test]
async fn incrbyfloat_inf_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<String> = redis::cmd("INCRBYFLOAT")
        .arg("k")
        .arg("inf")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "INCRBYFLOAT inf should error");
}

// ---------------------------------------------------------------------------
// APPEND / STRLEN tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn append_to_existing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("APPEND")
        .arg("k")
        .arg(" world")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 11);

    let val: String = redis::cmd("GET").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(val, "hello world");
}

#[tokio::test]
async fn append_creates_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let len: i64 = redis::cmd("APPEND")
        .arg("k")
        .arg("val")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 3);

    let val: String = redis::cmd("GET").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(val, "val");
}

#[tokio::test]
async fn strlen_returns_length() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("STRLEN").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(len, 5);
}

#[tokio::test]
async fn strlen_nonexistent_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let len: i64 = redis::cmd("STRLEN").arg("missing").query_async(&mut con).await.unwrap();
    assert_eq!(len, 0);
}

// ---------------------------------------------------------------------------
// GETRANGE / SETRANGE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn getrange_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("Hello World")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("GETRANGE")
        .arg("k")
        .arg("0")
        .arg("4")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "Hello");
}

#[tokio::test]
async fn getrange_with_negative_indices() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("Hello World")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("GETRANGE")
        .arg("k")
        .arg("-5")
        .arg("-1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "World");
}

#[tokio::test]
async fn getrange_start_greater_than_end() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("Hello World")
        .query_async(&mut con)
        .await
        .unwrap();

    let val: String = redis::cmd("GETRANGE")
        .arg("k")
        .arg("5")
        .arg("2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "");
}

#[tokio::test]
async fn getrange_nonexistent_returns_empty() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let val: String = redis::cmd("GETRANGE")
        .arg("missing")
        .arg("0")
        .arg("-1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "");
}

#[tokio::test]
async fn setrange_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("Hello World")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("SETRANGE")
        .arg("k")
        .arg("6")
        .arg("Redis")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 11);

    let val: String = redis::cmd("GET").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(val, "Hello Redis");
}

#[tokio::test]
async fn setrange_pads_with_zeros() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let len: i64 = redis::cmd("SETRANGE")
        .arg("k")
        .arg("5")
        .arg("!")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 6);

    // First 5 bytes should be zero-padding, 6th byte is '!'.
    let val: Vec<u8> = redis::cmd("GET").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(val, vec![0, 0, 0, 0, 0, b'!']);
}

#[tokio::test]
async fn setrange_negative_offset_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<i64> = redis::cmd("SETRANGE")
        .arg("k")
        .arg("-1")
        .arg("x")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "SETRANGE with negative offset should error");
}
