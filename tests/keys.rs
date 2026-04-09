// tests/keys.rs
//! Integration tests for key management commands (M5 Task 3).
//!
//! Tests the TTL read commands: TTL, PTTL, EXPIRETIME, PEXPIRETIME.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

// ---------------------------------------------------------------------------
// TTL tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ttl_nonexistent_returns_minus_two() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: i64 = redis::cmd("TTL")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, -2);
}

#[tokio::test]
async fn ttl_no_expiry_returns_minus_one() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET without TTL
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, -1);
}

#[tokio::test]
async fn ttl_with_expiry_returns_remaining() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET with EX 100 seconds
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .arg("EX")
        .arg(100)
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    // Should be between 0 and 100 (exclusive of 0, inclusive of 100 due to rounding)
    assert!(result > 0 && result <= 100, "TTL should be in (0, 100], got {}", result);
}

// ---------------------------------------------------------------------------
// PTTL tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pttl_with_expiry_returns_remaining_ms() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET with PX 50000 milliseconds
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .arg("PX")
        .arg(50000)
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("PTTL").arg("mykey").query_async(&mut con).await.unwrap();
    // Should be between 0 and 50000 (exclusive of 0, inclusive of 50000)
    assert!(
        result > 0 && result <= 50000,
        "PTTL should be in (0, 50000], got {}",
        result
    );
}

#[tokio::test]
async fn pttl_nonexistent_returns_minus_two() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: i64 = redis::cmd("PTTL")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, -2);
}

#[tokio::test]
async fn pttl_no_expiry_returns_minus_one() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET without TTL
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("PTTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, -1);
}

// ---------------------------------------------------------------------------
// EXPIRETIME tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expiretime_nonexistent_returns_minus_two() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: i64 = redis::cmd("EXPIRETIME")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, -2);
}

#[tokio::test]
async fn expiretime_no_expiry_returns_minus_one() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET without TTL
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("EXPIRETIME")
        .arg("mykey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, -1);
}

#[tokio::test]
async fn expiretime_returns_absolute_seconds() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Get current time in seconds
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // SET with EX 60 seconds
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .arg("EX")
        .arg(60)
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("EXPIRETIME")
        .arg("mykey")
        .query_async(&mut con)
        .await
        .unwrap();

    // Result should be approximately now + 60 seconds (within 5s tolerance)
    let expected = now_secs + 60;
    let diff = (result - expected).abs();
    assert!(
        diff <= 5,
        "EXPIRETIME should be ~{}, got {}, diff={}",
        expected,
        result,
        diff
    );
}

// ---------------------------------------------------------------------------
// PEXPIRETIME tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pexpiretime_returns_absolute_ms() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Get current time in milliseconds
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // SET with PX 60000 milliseconds
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .arg("PX")
        .arg(60000)
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("PEXPIRETIME")
        .arg("mykey")
        .query_async(&mut con)
        .await
        .unwrap();

    // Result should be approximately now + 60000 milliseconds (within 5s tolerance)
    let expected = now_ms + 60000;
    let diff = (result - expected).abs();
    assert!(
        diff <= 5000,
        "PEXPIRETIME should be ~{}, got {}, diff={}ms",
        expected,
        result,
        diff
    );
}

#[tokio::test]
async fn pexpiretime_nonexistent_returns_minus_two() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: i64 = redis::cmd("PEXPIRETIME")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, -2);
}

#[tokio::test]
async fn pexpiretime_no_expiry_returns_minus_one() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET without TTL
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: i64 = redis::cmd("PEXPIRETIME")
        .arg("mykey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, -1);
}

// ---------------------------------------------------------------------------
// Wrong arity tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ttl_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // 0 args
    let result: redis::RedisResult<i64> = redis::cmd("TTL").query_async(&mut con).await;
    assert!(result.is_err(), "TTL with 0 args should error");

    // 2 args
    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("a").arg("b").query_async(&mut con).await;
    assert!(result.is_err(), "TTL with 2 args should error");
}

#[tokio::test]
async fn pttl_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // 0 args
    let result: redis::RedisResult<i64> = redis::cmd("PTTL").query_async(&mut con).await;
    assert!(result.is_err(), "PTTL with 0 args should error");

    // 2 args
    let result: redis::RedisResult<i64> = redis::cmd("PTTL").arg("a").arg("b").query_async(&mut con).await;
    assert!(result.is_err(), "PTTL with 2 args should error");
}

#[tokio::test]
async fn expiretime_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // 0 args
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRETIME").query_async(&mut con).await;
    assert!(result.is_err(), "EXPIRETIME with 0 args should error");

    // 2 args
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRETIME").arg("a").arg("b").query_async(&mut con).await;
    assert!(result.is_err(), "EXPIRETIME with 2 args should error");
}

#[tokio::test]
async fn pexpiretime_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // 0 args
    let result: redis::RedisResult<i64> = redis::cmd("PEXPIRETIME").query_async(&mut con).await;
    assert!(result.is_err(), "PEXPIRETIME with 0 args should error");

    // 2 args
    let result: redis::RedisResult<i64> = redis::cmd("PEXPIRETIME").arg("a").arg("b").query_async(&mut con).await;
    assert!(result.is_err(), "PEXPIRETIME with 2 args should error");
}

// ---------------------------------------------------------------------------
// EXPIRE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expire_sets_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET key without TTL
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    // EXPIRE with 10 seconds
    let result: i64 = redis::cmd("EXPIRE")
        .arg("mykey")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    // Verify TTL is set
    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(ttl > 0 && ttl <= 10, "TTL should be in (0, 10], got {}", ttl);
}

#[tokio::test]
async fn expire_nonexistent_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: i64 = redis::cmd("EXPIRE")
        .arg("nonexistent")
        .arg(10)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 0);
}

// ---------------------------------------------------------------------------
// PEXPIRE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pexpire_sets_ttl_ms() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET key without TTL
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    // PEXPIRE with 50000 milliseconds
    let result: i64 = redis::cmd("PEXPIRE")
        .arg("mykey")
        .arg(50000)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    // Verify PTTL is set
    let pttl: i64 = redis::cmd("PTTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(pttl > 0 && pttl <= 50000, "PTTL should be in (0, 50000], got {}", pttl);
}

// ---------------------------------------------------------------------------
// EXPIREAT tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expireat_sets_absolute_expiry() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET key without TTL
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    // Get future timestamp (now + 60 seconds)
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let future_ts = now_secs + 60;

    // EXPIREAT with future timestamp
    let result: i64 = redis::cmd("EXPIREAT")
        .arg("mykey")
        .arg(future_ts)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    // Verify EXPIRETIME matches
    let expiretime: i64 = redis::cmd("EXPIRETIME")
        .arg("mykey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(expiretime, future_ts);
}

// ---------------------------------------------------------------------------
// PERSIST tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persist_removes_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET with EX
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .arg("EX")
        .arg(100)
        .query_async(&mut con)
        .await
        .unwrap();

    // Verify TTL is set
    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(ttl > 0, "TTL should be positive before PERSIST");

    // PERSIST should return 1
    let result: i64 = redis::cmd("PERSIST").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);

    // Verify TTL is now -1 (no expiry)
    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(ttl, -1);
}

#[tokio::test]
async fn persist_no_ttl_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET without EX
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    // PERSIST on key without TTL should return 0
    let result: i64 = redis::cmd("PERSIST").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn persist_nonexistent_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: i64 = redis::cmd("PERSIST")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 0);
}

// ---------------------------------------------------------------------------
// TYPE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn type_string_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET a string key
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    // TYPE should return "string"
    let result: String = redis::cmd("TYPE").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, "string");
}

#[tokio::test]
async fn type_nonexistent_returns_none() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // TYPE on nonexistent key should return "none"
    let result: String = redis::cmd("TYPE")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "none");
}

// ---------------------------------------------------------------------------
// UNLINK tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unlink_deletes_keys() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET two keys
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();

    // UNLINK a, b, c should return 2 (a and b exist, c doesn't)
    let result: i64 = redis::cmd("UNLINK")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 2);

    // Verify keys are deleted
    let exists_a: i64 = redis::cmd("EXISTS").arg("a").query_async(&mut con).await.unwrap();
    let exists_b: i64 = redis::cmd("EXISTS").arg("b").query_async(&mut con).await.unwrap();
    assert_eq!(exists_a, 0);
    assert_eq!(exists_b, 0);
}

// ---------------------------------------------------------------------------
// TOUCH tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn touch_returns_existing_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET two keys
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();

    // TOUCH a, b, c should return 2 (a and b exist, c doesn't)
    let result: i64 = redis::cmd("TOUCH")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 2);
}

// ---------------------------------------------------------------------------
// DBSIZE tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dbsize_empty_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // DBSIZE on empty DB should return 0
    // Use cmd with no args - redis crate requires explicit query for zero-arg commands
    let result: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn dbsize_reflects_key_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET three keys
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("c").arg("3").query_async(&mut con).await.unwrap();

    // DBSIZE should return 3
    let result: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(result, 3);

    // DEL one key
    let _: i64 = redis::cmd("DEL").arg("b").query_async(&mut con).await.unwrap();

    // DBSIZE should now return 2
    let result: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(result, 2);
}
