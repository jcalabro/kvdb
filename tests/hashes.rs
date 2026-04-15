// tests/hashes.rs
//! Integration tests for all 15 hash commands.
//!
//! Tests the full path: TCP connect -> RESP -> dispatch -> FDB -> response.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use std::collections::HashMap;

use harness::TestContext;

// ---------------------------------------------------------------------------
// HSET / HGET tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hset_and_hget() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let added: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("field1")
        .arg("value1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 1);

    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("field1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "value1");
}

#[tokio::test]
async fn hset_multiple_fields() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let added: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .arg("f3")
        .arg("v3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 3);

    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("f2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "v2");
}

#[tokio::test]
async fn hset_update_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("field1")
        .arg("original")
        .query_async(&mut con)
        .await
        .unwrap();

    // Updating an existing field returns 0 (not 1).
    let added: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("field1")
        .arg("updated")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(added, 0);

    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("field1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "updated");
}

#[tokio::test]
async fn hget_missing_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let val: Option<String> = redis::cmd("HGET")
        .arg("nonexistent")
        .arg("field1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn hget_missing_field() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Create hash with one field.
    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("exists")
        .arg("yes")
        .query_async(&mut con)
        .await
        .unwrap();

    // Query a field that doesn't exist in the hash.
    let val: Option<String> = redis::cmd("HGET")
        .arg("myhash")
        .arg("nope")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn hset_wrongtype() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Create a string key.
    let _: () = redis::cmd("SET")
        .arg("mystring")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    // HSET on a string key should return WRONGTYPE.
    let result: redis::RedisResult<i64> = redis::cmd("HSET")
        .arg("mystring")
        .arg("field1")
        .arg("value1")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "HSET on a string key should return WRONGTYPE");
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("WRONGTYPE"), "expected WRONGTYPE error, got: {err}");
}

// ---------------------------------------------------------------------------
// HDEL tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hdel_removes_fields() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .arg("f3")
        .arg("v3")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("HDEL")
        .arg("myhash")
        .arg("f1")
        .arg("f2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    // f3 should still exist.
    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("f3")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "v3");

    // f1 should be gone.
    let val: Option<String> = redis::cmd("HGET")
        .arg("myhash")
        .arg("f1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn hdel_nonexistent_field() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // HDEL on a key that doesn't exist returns 0.
    let removed: i64 = redis::cmd("HDEL")
        .arg("nonexistent")
        .arg("field1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 0);
}

#[tokio::test]
async fn hdel_last_field_deletes_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("only")
        .arg("field")
        .query_async(&mut con)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("HDEL")
        .arg("myhash")
        .arg("only")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // Key itself should be gone now (EXISTS returns 0).
    let exists: i64 = redis::cmd("EXISTS").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(exists, 0);
}

// ---------------------------------------------------------------------------
// HEXISTS test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hexists_existing_and_missing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("present")
        .arg("yes")
        .query_async(&mut con)
        .await
        .unwrap();

    // Existing field returns 1.
    let exists: i64 = redis::cmd("HEXISTS")
        .arg("myhash")
        .arg("present")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(exists, 1);

    // Missing field returns 0.
    let exists: i64 = redis::cmd("HEXISTS")
        .arg("myhash")
        .arg("absent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(exists, 0);

    // Missing key returns 0.
    let exists: i64 = redis::cmd("HEXISTS")
        .arg("nokey")
        .arg("field")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(exists, 0);
}

// ---------------------------------------------------------------------------
// HLEN test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hlen_tracks_cardinality() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Non-existent key returns 0.
    let len: i64 = redis::cmd("HLEN").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(len, 0);

    // Add 2 fields.
    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("HLEN").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(len, 2);

    // Delete 1 field.
    let _: i64 = redis::cmd("HDEL")
        .arg("myhash")
        .arg("f1")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("HLEN").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(len, 1);
}

// ---------------------------------------------------------------------------
// HGETALL / HKEYS / HVALS tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hgetall_returns_pairs() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("name")
        .arg("alice")
        .arg("age")
        .arg("30")
        .arg("city")
        .arg("nyc")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: HashMap<String, String> = redis::cmd("HGETALL").arg("myhash").query_async(&mut con).await.unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result.get("name").unwrap(), "alice");
    assert_eq!(result.get("age").unwrap(), "30");
    assert_eq!(result.get("city").unwrap(), "nyc");
}

#[tokio::test]
async fn hgetall_empty_hash() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: HashMap<String, String> = redis::cmd("HGETALL")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn hkeys_and_hvals() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("alpha")
        .arg("100")
        .arg("beta")
        .arg("200")
        .query_async(&mut con)
        .await
        .unwrap();

    let mut keys: Vec<String> = redis::cmd("HKEYS").arg("myhash").query_async(&mut con).await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["alpha", "beta"]);

    let mut vals: Vec<String> = redis::cmd("HVALS").arg("myhash").query_async(&mut con).await.unwrap();
    vals.sort();
    assert_eq!(vals, vec!["100", "200"]);
}

// ---------------------------------------------------------------------------
// HMGET test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hmget_mixed_existing_and_missing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .arg("f3")
        .arg("v3")
        .query_async(&mut con)
        .await
        .unwrap();

    // Request f1 (exists), f2 (missing), f3 (exists).
    let result: Vec<Option<String>> = redis::cmd("HMGET")
        .arg("myhash")
        .arg("f1")
        .arg("f2")
        .arg("f3")
        .query_async(&mut con)
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Some("v1".to_string()));
    assert_eq!(result[1], None);
    assert_eq!(result[2], Some("v3".to_string()));
}

// ---------------------------------------------------------------------------
// HMSET test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hmset_returns_ok() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: String = redis::cmd("HMSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // Verify the values were actually set.
    let v1: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("f1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v1, "v1");

    let v2: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("f2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(v2, "v2");
}

// ---------------------------------------------------------------------------
// HINCRBY tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hincrby_creates_field() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // HINCRBY on nonexistent hash/field creates it with value = increment.
    let val: i64 = redis::cmd("HINCRBY")
        .arg("myhash")
        .arg("counter")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, 5);

    // Increment again.
    let val: i64 = redis::cmd("HINCRBY")
        .arg("myhash")
        .arg("counter")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, 8);
}

#[tokio::test]
async fn hincrby_not_integer_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Set a non-numeric value.
    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("name")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // HINCRBY on a non-integer field should error.
    let result: redis::RedisResult<i64> = redis::cmd("HINCRBY")
        .arg("myhash")
        .arg("name")
        .arg(1)
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "HINCRBY on non-integer value should error");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.to_lowercase().contains("not an integer"),
        "expected 'not an integer' error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// HINCRBYFLOAT test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hincrbyfloat_creates_and_increments() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // First increment creates the field: 0 + 1.5 = 1.5
    let val: String = redis::cmd("HINCRBYFLOAT")
        .arg("myhash")
        .arg("price")
        .arg("1.5")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "1.5");

    // Second increment: 1.5 + 2.5 = 4 (integer result, Redis formats as "4").
    let val: String = redis::cmd("HINCRBYFLOAT")
        .arg("myhash")
        .arg("price")
        .arg("2.5")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "4");
}

// ---------------------------------------------------------------------------
// HSETNX test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hsetnx_only_sets_if_new() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // First call sets the field, returns 1.
    let result: i64 = redis::cmd("HSETNX")
        .arg("myhash")
        .arg("field1")
        .arg("original")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 1);

    // Second call does not overwrite, returns 0.
    let result: i64 = redis::cmd("HSETNX")
        .arg("myhash")
        .arg("field1")
        .arg("updated")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 0);

    // Value should still be the original.
    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("field1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "original");
}

// ---------------------------------------------------------------------------
// HSTRLEN test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hstrlen_returns_length() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("greeting")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let len: i64 = redis::cmd("HSTRLEN")
        .arg("myhash")
        .arg("greeting")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 5);

    // Missing field returns 0.
    let len: i64 = redis::cmd("HSTRLEN")
        .arg("myhash")
        .arg("nope")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(len, 0);
}

// ---------------------------------------------------------------------------
// HRANDFIELD tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hrandfield_returns_field_from_hash() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query_async(&mut con)
        .await
        .unwrap();

    // Single mode (no count arg): returns one field name.
    let field: String = redis::cmd("HRANDFIELD")
        .arg("myhash")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        ["a", "b", "c"].contains(&field.as_str()),
        "expected one of a/b/c, got: {field}"
    );
}

#[tokio::test]
async fn hrandfield_count_positive_distinct() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query_async(&mut con)
        .await
        .unwrap();

    // count=2: returns 2 distinct fields.
    let fields: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("myhash")
        .arg(2)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(fields.len(), 2);

    // All results should be distinct.
    let mut seen = std::collections::HashSet::new();
    for f in &fields {
        assert!(["a", "b", "c"].contains(&f.as_str()), "unexpected field: {f}");
        assert!(seen.insert(f.clone()), "duplicate field in distinct mode: {f}");
    }
}

#[tokio::test]
async fn hrandfield_count_exceeds_size() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("x")
        .arg("1")
        .arg("y")
        .arg("2")
        .query_async(&mut con)
        .await
        .unwrap();

    // count=5 from a 2-field hash: returns only 2.
    let fields: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("myhash")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(fields.len(), 2);
}

#[tokio::test]
async fn hrandfield_negative_count_allows_duplicates() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("only")
        .arg("1")
        .query_async(&mut con)
        .await
        .unwrap();

    // count=-5 from a 1-field hash: returns 5 copies (all "only").
    let fields: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("myhash")
        .arg(-5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(fields.len(), 5);
    for f in &fields {
        assert_eq!(f, "only");
    }
}

// ---------------------------------------------------------------------------
// Cross-type / TTL tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hash_del_removes_hash() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();

    let deleted: i64 = redis::cmd("DEL").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(deleted, 1);

    let len: i64 = redis::cmd("HLEN").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn hash_type_returns_hash() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();

    let key_type: String = redis::cmd("TYPE").arg("myhash").query_async(&mut con).await.unwrap();
    assert_eq!(key_type, "hash");
}

#[tokio::test]
async fn hash_expire_and_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();

    let set: i64 = redis::cmd("EXPIRE")
        .arg("myhash")
        .arg(100)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(set, 1);

    let ttl: i64 = redis::cmd("TTL").arg("myhash").query_async(&mut con).await.unwrap();
    assert!(ttl > 0, "expected TTL > 0, got: {ttl}");
    assert!(ttl <= 100, "expected TTL <= 100, got: {ttl}");
}

#[tokio::test]
async fn hset_wrongtype_on_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Create a string key.
    let _: () = redis::cmd("SET")
        .arg("mystring")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    // HSET on a string key should fail with WRONGTYPE.
    let result: redis::RedisResult<i64> = redis::cmd("HSET")
        .arg("mystring")
        .arg("f1")
        .arg("v1")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "HSET on string key should fail");
    let err = format!("{}", result.unwrap_err());
    assert!(err.contains("WRONGTYPE"), "expected WRONGTYPE, got: {err}");
}

// ---------------------------------------------------------------------------
// HRANDFIELD extended tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hrandfield_nonexistent_key_no_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // No-count mode on nonexistent key: returns nil.
    let val: Option<String> = redis::cmd("HRANDFIELD")
        .arg("nokey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn hrandfield_nonexistent_key_with_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Count mode on nonexistent key: returns empty array.
    let fields: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("nokey")
        .arg(5)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(fields.is_empty());
}

#[tokio::test]
async fn hrandfield_count_zero_returns_empty() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();

    // count=0 returns empty array, even when the hash has fields.
    let fields: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("myhash")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(fields.is_empty());
}

#[tokio::test]
async fn hrandfield_withvalues() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query_async(&mut con)
        .await
        .unwrap();

    // count=2 WITHVALUES: returns 4 elements (2 field-value pairs).
    let result: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("myhash")
        .arg(2)
        .arg("WITHVALUES")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result.len(), 4, "expected 2 field-value pairs (4 elements)");

    // Each even-indexed element should be a valid field, and the odd-indexed
    // element after it should be the correct value.
    let expected: std::collections::HashMap<&str, &str> = [("a", "1"), ("b", "2"), ("c", "3")].into_iter().collect();
    for pair in result.chunks(2) {
        let field = &pair[0];
        let value = &pair[1];
        assert!(expected.contains_key(field.as_str()), "unexpected field: {field}");
        assert_eq!(
            expected[field.as_str()],
            value.as_str(),
            "wrong value for field {field}"
        );
    }
}

#[tokio::test]
async fn hrandfield_negative_withvalues() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("only")
        .arg("val")
        .query_async(&mut con)
        .await
        .unwrap();

    // count=-3 WITHVALUES from 1-field hash: 3 pairs = 6 elements.
    let result: Vec<String> = redis::cmd("HRANDFIELD")
        .arg("myhash")
        .arg(-3)
        .arg("WITHVALUES")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result.len(), 6, "expected 3 field-value pairs (6 elements)");

    // All fields should be "only" and all values should be "val".
    for pair in result.chunks(2) {
        assert_eq!(pair[0], "only");
        assert_eq!(pair[1], "val");
    }
}

// ---------------------------------------------------------------------------
// HINCRBYFLOAT edge case tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hincrbyfloat_nan_rejected() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<String> = redis::cmd("HINCRBYFLOAT")
        .arg("myhash")
        .arg("field")
        .arg("NaN")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "HINCRBYFLOAT with NaN should error");
}

#[tokio::test]
async fn hincrbyfloat_inf_rejected() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<String> = redis::cmd("HINCRBYFLOAT")
        .arg("myhash")
        .arg("field")
        .arg("inf")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "HINCRBYFLOAT with inf should error");
}

#[tokio::test]
async fn hincrbyfloat_not_a_float_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Set a non-numeric field value.
    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("name")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    let result: redis::RedisResult<String> = redis::cmd("HINCRBYFLOAT")
        .arg("myhash")
        .arg("name")
        .arg("1.0")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "HINCRBYFLOAT on non-float value should error");
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.to_lowercase().contains("not a valid float"),
        "expected 'not a valid float' error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Arity error tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hash_arity_errors() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // HSET with too few args (needs key + at least one field-value pair).
    let result: redis::RedisResult<i64> = redis::cmd("HSET").arg("myhash").query_async(&mut con).await;
    assert!(result.is_err(), "HSET with 1 arg should error");

    // HSET with incomplete pair (key + field, no value).
    let result: redis::RedisResult<i64> = redis::cmd("HSET")
        .arg("myhash")
        .arg("field")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "HSET with key + field only should error");

    // HGET with wrong arg count (needs exactly key + field).
    let result: redis::RedisResult<String> = redis::cmd("HGET").arg("myhash").query_async(&mut con).await;
    assert!(result.is_err(), "HGET with 1 arg should error");

    // HDEL with too few args (needs key + at least one field).
    let result: redis::RedisResult<i64> = redis::cmd("HDEL").arg("myhash").query_async(&mut con).await;
    assert!(result.is_err(), "HDEL with 1 arg should error");
}
