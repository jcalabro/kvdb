//! Acceptance tests for key management and TTL commands — property-based and randomized.
//!
//! Run via `just accept`.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;
use std::collections::HashMap;

mod accept {
    use super::*;

    // -----------------------------------------------------------------------
    // TTL round-trip test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn ttl_roundtrip() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Use longer TTLs to avoid race conditions in test execution
        let ttl_values = [10, 30, 100, 3600];

        for &ttl in &ttl_values {
            let key = format!("ttl_key_{ttl}");

            // SET with EXPIRE
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg("value")
                .query_async(&mut con)
                .await
                .unwrap();

            let set_result: i64 = redis::cmd("EXPIRE")
                .arg(&key)
                .arg(ttl)
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(set_result, 1, "EXPIRE should return 1 for existing key");

            // TTL should return value in (0, original]
            let ttl_result: i64 = redis::cmd("TTL").arg(&key).query_async(&mut con).await.unwrap();
            assert!(
                ttl_result > 0 && ttl_result <= ttl,
                "TTL should be in range (0, {ttl}], got {ttl_result}"
            );

            // PTTL should be in milliseconds
            let pttl_result: i64 = redis::cmd("PTTL").arg(&key).query_async(&mut con).await.unwrap();
            assert!(
                pttl_result > 0 && pttl_result <= ttl * 1000,
                "PTTL should be in range (0, {0}], got {pttl_result}",
                ttl * 1000
            );

            // EXPIRETIME should return a unix timestamp
            let expiretime: i64 = redis::cmd("EXPIRETIME").arg(&key).query_async(&mut con).await.unwrap();
            assert!(expiretime > 0, "EXPIRETIME should return positive timestamp");

            // PEXPIRETIME should return a unix timestamp in ms
            let pexpiretime: i64 = redis::cmd("PEXPIRETIME").arg(&key).query_async(&mut con).await.unwrap();
            assert!(pexpiretime > 0, "PEXPIRETIME should return positive timestamp");
            assert_eq!(
                pexpiretime / 1000,
                expiretime,
                "PEXPIRETIME/1000 should match EXPIRETIME"
            );
        }
    }

    // -----------------------------------------------------------------------
    // PERSIST idempotence test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn persist_idempotence() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // SET key without TTL, PERSIST should return 0
        let _: () = redis::cmd("SET")
            .arg("persist_key")
            .arg("value")
            .query_async(&mut con)
            .await
            .unwrap();

        let result: i64 = redis::cmd("PERSIST")
            .arg("persist_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 0, "PERSIST on key without TTL should return 0");

        // EXPIRE, PERSIST should return 1
        let _: i64 = redis::cmd("EXPIRE")
            .arg("persist_key")
            .arg(100)
            .query_async(&mut con)
            .await
            .unwrap();

        let result: i64 = redis::cmd("PERSIST")
            .arg("persist_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 1, "PERSIST on key with TTL should return 1");

        // TTL should now be -1
        let ttl: i64 = redis::cmd("TTL")
            .arg("persist_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(ttl, -1, "TTL after PERSIST should be -1");

        // PERSIST again should return 0
        let result: i64 = redis::cmd("PERSIST")
            .arg("persist_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 0, "PERSIST again should return 0");
    }

    // -----------------------------------------------------------------------
    // RENAME preserves value and TTL test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn rename_preserves_value_and_ttl() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // SET with EX 100
        let _: () = redis::cmd("SET")
            .arg("old_key")
            .arg("test_value")
            .arg("EX")
            .arg("100")
            .query_async(&mut con)
            .await
            .unwrap();

        // RENAME
        let _: () = redis::cmd("RENAME")
            .arg("old_key")
            .arg("new_key")
            .query_async(&mut con)
            .await
            .unwrap();

        // Verify GET returns same value
        let value: String = redis::cmd("GET").arg("new_key").query_async(&mut con).await.unwrap();
        assert_eq!(value, "test_value", "RENAME should preserve value");

        // Verify TTL > 0
        let ttl: i64 = redis::cmd("TTL").arg("new_key").query_async(&mut con).await.unwrap();
        assert!(ttl > 0 && ttl <= 100, "RENAME should preserve TTL, got {ttl}");

        // Verify old key doesn't exist
        let exists: i64 = redis::cmd("EXISTS").arg("old_key").query_async(&mut con).await.unwrap();
        assert_eq!(exists, 0, "old key should not exist after RENAME");
    }

    // -----------------------------------------------------------------------
    // RENAMENX test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn renamenx_behavior() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Setup: create two keys
        let _: () = redis::cmd("SET")
            .arg("src")
            .arg("source_value")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("dest")
            .arg("dest_value")
            .query_async(&mut con)
            .await
            .unwrap();

        // RENAMENX to existing key should return 0
        let result: i64 = redis::cmd("RENAMENX")
            .arg("src")
            .arg("dest")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 0, "RENAMENX to existing key should return 0");

        // src should still exist
        let src_value: String = redis::cmd("GET").arg("src").query_async(&mut con).await.unwrap();
        assert_eq!(src_value, "source_value", "src should be unchanged");

        // RENAMENX to non-existing key should return 1
        let result: i64 = redis::cmd("RENAMENX")
            .arg("src")
            .arg("new_dest")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 1, "RENAMENX to non-existing key should return 1");

        // new_dest should have the value
        let new_value: String = redis::cmd("GET").arg("new_dest").query_async(&mut con).await.unwrap();
        assert_eq!(new_value, "source_value", "new_dest should have src's value");
    }

    // -----------------------------------------------------------------------
    // SELECT isolation test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn select_isolation() {
        let ctx = TestContext::new().await;

        // For DBs 0-3: SELECT, SET shared_key to "dbN"
        for db in 0..4 {
            // Use a fresh connection for each DB to avoid conflicts
            let mut con = ctx.connection().await;

            let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();

            let _: () = redis::cmd("SET")
                .arg("shared_key")
                .arg(format!("db{db}"))
                .query_async(&mut con)
                .await
                .unwrap();
        }

        // Read back — each DB should have its own value
        for db in 0..4 {
            let mut con = ctx.connection().await;

            let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();

            let value: String = redis::cmd("GET").arg("shared_key").query_async(&mut con).await.unwrap();
            assert_eq!(value, format!("db{db}"), "DB {db} should have its own value");
        }

        // Clean up with FLUSHDB for each
        for db in 0..4 {
            let mut con = ctx.connection().await;

            let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();
            let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // FLUSHDB isolation test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn flushdb_isolation() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // SET in db0
        let _: () = redis::cmd("SELECT").arg(0).query_async(&mut con).await.unwrap();
        let _: () = redis::cmd("SET")
            .arg("db0_key")
            .arg("db0_value")
            .query_async(&mut con)
            .await
            .unwrap();

        // SELECT 1, SET
        let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
        let _: () = redis::cmd("SET")
            .arg("db1_key")
            .arg("db1_value")
            .query_async(&mut con)
            .await
            .unwrap();

        // FLUSHDB in db1
        let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();

        // Verify db1 empty
        let exists: i64 = redis::cmd("EXISTS").arg("db1_key").query_async(&mut con).await.unwrap();
        assert_eq!(exists, 0, "db1 should be empty after FLUSHDB");

        // Verify db0 intact
        let _: () = redis::cmd("SELECT").arg(0).query_async(&mut con).await.unwrap();
        let value: String = redis::cmd("GET").arg("db0_key").query_async(&mut con).await.unwrap();
        assert_eq!(value, "db0_value", "db0 should be intact after FLUSHDB in db1");
    }

    // -----------------------------------------------------------------------
    // Randomized key lifecycle test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn randomized_key_lifecycle() {
        use rand::Rng;

        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let keys: Vec<String> = (0..20).map(|i| format!("lifecycle_{i}")).collect();
        let mut model: HashMap<String, String> = HashMap::new();
        let mut rng = rand::thread_rng();

        for _ in 0..500 {
            let key_idx = rng.gen_range(0..keys.len());
            let key = &keys[key_idx];

            match rng.gen_range(0..6u8) {
                // SET
                0 => {
                    let rand_val: u32 = rng.r#gen();
                    let value = format!("val_{rand_val}");
                    let _: () = redis::cmd("SET")
                        .arg(key.as_str())
                        .arg(&value)
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    model.insert(key.clone(), value);
                }
                // GET
                1 => {
                    let result: Option<String> =
                        redis::cmd("GET").arg(key.as_str()).query_async(&mut con).await.unwrap();
                    let expected = model.get(key).cloned();
                    assert_eq!(result, expected, "GET mismatch for key {key}");
                }
                // DEL
                2 => {
                    let existed = model.remove(key).is_some();
                    let count: i64 = redis::cmd("DEL").arg(key.as_str()).query_async(&mut con).await.unwrap();
                    let expected = if existed { 1 } else { 0 };
                    assert_eq!(count, expected, "DEL mismatch for key {key}");
                }
                // EXPIRE
                3 => {
                    let ttl = rng.gen_range(10..3600);
                    let result: i64 = redis::cmd("EXPIRE")
                        .arg(key.as_str())
                        .arg(ttl)
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let expected = if model.contains_key(key) { 1i64 } else { 0 };
                    assert_eq!(result, expected, "EXPIRE mismatch for key {key}");
                }
                // PERSIST
                4 => {
                    let _: i64 = redis::cmd("PERSIST")
                        .arg(key.as_str())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    // PERSIST doesn't change key existence, just TTL
                }
                // RENAME
                _ => {
                    if model.contains_key(key) {
                        let dest_idx = rng.gen_range(0..keys.len());
                        let dest = &keys[dest_idx];

                        let _: Result<(), redis::RedisError> = redis::cmd("RENAME")
                            .arg(key.as_str())
                            .arg(dest.as_str())
                            .query_async(&mut con)
                            .await;

                        // Update model: move key to dest
                        if let Some(value) = model.remove(key) {
                            model.insert(dest.clone(), value);
                        }
                    }
                }
            }
        }

        // Final verification: check all keys match the model
        for key in &keys {
            let result: Option<String> = redis::cmd("GET").arg(key.as_str()).query_async(&mut con).await.unwrap();
            let expected = model.get(key).cloned();
            assert_eq!(result, expected, "final GET mismatch for key {key}");
        }
    }

    // -----------------------------------------------------------------------
    // DBSIZE accuracy test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn dbsize_accuracy() {
        use rand::Rng;

        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let keys: Vec<String> = (0..50).map(|i| format!("dbsize_key_{i}")).collect();
        let mut expected_keys = std::collections::HashSet::new();
        let mut rng = rand::thread_rng();

        for _ in 0..200 {
            let key_idx = rng.gen_range(0..keys.len());
            let key = &keys[key_idx];

            if rng.gen_bool(0.6) {
                // SET
                let _: () = redis::cmd("SET")
                    .arg(key.as_str())
                    .arg("value")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                expected_keys.insert(key.clone());
            } else {
                // DEL
                let _: i64 = redis::cmd("DEL").arg(key.as_str()).query_async(&mut con).await.unwrap();
                expected_keys.remove(key);
            }
        }

        // Verify DBSIZE matches
        let dbsize: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
        assert_eq!(
            dbsize,
            expected_keys.len() as i64,
            "DBSIZE should match expected key count"
        );

        // Verify all expected keys exist
        for key in &expected_keys {
            let exists: i64 = redis::cmd("EXISTS")
                .arg(key.as_str())
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(exists, 1, "expected key {key} should exist");
        }
    }

    // -----------------------------------------------------------------------
    // TYPE command test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn type_command() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Non-existent key
        let result: String = redis::cmd("TYPE")
            .arg("nonexistent")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, "none", "TYPE on non-existent key should return 'none'");

        // String key
        let _: () = redis::cmd("SET")
            .arg("string_key")
            .arg("value")
            .query_async(&mut con)
            .await
            .unwrap();
        let result: String = redis::cmd("TYPE")
            .arg("string_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, "string", "TYPE on string key should return 'string'");
    }

    // -----------------------------------------------------------------------
    // TOUCH command test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn touch_command() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // TOUCH on non-existent key
        let result: i64 = redis::cmd("TOUCH")
            .arg("nonexistent")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 0, "TOUCH on non-existent key should return 0");

        // Create keys
        let _: () = redis::cmd("SET")
            .arg("key1")
            .arg("value1")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("key2")
            .arg("value2")
            .query_async(&mut con)
            .await
            .unwrap();

        // TOUCH multiple keys
        let result: i64 = redis::cmd("TOUCH")
            .arg("key1")
            .arg("key2")
            .arg("nonexistent")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 2, "TOUCH should return count of existing keys");
    }

    // -----------------------------------------------------------------------
    // UNLINK command test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unlink_command() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create keys
        let _: () = redis::cmd("SET")
            .arg("unlink1")
            .arg("value1")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("unlink2")
            .arg("value2")
            .query_async(&mut con)
            .await
            .unwrap();

        // UNLINK multiple keys
        let result: i64 = redis::cmd("UNLINK")
            .arg("unlink1")
            .arg("unlink2")
            .arg("nonexistent")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 2, "UNLINK should return count of deleted keys");

        // Verify keys are gone
        let exists: i64 = redis::cmd("EXISTS").arg("unlink1").query_async(&mut con).await.unwrap();
        assert_eq!(exists, 0, "unlink1 should not exist");

        let exists: i64 = redis::cmd("EXISTS").arg("unlink2").query_async(&mut con).await.unwrap();
        assert_eq!(exists, 0, "unlink2 should not exist");
    }

    // -----------------------------------------------------------------------
    // FLUSHALL test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn flushall_clears_all_databases() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // SET in multiple databases
        for db in 0..3 {
            let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();
            let _: () = redis::cmd("SET")
                .arg(format!("key_{db}"))
                .arg("value")
                .query_async(&mut con)
                .await
                .unwrap();
        }

        // FLUSHALL from db1
        let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
        let _: () = redis::cmd("FLUSHALL").query_async(&mut con).await.unwrap();

        // Verify all databases are empty
        for db in 0..3 {
            let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();
            let dbsize: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
            assert_eq!(dbsize, 0, "DB {db} should be empty after FLUSHALL");
        }
    }

    // -----------------------------------------------------------------------
    // PEXPIRE / PEXPIREAT / PTTL test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn pexpire_commands() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // PEXPIRE
        let _: () = redis::cmd("SET")
            .arg("pexpire_key")
            .arg("value")
            .query_async(&mut con)
            .await
            .unwrap();

        let result: i64 = redis::cmd("PEXPIRE")
            .arg("pexpire_key")
            .arg(5000)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 1, "PEXPIRE should return 1 for existing key");

        let pttl: i64 = redis::cmd("PTTL")
            .arg("pexpire_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(
            pttl > 0 && pttl <= 5000,
            "PTTL should be in range (0, 5000], got {pttl}"
        );

        // PEXPIREAT
        let future_ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 10_000;

        let _: () = redis::cmd("SET")
            .arg("pexpireat_key")
            .arg("value")
            .query_async(&mut con)
            .await
            .unwrap();

        let result: i64 = redis::cmd("PEXPIREAT")
            .arg("pexpireat_key")
            .arg(future_ts_ms)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 1, "PEXPIREAT should return 1 for existing key");

        let pttl: i64 = redis::cmd("PTTL")
            .arg("pexpireat_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(pttl > 0 && pttl <= 10_000, "PTTL should be positive");
    }

    // -----------------------------------------------------------------------
    // EXPIREAT test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn expireat_command() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let future_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 3600;

        let _: () = redis::cmd("SET")
            .arg("expireat_key")
            .arg("value")
            .query_async(&mut con)
            .await
            .unwrap();

        let result: i64 = redis::cmd("EXPIREAT")
            .arg("expireat_key")
            .arg(future_ts)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, 1, "EXPIREAT should return 1 for existing key");

        let ttl: i64 = redis::cmd("TTL")
            .arg("expireat_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(ttl > 0 && ttl <= 3600, "TTL should be positive");
    }
}
