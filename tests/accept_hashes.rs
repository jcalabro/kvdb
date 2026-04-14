//! Acceptance tests for hash commands — property-based and randomized.
//!
//! Run via `just accept`.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;
use proptest::prelude::*;
use std::collections::HashMap;

mod accept {
    use super::*;

    // -----------------------------------------------------------------------
    // Property: HSET/HGET roundtrip (proptest, 100 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn hset_hget_roundtrip(
            field in prop::collection::vec(any::<u8>(), 1..200),
            value in prop::collection::vec(any::<u8>(), 0..10_000),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let _: i64 = redis::cmd("HSET")
                    .arg("h")
                    .arg(&field[..])
                    .arg(&value[..])
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let got: Vec<u8> = redis::cmd("HGET")
                    .arg("h")
                    .arg(&field[..])
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert_eq!(got, value);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: HLEN matches distinct field count (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn hlen_matches_distinct_field_count(
            pairs in prop::collection::vec(
                (
                    prop::collection::vec(any::<u8>(), 1..50),
                    prop::collection::vec(any::<u8>(), 1..100),
                ),
                1..50,
            ),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // Track distinct fields with a HashMap.
                let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

                for (field, value) in &pairs {
                    let _: i64 = redis::cmd("HSET")
                        .arg("h")
                        .arg(&field[..])
                        .arg(&value[..])
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    model.insert(field.clone(), value.clone());
                }

                let len: i64 = redis::cmd("HLEN")
                    .arg("h")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert_eq!(len, model.len() as i64, "HLEN should match distinct field count");
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: HLEN == len(HKEYS) == len(HVALS) (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn hlen_hkeys_hvals_consistency(
            num_fields in 1usize..30,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // HSET f0..fN with known distinct fields.
                let mut cmd = redis::cmd("HSET");
                cmd.arg("h");
                for i in 0..num_fields {
                    cmd.arg(format!("f{i}"));
                    cmd.arg(format!("v{i}"));
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                let hlen: i64 = redis::cmd("HLEN")
                    .arg("h")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let keys: Vec<String> = redis::cmd("HKEYS")
                    .arg("h")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let vals: Vec<String> = redis::cmd("HVALS")
                    .arg("h")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert_eq!(hlen, num_fields as i64, "HLEN mismatch");
                prop_assert_eq!(keys.len(), num_fields, "HKEYS length mismatch");
                prop_assert_eq!(vals.len(), num_fields, "HVALS length mismatch");
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: HINCRBY commutativity (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn hincrby_commutativity(
            a in -1_000_000i64..1_000_000,
            b in -1_000_000i64..1_000_000,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // Path 1: HINCRBY h1.c a then b
                let _: i64 = redis::cmd("HINCRBY")
                    .arg("h1")
                    .arg("c")
                    .arg(a)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let result1: i64 = redis::cmd("HINCRBY")
                    .arg("h1")
                    .arg("c")
                    .arg(b)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                // Path 2: HINCRBY h2.c b then a
                let _: i64 = redis::cmd("HINCRBY")
                    .arg("h2")
                    .arg("c")
                    .arg(b)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let result2: i64 = redis::cmd("HINCRBY")
                    .arg("h2")
                    .arg("c")
                    .arg(a)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert_eq!(result1, result2, "HINCRBY should be commutative");
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Randomized hash operations (async test, 500 ops)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn randomized_hash_operations() {
        use rand::Rng;

        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let hash_keys: Vec<String> = (0..3).map(|i| format!("rh_{i}")).collect();
        let field_names: Vec<String> = (0..8).map(|i| format!("rf_{i}")).collect();

        // Model: HashMap<hash_key, HashMap<field, value>>
        let mut model: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut rng = rand::thread_rng();

        for _ in 0..500 {
            let hkey = &hash_keys[rng.gen_range(0..hash_keys.len())];
            match rng.gen_range(0..5u8) {
                // HSET
                0 => {
                    let field = &field_names[rng.gen_range(0..field_names.len())];
                    let val_len = rng.gen_range(0..100);
                    let val: String = (0..val_len).map(|_| (b'a' + rng.gen_range(0..26)) as char).collect();

                    let _: i64 = redis::cmd("HSET")
                        .arg(hkey.as_str())
                        .arg(field.as_str())
                        .arg(val.as_str())
                        .query_async(&mut con)
                        .await
                        .unwrap();

                    model.entry(hkey.clone()).or_default().insert(field.clone(), val);
                }
                // HGET
                1 => {
                    let field = &field_names[rng.gen_range(0..field_names.len())];
                    let result: Option<String> = redis::cmd("HGET")
                        .arg(hkey.as_str())
                        .arg(field.as_str())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let expected = model.get(hkey).and_then(|h| h.get(field)).cloned();
                    assert_eq!(result, expected, "HGET mismatch for {hkey}.{field}");
                }
                // HDEL
                2 => {
                    let field = &field_names[rng.gen_range(0..field_names.len())];
                    let existed = model.get_mut(hkey).map(|h| h.remove(field).is_some()).unwrap_or(false);

                    // Clean up empty hashes in the model.
                    if let Some(h) = model.get(hkey)
                        && h.is_empty()
                    {
                        model.remove(hkey);
                    }

                    let count: i64 = redis::cmd("HDEL")
                        .arg(hkey.as_str())
                        .arg(field.as_str())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let expected = if existed { 1 } else { 0 };
                    assert_eq!(count, expected, "HDEL mismatch for {hkey}.{field}");
                }
                // HLEN
                3 => {
                    let expected = model.get(hkey).map(|h| h.len()).unwrap_or(0) as i64;
                    let len: i64 = redis::cmd("HLEN")
                        .arg(hkey.as_str())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    assert_eq!(len, expected, "HLEN mismatch for {hkey}");
                }
                // HGETALL
                _ => {
                    let result: HashMap<String, String> = redis::cmd("HGETALL")
                        .arg(hkey.as_str())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let expected = model.get(hkey).cloned().unwrap_or_default();
                    assert_eq!(result, expected, "HGETALL mismatch for {hkey}");
                }
            }
        }

        // Final verification: check every hash key matches the model.
        for hkey in &hash_keys {
            let result: HashMap<String, String> = redis::cmd("HGETALL")
                .arg(hkey.as_str())
                .query_async(&mut con)
                .await
                .unwrap();
            let expected = model.get(hkey).cloned().unwrap_or_default();
            assert_eq!(result, expected, "final HGETALL mismatch for {hkey}");
        }
    }

    // -----------------------------------------------------------------------
    // HGETALL with 100 fields (async test)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn hgetall_100_fields() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let mut expected: HashMap<String, String> = HashMap::new();
        let mut cmd = redis::cmd("HSET");
        cmd.arg("bighash");
        for i in 0..100 {
            let field = format!("field_{i:04}");
            let value = format!("value_{i:04}");
            cmd.arg(&field);
            cmd.arg(&value);
            expected.insert(field, value);
        }

        let added: i64 = cmd.query_async(&mut con).await.unwrap();
        assert_eq!(added, 100, "HSET should report 100 new fields");

        let result: HashMap<String, String> = redis::cmd("HGETALL")
            .arg("bighash")
            .query_async(&mut con)
            .await
            .unwrap();

        assert_eq!(result.len(), 100, "HGETALL should return 100 fields");
        assert_eq!(result, expected, "HGETALL should match all field-value pairs");
    }

    // -----------------------------------------------------------------------
    // WRONGTYPE cross-type matrix (async test)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wrongtype_cross_type_matrix() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create a string key.
        let _: () = redis::cmd("SET")
            .arg("skey")
            .arg("hello")
            .query_async(&mut con)
            .await
            .unwrap();

        // All 13 hash-read commands should fail with WRONGTYPE on a string key.
        let cases: Vec<(&str, Vec<&str>)> = vec![
            ("HGET", vec!["skey", "f"]),
            ("HDEL", vec!["skey", "f"]),
            ("HEXISTS", vec!["skey", "f"]),
            ("HLEN", vec!["skey"]),
            ("HKEYS", vec!["skey"]),
            ("HVALS", vec!["skey"]),
            ("HGETALL", vec!["skey"]),
            ("HMGET", vec!["skey", "f"]),
            ("HINCRBY", vec!["skey", "f", "1"]),
            ("HINCRBYFLOAT", vec!["skey", "f", "1.0"]),
            ("HSETNX", vec!["skey", "f", "v"]),
            ("HSTRLEN", vec!["skey", "f"]),
            ("HRANDFIELD", vec!["skey"]),
        ];

        for (cmd_name, args) in &cases {
            let mut cmd = redis::cmd(cmd_name);
            for arg in args {
                cmd.arg(*arg);
            }
            let result: redis::RedisResult<redis::Value> = cmd.query_async(&mut con).await;
            assert!(result.is_err(), "{cmd_name} on a string key should return WRONGTYPE");
            let err = format!("{}", result.unwrap_err());
            assert!(
                err.contains("WRONGTYPE"),
                "{cmd_name}: expected WRONGTYPE error, got: {err}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Hash lazy expiry (async test)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn hash_lazy_expiry() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let _: i64 = redis::cmd("HSET")
            .arg("expiring")
            .arg("field1")
            .arg("value1")
            .query_async(&mut con)
            .await
            .unwrap();

        let _: i64 = redis::cmd("PEXPIRE")
            .arg("expiring")
            .arg(100)
            .query_async(&mut con)
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let result: Option<String> = redis::cmd("HGET")
            .arg("expiring")
            .arg("field1")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, None, "HGET should return None after expiry");
    }

    // -----------------------------------------------------------------------
    // HSET on expired hash creates new (async test)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn hset_on_expired_hash_creates_new() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create hash with old_field and set a short TTL.
        let _: i64 = redis::cmd("HSET")
            .arg("ephemeral")
            .arg("old_field")
            .arg("old_value")
            .query_async(&mut con)
            .await
            .unwrap();

        let _: i64 = redis::cmd("PEXPIRE")
            .arg("ephemeral")
            .arg(100)
            .query_async(&mut con)
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // HSET a new field on the expired key — should create a fresh hash.
        let _: i64 = redis::cmd("HSET")
            .arg("ephemeral")
            .arg("new_field")
            .arg("new_value")
            .query_async(&mut con)
            .await
            .unwrap();

        // old_field should be gone (the old hash expired).
        let old: Option<String> = redis::cmd("HGET")
            .arg("ephemeral")
            .arg("old_field")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(old, None, "old_field should not exist after expiry");

        // HLEN should be 1 (only new_field).
        let len: i64 = redis::cmd("HLEN").arg("ephemeral").query_async(&mut con).await.unwrap();
        assert_eq!(len, 1, "HLEN should be 1 for the newly created hash");

        // Verify the new field is correct.
        let new: String = redis::cmd("HGET")
            .arg("ephemeral")
            .arg("new_field")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(new, "new_value");
    }
}
