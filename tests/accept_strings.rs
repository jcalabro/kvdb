//! Acceptance tests for string commands — property-based and randomized.
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
    // Property tests (proptest)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn set_get_roundtrip(
            key in prop::collection::vec(any::<u8>(), 1..100),
            value in prop::collection::vec(any::<u8>(), 0..10_000),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;
                let _: () = redis::cmd("SET").arg(&key[..]).arg(&value[..])
                    .query_async(&mut con).await.unwrap();
                let got: Vec<u8> = redis::cmd("GET").arg(&key[..])
                    .query_async(&mut con).await.unwrap();
                prop_assert_eq!(got, value);
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn incr_commutativity(
            a in -1_000_000i64..1_000_000,
            b in -1_000_000i64..1_000_000,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // Path 1: INCRBY a then INCRBY b
                let _: i64 = redis::cmd("INCRBY").arg("k1").arg(a)
                    .query_async(&mut con).await.unwrap();
                let result1: i64 = redis::cmd("INCRBY").arg("k1").arg(b)
                    .query_async(&mut con).await.unwrap();

                // Path 2: INCRBY b then INCRBY a
                let _: i64 = redis::cmd("INCRBY").arg("k2").arg(b)
                    .query_async(&mut con).await.unwrap();
                let result2: i64 = redis::cmd("INCRBY").arg("k2").arg(a)
                    .query_async(&mut con).await.unwrap();

                prop_assert_eq!(result1, result2, "INCRBY should be commutative");
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn strlen_matches(
            key in prop::collection::vec(any::<u8>(), 1..100),
            value in prop::collection::vec(any::<u8>(), 0..10_000),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;
                let _: () = redis::cmd("SET").arg(&key[..]).arg(&value[..])
                    .query_async(&mut con).await.unwrap();
                let len: i64 = redis::cmd("STRLEN").arg(&key[..])
                    .query_async(&mut con).await.unwrap();
                prop_assert_eq!(len, value.len() as i64, "STRLEN should match value length");
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn append_concatenation(
            a in prop::collection::vec(any::<u8>(), 0..5_000),
            b in prop::collection::vec(any::<u8>(), 0..5_000),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;
                let _: i64 = redis::cmd("APPEND").arg("k").arg(&a[..])
                    .query_async(&mut con).await.unwrap();
                let _: i64 = redis::cmd("APPEND").arg("k").arg(&b[..])
                    .query_async(&mut con).await.unwrap();
                let got: Vec<u8> = redis::cmd("GET").arg("k")
                    .query_async(&mut con).await.unwrap();
                let mut expected = a.clone();
                expected.extend_from_slice(&b);
                prop_assert_eq!(got, expected, "APPEND should concatenate");
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Randomized command sequence test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn randomized_string_operations() {
        use rand::Rng;

        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let keys: Vec<String> = (0..10).map(|i| format!("rkey_{i}")).collect();
        let mut model: HashMap<String, Vec<u8>> = HashMap::new();
        let mut rng = rand::thread_rng();

        for _ in 0..500 {
            let key = &keys[rng.gen_range(0..keys.len())];
            match rng.gen_range(0..5u8) {
                // SET
                0 => {
                    let val_len = rng.gen_range(0..200);
                    let val: Vec<u8> = (0..val_len).map(|_| rng.r#gen()).collect();
                    let _: () = redis::cmd("SET")
                        .arg(key.as_str())
                        .arg(&val[..])
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    model.insert(key.clone(), val);
                }
                // GET
                1 => {
                    let result: Option<Vec<u8>> =
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
                // EXISTS
                3 => {
                    let expected = if model.contains_key(key) { 1i64 } else { 0 };
                    let count: i64 = redis::cmd("EXISTS")
                        .arg(key.as_str())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    assert_eq!(count, expected, "EXISTS mismatch for key {key}");
                }
                // APPEND
                _ => {
                    let append_len = rng.gen_range(1..50);
                    let append_data: Vec<u8> = (0..append_len).map(|_| rng.r#gen()).collect();
                    let _: i64 = redis::cmd("APPEND")
                        .arg(key.as_str())
                        .arg(&append_data[..])
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let entry = model.entry(key.clone()).or_default();
                    entry.extend_from_slice(&append_data);
                }
            }
        }

        // Final verification: check all keys match the model.
        for key in &keys {
            let result: Option<Vec<u8>> = redis::cmd("GET").arg(key.as_str()).query_async(&mut con).await.unwrap();
            let expected = model.get(key).cloned();
            assert_eq!(result, expected, "final GET mismatch for key {key}");
        }
    }

    // -----------------------------------------------------------------------
    // Chunking boundary test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn chunking_boundaries() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let sizes = [99_999, 100_000, 100_001, 200_000, 300_001];

        for &size in &sizes {
            let key = format!("chunk_{size}");
            let val = vec![b'A'; size];

            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(&val[..])
                .query_async(&mut con)
                .await
                .unwrap();

            let got: Vec<u8> = redis::cmd("GET").arg(&key).query_async(&mut con).await.unwrap();
            assert_eq!(got.len(), size, "length mismatch for size {size}");
            assert_eq!(got, val, "data mismatch for size {size}");
        }
    }

    // -----------------------------------------------------------------------
    // SET flag matrix test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn set_flag_combinations() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // -- Valid combinations --

        // NX on new key
        let result: Option<String> = redis::cmd("SET")
            .arg("fnx")
            .arg("v")
            .arg("NX")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, Some("OK".to_string()));

        // XX on existing key
        let result: Option<String> = redis::cmd("SET")
            .arg("fnx")
            .arg("v2")
            .arg("XX")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, Some("OK".to_string()));

        // NX + EX
        let result: Option<String> = redis::cmd("SET")
            .arg("fnx_ex")
            .arg("v")
            .arg("NX")
            .arg("EX")
            .arg("3600")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, Some("OK".to_string()));

        // NX + GET (Redis 7+: NX+GET returns nil when key doesn't exist, error when it does... actually Redis returns nil on non-existent)
        // NX + GET: key does not exist -> returns nil (old value is nil)
        let result: Option<String> = redis::cmd("SET")
            .arg("fnx_get")
            .arg("v")
            .arg("NX")
            .arg("GET")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, None, "NX+GET on non-existent key returns nil (old value)");

        // XX + PX
        let _: () = redis::cmd("SET")
            .arg("fxx_px")
            .arg("v1")
            .query_async(&mut con)
            .await
            .unwrap();
        let result: Option<String> = redis::cmd("SET")
            .arg("fxx_px")
            .arg("v2")
            .arg("XX")
            .arg("PX")
            .arg("60000")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, Some("OK".to_string()));

        // GET flag (returns old value)
        let _: () = redis::cmd("SET")
            .arg("fget")
            .arg("old")
            .query_async(&mut con)
            .await
            .unwrap();
        let result: Option<String> = redis::cmd("SET")
            .arg("fget")
            .arg("new")
            .arg("GET")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(result, Some("old".to_string()));

        // KEEPTTL
        let _: () = redis::cmd("SET")
            .arg("fkeepttl")
            .arg("v1")
            .arg("EX")
            .arg("3600")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("fkeepttl")
            .arg("v2")
            .arg("KEEPTTL")
            .query_async(&mut con)
            .await
            .unwrap();
        let val: String = redis::cmd("GET").arg("fkeepttl").query_async(&mut con).await.unwrap();
        assert_eq!(val, "v2");

        // EX
        let _: () = redis::cmd("SET")
            .arg("fex")
            .arg("v")
            .arg("EX")
            .arg("3600")
            .query_async(&mut con)
            .await
            .unwrap();
        let val: String = redis::cmd("GET").arg("fex").query_async(&mut con).await.unwrap();
        assert_eq!(val, "v");

        // PX
        let _: () = redis::cmd("SET")
            .arg("fpx")
            .arg("v")
            .arg("PX")
            .arg("3600000")
            .query_async(&mut con)
            .await
            .unwrap();
        let val: String = redis::cmd("GET").arg("fpx").query_async(&mut con).await.unwrap();
        assert_eq!(val, "v");

        // EXAT (future timestamp)
        let future_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let _: () = redis::cmd("SET")
            .arg("fexat")
            .arg("v")
            .arg("EXAT")
            .arg(future_ts.to_string())
            .query_async(&mut con)
            .await
            .unwrap();
        let val: String = redis::cmd("GET").arg("fexat").query_async(&mut con).await.unwrap();
        assert_eq!(val, "v");

        // PXAT (future timestamp in ms)
        let future_ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 3_600_000;
        let _: () = redis::cmd("SET")
            .arg("fpxat")
            .arg("v")
            .arg("PXAT")
            .arg(future_ts_ms.to_string())
            .query_async(&mut con)
            .await
            .unwrap();
        let val: String = redis::cmd("GET").arg("fpxat").query_async(&mut con).await.unwrap();
        assert_eq!(val, "v");

        // -- Invalid combinations --

        // NX + XX
        let result: redis::RedisResult<String> = redis::cmd("SET")
            .arg("bad1")
            .arg("v")
            .arg("NX")
            .arg("XX")
            .query_async(&mut con)
            .await;
        assert!(result.is_err(), "NX+XX should be rejected");

        // EX + PX
        let result: redis::RedisResult<String> = redis::cmd("SET")
            .arg("bad2")
            .arg("v")
            .arg("EX")
            .arg("10")
            .arg("PX")
            .arg("10000")
            .query_async(&mut con)
            .await;
        assert!(result.is_err(), "EX+PX should be rejected");

        // EX + KEEPTTL
        let result: redis::RedisResult<String> = redis::cmd("SET")
            .arg("bad3")
            .arg("v")
            .arg("EX")
            .arg("10")
            .arg("KEEPTTL")
            .query_async(&mut con)
            .await;
        assert!(result.is_err(), "EX+KEEPTTL should be rejected");
    }
}
