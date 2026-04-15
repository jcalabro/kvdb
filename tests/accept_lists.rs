//! Acceptance tests for list commands — property-based and randomized.
//!
//! Run via `just accept`. These tests exercise the M9 list commands
//! against a live kvdb server, using a VecDeque in-memory model to
//! verify correctness under randomized input sequences.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;
use proptest::prelude::*;
use std::collections::VecDeque;

mod accept {
    use super::*;

    // -----------------------------------------------------------------------
    // Property: LPUSH then LINDEX(0) returns the same element
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn lpush_lindex_roundtrip(
            value in prop::collection::vec(any::<u8>(), 0..500),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let len: i64 = redis::cmd("LPUSH")
                    .arg("l")
                    .arg(&value[..])
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(len, 1);

                let got: Vec<u8> = redis::cmd("LINDEX")
                    .arg("l")
                    .arg(0)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(got, value);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: LPUSH then LPOP returns the same element, list is empty
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn lpush_lpop_returns_pushed_element(
            value in prop::collection::vec(any::<u8>(), 0..500),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let _: i64 = redis::cmd("LPUSH")
                    .arg("l")
                    .arg(&value[..])
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let got: Vec<u8> = redis::cmd("LPOP")
                    .arg("l")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(got, value);

                // Key should be gone.
                let exists: i64 = redis::cmd("EXISTS")
                    .arg("l")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(exists, 0);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: LLEN(LPUSH(k, v)) == LLEN(k) + 1
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn llen_grows_by_one_per_push(
            count in 1usize..30,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                for i in 0..count {
                    let before: i64 = redis::cmd("LLEN")
                        .arg("l")
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let _: i64 = redis::cmd("LPUSH")
                        .arg("l")
                        .arg(format!("v{i}"))
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let after: i64 = redis::cmd("LLEN")
                        .arg("l")
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    prop_assert_eq!(after, before + 1);
                }
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: RPUSH + LRANGE returns input in order
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn rpush_lrange_preserves_order(
            elements in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 1..50),
                1..30,
            ),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let mut cmd = redis::cmd("RPUSH");
                cmd.arg("l");
                for e in &elements {
                    cmd.arg(e.as_slice());
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                let got: Vec<Vec<u8>> = redis::cmd("LRANGE")
                    .arg("l")
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(got, elements);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Model test: randomized LPUSH/RPUSH/LPOP/RPOP verified against VecDeque
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone)]
    enum Op {
        LPush(Vec<u8>),
        RPush(Vec<u8>),
        LPop,
        RPop,
        Len,
        IndexPos(usize),
        IndexNeg(usize),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            prop::collection::vec(any::<u8>(), 1..30).prop_map(Op::LPush),
            prop::collection::vec(any::<u8>(), 1..30).prop_map(Op::RPush),
            Just(Op::LPop),
            Just(Op::RPop),
            Just(Op::Len),
            (0usize..20).prop_map(Op::IndexPos),
            (1usize..20).prop_map(Op::IndexNeg),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        #[test]
        fn randomized_push_pop_matches_vecdeque(
            ops in prop::collection::vec(op_strategy(), 20..100),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;
                let mut model: VecDeque<Vec<u8>> = VecDeque::new();

                for op in &ops {
                    match op {
                        Op::LPush(v) => {
                            let got: i64 = redis::cmd("LPUSH")
                                .arg("l")
                                .arg(&v[..])
                                .query_async(&mut con)
                                .await
                                .unwrap();
                            model.push_front(v.clone());
                            prop_assert_eq!(got as usize, model.len());
                        }
                        Op::RPush(v) => {
                            let got: i64 = redis::cmd("RPUSH")
                                .arg("l")
                                .arg(&v[..])
                                .query_async(&mut con)
                                .await
                                .unwrap();
                            model.push_back(v.clone());
                            prop_assert_eq!(got as usize, model.len());
                        }
                        Op::LPop => {
                            let got: Option<Vec<u8>> = redis::cmd("LPOP")
                                .arg("l")
                                .query_async(&mut con)
                                .await
                                .unwrap();
                            let expected = model.pop_front();
                            prop_assert_eq!(got, expected);
                        }
                        Op::RPop => {
                            let got: Option<Vec<u8>> = redis::cmd("RPOP")
                                .arg("l")
                                .query_async(&mut con)
                                .await
                                .unwrap();
                            let expected = model.pop_back();
                            prop_assert_eq!(got, expected);
                        }
                        Op::Len => {
                            let got: i64 = redis::cmd("LLEN")
                                .arg("l")
                                .query_async(&mut con)
                                .await
                                .unwrap();
                            prop_assert_eq!(got as usize, model.len());
                        }
                        Op::IndexPos(i) => {
                            let got: Option<Vec<u8>> = redis::cmd("LINDEX")
                                .arg("l")
                                .arg(*i as i64)
                                .query_async(&mut con)
                                .await
                                .unwrap();
                            let expected = model.get(*i).cloned();
                            prop_assert_eq!(got, expected);
                        }
                        Op::IndexNeg(i) => {
                            let neg: i64 = -(*i as i64);
                            let got: Option<Vec<u8>> = redis::cmd("LINDEX")
                                .arg("l")
                                .arg(neg)
                                .query_async(&mut con)
                                .await
                                .unwrap();
                            let expected = if *i <= model.len() {
                                model.get(model.len() - *i).cloned()
                            } else {
                                None
                            };
                            prop_assert_eq!(got, expected);
                        }
                    }
                }

                // Final sanity: LRANGE matches the full model.
                let final_range: Vec<Vec<u8>> = redis::cmd("LRANGE")
                    .arg("l")
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let expected: Vec<Vec<u8>> = model.iter().cloned().collect();
                prop_assert_eq!(final_range, expected);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Model test: LREM matches Vec::retain semantics
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        #[test]
        fn lrem_matches_vec_retain(
            elements in prop::collection::vec(0u8..5, 3..40),
            target in 0u8..5,
            count in -10i64..10,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // Seed the list.
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg("l");
                for e in &elements {
                    cmd.arg(e.to_string());
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                // Compute expected result using the same count semantics.
                let mut model: Vec<String> = elements.iter().map(|e| e.to_string()).collect();
                let target_str = target.to_string();
                let limit: Option<usize> = if count == 0 {
                    None
                } else {
                    Some(count.unsigned_abs() as usize)
                };
                let mut removed_count = 0usize;
                if count >= 0 {
                    let mut i = 0;
                    while i < model.len() {
                        let cap_hit = limit.is_some_and(|l| removed_count >= l);
                        if !cap_hit && model[i] == target_str {
                            model.remove(i);
                            removed_count += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    let mut i = model.len();
                    while i > 0 {
                        i -= 1;
                        let cap_hit = limit.is_some_and(|l| removed_count >= l);
                        if !cap_hit && model[i] == target_str {
                            model.remove(i);
                            removed_count += 1;
                        }
                    }
                }

                let actual_removed: i64 = redis::cmd("LREM")
                    .arg("l")
                    .arg(count)
                    .arg(target_str.as_str())
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(actual_removed as usize, removed_count);

                // Verify the remaining list matches the model.
                let remaining: Vec<String> = redis::cmd("LRANGE")
                    .arg("l")
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(remaining, model);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Model test: LTRIM matches Vec slice semantics
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        #[test]
        fn ltrim_matches_slice(
            elements in prop::collection::vec(0u8..20, 1..30),
            start in -15i64..20,
            stop in -15i64..20,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let mut cmd = redis::cmd("RPUSH");
                cmd.arg("l");
                for e in &elements {
                    cmd.arg(e.to_string());
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                let _: () = redis::cmd("LTRIM")
                    .arg("l")
                    .arg(start)
                    .arg(stop)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                // Compute expected: normalize start/stop, clamp, slice.
                let len = elements.len() as i64;
                let mut s = if start < 0 { start + len } else { start };
                let mut e = if stop < 0 { stop + len } else { stop };
                if s < 0 { s = 0; }
                if e >= len { e = len - 1; }

                let expected: Vec<String> = if s > e || s >= len {
                    Vec::new()
                } else {
                    elements[s as usize..=e as usize].iter().map(|e| e.to_string()).collect()
                };

                let got: Vec<String> = redis::cmd("LRANGE")
                    .arg("l")
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(got, expected);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Large list: LRANGE paginates correctly for 5K elements
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(3))]

        #[test]
        fn lrange_paginates_large_list(
            count in 3000usize..5000,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // Push in chunks to avoid giant single-command args.
                for chunk_start in (0..count).step_by(500) {
                    let chunk_end = (chunk_start + 500).min(count);
                    let mut cmd = redis::cmd("RPUSH");
                    cmd.arg("l");
                    for i in chunk_start..chunk_end {
                        cmd.arg(format!("{i}"));
                    }
                    let _: i64 = cmd.query_async(&mut con).await.unwrap();
                }

                let got: Vec<String> = redis::cmd("LRANGE")
                    .arg("l")
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(got.len(), count);
                // Spot check first/last.
                prop_assert_eq!(got.first().map(|s| s.as_str()), Some("0"));
                prop_assert_eq!(got.last().cloned(), Some(format!("{}", count - 1)));
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // TTL: list with EXPIRE is cleaned up by background worker
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_lazy_expiry() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create a list with a very short TTL.
        let _: i64 = redis::cmd("RPUSH")
            .arg("expiring_list")
            .arg("a")
            .arg("b")
            .arg("c")
            .query_async(&mut con)
            .await
            .unwrap();

        let _: i64 = redis::cmd("PEXPIRE")
            .arg("expiring_list")
            .arg(200)
            .query_async(&mut con)
            .await
            .unwrap();

        // Before expiry: list is accessible.
        let llen: i64 = redis::cmd("LLEN")
            .arg("expiring_list")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(llen, 3);

        // Wait for expiry (200ms TTL + 250ms background scan + margin).
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        // After expiry: key should be gone (lazy or active expiry).
        let llen: i64 = redis::cmd("LLEN")
            .arg("expiring_list")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(llen, 0);

        let exists: i64 = redis::cmd("EXISTS")
            .arg("expiring_list")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(exists, 0);
    }

    // -----------------------------------------------------------------------
    // LPUSH on an expired key of a different type creates a new list
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn lpush_on_expired_string_creates_new_list() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create a string with a very short TTL.
        let _: () = redis::cmd("SET")
            .arg("morph")
            .arg("string_value")
            .arg("PX")
            .arg(200)
            .query_async(&mut con)
            .await
            .unwrap();

        // Wait for the string to expire.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // LPUSH should succeed — the expired string should be cleaned up
        // and a new list created.
        let len: i64 = redis::cmd("LPUSH")
            .arg("morph")
            .arg("list_elem")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(len, 1);

        let t: String = redis::cmd("TYPE").arg("morph").query_async(&mut con).await.unwrap();
        assert_eq!(t, "list");

        let got: Vec<String> = redis::cmd("LRANGE")
            .arg("morph")
            .arg(0)
            .arg(-1)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(got, vec!["list_elem"]);
    }

    // -----------------------------------------------------------------------
    // WRONGTYPE: list commands on hash keys and vice versa
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wrongtype_list_vs_hash() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create a hash.
        let _: i64 = redis::cmd("HSET")
            .arg("myhash")
            .arg("f1")
            .arg("v1")
            .query_async(&mut con)
            .await
            .unwrap();

        // List commands on a hash should fail with WRONGTYPE.
        let result: redis::RedisResult<i64> = redis::cmd("LPUSH").arg("myhash").arg("x").query_async(&mut con).await;
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("WRONGTYPE"), "expected WRONGTYPE, got: {err}");

        // Create a list.
        let _: i64 = redis::cmd("RPUSH")
            .arg("mylist")
            .arg("a")
            .query_async(&mut con)
            .await
            .unwrap();

        // Hash commands on a list should fail with WRONGTYPE.
        let result: redis::RedisResult<i64> = redis::cmd("HSET")
            .arg("mylist")
            .arg("f1")
            .arg("v1")
            .query_async(&mut con)
            .await;
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("WRONGTYPE"), "expected WRONGTYPE, got: {err}");
    }

    // -----------------------------------------------------------------------
    // LMOVE: element value is preserved across source/destination
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn lmove_preserves_element_value(
            elements in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 1..50),
                2..10,
            ),
            from_left in any::<bool>(),
            to_left in any::<bool>(),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // Seed source list.
                let mut cmd = redis::cmd("RPUSH");
                cmd.arg("src");
                for e in &elements {
                    cmd.arg(e.as_slice());
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                let wherefrom = if from_left { "LEFT" } else { "RIGHT" };
                let whereto = if to_left { "LEFT" } else { "RIGHT" };

                let moved: Vec<u8> = redis::cmd("LMOVE")
                    .arg("src")
                    .arg("dst")
                    .arg(wherefrom)
                    .arg(whereto)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                // The moved element must match what was at the popped end.
                let expected = if from_left {
                    elements.first().unwrap().clone()
                } else {
                    elements.last().unwrap().clone()
                };
                prop_assert_eq!(&moved, &expected);

                // Destination should contain exactly the moved element.
                let dst: Vec<Vec<u8>> = redis::cmd("LRANGE")
                    .arg("dst")
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(dst.len(), 1);
                prop_assert_eq!(&dst[0], &expected);
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // LMOVE: source deletion when emptied
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn accept_lmove_source_deletion() {
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

        for expected in &["a", "b", "c"] {
            let moved: String = redis::cmd("LMOVE")
                .arg("src")
                .arg("dst")
                .arg("LEFT")
                .arg("RIGHT")
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(&moved, expected);
        }

        let exists: i64 = redis::cmd("EXISTS").arg("src").query_async(&mut con).await.unwrap();
        assert_eq!(exists, 0);

        let dst: Vec<String> = redis::cmd("LRANGE")
            .arg("dst")
            .arg(0)
            .arg(-1)
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(dst, vec!["a", "b", "c"]);
    }

    // -----------------------------------------------------------------------
    // LMPOP: ordering determinism (always picks first non-empty key)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        #[test]
        fn lmpop_picks_first_nonempty_deterministically(
            has_elements in prop::collection::vec(any::<bool>(), 3..=3),
            from_left in any::<bool>(),
        ) {
            // Ensure at least one key has elements.
            let has_elements: Vec<bool> = if has_elements.iter().all(|&b| !b) {
                vec![true, false, false]
            } else {
                has_elements
            };

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let keys = ["k0", "k1", "k2"];
                let mut first_nonempty: Option<&str> = None;

                for (i, &has) in has_elements.iter().enumerate() {
                    if has {
                        let _: i64 = redis::cmd("RPUSH")
                            .arg(keys[i])
                            .arg(format!("elem_{i}"))
                            .query_async(&mut con)
                            .await
                            .unwrap();
                        if first_nonempty.is_none() {
                            first_nonempty = Some(keys[i]);
                        }
                    }
                }

                let direction = if from_left { "LEFT" } else { "RIGHT" };
                let result: Vec<redis::Value> = redis::cmd("LMPOP")
                    .arg(3)
                    .arg("k0")
                    .arg("k1")
                    .arg("k2")
                    .arg(direction)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let expected_key = first_nonempty.unwrap();
                match &result[0] {
                    redis::Value::BulkString(b) => {
                        prop_assert_eq!(
                            std::str::from_utf8(b).unwrap(),
                            expected_key,
                        );
                    }
                    other => prop_assert!(false, "expected bulk string key name, got: {:?}", other),
                }
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // LMPOP: randomized model test with multiple keys
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]

        #[test]
        fn lmpop_matches_model(
            list0 in prop::collection::vec(0u8..20, 0..10),
            list1 in prop::collection::vec(0u8..20, 0..10),
            list2 in prop::collection::vec(0u8..20, 0..10),
            from_left in any::<bool>(),
            count in 1u64..5,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let keys = ["k0", "k1", "k2"];
                let mut models: Vec<VecDeque<String>> = vec![
                    list0.iter().map(|e| e.to_string()).collect(),
                    list1.iter().map(|e| e.to_string()).collect(),
                    list2.iter().map(|e| e.to_string()).collect(),
                ];

                // Seed lists in Redis.
                for (i, list) in [&list0, &list1, &list2].iter().enumerate() {
                    if !list.is_empty() {
                        let mut cmd = redis::cmd("RPUSH");
                        cmd.arg(keys[i]);
                        for e in list.iter() {
                            cmd.arg(e.to_string());
                        }
                        let _: i64 = cmd.query_async(&mut con).await.unwrap();
                    }
                }

                let direction = if from_left { "LEFT" } else { "RIGHT" };

                // Compute model expectation: find first non-empty.
                let mut expected_key_idx: Option<usize> = None;
                for (i, m) in models.iter().enumerate() {
                    if !m.is_empty() {
                        expected_key_idx = Some(i);
                        break;
                    }
                }

                let result: redis::Value = redis::cmd("LMPOP")
                    .arg(3)
                    .arg("k0")
                    .arg("k1")
                    .arg("k2")
                    .arg(direction)
                    .arg("COUNT")
                    .arg(count)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                match expected_key_idx {
                    None => {
                        prop_assert_eq!(result, redis::Value::Nil);
                    }
                    Some(ki) => {
                        let model = &mut models[ki];
                        let to_pop = (count as usize).min(model.len());
                        let mut expected_popped = Vec::new();
                        for _ in 0..to_pop {
                            let e = if from_left {
                                model.pop_front().unwrap()
                            } else {
                                model.pop_back().unwrap()
                            };
                            expected_popped.push(e);
                        }

                        match result {
                            redis::Value::Array(ref arr) => {
                                prop_assert_eq!(arr.len(), 2);
                                match &arr[0] {
                                    redis::Value::BulkString(b) => {
                                        prop_assert_eq!(
                                            std::str::from_utf8(b).unwrap(),
                                            keys[ki],
                                        );
                                    }
                                    other => prop_assert!(false, "expected key, got: {:?}", other),
                                }
                                match &arr[1] {
                                    redis::Value::Array(elems) => {
                                        prop_assert_eq!(elems.len(), expected_popped.len());
                                        for (j, elem) in elems.iter().enumerate() {
                                            match elem {
                                                redis::Value::BulkString(b) => {
                                                    prop_assert_eq!(
                                                        std::str::from_utf8(b).unwrap(),
                                                        expected_popped[j].as_str(),
                                                    );
                                                }
                                                other => prop_assert!(false, "expected element, got: {:?}", other),
                                            }
                                        }
                                    }
                                    other => prop_assert!(false, "expected elems array, got: {:?}", other),
                                }
                            }
                            other => prop_assert!(false, "expected array result, got: {:?}", other),
                        }

                        // Verify remaining list matches model.
                        let remaining: Vec<String> = redis::cmd("LRANGE")
                            .arg(keys[ki])
                            .arg(0)
                            .arg(-1)
                            .query_async(&mut con)
                            .await
                            .unwrap();
                        let model_remaining: Vec<String> = model.iter().cloned().collect();
                        prop_assert_eq!(remaining, model_remaining);
                    }
                }
                Ok(())
            })?;
        }
    }
}
