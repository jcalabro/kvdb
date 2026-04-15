//! Acceptance tests for sorted set commands -- property-based and randomized.
//!
//! Run via `just accept`.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;
use proptest::prelude::*;
use std::collections::BTreeMap;

mod accept {
    use super::*;

    // -----------------------------------------------------------------------
    // Property: ZADD/ZSCORE round-trip (proptest, 100 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn zadd_zscore_roundtrip(
            score in -1e15f64..1e15f64,
            member in "[a-z]{1,20}",
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let added: i64 = redis::cmd("ZADD")
                    .arg("z")
                    .arg(score)
                    .arg(&member)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(added, 1);

                let got: f64 = redis::cmd("ZSCORE")
                    .arg("z")
                    .arg(&member)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert!(
                    (got - score).abs() < 1e-10,
                    "score mismatch: got={got}, expected={score}"
                );

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: ZRANK + ZREVRANK == ZCARD - 1 (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn zrank_plus_zrevrank_eq_zcard_minus_one(
            count in 2usize..20,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // ZADD members with unique scores (index as score).
                for i in 0..count {
                    let _: i64 = redis::cmd("ZADD")
                        .arg("z")
                        .arg(i as f64)
                        .arg(format!("m{i}"))
                        .query_async(&mut con)
                        .await
                        .unwrap();
                }

                let card: i64 = redis::cmd("ZCARD")
                    .arg("z")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(card, count as i64);

                // Pick a member in the middle to verify the invariant.
                let sample = count / 2;
                let sample_member = format!("m{sample}");

                let rank: i64 = redis::cmd("ZRANK")
                    .arg("z")
                    .arg(&sample_member)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let revrank: i64 = redis::cmd("ZREVRANK")
                    .arg("z")
                    .arg(&sample_member)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert!(
                    rank + revrank == card - 1,
                    "ZRANK + ZREVRANK should equal ZCARD-1: {} + {} != {}",
                    rank, revrank, card - 1
                );

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: ZRANGE always sorted (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn zrange_always_sorted(
            scores in prop::collection::vec(-1e6f64..1e6f64, 2..50),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // ZADD each score with a unique member name.
                let mut cmd = redis::cmd("ZADD");
                cmd.arg("z");
                for (i, score) in scores.iter().enumerate() {
                    cmd.arg(*score).arg(format!("m{i}"));
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                // ZRANGE 0 -1 WITHSCORES returns [member, score, member, score, ...].
                let result: Vec<String> = redis::cmd("ZRANGE")
                    .arg("z")
                    .arg(0)
                    .arg(-1)
                    .arg("WITHSCORES")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                // Extract scores (every other element starting at index 1).
                let returned_scores: Vec<f64> = result
                    .iter()
                    .skip(1)
                    .step_by(2)
                    .map(|s| s.parse::<f64>().unwrap())
                    .collect();

                // Verify non-decreasing order.
                for i in 1..returned_scores.len() {
                    prop_assert!(
                        returned_scores[i] >= returned_scores[i - 1],
                        "scores not sorted: [{}] {} < [{}] {}",
                        i - 1,
                        returned_scores[i - 1],
                        i,
                        returned_scores[i]
                    );
                }

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Randomized ZADD/ZREM/ZSCORE sequences against BTreeMap model
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn accept_randomized_zset_model() {
        for iteration in 0..5 {
            let ctx = TestContext::new().await;
            let mut con = ctx.connection().await;
            let mut model: BTreeMap<String, f64> = BTreeMap::new();

            // Simple LCG for reproducible randomness seeded from wall clock + iteration.
            let mut seed: u64 = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            seed = seed.wrapping_add(iteration);

            for _ in 0..200 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let member = format!("m{}", seed % 20);
                let op = seed % 4;

                match op {
                    0 | 1 => {
                        // ZADD
                        let score = ((seed % 2000) as f64) - 1000.0;
                        let added: i64 = redis::cmd("ZADD")
                            .arg("zkey")
                            .arg(score)
                            .arg(&member)
                            .query_async(&mut con)
                            .await
                            .unwrap();
                        let was_new = model.insert(member, score).is_none();
                        assert_eq!(added, if was_new { 1 } else { 0 });
                    }
                    2 => {
                        // ZREM
                        let removed: i64 = redis::cmd("ZREM")
                            .arg("zkey")
                            .arg(&member)
                            .query_async(&mut con)
                            .await
                            .unwrap();
                        let was_present = model.remove(&member).is_some();
                        assert_eq!(removed, if was_present { 1 } else { 0 });
                    }
                    3 => {
                        // ZSCORE
                        let score: Option<f64> = redis::cmd("ZSCORE")
                            .arg("zkey")
                            .arg(&member)
                            .query_async(&mut con)
                            .await
                            .unwrap();
                        let model_score = model.get(&member).copied();
                        match (score, model_score) {
                            (Some(got), Some(expected)) => {
                                assert!(
                                    (got - expected).abs() < 1e-10,
                                    "score mismatch for {member}: kvdb={got}, model={expected}"
                                );
                            }
                            (None, None) => {}
                            _ => panic!("presence mismatch for {member}: kvdb={score:?}, model={model_score:?}"),
                        }
                    }
                    _ => unreachable!(),
                }
            }

            // Final: ZCARD matches model.
            let card: i64 = redis::cmd("ZCARD").arg("zkey").query_async(&mut con).await.unwrap();
            assert_eq!(card as usize, model.len());
        }
    }

    // -----------------------------------------------------------------------
    // Sorted set lazy expiry
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn accept_sorted_set_lazy_expiry() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create a sorted set.
        let _: i64 = redis::cmd("ZADD")
            .arg("expiring_zset")
            .arg(1.0)
            .arg("a")
            .arg(2.0)
            .arg("b")
            .arg(3.0)
            .arg("c")
            .query_async(&mut con)
            .await
            .unwrap();

        // Set a very short TTL.
        let _: i64 = redis::cmd("PEXPIRE")
            .arg("expiring_zset")
            .arg(200)
            .query_async(&mut con)
            .await
            .unwrap();

        // Before expiry: sorted set is accessible.
        let card: i64 = redis::cmd("ZCARD")
            .arg("expiring_zset")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(card, 3);

        // Wait for expiry (200ms TTL + 250ms background scan + margin).
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        // After expiry: key should be gone (lazy or active expiry).
        let card: i64 = redis::cmd("ZCARD")
            .arg("expiring_zset")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(card, 0);

        let exists: i64 = redis::cmd("EXISTS")
            .arg("expiring_zset")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(exists, 0);
    }

    // -----------------------------------------------------------------------
    // ZADD on expired string creates sorted set
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn accept_zadd_on_expired_string_creates_zset() {
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

        // ZADD should succeed -- the expired string should be cleaned up
        // and a new sorted set created.
        let added: i64 = redis::cmd("ZADD")
            .arg("morph")
            .arg(42.0)
            .arg("member")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(added, 1);

        let t: String = redis::cmd("TYPE").arg("morph").query_async(&mut con).await.unwrap();
        assert_eq!(t, "zset");

        let score: f64 = redis::cmd("ZSCORE")
            .arg("morph")
            .arg("member")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!((score - 42.0).abs() < f64::EPSILON);
    }

    // -----------------------------------------------------------------------
    // WRONGTYPE cross-type matrix (sorted set vs hash)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn accept_wrongtype_zset_on_hash() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create a hash.
        let _: i64 = redis::cmd("HSET")
            .arg("mykey")
            .arg("field")
            .arg("value")
            .query_async(&mut con)
            .await
            .unwrap();

        // Sorted set commands on a hash should fail with WRONGTYPE.
        let err = redis::cmd("ZADD")
            .arg("mykey")
            .arg(1.0)
            .arg("member")
            .query_async::<i64>(&mut con)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");

        let err = redis::cmd("ZCARD")
            .arg("mykey")
            .query_async::<i64>(&mut con)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");

        let err = redis::cmd("ZSCORE")
            .arg("mykey")
            .arg("member")
            .query_async::<Option<f64>>(&mut con)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");

        let err = redis::cmd("ZRANK")
            .arg("mykey")
            .arg("member")
            .query_async::<Option<i64>>(&mut con)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
    }

    #[tokio::test]
    async fn accept_wrongtype_hash_on_zset() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Create a sorted set.
        let _: i64 = redis::cmd("ZADD")
            .arg("mykey")
            .arg(1.0)
            .arg("member")
            .query_async(&mut con)
            .await
            .unwrap();

        // Hash commands on a sorted set should fail with WRONGTYPE.
        let err = redis::cmd("HSET")
            .arg("mykey")
            .arg("field")
            .arg("value")
            .query_async::<i64>(&mut con)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");

        let err = redis::cmd("HGET")
            .arg("mykey")
            .arg("field")
            .query_async::<Option<String>>(&mut con)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");

        let err = redis::cmd("HLEN")
            .arg("mykey")
            .query_async::<i64>(&mut con)
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
    }

    // -----------------------------------------------------------------------
    // Property: ZPOPMIN returns scores in non-decreasing order (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn zpopmin_returns_ascending_scores(
            scores in prop::collection::vec(-1e6f64..1e6f64, 2..30),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let mut cmd = redis::cmd("ZADD");
                cmd.arg("z");
                for (i, score) in scores.iter().enumerate() {
                    cmd.arg(*score).arg(format!("m{i}"));
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                let card: i64 = redis::cmd("ZCARD")
                    .arg("z")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                // Pop all members one by one and verify non-decreasing scores.
                let mut prev_score: Option<f64> = None;
                for _ in 0..card {
                    let result: Vec<String> = redis::cmd("ZPOPMIN")
                        .arg("z")
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    prop_assert!(result.len() == 2, "ZPOPMIN should return [member, score]");
                    let score: f64 = result[1].parse().unwrap();
                    if let Some(prev) = prev_score {
                        prop_assert!(
                            score >= prev,
                            "ZPOPMIN scores not non-decreasing: {} after {}",
                            score, prev
                        );
                    }
                    prev_score = Some(score);
                }

                // After popping all, set should be empty.
                let final_card: i64 = redis::cmd("ZCARD")
                    .arg("z")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(final_card, 0);

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: ZCOUNT == len(ZRANGEBYSCORE) for same bounds (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn zcount_matches_zrangebyscore_len(
            scores in prop::collection::vec(-1e6f64..1e6f64, 2..30),
            lo in -1e6f64..1e6f64,
            hi in -1e6f64..1e6f64,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let mut cmd = redis::cmd("ZADD");
                cmd.arg("z");
                for (i, score) in scores.iter().enumerate() {
                    cmd.arg(*score).arg(format!("m{i}"));
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                // Ensure lo <= hi for meaningful range.
                let (min, max) = if lo <= hi { (lo, hi) } else { (hi, lo) };
                let min_str = format!("{min}");
                let max_str = format!("{max}");

                let count: i64 = redis::cmd("ZCOUNT")
                    .arg("z")
                    .arg(&min_str)
                    .arg(&max_str)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let members: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                    .arg("z")
                    .arg(&min_str)
                    .arg(&max_str)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert_eq!(
                    count, members.len() as i64,
                    "ZCOUNT={} but ZRANGEBYSCORE returned {} members for range [{}, {}]",
                    count, members.len(), min, max
                );

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: ZREMRANGEBYRANK preserves cardinality invariant (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn zremrangebyrank_cardinality_invariant(
            count in 3usize..20,
            start_pct in 0usize..100,
            stop_pct in 0usize..100,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // ZADD members with unique scores.
                let mut cmd = redis::cmd("ZADD");
                cmd.arg("z");
                for i in 0..count {
                    cmd.arg(i as f64).arg(format!("m{i}"));
                }
                let added: i64 = cmd.query_async(&mut con).await.unwrap();
                prop_assert_eq!(added, count as i64);

                // Convert percentages to ranks.
                let start = (start_pct * count / 100).min(count - 1) as i64;
                let stop = (stop_pct * count / 100).min(count - 1) as i64;

                let removed: i64 = redis::cmd("ZREMRANGEBYRANK")
                    .arg("z")
                    .arg(start)
                    .arg(stop)
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let remaining: i64 = redis::cmd("ZCARD")
                    .arg("z")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert_eq!(
                    remaining, count as i64 - removed,
                    "ZCARD ({}) != initial ({}) - removed ({})",
                    remaining, count, removed
                );

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: ZRANGEBYLEX + ZLEXCOUNT consistency (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn zlexcount_matches_zrangebylex_len(
            members in prop::collection::vec("[a-z]{1,5}", 2..20),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                // All same score for meaningful lex ordering.
                let mut cmd = redis::cmd("ZADD");
                cmd.arg("z");
                for m in &members {
                    cmd.arg(0.0).arg(m.as_str());
                }
                let _: i64 = cmd.query_async(&mut con).await.unwrap();

                // Full lex range.
                let count: i64 = redis::cmd("ZLEXCOUNT")
                    .arg("z")
                    .arg("-")
                    .arg("+")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let range_members: Vec<String> = redis::cmd("ZRANGEBYLEX")
                    .arg("z")
                    .arg("-")
                    .arg("+")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                prop_assert_eq!(
                    count, range_members.len() as i64,
                    "ZLEXCOUNT={} but ZRANGEBYLEX returned {} members",
                    count, range_members.len()
                );

                // Also verify lex ordering.
                for i in 1..range_members.len() {
                    prop_assert!(
                        range_members[i] >= range_members[i - 1],
                        "ZRANGEBYLEX not sorted: {:?} < {:?}",
                        range_members[i - 1], range_members[i]
                    );
                }

                Ok(())
            })?;
        }
    }
}
