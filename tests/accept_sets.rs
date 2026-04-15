//! Acceptance tests for set commands — property-based and randomized.
//!
//! Run via `just accept`.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;
use proptest::prelude::*;
use std::collections::HashSet;

mod accept {
    use super::*;

    // -----------------------------------------------------------------------
    // Property: SADD/SISMEMBER roundtrip (proptest, 100 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn sadd_sismember_roundtrip(
            member in prop::collection::vec(any::<u8>(), 1..200),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let added: i64 = redis::cmd("SADD")
                    .arg("s")
                    .arg(&member[..])
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(added, 1);

                let is_member: i64 = redis::cmd("SISMEMBER")
                    .arg("s")
                    .arg(&member[..])
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(is_member, 1);

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: SCARD(SADD(k, m)) <= SCARD(k) + 1 (proptest, 50 cases)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn scard_sadd_monotonicity(
            members in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 1..50),
                1..30,
            ),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                for member in &members {
                    let before: i64 = redis::cmd("SCARD")
                        .arg("s")
                        .query_async(&mut con)
                        .await
                        .unwrap();

                    let _: i64 = redis::cmd("SADD")
                        .arg("s")
                        .arg(&member[..])
                        .query_async(&mut con)
                        .await
                        .unwrap();

                    let after: i64 = redis::cmd("SCARD")
                        .arg("s")
                        .query_async(&mut con)
                        .await
                        .unwrap();

                    prop_assert!(
                        after <= before + 1,
                        "SCARD should increase by at most 1: before={before}, after={after}"
                    );
                    prop_assert!(
                        after >= before,
                        "SCARD should not decrease after SADD: before={before}, after={after}"
                    );
                }
                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: SINTER commutativity — SINTER(A, B) == SINTER(B, A)
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn sinter_commutative(
            set_a in prop::collection::hash_set(1u32..1000, 0..30),
            set_b in prop::collection::hash_set(1u32..1000, 0..30),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                for m in &set_a {
                    let _: i64 = redis::cmd("SADD")
                        .arg("a")
                        .arg(m.to_string())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                }

                for m in &set_b {
                    let _: i64 = redis::cmd("SADD")
                        .arg("b")
                        .arg(m.to_string())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                }

                let inter_ab: Vec<String> = redis::cmd("SINTER")
                    .arg("a")
                    .arg("b")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let inter_ba: Vec<String> = redis::cmd("SINTER")
                    .arg("b")
                    .arg("a")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let set_ab: HashSet<String> = inter_ab.into_iter().collect();
                let set_ba: HashSet<String> = inter_ba.into_iter().collect();

                prop_assert_eq!(&set_ab, &set_ba, "SINTER should be commutative");

                // Also verify against model.
                let model_a: HashSet<String> = set_a.iter().map(|m| m.to_string()).collect();
                let model_b: HashSet<String> = set_b.iter().map(|m| m.to_string()).collect();
                let expected: HashSet<String> = model_a.intersection(&model_b).cloned().collect();

                prop_assert_eq!(&set_ab, &expected, "SINTER result should match model");

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Property: SUNION superset — SUNION(A, B) ⊇ A and SUNION(A, B) ⊇ B
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn sunion_superset(
            set_a in prop::collection::hash_set(1u32..500, 0..20),
            set_b in prop::collection::hash_set(1u32..500, 0..20),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                for m in &set_a {
                    let _: i64 = redis::cmd("SADD")
                        .arg("a")
                        .arg(m.to_string())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                }

                for m in &set_b {
                    let _: i64 = redis::cmd("SADD")
                        .arg("b")
                        .arg(m.to_string())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                }

                let union: Vec<String> = redis::cmd("SUNION")
                    .arg("a")
                    .arg("b")
                    .query_async(&mut con)
                    .await
                    .unwrap();

                let union_set: HashSet<String> = union.into_iter().collect();
                let model_a: HashSet<String> = set_a.iter().map(|m| m.to_string()).collect();
                let model_b: HashSet<String> = set_b.iter().map(|m| m.to_string()).collect();

                prop_assert!(
                    model_a.is_subset(&union_set),
                    "SUNION should contain all members of A"
                );
                prop_assert!(
                    model_b.is_subset(&union_set),
                    "SUNION should contain all members of B"
                );

                // Also check exact equality with model.
                let expected: HashSet<String> = model_a.union(&model_b).cloned().collect();
                prop_assert_eq!(&union_set, &expected, "SUNION result should match model");

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Randomized SADD/SREM sequences verified against HashSet model
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        #[test]
        fn random_sadd_srem_matches_model(
            ops in prop::collection::vec(
                (any::<bool>(), 0u32..50),  // (is_add, member_id)
                1..100,
            ),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                let mut model: HashSet<String> = HashSet::new();

                for (is_add, member_id) in &ops {
                    let member = member_id.to_string();

                    if *is_add {
                        let _: i64 = redis::cmd("SADD")
                            .arg("s")
                            .arg(&member)
                            .query_async(&mut con)
                            .await
                            .unwrap();
                        model.insert(member);
                    } else {
                        let _: i64 = redis::cmd("SREM")
                            .arg("s")
                            .arg(&member)
                            .query_async(&mut con)
                            .await
                            .unwrap();
                        model.remove(&member);
                    }
                }

                // Verify cardinality.
                let card: i64 = redis::cmd("SCARD")
                    .arg("s")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                prop_assert_eq!(card, model.len() as i64, "SCARD should match model");

                // Verify SMEMBERS matches model.
                if !model.is_empty() {
                    let members: Vec<String> = redis::cmd("SMEMBERS")
                        .arg("s")
                        .query_async(&mut con)
                        .await
                        .unwrap();
                    let server_set: HashSet<String> = members.into_iter().collect();
                    prop_assert_eq!(&server_set, &model, "SMEMBERS should match model");
                }

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // Large sets (1K members)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn large_set_1k_members() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Add 1000 members in batches of 100.
        for batch in 0..10 {
            let mut cmd = redis::cmd("SADD");
            cmd.arg("bigset");
            for i in 0..100 {
                cmd.arg(format!("member_{}", batch * 100 + i));
            }
            let _: i64 = cmd.query_async(&mut con).await.unwrap();
        }

        let card: i64 = redis::cmd("SCARD").arg("bigset").query_async(&mut con).await.unwrap();
        assert_eq!(card, 1000);

        let members: Vec<String> = redis::cmd("SMEMBERS")
            .arg("bigset")
            .query_async(&mut con)
            .await
            .unwrap();
        assert_eq!(members.len(), 1000);

        // Verify all members exist.
        let member_set: HashSet<String> = members.into_iter().collect();
        for i in 0..1000 {
            assert!(member_set.contains(&format!("member_{i}")), "missing member_{i}");
        }
    }

    // -----------------------------------------------------------------------
    // SUNION / SDIFF / SINTER on randomized pairs
    // -----------------------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(30))]

        #[test]
        fn multi_set_ops_match_model(
            set_a in prop::collection::hash_set(0u32..200, 0..50),
            set_b in prop::collection::hash_set(0u32..200, 0..50),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let ctx = TestContext::new().await;
                let mut con = ctx.connection().await;

                for m in &set_a {
                    let _: i64 = redis::cmd("SADD")
                        .arg("a")
                        .arg(m.to_string())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                }

                for m in &set_b {
                    let _: i64 = redis::cmd("SADD")
                        .arg("b")
                        .arg(m.to_string())
                        .query_async(&mut con)
                        .await
                        .unwrap();
                }

                let model_a: HashSet<String> = set_a.iter().map(|m| m.to_string()).collect();
                let model_b: HashSet<String> = set_b.iter().map(|m| m.to_string()).collect();

                // SINTER
                let inter: Vec<String> = redis::cmd("SINTER")
                    .arg("a")
                    .arg("b")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let inter_set: HashSet<String> = inter.into_iter().collect();
                let expected_inter: HashSet<String> = model_a.intersection(&model_b).cloned().collect();
                prop_assert_eq!(&inter_set, &expected_inter, "SINTER mismatch");

                // SUNION
                let union: Vec<String> = redis::cmd("SUNION")
                    .arg("a")
                    .arg("b")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let union_set: HashSet<String> = union.into_iter().collect();
                let expected_union: HashSet<String> = model_a.union(&model_b).cloned().collect();
                prop_assert_eq!(&union_set, &expected_union, "SUNION mismatch");

                // SDIFF (a - b)
                let diff: Vec<String> = redis::cmd("SDIFF")
                    .arg("a")
                    .arg("b")
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let diff_set: HashSet<String> = diff.into_iter().collect();
                let expected_diff: HashSet<String> = model_a.difference(&model_b).cloned().collect();
                prop_assert_eq!(&diff_set, &expected_diff, "SDIFF mismatch");

                Ok(())
            })?;
        }
    }

    // -----------------------------------------------------------------------
    // WRONGTYPE cross-type matrix
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wrongtype_set_on_hash() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let _: i64 = redis::cmd("HSET")
            .arg("mykey")
            .arg("field")
            .arg("value")
            .query_async(&mut con)
            .await
            .unwrap();

        let err = redis::cmd("SADD")
            .arg("mykey")
            .arg("member")
            .query_async::<i64>(&mut con)
            .await
            .unwrap_err();

        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
    }

    #[tokio::test]
    async fn wrongtype_hash_on_set() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let _: i64 = redis::cmd("SADD")
            .arg("mykey")
            .arg("member")
            .query_async(&mut con)
            .await
            .unwrap();

        let err = redis::cmd("HSET")
            .arg("mykey")
            .arg("field")
            .arg("value")
            .query_async::<i64>(&mut con)
            .await
            .unwrap_err();

        let msg = format!("{err}");
        assert!(msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {msg}");
    }
}
