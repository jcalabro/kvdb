// tests/accept_pubsub.rs
//! Acceptance tests for pub/sub (Milestone 11).
//!
//! These are exhaustive tests: message ordering, delivery completeness,
//! pattern matching correctness, and concurrent publish/subscribe stress.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use std::time::Duration;

use futures::StreamExt;
use harness::TestContext;

/// Accept test: all messages delivered, no drops, preserving order.
///
/// Publishes N messages on one connection, verifies a subscriber
/// receives all of them in exact order. Tests the local delivery path.
#[tokio::test]
async fn accept_pubsub_ordered_delivery_no_drops() {
    const N: usize = 50;

    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("accept_ordered").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish N messages.
    for i in 0..N {
        let _: i64 = redis::cmd("PUBLISH")
            .arg("accept_ordered")
            .arg(format!("msg{i:04}"))
            .query_async(&mut pub_con)
            .await
            .unwrap();
    }

    // Receive and verify order.
    let mut stream = sub_con.on_message();
    for i in 0..N {
        let msg = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for msg {i}"))
            .expect("stream ended");
        let payload = msg.get_payload::<String>().unwrap();
        assert_eq!(payload, format!("msg{i:04}"), "message {i} out of order: got {payload}");
    }
}

/// Accept test: pattern matching correctness across a variety of patterns.
///
/// For each (pattern, channels) pair, verify that:
/// - All matching channels trigger delivery
/// - Non-matching channels don't trigger delivery
#[tokio::test]
async fn accept_pubsub_pattern_matching_correctness() {
    let cases: &[(&str, &[&str], &[&str])] = &[
        (
            "news.*",
            &["news.sports", "news.tech", "news."],
            &["sport.news", "news"],
        ),
        ("h?llo", &["hello", "hallo", "hzllo"], &["hllo", "heello", "hllo2"]),
        ("h[ae]llo", &["hello", "hallo"], &["hillo", "hllo"]),
        ("*", &["anything", "news", "12345", ""], &[]),
        (
            "prefix:*:suffix",
            &["prefix::suffix", "prefix:middle:suffix"],
            &["prefix:no", "other:middle:suffix"],
        ),
    ];

    for (pattern, matching, non_matching) in cases {
        let ctx = TestContext::new().await;
        let mut pub_con = ctx.connection().await;
        let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

        sub_con.psubscribe(*pattern).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Publish to matching channels — should deliver.
        for &ch in *matching {
            let _: i64 = redis::cmd("PUBLISH")
                .arg(ch)
                .arg("match")
                .query_async(&mut pub_con)
                .await
                .unwrap();
        }

        // Publish to non-matching channels — should NOT deliver.
        for &ch in *non_matching {
            let _: i64 = redis::cmd("PUBLISH")
                .arg(ch)
                .arg("nomatch")
                .query_async(&mut pub_con)
                .await
                .unwrap();
        }

        // Collect all delivered messages.
        let mut delivered = Vec::new();
        let collect_until = tokio::time::Instant::now() + Duration::from_millis(300);
        let mut stream = sub_con.on_message();
        loop {
            let remaining = collect_until.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(msg)) => {
                    delivered.push(msg.get_channel_name().to_string());
                }
                _ => break,
            }
        }

        // Every matching channel should appear exactly once (we published once each).
        // Non-matching channels should never appear.
        for &ch in *matching {
            assert!(
                delivered.contains(&ch.to_string()),
                "pattern={pattern:?}: expected delivery on channel {ch:?}, got {delivered:?}"
            );
        }
        for &ch in *non_matching {
            assert!(
                !delivered.contains(&ch.to_string()),
                "pattern={pattern:?}: unexpected delivery on non-matching channel {ch:?}"
            );
        }
        // Each matching channel should appear at most once — no duplicates.
        let unique: std::collections::HashSet<_> = delivered.iter().collect();
        assert_eq!(
            unique.len(),
            delivered.len(),
            "pattern={pattern:?}: duplicate deliveries detected: {delivered:?}"
        );
    }
}

/// Accept test: fan-out — N subscribers all receive every message.
#[tokio::test]
async fn accept_pubsub_fan_out_all_subscribers_receive() {
    const N_SUBS: usize = 5;
    const N_MSGS: usize = 10;

    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;

    let mut subs: Vec<redis::aio::PubSub> = Vec::new();
    for _ in 0..N_SUBS {
        let mut sub = ctx.client.get_async_pubsub().await.unwrap();
        sub.subscribe("fanout").await.unwrap();
        subs.push(sub);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..N_MSGS {
        let _: i64 = redis::cmd("PUBLISH")
            .arg("fanout")
            .arg(format!("m{i}"))
            .query_async(&mut pub_con)
            .await
            .unwrap();
    }

    // Each subscriber should receive all messages.
    for (sub_idx, mut sub) in subs.into_iter().enumerate() {
        let mut stream = sub.on_message();
        for msg_idx in 0..N_MSGS {
            let msg = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap_or_else(|_| panic!("sub {sub_idx} timed out at msg {msg_idx}"))
                .expect("stream ended");
            let payload = msg.get_payload::<String>().unwrap();
            assert_eq!(
                payload,
                format!("m{msg_idx}"),
                "sub {sub_idx} got wrong message at index {msg_idx}"
            );
        }
    }
}

/// Accept test: subscribe + unsubscribe lifecycle.
///
/// Verify that unsubscribing stops delivery, and re-subscribing
/// only delivers new messages (no replay).
#[tokio::test]
async fn accept_pubsub_subscribe_unsubscribe_no_replay() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("lifecycle").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish one message while subscribed.
    let _: i64 = redis::cmd("PUBLISH")
        .arg("lifecycle")
        .arg("before_unsub")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    // Receive it.
    let msg = tokio::time::timeout(Duration::from_secs(3), sub_con.on_message().next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.get_payload::<String>().unwrap(), "before_unsub");

    // Unsubscribe.
    sub_con.unsubscribe("lifecycle").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish during the unsubscribed period.
    let _: i64 = redis::cmd("PUBLISH")
        .arg("lifecycle")
        .arg("during_unsub")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Re-subscribe.
    sub_con.subscribe("lifecycle").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish after re-subscribing.
    let _: i64 = redis::cmd("PUBLISH")
        .arg("lifecycle")
        .arg("after_resub")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    // Should only get "after_resub" — no replay of "during_unsub".
    let msg = tokio::time::timeout(Duration::from_secs(3), sub_con.on_message().next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        msg.get_payload::<String>().unwrap(),
        "after_resub",
        "should not have received the during-unsub message (pub/sub has no replay)"
    );
}

/// Accept test: PUBSUB CHANNELS accurately tracks subscriptions.
#[tokio::test]
async fn accept_pubsub_channels_tracks_subscriptions_accurately() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;

    let base_count = || async {
        let chs: Vec<String> = redis::cmd("PUBSUB")
            .arg("CHANNELS")
            .query_async(&mut redis::aio::MultiplexedConnection::clone(&pub_con))
            .await
            .unwrap_or_default();
        chs.len()
    };

    let initial = base_count().await;

    let mut s1 = ctx.client.get_async_pubsub().await.unwrap();
    let mut s2 = ctx.client.get_async_pubsub().await.unwrap();

    s1.subscribe("track1").await.unwrap();
    s2.subscribe("track2").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after_sub: Vec<String> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert!(after_sub.contains(&"track1".to_string()));
    assert!(after_sub.contains(&"track2".to_string()));
    assert_eq!(after_sub.len(), initial + 2);

    s1.unsubscribe("track1").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after_unsub: Vec<String> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert!(!after_unsub.contains(&"track1".to_string()));
    assert!(after_unsub.contains(&"track2".to_string()));
}

/// Accept test: concurrent publishers don't corrupt message ordering
/// for a single subscriber.
#[tokio::test]
async fn accept_pubsub_concurrent_publishers_no_corruption() {
    const N_PUBS: usize = 3;
    const MSGS_PER_PUB: usize = 5;

    let ctx = TestContext::new().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("concurrent").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Spawn N_PUBS concurrent publishers.
    let mut handles = Vec::new();
    for pub_idx in 0..N_PUBS {
        let client = ctx.client.clone();
        handles.push(tokio::spawn(async move {
            let mut con = client.get_multiplexed_async_connection().await.unwrap();
            for msg_idx in 0..MSGS_PER_PUB {
                let payload = format!("pub{pub_idx}:msg{msg_idx}");
                let _: i64 = redis::cmd("PUBLISH")
                    .arg("concurrent")
                    .arg(&payload)
                    .query_async(&mut con)
                    .await
                    .unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Receive all N_PUBS * MSGS_PER_PUB messages.
    let total = N_PUBS * MSGS_PER_PUB;
    let mut received = Vec::new();
    let mut stream = sub_con.on_message();
    for _ in 0..total {
        let msg = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("timed out waiting for concurrent message")
            .expect("stream ended");
        received.push(msg.get_payload::<String>().unwrap());
    }

    assert_eq!(received.len(), total, "wrong message count");

    // Each per-publisher sequence should be internally ordered
    // (messages from the same publisher must arrive in order).
    for pub_idx in 0..N_PUBS {
        let mut last_msg_idx: Option<usize> = None;
        for msg in &received {
            if let Some(rest) = msg.strip_prefix(&format!("pub{pub_idx}:msg")) {
                let idx: usize = rest.parse().unwrap();
                if let Some(last) = last_msg_idx {
                    assert!(idx > last, "pub{pub_idx}: messages out of order: {last} then {idx}");
                }
                last_msg_idx = Some(idx);
            }
        }
    }
}

/// Accept test: no messages delivered after UNSUBSCRIBE.
///
/// Publishes N messages to a channel while subscribed, receives them,
/// then unsubscribes and publishes N more. Asserts that the second batch
/// is never delivered. This is the canonical test for watcher-task
/// cancellation correctness.
#[tokio::test]
async fn accept_pubsub_no_delivery_after_unsubscribe() {
    const N: usize = 5;

    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("no_delivery_after_unsub").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 1: publish and receive N messages while subscribed.
    for i in 0..N {
        let _: i64 = redis::cmd("PUBLISH")
            .arg("no_delivery_after_unsub")
            .arg(format!("before:{i}"))
            .query_async(&mut pub_con)
            .await
            .unwrap();
    }

    let mut stream = sub_con.on_message();
    for i in 0..N {
        let msg = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out at message {i}"))
            .expect("stream ended");
        assert!(
            msg.get_payload::<String>().unwrap().starts_with("before:"),
            "unexpected payload at msg {i}"
        );
    }

    // Phase 2: unsubscribe and publish N more messages.
    drop(stream); // release borrow on sub_con
    sub_con.unsubscribe("no_delivery_after_unsub").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..N {
        let count: i64 = redis::cmd("PUBLISH")
            .arg("no_delivery_after_unsub")
            .arg(format!("after:{i}"))
            .query_async(&mut pub_con)
            .await
            .unwrap();
        assert_eq!(count, 0, "publish count should be 0 after unsubscribe (msg {i})");
    }

    // Drain any pending messages with a short window — should be none.
    let mut received_after = Vec::new();
    let window = tokio::time::Instant::now() + Duration::from_millis(300);
    let mut stream = sub_con.on_message();
    loop {
        let remaining = window.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(msg)) => received_after.push(msg.get_payload::<String>().unwrap()),
            _ => break,
        }
    }

    assert!(
        received_after.is_empty(),
        "received messages after UNSUBSCRIBE: {received_after:?}"
    );
}
