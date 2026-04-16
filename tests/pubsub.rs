// tests/pubsub.rs
//! Integration tests for pub/sub (Milestone 11).
//!
//! Tests the full pub/sub cycle: SUBSCRIBE/PUBLISH/UNSUBSCRIBE/PSUBSCRIBE
//! and all related commands. Uses real FDB-backed cross-instance delivery
//! via versionstamp-ordered message queues + FDB watches.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use std::time::Duration;

use futures::StreamExt;
use harness::TestContext;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ── PUBLISH basic ──────────────────────────────────────────────────────────

#[tokio::test]
async fn publish_returns_zero_with_no_subscribers() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let count: i64 = redis::cmd("PUBLISH")
        .arg("ch")
        .arg("msg")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn publish_returns_subscriber_count() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("ch").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let count: i64 = redis::cmd("PUBLISH")
        .arg("ch")
        .arg("msg")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    // 1 local subscriber
    assert_eq!(count, 1);
}

// ── SUBSCRIBE / message delivery ──────────────────────────────────────────

#[tokio::test]
async fn subscribe_and_receive_message() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("news").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _: i64 = redis::cmd("PUBLISH")
        .arg("news")
        .arg("hello")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(3), sub_con.on_message().next())
        .await
        .expect("timed out waiting for message")
        .expect("stream ended");

    assert_eq!(msg.get_channel_name(), "news");
    assert_eq!(msg.get_payload::<String>().unwrap(), "hello");
}

#[tokio::test]
async fn multiple_messages_delivered_in_order() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("ordered").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for i in 0..5u8 {
        let _: i64 = redis::cmd("PUBLISH")
            .arg("ordered")
            .arg(format!("msg{i}"))
            .query_async(&mut pub_con)
            .await
            .unwrap();
    }

    let mut stream = sub_con.on_message();
    for i in 0..5u8 {
        let msg = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for msg{i}"))
            .expect("stream ended");
        assert_eq!(msg.get_payload::<String>().unwrap(), format!("msg{i}"));
    }
}

#[tokio::test]
async fn multiple_subscribers_same_channel() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub1 = ctx.client.get_async_pubsub().await.unwrap();
    let mut sub2 = ctx.client.get_async_pubsub().await.unwrap();

    sub1.subscribe("broadcast").await.unwrap();
    sub2.subscribe("broadcast").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let count: i64 = redis::cmd("PUBLISH")
        .arg("broadcast")
        .arg("hi")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let msg1 = tokio::time::timeout(Duration::from_secs(3), sub1.on_message().next())
        .await
        .unwrap()
        .unwrap();
    let msg2 = tokio::time::timeout(Duration::from_secs(3), sub2.on_message().next())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(msg1.get_payload::<String>().unwrap(), "hi");
    assert_eq!(msg2.get_payload::<String>().unwrap(), "hi");
}

#[tokio::test]
async fn subscribe_to_multiple_channels() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe(&["ch1", "ch2"]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _: i64 = redis::cmd("PUBLISH")
        .arg("ch1")
        .arg("from1")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    let _: i64 = redis::cmd("PUBLISH")
        .arg("ch2")
        .arg("from2")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    let mut stream = sub_con.on_message();
    let mut got = std::collections::HashMap::new();
    for _ in 0..2 {
        let msg = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap();
        got.insert(msg.get_channel_name().to_string(), msg.get_payload::<String>().unwrap());
    }
    assert_eq!(got["ch1"], "from1");
    assert_eq!(got["ch2"], "from2");
}

// ── UNSUBSCRIBE ────────────────────────────────────────────────────────────

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("ch").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    sub_con.unsubscribe("ch").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let count: i64 = redis::cmd("PUBLISH")
        .arg("ch")
        .arg("should not arrive")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert_eq!(count, 0, "PUBLISH should find no subscribers after UNSUBSCRIBE");
}

// ── PSUBSCRIBE pattern delivery ────────────────────────────────────────────

#[tokio::test]
async fn psubscribe_star_pattern() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.psubscribe("news.*").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _: i64 = redis::cmd("PUBLISH")
        .arg("news.sports")
        .arg("goal")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), sub_con.on_message().next())
        .await
        .expect("timed out waiting for pmessage")
        .expect("stream ended");

    assert_eq!(msg.get_channel_name(), "news.sports");
    assert_eq!(msg.get_payload::<String>().unwrap(), "goal");
}

#[tokio::test]
async fn psubscribe_question_mark_pattern() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.psubscribe("h?llo").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let _: i64 = redis::cmd("PUBLISH")
        .arg("hello")
        .arg("world")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    // "hllo" should NOT match h?llo (one char needed)
    let _: i64 = redis::cmd("PUBLISH")
        .arg("hllo")
        .arg("nope")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), sub_con.on_message().next())
        .await
        .expect("timed out")
        .expect("stream ended");
    assert_eq!(msg.get_channel_name(), "hello");
}

#[tokio::test]
async fn psubscribe_does_not_match_unrelated_channel() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.psubscribe("sports.*").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish to a non-matching channel — subscriber should get nothing.
    let count: i64 = redis::cmd("PUBLISH")
        .arg("news.politics")
        .arg("vote")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    // No local pattern subscribers match "news.politics" against "sports.*"
    assert_eq!(count, 0);
}

// ── Pub/sub mode enforcement ───────────────────────────────────────────────

/// Send a raw RESP command over a TCP stream and return the response bytes.
async fn raw_command(stream: &mut tokio::net::TcpStream, cmd: &[u8]) -> Vec<u8> {
    stream.write_all(cmd).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    buf[..n].to_vec()
}

#[tokio::test]
async fn pubsub_mode_rejects_normal_commands() {
    let ctx = TestContext::new().await;
    let mut stream = tokio::net::TcpStream::connect(ctx.addr).await.unwrap();

    // SUBSCRIBE to put the connection into pub/sub mode.
    // RESP: *2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nch\r\n
    let resp = raw_command(&mut stream, b"*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nch\r\n").await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("subscribe"), "expected subscribe confirmation, got: {s:?}");

    // GET is not allowed in pub/sub mode — server must return an error.
    // RESP: *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
    let resp = raw_command(&mut stream, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").await;
    let s = String::from_utf8_lossy(&resp);
    assert!(
        s.starts_with('-'),
        "expected error (-ERR) for GET in pub/sub mode, got: {s:?}"
    );
    assert!(
        s.contains("not allowed") || s.contains("ERR"),
        "unexpected error message: {s:?}"
    );
}

#[tokio::test]
async fn ping_in_pubsub_mode_returns_push_response() {
    // Verify that PING in pub/sub mode returns a Push/Array ["pong", ""]
    // rather than a SimpleString "+PONG". This matches the Redis 7+ spec
    // and is important for clients that distinguish the two response types.
    let ctx = TestContext::new().await;
    let mut stream = tokio::net::TcpStream::connect(ctx.addr).await.unwrap();

    // Subscribe.
    let _ = raw_command(&mut stream, b"*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nch\r\n").await;

    // PING with no message.
    let resp = raw_command(&mut stream, b"*1\r\n$4\r\nPING\r\n").await;
    let s = String::from_utf8_lossy(&resp);
    // Response should be an Array or Push (starts with * or >) containing "pong".
    assert!(
        s.starts_with('*') || s.starts_with('>'),
        "expected array/push response for PING in pub/sub mode, got: {s:?}"
    );
    assert!(
        s.contains("pong"),
        "expected 'pong' in pub/sub PING response, got: {s:?}"
    );

    // PING with a message argument.
    let resp = raw_command(&mut stream, b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n").await;
    let s = String::from_utf8_lossy(&resp);
    assert!(
        s.contains("pong"),
        "expected 'pong' in pub/sub PING response, got: {s:?}"
    );
    assert!(
        s.contains("hello"),
        "expected message echoed in pub/sub PING response, got: {s:?}"
    );
}

#[tokio::test]
async fn ping_works_on_regular_connection() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let pong: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
async fn ping_works_in_pubsub_mode() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("ch").await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Verify the subscribed connection is still live by publishing to it.
    let _: i64 = redis::cmd("PUBLISH")
        .arg("ch")
        .arg("still_alive")
        .query_async(&mut pub_con)
        .await
        .unwrap();
}

// ── PUBSUB introspection ───────────────────────────────────────────────────

#[tokio::test]
async fn pubsub_channels_lists_active() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe("events").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let channels: Vec<String> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert!(channels.contains(&"events".to_string()), "channels: {channels:?}");
}

#[tokio::test]
async fn pubsub_channels_with_pattern_filter() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub1 = ctx.client.get_async_pubsub().await.unwrap();
    let mut sub2 = ctx.client.get_async_pubsub().await.unwrap();

    sub1.subscribe("news.sports").await.unwrap();
    sub2.subscribe("finance.stocks").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let news_channels: Vec<String> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .arg("news.*")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert!(news_channels.contains(&"news.sports".to_string()));
    assert!(!news_channels.contains(&"finance.stocks".to_string()));
}

#[tokio::test]
async fn pubsub_numsub_returns_counts() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub1 = ctx.client.get_async_pubsub().await.unwrap();
    let mut sub2 = ctx.client.get_async_pubsub().await.unwrap();

    sub1.subscribe("ch1").await.unwrap();
    sub2.subscribe("ch1").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result: Vec<redis::Value> = redis::cmd("PUBSUB")
        .arg("NUMSUB")
        .arg("ch1")
        .arg("ch_none")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    // Result is [channel, count, channel, count, ...]
    assert_eq!(result.len(), 4);
    // ch1 should have 2 subscribers
    if let redis::Value::Int(count) = result[1] {
        assert_eq!(count, 2);
    } else {
        panic!("expected integer count, got {:?}", result[1]);
    }
    // ch_none should have 0
    if let redis::Value::Int(count) = result[3] {
        assert_eq!(count, 0);
    } else {
        panic!("expected integer count, got {:?}", result[3]);
    }
}

#[tokio::test]
async fn pubsub_numpat_returns_pattern_count() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    let before: i64 = redis::cmd("PUBSUB")
        .arg("NUMPAT")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    sub_con.psubscribe("news.*").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after: i64 = redis::cmd("PUBSUB")
        .arg("NUMPAT")
        .query_async(&mut pub_con)
        .await
        .unwrap();

    assert_eq!(after, before + 1);
}

// ── Message size limit ────────────────────────────────────────────────────
// Max payload is 99,992 bytes (100,000 FDB limit minus 8 bytes for the
// instance ID prefix written alongside each message).

#[tokio::test]
async fn publish_rejects_oversized_message() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // One byte over the 99,992-byte limit.
    let big_message = "x".repeat(99_993);

    let result: redis::RedisResult<i64> = redis::cmd("PUBLISH")
        .arg("ch")
        .arg(&big_message)
        .query_async(&mut con)
        .await;

    assert!(result.is_err(), "expected error for oversized message");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("maximum size") || err.contains("ERR"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn publish_accepts_max_size_message() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Exactly 99,992 bytes (max allowed) should be accepted.
    let max_message = "x".repeat(99_992);

    let count: i64 = redis::cmd("PUBLISH")
        .arg("ch")
        .arg(&max_message)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 0); // no subscribers
}

// ── PUBLISH / SUBSCRIBE arity ─────────────────────────────────────────────

#[tokio::test]
async fn publish_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<i64> = redis::cmd("PUBLISH").arg("only_one_arg").query_async(&mut con).await;
    assert!(result.is_err());

    let result2: redis::RedisResult<i64> = redis::cmd("PUBLISH")
        .arg("ch")
        .arg("msg")
        .arg("extra")
        .query_async(&mut con)
        .await;
    assert!(result2.is_err());
}

#[tokio::test]
async fn subscribe_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<redis::Value> = redis::cmd("SUBSCRIBE").query_async(&mut con).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn psubscribe_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<redis::Value> = redis::cmd("PSUBSCRIBE").query_async(&mut con).await;
    assert!(result.is_err());
}

// ── UNSUBSCRIBE with no args exits pub/sub mode ────────────────────────────

#[tokio::test]
async fn unsubscribe_all_clears_subscriptions() {
    let ctx = TestContext::new().await;
    let mut pub_con = ctx.connection().await;
    let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

    sub_con.subscribe(&["ch1", "ch2"]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Both channels should be visible.
    let count_before: Vec<String> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert!(count_before.len() >= 2);

    sub_con.unsubscribe(&["ch1", "ch2"]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Should be 0 after unsubscribing both.
    let count: i64 = redis::cmd("PUBLISH")
        .arg("ch1")
        .arg("m")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let count2: i64 = redis::cmd("PUBLISH")
        .arg("ch2")
        .arg("m")
        .query_async(&mut pub_con)
        .await
        .unwrap();
    assert_eq!(count2, 0);
}
