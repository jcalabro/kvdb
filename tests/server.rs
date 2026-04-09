// tests/server.rs
//! Integration tests for the TCP server shell (Milestone 2).
//!
//! These test the full connection lifecycle: TCP connect, RESP parse,
//! command dispatch, RESP encode, TCP write. Each test spins up an
//! isolated server via TestContext.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

#[tokio::test]
async fn ping_returns_pong() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(result, "PONG");
}

#[tokio::test]
async fn echo_returns_argument() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: String = redis::cmd("ECHO")
        .arg("hello world")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "hello world");
}

#[tokio::test]
async fn pipelining_executes_all_commands() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let mut pipe = redis::pipe();
    for _ in 0..100 {
        pipe.cmd("PING");
    }
    let results: Vec<String> = pipe.query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 100);
    for result in results {
        assert_eq!(result, "PONG");
    }
}

#[tokio::test]
async fn unknown_command_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("FAKECMD").query_async(&mut con).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let detail = err.detail().unwrap_or("");
    assert!(
        detail.to_lowercase().contains("unknown command"),
        "Expected error to contain 'unknown command', got: {}",
        detail
    );
}

#[tokio::test]
async fn ping_with_message_returns_message() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: String = redis::cmd("PING")
        .arg("custom message")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "custom message");
}

#[tokio::test]
async fn multiple_connections_independent() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    // Send PING on first connection
    let result1: String = redis::cmd("PING").query_async(&mut con1).await.unwrap();
    assert_eq!(result1, "PONG");

    // Send ECHO on second connection
    let result2: String = redis::cmd("ECHO")
        .arg("connection2")
        .query_async(&mut con2)
        .await
        .unwrap();
    assert_eq!(result2, "connection2");

    // Verify first connection still works independently
    let result3: String = redis::cmd("ECHO")
        .arg("connection1")
        .query_async(&mut con1)
        .await
        .unwrap();
    assert_eq!(result3, "connection1");
}
