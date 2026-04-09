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
