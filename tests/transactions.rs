// tests/transactions.rs
//! Integration tests for MULTI/EXEC/DISCARD/WATCH/UNWATCH (M10).
//!
//! Tests the full path: TCP connect -> RESP -> dispatch -> FDB -> response.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

// ---------------------------------------------------------------------------
// MULTI / EXEC basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_exec_basic() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // SET a key, then MULTI/EXEC to SET and GET within the transaction.
    let _: () = redis::cmd("SET")
        .arg("key1")
        .arg("before")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("key1")
        .arg("after")
        .query_async(&mut con)
        .await
        .unwrap(); // returns QUEUED
    let _: () = redis::cmd("GET").arg("key1").query_async(&mut con).await.unwrap(); // returns QUEUED

    // EXEC returns array of results.
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 2);

    // Verify the SET took effect.
    let val: String = redis::cmd("GET").arg("key1").query_async(&mut con).await.unwrap();
    assert_eq!(val, "after");
}

#[tokio::test]
async fn multi_exec_empty() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert!(results.is_empty(), "empty MULTI/EXEC should return empty array");
}

#[tokio::test]
async fn multi_exec_multiple_types() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("mk")
        .arg("mv")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("INCR").arg("counter").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("GET").arg("mk").query_async(&mut con).await.unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 3);

    // Verify INCR result.
    let counter: i64 = redis::cmd("GET").arg("counter").query_async(&mut con).await.unwrap();
    assert_eq!(counter, 1);
}

// ---------------------------------------------------------------------------
// DISCARD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discard_clears_queue() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("dk")
        .arg("original")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("dk")
        .arg("changed")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("DISCARD").query_async(&mut con).await.unwrap();

    // The SET inside the discarded MULTI should NOT have taken effect.
    let val: String = redis::cmd("GET").arg("dk").query_async(&mut con).await.unwrap();
    assert_eq!(val, "original");
}

#[tokio::test]
async fn discard_without_multi_is_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<()> = redis::cmd("DISCARD").query_async(&mut con).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("DISCARD without MULTI"),
        "expected DISCARD without MULTI, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Nested MULTI error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nested_multi_is_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let result: redis::RedisResult<()> = redis::cmd("MULTI").query_async(&mut con).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("MULTI calls can not be nested"),
        "expected nested MULTI error, got: {err_msg}"
    );

    // Should still be in MULTI mode -- DISCARD should work.
    let _: () = redis::cmd("DISCARD").query_async(&mut con).await.unwrap();
}

// ---------------------------------------------------------------------------
// EXEC without MULTI
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exec_without_multi_is_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("EXEC").query_async(&mut con).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("EXEC without MULTI"),
        "expected EXEC without MULTI, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Unknown command in MULTI sets error flag (EXECABORT)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_command_in_multi_causes_execabort() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();

    // Queue a valid command.
    let _: () = redis::cmd("SET").arg("x").arg("1").query_async(&mut con).await.unwrap();

    // Queue an unknown command -- should set the error flag.
    let result: redis::RedisResult<()> = redis::cmd("FAKECMD").arg("blah").query_async(&mut con).await;
    assert!(result.is_err());

    // EXEC should return EXECABORT.
    let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("EXEC").query_async(&mut con).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("EXECABORT") || err_msg.contains("ExecAbortError") || err_msg.contains("discarded"),
        "expected EXECABORT, got: {err_msg}"
    );

    // The SET should NOT have taken effect.
    let val: Option<String> = redis::cmd("GET").arg("x").query_async(&mut con).await.unwrap();
    assert_eq!(val, None);
}

// ---------------------------------------------------------------------------
// WRONGTYPE in MULTI does NOT abort other commands (no-rollback)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrongtype_in_multi_no_rollback() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Create a string key.
    let _: () = redis::cmd("SET")
        .arg("str_key")
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();

    // Command 1: valid SET.
    let _: () = redis::cmd("SET")
        .arg("new_key")
        .arg("value")
        .query_async(&mut con)
        .await
        .unwrap();

    // Command 2: LPUSH on a string key -- WRONGTYPE runtime error.
    let _: () = redis::cmd("LPUSH")
        .arg("str_key")
        .arg("oops")
        .query_async(&mut con)
        .await
        .unwrap();

    // Command 3: another valid SET.
    let _: () = redis::cmd("SET")
        .arg("another_key")
        .arg("val2")
        .query_async(&mut con)
        .await
        .unwrap();

    // EXEC returns the results. The redis-rs multiplexed connection
    // may surface the WRONGTYPE error from the result array as a client
    // error, so we accept either success or error from the EXEC call.
    // The key test is whether the non-erroring commands persisted.
    let _exec_result: redis::RedisResult<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await;

    // Commands 1 and 3 should have succeeded (no rollback).
    // Command 2 (LPUSH on string) should have failed but not prevented
    // the other commands from committing.
    let v1: String = redis::cmd("GET").arg("new_key").query_async(&mut con).await.unwrap();
    assert_eq!(
        v1, "value",
        "SET new_key should have committed despite WRONGTYPE in another command"
    );

    let v3: String = redis::cmd("GET")
        .arg("another_key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        v3, "val2",
        "SET another_key should have committed despite WRONGTYPE in another command"
    );

    // The original string key should be unchanged (LPUSH failed).
    let original: String = redis::cmd("GET").arg("str_key").query_async(&mut con).await.unwrap();
    assert_eq!(original, "hello", "LPUSH on string key should not have modified it");
}

// ---------------------------------------------------------------------------
// WATCH / UNWATCH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watch_no_conflict_succeeds() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("wk")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();

    // WATCH the key, then MULTI/EXEC without any other client modifying it.
    let _: () = redis::cmd("WATCH").arg("wk").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("wk")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 1, "EXEC should succeed with 1 result");

    let val: String = redis::cmd("GET").arg("wk").query_async(&mut con).await.unwrap();
    assert_eq!(val, "v2");
}

#[tokio::test]
async fn watch_conflict_aborts() {
    let ctx = TestContext::new().await;

    // Connection 1 watches a key.
    let mut con1 = ctx.connection().await;
    // Connection 2 will modify the watched key.
    let mut con2 = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("watched")
        .arg("original")
        .query_async(&mut con1)
        .await
        .unwrap();

    // Con1: WATCH + MULTI.
    let _: () = redis::cmd("WATCH").arg("watched").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("watched")
        .arg("con1_value")
        .query_async(&mut con1)
        .await
        .unwrap();

    // Con2: modify the watched key before con1 EXECs.
    let _: () = redis::cmd("SET")
        .arg("watched")
        .arg("con2_value")
        .query_async(&mut con2)
        .await
        .unwrap();

    // Con1: EXEC should return nil (transaction aborted).
    let result: Option<Vec<redis::Value>> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert!(
        result.is_none(),
        "EXEC should return nil when WATCH conflict detected, got: {result:?}"
    );

    // The key should have con2's value.
    let val: String = redis::cmd("GET").arg("watched").query_async(&mut con1).await.unwrap();
    assert_eq!(val, "con2_value");
}

#[tokio::test]
async fn unwatch_clears_watches() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("uwk")
        .arg("v1")
        .query_async(&mut con1)
        .await
        .unwrap();

    // WATCH then UNWATCH.
    let _: () = redis::cmd("WATCH").arg("uwk").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("UNWATCH").query_async(&mut con1).await.unwrap();

    // Now con2 modifies the key.
    let _: () = redis::cmd("SET")
        .arg("uwk")
        .arg("v2")
        .query_async(&mut con2)
        .await
        .unwrap();

    // MULTI/EXEC on con1 should succeed because we UNWATCHed.
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("uwk")
        .arg("v3")
        .query_async(&mut con1)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert_eq!(results.len(), 1, "EXEC should succeed after UNWATCH");

    let val: String = redis::cmd("GET").arg("uwk").query_async(&mut con1).await.unwrap();
    assert_eq!(val, "v3");
}

#[tokio::test]
async fn watch_inside_multi_is_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let result: redis::RedisResult<()> = redis::cmd("WATCH").arg("somekey").query_async(&mut con).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("WATCH inside MULTI is not allowed"),
        "expected WATCH inside MULTI error, got: {err_msg}"
    );

    // Clean up.
    let _: () = redis::cmd("DISCARD").query_async(&mut con).await.unwrap();
}

// ---------------------------------------------------------------------------
// WATCH clears after EXEC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watch_cleared_after_exec() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("wce")
        .arg("v1")
        .query_async(&mut con1)
        .await
        .unwrap();

    // First WATCH + MULTI/EXEC (should succeed).
    let _: () = redis::cmd("WATCH").arg("wce").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("wce")
        .arg("v2")
        .query_async(&mut con1)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert_eq!(results.len(), 1);

    // Now con2 modifies the key.
    let _: () = redis::cmd("SET")
        .arg("wce")
        .arg("v3")
        .query_async(&mut con2)
        .await
        .unwrap();

    // Second MULTI/EXEC WITHOUT re-WATCHing should succeed
    // (watches were cleared by the first EXEC).
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("wce")
        .arg("v4")
        .query_async(&mut con1)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert_eq!(results.len(), 1, "second EXEC should succeed without WATCH");

    let val: String = redis::cmd("GET").arg("wce").query_async(&mut con1).await.unwrap();
    assert_eq!(val, "v4");
}

// ---------------------------------------------------------------------------
// WATCH cleared after DISCARD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watch_cleared_after_discard() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("wcd")
        .arg("v1")
        .query_async(&mut con1)
        .await
        .unwrap();

    // WATCH + MULTI + DISCARD.
    let _: () = redis::cmd("WATCH").arg("wcd").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("DISCARD").query_async(&mut con1).await.unwrap();

    // Con2 modifies the key.
    let _: () = redis::cmd("SET")
        .arg("wcd")
        .arg("v2")
        .query_async(&mut con2)
        .await
        .unwrap();

    // MULTI/EXEC without re-WATCHing should succeed.
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("wcd")
        .arg("v3")
        .query_async(&mut con1)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert_eq!(results.len(), 1, "EXEC should succeed -- watches cleared by DISCARD");
}

// ---------------------------------------------------------------------------
// MULTI/EXEC with hash commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_exec_with_hashes() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("HSET")
        .arg("myhash")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("HGET")
        .arg("myhash")
        .arg("f1")
        .query_async(&mut con)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 3);

    // Verify the hash was created.
    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("f2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "v2");
}

// ---------------------------------------------------------------------------
// Multiple WATCH calls accumulate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_watch_calls_accumulate() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("wk1")
        .arg("a")
        .query_async(&mut con1)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("wk2")
        .arg("b")
        .query_async(&mut con1)
        .await
        .unwrap();

    // WATCH key1, then WATCH key2 (accumulates).
    let _: () = redis::cmd("WATCH").arg("wk1").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("WATCH").arg("wk2").query_async(&mut con1).await.unwrap();

    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("wk1")
        .arg("x")
        .query_async(&mut con1)
        .await
        .unwrap();

    // Con2 modifies key2 (the second watched key).
    let _: () = redis::cmd("SET")
        .arg("wk2")
        .arg("changed")
        .query_async(&mut con2)
        .await
        .unwrap();

    // EXEC should fail -- wk2 was watched and modified.
    let result: Option<Vec<redis::Value>> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert!(
        result.is_none(),
        "EXEC should return nil when any watched key is modified"
    );
}

// ---------------------------------------------------------------------------
// Read-your-writes inside MULTI/EXEC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_exec_read_your_writes() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();

    // Write a key and immediately read it back within the same MULTI.
    let _: () = redis::cmd("SET")
        .arg("ryw_key")
        .arg("written_in_multi")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("GET").arg("ryw_key").query_async(&mut con).await.unwrap();

    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 2);

    // The GET (second result) should see the SET from the first command.
    // results[0] is SET -> OK, results[1] is GET -> "written_in_multi"
    match &results[1] {
        redis::Value::BulkString(data) => {
            assert_eq!(data, b"written_in_multi", "GET inside MULTI should see preceding SET");
        }
        redis::Value::SimpleString(s) => {
            assert_eq!(s, "written_in_multi", "GET inside MULTI should see preceding SET");
        }
        other => panic!("expected bulk string from GET, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// WATCH on non-existent key that gets created
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watch_nonexistent_key_created() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    // WATCH a key that doesn't exist yet (meta_snapshot = None).
    let _: () = redis::cmd("WATCH")
        .arg("ghost_key")
        .query_async(&mut con1)
        .await
        .unwrap();

    // Con2 creates the key.
    let _: () = redis::cmd("SET")
        .arg("ghost_key")
        .arg("born")
        .query_async(&mut con2)
        .await
        .unwrap();

    // Con1: MULTI/EXEC should abort -- key went from None to Some.
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("ghost_key")
        .arg("con1_value")
        .query_async(&mut con1)
        .await
        .unwrap();
    let result: Option<Vec<redis::Value>> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert!(
        result.is_none(),
        "EXEC should abort when a WATCHed non-existent key is created"
    );

    // The key should have con2's value.
    let val: String = redis::cmd("GET").arg("ghost_key").query_async(&mut con1).await.unwrap();
    assert_eq!(val, "born");
}

// ---------------------------------------------------------------------------
// WATCH on existing key that gets deleted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watch_existing_key_deleted() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("doomed")
        .arg("alive")
        .query_async(&mut con1)
        .await
        .unwrap();

    // WATCH the key.
    let _: () = redis::cmd("WATCH").arg("doomed").query_async(&mut con1).await.unwrap();

    // Con2 deletes it.
    let deleted: i64 = redis::cmd("DEL").arg("doomed").query_async(&mut con2).await.unwrap();
    assert_eq!(deleted, 1);

    // Con1: MULTI/EXEC should abort -- key went from Some to None.
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("doomed")
        .arg("resurrected")
        .query_async(&mut con1)
        .await
        .unwrap();
    let result: Option<Vec<redis::Value>> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert!(result.is_none(), "EXEC should abort when a WATCHed key is deleted");

    // Key should still be gone.
    let val: Option<String> = redis::cmd("GET").arg("doomed").query_async(&mut con1).await.unwrap();
    assert_eq!(val, None);
}

// ---------------------------------------------------------------------------
// Recovery after failed EXEC -- client can immediately start new MULTI/EXEC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn recovery_after_failed_exec() {
    let ctx = TestContext::new().await;
    let mut con1 = ctx.connection().await;
    let mut con2 = ctx.connection().await;

    let _: () = redis::cmd("SET")
        .arg("rec_key")
        .arg("v1")
        .query_async(&mut con1)
        .await
        .unwrap();

    // First attempt: WATCH + MULTI/EXEC that gets aborted.
    let _: () = redis::cmd("WATCH").arg("rec_key").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("rec_key")
        .arg("attempt1")
        .query_async(&mut con1)
        .await
        .unwrap();

    // Con2 modifies it.
    let _: () = redis::cmd("SET")
        .arg("rec_key")
        .arg("con2")
        .query_async(&mut con2)
        .await
        .unwrap();

    let result: Option<Vec<redis::Value>> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert!(result.is_none(), "first EXEC should abort");

    // Second attempt: should succeed -- state is clean after failed EXEC.
    let _: () = redis::cmd("MULTI").query_async(&mut con1).await.unwrap();
    let _: () = redis::cmd("SET")
        .arg("rec_key")
        .arg("attempt2")
        .query_async(&mut con1)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con1).await.unwrap();
    assert_eq!(results.len(), 1, "second EXEC should succeed");

    let val: String = redis::cmd("GET").arg("rec_key").query_async(&mut con1).await.unwrap();
    assert_eq!(val, "attempt2");
}

// ---------------------------------------------------------------------------
// MULTI/EXEC with list commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_exec_with_lists() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("LLEN").arg("mylist").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("LPOP").arg("mylist").query_async(&mut con).await.unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 3);

    // After EXEC: list should have ["b", "c"].
    let remaining: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(remaining, vec!["b", "c"]);
}

// ---------------------------------------------------------------------------
// MULTI/EXEC with set commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_exec_with_sets() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SADD")
        .arg("myset")
        .arg("x")
        .arg("y")
        .arg("z")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SREM")
        .arg("myset")
        .arg("y")
        .query_async(&mut con)
        .await
        .unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 3);

    // After EXEC: set should have {x, z}.
    let card: i64 = redis::cmd("SCARD").arg("myset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);

    let is_member: bool = redis::cmd("SISMEMBER")
        .arg("myset")
        .arg("y")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(!is_member, "y should have been removed inside MULTI");
}

// ---------------------------------------------------------------------------
// MULTI/EXEC with sorted set commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_exec_with_sorted_sets() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    let _: () = redis::cmd("MULTI").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0)
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("ZADD")
        .arg("myzset")
        .arg(2.0)
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
    assert_eq!(results.len(), 3);

    // After EXEC: sorted set should have 2 members.
    let card: i64 = redis::cmd("ZCARD").arg("myzset").query_async(&mut con).await.unwrap();
    assert_eq!(card, 2);

    let score: f64 = redis::cmd("ZSCORE")
        .arg("myzset")
        .arg("bob")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!((score - 2.0).abs() < f64::EPSILON);
}
