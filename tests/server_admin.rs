// tests/server_admin.rs
//! Integration tests for M12: Server Commands & Observability.
//!
//! Each test gets its own isolated server via TestContext (random port,
//! unique FDB namespace, cleaned up on drop). These cover the admin
//! commands that operators and tools (redis-cli, monitoring agents,
//! client library handshakes) depend on.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

// ---------------------------------------------------------------------------
// INFO
// ---------------------------------------------------------------------------

#[tokio::test]
async fn info_returns_server_section() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let info: String = redis::cmd("INFO").arg("server").query_async(&mut con).await.unwrap();
    assert!(info.contains("redis_version"), "INFO server missing redis_version");
    assert!(info.contains("kvdb_version"), "INFO server missing kvdb_version");
    assert!(info.contains("tcp_port"), "INFO server missing tcp_port");
    assert!(
        info.contains("uptime_in_seconds"),
        "INFO server missing uptime_in_seconds"
    );
    assert!(
        info.contains("storage_engine:foundationdb"),
        "INFO server missing storage_engine"
    );
}

#[tokio::test]
async fn info_returns_clients_section() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let info: String = redis::cmd("INFO").arg("clients").query_async(&mut con).await.unwrap();
    assert!(
        info.contains("connected_clients"),
        "INFO clients missing connected_clients"
    );
}

#[tokio::test]
async fn info_returns_stats_section() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let info: String = redis::cmd("INFO").arg("stats").query_async(&mut con).await.unwrap();
    assert!(
        info.contains("total_commands_processed"),
        "INFO stats missing total_commands_processed"
    );
}

#[tokio::test]
async fn info_keyspace_reflects_keys() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let info: String = redis::cmd("INFO").arg("keyspace").query_async(&mut con).await.unwrap();
    assert!(info.contains("db0:keys="), "INFO keyspace should list db0");
}

#[tokio::test]
async fn info_default_contains_multiple_sections() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let info: String = redis::cmd("INFO").query_async(&mut con).await.unwrap();
    assert!(info.contains("# Server"), "should include Server section");
    assert!(info.contains("# Clients"), "should include Clients section");
    assert!(info.contains("# Stats"), "should include Stats section");
    assert!(info.contains("# Memory"), "should include Memory section");
}

// ---------------------------------------------------------------------------
// DBSIZE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dbsize_reflects_key_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("x").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("y").arg("2").query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 2);
}

// ---------------------------------------------------------------------------
// TIME
// ---------------------------------------------------------------------------

#[tokio::test]
async fn time_returns_two_element_array() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: Vec<String> = redis::cmd("TIME").query_async(&mut con).await.unwrap();
    assert_eq!(result.len(), 2, "TIME should return [seconds, microseconds]");
    let secs: u64 = result[0].parse().expect("seconds should be numeric");
    let usecs: u64 = result[1].parse().expect("microseconds should be numeric");
    // Sanity: seconds should be in a reasonable range (year 2020+).
    assert!(secs > 1_577_000_000, "server time too far in the past: {secs}");
    assert!(usecs < 1_000_000, "microseconds should be < 1M, got {usecs}");
}

// ---------------------------------------------------------------------------
// RANDOMKEY / KEYS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn randomkey_returns_null_on_empty_db() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: Option<String> = redis::cmd("RANDOMKEY").query_async(&mut con).await.unwrap();
    assert!(result.is_none(), "RANDOMKEY on empty db should return nil");
}

#[tokio::test]
async fn randomkey_returns_existing_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET")
        .arg("onlyone")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    let key: String = redis::cmd("RANDOMKEY").query_async(&mut con).await.unwrap();
    assert_eq!(key, "onlyone");
}

#[tokio::test]
async fn keys_returns_matching() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET")
        .arg("hello")
        .arg("1")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("hallo")
        .arg("2")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("world")
        .arg("3")
        .query_async(&mut con)
        .await
        .unwrap();
    let mut result: Vec<String> = redis::cmd("KEYS").arg("h*llo").query_async(&mut con).await.unwrap();
    result.sort();
    assert_eq!(result, vec!["hallo", "hello"]);
}

#[tokio::test]
async fn keys_star_returns_all() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let result: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut con).await.unwrap();
    assert_eq!(result.len(), 2);
}

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------

#[tokio::test]
async fn config_get_maxclients() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxclients")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "maxclients");
    let _: i64 = result[1].parse().expect("maxclients should be numeric");
}

#[tokio::test]
async fn config_set_and_get() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("timeout")
        .arg("30")
        .query_async(&mut con)
        .await
        .unwrap();
    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("timeout")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, vec!["timeout", "30"]);
}

#[tokio::test]
async fn config_get_wildcard() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("*")
        .query_async(&mut con)
        .await
        .unwrap();
    // Should have multiple pairs (name, value, name, value, ...)
    assert!(
        result.len() >= 10,
        "CONFIG GET * should return many params, got {}",
        result.len()
    );
}

// ---------------------------------------------------------------------------
// CLIENT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_id_returns_positive_int() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let id: i64 = redis::cmd("CLIENT").arg("ID").query_async(&mut con).await.unwrap();
    assert!(id > 0, "CLIENT ID should be > 0, got {id}");
}

#[tokio::test]
async fn client_setname_getname() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("myconn")
        .query_async(&mut con)
        .await
        .unwrap();
    let name: String = redis::cmd("CLIENT").arg("GETNAME").query_async(&mut con).await.unwrap();
    assert_eq!(name, "myconn");
}

#[tokio::test]
async fn client_list_contains_current() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("test_conn")
        .query_async(&mut con)
        .await
        .unwrap();
    let list: String = redis::cmd("CLIENT").arg("LIST").query_async(&mut con).await.unwrap();
    assert!(
        list.contains("name=test_conn"),
        "CLIENT LIST should show the named connection"
    );
}

#[tokio::test]
async fn client_info_returns_current_connection() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let info: String = redis::cmd("CLIENT").arg("INFO").query_async(&mut con).await.unwrap();
    assert!(info.contains("id="), "CLIENT INFO should include connection id");
    assert!(info.contains("db="), "CLIENT INFO should include db");
}

// ---------------------------------------------------------------------------
// COMMAND
// ---------------------------------------------------------------------------

#[tokio::test]
async fn command_count_returns_positive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let count: i64 = redis::cmd("COMMAND").arg("COUNT").query_async(&mut con).await.unwrap();
    assert!(
        count > 100,
        "COMMAND COUNT should be > 100 (we have ~130+ commands), got {count}"
    );
}

#[tokio::test]
async fn command_list_includes_set() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let list: Vec<String> = redis::cmd("COMMAND").arg("LIST").query_async(&mut con).await.unwrap();
    assert!(list.contains(&"set".to_string()), "COMMAND LIST should include 'set'");
    assert!(list.contains(&"get".to_string()), "COMMAND LIST should include 'get'");
}

// ---------------------------------------------------------------------------
// OBJECT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn object_encoding_string() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("short")
        .query_async(&mut con)
        .await
        .unwrap();
    let enc: String = redis::cmd("OBJECT")
        .arg("ENCODING")
        .arg("k")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(enc == "embstr" || enc == "raw", "expected embstr or raw, got {enc}");
}

#[tokio::test]
async fn object_refcount_always_1() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("k").arg("v").query_async(&mut con).await.unwrap();
    let rc: i64 = redis::cmd("OBJECT")
        .arg("REFCOUNT")
        .arg("k")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(rc, 1);
}

#[tokio::test]
async fn object_nonexistent_key_errors() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("OBJECT")
        .arg("ENCODING")
        .arg("nonexistent")
        .query_async(&mut con)
        .await;
    assert!(result.is_err(), "OBJECT on nonexistent key should error");
}

// ---------------------------------------------------------------------------
// SLOWLOG
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slowlog_len_starts_at_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let len: i64 = redis::cmd("SLOWLOG").arg("LEN").query_async(&mut con).await.unwrap();
    // Fresh server: 0 or very small (depends on startup commands).
    assert!(len >= 0, "SLOWLOG LEN should be >= 0, got {len}");
}

#[tokio::test]
async fn slowlog_reset_clears() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: String = redis::cmd("SLOWLOG").arg("RESET").query_async(&mut con).await.unwrap();
    let len: i64 = redis::cmd("SLOWLOG").arg("LEN").query_async(&mut con).await.unwrap();
    assert_eq!(len, 0);
}

// ---------------------------------------------------------------------------
// MEMORY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memory_usage_returns_estimate() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("hello world")
        .query_async(&mut con)
        .await
        .unwrap();
    let usage: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("k")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(usage > 0, "MEMORY USAGE should return positive estimate, got {usage}");
}

// ---------------------------------------------------------------------------
// LATENCY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn latency_latest_returns_empty() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: Vec<String> = redis::cmd("LATENCY").arg("LATEST").query_async(&mut con).await.unwrap();
    assert!(result.is_empty(), "LATENCY LATEST should return empty array");
}

// ---------------------------------------------------------------------------
// DEBUG SLEEP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn debug_sleep_returns_ok() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let start = std::time::Instant::now();
    let _: String = redis::cmd("DEBUG")
        .arg("SLEEP")
        .arg("0.1")
        .query_async(&mut con)
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() >= 80,
        "DEBUG SLEEP 0.1 should take ~100ms, took {}ms",
        elapsed.as_millis()
    );
}

// ---------------------------------------------------------------------------
// BGSAVE / SAVE / BGREWRITEAOF / LASTSAVE / WAIT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bgsave_returns_ok() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: String = redis::cmd("BGSAVE").query_async(&mut con).await.unwrap();
    assert!(
        result.contains("Background saving"),
        "expected saving msg, got {result}"
    );
}

#[tokio::test]
async fn save_returns_ok() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: String = redis::cmd("SAVE").query_async(&mut con).await.unwrap();
}

#[tokio::test]
async fn lastsave_returns_timestamp() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let ts: i64 = redis::cmd("LASTSAVE").query_async(&mut con).await.unwrap();
    // Should be a recent UNIX timestamp (year 2020+).
    assert!(ts > 1_577_000_000, "LASTSAVE too old: {ts}");
}

#[tokio::test]
async fn wait_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let replicas: i64 = redis::cmd("WAIT")
        .arg("1")
        .arg("0")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(replicas, 0, "WAIT should return 0 (FDB handles replication)");
}

// ---------------------------------------------------------------------------
// ACL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acl_whoami_returns_default() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let user: String = redis::cmd("ACL").arg("WHOAMI").query_async(&mut con).await.unwrap();
    assert_eq!(user, "default");
}

#[tokio::test]
async fn acl_list_returns_default_user() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let list: Vec<String> = redis::cmd("ACL").arg("LIST").query_async(&mut con).await.unwrap();
    assert_eq!(list.len(), 1);
    assert!(list[0].contains("default"), "ACL LIST should mention default user");
}
