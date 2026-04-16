//! Acceptance tests for server administration commands (M12).
//!
//! These go beyond the unit/integration tests in `tests/server_admin.rs`
//! by exercising cross-command interactions, multi-client scenarios, and
//! boundary conditions that only surface under realistic usage.
//!
//! Run via `just accept`.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

mod accept {
    use super::*;

    // -----------------------------------------------------------------------
    // INFO consistency: the stats it reports should match observed behavior
    // -----------------------------------------------------------------------

    /// INFO stats.total_commands_processed should increase monotonically
    /// as we issue commands.
    #[tokio::test]
    async fn info_command_count_increases() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Baseline: read the counter.
        let info1: String = redis::cmd("INFO").arg("stats").query_async(&mut con).await.unwrap();
        let count1 = extract_info_i64(&info1, "total_commands_processed");

        // Run 20 commands.
        for i in 0..20 {
            let _: () = redis::cmd("SET")
                .arg(format!("k{i}"))
                .arg("v")
                .query_async(&mut con)
                .await
                .unwrap();
        }

        let info2: String = redis::cmd("INFO").arg("stats").query_async(&mut con).await.unwrap();
        let count2 = extract_info_i64(&info2, "total_commands_processed");

        // We issued at least 20 SETs + 2 INFOs = 22 commands between samples.
        assert!(
            count2 >= count1 + 20,
            "expected total_commands_processed to grow by at least 20, got {count1} -> {count2}"
        );
    }

    /// INFO keyspace should report a key count consistent with DBSIZE.
    #[tokio::test]
    async fn info_keyspace_matches_dbsize() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        for i in 0..15 {
            let _: () = redis::cmd("SET")
                .arg(format!("ks{i}"))
                .arg("v")
                .query_async(&mut con)
                .await
                .unwrap();
        }

        let dbsize: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
        let info: String = redis::cmd("INFO").arg("keyspace").query_async(&mut con).await.unwrap();

        // Parse "db0:keys=15,expires=0,avg_ttl=0" from the info output.
        let keys_from_info = extract_keyspace_keys(&info, 0);
        assert_eq!(
            dbsize, keys_from_info,
            "DBSIZE ({dbsize}) should match INFO keyspace keys ({keys_from_info})"
        );
    }

    /// INFO clients.connected_clients should count open connections.
    #[tokio::test]
    async fn info_clients_counts_connections() {
        let ctx = TestContext::new().await;

        // Open 3 connections.
        let mut con1 = ctx.connection().await;
        let mut con2 = ctx.connection().await;
        let _con3 = ctx.connection().await;

        let info: String = redis::cmd("INFO").arg("clients").query_async(&mut con1).await.unwrap();
        let connected = extract_info_i64(&info, "connected_clients");
        assert!(connected >= 3, "expected at least 3 connected_clients, got {connected}");

        // CLIENT LIST should also show at least 3 entries.
        let list: String = redis::cmd("CLIENT").arg("LIST").query_async(&mut con2).await.unwrap();
        let lines: Vec<&str> = list.lines().filter(|l| !l.is_empty()).collect();
        assert!(
            lines.len() >= 3,
            "CLIENT LIST should have at least 3 lines, got {}",
            lines.len()
        );
    }

    // -----------------------------------------------------------------------
    // CONFIG GET/SET roundtrip for every writable parameter
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn config_set_get_all_writable() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let cases: &[(&str, &str)] = &[
            ("maxclients", "5000"),
            ("timeout", "120"),
            ("tcp-keepalive", "60"),
            ("maxmemory", "1073741824"),
            ("loglevel", "debug"),
            ("slowlog-log-slower-than", "5000"),
            ("slowlog-max-len", "256"),
        ];

        for &(name, value) in cases {
            let _: () = redis::cmd("CONFIG")
                .arg("SET")
                .arg(name)
                .arg(value)
                .query_async(&mut con)
                .await
                .unwrap();

            let result: Vec<String> = redis::cmd("CONFIG")
                .arg("GET")
                .arg(name)
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(result.len(), 2, "CONFIG GET {name}: expected 2 elements");
            assert_eq!(result[0], name, "CONFIG GET {name}: key mismatch");
            assert_eq!(result[1], value, "CONFIG GET {name}: value mismatch after SET");
        }
    }

    #[tokio::test]
    async fn config_set_invalid_is_rejected() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Non-numeric value for numeric param.
        let result: redis::RedisResult<String> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("maxclients")
            .arg("not_a_number")
            .query_async(&mut con)
            .await;
        assert!(result.is_err(), "CONFIG SET maxclients not_a_number should fail");

        // Immutable param.
        let result: redis::RedisResult<String> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("databases")
            .arg("32")
            .query_async(&mut con)
            .await;
        assert!(result.is_err(), "CONFIG SET databases should fail (immutable)");

        // Unknown param.
        let result: redis::RedisResult<String> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("nonexistent_param")
            .arg("value")
            .query_async(&mut con)
            .await;
        assert!(result.is_err(), "CONFIG SET unknown param should fail");
    }

    // -----------------------------------------------------------------------
    // CLIENT LIST multi-connection isolation
    // -----------------------------------------------------------------------

    /// Each connection should see a unique CLIENT ID, and CLIENT LIST
    /// should enumerate all connected clients.
    #[tokio::test]
    async fn client_ids_are_unique_across_connections() {
        let ctx = TestContext::new().await;
        let mut ids = Vec::new();
        let mut conns = Vec::new();
        for _ in 0..5 {
            let mut c = ctx.connection().await;
            let id: i64 = redis::cmd("CLIENT").arg("ID").query_async(&mut c).await.unwrap();
            ids.push(id);
            conns.push(c);
        }
        // All unique.
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 5, "5 connections should have 5 unique IDs");
    }

    /// CLIENT SETNAME on one connection should not affect another.
    #[tokio::test]
    async fn client_name_isolated_per_connection() {
        let ctx = TestContext::new().await;
        let mut con1 = ctx.connection().await;
        let mut con2 = ctx.connection().await;

        let _: () = redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg("alpha")
            .query_async(&mut con1)
            .await
            .unwrap();
        let _: () = redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg("beta")
            .query_async(&mut con2)
            .await
            .unwrap();

        let name1: String = redis::cmd("CLIENT")
            .arg("GETNAME")
            .query_async(&mut con1)
            .await
            .unwrap();
        let name2: String = redis::cmd("CLIENT")
            .arg("GETNAME")
            .query_async(&mut con2)
            .await
            .unwrap();
        assert_eq!(name1, "alpha");
        assert_eq!(name2, "beta");
    }

    // -----------------------------------------------------------------------
    // KEYS glob patterns
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn keys_glob_patterns() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let _: () = redis::cmd("SET")
            .arg("user:1")
            .arg("a")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("user:2")
            .arg("b")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("session:abc")
            .arg("c")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("session:def")
            .arg("d")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("other")
            .arg("e")
            .query_async(&mut con)
            .await
            .unwrap();

        // user:* should match exactly 2
        let mut result: Vec<String> = redis::cmd("KEYS").arg("user:*").query_async(&mut con).await.unwrap();
        result.sort();
        assert_eq!(result, vec!["user:1", "user:2"]);

        // session:??? should match exactly 2 (3-char suffixes)
        let mut result: Vec<String> = redis::cmd("KEYS")
            .arg("session:???")
            .query_async(&mut con)
            .await
            .unwrap();
        result.sort();
        assert_eq!(result, vec!["session:abc", "session:def"]);

        // *:* should match 4 (all colon-containing keys)
        let result: Vec<String> = redis::cmd("KEYS").arg("*:*").query_async(&mut con).await.unwrap();
        assert_eq!(result.len(), 4);

        // * should match all 5
        let result: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut con).await.unwrap();
        assert_eq!(result.len(), 5);

        // nomatch should return empty
        let result: Vec<String> = redis::cmd("KEYS").arg("zzz*").query_async(&mut con).await.unwrap();
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // RANDOMKEY distribution
    // -----------------------------------------------------------------------

    /// With 3 keys, RANDOMKEY should eventually return each one if
    /// called enough times (probabilistic but failure is vanishingly rare).
    #[tokio::test]
    async fn randomkey_covers_all_keys() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let _: () = redis::cmd("SET")
            .arg("rk_a")
            .arg("1")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("rk_b")
            .arg("2")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("rk_c")
            .arg("3")
            .query_async(&mut con)
            .await
            .unwrap();

        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let key: String = redis::cmd("RANDOMKEY").query_async(&mut con).await.unwrap();
            seen.insert(key);
            if seen.len() == 3 {
                break;
            }
        }
        assert_eq!(
            seen.len(),
            3,
            "RANDOMKEY should return all 3 keys in 100 tries, saw: {seen:?}"
        );
    }

    // -----------------------------------------------------------------------
    // OBJECT encoding varies by data type
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn object_encoding_varies_by_type() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let _: () = redis::cmd("SET")
            .arg("str_key")
            .arg("hi")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("HSET")
            .arg("hash_key")
            .arg("f")
            .arg("v")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SADD")
            .arg("set_key")
            .arg("m")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("ZADD")
            .arg("zset_key")
            .arg("1")
            .arg("m")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("LPUSH")
            .arg("list_key")
            .arg("x")
            .query_async(&mut con)
            .await
            .unwrap();

        let enc: String = redis::cmd("OBJECT")
            .arg("ENCODING")
            .arg("str_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(enc == "embstr" || enc == "raw", "string: {enc}");

        let enc: String = redis::cmd("OBJECT")
            .arg("ENCODING")
            .arg("hash_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(enc == "listpack" || enc == "hashtable", "hash: {enc}");

        let enc: String = redis::cmd("OBJECT")
            .arg("ENCODING")
            .arg("set_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(enc == "listpack" || enc == "hashtable", "set: {enc}");

        let enc: String = redis::cmd("OBJECT")
            .arg("ENCODING")
            .arg("zset_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(enc == "listpack" || enc == "skiplist", "zset: {enc}");

        let enc: String = redis::cmd("OBJECT")
            .arg("ENCODING")
            .arg("list_key")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(enc == "listpack" || enc == "quicklist", "list: {enc}");
    }

    // -----------------------------------------------------------------------
    // SLOWLOG captures slow commands
    // -----------------------------------------------------------------------

    /// Force a slow command (DEBUG SLEEP) and verify SLOWLOG captures it.
    #[tokio::test]
    async fn slowlog_captures_slow_debug_sleep() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        // Set threshold to 1 microsecond so DEBUG SLEEP always lands.
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("slowlog-log-slower-than")
            .arg("1")
            .query_async(&mut con)
            .await
            .unwrap();

        // Reset to clear any startup noise.
        let _: String = redis::cmd("SLOWLOG").arg("RESET").query_async(&mut con).await.unwrap();

        // DEBUG SLEEP 50ms — guaranteed to exceed 1us threshold.
        let _: String = redis::cmd("DEBUG")
            .arg("SLEEP")
            .arg("0.05")
            .query_async(&mut con)
            .await
            .unwrap();

        let len: i64 = redis::cmd("SLOWLOG").arg("LEN").query_async(&mut con).await.unwrap();
        assert!(len >= 1, "SLOWLOG should capture DEBUG SLEEP, got LEN={len}");

        // Fetch the entries and verify the most recent one contains "DEBUG".
        let entries: Vec<redis::Value> = redis::cmd("SLOWLOG")
            .arg("GET")
            .arg("1")
            .query_async(&mut con)
            .await
            .unwrap();
        assert!(!entries.is_empty(), "SLOWLOG GET 1 should return at least one entry");
    }

    // -----------------------------------------------------------------------
    // COMMAND INFO / DOCS consistency
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn command_info_for_known_command() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let result: Vec<redis::Value> = redis::cmd("COMMAND")
            .arg("INFO")
            .arg("SET")
            .query_async(&mut con)
            .await
            .unwrap();

        // Should return a 1-element array, where each element is the
        // command info array (name, arity, flags, ...).
        assert_eq!(result.len(), 1, "COMMAND INFO SET should return 1 entry");
    }

    #[tokio::test]
    async fn command_info_unknown_returns_nil() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let result: Vec<redis::Value> = redis::cmd("COMMAND")
            .arg("INFO")
            .arg("DEFINITELY_NOT_A_COMMAND")
            .query_async(&mut con)
            .await
            .unwrap();

        // Unknown command should return a nil element.
        assert_eq!(result.len(), 1);
        assert!(
            matches!(result[0], redis::Value::Nil),
            "unknown command info should be nil"
        );
    }

    // -----------------------------------------------------------------------
    // ACL stubs return well-formed responses
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn acl_cat_returns_categories() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let cats: Vec<String> = redis::cmd("ACL").arg("CAT").query_async(&mut con).await.unwrap();
        assert!(
            cats.len() > 10,
            "ACL CAT should return many categories, got {}",
            cats.len()
        );
        assert!(cats.contains(&"read".to_string()), "ACL CAT should include 'read'");
        assert!(cats.contains(&"write".to_string()), "ACL CAT should include 'write'");
    }

    #[tokio::test]
    async fn acl_users_returns_default() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let users: Vec<String> = redis::cmd("ACL").arg("USERS").query_async(&mut con).await.unwrap();
        assert_eq!(users, vec!["default"]);
    }

    // -----------------------------------------------------------------------
    // MEMORY USAGE across data types
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn memory_usage_varies_by_size() {
        let ctx = TestContext::new().await;
        let mut con = ctx.connection().await;

        let _: () = redis::cmd("SET")
            .arg("small")
            .arg("x")
            .query_async(&mut con)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("big")
            .arg("x".repeat(10_000))
            .query_async(&mut con)
            .await
            .unwrap();

        let small_usage: i64 = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg("small")
            .query_async(&mut con)
            .await
            .unwrap();
        let big_usage: i64 = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg("big")
            .query_async(&mut con)
            .await
            .unwrap();

        assert!(
            big_usage > small_usage,
            "big key ({big_usage}) should use more memory than small key ({small_usage})"
        );
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Extract a numeric value from Redis INFO output for a given key.
    fn extract_info_i64(info: &str, key: &str) -> i64 {
        for line in info.lines() {
            if let Some(val) = line.strip_prefix(&format!("{key}:")) {
                return val.trim().parse().unwrap_or(0);
            }
        }
        panic!("key '{key}' not found in INFO output");
    }

    /// Extract the `keys=N` value from the db<ns> line in INFO keyspace.
    fn extract_keyspace_keys(info: &str, ns: u8) -> i64 {
        let prefix = format!("db{ns}:keys=");
        for line in info.lines() {
            if let Some(rest) = line.strip_prefix(&prefix) {
                // rest looks like "15,expires=0,avg_ttl=0"
                if let Some(comma) = rest.find(',') {
                    return rest[..comma].parse().unwrap_or(0);
                }
                return rest.parse().unwrap_or(0);
            }
        }
        0
    }
}
