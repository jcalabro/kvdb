//! Smoke test client for kvdb.
//!
//! Connects to a running kvdb server and exercises every implemented
//! command, printing pass/fail results. Exits with code 1 if any
//! check fails.
//!
//! Usage:
//!   cargo run --example smoke                       # default 127.0.0.1:6379
//!   cargo run --example smoke -- 127.0.0.1:6399     # custom address
//!   just smoke                                      # starts server, runs smoke, stops server

use std::process::ExitCode;

struct SmokeTest {
    passed: usize,
    failed: usize,
}

impl SmokeTest {
    fn new() -> Self {
        Self { passed: 0, failed: 0 }
    }

    fn pass(&mut self, name: &str) {
        self.passed += 1;
        println!("  PASS  {name}");
    }

    fn fail(&mut self, name: &str, detail: &str) {
        self.failed += 1;
        println!("  FAIL  {name}: {detail}");
    }

    /// Check that a result matches an expected value.
    fn check_eq<T: PartialEq + std::fmt::Debug>(
        &mut self,
        name: &str,
        got: Result<T, impl std::fmt::Debug>,
        expected: T,
    ) {
        match got {
            Ok(val) if val == expected => self.pass(name),
            Ok(val) => self.fail(name, &format!("expected {expected:?}, got {val:?}")),
            Err(e) => self.fail(name, &format!("error: {e:?}")),
        }
    }

    /// Check that a result is an error.
    fn check_err<T: std::fmt::Debug>(&mut self, name: &str, got: Result<T, impl std::fmt::Debug>) {
        match got {
            Err(_) => self.pass(name),
            Ok(val) => self.fail(name, &format!("expected error, got {val:?}")),
        }
    }

    fn summary(&self) -> bool {
        let total = self.passed + self.failed;
        println!();
        if self.failed == 0 {
            println!("All {total} checks passed.");
            true
        } else {
            println!("{} passed, {} FAILED (out of {total})", self.passed, self.failed);
            false
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:6379".to_string());

    println!("kvdb smoke test against {addr}");
    println!();

    let client = match redis::Client::open(format!("redis://{addr}")) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to create client: {e}");
            return ExitCode::FAILURE;
        }
    };

    let mut con = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to {addr}: {e}");
            eprintln!("Is the server running? Start it with: just run --bind-addr {addr}");
            return ExitCode::FAILURE;
        }
    };

    let mut t = SmokeTest::new();

    // ── PING ───────────────────────────────────────────────────
    println!("PING");

    let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
    t.check_eq("PING returns PONG", result, "PONG".to_string());

    let result: redis::RedisResult<String> = redis::cmd("PING").arg("hello").query_async(&mut con).await;
    t.check_eq("PING with message", result, "hello".to_string());

    // ── ECHO ───────────────────────────────────────────────────
    println!("ECHO");

    let result: redis::RedisResult<String> = redis::cmd("ECHO").arg("smoke test").query_async(&mut con).await;
    t.check_eq("ECHO returns argument", result, "smoke test".to_string());

    let result: redis::RedisResult<String> = redis::cmd("ECHO")
        .arg("binary\x00safe\r\ndata")
        .query_async(&mut con)
        .await;
    t.check_eq("ECHO binary-safe", result, "binary\x00safe\r\ndata".to_string());

    // ── Pipelining ─────────────────────────────────────────────
    println!("Pipelining");

    let mut pipe = redis::pipe();
    for i in 0..50 {
        pipe.cmd("PING").arg(format!("msg-{i}"));
    }
    let results: redis::RedisResult<Vec<String>> = pipe.query_async(&mut con).await;
    match results {
        Ok(vals) => {
            if vals.len() == 50 && vals.iter().enumerate().all(|(i, v)| *v == format!("msg-{i}")) {
                t.pass("50-command pipeline");
            } else {
                t.fail("50-command pipeline", &format!("wrong results: len={}", vals.len()));
            }
        }
        Err(e) => t.fail("50-command pipeline", &format!("error: {e}")),
    }

    // ── COMMAND stubs ──────────────────────────────────────────
    println!("COMMAND");

    let result: redis::RedisResult<i64> = redis::cmd("COMMAND").arg("COUNT").query_async(&mut con).await;
    match result {
        Ok(_) => t.pass("COMMAND COUNT responds"),
        Err(e) => t.fail("COMMAND COUNT responds", &format!("error: {e}")),
    }

    let result: redis::RedisResult<redis::Value> = redis::cmd("COMMAND").arg("DOCS").query_async(&mut con).await;
    match result {
        Ok(_) => t.pass("COMMAND DOCS responds"),
        Err(e) => t.fail("COMMAND DOCS responds", &format!("error: {e}")),
    }

    // ── HELLO ───────────────────────────────────────────────────
    println!("HELLO");

    let result: redis::RedisResult<redis::Value> = redis::cmd("HELLO").query_async(&mut con).await;
    match result {
        Ok(_) => t.pass("HELLO returns server info"),
        Err(e) => t.fail("HELLO returns server info", &format!("error: {e}")),
    }

    let result: redis::RedisResult<redis::Value> = redis::cmd("HELLO").arg("99").query_async(&mut con).await;
    t.check_err("HELLO rejects invalid protocol version", result);

    // ── CLIENT stubs ───────────────────────────────────────────
    println!("CLIENT");

    let result: redis::RedisResult<String> = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("smoke-test")
        .query_async(&mut con)
        .await;
    t.check_eq("CLIENT SETNAME", result, "OK".to_string());

    // ── Error handling ─────────────────────────────────────────
    println!("Error handling");

    let result: redis::RedisResult<String> = redis::cmd("NONEXISTENT").query_async(&mut con).await;
    t.check_err("unknown command returns error", result);

    let result: redis::RedisResult<String> = redis::cmd("ECHO").query_async(&mut con).await;
    t.check_err("ECHO with no args returns error", result);

    // ── Multiple sequential commands ───────────────────────────
    println!("Sequential");

    for i in 0..10 {
        let msg = format!("seq-{i}");
        let result: redis::RedisResult<String> = redis::cmd("ECHO").arg(&msg).query_async(&mut con).await;
        if let Err(e) = &result {
            t.fail(&format!("sequential ECHO {i}"), &format!("error: {e}"));
            break;
        }
        if i == 9 {
            t.check_eq("10 sequential ECHOs", result, msg);
        }
    }

    // ── String Commands ───────────────────────────────────────────
    println!("String Commands");

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("greeting")
        .arg("hello world")
        .query_async(&mut con)
        .await;
    t.check_eq("SET greeting", result, "OK".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("greeting").query_async(&mut con).await;
    t.check_eq("GET greeting", result, "hello world".to_string());

    let result: redis::RedisResult<String> = redis::cmd("SET").arg("counter").arg("0").query_async(&mut con).await;
    t.check_eq("SET counter 0", result, "OK".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("INCR").arg("counter").query_async(&mut con).await;
    t.check_eq("INCR counter", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("INCRBY")
        .arg("counter")
        .arg("41")
        .query_async(&mut con)
        .await;
    t.check_eq("INCRBY counter 41", result, 42);

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("counter").query_async(&mut con).await;
    t.check_eq("GET counter after INCRBY", result, "42".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("DECR").arg("counter").query_async(&mut con).await;
    t.check_eq("DECR counter", result, 41);

    let result: redis::RedisResult<i64> = redis::cmd("DECRBY")
        .arg("counter")
        .arg("40")
        .query_async(&mut con)
        .await;
    t.check_eq("DECRBY counter 40", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("APPEND")
        .arg("greeting")
        .arg(" from kvdb")
        .query_async(&mut con)
        .await;
    t.check_eq("APPEND greeting", result, 21);

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("greeting").query_async(&mut con).await;
    t.check_eq("GET greeting after APPEND", result, "hello world from kvdb".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("STRLEN").arg("greeting").query_async(&mut con).await;
    t.check_eq("STRLEN greeting", result, 21);

    let result: redis::RedisResult<String> = redis::cmd("GETRANGE")
        .arg("greeting")
        .arg("0")
        .arg("4")
        .query_async(&mut con)
        .await;
    t.check_eq("GETRANGE greeting 0 4", result, "hello".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("SETRANGE")
        .arg("greeting")
        .arg("0")
        .arg("HELLO")
        .query_async(&mut con)
        .await;
    t.check_eq("SETRANGE greeting 0 HELLO", result, 21);

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("greeting").query_async(&mut con).await;
    t.check_eq(
        "GET greeting after SETRANGE",
        result,
        "HELLO world from kvdb".to_string(),
    );

    let result: redis::RedisResult<String> = redis::cmd("MSET")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query_async(&mut con)
        .await;
    t.check_eq("MSET a 1 b 2 c 3", result, "OK".to_string());

    let result: redis::RedisResult<Vec<Option<String>>> = redis::cmd("MGET")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await;
    t.check_eq(
        "MGET a b c",
        result,
        vec![Some("1".to_string()), Some("2".to_string()), Some("3".to_string())],
    );

    let result: redis::RedisResult<i64> = redis::cmd("SETNX")
        .arg("newkey")
        .arg("value")
        .query_async(&mut con)
        .await;
    t.check_eq("SETNX newkey (new)", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("SETNX")
        .arg("newkey")
        .arg("other")
        .query_async(&mut con)
        .await;
    t.check_eq("SETNX newkey (existing)", result, 0);

    let result: redis::RedisResult<i64> = redis::cmd("DEL")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("newkey")
        .arg("greeting")
        .arg("counter")
        .query_async(&mut con)
        .await;
    t.check_eq("DEL a b c newkey greeting counter", result, 6);

    let result: redis::RedisResult<i64> = redis::cmd("EXISTS")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await;
    t.check_eq("EXISTS a b c (all deleted)", result, 0);

    let result: redis::RedisResult<String> = redis::cmd("INCRBYFLOAT")
        .arg("pi")
        .arg("3.14159")
        .query_async(&mut con)
        .await;
    t.check_eq("INCRBYFLOAT pi 3.14159", result, "3.14159".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("DEL").arg("pi").query_async(&mut con).await;
    t.check_eq("DEL pi", result, 1);

    // GETDEL: set a key, then getdel it, then verify it's gone
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("tempkey")
        .arg("tempval")
        .query_async(&mut con)
        .await;

    let result: redis::RedisResult<String> = redis::cmd("GETDEL").arg("tempkey").query_async(&mut con).await;
    t.check_eq("GETDEL tempkey returns value", result, "tempval".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("EXISTS").arg("tempkey").query_async(&mut con).await;
    t.check_eq("GETDEL tempkey is gone", result, 0);

    // ── String Error Handling ─────────────────────────────────────
    println!("String Error Handling");

    let result: redis::RedisResult<String> = redis::cmd("GET").query_async(&mut con).await;
    t.check_err("GET no args", result);

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("a").arg("b").query_async(&mut con).await;
    t.check_err("GET too many args", result);

    let result: redis::RedisResult<String> = redis::cmd("SET").query_async(&mut con).await;
    t.check_err("SET no args", result);

    let result: redis::RedisResult<String> = redis::cmd("SET").arg("onlykey").query_async(&mut con).await;
    t.check_err("SET only key", result);

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("NX")
        .arg("XX")
        .query_async(&mut con)
        .await;
    t.check_err("SET key val NX XX", result);

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("EX")
        .arg("0")
        .query_async(&mut con)
        .await;
    t.check_err("SET key val EX 0", result);

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("EX")
        .arg("-5")
        .query_async(&mut con)
        .await;
    t.check_err("SET key val EX -5", result);

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("EX")
        .arg("notanumber")
        .query_async(&mut con)
        .await;
    t.check_err("SET key val EX notanumber", result);

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("key")
        .arg("val")
        .arg("BADOPTION")
        .query_async(&mut con)
        .await;
    t.check_err("SET key val BADOPTION", result);

    let result: redis::RedisResult<String> = redis::cmd("MSET").arg("a").query_async(&mut con).await;
    t.check_err("MSET odd args", result);

    let result: redis::RedisResult<i64> = redis::cmd("DEL").query_async(&mut con).await;
    t.check_err("DEL no args", result);

    let result: redis::RedisResult<i64> = redis::cmd("SETRANGE")
        .arg("key")
        .arg("-1")
        .arg("x")
        .query_async(&mut con)
        .await;
    t.check_err("SETRANGE negative offset", result);

    // ── Post-error liveness check ─────────────────────────────────
    let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
    t.check_eq("server alive after errors", result, "PONG".to_string());

    // ── QUIT (open a separate connection for this) ─────────────
    println!("QUIT");

    let mut quit_con = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            t.fail("QUIT", &format!("failed to open second connection: {e}"));
            return if t.summary() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            };
        }
    };

    // Verify the connection works before QUIT
    let pre: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut quit_con).await;
    t.check_eq("pre-QUIT PING", pre, "PONG".to_string());

    let quit_result: redis::RedisResult<String> = redis::cmd("QUIT").query_async(&mut quit_con).await;
    match quit_result {
        Ok(val) if val == "OK" => t.pass("QUIT returns OK"),
        Ok(val) => t.fail("QUIT returns OK", &format!("got {val:?}")),
        // Some clients interpret connection close as an error, which is fine
        Err(_) => t.pass("QUIT returns OK (connection closed)"),
    }

    // Original connection should still work after the other one quit
    let post: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
    t.check_eq(
        "original connection still alive after other QUIT",
        post,
        "PONG".to_string(),
    );

    // ── Summary ────────────────────────────────────────────────
    if t.summary() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
