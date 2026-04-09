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
