# Unified Smoke Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the smoke test and load generator into a single `examples/smoke.rs` binary with two phases (validate, then load) and optional server spawning.

**Architecture:** The binary runs phase 1 (deterministic correctness checks) then phase 2 (concurrent load generation). When `--spawn-server` is passed, it finds a free port, spawns a kvdb child process, waits for readiness, runs both phases, and kills the server. Both phases are extracted into standalone async functions called from main.

**Tech Stack:** Rust, tokio, redis-rs (async), rand, std::process for server management.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `examples/smoke.rs` | Rewrite | CLI, server spawn, phase 1 validate, phase 2 load, main orchestrator |
| `examples/loadgen.rs` | Delete | Removed — functionality merged into smoke.rs |
| `justfile` | Modify (lines 64-90) | Update `smoke` recipe, delete `loadgen` recipe |

---

### Task 1: Scaffold the merged binary with CLI parsing

**Files:**
- Rewrite: `examples/smoke.rs`

This task replaces the entire file with the new structure. We start with CLI parsing and a skeleton main that prints the parsed args and exits. The existing smoke.rs content will be re-added in Task 3.

- [ ] **Step 1: Write the new `examples/smoke.rs` with CLI and skeleton main**

```rust
//! Smoke test and load generator for kvdb.
//!
//! Phase 1 (validate): sequential correctness checks with known expected results.
//! Phase 2 (load): concurrent randomized workload with throughput reporting.
//!
//! Usage:
//!   cargo run --example smoke                                    # against localhost:6379
//!   cargo run --example smoke -- --spawn-server                  # auto-start server
//!   cargo run --example smoke -- --spawn-server -s 30 -c 64      # 30s, 64 connections
//!   cargo run --release --example smoke -- -s 30 --addr host:port # against existing server

use std::process::ExitCode;

// ── CLI ──────────────────────────────────────────────────────────────────────

struct Args {
    addr: String,
    seconds: u64,
    connections: usize,
    spawn_server: bool,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut addr = String::from("127.0.0.1:6379");
    let mut seconds: u64 = 10;
    let mut connections: usize = 32;
    let mut spawn_server = false;

    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--addr" | "-a" => {
                addr = args.next().expect("--addr requires a value");
            }
            "--seconds" | "-s" => {
                seconds = args
                    .next()
                    .expect("--seconds requires a value")
                    .parse()
                    .expect("--seconds must be a positive integer");
            }
            "--connections" | "-c" => {
                connections = args
                    .next()
                    .expect("--connections requires a value")
                    .parse()
                    .expect("--connections must be a positive integer");
            }
            "--spawn-server" => {
                spawn_server = true;
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: smoke [--addr HOST:PORT] [--seconds N] [--connections N] [--spawn-server]"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown flag: {other}");
                std::process::exit(1);
            }
        }
    }

    Args {
        addr,
        seconds,
        connections,
        spawn_server,
    }
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> ExitCode {
    let args = parse_args();
    println!(
        "smoke: addr={}, seconds={}, connections={}, spawn_server={}",
        args.addr, args.seconds, args.connections, args.spawn_server
    );
    ExitCode::SUCCESS
}
```

- [ ] **Step 2: Verify it compiles and runs**

Run: `cargo run --example smoke -- --help`

Expected: prints usage line and exits 0.

Run: `cargo run --example smoke -- --spawn-server -s 5 -c 16`

Expected: prints `smoke: addr=127.0.0.1:6379, seconds=5, connections=16, spawn_server=true` and exits 0.

- [ ] **Step 3: Commit**

```bash
git add examples/smoke.rs
git commit -m "refactor: scaffold unified smoke binary with CLI parsing"
```

---

### Task 2: Add server spawn support

**Files:**
- Modify: `examples/smoke.rs`

Add three functions: `find_free_port()`, `spawn_server()`, and `wait_for_ready()`. Wire them into main so `--spawn-server` starts a kvdb process and kills it on exit.

- [ ] **Step 1: Add the server spawn section and update main**

Add these imports at the top of the file (merge with existing):

```rust
use std::net::TcpListener;
use std::process::{Child, Command, ExitCode};
use std::time::Duration;
```

Add the server spawn section after the CLI section:

```rust
// ── Server Spawn ─────────────────────────────────────────────────────────────

/// Bind to port 0 to let the OS assign a free port, then release it.
fn find_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Spawn a kvdb server as a child process on the given port.
fn spawn_server(port: u16) -> Child {
    Command::new("cargo")
        .args([
            "run",
            "--",
            "--bind-addr",
            &format!("127.0.0.1:{port}"),
            "--log-level",
            "warn",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to spawn kvdb server")
}

/// Poll the server with TCP connect attempts until it accepts connections.
/// Returns Ok(()) on success, Err(message) if it times out after 5 seconds.
async fn wait_for_ready(addr: &str) -> Result<(), String> {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(format!("server at {addr} did not become ready within 5 seconds"))
}
```

Replace the main function with:

```rust
#[tokio::main]
async fn main() -> ExitCode {
    let mut args = parse_args();

    // Optionally spawn a server.
    let mut server: Option<Child> = None;
    if args.spawn_server {
        let port = find_free_port();
        args.addr = format!("127.0.0.1:{port}");
        println!("Spawning kvdb server on {}", args.addr);
        let child = spawn_server(port);
        server = Some(child);

        if let Err(e) = wait_for_ready(&args.addr).await {
            eprintln!("{e}");
            if let Some(mut s) = server {
                let _ = s.kill();
                let _ = s.wait();
            }
            return ExitCode::FAILURE;
        }
        println!("Server ready.");
    }

    println!();
    println!("smoke: addr={}, seconds={}, connections={}", args.addr, args.seconds, args.connections);

    // TODO: Phase 1 (validate) and Phase 2 (load) will be added in subsequent tasks.

    // Clean up server if we spawned it.
    if let Some(mut s) = server {
        let _ = s.kill();
        let _ = s.wait();
    }

    ExitCode::SUCCESS
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build --example smoke`

Expected: compiles without warnings.

- [ ] **Step 3: Test server spawn end-to-end (requires FDB running)**

Run: `cargo run --example smoke -- --spawn-server`

Expected: prints "Spawning kvdb server on 127.0.0.1:NNNNN", "Server ready.", then the smoke line, and exits 0. The spawned server process is killed cleanly.

- [ ] **Step 4: Commit**

```bash
git add examples/smoke.rs
git commit -m "feat(smoke): add --spawn-server with port discovery and readiness polling"
```

---

### Task 3: Add Phase 1 (validate)

**Files:**
- Modify: `examples/smoke.rs`

Port the `SmokeTest` struct and all 48 validation checks from the old smoke.rs into a `run_validate()` async function. This is a direct copy of the existing logic, wrapped in a function that takes the address and returns a bool.

- [ ] **Step 1: Add the SmokeTest struct and run_validate function**

Add this section after the server spawn section:

```rust
// ── Phase 1: Validate ────────────────────────────────────────────────────────

struct SmokeTest {
    passed: usize,
    failed: usize,
}

impl SmokeTest {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
        }
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
    fn check_err<T: std::fmt::Debug>(
        &mut self,
        name: &str,
        got: Result<T, impl std::fmt::Debug>,
    ) {
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
            println!(
                "{} passed, {} FAILED (out of {total})",
                self.passed, self.failed
            );
            false
        }
    }
}

/// Run all deterministic validation checks. Returns true if all passed.
async fn run_validate(addr: &str) -> bool {
    println!("=== Phase 1: Validate ===");
    println!();

    let client = match redis::Client::open(format!("redis://{addr}")) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to create client: {e}");
            return false;
        }
    };

    let mut con = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to {addr}: {e}");
            eprintln!("Is the server running? Start with: cargo run -- --bind-addr {addr}");
            return false;
        }
    };

    let mut t = SmokeTest::new();

    // ── PING ───────────────────────────────────────────────────
    println!("PING");

    let result: redis::RedisResult<String> =
        redis::cmd("PING").query_async(&mut con).await;
    t.check_eq("PING returns PONG", result, "PONG".to_string());

    let result: redis::RedisResult<String> =
        redis::cmd("PING").arg("hello").query_async(&mut con).await;
    t.check_eq("PING with message", result, "hello".to_string());

    // ── ECHO ───────────────────────────────────────────────────
    println!("ECHO");

    let result: redis::RedisResult<String> =
        redis::cmd("ECHO").arg("smoke test").query_async(&mut con).await;
    t.check_eq("ECHO returns argument", result, "smoke test".to_string());

    let result: redis::RedisResult<String> = redis::cmd("ECHO")
        .arg("binary\x00safe\r\ndata")
        .query_async(&mut con)
        .await;
    t.check_eq(
        "ECHO binary-safe",
        result,
        "binary\x00safe\r\ndata".to_string(),
    );

    // ── Pipelining ─────────────────────────────────────────────
    println!("Pipelining");

    let mut pipe = redis::pipe();
    for i in 0..50 {
        pipe.cmd("PING").arg(format!("msg-{i}"));
    }
    let results: redis::RedisResult<Vec<String>> = pipe.query_async(&mut con).await;
    match results {
        Ok(vals) => {
            if vals.len() == 50
                && vals.iter().enumerate().all(|(i, v)| *v == format!("msg-{i}"))
            {
                t.pass("50-command pipeline");
            } else {
                t.fail(
                    "50-command pipeline",
                    &format!("wrong results: len={}", vals.len()),
                );
            }
        }
        Err(e) => t.fail("50-command pipeline", &format!("error: {e}")),
    }

    // ── COMMAND stubs ──────────────────────────────────────────
    println!("COMMAND");

    let result: redis::RedisResult<i64> =
        redis::cmd("COMMAND").arg("COUNT").query_async(&mut con).await;
    match result {
        Ok(_) => t.pass("COMMAND COUNT responds"),
        Err(e) => t.fail("COMMAND COUNT responds", &format!("error: {e}")),
    }

    let result: redis::RedisResult<redis::Value> =
        redis::cmd("COMMAND").arg("DOCS").query_async(&mut con).await;
    match result {
        Ok(_) => t.pass("COMMAND DOCS responds"),
        Err(e) => t.fail("COMMAND DOCS responds", &format!("error: {e}")),
    }

    // ── HELLO ──────────────────────────────────────────────────
    println!("HELLO");

    let result: redis::RedisResult<redis::Value> =
        redis::cmd("HELLO").query_async(&mut con).await;
    match result {
        Ok(_) => t.pass("HELLO returns server info"),
        Err(e) => t.fail("HELLO returns server info", &format!("error: {e}")),
    }

    let result: redis::RedisResult<redis::Value> =
        redis::cmd("HELLO").arg("99").query_async(&mut con).await;
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

    let result: redis::RedisResult<String> =
        redis::cmd("NONEXISTENT").query_async(&mut con).await;
    t.check_err("unknown command returns error", result);

    let result: redis::RedisResult<String> =
        redis::cmd("ECHO").query_async(&mut con).await;
    t.check_err("ECHO with no args returns error", result);

    // ── Multiple sequential commands ───────────────────────────
    println!("Sequential");

    for i in 0..10 {
        let msg = format!("seq-{i}");
        let result: redis::RedisResult<String> =
            redis::cmd("ECHO").arg(&msg).query_async(&mut con).await;
        if let Err(e) = &result {
            t.fail(&format!("sequential ECHO {i}"), &format!("error: {e}"));
            break;
        }
        if i == 9 {
            t.check_eq("10 sequential ECHOs", result, msg);
        }
    }

    // ── String Commands ────────────────────────────────────────
    println!("String Commands");

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("greeting")
        .arg("hello world")
        .query_async(&mut con)
        .await;
    t.check_eq("SET greeting", result, "OK".to_string());

    let result: redis::RedisResult<String> =
        redis::cmd("GET").arg("greeting").query_async(&mut con).await;
    t.check_eq("GET greeting", result, "hello world".to_string());

    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("counter")
        .arg("0")
        .query_async(&mut con)
        .await;
    t.check_eq("SET counter 0", result, "OK".to_string());

    let result: redis::RedisResult<i64> =
        redis::cmd("INCR").arg("counter").query_async(&mut con).await;
    t.check_eq("INCR counter", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("INCRBY")
        .arg("counter")
        .arg("41")
        .query_async(&mut con)
        .await;
    t.check_eq("INCRBY counter 41", result, 42);

    let result: redis::RedisResult<String> =
        redis::cmd("GET").arg("counter").query_async(&mut con).await;
    t.check_eq("GET counter after INCRBY", result, "42".to_string());

    let result: redis::RedisResult<i64> =
        redis::cmd("DECR").arg("counter").query_async(&mut con).await;
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

    let result: redis::RedisResult<String> =
        redis::cmd("GET").arg("greeting").query_async(&mut con).await;
    t.check_eq(
        "GET greeting after APPEND",
        result,
        "hello world from kvdb".to_string(),
    );

    let result: redis::RedisResult<i64> =
        redis::cmd("STRLEN").arg("greeting").query_async(&mut con).await;
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

    let result: redis::RedisResult<String> =
        redis::cmd("GET").arg("greeting").query_async(&mut con).await;
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
        vec![
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3".to_string()),
        ],
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

    let result: redis::RedisResult<i64> =
        redis::cmd("DEL").arg("pi").query_async(&mut con).await;
    t.check_eq("DEL pi", result, 1);

    // GETDEL: set a key, then getdel it, then verify it's gone
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("tempkey")
        .arg("tempval")
        .query_async(&mut con)
        .await;

    let result: redis::RedisResult<String> =
        redis::cmd("GETDEL").arg("tempkey").query_async(&mut con).await;
    t.check_eq("GETDEL tempkey returns value", result, "tempval".to_string());

    let result: redis::RedisResult<i64> =
        redis::cmd("EXISTS").arg("tempkey").query_async(&mut con).await;
    t.check_eq("GETDEL tempkey is gone", result, 0);

    // ── String Error Handling ──────────────────────────────────
    println!("String Error Handling");

    let result: redis::RedisResult<String> =
        redis::cmd("GET").query_async(&mut con).await;
    t.check_err("GET no args", result);

    let result: redis::RedisResult<String> =
        redis::cmd("GET").arg("a").arg("b").query_async(&mut con).await;
    t.check_err("GET too many args", result);

    let result: redis::RedisResult<String> =
        redis::cmd("SET").query_async(&mut con).await;
    t.check_err("SET no args", result);

    let result: redis::RedisResult<String> =
        redis::cmd("SET").arg("onlykey").query_async(&mut con).await;
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

    let result: redis::RedisResult<String> =
        redis::cmd("MSET").arg("a").query_async(&mut con).await;
    t.check_err("MSET odd args", result);

    let result: redis::RedisResult<i64> =
        redis::cmd("DEL").query_async(&mut con).await;
    t.check_err("DEL no args", result);

    let result: redis::RedisResult<i64> = redis::cmd("SETRANGE")
        .arg("key")
        .arg("-1")
        .arg("x")
        .query_async(&mut con)
        .await;
    t.check_err("SETRANGE negative offset", result);

    // ── Post-error liveness check ──────────────────────────────
    let result: redis::RedisResult<String> =
        redis::cmd("PING").query_async(&mut con).await;
    t.check_eq("server alive after errors", result, "PONG".to_string());

    // ── QUIT (open a separate connection for this) ─────────────
    println!("QUIT");

    let mut quit_con = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            t.fail("QUIT", &format!("failed to open second connection: {e}"));
            return t.summary();
        }
    };

    let pre: redis::RedisResult<String> =
        redis::cmd("PING").query_async(&mut quit_con).await;
    t.check_eq("pre-QUIT PING", pre, "PONG".to_string());

    let quit_result: redis::RedisResult<String> =
        redis::cmd("QUIT").query_async(&mut quit_con).await;
    match quit_result {
        Ok(val) if val == "OK" => t.pass("QUIT returns OK"),
        Ok(val) => t.fail("QUIT returns OK", &format!("got {val:?}")),
        // Some clients interpret connection close as an error, which is fine
        Err(_) => t.pass("QUIT returns OK (connection closed)"),
    }

    let post: redis::RedisResult<String> =
        redis::cmd("PING").query_async(&mut con).await;
    t.check_eq(
        "original connection still alive after other QUIT",
        post,
        "PONG".to_string(),
    );

    // ── Summary ────────────────────────────────────────────────
    t.summary()
}
```

- [ ] **Step 2: Wire run_validate into main**

Update main to call `run_validate` after the server readiness check. Replace the `println!("smoke: ...")` placeholder with:

```rust
    // Phase 1: Validate
    let validate_ok = run_validate(&args.addr).await;

    // Clean up server if we spawned it.
    if let Some(mut s) = server {
        let _ = s.kill();
        let _ = s.wait();
    }

    if !validate_ok {
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo build --example smoke`

Expected: compiles without warnings.

- [ ] **Step 4: Run validation against a live server**

Run: `cargo run --example smoke -- --spawn-server`

Expected: prints `=== Phase 1: Validate ===`, all 48 checks pass, server is killed, exits 0.

- [ ] **Step 5: Commit**

```bash
git add examples/smoke.rs
git commit -m "feat(smoke): add phase 1 validate with all 48 correctness checks"
```

---

### Task 4: Add Phase 2 (load)

**Files:**
- Modify: `examples/smoke.rs`

Port the entire load generator from `examples/loadgen.rs` into a `run_load()` async function. This includes: `Stats`, `run_one_op()`, `random_value()`, the worker loop, and the reporter task.

- [ ] **Step 1: Add imports for the load phase**

Add these imports at the top (merge with existing):

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis::AsyncCommands;
use tokio::task::JoinSet;
```

- [ ] **Step 2: Add the Stats struct, run_one_op, random_value, and worker**

Add this section after the validate section:

```rust
// ── Phase 2: Load ────────────────────────────────────────────────────────────

struct Stats {
    ops: AtomicU64,
    errors: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            ops: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

// Workload distribution (weights sum to 100):
//
//   GET          31%    — point reads dominate
//   SET          15%    — writes (some with TTL, NX, XX, GET)
//   MGET          8%    — batch reads
//   MSET          4%    — batch writes
//   INCR/DECR     8%    — counters
//   INCRBY/DECRBY 4%    — variable counters
//   INCRBYFLOAT   2%    — float counters
//   EXISTS        5%    — key existence checks
//   DEL           4%    — deletions
//   APPEND        3%    — append-only logs
//   STRLEN        2%    — length checks
//   GETRANGE      2%    — substring reads
//   SETRANGE      2%    — substring writes
//   SETNX         2%    — conditional sets
//   SETEX/PSETEX  2%    — sets with TTL
//   GETDEL        1%    — atomic get-and-delete
//   TTL ops       4%    — TTL management (EXPIRE, TTL, PERSIST, TYPE)
//   PING          1%    — health checks

/// Pick a random operation and execute it.
///
/// Key prefixes are partitioned by logical type to avoid cross-type collisions:
///   `lg:s:<n>`   — string values
///   `lg:c:<n>`   — integer counters
///   `lg:f:<n>`   — float counters
///   `lg:a:<n>`   — append-only strings
async fn run_one_op(
    con: &mut redis::aio::MultiplexedConnection,
    rng: &mut StdRng,
    key_space: u32,
) -> Result<(), String> {
    let roll: u32 = rng.gen_range(0..100);

    let str_key = format!("lg:s:{}", rng.gen_range(0..key_space));
    let ctr_key = format!("lg:c:{}", rng.gen_range(0..key_space / 4));
    let flt_key = format!("lg:f:{}", rng.gen_range(0..key_space / 4));
    let app_key = format!("lg:a:{}", rng.gen_range(0..key_space));

    match roll {
        // GET (31%)
        0..=30 => redis::cmd("GET")
            .arg(&str_key)
            .query_async::<Option<String>>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // TTL operations (4%)
        31..=34 => {
            let op = rng.gen_range(0..4);
            match op {
                0 => {
                    let ttl: u32 = rng.gen_range(10..300);
                    redis::cmd("EXPIRE")
                        .arg(&str_key)
                        .arg(ttl)
                        .query_async::<i64>(con)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                1 => redis::cmd("TTL")
                    .arg(&str_key)
                    .query_async::<i64>(con)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                2 => redis::cmd("PERSIST")
                    .arg(&str_key)
                    .query_async::<i64>(con)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                3 => redis::cmd("TYPE")
                    .arg(&str_key)
                    .query_async::<String>(con)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                _ => unreachable!(),
            }
        }

        // SET (15%) — with random flags
        35..=49 => {
            let value = random_value(rng);
            let variant: u8 = rng.gen_range(0..5);
            let mut cmd = redis::cmd("SET");
            cmd.arg(&str_key).arg(&value);
            match variant {
                0 => {}
                1 => { cmd.arg("NX"); }
                2 => { cmd.arg("XX"); }
                3 => { cmd.arg("EX").arg(rng.gen_range(10..300)); }
                4 => { cmd.arg("GET"); }
                _ => unreachable!(),
            }
            cmd.query_async::<redis::Value>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // MGET (8%)
        50..=57 => {
            let n = rng.gen_range(2..=8);
            let keys: Vec<String> = (0..n)
                .map(|_| format!("lg:s:{}", rng.gen_range(0..key_space)))
                .collect();
            redis::cmd("MGET")
                .arg(&keys)
                .query_async::<Vec<Option<String>>>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // MSET (4%)
        58..=61 => {
            let n = rng.gen_range(2..=6);
            let mut cmd = redis::cmd("MSET");
            for _ in 0..n {
                let k = format!("lg:s:{}", rng.gen_range(0..key_space));
                cmd.arg(k).arg(random_value(rng));
            }
            cmd.query_async::<redis::Value>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // INCR / DECR (8%)
        62..=69 => {
            let cmd_name = if rng.gen_bool(0.5) { "INCR" } else { "DECR" };
            redis::cmd(cmd_name)
                .arg(&ctr_key)
                .query_async::<i64>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // INCRBY / DECRBY (4%)
        70..=73 => {
            let delta: i64 = rng.gen_range(1..100);
            let cmd_name = if rng.gen_bool(0.5) { "INCRBY" } else { "DECRBY" };
            redis::cmd(cmd_name)
                .arg(&ctr_key)
                .arg(delta)
                .query_async::<i64>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // INCRBYFLOAT (2%)
        74..=75 => {
            let delta: f64 = rng.gen_range(-10.0..10.0);
            redis::cmd("INCRBYFLOAT")
                .arg(&flt_key)
                .arg(delta)
                .query_async::<String>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // EXISTS (5%)
        76..=80 => {
            let n = rng.gen_range(1..=4);
            let mut cmd = redis::cmd("EXISTS");
            for _ in 0..n {
                cmd.arg(format!("lg:s:{}", rng.gen_range(0..key_space)));
            }
            cmd.query_async::<i64>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // DEL (4%)
        81..=84 => {
            let n = rng.gen_range(1..=3);
            let mut cmd = redis::cmd("DEL");
            for _ in 0..n {
                cmd.arg(format!("lg:s:{}", rng.gen_range(0..key_space)));
            }
            cmd.query_async::<i64>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // APPEND (3%)
        85..=87 => {
            let suffix: String = (0..rng.gen_range(4..32))
                .map(|_| rng.gen_range(b'a'..=b'z') as char)
                .collect();
            redis::cmd("APPEND")
                .arg(&app_key)
                .arg(&suffix)
                .query_async::<i64>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // STRLEN (2%)
        88..=89 => redis::cmd("STRLEN")
            .arg(&app_key)
            .query_async::<i64>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // GETRANGE (2%)
        90..=91 => {
            let start: i64 = rng.gen_range(0..20);
            let end: i64 = start + rng.gen_range(1..50);
            redis::cmd("GETRANGE")
                .arg(&app_key)
                .arg(start)
                .arg(end)
                .query_async::<String>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // SETRANGE (2%)
        92..=93 => {
            let offset: u32 = rng.gen_range(0..20);
            let patch: String = (0..rng.gen_range(1..10))
                .map(|_| rng.gen_range(b'a'..=b'z') as char)
                .collect();
            redis::cmd("SETRANGE")
                .arg(&app_key)
                .arg(offset)
                .arg(&patch)
                .query_async::<i64>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // SETNX (2%)
        94..=95 => con
            .set_nx::<_, _, i64>(&str_key, random_value(rng))
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // SETEX / PSETEX (2%)
        96..=97 => {
            if rng.gen_bool(0.5) {
                redis::cmd("SETEX")
                    .arg(&str_key)
                    .arg(rng.gen_range(10..300))
                    .arg(random_value(rng))
                    .query_async::<String>(con)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            } else {
                redis::cmd("PSETEX")
                    .arg(&str_key)
                    .arg(rng.gen_range(10_000..300_000))
                    .arg(random_value(rng))
                    .query_async::<String>(con)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }
        }

        // GETDEL (1%)
        98 => redis::cmd("GETDEL")
            .arg(&str_key)
            .query_async::<redis::Value>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // PING (1%)
        99 => redis::cmd("PING")
            .query_async::<String>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        _ => unreachable!(),
    }
}

/// Generate a random value with realistic size distribution.
fn random_value(rng: &mut StdRng) -> String {
    let len = match rng.gen_range(0..100u32) {
        0..=59 => rng.gen_range(8..128),       // 60%: small values
        60..=84 => rng.gen_range(128..1024),   // 25%: medium values
        85..=94 => rng.gen_range(1024..8192),  // 10%: larger values
        _ => rng.gen_range(8192..32768),       //  5%: big payloads
    };
    (0..len)
        .map(|_| rng.gen_range(b'!'..=b'~') as char)
        .collect()
}

async fn worker(addr: String, deadline: Instant, stats: Arc<Stats>, worker_id: usize) {
    let client = match redis::Client::open(format!("redis://{addr}")) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("worker {worker_id}: failed to create client: {e}");
            return;
        }
    };

    let mut con = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("worker {worker_id}: connection failed: {e}");
            return;
        }
    };

    let mut rng = StdRng::from_entropy();
    let key_space: u32 = 5_000;

    while Instant::now() < deadline {
        match run_one_op(&mut con, &mut rng, key_space).await {
            Ok(()) => {
                stats.ops.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
```

- [ ] **Step 3: Add run_load function**

Add immediately after the `worker` function:

```rust
/// Run the load phase: spawn workers and a reporter, wait for completion.
async fn run_load(addr: &str, seconds: u64, connections: usize) {
    println!("=== Phase 2: Load ({seconds}s, {connections} connections) ===");
    println!();

    let stats = Arc::new(Stats::new());
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let start = Instant::now();

    // Reporter task: prints throughput every second.
    let reporter_stats = Arc::clone(&stats);
    let reporter = tokio::spawn(async move {
        let mut prev_ops: u64 = 0;
        let mut prev_errors: u64 = 0;
        let mut tick: u64 = 0;

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            tick += 1;

            let total_ops = reporter_stats.ops.load(Ordering::Relaxed);
            let total_errors = reporter_stats.errors.load(Ordering::Relaxed);
            let delta_ops = total_ops - prev_ops;
            let delta_errors = total_errors - prev_errors;
            prev_ops = total_ops;
            prev_errors = total_errors;

            eprintln!(
                "[{tick:>3}s]  {delta_ops:>8} ops/s  ({delta_errors:>4} errs/s)  total: {total_ops}",
            );

            if Instant::now() >= deadline {
                break;
            }
        }
    });

    // Spawn all worker tasks.
    let mut workers = JoinSet::new();
    for id in 0..connections {
        let addr = addr.to_string();
        let stats = Arc::clone(&stats);
        workers.spawn(worker(addr, deadline, stats, id));
    }

    // Wait for all workers to finish.
    while workers.join_next().await.is_some() {}

    // Stop reporter.
    reporter.abort();
    let _ = reporter.await;

    // Final summary.
    let elapsed = start.elapsed().as_secs_f64();
    let total_ops = stats.ops.load(Ordering::Relaxed);
    let total_errors = stats.errors.load(Ordering::Relaxed);
    let throughput = total_ops as f64 / elapsed;

    println!();
    println!("--- smoke summary ---");
    println!("  duration:    {elapsed:.2}s");
    println!("  connections: {connections}");
    println!("  total ops:   {total_ops}");
    println!(
        "  total errors: {total_errors} ({:.1}%)",
        if total_ops + total_errors > 0 {
            total_errors as f64 / (total_ops + total_errors) as f64 * 100.0
        } else {
            0.0
        }
    );
    println!("  throughput:  {throughput:.0} ops/s");
}
```

- [ ] **Step 4: Wire run_load into main**

Update main so that after validation passes, it runs the load phase before cleanup. The main function's phase section becomes:

```rust
    // Phase 1: Validate
    let validate_ok = run_validate(&args.addr).await;

    if !validate_ok {
        // Clean up server if we spawned it.
        if let Some(mut s) = server {
            let _ = s.kill();
            let _ = s.wait();
        }
        return ExitCode::FAILURE;
    }

    println!();

    // Phase 2: Load
    run_load(&args.addr, args.seconds, args.connections).await;

    // Clean up server if we spawned it.
    if let Some(mut s) = server {
        let _ = s.kill();
        let _ = s.wait();
    }

    ExitCode::SUCCESS
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo build --example smoke`

Expected: compiles without warnings.

- [ ] **Step 6: Run both phases end-to-end with a short duration**

Run: `cargo run --example smoke -- --spawn-server -s 3 -c 4`

Expected: Phase 1 prints all 48 checks passing, Phase 2 runs for ~3 seconds with 4 connections, prints per-second throughput, then the summary. Server is killed. Exits 0.

- [ ] **Step 7: Commit**

```bash
git add examples/smoke.rs
git commit -m "feat(smoke): add phase 2 load generator with throughput reporting"
```

---

### Task 5: Delete loadgen and update justfile

**Files:**
- Delete: `examples/loadgen.rs`
- Modify: `justfile` (lines 64-90)

- [ ] **Step 1: Delete `examples/loadgen.rs`**

```bash
rm examples/loadgen.rs
```

- [ ] **Step 2: Update the justfile**

Replace the `smoke` recipe (lines 64-86) and `loadgen` recipe (lines 88-90) with the new single recipe. The old `smoke` recipe is a multi-line bash script; the new one is a single command:

Old (lines 64-90):
```just
# Run smoke tests against a live server (starts one automatically)
smoke:
    #!/usr/bin/env bash
    set -e
    cargo build --example smoke 2>&1

    cargo run -- --bind-addr 127.0.0.1:6399 --log-level warn &
    SERVER_PID=$!

    # Wait for server to accept connections
    for i in $(seq 1 50); do
        if redis-cli -p 6399 PING >/dev/null 2>&1; then break; fi
        sleep 0.1
    done

    set +e
    cargo run --example smoke -- 127.0.0.1:6399
    STATUS=$?
    set -e

    kill $SERVER_PID 2>/dev/null
    wait $SERVER_PID 2>/dev/null || true
    exit $STATUS

# Run load generator against a live server (default 10s, 32 connections)
loadgen seconds="10" connections="32" addr="127.0.0.1:6379":
    cargo run --release --example loadgen -- --seconds {{seconds}} --connections {{connections}} --addr {{addr}}
```

New:
```just
# Run smoke tests (validate + load) against a live server (starts one automatically)
smoke seconds="10" connections="32":
    cargo run --example smoke -- --spawn-server --seconds {{seconds}} --connections {{connections}}
```

- [ ] **Step 3: Verify lint passes**

Run: `cargo clippy --workspace --tests --examples -- -D warnings`

Expected: no warnings or errors. This also confirms loadgen.rs deletion doesn't break anything.

- [ ] **Step 4: Verify `just smoke` works with default and custom args**

Run: `just smoke seconds=3 connections=4`

Expected: spawns server, runs validate (48 checks pass), runs load (3 seconds, 4 connections), prints summary, exits 0.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: remove loadgen, consolidate into unified smoke binary

Delete examples/loadgen.rs and the justfile loadgen recipe.
The smoke binary now handles both validation and load generation.
just smoke accepts seconds= and connections= parameters."
```

---

### Task 6: Final verification

**Files:** None — this is a verification-only task.

- [ ] **Step 1: Run `just lint`**

Run: `just lint`

Expected: clippy + fmt check both pass with zero warnings.

- [ ] **Step 2: Run `just smoke` with defaults**

Run: `just smoke`

Expected: full run — server spawned, 48 validation checks pass, 10 seconds of load with 32 connections, summary printed, exits 0.

- [ ] **Step 3: Run smoke against an external server (no --spawn-server)**

Start a server manually, then run smoke against it:

```bash
cargo run -- --bind-addr 127.0.0.1:6399 --log-level warn &
SERVER_PID=$!
sleep 2
cargo run --example smoke -- --addr 127.0.0.1:6399 -s 3 -c 4
kill $SERVER_PID
```

Expected: connects to the pre-existing server, runs both phases, exits 0.

- [ ] **Step 4: Verify --help output**

Run: `cargo run --example smoke -- --help`

Expected: prints usage showing all flags (--addr, --seconds, --connections, --spawn-server, --help).

- [ ] **Step 5: Run `just test` to ensure no regressions**

Run: `just test`

Expected: all unit and integration tests pass. The smoke binary is unrelated to these tests, but we want to confirm nothing was broken by the file changes.
