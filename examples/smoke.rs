//! Unified smoke test and load generator for kvdb.
//!
//! Phase 1 (Validate): deterministic correctness checks against every
//! implemented command.
//! Phase 2 (Load): concurrent randomized workload with throughput reporting.
//!
//! Usage:
//!   cargo run --example smoke                                    # validate + 10s load
//!   cargo run --example smoke -- --spawn-server                  # auto-start kvdb
//!   cargo run --example smoke -- --seconds 30 --connections 64   # tune load phase
//!   cargo run --example smoke -- --addr 127.0.0.1:6399           # custom address
//!   just smoke                                                   # starts server, runs smoke, stops server

use std::net::TcpListener;
use std::process::{Child, Command, ExitCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis::AsyncCommands;
use tokio::task::JoinSet;

// ===========================================================================
// CLI
// ===========================================================================

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
                eprintln!("Usage: smoke [--addr HOST:PORT] [--seconds N] [--connections N] [--spawn-server]");
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

// ===========================================================================
// Server Spawn
// ===========================================================================

/// Find a free TCP port by binding to port 0 and reading the assigned port.
fn find_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Spawn the kvdb server as a child process on the given address.
fn spawn_server(addr: &str) -> Child {
    Command::new("cargo")
        .args(["run", "--", "--bind-addr", addr, "--log-level", "warn"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .expect("failed to spawn kvdb server")
}

/// Poll until the server accepts TCP connections.
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

// ===========================================================================
// Phase 1: Validate
// ===========================================================================

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

/// Run all deterministic validation checks. Returns true if every check passed.
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
            eprintln!("Is the server running? Start it with: just run --bind-addr {addr}");
            return false;
        }
    };

    let mut t = SmokeTest::new();

    // -- PING ---------------------------------------------------------------
    println!("PING");

    let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
    t.check_eq("PING returns PONG", result, "PONG".to_string());

    let result: redis::RedisResult<String> = redis::cmd("PING").arg("hello").query_async(&mut con).await;
    t.check_eq("PING with message", result, "hello".to_string());

    // -- ECHO ---------------------------------------------------------------
    println!("ECHO");

    let result: redis::RedisResult<String> = redis::cmd("ECHO").arg("smoke test").query_async(&mut con).await;
    t.check_eq("ECHO returns argument", result, "smoke test".to_string());

    let result: redis::RedisResult<String> = redis::cmd("ECHO")
        .arg("binary\x00safe\r\ndata")
        .query_async(&mut con)
        .await;
    t.check_eq("ECHO binary-safe", result, "binary\x00safe\r\ndata".to_string());

    // -- Pipelining ---------------------------------------------------------
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

    // -- COMMAND stubs ------------------------------------------------------
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

    // -- HELLO --------------------------------------------------------------
    println!("HELLO");

    let result: redis::RedisResult<redis::Value> = redis::cmd("HELLO").query_async(&mut con).await;
    match result {
        Ok(_) => t.pass("HELLO returns server info"),
        Err(e) => t.fail("HELLO returns server info", &format!("error: {e}")),
    }

    let result: redis::RedisResult<redis::Value> = redis::cmd("HELLO").arg("99").query_async(&mut con).await;
    t.check_err("HELLO rejects invalid protocol version", result);

    // -- CLIENT stubs -------------------------------------------------------
    println!("CLIENT");

    let result: redis::RedisResult<String> = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("smoke-test")
        .query_async(&mut con)
        .await;
    t.check_eq("CLIENT SETNAME", result, "OK".to_string());

    // -- Error handling -----------------------------------------------------
    println!("Error handling");

    let result: redis::RedisResult<String> = redis::cmd("NONEXISTENT").query_async(&mut con).await;
    t.check_err("unknown command returns error", result);

    let result: redis::RedisResult<String> = redis::cmd("ECHO").query_async(&mut con).await;
    t.check_err("ECHO with no args returns error", result);

    // -- Multiple sequential commands ---------------------------------------
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

    // -- String Commands ----------------------------------------------------
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

    // -- String Error Handling ----------------------------------------------
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

    // -- SET flags (success paths) ------------------------------------------
    println!("SET flags");

    // SET with EX: set a key with a 60-second TTL
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("ttl-key")
        .arg("expires-soon")
        .arg("EX")
        .arg("60")
        .query_async(&mut con)
        .await;
    t.check_eq("SET ttl-key EX 60", result, "OK".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("ttl-key").query_async(&mut con).await;
    t.check_eq("GET ttl-key after SET EX", result, "expires-soon".to_string());

    // SET with PX: set a key with a 60000ms TTL
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("px-key")
        .arg("px-value")
        .arg("PX")
        .arg("60000")
        .query_async(&mut con)
        .await;
    t.check_eq("SET px-key PX 60000", result, "OK".to_string());

    // SET with GET: returns old value
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("get-flag-key")
        .arg("old-value")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("get-flag-key")
        .arg("new-value")
        .arg("GET")
        .query_async(&mut con)
        .await;
    t.check_eq("SET GET returns old value", result, "old-value".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("get-flag-key").query_async(&mut con).await;
    t.check_eq("GET after SET GET", result, "new-value".to_string());

    // SET NX on new key succeeds
    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("nx-test").query_async(&mut con).await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("nx-test")
        .arg("first")
        .arg("NX")
        .query_async(&mut con)
        .await;
    t.check_eq("SET NX on new key", result, "OK".to_string());

    // SET XX on existing key succeeds
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("nx-test")
        .arg("updated")
        .arg("XX")
        .query_async(&mut con)
        .await;
    t.check_eq("SET XX on existing key", result, "OK".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("nx-test").query_async(&mut con).await;
    t.check_eq("GET after SET XX", result, "updated".to_string());

    // SET with KEEPTTL preserves existing TTL
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("keepttl-key")
        .arg("v1")
        .arg("EX")
        .arg("300")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("keepttl-key")
        .arg("v2")
        .arg("KEEPTTL")
        .query_async(&mut con)
        .await;
    t.check_eq("SET KEEPTTL", result, "OK".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("keepttl-key").query_async(&mut con).await;
    match result {
        Ok(ttl) if ttl > 0 => t.pass("TTL preserved after SET KEEPTTL"),
        Ok(ttl) => t.fail("TTL preserved after SET KEEPTTL", &format!("expected >0, got {ttl}")),
        Err(e) => t.fail("TTL preserved after SET KEEPTTL", &format!("error: {e}")),
    }

    // cleanup SET flags keys
    let _: redis::RedisResult<i64> = redis::cmd("DEL")
        .arg("ttl-key")
        .arg("px-key")
        .arg("get-flag-key")
        .arg("nx-test")
        .arg("keepttl-key")
        .query_async(&mut con)
        .await;

    // -- SETEX / PSETEX -----------------------------------------------------
    println!("SETEX / PSETEX");

    let result: redis::RedisResult<String> = redis::cmd("SETEX")
        .arg("setex-key")
        .arg("120")
        .arg("setex-val")
        .query_async(&mut con)
        .await;
    t.check_eq("SETEX setex-key 120", result, "OK".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("setex-key").query_async(&mut con).await;
    t.check_eq("GET setex-key", result, "setex-val".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("setex-key").query_async(&mut con).await;
    match result {
        Ok(ttl) if ttl > 0 && ttl <= 120 => t.pass("TTL setex-key in range"),
        Ok(ttl) => t.fail("TTL setex-key in range", &format!("expected 1..=120, got {ttl}")),
        Err(e) => t.fail("TTL setex-key in range", &format!("error: {e}")),
    }

    let result: redis::RedisResult<String> = redis::cmd("PSETEX")
        .arg("psetex-key")
        .arg("120000")
        .arg("psetex-val")
        .query_async(&mut con)
        .await;
    t.check_eq("PSETEX psetex-key 120000", result, "OK".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("psetex-key").query_async(&mut con).await;
    t.check_eq("GET psetex-key", result, "psetex-val".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("PTTL").arg("psetex-key").query_async(&mut con).await;
    match result {
        Ok(pttl) if pttl > 0 && pttl <= 120_000 => t.pass("PTTL psetex-key in range"),
        Ok(pttl) => t.fail("PTTL psetex-key in range", &format!("expected 1..=120000, got {pttl}")),
        Err(e) => t.fail("PTTL psetex-key in range", &format!("error: {e}")),
    }

    // SETEX error cases
    let result: redis::RedisResult<String> = redis::cmd("SETEX")
        .arg("k")
        .arg("0")
        .arg("v")
        .query_async(&mut con)
        .await;
    t.check_err("SETEX zero seconds", result);

    let result: redis::RedisResult<String> = redis::cmd("SETEX")
        .arg("k")
        .arg("-5")
        .arg("v")
        .query_async(&mut con)
        .await;
    t.check_err("SETEX negative seconds", result);

    let result: redis::RedisResult<String> = redis::cmd("SETEX").arg("k").arg("v").query_async(&mut con).await;
    t.check_err("SETEX wrong arity (2 args)", result);

    let result: redis::RedisResult<String> = redis::cmd("PSETEX")
        .arg("k")
        .arg("0")
        .arg("v")
        .query_async(&mut con)
        .await;
    t.check_err("PSETEX zero ms", result);

    // cleanup
    let _: redis::RedisResult<i64> = redis::cmd("DEL")
        .arg("setex-key")
        .arg("psetex-key")
        .query_async(&mut con)
        .await;

    // -- TTL / PTTL ---------------------------------------------------------
    println!("TTL / PTTL");

    // TTL on nonexistent key returns -2
    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("no-such-key").query_async(&mut con).await;
    t.check_eq("TTL nonexistent returns -2", result, -2);

    // Set a key with no TTL, TTL returns -1
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("no-ttl-key")
        .arg("val")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("no-ttl-key").query_async(&mut con).await;
    t.check_eq("TTL no-expiry returns -1", result, -1);

    // Set a key with EX, TTL returns positive value
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("ttl-check")
        .arg("val")
        .arg("EX")
        .arg("300")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("ttl-check").query_async(&mut con).await;
    match result {
        Ok(ttl) if ttl > 0 && ttl <= 300 => t.pass("TTL returns positive seconds"),
        Ok(ttl) => t.fail("TTL returns positive seconds", &format!("expected 1..=300, got {ttl}")),
        Err(e) => t.fail("TTL returns positive seconds", &format!("error: {e}")),
    }

    // PTTL on nonexistent returns -2
    let result: redis::RedisResult<i64> = redis::cmd("PTTL").arg("no-such-key").query_async(&mut con).await;
    t.check_eq("PTTL nonexistent returns -2", result, -2);

    // PTTL on key with no expiry returns -1
    let result: redis::RedisResult<i64> = redis::cmd("PTTL").arg("no-ttl-key").query_async(&mut con).await;
    t.check_eq("PTTL no-expiry returns -1", result, -1);

    // PTTL on key with TTL returns positive ms
    let result: redis::RedisResult<i64> = redis::cmd("PTTL").arg("ttl-check").query_async(&mut con).await;
    match result {
        Ok(pttl) if pttl > 0 && pttl <= 300_000 => t.pass("PTTL returns positive ms"),
        Ok(pttl) => t.fail("PTTL returns positive ms", &format!("expected 1..=300000, got {pttl}")),
        Err(e) => t.fail("PTTL returns positive ms", &format!("error: {e}")),
    }

    // TTL error: wrong arity
    let result: redis::RedisResult<i64> = redis::cmd("TTL").query_async(&mut con).await;
    t.check_err("TTL wrong arity (0 args)", result);

    let result: redis::RedisResult<i64> = redis::cmd("PTTL").query_async(&mut con).await;
    t.check_err("PTTL wrong arity (0 args)", result);

    // -- EXPIRE / PEXPIRE ---------------------------------------------------
    println!("EXPIRE / PEXPIRE");

    // EXPIRE on existing key returns 1
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRE")
        .arg("no-ttl-key")
        .arg("60")
        .query_async(&mut con)
        .await;
    t.check_eq("EXPIRE existing key returns 1", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("no-ttl-key").query_async(&mut con).await;
    match result {
        Ok(ttl) if ttl > 0 && ttl <= 60 => t.pass("TTL after EXPIRE in range"),
        Ok(ttl) => t.fail("TTL after EXPIRE in range", &format!("expected 1..=60, got {ttl}")),
        Err(e) => t.fail("TTL after EXPIRE in range", &format!("error: {e}")),
    }

    // EXPIRE on nonexistent key returns 0
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRE")
        .arg("no-such-key")
        .arg("60")
        .query_async(&mut con)
        .await;
    t.check_eq("EXPIRE nonexistent returns 0", result, 0);

    // PEXPIRE sets TTL in ms
    let result: redis::RedisResult<i64> = redis::cmd("PEXPIRE")
        .arg("ttl-check")
        .arg("60000")
        .query_async(&mut con)
        .await;
    t.check_eq("PEXPIRE existing key returns 1", result, 1);

    // EXPIRE error cases
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRE")
        .arg("no-ttl-key")
        .arg("-5")
        .query_async(&mut con)
        .await;
    t.check_err("EXPIRE negative seconds", result);

    let result: redis::RedisResult<i64> = redis::cmd("EXPIRE")
        .arg("no-ttl-key")
        .arg("notanumber")
        .query_async(&mut con)
        .await;
    t.check_err("EXPIRE non-integer", result);

    let result: redis::RedisResult<i64> = redis::cmd("EXPIRE").query_async(&mut con).await;
    t.check_err("EXPIRE wrong arity (0 args)", result);

    // -- EXPIREAT / PEXPIREAT -----------------------------------------------
    println!("EXPIREAT / PEXPIREAT");

    // EXPIREAT with a future timestamp
    let future_secs: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let result: redis::RedisResult<i64> = redis::cmd("EXPIREAT")
        .arg("ttl-check")
        .arg(future_secs)
        .query_async(&mut con)
        .await;
    t.check_eq("EXPIREAT future timestamp returns 1", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("EXPIRETIME").arg("ttl-check").query_async(&mut con).await;
    match result {
        Ok(ts) if ts > 0 => t.pass("EXPIRETIME returns positive timestamp"),
        Ok(ts) => t.fail("EXPIRETIME returns positive timestamp", &format!("got {ts}")),
        Err(e) => t.fail("EXPIRETIME returns positive timestamp", &format!("error: {e}")),
    }

    // PEXPIREAT with a future ms timestamp
    let future_ms: u128 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 300_000;
    let result: redis::RedisResult<i64> = redis::cmd("PEXPIREAT")
        .arg("no-ttl-key")
        .arg(future_ms as u64)
        .query_async(&mut con)
        .await;
    t.check_eq("PEXPIREAT future timestamp returns 1", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("PEXPIRETIME").arg("no-ttl-key").query_async(&mut con).await;
    match result {
        Ok(ts) if ts > 0 => t.pass("PEXPIRETIME returns positive timestamp"),
        Ok(ts) => t.fail("PEXPIRETIME returns positive timestamp", &format!("got {ts}")),
        Err(e) => t.fail("PEXPIRETIME returns positive timestamp", &format!("error: {e}")),
    }

    // EXPIREAT on nonexistent returns 0
    let result: redis::RedisResult<i64> = redis::cmd("EXPIREAT")
        .arg("no-such-key")
        .arg(future_secs)
        .query_async(&mut con)
        .await;
    t.check_eq("EXPIREAT nonexistent returns 0", result, 0);

    // EXPIREAT negative timestamp is error
    let result: redis::RedisResult<i64> = redis::cmd("EXPIREAT")
        .arg("ttl-check")
        .arg("-1")
        .query_async(&mut con)
        .await;
    t.check_err("EXPIREAT negative timestamp", result);

    // -- EXPIRETIME / PEXPIRETIME -------------------------------------------
    println!("EXPIRETIME / PEXPIRETIME");

    // EXPIRETIME on nonexistent returns -2
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRETIME").arg("no-such-key").query_async(&mut con).await;
    t.check_eq("EXPIRETIME nonexistent returns -2", result, -2);

    // PEXPIRETIME on nonexistent returns -2
    let result: redis::RedisResult<i64> = redis::cmd("PEXPIRETIME").arg("no-such-key").query_async(&mut con).await;
    t.check_eq("PEXPIRETIME nonexistent returns -2", result, -2);

    // Set a key with no TTL, EXPIRETIME returns -1
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("no-ttl-key2")
        .arg("val")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRETIME").arg("no-ttl-key2").query_async(&mut con).await;
    t.check_eq("EXPIRETIME no-expiry returns -1", result, -1);

    let result: redis::RedisResult<i64> = redis::cmd("PEXPIRETIME").arg("no-ttl-key2").query_async(&mut con).await;
    t.check_eq("PEXPIRETIME no-expiry returns -1", result, -1);

    // Wrong arity
    let result: redis::RedisResult<i64> = redis::cmd("EXPIRETIME").query_async(&mut con).await;
    t.check_err("EXPIRETIME wrong arity (0 args)", result);

    let result: redis::RedisResult<i64> = redis::cmd("PEXPIRETIME").query_async(&mut con).await;
    t.check_err("PEXPIRETIME wrong arity (0 args)", result);

    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("no-ttl-key2").query_async(&mut con).await;

    // -- PERSIST ------------------------------------------------------------
    println!("PERSIST");

    // PERSIST removes TTL, returns 1
    let result: redis::RedisResult<i64> = redis::cmd("PERSIST").arg("ttl-check").query_async(&mut con).await;
    t.check_eq("PERSIST key with TTL returns 1", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("ttl-check").query_async(&mut con).await;
    t.check_eq("TTL after PERSIST returns -1", result, -1);

    // PERSIST on key with no TTL returns 0
    let result: redis::RedisResult<i64> = redis::cmd("PERSIST").arg("ttl-check").query_async(&mut con).await;
    t.check_eq("PERSIST no-TTL returns 0", result, 0);

    // PERSIST on nonexistent returns 0
    let result: redis::RedisResult<i64> = redis::cmd("PERSIST").arg("no-such-key").query_async(&mut con).await;
    t.check_eq("PERSIST nonexistent returns 0", result, 0);

    // -- TYPE ---------------------------------------------------------------
    println!("TYPE");

    let result: redis::RedisResult<String> = redis::cmd("TYPE").arg("ttl-check").query_async(&mut con).await;
    t.check_eq("TYPE string key", result, "string".to_string());

    let result: redis::RedisResult<String> = redis::cmd("TYPE").arg("no-such-key").query_async(&mut con).await;
    t.check_eq("TYPE nonexistent returns none", result, "none".to_string());

    let result: redis::RedisResult<String> = redis::cmd("TYPE").query_async(&mut con).await;
    t.check_err("TYPE wrong arity (0 args)", result);

    // -- RENAME / RENAMENX --------------------------------------------------
    println!("RENAME / RENAMENX");

    // Set up key for rename
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("rename-src")
        .arg("rename-val")
        .arg("EX")
        .arg("300")
        .query_async(&mut con)
        .await;

    // RENAME moves key and preserves value
    let result: redis::RedisResult<String> = redis::cmd("RENAME")
        .arg("rename-src")
        .arg("rename-dst")
        .query_async(&mut con)
        .await;
    t.check_eq("RENAME returns OK", result, "OK".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("rename-dst").query_async(&mut con).await;
    t.check_eq("GET rename-dst after RENAME", result, "rename-val".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("EXISTS").arg("rename-src").query_async(&mut con).await;
    t.check_eq("rename-src gone after RENAME", result, 0);

    // RENAME preserves TTL
    let result: redis::RedisResult<i64> = redis::cmd("TTL").arg("rename-dst").query_async(&mut con).await;
    match result {
        Ok(ttl) if ttl > 0 => t.pass("RENAME preserves TTL"),
        Ok(ttl) => t.fail("RENAME preserves TTL", &format!("expected >0, got {ttl}")),
        Err(e) => t.fail("RENAME preserves TTL", &format!("error: {e}")),
    }

    // RENAME nonexistent source returns error
    let result: redis::RedisResult<String> = redis::cmd("RENAME")
        .arg("no-such-key")
        .arg("dst")
        .query_async(&mut con)
        .await;
    t.check_err("RENAME nonexistent source", result);

    // RENAME wrong arity
    let result: redis::RedisResult<String> = redis::cmd("RENAME").arg("only-one").query_async(&mut con).await;
    t.check_err("RENAME wrong arity (1 arg)", result);

    // RENAMENX succeeds when dst missing
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("rnx-src")
        .arg("rnx-val")
        .query_async(&mut con)
        .await;
    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("rnx-dst").query_async(&mut con).await;
    let result: redis::RedisResult<i64> = redis::cmd("RENAMENX")
        .arg("rnx-src")
        .arg("rnx-dst")
        .query_async(&mut con)
        .await;
    t.check_eq("RENAMENX dst missing returns 1", result, 1);

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("rnx-dst").query_async(&mut con).await;
    t.check_eq("GET rnx-dst after RENAMENX", result, "rnx-val".to_string());

    // RENAMENX fails when dst exists
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("rnx-src2")
        .arg("val2")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("RENAMENX")
        .arg("rnx-src2")
        .arg("rnx-dst")
        .query_async(&mut con)
        .await;
    t.check_eq("RENAMENX dst exists returns 0", result, 0);

    // cleanup rename keys
    let _: redis::RedisResult<i64> = redis::cmd("DEL")
        .arg("rename-dst")
        .arg("rnx-dst")
        .arg("rnx-src2")
        .query_async(&mut con)
        .await;

    // -- UNLINK / TOUCH -----------------------------------------------------
    println!("UNLINK / TOUCH");

    let _: redis::RedisResult<String> = redis::cmd("MSET")
        .arg("ul1")
        .arg("v1")
        .arg("ul2")
        .arg("v2")
        .arg("ul3")
        .arg("v3")
        .query_async(&mut con)
        .await;

    // UNLINK deletes keys, returns count
    let result: redis::RedisResult<i64> = redis::cmd("UNLINK")
        .arg("ul1")
        .arg("ul2")
        .arg("ul-nonexistent")
        .query_async(&mut con)
        .await;
    t.check_eq("UNLINK returns deleted count", result, 2);

    // TOUCH returns count of existing keys
    let result: redis::RedisResult<i64> = redis::cmd("TOUCH")
        .arg("ul3")
        .arg("ul-nonexistent")
        .query_async(&mut con)
        .await;
    t.check_eq("TOUCH returns existing count", result, 1);

    // UNLINK wrong arity
    let result: redis::RedisResult<i64> = redis::cmd("UNLINK").query_async(&mut con).await;
    t.check_err("UNLINK wrong arity (0 args)", result);

    // TOUCH wrong arity
    let result: redis::RedisResult<i64> = redis::cmd("TOUCH").query_async(&mut con).await;
    t.check_err("TOUCH wrong arity (0 args)", result);

    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("ul3").query_async(&mut con).await;

    // -- DBSIZE -------------------------------------------------------------
    println!("DBSIZE");

    // Switch to a clean database for DBSIZE testing
    let result: redis::RedisResult<String> = redis::cmd("SELECT").arg("15").query_async(&mut con).await;
    t.check_eq("SELECT 15", result, "OK".to_string());

    let _: redis::RedisResult<String> = redis::cmd("FLUSHDB").query_async(&mut con).await;

    let result: redis::RedisResult<i64> = redis::cmd("DBSIZE").query_async(&mut con).await;
    t.check_eq("DBSIZE empty db", result, 0);

    let _: redis::RedisResult<String> = redis::cmd("MSET")
        .arg("db-a")
        .arg("1")
        .arg("db-b")
        .arg("2")
        .arg("db-c")
        .arg("3")
        .query_async(&mut con)
        .await;

    let result: redis::RedisResult<i64> = redis::cmd("DBSIZE").query_async(&mut con).await;
    t.check_eq("DBSIZE after 3 keys", result, 3);

    let result: redis::RedisResult<i64> = redis::cmd("DBSIZE").arg("extra").query_async(&mut con).await;
    t.check_err("DBSIZE wrong arity (1 arg)", result);

    // cleanup: flush and return to db 0
    let _: redis::RedisResult<String> = redis::cmd("FLUSHDB").query_async(&mut con).await;
    let _: redis::RedisResult<String> = redis::cmd("SELECT").arg("0").query_async(&mut con).await;

    // -- SELECT -------------------------------------------------------------
    println!("SELECT");

    // Write in db 0, select db 1, verify key absent, return to db 0
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("select-test")
        .arg("in-db0")
        .query_async(&mut con)
        .await;

    let result: redis::RedisResult<String> = redis::cmd("SELECT").arg("1").query_async(&mut con).await;
    t.check_eq("SELECT 1", result, "OK".to_string());

    let result: redis::RedisResult<Option<String>> = redis::cmd("GET").arg("select-test").query_async(&mut con).await;
    t.check_eq("GET select-test in db 1 is nil", result, None);

    let result: redis::RedisResult<String> = redis::cmd("SELECT").arg("0").query_async(&mut con).await;
    t.check_eq("SELECT 0", result, "OK".to_string());

    let result: redis::RedisResult<String> = redis::cmd("GET").arg("select-test").query_async(&mut con).await;
    t.check_eq("GET select-test back in db 0", result, "in-db0".to_string());

    // SELECT error cases
    let result: redis::RedisResult<String> = redis::cmd("SELECT").arg("16").query_async(&mut con).await;
    t.check_err("SELECT out of range (16)", result);

    let result: redis::RedisResult<String> = redis::cmd("SELECT").arg("-1").query_async(&mut con).await;
    t.check_err("SELECT negative", result);

    let result: redis::RedisResult<String> = redis::cmd("SELECT").arg("notanumber").query_async(&mut con).await;
    t.check_err("SELECT non-integer", result);

    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("select-test").query_async(&mut con).await;

    // -- FLUSHDB / FLUSHALL -------------------------------------------------
    println!("FLUSHDB / FLUSHALL");

    // FLUSHDB clears current db only
    let result: redis::RedisResult<String> = redis::cmd("SELECT").arg("14").query_async(&mut con).await;
    t.check_eq("SELECT 14 for FLUSHDB test", result, "OK".to_string());

    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("flush-key")
        .arg("flush-val")
        .query_async(&mut con)
        .await;

    let result: redis::RedisResult<String> = redis::cmd("FLUSHDB").query_async(&mut con).await;
    t.check_eq("FLUSHDB returns OK", result, "OK".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("DBSIZE").query_async(&mut con).await;
    t.check_eq("DBSIZE after FLUSHDB", result, 0);

    // FLUSHALL clears all dbs: set keys in db 13 and 14, flushall, verify both empty
    let _: redis::RedisResult<String> = redis::cmd("SELECT").arg("13").query_async(&mut con).await;
    let _: redis::RedisResult<String> = redis::cmd("SET").arg("fa-key-13").arg("v").query_async(&mut con).await;
    let _: redis::RedisResult<String> = redis::cmd("SELECT").arg("14").query_async(&mut con).await;
    let _: redis::RedisResult<String> = redis::cmd("SET").arg("fa-key-14").arg("v").query_async(&mut con).await;

    let result: redis::RedisResult<String> = redis::cmd("FLUSHALL").query_async(&mut con).await;
    t.check_eq("FLUSHALL returns OK", result, "OK".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("DBSIZE").query_async(&mut con).await;
    t.check_eq("DBSIZE db 14 after FLUSHALL", result, 0);

    let _: redis::RedisResult<String> = redis::cmd("SELECT").arg("13").query_async(&mut con).await;
    let result: redis::RedisResult<i64> = redis::cmd("DBSIZE").query_async(&mut con).await;
    t.check_eq("DBSIZE db 13 after FLUSHALL", result, 0);

    // Return to db 0
    let _: redis::RedisResult<String> = redis::cmd("SELECT").arg("0").query_async(&mut con).await;

    // FLUSHDB wrong arity
    let result: redis::RedisResult<String> = redis::cmd("FLUSHDB").arg("a").arg("b").query_async(&mut con).await;
    t.check_err("FLUSHDB wrong arity (2 args)", result);

    // cleanup TTL test keys still in db 0
    let _: redis::RedisResult<i64> = redis::cmd("DEL")
        .arg("ttl-check")
        .arg("no-ttl-key")
        .query_async(&mut con)
        .await;

    // -- Hash Commands ------------------------------------------------------
    println!("Hash Commands");

    let result: redis::RedisResult<i64> = redis::cmd("HSET")
        .arg("smoke_hash")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .query_async(&mut con)
        .await;
    t.check_eq("HSET multi-field", result, 2);

    let result: redis::RedisResult<String> = redis::cmd("HGET")
        .arg("smoke_hash")
        .arg("f1")
        .query_async(&mut con)
        .await;
    t.check_eq("HGET", result, "v1".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("HLEN").arg("smoke_hash").query_async(&mut con).await;
    t.check_eq("HLEN", result, 2);

    let result: redis::RedisResult<i64> = redis::cmd("HEXISTS")
        .arg("smoke_hash")
        .arg("f1")
        .query_async(&mut con)
        .await;
    t.check_eq("HEXISTS", result, 1);

    let result: redis::RedisResult<std::collections::HashMap<String, String>> =
        redis::cmd("HGETALL").arg("smoke_hash").query_async(&mut con).await;
    match result {
        Ok(map) if map.len() == 2 && map.get("f1") == Some(&"v1".to_string()) => {
            t.pass("HGETALL");
        }
        Ok(map) => t.fail("HGETALL", &format!("wrong map: {map:?}")),
        Err(e) => t.fail("HGETALL", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<Vec<String>> = redis::cmd("HKEYS").arg("smoke_hash").query_async(&mut con).await;
    match result {
        Ok(mut keys) => {
            keys.sort();
            if keys == vec!["f1", "f2"] {
                t.pass("HKEYS");
            } else {
                t.fail("HKEYS", &format!("wrong keys: {keys:?}"));
            }
        }
        Err(e) => t.fail("HKEYS", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<Vec<String>> = redis::cmd("HVALS").arg("smoke_hash").query_async(&mut con).await;
    match result {
        Ok(mut vals) => {
            vals.sort();
            if vals == vec!["v1", "v2"] {
                t.pass("HVALS");
            } else {
                t.fail("HVALS", &format!("wrong vals: {vals:?}"));
            }
        }
        Err(e) => t.fail("HVALS", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<Vec<Option<String>>> = redis::cmd("HMGET")
        .arg("smoke_hash")
        .arg("f1")
        .arg("nope")
        .arg("f2")
        .query_async(&mut con)
        .await;
    match result {
        Ok(vals) if vals == vec![Some("v1".to_string()), None, Some("v2".to_string())] => {
            t.pass("HMGET");
        }
        Ok(vals) => t.fail("HMGET", &format!("wrong results: {vals:?}")),
        Err(e) => t.fail("HMGET", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<i64> = redis::cmd("HINCRBY")
        .arg("smoke_hash")
        .arg("counter")
        .arg(10)
        .query_async(&mut con)
        .await;
    t.check_eq("HINCRBY", result, 10);

    let result: redis::RedisResult<String> = redis::cmd("HINCRBYFLOAT")
        .arg("smoke_hash")
        .arg("float_f")
        .arg("1.5")
        .query_async(&mut con)
        .await;
    t.check_eq("HINCRBYFLOAT", result, "1.5".to_string());

    let result: redis::RedisResult<i64> = redis::cmd("HSETNX")
        .arg("smoke_hash")
        .arg("f1")
        .arg("new")
        .query_async(&mut con)
        .await;
    t.check_eq("HSETNX existing", result, 0);

    let result: redis::RedisResult<i64> = redis::cmd("HSETNX")
        .arg("smoke_hash")
        .arg("newf")
        .arg("newv")
        .query_async(&mut con)
        .await;
    t.check_eq("HSETNX new", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("HSTRLEN")
        .arg("smoke_hash")
        .arg("f1")
        .query_async(&mut con)
        .await;
    t.check_eq("HSTRLEN", result, 2); // "v1" is 2 bytes

    let result: redis::RedisResult<i64> = redis::cmd("HDEL")
        .arg("smoke_hash")
        .arg("f1")
        .arg("f2")
        .query_async(&mut con)
        .await;
    t.check_eq("HDEL", result, 2);

    let result: redis::RedisResult<String> = redis::cmd("HRANDFIELD").arg("smoke_hash").query_async(&mut con).await;
    match result {
        Ok(field) if ["counter", "float_f", "newf"].contains(&field.as_str()) => {
            t.pass("HRANDFIELD");
        }
        Ok(field) => t.fail("HRANDFIELD", &format!("unexpected field: {field}")),
        Err(e) => t.fail("HRANDFIELD", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<String> = redis::cmd("HMSET")
        .arg("smoke_hmset")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .query_async(&mut con)
        .await;
    t.check_eq("HMSET", result, "OK".to_string());

    // -- Set Commands ---------------------------------------------------------
    println!("Set Commands");

    let result: redis::RedisResult<i64> = redis::cmd("SADD")
        .arg("smoke_set")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await;
    t.check_eq("SADD multi-member", result, 3);

    let result: redis::RedisResult<i64> = redis::cmd("SADD")
        .arg("smoke_set")
        .arg("a")
        .arg("d")
        .query_async(&mut con)
        .await;
    t.check_eq("SADD partial overlap", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("SCARD").arg("smoke_set").query_async(&mut con).await;
    t.check_eq("SCARD", result, 4);

    let result: redis::RedisResult<i64> = redis::cmd("SISMEMBER")
        .arg("smoke_set")
        .arg("b")
        .query_async(&mut con)
        .await;
    t.check_eq("SISMEMBER exists", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("SISMEMBER")
        .arg("smoke_set")
        .arg("z")
        .query_async(&mut con)
        .await;
    t.check_eq("SISMEMBER missing", result, 0);

    let result: redis::RedisResult<Vec<i64>> = redis::cmd("SMISMEMBER")
        .arg("smoke_set")
        .arg("a")
        .arg("z")
        .arg("c")
        .query_async(&mut con)
        .await;
    t.check_eq("SMISMEMBER", result, vec![1, 0, 1]);

    let result: redis::RedisResult<Vec<String>> = redis::cmd("SMEMBERS").arg("smoke_set").query_async(&mut con).await;
    match result {
        Ok(mut members) => {
            members.sort();
            if members == vec!["a", "b", "c", "d"] {
                t.pass("SMEMBERS");
            } else {
                t.fail("SMEMBERS", &format!("wrong members: {members:?}"));
            }
        }
        Err(e) => t.fail("SMEMBERS", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<i64> = redis::cmd("SREM")
        .arg("smoke_set")
        .arg("d")
        .arg("z")
        .query_async(&mut con)
        .await;
    t.check_eq("SREM", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("SCARD").arg("smoke_set").query_async(&mut con).await;
    t.check_eq("SCARD after SREM", result, 3);

    // SPOP: pop one member and verify it was removed.
    let result: redis::RedisResult<String> = redis::cmd("SPOP").arg("smoke_set").query_async(&mut con).await;
    match result {
        Ok(member) if ["a", "b", "c"].contains(&member.as_str()) => {
            t.pass("SPOP");
        }
        Ok(member) => t.fail("SPOP", &format!("unexpected member: {member}")),
        Err(e) => t.fail("SPOP", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<i64> = redis::cmd("SCARD").arg("smoke_set").query_async(&mut con).await;
    t.check_eq("SCARD after SPOP", result, 2);

    // SRANDMEMBER: should return a member without removing it.
    let result: redis::RedisResult<String> = redis::cmd("SRANDMEMBER").arg("smoke_set").query_async(&mut con).await;
    match result {
        Ok(_member) => t.pass("SRANDMEMBER"),
        Err(e) => t.fail("SRANDMEMBER", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<i64> = redis::cmd("SCARD").arg("smoke_set").query_async(&mut con).await;
    t.check_eq("SCARD after SRANDMEMBER (unchanged)", result, 2);

    // SMOVE
    let _: redis::RedisResult<i64> = redis::cmd("SADD")
        .arg("smoke_src")
        .arg("x")
        .arg("y")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("SMOVE")
        .arg("smoke_src")
        .arg("smoke_dst")
        .arg("x")
        .query_async(&mut con)
        .await;
    t.check_eq("SMOVE", result, 1);

    let result: redis::RedisResult<i64> = redis::cmd("SISMEMBER")
        .arg("smoke_dst")
        .arg("x")
        .query_async(&mut con)
        .await;
    t.check_eq("SMOVE destination has member", result, 1);

    // SINTER / SUNION / SDIFF
    let _: redis::RedisResult<i64> = redis::cmd("SADD")
        .arg("smoke_a")
        .arg("1")
        .arg("2")
        .arg("3")
        .query_async(&mut con)
        .await;
    let _: redis::RedisResult<i64> = redis::cmd("SADD")
        .arg("smoke_b")
        .arg("2")
        .arg("3")
        .arg("4")
        .query_async(&mut con)
        .await;

    let result: redis::RedisResult<Vec<String>> = redis::cmd("SINTER")
        .arg("smoke_a")
        .arg("smoke_b")
        .query_async(&mut con)
        .await;
    match result {
        Ok(mut members) => {
            members.sort();
            if members == vec!["2", "3"] {
                t.pass("SINTER");
            } else {
                t.fail("SINTER", &format!("wrong result: {members:?}"));
            }
        }
        Err(e) => t.fail("SINTER", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<Vec<String>> = redis::cmd("SUNION")
        .arg("smoke_a")
        .arg("smoke_b")
        .query_async(&mut con)
        .await;
    match result {
        Ok(mut members) => {
            members.sort();
            if members == vec!["1", "2", "3", "4"] {
                t.pass("SUNION");
            } else {
                t.fail("SUNION", &format!("wrong result: {members:?}"));
            }
        }
        Err(e) => t.fail("SUNION", &format!("error: {e:?}")),
    }

    let result: redis::RedisResult<Vec<String>> = redis::cmd("SDIFF")
        .arg("smoke_a")
        .arg("smoke_b")
        .query_async(&mut con)
        .await;
    match result {
        Ok(members) if members == vec!["1".to_string()] => t.pass("SDIFF"),
        Ok(members) => t.fail("SDIFF", &format!("wrong result: {members:?}")),
        Err(e) => t.fail("SDIFF", &format!("error: {e:?}")),
    }

    // SINTERSTORE / SUNIONSTORE / SDIFFSTORE
    let result: redis::RedisResult<i64> = redis::cmd("SINTERSTORE")
        .arg("smoke_inter_dest")
        .arg("smoke_a")
        .arg("smoke_b")
        .query_async(&mut con)
        .await;
    t.check_eq("SINTERSTORE", result, 2);

    let result: redis::RedisResult<i64> = redis::cmd("SUNIONSTORE")
        .arg("smoke_union_dest")
        .arg("smoke_a")
        .arg("smoke_b")
        .query_async(&mut con)
        .await;
    t.check_eq("SUNIONSTORE", result, 4);

    let result: redis::RedisResult<i64> = redis::cmd("SDIFFSTORE")
        .arg("smoke_diff_dest")
        .arg("smoke_a")
        .arg("smoke_b")
        .query_async(&mut con)
        .await;
    t.check_eq("SDIFFSTORE", result, 1);

    // SINTERCARD
    let result: redis::RedisResult<i64> = redis::cmd("SINTERCARD")
        .arg(2)
        .arg("smoke_a")
        .arg("smoke_b")
        .query_async(&mut con)
        .await;
    t.check_eq("SINTERCARD", result, 2);

    let result: redis::RedisResult<i64> = redis::cmd("SINTERCARD")
        .arg(2)
        .arg("smoke_a")
        .arg("smoke_b")
        .arg("LIMIT")
        .arg(1)
        .query_async(&mut con)
        .await;
    t.check_eq("SINTERCARD LIMIT", result, 1);

    // WRONGTYPE: SADD on string key
    let _: redis::RedisResult<()> = redis::cmd("SET")
        .arg("smoke_str_for_set")
        .arg("hello")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("SADD")
        .arg("smoke_str_for_set")
        .arg("member")
        .query_async(&mut con)
        .await;
    match result {
        Err(e) if format!("{e}").contains("WRONGTYPE") => t.pass("SADD WRONGTYPE on string"),
        Err(e) => t.fail("SADD WRONGTYPE on string", &format!("wrong error: {e:?}")),
        Ok(v) => t.fail("SADD WRONGTYPE on string", &format!("expected error, got: {v}")),
    }

    // -- List Commands ------------------------------------------------------
    println!("List Commands");

    // LPUSH / RPUSH / LLEN
    let result: redis::RedisResult<i64> = redis::cmd("LPUSH")
        .arg("smoke_list")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut con)
        .await;
    t.check_eq("LPUSH three elements", result, 3);

    let result: redis::RedisResult<i64> = redis::cmd("RPUSH")
        .arg("smoke_list")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await;
    t.check_eq("RPUSH two elements", result, 5);

    let result: redis::RedisResult<i64> = redis::cmd("LLEN").arg("smoke_list").query_async(&mut con).await;
    t.check_eq("LLEN after push", result, 5);

    // LRANGE: LPUSH reversed order + RPUSH appended -> [c, b, a, d, e]
    let result: redis::RedisResult<Vec<String>> = redis::cmd("LRANGE")
        .arg("smoke_list")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await;
    t.check_eq(
        "LRANGE full list",
        result,
        vec!["c".to_string(), "b".into(), "a".into(), "d".into(), "e".into()],
    );

    // LINDEX positive + negative
    let result: redis::RedisResult<String> = redis::cmd("LINDEX")
        .arg("smoke_list")
        .arg(0)
        .query_async(&mut con)
        .await;
    t.check_eq("LINDEX 0", result, "c".to_string());

    let result: redis::RedisResult<String> = redis::cmd("LINDEX")
        .arg("smoke_list")
        .arg(-1)
        .query_async(&mut con)
        .await;
    t.check_eq("LINDEX -1", result, "e".to_string());

    // LINDEX out of range returns Nil
    let result: redis::RedisResult<Option<String>> = redis::cmd("LINDEX")
        .arg("smoke_list")
        .arg(100)
        .query_async(&mut con)
        .await;
    t.check_eq("LINDEX out of range", result, None::<String>);

    // LPOP / RPOP single
    let result: redis::RedisResult<String> = redis::cmd("LPOP").arg("smoke_list").query_async(&mut con).await;
    t.check_eq("LPOP single", result, "c".to_string());

    let result: redis::RedisResult<String> = redis::cmd("RPOP").arg("smoke_list").query_async(&mut con).await;
    t.check_eq("RPOP single", result, "e".to_string());

    // LPOP with count
    let result: redis::RedisResult<Vec<String>> =
        redis::cmd("LPOP").arg("smoke_list").arg(2).query_async(&mut con).await;
    t.check_eq("LPOP with count=2", result, vec!["b".to_string(), "a".into()]);

    // LSET + LINDEX round-trip
    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("smoke_list").query_async(&mut con).await;
    let _: redis::RedisResult<i64> = redis::cmd("RPUSH")
        .arg("smoke_list")
        .arg("x")
        .arg("y")
        .arg("z")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<String> = redis::cmd("LSET")
        .arg("smoke_list")
        .arg(1)
        .arg("Y")
        .query_async(&mut con)
        .await;
    t.check_eq("LSET OK", result, "OK".to_string());
    let result: redis::RedisResult<String> = redis::cmd("LINDEX")
        .arg("smoke_list")
        .arg(1)
        .query_async(&mut con)
        .await;
    t.check_eq("LINDEX after LSET", result, "Y".to_string());

    // LSET out of range
    let result: redis::RedisResult<String> = redis::cmd("LSET")
        .arg("smoke_list")
        .arg(100)
        .arg("z")
        .query_async(&mut con)
        .await;
    t.check_err("LSET out of range errors", result);

    // LTRIM
    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("smoke_list").query_async(&mut con).await;
    let _: redis::RedisResult<i64> = redis::cmd("RPUSH")
        .arg("smoke_list")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<String> = redis::cmd("LTRIM")
        .arg("smoke_list")
        .arg(1)
        .arg(3)
        .query_async(&mut con)
        .await;
    t.check_eq("LTRIM 1..3", result, "OK".to_string());
    let result: redis::RedisResult<Vec<String>> = redis::cmd("LRANGE")
        .arg("smoke_list")
        .arg(0)
        .arg(-1)
        .query_async(&mut con)
        .await;
    t.check_eq(
        "LRANGE after LTRIM",
        result,
        vec!["b".to_string(), "c".into(), "d".into()],
    );

    // LINSERT before/after
    let result: redis::RedisResult<i64> = redis::cmd("LINSERT")
        .arg("smoke_list")
        .arg("BEFORE")
        .arg("c")
        .arg("BB")
        .query_async(&mut con)
        .await;
    t.check_eq("LINSERT BEFORE", result, 4);

    let result: redis::RedisResult<i64> = redis::cmd("LINSERT")
        .arg("smoke_list")
        .arg("AFTER")
        .arg("c")
        .arg("CC")
        .query_async(&mut con)
        .await;
    t.check_eq("LINSERT AFTER", result, 5);

    // LINSERT missing pivot
    let result: redis::RedisResult<i64> = redis::cmd("LINSERT")
        .arg("smoke_list")
        .arg("BEFORE")
        .arg("nope")
        .arg("X")
        .query_async(&mut con)
        .await;
    t.check_eq("LINSERT missing pivot returns -1", result, -1);

    // LREM
    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("smoke_list").query_async(&mut con).await;
    let _: redis::RedisResult<i64> = redis::cmd("RPUSH")
        .arg("smoke_list")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("b")
        .arg("a")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("LREM")
        .arg("smoke_list")
        .arg(2)
        .arg("a")
        .query_async(&mut con)
        .await;
    t.check_eq("LREM count=2", result, 2);

    let result: redis::RedisResult<i64> = redis::cmd("LREM")
        .arg("smoke_list")
        .arg(0)
        .arg("a")
        .query_async(&mut con)
        .await;
    t.check_eq("LREM count=0 removes remaining match", result, 1);

    // LPOS
    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("smoke_list").query_async(&mut con).await;
    let _: redis::RedisResult<i64> = redis::cmd("RPUSH")
        .arg("smoke_list")
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("c")
        .arg("a")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("LPOS")
        .arg("smoke_list")
        .arg("a")
        .query_async(&mut con)
        .await;
    t.check_eq("LPOS first match", result, 0);

    let result: redis::RedisResult<Vec<i64>> = redis::cmd("LPOS")
        .arg("smoke_list")
        .arg("a")
        .arg("COUNT")
        .arg(0)
        .query_async(&mut con)
        .await;
    t.check_eq("LPOS COUNT=0 returns all", result, vec![0i64, 2, 4]);

    // LPUSHX / RPUSHX on nonexistent key: no-op
    let result: redis::RedisResult<i64> = redis::cmd("LPUSHX")
        .arg("smoke_list_missing")
        .arg("x")
        .query_async(&mut con)
        .await;
    t.check_eq("LPUSHX on missing key", result, 0);

    // WRONGTYPE: LPUSH on string key
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("smoke_str_for_list")
        .arg("hi")
        .query_async(&mut con)
        .await;
    let result: redis::RedisResult<i64> = redis::cmd("LPUSH")
        .arg("smoke_str_for_list")
        .arg("x")
        .query_async(&mut con)
        .await;
    match result {
        Err(e) if format!("{e}").contains("WRONGTYPE") => t.pass("LPUSH WRONGTYPE on string"),
        Err(e) => t.fail("LPUSH WRONGTYPE on string", &format!("wrong error: {e:?}")),
        Ok(v) => t.fail("LPUSH WRONGTYPE on string", &format!("expected error, got: {v}")),
    }

    // Empty-list auto-deletion: pop all, verify EXISTS == 0
    let _: redis::RedisResult<i64> = redis::cmd("DEL").arg("smoke_list").query_async(&mut con).await;
    let _: redis::RedisResult<i64> = redis::cmd("RPUSH")
        .arg("smoke_list")
        .arg("x")
        .query_async(&mut con)
        .await;
    let _: redis::RedisResult<String> = redis::cmd("LPOP").arg("smoke_list").query_async(&mut con).await;
    let result: redis::RedisResult<i64> = redis::cmd("EXISTS").arg("smoke_list").query_async(&mut con).await;
    t.check_eq("EXISTS=0 after final pop", result, 0);

    // -- Post-error liveness check ------------------------------------------
    let result: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut con).await;
    t.check_eq("server alive after errors", result, "PONG".to_string());

    // -- QUIT (open a separate connection for this) -------------------------
    println!("QUIT");

    let mut quit_con = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            t.fail("QUIT", &format!("failed to open second connection: {e}"));
            return t.summary();
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

    // -- Summary ------------------------------------------------------------
    t.summary()
}

// ===========================================================================
// Phase 2: Load
// ===========================================================================

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

// ---------------------------------------------------------------------------
// Workload distribution
// ---------------------------------------------------------------------------
//
// Weights loosely model a read-heavy web-app workload:
//
//   GET          31%    -- point reads dominate
//   SET          15%    -- writes (some with TTL, NX, XX, GET)
//   MGET          8%    -- batch reads
//   MSET          4%    -- batch writes
//   INCR/DECR     8%    -- counters
//   INCRBY/DECRBY 4%    -- variable counters
//   INCRBYFLOAT   2%    -- float counters
//   EXISTS        5%    -- key existence checks
//   DEL           4%    -- deletions
//   APPEND        3%    -- append-only logs
//   STRLEN        2%    -- length checks
//   GETRANGE      2%    -- substring reads
//   SETRANGE      2%    -- substring writes
//   SETNX         2%    -- conditional sets
//   SETEX/PSETEX  2%    -- sets with TTL
//   GETDEL        1%    -- atomic get-and-delete
//   TTL ops       4%    -- TTL management (EXPIRE, TTL, PERSIST, TYPE)
//   PING          1%    -- health checks
//
// Total = 100%

/// Pick a random operation and execute it. Returns Ok(()) on success,
/// or an error string for logging (but we mostly just count errors).
///
/// Key prefixes are partitioned by logical type to avoid cross-type
/// collisions (e.g., INCR hitting a non-numeric string):
///
///   `lg:s:<n>`   -- string values (SET, GET, MGET, MSET, SETNX, etc.)
///   `lg:c:<n>`   -- integer counters (INCR, DECR, INCRBY, DECRBY)
///   `lg:f:<n>`   -- float counters (INCRBYFLOAT)
///   `lg:a:<n>`   -- append-only strings (APPEND, STRLEN, GETRANGE, SETRANGE)
async fn run_one_op(
    con: &mut redis::aio::MultiplexedConnection,
    rng: &mut StdRng,
    key_space: u32,
) -> Result<(), String> {
    let roll: u32 = rng.gen_range(0..100);

    // String-value keys (SET/GET/MGET/MSET/SETNX/SETEX/PSETEX/GETDEL/DEL/EXISTS)
    let str_key = format!("lg:s:{}", rng.gen_range(0..key_space));
    // Counter keys (INCR/DECR/INCRBY/DECRBY) -- integer only
    let ctr_key = format!("lg:c:{}", rng.gen_range(0..key_space / 4));
    // Float counter keys (INCRBYFLOAT)
    let flt_key = format!("lg:f:{}", rng.gen_range(0..key_space / 4));
    // Append-only keys (APPEND/STRLEN/GETRANGE/SETRANGE) -- always text
    let app_key = format!("lg:a:{}", rng.gen_range(0..key_space));

    match roll {
        // ----- GET (31%) -----
        0..=30 => redis::cmd("GET")
            .arg(&str_key)
            .query_async::<Option<String>>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // ----- TTL operations (4%) -----
        31..=34 => {
            let op = rng.gen_range(0..4);
            match op {
                0 => {
                    // EXPIRE with random TTL 10-300 seconds
                    let ttl: u32 = rng.gen_range(10..300);
                    redis::cmd("EXPIRE")
                        .arg(&str_key)
                        .arg(ttl)
                        .query_async::<i64>(con)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                1 => {
                    // TTL
                    redis::cmd("TTL")
                        .arg(&str_key)
                        .query_async::<i64>(con)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                2 => {
                    // PERSIST
                    redis::cmd("PERSIST")
                        .arg(&str_key)
                        .query_async::<i64>(con)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                3 => {
                    // TYPE
                    redis::cmd("TYPE")
                        .arg(&str_key)
                        .query_async::<String>(con)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                _ => unreachable!(),
            }
        }

        // ----- SET (15%) -- with random flags -----
        35..=49 => {
            let value = random_value(rng);
            let variant: u8 = rng.gen_range(0..5);
            let mut cmd = redis::cmd("SET");
            cmd.arg(&str_key).arg(&value);
            match variant {
                0 => {} // plain SET
                1 => {
                    cmd.arg("NX");
                }
                2 => {
                    cmd.arg("XX");
                }
                3 => {
                    cmd.arg("EX").arg(rng.gen_range(10..300));
                }
                4 => {
                    cmd.arg("GET");
                }
                _ => unreachable!(),
            }
            cmd.query_async::<redis::Value>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // ----- MGET (8%) -----
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

        // ----- MSET (4%) -----
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

        // ----- INCR / DECR (8%) -----
        62..=69 => {
            let cmd_name = if rng.gen_bool(0.5) { "INCR" } else { "DECR" };
            redis::cmd(cmd_name)
                .arg(&ctr_key)
                .query_async::<i64>(con)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        // ----- INCRBY / DECRBY (4%) -----
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

        // ----- INCRBYFLOAT (2%) -----
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

        // ----- EXISTS (5%) -----
        76..=80 => {
            let n = rng.gen_range(1..=4);
            let mut cmd = redis::cmd("EXISTS");
            for _ in 0..n {
                cmd.arg(format!("lg:s:{}", rng.gen_range(0..key_space)));
            }
            cmd.query_async::<i64>(con).await.map(|_| ()).map_err(|e| e.to_string())
        }

        // ----- DEL (4%) -----
        81..=84 => {
            let n = rng.gen_range(1..=3);
            let mut cmd = redis::cmd("DEL");
            for _ in 0..n {
                cmd.arg(format!("lg:s:{}", rng.gen_range(0..key_space)));
            }
            cmd.query_async::<i64>(con).await.map(|_| ()).map_err(|e| e.to_string())
        }

        // ----- APPEND (3%) -----
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

        // ----- STRLEN (2%) -----
        88..=89 => redis::cmd("STRLEN")
            .arg(&app_key)
            .query_async::<i64>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // ----- GETRANGE (2%) -----
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

        // ----- SETRANGE (2%) -----
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

        // ----- SETNX (2%) -----
        94..=95 => con
            .set_nx::<_, _, i64>(&str_key, random_value(rng))
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // ----- SETEX / PSETEX (2%) -----
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

        // ----- GETDEL (1%) -----
        98 => redis::cmd("GETDEL")
            .arg(&str_key)
            .query_async::<redis::Value>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        // ----- PING (1%) -----
        99 => redis::cmd("PING")
            .query_async::<String>(con)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),

        _ => unreachable!(),
    }
}

/// Generate a random value of varying size to simulate realistic payloads.
/// Most values are small (JSON-ish snippets), with occasional larger ones.
fn random_value(rng: &mut StdRng) -> String {
    let len = match rng.gen_range(0..100u32) {
        0..=59 => rng.gen_range(8..128),      // 60%: small values
        60..=84 => rng.gen_range(128..1024),  // 25%: medium values
        85..=94 => rng.gen_range(1024..8192), // 10%: larger values
        _ => rng.gen_range(8192..32768),      //  5%: big payloads
    };
    (0..len).map(|_| rng.gen_range(b'!'..=b'~') as char).collect()
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

    // Each worker gets its own fast RNG seeded from thread entropy.
    let mut rng = StdRng::from_entropy();

    // Scale key space with connection count so workers overlap on keys
    // (simulating contention) but also have breadth.
    let key_space: u32 = 5_000;

    while Instant::now() < deadline {
        match run_one_op(&mut con, &mut rng, key_space).await {
            Ok(()) => {
                stats.ops.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                // WRONGTYPE and similar are expected when random ops hit
                // keys of the wrong type (e.g., INCR on a string value
                // that isn't numeric). We count but don't log them.
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Run the load phase: spawn concurrent workers, report throughput, print summary.
async fn run_load(addr: &str, seconds: u64, connections: usize) {
    println!("=== Phase 2: Load ({seconds}s, {connections} connections) ===");
    println!();

    let stats = Arc::new(Stats::new());
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let start = Instant::now();

    // Spawn reporter task that prints throughput every second.
    let reporter_stats = Arc::clone(&stats);
    let reporter_deadline = deadline;
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

            eprintln!("[{tick:>3}s]  {delta_ops:>8} ops/s  ({delta_errors:>4} errs/s)  total: {total_ops}",);

            if Instant::now() >= reporter_deadline {
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

// ===========================================================================
// Main
// ===========================================================================

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = parse_args();

    // Optionally spawn server on a free port.
    let mut child: Option<Child> = None;
    if args.spawn_server {
        let port = find_free_port();
        args.addr = format!("127.0.0.1:{port}");
        eprintln!("spawning kvdb on {}", args.addr);
        let server = spawn_server(&args.addr);
        child = Some(server);

        if let Err(e) = wait_for_ready(&args.addr).await {
            eprintln!("{e}");
            if let Some(mut c) = child {
                let _ = c.kill();
                let _ = c.wait();
            }
            return ExitCode::FAILURE;
        }
    }

    // Phase 1: Validate
    let ok = run_validate(&args.addr).await;
    if !ok {
        if let Some(mut c) = child {
            let _ = c.kill();
            let _ = c.wait();
        }
        return ExitCode::FAILURE;
    }

    println!();

    // Phase 2: Load
    run_load(&args.addr, args.seconds, args.connections).await;

    // Cleanup
    if let Some(mut c) = child {
        let _ = c.kill();
        let _ = c.wait();
    }

    ExitCode::SUCCESS
}
