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
    Err(format!(
        "server at {addr} did not become ready within 5 seconds"
    ))
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
