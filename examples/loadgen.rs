//! Load generator for kvdb.
//!
//! Simulates a realistic Redis workload by running a mix of commands across
//! multiple concurrent connections for a configurable duration. Useful for
//! profiling with Tracy, stress-testing under load, and validating behavior
//! under concurrency.
//!
//! Usage:
//!   cargo run --release --example loadgen -- --seconds 30
//!   cargo run --release --example loadgen -- --seconds 10 --connections 50 --addr 127.0.0.1:6379

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis::AsyncCommands;
use tokio::task::JoinSet;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Args {
    addr: String,
    seconds: u64,
    connections: usize,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut addr = String::from("127.0.0.1:6379");
    let mut seconds: u64 = 10;
    let mut connections: usize = 32;

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
            "--help" | "-h" => {
                eprintln!("Usage: loadgen [--addr HOST:PORT] [--seconds N] [--connections N]");
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
    }
}

// ---------------------------------------------------------------------------
// Per-worker stats
// ---------------------------------------------------------------------------

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
//
// Total = 100%

/// Pick a random operation and execute it. Returns Ok(()) on success,
/// or an error string for logging (but we mostly just count errors).
///
/// Key prefixes are partitioned by logical type to avoid cross-type
/// collisions (e.g., INCR hitting a non-numeric string):
///
///   `lg:s:<n>`   — string values (SET, GET, MGET, MSET, SETNX, etc.)
///   `lg:c:<n>`   — integer counters (INCR, DECR, INCRBY, DECRBY)
///   `lg:f:<n>`   — float counters (INCRBYFLOAT)
///   `lg:a:<n>`   — append-only strings (APPEND, STRLEN, GETRANGE, SETRANGE)
async fn run_one_op(
    con: &mut redis::aio::MultiplexedConnection,
    rng: &mut StdRng,
    key_space: u32,
) -> Result<(), String> {
    let roll: u32 = rng.gen_range(0..100);

    // String-value keys (SET/GET/MGET/MSET/SETNX/SETEX/PSETEX/GETDEL/DEL/EXISTS)
    let str_key = format!("lg:s:{}", rng.gen_range(0..key_space));
    // Counter keys (INCR/DECR/INCRBY/DECRBY) — integer only
    let ctr_key = format!("lg:c:{}", rng.gen_range(0..key_space / 4));
    // Float counter keys (INCRBYFLOAT)
    let flt_key = format!("lg:f:{}", rng.gen_range(0..key_space / 4));
    // Append-only keys (APPEND/STRLEN/GETRANGE/SETRANGE) — always text
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

        // ----- SET (15%) — with random flags -----
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

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = parse_args();

    println!(
        "loadgen: {} connections, {} seconds, target {}",
        args.connections, args.seconds, args.addr
    );

    let stats = Arc::new(Stats::new());
    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    let start = Instant::now();

    // Spawn reporter task that prints throughput every second.
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

            eprintln!("[{tick:>3}s]  {delta_ops:>8} ops/s  ({delta_errors:>4} errs/s)  total: {total_ops}",);

            if Instant::now() >= deadline {
                break;
            }
        }
    });

    // Spawn all worker tasks.
    let mut workers = JoinSet::new();
    for id in 0..args.connections {
        let addr = args.addr.clone();
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
    println!("--- loadgen summary ---");
    println!("  duration:    {elapsed:.2}s");
    println!("  connections: {}", args.connections);
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
