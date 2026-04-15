//! Criterion benchmarks for RESP protocol parsing, encoding, and
//! full-stack string command latency.
//!
//! Run with `just bench` or `cargo bench`.
//!
//! These benchmarks measure:
//! - The hot path: parsing client commands and encoding server responses.
//! - Full-stack latency: client -> RESP parse -> dispatch -> FDB transaction -> response encode.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use kvdb::config::ServerConfig;
use kvdb::protocol::encoder::encode;
use kvdb::protocol::parser::parse;
use kvdb::protocol::types::RespValue;
use kvdb::storage::database::Database;
use kvdb::storage::meta::{KeyType, ObjectMeta};

// ---------------------------------------------------------------------------
// Parser benchmarks
// ---------------------------------------------------------------------------

fn bench_parse_simple_string(c: &mut Criterion) {
    let input = b"+OK\r\n";
    c.bench_function("parse/simple_string", |b| {
        b.iter_batched(
            || BytesMut::from(&input[..]),
            |mut buf| parse(&mut buf).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

fn bench_parse_integer(c: &mut Criterion) {
    let input = b":123456789\r\n";
    c.bench_function("parse/integer", |b| {
        b.iter_batched(
            || BytesMut::from(&input[..]),
            |mut buf| parse(&mut buf).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

fn bench_parse_bulk_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse/bulk_string");

    for size in [16, 256, 4096, 65536] {
        let data = vec![b'x'; size];
        let mut input = BytesMut::new();
        input.extend_from_slice(format!("${}\r\n", size).as_bytes());
        input.extend_from_slice(&data);
        input.extend_from_slice(b"\r\n");
        let input_bytes = input.freeze();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &input_bytes, |b, input| {
            b.iter_batched(
                || BytesMut::from(&input[..]),
                |mut buf| parse(&mut buf).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_parse_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse/array");

    for count in [1, 10, 100, 1000] {
        // Array of bulk strings: *N\r\n$3\r\nfoo\r\n...
        let mut input = BytesMut::new();
        input.extend_from_slice(format!("*{}\r\n", count).as_bytes());
        for _ in 0..count {
            input.extend_from_slice(b"$3\r\nfoo\r\n");
        }
        let input_bytes = input.freeze();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &input_bytes, |b, input| {
            b.iter_batched(
                || BytesMut::from(&input[..]),
                |mut buf| parse(&mut buf).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_parse_redis_set_command(c: &mut Criterion) {
    // Realistic: SET key value → *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
    let input = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    c.bench_function("parse/redis_set_cmd", |b| {
        b.iter_batched(
            || BytesMut::from(&input[..]),
            |mut buf| parse(&mut buf).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

fn bench_parse_pipeline(c: &mut Criterion) {
    // 100 PING commands pipelined
    let mut input = BytesMut::new();
    for _ in 0..100 {
        input.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    }
    let input_bytes = input.freeze();

    c.bench_function("parse/pipeline_100_pings", |b| {
        b.iter_batched(
            || BytesMut::from(&input_bytes[..]),
            |mut buf| {
                let mut count = 0;
                while parse(&mut buf).unwrap().is_some() {
                    count += 1;
                }
                assert_eq!(count, 100);
            },
            BatchSize::SmallInput,
        )
    });
}

// ---------------------------------------------------------------------------
// Encoder benchmarks
// ---------------------------------------------------------------------------

fn bench_encode_simple_string(c: &mut Criterion) {
    let value = RespValue::SimpleString(Bytes::from_static(b"OK"));
    c.bench_function("encode/simple_string", |b| b.iter(|| encode(&value, 3)));
}

fn bench_encode_bulk_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode/bulk_string");

    for size in [16, 256, 4096, 65536] {
        let data = Bytes::from(vec![b'x'; size]);
        let value = RespValue::BulkString(Some(data));

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &value, |b, val| {
            b.iter(|| encode(val, 3))
        });
    }
    group.finish();
}

fn bench_encode_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode/array");

    for count in [1, 10, 100, 1000] {
        let elements: Vec<RespValue> = (0..count)
            .map(|_| RespValue::BulkString(Some(Bytes::from_static(b"foo"))))
            .collect();
        let value = RespValue::Array(Some(elements));

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &value, |b, val| {
            b.iter(|| encode(val, 3))
        });
    }
    group.finish();
}

fn bench_encode_resp2_downgrade(c: &mut Criterion) {
    // Map with 10 entries — tests the RESP3→RESP2 downgrade path
    let pairs: Vec<(RespValue, RespValue)> = (0..10)
        .map(|i| {
            (
                RespValue::SimpleString(Bytes::from(format!("key{}", i))),
                RespValue::Integer(i),
            )
        })
        .collect();
    let value = RespValue::Map(pairs);

    c.bench_function("encode/resp2_map_downgrade", |b| b.iter(|| encode(&value, 2)));
}

// ---------------------------------------------------------------------------
// Round-trip benchmarks
// ---------------------------------------------------------------------------

fn bench_roundtrip(c: &mut Criterion) {
    // Realistic command-response cycle:
    // Parse SET command, encode OK response
    let cmd_bytes = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let response = RespValue::SimpleString(Bytes::from_static(b"OK"));

    c.bench_function("roundtrip/set_cmd_ok_response", |b| {
        b.iter_batched(
            || BytesMut::from(&cmd_bytes[..]),
            |mut buf| {
                let _cmd = parse(&mut buf).unwrap();
                let _resp = encode(&response, 2);
            },
            BatchSize::SmallInput,
        )
    });
}

// ---------------------------------------------------------------------------
// ObjectMeta serialization benchmarks
// ---------------------------------------------------------------------------

fn bench_meta_serialize(c: &mut Criterion) {
    let meta = ObjectMeta {
        key_type: KeyType::String,
        num_chunks: 5,
        size_bytes: 500_000,
        expires_at_ms: 1_700_000_000_000,
        cardinality: 0,
        last_accessed_ms: 1_700_000_001_000,
        list_head: 0,
        list_tail: 0,
        list_length: 0,
    };

    c.bench_function("meta_serialize", |b| {
        b.iter(|| {
            let _ = criterion::black_box(&meta).serialize().unwrap();
        });
    });

    let bytes = meta.serialize().unwrap();
    c.bench_function("meta_deserialize", |b| {
        b.iter(|| {
            let _ = ObjectMeta::deserialize(criterion::black_box(&bytes)).unwrap();
        });
    });
}

// ---------------------------------------------------------------------------
// Full-stack string command benchmarks
// ---------------------------------------------------------------------------
//
// These benchmarks measure the full round-trip latency of string commands:
// client → RESP parse → dispatch → FDB transaction → response encode.
//
// A single kvdb server is started once for all benchmarks using a dedicated
// tokio runtime and an isolated FDB namespace.

/// Start a kvdb server on a random port with an isolated FDB namespace.
/// Returns the bound address, a shutdown sender, and the server task handle.
async fn start_bench_server() -> (
    SocketAddr,
    tokio::sync::broadcast::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let root_prefix = format!("kvdb_bench_{}", uuid::Uuid::new_v4());
    let db = Database::new("fdb.cluster").expect("failed to open FDB — is `just up` running?");

    let mut config = ServerConfig::default();
    config.bind_addr = addr;
    config.db = Some(db);
    config.root_prefix = Some(root_prefix);
    // Suppress noisy server logs during benchmarks.
    config.log_level = "warn".into();

    let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);

    let handle = tokio::spawn(async move {
        let _ = kvdb::server::listener::run(config, shutdown_rx).await;
    });

    // Poll until the server accepts connections.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() > deadline {
            panic!("bench server did not start within 5 seconds on {addr}");
        }
        match tokio::net::TcpStream::connect(addr).await {
            Ok(_) => break,
            Err(_) => tokio::time::sleep(tokio::time::Duration::from_millis(5)).await,
        }
    }

    (addr, shutdown_tx, handle)
}

fn bench_string_commands(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (addr, _shutdown_tx, _handle) = rt.block_on(start_bench_server());

    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con = rt.block_on(async { client.get_multiplexed_async_connection().await.unwrap() });

    let mut group = c.benchmark_group("string");

    // 1. SET of a 64-byte value.
    group.bench_function("set_64b", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_set_{i}");
            i += 1;
            rt.block_on(async {
                let _: () = redis::cmd("SET")
                    .arg(&key)
                    .arg(&[0u8; 64][..])
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 2. GET of a 64-byte value (pre-populated).
    rt.block_on(async {
        let _: () = redis::cmd("SET")
            .arg("bench_get_target")
            .arg(&[0u8; 64][..])
            .query_async(&mut con)
            .await
            .unwrap();
    });

    group.bench_function("get_64b", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<u8> = redis::cmd("GET")
                    .arg("bench_get_target")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 3. SET + GET of a 1KB value.
    let value_1kb = vec![b'x'; 1024];
    group.bench_function("set_get_1kb", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_sg_{i}");
            i += 1;
            rt.block_on(async {
                let _: () = redis::cmd("SET")
                    .arg(&key)
                    .arg(&value_1kb[..])
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let _: Vec<u8> = redis::cmd("GET").arg(&key).query_async(&mut con).await.unwrap();
            });
        });
    });

    // 4. INCR on an existing counter.
    rt.block_on(async {
        let _: () = redis::cmd("SET")
            .arg("bench_incr_ctr")
            .arg("0")
            .query_async(&mut con)
            .await
            .unwrap();
    });

    group.bench_function("incr", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: i64 = redis::cmd("INCR")
                    .arg("bench_incr_ctr")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 5. MSET of 10 key-value pairs.
    group.bench_function("mset_10", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let mut cmd = redis::cmd("MSET");
            for j in 0..10u64 {
                cmd.arg(format!("bench_mset_{i}_{j}"));
                cmd.arg(&[b'v'; 64][..]);
            }
            i += 1;
            rt.block_on(async {
                let _: () = cmd.query_async(&mut con).await.unwrap();
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Full-stack hash command benchmarks
// ---------------------------------------------------------------------------

fn bench_hash_commands(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (addr, _shutdown_tx, _handle) = rt.block_on(start_bench_server());

    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con = rt.block_on(async { client.get_multiplexed_async_connection().await.unwrap() });

    let mut group = c.benchmark_group("hash");

    // 1. HSET single field.
    group.bench_function("hset_1field", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_hset_{i}");
            i += 1;
            rt.block_on(async {
                let _: i64 = redis::cmd("HSET")
                    .arg(&key)
                    .arg("field")
                    .arg(&[0u8; 64][..])
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 2. HGET single field (pre-populated).
    rt.block_on(async {
        let _: i64 = redis::cmd("HSET")
            .arg("bench_hget_target")
            .arg("field")
            .arg(&[0u8; 64][..])
            .query_async(&mut con)
            .await
            .unwrap();
    });

    group.bench_function("hget_1field", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<u8> = redis::cmd("HGET")
                    .arg("bench_hget_target")
                    .arg("field")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 3. HSET 10 fields at once.
    group.bench_function("hset_10fields", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_hset10_{i}");
            i += 1;
            let mut cmd = redis::cmd("HSET");
            cmd.arg(&key);
            for j in 0..10u64 {
                cmd.arg(format!("f{j}"));
                cmd.arg(&[b'v'; 64][..]);
            }
            rt.block_on(async {
                let _: i64 = cmd.query_async(&mut con).await.unwrap();
            });
        });
    });

    // 4. HGETALL on a 10-field hash (pre-populated).
    rt.block_on(async {
        let mut cmd = redis::cmd("HSET");
        cmd.arg("bench_hgetall_target");
        for j in 0..10u64 {
            cmd.arg(format!("f{j}"));
            cmd.arg(&[b'v'; 64][..]);
        }
        let _: i64 = cmd.query_async(&mut con).await.unwrap();
    });

    group.bench_function("hgetall_10fields", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: std::collections::HashMap<String, Vec<u8>> = redis::cmd("HGETALL")
                    .arg("bench_hgetall_target")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 5. HINCRBY on an existing counter.
    rt.block_on(async {
        let _: i64 = redis::cmd("HSET")
            .arg("bench_hincr")
            .arg("counter")
            .arg("0")
            .query_async(&mut con)
            .await
            .unwrap();
    });

    group.bench_function("hincrby", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: i64 = redis::cmd("HINCRBY")
                    .arg("bench_hincr")
                    .arg("counter")
                    .arg(1)
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Full-stack set command benchmarks
// ---------------------------------------------------------------------------

fn bench_set_commands(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (addr, _shutdown_tx, _handle) = rt.block_on(start_bench_server());

    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con = rt.block_on(async { client.get_multiplexed_async_connection().await.unwrap() });

    let mut group = c.benchmark_group("set");

    // 1. SADD single member to a fresh set.
    group.bench_function("sadd_1member", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_sadd_{i}");
            i += 1;
            rt.block_on(async {
                let _: i64 = redis::cmd("SADD")
                    .arg(&key)
                    .arg("member")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 2. SADD 10 members at once.
    group.bench_function("sadd_10members", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("bench_sadd10_{i}");
            i += 1;
            let mut cmd = redis::cmd("SADD");
            cmd.arg(&key);
            for j in 0..10u64 {
                cmd.arg(format!("m{j}"));
            }
            rt.block_on(async {
                let _: i64 = cmd.query_async(&mut con).await.unwrap();
            });
        });
    });

    // 3. SISMEMBER on an existing set.
    rt.block_on(async {
        let mut cmd = redis::cmd("SADD");
        cmd.arg("bench_sismember_target");
        for j in 0..100u64 {
            cmd.arg(format!("m{j}"));
        }
        let _: i64 = cmd.query_async(&mut con).await.unwrap();
    });

    group.bench_function("sismember", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: i64 = redis::cmd("SISMEMBER")
                    .arg("bench_sismember_target")
                    .arg("m50")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 4. SMEMBERS on a 100-member set.
    group.bench_function("smembers_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<String> = redis::cmd("SMEMBERS")
                    .arg("bench_sismember_target")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    // 5. SINTER of two 50-member sets with ~25 overlap.
    rt.block_on(async {
        let mut cmd_a = redis::cmd("SADD");
        cmd_a.arg("bench_sinter_a");
        for j in 0..50u64 {
            cmd_a.arg(format!("m{j}"));
        }
        let _: i64 = cmd_a.query_async(&mut con).await.unwrap();

        let mut cmd_b = redis::cmd("SADD");
        cmd_b.arg("bench_sinter_b");
        for j in 25..75u64 {
            cmd_b.arg(format!("m{j}"));
        }
        let _: i64 = cmd_b.query_async(&mut con).await.unwrap();
    });

    group.bench_function("sinter_50x50", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<String> = redis::cmd("SINTER")
                    .arg("bench_sinter_a")
                    .arg("bench_sinter_b")
                    .query_async(&mut con)
                    .await
                    .unwrap();
            });
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    // Fast feedback: 500ms warmup + 1.5s measurement × 30 samples.
    // Full suite runs in ~50s. Results are still statistically meaningful
    // for detecting regressions — use `cargo bench -- --sample-size 100`
    // for publication-quality numbers.
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_millis(1500))
        .sample_size(30)
        .without_plots();
    targets =
        bench_parse_simple_string,
        bench_parse_integer,
        bench_parse_bulk_string,
        bench_parse_array,
        bench_parse_redis_set_command,
        bench_parse_pipeline,
        bench_encode_simple_string,
        bench_encode_bulk_string,
        bench_encode_array,
        bench_encode_resp2_downgrade,
        bench_roundtrip,
        bench_meta_serialize,
        bench_string_commands,
        bench_hash_commands,
        bench_set_commands,
}
criterion_main!(benches);
