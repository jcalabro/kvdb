//! Criterion benchmarks for RESP protocol parsing and encoding.
//!
//! Run with `just bench` or `cargo bench`.
//!
//! These benchmarks measure the hot path: parsing client commands and
//! encoding server responses. The parser/encoder run on every single
//! request, so even small improvements here compound at scale.

use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use kvdb::protocol::encoder::encode;
use kvdb::protocol::parser::parse;
use kvdb::protocol::types::RespValue;
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
}
criterion_main!(benches);
