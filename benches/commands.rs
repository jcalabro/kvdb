//! Criterion benchmarks for Redis command execution.
//!
//! Run with `just bench` or `cargo bench`.
//! M4+ will add per-command benchmarks against a live server + FDB.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_placeholder(c: &mut Criterion) {
    c.bench_function("noop", |b| b.iter(|| {}));
}

criterion_group!(benches, bench_placeholder);
criterion_main!(benches);
