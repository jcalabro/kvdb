# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

kvdb is a Redis-compatible database built on FoundationDB in Rust. It implements the RESP3 protocol (with RESP2 backward compatibility) and maps Redis data structures onto FDB's ordered key-value store. FDB provides strict serializability — stronger consistency than Redis itself.

## Build & Development Commands

All development is done through `just` (a command runner):

- `just` — lint + fast tests (the inner loop, must stay under 2 seconds)
- `just test` — unit + integration tests via cargo-nextest
- `just accept` — exhaustive acceptance tests (property-based, randomized)
- `just ci` — lint + test + accept + doc build (run before pushing)
- `just lint` — clippy (deny warnings) + rustfmt check
- `just fmt` — auto-format code
- `just up` / `just down` — start/stop local 3-node FDB cluster via docker compose
- `just smoke` — start server, run smoke test client, stop server
- `just fuzz` — run cargo-fuzz against RESP parser (requires nightly)
- `just bench` — criterion benchmarks
- `just run` — run the server locally in debug mode

Run a single test: `cargo nextest run <test_name>`

Tests use cargo-nextest, not `cargo test`. Test tiers are split by naming convention:
- `just test` runs everything *except* tests matching `accept::`
- `just accept` runs *only* tests matching `accept::`

## Architecture

```
Client ←RESP→ Server → Commands → Storage → FoundationDB
                 ↑                    ↑
             Protocol              ObjectMeta + Chunking
```

**`protocol/`** — RESP3/RESP2 parser and encoder. Zero-copy parsing via `bytes::BytesMut`. The `RespValue` enum covers all RESP2+RESP3 wire types. `RedisCommand` is the parsed form used by dispatch.

**`server/`** — Tokio TCP listener with semaphore-based connection limiting. One task per connection. Each connection tracks its protocol version (2/3), selected database namespace (0-15), and optional MULTI/EXEC transaction state (queued commands, watched keys with meta snapshots).

**`commands/`** — Command dispatch: case-insensitive name lookup, delegates to per-command handlers. Returns `RespValue` responses. `transaction.rs` implements MULTI/EXEC/DISCARD/WATCH/UNWATCH.

**`storage/`** — FDB integration layer:
- `database.rs` — FDB connection lifecycle
- `directories.rs` — FDB directory layout per namespace (`meta/`, `obj/`, `hash/`, `set/`, `zset/`, `zset_idx/`, `list/`, `expire/`)
- `meta.rs` — `ObjectMeta` struct read on every command for type enforcement, chunk tracking, and TTL checking
- `chunking.rs` — Transparent value splitting at 100,000-byte boundaries (FDB's hard limit)
- `transaction.rs` — Transaction wrapper with metrics; supports shared-transaction mode for MULTI/EXEC (via `run_transact` with `shared_tr` parameter)

**`observability/`** — Composable tracing + metrics + profiling:
- `tracing_init.rs` — Builds a `Vec<Box<dyn Layer<Registry>>>` from EnvFilter + fmt (text/JSON) + Tracy (opt-in) + OpenTelemetry (opt-in), applied in one `.with()` call to avoid nested `Layered<>` type issues.
- `metrics.rs` — Prometheus metric definitions (`lazy_static!` globals) and a minimal TCP-based HTTP scrape endpoint. Pre-defined metrics: connections, command duration/count, FDB transaction duration/conflicts/retries, TTL expiry.

**`ttl/`** — Hybrid lazy (check on read) + active (background scan every 250ms) expiration.

**`error.rs`** — Three error types: `ProtocolError`, `CommandError`, and top-level `Error`. Uses `thiserror` for library errors, `anyhow` at the application boundary (main.rs).

**`config.rs`** — `ServerConfig` via clap derive with env var overrides (`KVDB_BIND_ADDR`, `KVDB_MAX_CONNECTIONS`, `KVDB_FDB_CLUSTER_FILE`, `KVDB_LOG_LEVEL`, `KVDB_LOG_FORMAT`, `KVDB_TRACY`, `KVDB_OTLP_ENDPOINT`, `KVDB_METRICS_ADDR`).

## Key Design Constraints

- **FDB hard limits**: 5-second transaction timeout, 10MB transaction size, 100,000-byte value limit, ~100K reads/sec per key. These are architectural, not configurable.
- **One Redis command = one FDB transaction** (except MULTI/EXEC which batches).
- **Every command reads ObjectMeta first** — this enforces type safety (WRONGTYPE errors), checks TTL expiry, and locates chunked data. It's a mandatory per-command cost.
- **Redis no-rollback semantics**: MULTI/EXEC must emulate Redis's partial-execution behavior (command errors don't abort the transaction), even though FDB is all-or-nothing by default.
- **WATCH uses meta snapshot comparison**: At WATCH time, the raw ObjectMeta bytes are snapshot-read and stored. At EXEC time, the current meta is re-read and compared — if different, the transaction aborts (returns nil). This correctly detects modifications by other clients regardless of FDB transaction read versions.
- **MULTI/EXEC 5-second limit**: All queued commands execute in a single FDB transaction, subject to FDB's 5-second timeout and 10MB transaction size limit. This is a documented limitation.

## FDB Key Layout

Each namespace (0-15, maps to Redis SELECT) uses FDB directories:
```
kvdb/<ns>/meta/<key>                  → ObjectMeta (type, chunks, TTL, cardinality)
kvdb/<ns>/obj/<key, chunk_offset>     → value chunks (100KB each)
kvdb/<ns>/hash/<key, field>           → field value
kvdb/<ns>/set/<key, member>           → empty (existence = membership)
kvdb/<ns>/zset/<key, score, member>   → empty (score uses tuple layer float encoding)
kvdb/<ns>/zset_idx/<key, member>      → score bytes
kvdb/<ns>/list/<key, index>           → element
kvdb/<ns>/expire/<key>                → expiry timestamp (ms)
```

## Observability

Four providers, all optional except stdout logging:

- **fmt** (always on): `--log-format text` (default) or `--log-format json` for structured output.
- **Tracy**: Compile with `cargo build --features tracy`, then run with `--tracy`. Connects to the Tracy GUI for real-time span/CPU/allocation profiling. The `tracy` feature is opt-in to avoid compiling `tracy-client-sys` (C++) in normal builds.
- **OpenTelemetry**: `--otlp-endpoint http://localhost:4318` enables OTLP span export via HTTP.
- **Prometheus**: `--metrics-addr 0.0.0.0:9090` starts an HTTP scrape endpoint. Metrics are defined as `lazy_static` globals in `observability/metrics.rs`.

All tracing layers are collected into a `Vec<Box<dyn Layer<Registry>>>` and applied in a single `.with()` call — this is the pattern that avoids the nested `Layered<>` type mismatch that otherwise breaks when composing layers from different crates.

We should be relatively minimal with our logging, but have robust prometheus metrics and tracing in major areas of the code.

## Implementation Status

The project follows a milestone plan in `docs/IMPLEMENTATION_PLAN.md`. Design details are in `docs/DESIGN.md`. FDB patterns and constraints are documented in `docs/fdb_research.md`.
