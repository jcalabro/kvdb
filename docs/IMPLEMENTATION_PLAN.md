# Implementation Plan

## Progress

| Milestone | Status | Notes |
|-----------|--------|-------|
| M0: Project Skeleton | **Complete** | All exit criteria met. `just` inner loop at ~1.1s. 8 unit tests passing. |
| M1: RESP3 Parser/Encoder | Not started | |
| M2: TCP Server Shell | Not started | |
| M3: FDB Storage Layer | Not started | |
| M4: String Commands | Not started | |
| M5: Key Management & TTL | Not started | |
| M6: Hash Commands | Not started | |
| M7: Set Commands | Not started | |
| M8: Sorted Set Commands | Not started | |
| M9: List Commands | Not started | |
| M10: Transactions | Not started | |
| M11: Pub/Sub | Not started | |
| M12: Server Commands & Observability | Not started | |
| M13: Compatibility Testing | Not started | |

### What's been built (M0)

**Dev infrastructure**: justfile (12 recipes), docker-compose.yml (3-node FDB 7.3.63), fdb.cluster, .config/nextest.toml, rustfmt.toml, .github/workflows/ci.yml.

**Source modules** (all compile, lint-clean, documented):
- `src/protocol/` — `RespValue` enum (all RESP2+RESP3 types), `RedisCommand`, parser/encoder stubs
- `src/server/` — TCP listener with semaphore-based connection limiting, connection handler stub
- `src/storage/` — `Database`, `Directories`, `ObjectMeta`, chunking (with unit tests), transaction types
- `src/commands/` — dispatch with PING/ECHO handlers and 5 unit tests
- `src/ttl/` — `ExpiryConfig` with defaults
- `src/observability/` — Composable tracing subscriber (fmt text/JSON + Tracy opt-in + OpenTelemetry OTLP), Prometheus metrics definitions (connections, commands, FDB transactions, TTL expiry) with HTTP scrape endpoint. Tracy is behind a cargo feature flag (`--features tracy`) to avoid compiling C++ in normal builds.
- `src/config.rs` — `ServerConfig` with CLI args + env var overrides for all settings including observability
- `src/error.rs` — `Error`, `ProtocolError`, `CommandError` with thiserror

**Test infrastructure**: `tests/harness/mod.rs` (TestContext with random-port server startup, redis-rs client), integration/ and accept/ directories, fuzz target for RESP parser, criterion bench scaffold.

---

This plan breaks the kvdb build into milestones that each produce something testable. Each milestone builds on the last. Nothing is speculative — every step ends with working code and passing tests.

**Testing philosophy**: Three tiers with escalating scope and time:

| Command | What | Target | When you run it |
|---------|------|--------|-----------------|
| `just test` | Unit + integration tests against real FDB. Each command exercised with a handful of representative cases. | **1-2 seconds** | After every change — reflexive, no thought |
| `just accept` | Exhaustive end-to-end acceptance. Randomized inputs, property-based fuzzing against live server, large corpus, chaos sequences. | **< 15 seconds** | Before committing |
| `just ci` | Lint + test + accept + doc build. Everything CI does. | As long as needed | Before pushing a PR |

`just test` is the OODA loop. It talks to real FDB — every test starts a real server, sends real Redis commands, and verifies results against real storage. But each command gets only a handful of focused cases (happy path, error path, one edge case). This keeps it at 1-2 seconds while still catching real bugs in the full stack.

`just accept` is the heavy artillery. It runs property-based tests that generate thousands of randomized command sequences against the live server, verifying invariants hold across arbitrary inputs. It tests large values (chunking boundaries), large collections (transaction limit boundaries), interleaved command types, concurrent clients, TTL races, and WRONGTYPE edge cases. This is where you find the bugs that targeted tests miss.

Every milestone adds tests to both tiers. `just test` gets the focused integration cases. `just accept` gets the exhaustive randomized coverage. By the time we reach Milestone 13, the formal compatibility run is largely confirming what our own acceptance tests already cover.

---

## Milestone 0: Project Skeleton, Dev Infrastructure, and Test Harness

**Goal**: `just up` stands up a local FDB cluster. `just` (default: lint + unit tests) completes in 1-2 seconds. `just accept` runs end-to-end tests. The test harness is ready before any feature code is written.

### Development Infrastructure

**justfile** — the single entry point for all development tasks:

```justfile
set shell := ["bash", "-cu"]

# Default: lint + fast tests. Your inner loop — must stay under 2 seconds.
default: lint test

# Unit + integration tests against real FDB. Focused cases, not exhaustive.
# Requires `just up` first.
test:
    cargo nextest run -E 'not test(accept::)'

# Exhaustive acceptance tests. Randomized inputs, property-based fuzzing,
# large corpus, chaos sequences against live server + FDB.
accept:
    cargo nextest run -E 'test(accept::)'

# Full CI pipeline — lint, all tests, doc build. Run before pushing.
ci: lint test accept
    cargo doc --no-deps

# Run clippy and format check
lint:
    cargo clippy --workspace --tests -- -D warnings
    cargo fmt --check

# Stand up local FDB cluster via docker compose, configure if first run
up:
    #!/usr/bin/env bash
    docker compose up -d

    if ! fdbcli -C fdb.cluster --exec status --timeout 1 2>/dev/null ; then
        echo "Configuring new FDB cluster..."
        if ! fdbcli -C fdb.cluster --exec "configure new single ssd-redwood-1 ; status" --timeout 10 ; then
            echo "Unable to configure new FDB cluster."
            exit 1
        fi
    fi

    echo "FDB cluster is ready"

# Tear down local FDB cluster
down:
    docker compose down --remove-orphans

# Run the fuzzer (default 30s per target, or pass duration)
fuzz duration="30":
    cargo +nightly fuzz run resp_parser -- -max_total_time={{duration}}

# Run benchmarks
bench:
    cargo bench

# Open FDB CLI shell connected to local cluster
fdbcli:
    fdbcli -C fdb.cluster

# Build release binary
build:
    cargo build --release

# Run the server locally (debug mode)
run *ARGS:
    cargo run -- {{ARGS}}

# Clean everything
clean:
    cargo clean
    docker compose down --remove-orphans --volumes
```

The key invariant: `just` (which runs `just lint test`) finishes in 1-2 seconds. It talks to real FDB but each command gets only a handful of focused test cases — enough to catch real bugs in the full stack without being exhaustive. `just accept` is where the exhaustive randomized testing lives.

**docker-compose.yml** — local FDB cluster (mirrors bluesky-social/kvdb):

```yaml
services:
  fdb-coordinator:
    image: foundationdb/foundationdb:7.3.63
    platform: "linux/amd64"
    container_name: kvdb-fdb-coordinator
    network_mode: host
    environment:
      FDB_NETWORKING_MODE: host

  fdb-server-1:
    depends_on:
      - fdb-coordinator
    platform: "linux/amd64"
    image: foundationdb/foundationdb:7.3.63
    container_name: kvdb-fdb-server-1
    network_mode: host
    environment:
      FDB_NETWORKING_MODE: host
      FDB_PORT: 4501
      FDB_COORDINATOR: localhost

  fdb-server-2:
    depends_on:
      - fdb-coordinator
    platform: "linux/amd64"
    image: foundationdb/foundationdb:7.3.63
    container_name: kvdb-fdb-server-2
    network_mode: host
    environment:
      FDB_NETWORKING_MODE: host
      FDB_PORT: 4502
      FDB_COORDINATOR: localhost

  fdb-server-3:
    depends_on:
      - fdb-coordinator
    platform: "linux/amd64"
    image: foundationdb/foundationdb:7.3.63
    container_name: kvdb-fdb-server-3
    network_mode: host
    environment:
      FDB_NETWORKING_MODE: host
      FDB_PORT: 4503
      FDB_COORDINATOR: localhost
```

**fdb.cluster**: `docker:docker@127.0.0.1:4500`

**cargo-nextest** — why nextest instead of `cargo test`:
- Runs each test in its own process — true isolation, no shared state leaks between tests
- Parallel by default with smart scheduling — longer tests start first
- Retries for transient FDB failures in CI profile
- Clean, readable output with per-test timing — immediately see what's slow
- Filter expressions split tiers: `-E 'not test(accept::)'` for fast tests, `-E 'test(accept::)'` for exhaustive acceptance

**.config/nextest.toml**:
```toml
[store]
dir = "target/nextest"

[profile.default]
retries = 1
fail-fast = false
slow-timeout = { period = "10s", terminate-after = 2 }

[profile.ci]
retries = 2
fail-fast = true
```

### Project Structure

```
kvdb/
  justfile                    # Development task runner
  docker-compose.yml          # Local FDB cluster
  fdb.cluster                 # FDB connection config
  .config/nextest.toml        # Test runner config
  Cargo.toml                  # Workspace root
  rustfmt.toml                # Formatting config
  .github/workflows/ci.yml    # CI pipeline
  src/
    main.rs
    lib.rs
    config.rs
    protocol/                 # RESP parser/encoder
    server/                   # TCP listener, connection handler
    storage/                  # FDB integration, ObjectMeta, chunking
    commands/                 # Command dispatch and implementations
    ttl/                      # Expiry background worker
    error.rs
  tests/
    harness/                  # Shared test infrastructure (TestContext, helpers)
      mod.rs
    integration/              # `just test` — focused cases per command, hits real FDB
      strings.rs
      hashes.rs
      sets.rs
      sorted_sets.rs
      lists.rs
      keys.rs
      transactions.rs
      pubsub.rs
      server.rs
    accept/                   # `just accept` — exhaustive randomized testing
      strings.rs              #   property tests, model checking, chaos sequences
      hashes.rs
      sets.rs
      sorted_sets.rs
      lists.rs
      keys.rs
      transactions.rs
      mixed.rs                #   cross-type randomized command interleaving
  fuzz/                       # Fuzz targets
  benches/                    # Criterion benchmarks
```

### Cargo Dependencies

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
bytes = "1"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
clap = { version = "4", features = ["derive"] }

[dev-dependencies]
proptest = "1"
criterion = { version = "0.5", features = ["async_tokio", "html_reports"] }
redis = { version = "0.27", features = ["tokio-comp", "aio"] }
tokio = { version = "1", features = ["full", "test-util"] }
```

### Test Harness

`tests/harness/mod.rs` — the critical piece:

```rust
/// Spins up a kvdb server on a random available port, returns a
/// connected redis::Client ready for testing. Tears down on drop.
///
/// Usage in any test:
///   let ctx = TestContext::new().await;
///   let mut con = ctx.conn().await;
///   let _: () = con.set("foo", "bar").await.unwrap();
///   let val: String = con.get("foo").await.unwrap();
///   assert_eq!(val, "bar");
pub struct TestContext {
    pub client: redis::Client,
    pub port: u16,
    shutdown: tokio::sync::oneshot::Sender<()>,
    _server: JoinHandle<()>,
}

impl TestContext {
    /// Start a server with a unique FDB namespace. Waits for port to accept connections.
    pub async fn new() -> Self { /* ... */ }
    pub async fn new_with_config(config: ServerConfig) -> Self { /* ... */ }
    /// Get a fresh connection (each call returns an independent connection).
    pub async fn conn(&self) -> redis::aio::MultiplexedConnection { /* ... */ }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Signal server to shut down; JoinHandle is dropped which cancels the task
    }
}
```

Requirements:
- **Zero manual setup**: `just up` once to start FDB, then both `just test` and `just accept` work. No config files to edit, no server to start manually.
- **Isolated**: each test gets its own server on a random port and its own FDB namespace (`kvdb_test_<uuid>`). Tests run in parallel without interfering. Namespace cleaned up on drop.
- **Fast startup**: server binds and is ready in <50ms. The harness polls until the port accepts connections (no `sleep`).
- **Real Redis client**: tests use the `redis` crate (redis-rs), the same client library real applications use. This is not a mock — it validates the wire protocol end-to-end.
- **FDB optional at M0**: for milestones before FDB is wired up (M0-M2), the harness starts the server in a "stub" mode. After M3, tests run against real FDB.

### CI Pipeline

`.github/workflows/ci.yml`:
```yaml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: taiki-e/install-action@nextest

      # Install FDB client library
      - name: Install FoundationDB client
        run: |
          wget https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_amd64.deb
          sudo dpkg -i foundationdb-clients_7.3.63-1_amd64.deb

      # Start FDB cluster
      - name: Start FDB
        run: just up

      # Lint + test
      - name: CI
        run: just ci

      - name: Stop FDB
        if: always()
        run: just down

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt
      - run: just lint
```

### Exit Criteria

- [x] `just up` starts a 3-node FDB cluster locally via docker compose
- [x] `just` (default: lint + test) completes in <2 seconds — **achieved: ~1.1s with 8 tests**
- [x] `just lint` passes with zero warnings
- [x] `just ci` runs lint + test + accept + doc build
- [x] CI pipeline structure in place (.github/workflows/ci.yml)
- [x] `TestContext::new()` compiles (panics at runtime until M2 wires up the server)
- [x] No feature code yet — purely infrastructure
- [x] Observability scaffolding: tracing (fmt/Tracy/OTEL), Prometheus metrics, all wired into config

---

## Milestone 1: RESP3 Protocol Parser and Encoder

**Goal**: A standalone, fuzz-tested RESP3 parser and encoder with zero-copy parsing via `bytes::BytesMut`.

**Work**:

1. **Types** (`protocol/types.rs`):
   - `RespValue` enum covering all RESP2 + RESP3 types (SimpleString, Error, Integer, BulkString, Array, Null, Boolean, Double, BigNumber, BulkError, VerbatimString, Map, Set, Push, Attribute)
   - `RedisCommand` struct: command name + args as `Vec<Bytes>`
   - Conversion helpers: `RespValue -> i64`, `RespValue -> Bytes`, etc.

2. **Parser** (`protocol/parser.rs`):
   - Incremental parser: feed `BytesMut`, get back `Option<RespValue>` or `Err`
   - Handle partial reads (client may send half a frame)
   - Type byte dispatch: `+`, `-`, `:`, `$`, `*` (RESP2), `_`, `#`, `,`, `(`, `!`, `=`, `%`, `~`, `>`, `|` (RESP3)
   - Zero-copy: parsed `BulkString` values are `Bytes` slices into the original buffer, not copies

3. **Encoder** (`protocol/encoder.rs`):
   - `encode(value: &RespValue, protocol_version: u8) -> BytesMut`
   - RESP2 mode: downgrade RESP3 types (Map -> Array of pairs, Set -> Array, Boolean -> Integer, Null -> BulkNull, etc.)
   - RESP3 mode: native encoding for all types

4. **Tests**:
   - Unit tests for every type: parse(encode(value)) == value round-trip
   - Edge cases: empty bulk strings, null values, nested arrays, max-depth nesting
   - Property tests (proptest): generate arbitrary `RespValue`, encode, parse, assert equality
   - Fuzz target (`fuzz/fuzz_targets/resp_parser.rs`): feed arbitrary bytes, assert no panics
   - Run fuzzer for initial corpus generation

**Exit criteria**: `just test` passes all parser/encoder unit + property tests in <2 seconds. `just fuzz` runs for 10 minutes without crashes. Property tests pass with 10K cases.

---

## Milestone 2: TCP Server Shell + Acceptance Test Baseline

**Goal**: A TCP server on port 6379 that accepts connections, parses RESP commands, and replies with hardcoded responses. `redis-cli PING` returns `PONG`. **The acceptance test harness is fully operational from this point forward.**

**Work**:

1. **Listener** (`server/listener.rs`):
   - `TcpListener::bind` on configurable address
   - Semaphore-based connection limiting (default 10K)
   - Spawn a tokio task per connection

2. **Connection handler** (`server/connection.rs`):
   - Per-connection state: read buffer (`BytesMut`), write buffer, protocol version (2 or 3), selected database (0-15)
   - Read loop: `socket.read_buf(&mut buffer)` -> `parser.parse(&mut buffer)` -> dispatch -> encode response -> `socket.write_all`
   - Pipelining: parse and execute all complete commands in the buffer before flushing writes

3. **Command dispatch** (`commands/mod.rs`):
   - Parse `RedisCommand` from `RespValue::Array` of bulk strings
   - Case-insensitive command name lookup
   - Stub dispatcher that returns `-ERR unknown command` for everything except PING, ECHO, HELLO, QUIT, COMMAND
   - PING -> `+PONG\r\n`
   - ECHO -> return the argument
   - HELLO -> protocol negotiation, switch connection to RESP3 if requested
   - QUIT -> close connection
   - COMMAND DOCS / COMMAND COUNT -> minimal stubs for client handshake

4. **Config** (`config.rs`):
   - `ServerConfig` struct: bind address, max connections, FDB cluster file path, log level
   - Load from TOML file, environment variable overrides (`KVDB_BIND_ADDR`, etc.)
   - CLI arg parsing with `clap`

5. **Wire up TestContext**: implement the test harness from M0 so it starts the real server, connects with redis-rs, and tears down cleanly.

6. **Acceptance tests** (`tests/integration/server.rs`):
   ```rust
   #[tokio::test]
   async fn ping_returns_pong() {
       let ctx = TestContext::new().await;
       let result: String = redis::cmd("PING").query_async(&mut ctx.conn()).await.unwrap();
       assert_eq!(result, "PONG");
   }

   #[tokio::test]
   async fn echo_returns_argument() {
       let ctx = TestContext::new().await;
       let result: String = redis::cmd("ECHO").arg("hello").query_async(&mut ctx.conn()).await.unwrap();
       assert_eq!(result, "hello");
   }

   #[tokio::test]
   async fn pipelining_works() {
       let ctx = TestContext::new().await;
       let mut pipe = redis::pipe();
       for _ in 0..100 { pipe.cmd("PING"); }
       let results: Vec<String> = pipe.query_async(&mut ctx.conn()).await.unwrap();
       assert_eq!(results.len(), 100);
       assert!(results.iter().all(|r| r == "PONG"));
   }

   #[tokio::test]
   async fn unknown_command_returns_error() {
       let ctx = TestContext::new().await;
       let result: redis::RedisResult<String> = redis::cmd("FAKECMD").query_async(&mut ctx.conn()).await;
       assert!(result.is_err());
   }
   ```

**Exit criteria**: `redis-cli -p <port> PING` returns `PONG`. `just accept` passes all acceptance tests. Pipelining works. HELLO negotiation works. **From this point on, `just accept` always runs the full acceptance suite, and `just ci` runs everything.**

---

## Milestone 3: FDB Storage Layer

**Goal**: ObjectMeta, value chunking, directory layout, and transaction wrapper — all tested against a real FDB instance.

**Work**:

1. **FDB initialization** (`storage/database.rs`):
   - `boot()` with `Once` guard
   - `Database::default()` from cluster file
   - Configurable transaction timeout and retry limit

2. **Directory layout** (`storage/directories.rs`):
   - Open/create FDB directory subspaces matching DESIGN.md §3:
     `kvdb/<ns>/meta/`, `kvdb/<ns>/obj/`, `kvdb/<ns>/hash/`, `kvdb/<ns>/set/`, `kvdb/<ns>/zset/`, `kvdb/<ns>/zset_idx/`, `kvdb/<ns>/list/`, `kvdb/<ns>/expire/`
   - `Directories` struct holding all subspace handles for a given namespace
   - Lazy initialization per namespace (only create on first `SELECT`)

3. **ObjectMeta** (`storage/meta.rs`):
   - Struct with fields: `key_type` (enum), `num_chunks`, `size_bytes`, `expires_at_ms`, `cardinality`, `last_accessed_ms`, list-specific fields (head, tail, length)
   - Serialize/deserialize via `bincode` (compact binary, no schema evolution needed for MVP)
   - `read_meta(tr, ns, key)` -> `Option<ObjectMeta>` with lazy expiry check
   - `write_meta(tr, ns, key, meta)` -> `()`
   - `delete_meta(tr, ns, key)` -> `()`

4. **Value chunking** (`storage/chunking.rs`):
   - `write_chunks(tr, subspace, key, data: &[u8])` -> `num_chunks`
     - Splits into 100,000-byte chunks, writes each at `subspace.pack((key, chunk_index))`
   - `read_chunks(tr, subspace, key, num_chunks: u32)` -> `Vec<u8>`
     - Fires `num_chunks` parallel `get()` futures, concatenates in order
   - `delete_chunks(tr, subspace, key, num_chunks: u32)` -> `()`
     - Clears all chunk keys

5. **Transaction wrapper** (`storage/transaction.rs`):
   - Thin wrapper around `db.run()` that adds metrics (duration, retries, conflicts)
   - `run_readonly(db, f)` — uses snapshot reads where appropriate
   - `run_readwrite(db, f)` — standard transaction with retry

6. **Test isolation**: Update `TestContext` to allocate a unique FDB namespace per test (e.g., `kvdb_test_<uuid>/`), and clean it up on teardown. This lets all acceptance tests run in parallel against the same FDB cluster without interfering.

7. **Tests** (require a running FDB instance):
   - Unit: write 1-byte value, read back, verify
   - Unit: write 500KB value (5 chunks), read back, verify byte-for-byte equality
   - Unit: write ObjectMeta, read back, verify all fields
   - Unit: lazy expiry — write with expires_at_ms in the past, read back, get None
   - Unit: directory creation — verify all subspaces exist after initialization
   - Property test: write random-length data (0 to 1MB), read back, verify round-trip
   - **Prior acceptance tests still pass** (PING, ECHO, pipelining — nothing regressed)

**Exit criteria**: All storage tests pass against a real FDB cluster. Chunking handles 0 bytes through 1MB correctly. ObjectMeta round-trips all fields. Prior acceptance tests still green.

---

## Milestone 4: String Commands

**Goal**: GET, SET, MGET, MSET, DEL, EXISTS, INCR, DECR, APPEND, STRLEN, GETDEL, SETNX, SETEX, PSETEX, GETRANGE, SETRANGE, INCRBY, DECRBY, INCRBYFLOAT. Full SET option support (EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET).

**Work**:

1. **String storage** (`commands/string.rs`):
   - GET: read meta (type check + expiry check), read chunks, return bulk string
   - SET: write meta (type: String), write chunks, optionally set expiry. Handle all SET flags:
     - `EX seconds` / `PX milliseconds` / `EXAT timestamp` / `PXAT ms-timestamp` -> set expires_at_ms
     - `NX` -> only set if key does not exist (check meta)
     - `XX` -> only set if key already exists
     - `KEEPTTL` -> preserve existing TTL
     - `GET` -> return old value before overwriting (replaces deprecated GETSET)
   - DEL: delete meta + all chunks + expire entry. Return count of deleted keys. For multi-key DEL, delete each in the same transaction.
   - EXISTS: read meta, return 1/0. Multi-key: return count.
   - MGET: read all keys in parallel within one transaction, return array
   - MSET: write all keys in one transaction (batch if > ~500 keys)

2. **Numeric operations** (`commands/string.rs`):
   - INCR/DECR/INCRBY/DECRBY: read meta + value, parse as i64, increment, write back. Return new value. Error if value is not a valid integer.
   - INCRBYFLOAT: same but with f64 parsing. Return the new value as a bulk string.
   - All numeric ops create the key with value "0" if it doesn't exist.

3. **String manipulation**:
   - APPEND: read existing value (or empty), concatenate, write back. Return new length.
   - STRLEN: read meta, return size_bytes. No need to read actual data.
   - GETRANGE: read value, return substring. Handle negative indices.
   - SETRANGE: read value, pad with zero bytes if needed, overwrite at offset, write back.

4. **Wire up dispatch**: Connect string commands to the command dispatcher from Milestone 2.

5. **Integration tests** (`tests/strings.rs` — runs in `just test`):

   Focused cases, 3-5 per command. Fast. Hits real FDB.
   ```rust
   #[tokio::test]
   async fn set_and_get() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = con.set("foo", "bar").await.unwrap();
       let val: String = con.get("foo").await.unwrap();
       assert_eq!(val, "bar");
   }

   #[tokio::test]
   async fn incr_creates_key() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let val: i64 = con.incr("counter", 1).await.unwrap();
       assert_eq!(val, 1);
   }

   #[tokio::test]
   async fn set_nx_fails_on_existing() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = con.set("foo", "bar").await.unwrap();
       let result: bool = con.set_nx("foo", "baz").await.unwrap();
       assert!(!result);
   }

   #[tokio::test]
   async fn large_value_chunking() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let big_val = "x".repeat(500_000); // 500KB, 5 chunks
       let _: () = con.set("big", &big_val).await.unwrap();
       let val: String = con.get("big").await.unwrap();
       assert_eq!(val, big_val);
   }
   ```

6. **Acceptance tests** (`tests/accept/strings.rs` — runs in `just accept`):

   Exhaustive randomized testing. Thousands of cases. Finds the bugs targeted tests miss.
   ```rust
   use proptest::prelude::*;

   // Property: SET(k,v); GET(k) == v for arbitrary binary keys and values
   proptest! {
       #[test]
       fn set_get_roundtrip(key in any::<Vec<u8>>(), value in any::<Vec<u8>>()) {
           let rt = tokio::runtime::Runtime::new().unwrap();
           rt.block_on(async {
               let ctx = TestContext::new().await;
               let mut con = ctx.conn().await;
               let _: () = con.set(&key, &value).await.unwrap();
               let got: Vec<u8> = con.get(&key).await.unwrap();
               prop_assert_eq!(got, value);
           });
       }
   }

   // Property: INCR is commutative — order of increments doesn't matter
   proptest! {
       #[test]
       fn incr_commutative(a in 1i64..1000, b in 1i64..1000) {
           // ... INCRBY a then INCRBY b == INCRBY b then INCRBY a
       }
   }

   // Randomized command sequences: interleave SET, GET, DEL, INCR, APPEND
   // on a small key space, verify model consistency
   #[tokio::test]
   async fn randomized_string_operations() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let mut model: HashMap<String, Vec<u8>> = HashMap::new();
       let mut rng = rand::thread_rng();

       for _ in 0..1000 {
           let key = format!("k{}", rng.gen_range(0..10));
           match rng.gen_range(0..4) {
               0 => { /* SET random value, update model */ }
               1 => { /* GET, assert matches model */ }
               2 => { /* DEL, update model */ }
               3 => { /* APPEND, update model */ }
               _ => unreachable!(),
           }
       }
   }

   // Chunking boundary tests: values at 99_999, 100_000, 100_001, 200_000 bytes
   #[tokio::test]
   async fn chunking_boundaries() { /* ... */ }

   // SET with all flag combinations: EX, PX, NX, XX, KEEPTTL, GET
   #[tokio::test]
   async fn set_flag_matrix() { /* ... */ }
   ```

**Exit criteria**: All string commands pass integration tests (`just test` under 2 seconds) and acceptance tests (`just accept` under 15 seconds). `redis-cli` works interactively. Randomized testing finds no invariant violations.

---

## Milestone 5: Key Management and TTL

**Goal**: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, EXPIRETIME, PEXPIRETIME, TYPE, RENAME, RENAMENX, UNLINK, TOUCH, DBSIZE, SELECT, FLUSHDB, FLUSHALL. Background expiry worker.

**Work**:

1. **TTL commands** (`commands/keys.rs`):
   - EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT: read meta, set expires_at_ms, write to both meta and expire/ directory
   - TTL/PTTL: read meta, compute remaining time. Return -2 if key doesn't exist, -1 if no expiry.
   - PERSIST: clear expires_at_ms, remove expire/ entry
   - EXPIRETIME/PEXPIRETIME: return the absolute expiry timestamp

2. **Key commands**:
   - TYPE: read meta, return type name string ("string", "hash", "set", "zset", "list", "stream", "none")
   - RENAME: read source meta + data, write to destination, delete source. Single transaction.
   - RENAMENX: same but only if destination doesn't exist
   - UNLINK: same as DEL but with async tombstone deletion for large keys
   - TOUCH: update last_accessed_ms, return count of existing keys
   - DBSIZE: range-read count on meta/ directory (or maintain a counter)
   - SELECT: switch connection's active namespace (0-15)
   - FLUSHDB: clear all keys in current namespace directory
   - FLUSHALL: clear all namespace directories

3. **Background expiry worker** (`ttl/background.rs`):
   - Spawn as tokio task on server startup
   - Every 250ms: scan expire/ directory, process up to 1000 expired keys per batch
   - For each expired key: verify timestamp has passed, then delete meta + data + expire entry
   - Large key deletion: if num_chunks > threshold or collection cardinality is large, tombstone and clean up incrementally across multiple batches
   - Metrics: expired_keys_total counter, expiry_backlog gauge

4. **Acceptance tests** (`tests/integration/keys.rs`):
   ```rust
   #[tokio::test]
   async fn expire_and_ttl() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = con.set("foo", "bar").await.unwrap();
       let _: () = con.expire("foo", 10).await.unwrap();
       let ttl: i64 = con.ttl("foo").await.unwrap();
       assert!(ttl > 0 && ttl <= 10);
   }

   #[tokio::test]
   async fn background_expiry() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = redis::cmd("SET").arg("foo").arg("bar").arg("PX").arg(200)
           .query_async(&mut con).await.unwrap();
       // Wait for background worker to clean up (250ms interval + margin)
       tokio::time::sleep(Duration::from_millis(600)).await;
       let exists: bool = con.exists("foo").await.unwrap();
       assert!(!exists);
   }

   #[tokio::test]
   async fn select_isolates_databases() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = con.set("foo", "db0").await.unwrap();
       let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
       let val: Option<String> = con.get("foo").await.unwrap();
       assert_eq!(val, None); // not visible in db 1
   }

   #[tokio::test]
   async fn type_returns_correct_type() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = con.set("str_key", "val").await.unwrap();
       let t: String = redis::cmd("TYPE").arg("str_key")
           .query_async(&mut con).await.unwrap();
       assert_eq!(t, "string");
   }
   ```

**Exit criteria**: TTL expiration works (both lazy and active). Background worker cleans up within 500ms. SELECT/FLUSHDB/FLUSHALL work. `just test` still under 2 seconds. `just accept` under 15 seconds.

---

## Milestones 6-9: Data Structure Commands

Each data structure milestone follows the same two-tier test pattern:

1. Implement the commands
2. Wire up dispatch
3. **`just test` tier**: Add focused integration tests (3-5 per command) — happy path, error path, one edge case. These hit real FDB.
4. **`just accept` tier**: Add exhaustive randomized tests — property-based invariant checks, randomized command sequences against a model, WRONGTYPE cross-type matrix, large collection edge cases.
5. Verify all prior tests still pass in both tiers

### Milestone 6: Hash Commands

**Commands**: HGET, HSET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL, HMGET, HMSET, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD.

**`just test`**: HSET/HGET round-trip, HGETALL on hash with 100 fields, HDEL decrements HLEN, WRONGTYPE when HSET on a string key, HMGET with mix of existing and missing fields.

**`just accept`**: Property tests — HGET(HSET(k, f, v), f) == v for arbitrary binary fields/values, HLEN == len(HKEYS) == len(HVALS). Randomized sequences of HSET/HDEL/HGET on overlapping field sets. Hash with 10K fields (tests snapshot read path).

### Milestone 7: Set Commands

**Commands**: SADD, SREM, SISMEMBER, SMISMEMBER, SCARD, SMEMBERS, SPOP, SRANDMEMBER, SMOVE, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SINTERCARD.

**`just test`**: SADD/SISMEMBER/SREM round-trip, SINTER of two overlapping sets, SCARD after add/remove, SPOP removes and returns a member, SUNION of disjoint sets.

**`just accept`**: Property tests — SINTER(A, B) == SINTER(B, A), SCARD(SADD(k, m)) <= SCARD(k) + 1. Randomized SADD/SREM sequences verified against in-memory HashSet model. Large sets (10K members). SUNION/SDIFF/SINTER on randomized set pairs.

### Milestone 8: Sorted Set Commands

**Commands**: ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZPOPMIN, ZPOPMAX, ZMSCORE, ZRANDMEMBER.

**`just test`**: ZADD/ZSCORE/ZRANK round-trip, ZRANGE returns members in score order, ZADD with negative scores sorts correctly, ZADD NX/XX flags, ZRANGEBYSCORE with -inf and +inf, ZREVRANGE reverse order.

**`just accept`**: Property tests — ZRANK(k, m) + ZREVRANK(k, m) == ZCARD(k) - 1, ZRANGE always sorted, dual index consistency (ZSCORE matches ZRANGE position). Randomized ZADD/ZREM sequences verified against BTreeMap model. Scores at IEEE 754 edge cases (-0, NaN, ±inf, subnormals). Large sorted sets (10K members).

### Milestone 9: List Commands

**Commands**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LTRIM, LREM, LPUSHX, RPUSHX, LINSERT, LPOS, LMOVE, LMPOP.

**`just test`**: LPUSH/RPUSH/LPOP/RPOP round-trip, LRANGE returns correct order, LINDEX with positive and negative indices, LINSERT BEFORE/AFTER, LMOVE between two lists.

**`just accept`**: Property tests — LLEN(LPUSH(k, v)) == LLEN(k) + 1, LPUSH then LPOP returns same element. Randomized interleaved LPUSH/RPUSH/LPOP/RPOP sequences verified against VecDeque model. LRANGE on lists with 5K elements. LREM/LTRIM correctness after random insertions.

**Exit criteria for M6-M9**: Each milestone's acceptance tests pass via `just accept`. All prior milestones' acceptance tests still pass. WRONGTYPE enforcement works across all types. `just test` still under 2 seconds. `just accept` under 15 seconds.

---

## Milestone 10: Transactions (MULTI/EXEC/WATCH)

**Goal**: MULTI, EXEC, DISCARD, WATCH, UNWATCH with Redis-compatible semantics.

**Work**:

1. **Transaction state** (`server/connection.rs`):
   - Per-connection state: `transaction_mode: Option<TransactionState>`
   - `TransactionState`: queued commands, watched keys, error flag
   - MULTI: enter transaction mode, start queuing commands
   - DISCARD: clear queue and watched keys, exit transaction mode
   - Queued commands return `+QUEUED\r\n`
   - Syntax errors during queuing set error flag (EXEC will return error)

2. **EXEC** (`commands/transaction.rs`):
   - If error flag set: return error, discard queue
   - Read watched key versions in preliminary snapshot read
   - Open FDB transaction with `add_read_conflict_key()` only for watched keys
   - Execute each queued command using snapshot reads for all non-write operations
   - Collect results (including per-command errors — no rollback)
   - Commit transaction. If conflict on watched key, return nil (transaction aborted).
   - Return array of results

3. **WATCH/UNWATCH**:
   - WATCH: record key names in connection state (must be called before MULTI)
   - UNWATCH: clear watched keys

4. **Acceptance tests** (`tests/integration/transactions.rs`):
   ```rust
   #[tokio::test]
   async fn multi_exec_atomicity() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let results: Vec<String> = redis::pipe()
           .atomic()
           .set("a", "1").ignore()
           .set("b", "2").ignore()
           .get("a")
           .get("b")
           .query_async(&mut con).await.unwrap();
       assert_eq!(results, vec!["1", "2"]);
   }

   #[tokio::test]
   async fn watch_detects_conflict() {
       let ctx = TestContext::new().await;
       let mut con1 = ctx.conn().await;
       let mut con2 = ctx.conn().await;

       let _: () = con1.set("key", "original").await.unwrap();

       // con1 watches the key
       redis::cmd("WATCH").arg("key").query_async::<()>(&mut con1).await.unwrap();

       // con2 modifies it
       let _: () = con2.set("key", "modified").await.unwrap();

       // con1's transaction should fail
       let result: Option<Vec<String>> = redis::pipe()
           .atomic()
           .set("key", "from_tx").ignore()
           .get("key")
           .query_async(&mut con1).await.ok();
       assert!(result.is_none()); // transaction aborted
   }

   #[tokio::test]
   async fn wrongtype_in_multi_doesnt_abort_other_commands() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = con.set("str_key", "value").await.unwrap();
       // Queue: SET a 1, HSET str_key field val (WRONGTYPE), SET b 2
       // Expect: OK, Error, OK — no rollback
       redis::cmd("MULTI").query_async::<()>(&mut con).await.unwrap();
       redis::cmd("SET").arg("a").arg("1").query_async::<()>(&mut con).await.unwrap();
       redis::cmd("HSET").arg("str_key").arg("f").arg("v").query_async::<()>(&mut con).await.unwrap();
       redis::cmd("SET").arg("b").arg("2").query_async::<()>(&mut con).await.unwrap();
       let results: Vec<redis::Value> = redis::cmd("EXEC").query_async(&mut con).await.unwrap();
       assert_eq!(results.len(), 3);
       // a and b should be set despite the WRONGTYPE error on command 2
       let a: String = con.get("a").await.unwrap();
       assert_eq!(a, "1");
       let b: String = con.get("b").await.unwrap();
       assert_eq!(b, "2");
   }
   ```

**Exit criteria**: MULTI/EXEC works. WATCH detects conflicts. No-rollback semantics verified by acceptance test. `just test` under 2 seconds. `just accept` under 15 seconds.

---

## Milestone 11: Pub/Sub

**Goal**: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE. Cross-instance delivery via FDB.

**Work**:

1. **Message queue** (`pubsub/queue.rs`):
   - PUBLISH: write message to `pubsub/<channel, versionstamp>` + update notification key `pubsub_notify/<channel>`
   - Per-channel message retention: configurable (default 1000 messages or 60 seconds)
   - Background cleanup of old messages

2. **Subscription manager** (`pubsub/subscription.rs`):
   - Per-connection subscription set
   - SUBSCRIBE: add channel to set, start FDB watch on `pubsub_notify/<channel>`
   - Watch fires -> read new messages from queue -> push to client via RESP Push type
   - PSUBSCRIBE: expand pattern to matching channels (re-evaluate periodically)
   - UNSUBSCRIBE: cancel watch, remove from set
   - Connection enters pub/sub mode: only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT allowed

3. **Acceptance tests** (`tests/integration/pubsub.rs`):
   ```rust
   #[tokio::test]
   async fn subscribe_and_publish() {
       let ctx = TestContext::new().await;
       let mut pub_con = ctx.conn().await;
       let mut sub_con = ctx.client.get_async_pubsub().await.unwrap();

       sub_con.subscribe("test_channel").await.unwrap();

       // Give subscription time to register
       tokio::time::sleep(Duration::from_millis(50)).await;

       let _: () = pub_con.publish("test_channel", "hello").await.unwrap();

       let msg = tokio::time::timeout(Duration::from_secs(2), sub_con.on_message().next())
           .await.unwrap().unwrap();
       assert_eq!(msg.get_channel_name(), "test_channel");
       assert_eq!(msg.get_payload::<String>().unwrap(), "hello");
   }
   ```

**Exit criteria**: Pub/sub works across connections. Pattern matching works. Messages delivered in order. `just test` under 2 seconds. `just accept` under 15 seconds.

---

## Milestone 12: Server Commands, Observability, and Polish

**Goal**: INFO, CONFIG, CLIENT, TIME, DBSIZE, RANDOMKEY. Prometheus metrics. OpenTelemetry tracing. TLS. AUTH. Graceful shutdown.

**Work**:

1. **Server commands**: INFO, CONFIG GET/SET, CLIENT LIST/SETNAME/GETNAME/ID, TIME, RANDOMKEY, COMMAND/COMMAND COUNT/COMMAND DOCS
2. **Observability**: Prometheus `/metrics` endpoint, command latency histograms, FDB transaction metrics, OpenTelemetry spans, structured JSON logging
3. **TLS**: Optional via `rustls` + `tokio-rustls`
4. **AUTH**: Basic AUTH command with configurable password
5. **Graceful shutdown**: SIGTERM/SIGINT handler, drain connections, flush workers

6. **Acceptance tests** (`tests/integration/server.rs` — extends the M2 tests):
   ```rust
   #[tokio::test]
   async fn info_returns_server_section() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let info: String = redis::cmd("INFO").arg("server")
           .query_async(&mut con).await.unwrap();
       assert!(info.contains("redis_version"));
       assert!(info.contains("tcp_port"));
   }

   #[tokio::test]
   async fn dbsize_reflects_key_count() {
       let ctx = TestContext::new().await;
       let mut con = ctx.conn().await;
       let _: () = con.set("a", "1").await.unwrap();
       let _: () = con.set("b", "2").await.unwrap();
       let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
       assert_eq!(size, 2);
   }
   ```

**Exit criteria**: INFO works. Metrics exposed. AUTH works. Graceful shutdown works. redis-benchmark runs successfully. `just test` under 2 seconds. `just accept` under 15 seconds.

---

## Milestone 13: Compatibility Testing and Hardening

**Goal**: Run external test suites. Fix failures until 90%+ pass rate. This milestone is largely a formality — our own acceptance suite should already cover the vast majority of behaviors.

**Work**:

1. Clone Redis repo, configure TCL test suite to point at kvdb
2. Run `./runtest` per data type, collect results
3. Triage failures:
   - Missing command: implement or document as unsupported
   - Wrong behavior: fix
   - Cluster/replication/persistence-specific: skip (not applicable)
4. Run redis-py test suite, fix compatibility issues
5. Run go-redis test suite
6. Build compatibility matrix: command, status (pass/fail/skip), notes
7. Performance baseline: redis-benchmark comparison report
8. **Every fix made during this milestone gets a new acceptance test in our suite** so the fix is permanently guarded

**Exit criteria**: 90%+ pass rate on Redis TCL suite for implemented commands. redis-py and go-redis basic tests pass. Performance baseline documented. Our own acceptance suite covers every behavior we've verified.

---

## Dependency Graph

```
M0  Project Skeleton + Test Harness
 |
M1  RESP3 Parser/Encoder
 |
M2  TCP Server Shell + Acceptance Baseline
 |
M3  FDB Storage Layer
 |
M4  String Commands ────────── M5  Key Management & TTL
 |                                |
 ├── M6  Hash Commands            |
 ├── M7  Set Commands             |
 ├── M8  Sorted Set Commands      |
 └── M9  List Commands            |
       |                          |
       M10 Transactions ──────────┘
       |
       M11 Pub/Sub
       |
       M12 Server Commands & Observability
       |
       M13 Compatibility Testing
```

M0-M4 are strictly sequential. After M4, milestones M5-M9 can be worked in parallel (they share the storage layer but don't depend on each other). M10 depends on all data structures being in place. M11-M13 are sequential.

---

## Test Suite Invariants (enforced from M2 onward)

1. **`just` is the reflex.** Lint + focused integration tests against real FDB. Under 2 seconds. You run it after every change without thinking.
2. **`just accept` is the stress test.** Thousands of randomized command sequences, property-based invariant checking, large values, concurrent clients, chaos inputs — all against real server + real FDB. Under 15 seconds. You run it before committing.
3. **`just ci` is the gate.** Lint + test + accept + doc build. Everything CI does. If it passes locally, the PR is green.
4. **Both tiers use `TestContext`** which starts a real server and connects with redis-rs. No mocks anywhere. The difference is breadth: `just test` has a handful of cases per command, `just accept` has thousands of randomized cases.
5. **Tests are isolated**: each test gets its own server on a random port and its own FDB namespace. nextest runs each test in its own process. Tests run fully in parallel.
6. **Every bug fix adds a regression test.** The suite only grows.
7. **Slow tests are killed.** nextest is configured with a 10-second timeout per individual test. Any test exceeding that is terminated and reported as a failure.
8. **`just test` stays fast by being focused, not by being fake.** It hits real FDB on every run. Speed comes from each command having only 3-5 targeted test cases (happy path, error path, key edge case) — not from mocking out the storage layer.

---

## What's NOT in Phase 1

These are explicitly deferred to Phase 2+:

- **Blocking operations** (BLPOP, BRPOP, BLMOVE) — requires parked connection architecture
- **SCAN/HSCAN/SSCAN/ZSCAN** — cursor-based iteration
- **Bitmaps** (GETBIT, SETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD)
- **HyperLogLog** (PFADD, PFCOUNT, PFMERGE)
- **Geospatial** (GEOADD, GEOSEARCH, etc.)
- **Streams** (XADD, XREAD, XRANGE, consumer groups)
- **Lua scripting** (EVAL, EVALSHA)
- **Full eviction policies** (LRU, LFU — Phase 1 has noeviction only)
- **Cluster protocol** (CLUSTER commands, MOVED/ASK redirections)
- **Replication protocol** (REPLICAOF, SYNC)
