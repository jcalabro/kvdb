# Implementation Plan

## Progress

| Milestone | Status | Notes |
|-----------|--------|-------|
| M0: Project Skeleton | **Complete** | All exit criteria met. `just` inner loop at ~1.1s. 8 unit tests passing. |
| M1: RESP3 Parser/Encoder | **Complete** | 99 unit tests, 15K+ proptest cases, 4 fuzz targets (2.2M+ runs, 0 crashes), criterion benchmarks. Parser: ~37ns simple, ~126ns SET cmd. Encoder: ~12ns simple, ~17ns bulk. 2 bugs found and fixed by fuzzer. |
| M2: TCP Server Shell | **Complete** | Connection handler with read/parse/dispatch/encode/write loop. Pipelining support. PING, ECHO, HELLO, QUIT, COMMAND, CLIENT handlers. 10 unit tests, 6 integration tests, 120 total tests passing. redis-cli verified. |
| M3: FDB Storage Layer | **Complete** | Database init (Once-guarded boot), FDB directory layer (8 subspaces/namespace), ObjectMeta with bincode serde + lazy expiry, transparent chunking (100KB, parallel reads), instrumented transaction wrapper (Prometheus + tracing). 11 unit tests, 18 integration tests, 3 property tests (350 cases), 2 benchmarks (serialize ~7ns, deserialize ~8ns). 154 total tests, `just test` at ~1s, `just accept` at ~14s. |
| M4: String Commands | **Complete** | 19 commands (GET, SET, MGET, MSET, DEL, EXISTS, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETRANGE, SETRANGE, SETNX, SETEX, PSETEX, GETDEL). Full SET flags (NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET). Async dispatch, FDB-backed ConnectionState, storage helpers module. 65 integration tests, 7 acceptance tests (property-based + randomized), expanded smoke tests, 5 string benchmarks. 222 total tests, `just test` at ~0.7s, `just accept` at ~4s. |
| M5: Key Management & TTL | **Complete** | 18 commands (EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST, EXPIRETIME, PEXPIRETIME, TYPE, RENAME, RENAMENX, UNLINK, TOUCH, DBSIZE, SELECT, FLUSHDB, FLUSHALL). Background expiry worker (250ms scan, 1000 key batches). NamespaceCache for SELECT. 40 integration tests, 14 acceptance tests. |
| M6: Hash Commands | **Complete** | 15 commands (HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL, HMGET, HMSET, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD). FDB range reads for HGETALL/HKEYS/HVALS. Cardinality tracking via ObjectMeta. 30 integration tests, 9 acceptance tests (property-based + randomized 500-op model checking). |
| M7: Set Commands | **Complete** | 16 commands (SADD, SREM, SISMEMBER, SMISMEMBER, SCARD, SMEMBERS, SPOP, SRANDMEMBER, SMOVE, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SINTERCARD). Direct FDB keys (existence = membership). Parallel existence checks via join_all. Multi-set ops computed in-memory. 37 integration tests, 9 acceptance tests (property-based + randomized model checking). |
| M8: List Commands | **Complete** | 14 commands (LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LTRIM, LREM, LPUSHX, RPUSHX, LINSERT, LPOS). Index-based storage (O(1) push/pop/LINDEX, O(k) LRANGE). Compact-rewrite for LREM/LINSERT. Element size validation (100KB FDB limit). 37 integration tests, 10 acceptance tests (property-based VecDeque model + TTL + WRONGTYPE). 392 total tests, `just test` at ~2s, `just accept` at ~16s. |
| M9: Sorted Set Commands | Not started | Moved from M8; lists promoted to M8 for implementation-order reasons. |
| M9.5: Cross-Key List Commands | Not started | LMOVE, LMPOP. |
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

### What's been built (M1)

**Parser** (`src/protocol/parser.rs`) — Two-phase incremental RESP parser covering all 16 type prefixes (`+`, `-`, `:`, `$`, `*`, `_`, `#`, `,`, `(`, `!`, `=`, `%`, `~`, `>`, `|`). Phase 1 scans for frame completeness without modifying the buffer; phase 2 decodes with zero-copy `Bytes::slice()` for bulk payloads. Safety limits: max nesting depth 128, max line 64KB, max bulk 512MB. Hand-rolled integer parser handles `i64::MIN` without overflow.

**Encoder** (`src/protocol/encoder.rs`) — Full RESP3 native encoding plus RESP2 downgrade (Map→flat Array, Set→Array, Boolean→Integer 0/1, Null→`$-1\r\n`, Double→BulkString, BulkError→simple Error with binary-safe fallback for payloads containing `\r`/`\n`). Uses `itoa`/`ryu` for fast integer/float formatting.

**Types** (`src/protocol/types.rs`) — `RedisCommand::from_resp()` parses Array-of-BulkStrings into command + args with case-insensitive uppercasing. Added `RespValue::ok()` and `RespValue::err()` convenience constructors.

**Error** (`src/error.rs`) — Added `ProtocolError::NestingTooDeep` variant.

**Testing** (99 unit tests + 15K proptest cases + 4 fuzz targets):
- Unit tests cover every RESP type in both parser and encoder, edge cases (empty, null, nested, binary data with embedded `\r\n`), RESP2 downgrade, incremental parsing, error conditions.
- Property tests (`tests/accept_protocol.rs`): RESP3 round-trip, RESP2 encode-then-parse, incremental parsing at arbitrary split points. 5K cases per property, 15K total.
- Fuzz targets: `resp_parser` (raw bytes), `resp_multi_frame` (parse loop), `resp_roundtrip` (structured encode→parse), `resp_encoder` (encoder output validity). 2 bugs found and fixed: boolean/null decoder sync and `i64::MIN` overflow.

**Benchmarks** (`benches/commands.rs`) — Criterion benchmarks for parser and encoder across value sizes and types:
- Parse simple string: ~37ns (~27M ops/sec)
- Parse `SET key value` command: ~126ns (~8M ops/sec)
- Parse 100-PING pipeline: ~5.7µs (~18M cmds/sec)
- Encode simple string: ~12ns (~83M ops/sec)
- Full roundtrip (SET + OK): ~152ns (~6.5M ops/sec)

**`just` inner loop**: 99 tests in ~0.5s. **`just accept`**: 15K+ proptest cases in ~1.1s. **`just fuzz`**: runs all 4 targets (default 30s each).

### What's been built (M2)

**Connection handler** (`src/server/connection.rs`) — Full read/parse/dispatch/encode/write loop with natural pipelining support. Reads into a `BytesMut` buffer, extracts all complete RESP frames via the M1 parser, dispatches each to a command handler, encodes responses via the M1 encoder, and flushes all accumulated responses in a single `write_all`. Per-connection state tracks protocol version (2/3) and selected database (0-15). Prometheus metrics: active connections, command counts by name/status, command duration histograms.

**Command dispatch** (`src/commands/mod.rs`) — `CommandResponse` enum (Reply/Close) for connection lifecycle control. `dispatch()` takes `&mut ConnectionState` for protocol negotiation. Handlers: PING (with optional message), ECHO, HELLO (RESP2/RESP3 protocol negotiation), QUIT (clean close), COMMAND (COUNT/DOCS/LIST/INFO stubs), CLIENT (SETNAME/GETNAME/ID/INFO stubs). 10 unit tests.

**Integration tests** (`tests/server.rs`) — 6 tests covering full TCP round-trip: PING, ECHO, PING with message, pipelining (100 commands), unknown command error, multi-connection independence. Each test uses `TestContext` with real server on random port.

**`just` inner loop**: 120 tests in ~0.04s. **`just accept`**: 7 acceptance tests (15K+ proptest cases) in ~1.6s.

### What's been built (M3)

**Database initialization** (`src/storage/database.rs`) — `boot()` starts the FDB network thread exactly once via `std::sync::Once`, leaking the `NetworkAutoStop` handle for process-lifetime safety. `Database` wraps `foundationdb::Database` in `Arc` with constructors from cluster file path or default.

**Directory layout** (`src/storage/directories.rs`) — `Directories` struct holds 8 `DirectorySubspace` handles (meta, obj, hash, set, zset, zset_idx, list, expire) opened via FDB's directory layer in a single transaction. `root_prefix` parameter enables test isolation (`"kvdb"` in production, `"kvdb_test_<uuid>"` in tests). Helper methods for key construction: `meta_key()`, `obj_chunk_key()`, `expire_key()`, `obj_key_range()`.

**ObjectMeta** (`src/storage/meta.rs`) — Per-key metadata with serde/bincode serialization (~50-60 bytes). `read()` includes lazy expiry check via `now_ms` parameter. `write()` and `delete()` are synchronous (buffer in transaction). 8 unit tests covering all key types, serde roundtrips, and expiry edge cases.

**Value chunking** (`src/storage/chunking.rs`) — Transparent splitting at 100,000-byte boundaries. `write_chunks()` splits and stores sequentially. `read_chunks()` fires parallel `get()` futures for all chunks. `delete_chunks()` uses `clear_range`. 3 unit tests for chunk_count calculations.

**Transaction wrapper** (`src/storage/transaction.rs`) — `run_transact()` wraps FDB's auto-retry `db.run()` with 5-second timeout, Prometheus duration/conflict metrics, and tracing `info_span`.

**Error handling** (`src/error.rs`) — `StorageError` enum with 5 variants (Fdb, FdbBinding, Serialization, DataCorruption, Directory) wired into top-level `Error` via `#[from]`.

**Integration tests** (`tests/storage.rs`) — 18 tests against real FDB: ObjectMeta CRUD (6), chunking at various sizes and boundaries (7), directory verification (4), transaction wrapper (1). Each test isolated via UUID root prefix with automatic cleanup.

**Property tests** (`tests/accept_storage.rs`) — 3 proptest suites: chunk round-trip (100 cases, 0-1MB random data), ObjectMeta serde round-trip (200 cases), ObjectMeta FDB round-trip (50 cases).

**Benchmarks** (`benches/commands.rs`) — ObjectMeta serialize (~7ns) and deserialize (~8ns) via criterion.

**`just test`**: 151 tests in ~1.0s. **`just accept`**: 10 acceptance tests (350 proptest cases) in ~14s.

### What's been built (M4)

**Async command dispatch** (`src/commands/mod.rs`) — `dispatch()` is now `pub async fn`, enabling FDB-backed commands. All 19 string commands routed by name. Prometheus metric labels for all commands.

**ConnectionState with FDB** (`src/server/connection.rs`) — `ConnectionState` holds `Database` and `Directories` handles. Server initializes FDB on startup (`listener.rs`) with retry logic for directory conflicts. Each connection gets cloned handles.

**Storage helpers** (`src/storage/helpers.rs`) — High-level `get_string()`, `write_string()`, `delete_object()` that encapsulate the ObjectMeta + type-check + chunk read/write cycle. Error conversion helpers (`cmd_err`, `storage_err`, `storage_err_to_resp`). Type-agnostic `delete_data_for_meta()` handles cleanup for all key types (String, Hash, Set, SortedSet, List, Stream).

**String commands** (`src/commands/strings.rs`, ~1100 lines) — 19 handlers:
- **Core**: GET, SET (full flags: NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET), GETDEL
- **Multi-key**: MGET (parallel-safe), MSET, DEL, EXISTS
- **Numeric**: INCR, DECR, INCRBY, DECRBY (shared `incr_by_impl`, checked overflow), INCRBYFLOAT (NaN/Inf protection, Redis float formatting)
- **String ops**: APPEND, STRLEN (meta-only read), GETRANGE (negative indices), SETRANGE (zero-padding, 512MB limit)
- **Sugar**: SETNX (returns integer), SETEX, PSETEX

SET flag parsing validates all mutual exclusions. TTL computation happens inside FDB transactions (no drift on retry). WRONGTYPE enforced for all string-specific commands; DEL/EXISTS/MGET are type-agnostic.

**Error propagation** (`src/error.rs`, `src/storage/transaction.rs`) — `StorageError::Command` variant extracts `CommandError` from `FdbBindingError::CustomError` via downcast, preserving Redis error prefixes (WRONGTYPE, ERR).

**Test harness** (`tests/harness/mod.rs`) — `TestContext` creates isolated FDB namespace per test (`kvdb_test_<uuid>`), injects into server config, cleans up on Drop. `write_fake_meta()` helper for WRONGTYPE testing.

**Integration tests** (`tests/strings.rs`) — 65 tests covering all 19 commands: happy paths, error paths (arity, flag conflicts, invalid TTLs, overflow, NaN/Inf), WRONGTYPE enforcement, expiry interaction, chunking (500KB values).

**Acceptance tests** (`tests/accept_strings.rs`) — 7 tests: SET/GET roundtrip property (100 cases), INCR commutativity (50 cases), STRLEN/APPEND properties, randomized 500-op command sequences against HashMap model, chunking boundaries (99999-300001 bytes), SET flag combination matrix.

**Smoke tests** (`examples/smoke.rs`) — 22 happy-path + 11 error-path checks. Post-error liveness verification.

**Benchmarks** (`benches/commands.rs`) — 5 full-stack benchmarks: set_64b, get_64b, set_get_1kb, incr, mset_10.

**`just test`**: 205 tests in ~0.7s. **`just accept`**: 17 acceptance tests in ~4s.

### What's been built (M5)

**Key management** (`src/commands/keys.rs`, ~1400 lines) — 18 handlers:
- **TTL operations**: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT (shared implementation with type overload), TTL, PTTL (return -2 for missing, -1 for no expiry), PERSIST (clear TTL), EXPIRETIME, PEXPIRETIME (absolute timestamps)
- **Key operations**: TYPE (returns string type name), RENAME (atomic move), RENAMENX (conditional rename), UNLINK (DEL alias — async tombstone deletion deferred to Phase 2), TOUCH (update last_accessed_ms)
- **Database operations**: DBSIZE (range-read count on meta/ directory), SELECT (namespace switching with NamespaceCache), FLUSHDB (clear current namespace), FLUSHALL (clear all 16 namespaces)

**Namespace cache** (`src/storage/namespace.rs`) — `NamespaceCache` struct holds directories for all 16 namespaces, lazy-initialized on first SELECT. Wrapped in Arc<Mutex<>> for safe concurrent access across connections. Per-connection state tracks current namespace (0-15).

**Background expiry worker** (`src/ttl/worker.rs`) — Spawned as tokio task on server startup. Every 250ms: scans expire/ directory, processes up to 1000 expired keys per batch. For each expired key: verifies timestamp has passed, deletes meta + data + expire entry. Handles all key types (String, Hash, Set, SortedSet, List) via shared `delete_data_for_meta()` helper. Prometheus metrics: expired_keys_total counter.

**Storage helpers** (`src/storage/helpers.rs`) — Enhanced `delete_data_for_meta()` to handle deletion logic for all key types, used by both DEL command and background expiry worker.

**Integration tests** (`tests/keys.rs`) — 40 tests covering all 18 commands: EXPIRE/TTL/PERSIST/EXPIRETIME round-trips, background expiry timing verification, RENAME atomicity, RENAMENX conditional logic, SELECT namespace isolation, FLUSHDB/FLUSHALL cleanup, DBSIZE accuracy, TYPE returns correct types, TOUCH updates without modifying data, WRONGTYPE enforcement for type-specific expiry operations.

**Acceptance tests** (`tests/accept_keys.rs`) — 14 tests: TTL/EXPIRETIME property tests (100 cases), SELECT isolation with randomized cross-namespace operations (50 cases), FLUSHDB/FLUSHALL correctness, DBSIZE accuracy after randomized SET/DEL sequences (200 cases), RENAME/RENAMENX property tests, randomized key lifecycle sequences (EXPIRE/PERSIST/TYPE/DEL/TOUCH interleaved, verified against in-memory model, 500 ops).

**`just test`**: 253 tests in ~0.8s. **`just accept`**: 31 acceptance tests in ~5.7s.

### What's been built (M6)

**Hash commands** (`src/commands/hashes.rs`, ~1050 lines) — 15 handlers:
- **Core**: HSET (variadic field-value pairs), HGET, HDEL (variadic), HEXISTS, HLEN (O(1) from cardinality)
- **Bulk reads**: HGETALL, HKEYS, HVALS (paginated FDB range reads), HMGET (parallel lookups via join_all)
- **Mutations**: HINCRBY (checked overflow), HINCRBYFLOAT (NaN/Inf protection, Redis float formatting), HSETNX (conditional set)
- **Utilities**: HSTRLEN (field value length), HRANDFIELD (positive count = distinct, negative count = duplicates, WITHVALUES flag), HMSET (deprecated alias)

Hash fields are stored as individual FDB key-value pairs: `hash/<redis_key, field_name> -> field_value`. ObjectMeta tracks cardinality (field count). Parallel existence checks via `join_all` for multi-field operations. `read_hash_fields()` helper handles paginated range reads.

**Integration tests** (`tests/hashes.rs`) — 30 tests covering all 15 commands: HSET/HGET round-trip, multi-field operations, cardinality tracking, WRONGTYPE enforcement, HRANDFIELD count modes, HINCRBY overflow, HINCRBYFLOAT edge cases.

**Acceptance tests** (`tests/accept_hashes.rs`) — 9 tests: HSET/HGET round-trip property (100 cases), HLEN matches distinct field count (50 cases), HLEN/HKEYS/HVALS consistency (50 cases), HGETALL with 100 fields, HINCRBY commutativity (50 cases), randomized 500-op hash operations against HashMap model (30 cases), lazy expiry integration, HSET on expired hash, WRONGTYPE cross-type matrix.

**`just test`**: 291 tests in ~1.3s. **`just accept`**: 40 acceptance tests in ~8.5s.

### What's been built (M7)

**Set commands** (`src/commands/sets.rs`, ~650 lines) — 16 handlers:
- **Core**: SADD (variadic, deduplicates within call), SREM (variadic), SISMEMBER, SMISMEMBER (parallel lookups), SCARD (O(1) from cardinality), SMEMBERS (paginated range read)
- **Random selection**: SPOP (with optional count), SRANDMEMBER (positive count = distinct, negative count = allows duplicates)
- **Multi-set**: SINTER, SUNION, SDIFF (load all sets into HashSet, compute in-memory), SINTERSTORE, SUNIONSTORE, SDIFFSTORE (compute + write result to destination)
- **Utilities**: SMOVE (atomic move between sets), SINTERCARD (with LIMIT short-circuit)

Set members are stored as individual FDB keys with empty values: `set/<redis_key, member> -> b""`. Membership is determined by key existence — no value payload needed. This differs from the Go MVP's roaring bitmap approach but better fits FDB's strengths: no serialization bottleneck, better concurrency (each member is an independent key), simpler code. ObjectMeta tracks cardinality. Shared helpers: `read_set_members()` (paginated range read), `read_set_meta_for_write()` (type check + expired cleanup), `read_set_meta_for_read()` (type check only). Multi-set operations use `multi_set_op()` generic over the set operation function.

**Integration tests** (`tests/sets.rs`) — 37 tests covering all 16 commands: SADD/SISMEMBER/SREM round-trip, duplicate deduplication within SADD, SCARD after add/remove, SMEMBERS enumeration, SPOP removal + cardinality, SRANDMEMBER with positive/negative count, SMOVE between sets, SINTER/SUNION/SDIFF correctness, store variants overwrite existing keys, SINTERCARD with LIMIT, WRONGTYPE enforcement, TTL preservation.

**Acceptance tests** (`tests/accept_sets.rs`) — 9 tests: SADD/SISMEMBER round-trip property (100 cases), SCARD monotonicity (50 cases), SINTER commutativity (50 cases), SUNION superset property (50 cases), randomized SADD/SREM sequences against HashSet model (30 cases), multi-set ops (SINTER/SUNION/SDIFF) against model (30 cases), large set with 1K members, WRONGTYPE cross-type matrix (set vs hash).

**Smoke tests** (`examples/smoke.rs`) — ~30 set validation checks: SADD, SCARD, SISMEMBER, SMISMEMBER, SMEMBERS, SREM, SPOP, SRANDMEMBER, SMOVE, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SINTERCARD (with LIMIT), WRONGTYPE enforcement.

**`just test`**: 333 tests in ~1.3s. **`just accept`**: 49 acceptance tests in ~10.5s.

### What's been built (M8)

**List commands** (`src/commands/lists.rs`, ~1200 lines) — 14 handlers:
- **Core**: LPUSH, RPUSH (variadic, create-on-demand), LPOP, RPOP (with optional count), LLEN (O(1) from meta), LINDEX (O(1) direct lookup)
- **Range**: LRANGE (paginated FDB range reads with Redis clamping), LSET (O(1) element update), LTRIM (range keep with clear_range)
- **Conditional push**: LPUSHX, RPUSHX (only-if-exists semantics)
- **Linear scan**: LREM (count > 0 forward, count < 0 backward, count == 0 all), LINSERT (BEFORE/AFTER pivot), LPOS (RANK/COUNT/MAXLEN options)

**Design: index-based storage** — Elements stored at `list/<key, i64_index>` with signed integer indices. `ObjectMeta` tracks `list_head` (inclusive), `list_tail` (inclusive), `list_length`. LPUSH decrements head, RPUSH increments tail, giving O(1) push/pop and O(1) random access via LINDEX. This is a strict improvement over the Go MVP's linked-list approach (O(n) LINDEX, 3+ FDB reads per pop). LREM/LINSERT use compact-rewrite: read all elements, splice in memory, clear old range, rewrite at contiguous indices [0, N-1] — O(n) same as Redis.

**Element size validation** — List elements are stored directly as FDB values (not chunked). Elements exceeding 100,000 bytes (FDB's hard limit) are rejected with a clear error message. This applies to LPUSH, RPUSH, LSET, and LINSERT.

**LPOS optimization** — When MAXLEN is specified, only the relevant portion of the list is read from FDB (first N or last N elements depending on RANK direction), avoiding unnecessary reads on large lists.

**Shared helpers**: `read_list_meta_for_write()` (type check + expired cleanup), `read_list_meta_for_read()` (type check only), `resolve_index()` (logical-to-FDB index translation), `normalize_range()` (Redis clamping semantics), `read_element_range()` (paginated FDB range reads), `write_or_delete_list_meta()` (preserves TTL, deletes key when empty), `compact_rewrite()` (clear + rewrite for LREM/LINSERT).

**Integration tests** (`tests/lists.rs`) — 37 tests covering all 14 commands: LPUSH/RPUSH ordering (reversed vs preserved), interleaved push, LPOP/RPOP single and with count, count exceeds length, missing key returns Nil, LLEN on nonexistent and wrong type, LINDEX positive/negative/out-of-bounds, LRANGE full/subset/clamp, LSET update/out-of-range/no-such-key, LTRIM basic/empty-range/single-element, LPUSHX/RPUSHX existing-list/nonexistent-noop, LREM positive/negative/zero count and removes-all-deletes-key, LINSERT before/after and missing pivot/key, LPOS basic/RANK/COUNT/RANK-zero-rejection, WRONGTYPE matrix (11 commands on string key), arity errors (9 cases).

**Acceptance tests** (`tests/accept_lists.rs`) — 10 tests: LPUSH/LINDEX roundtrip (100 cases), LPUSH/LPOP roundtrip (100 cases), LLEN grows-by-one (50 cases), RPUSH/LRANGE order preservation (50 cases), LREM matches Vec::retain model (30 cases), LTRIM matches slice model (30 cases), large list pagination 3-5K elements (3 cases), randomized LPUSH/RPUSH/LPOP/RPOP/LLEN/LINDEX against VecDeque model (30 cases), list lazy expiry, LPUSH on expired string creates new list, WRONGTYPE list vs hash cross-type.

**Smoke tests** (`examples/smoke.rs`) — ~30 list validation checks: LPUSH, RPUSH, LLEN, LRANGE, LINDEX, LPOP, RPOP, LSET, LTRIM, LINSERT, LREM, LPOS, LPUSHX, RPUSHX, WRONGTYPE enforcement. Load phase includes list operations (LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE at ~10% of workload).

**`just test`**: 392 tests in ~2s. **`just accept`**: 60 acceptance tests in ~16s.

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

### Milestone 8: List Commands

**Commands**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LTRIM, LREM, LPUSHX, RPUSHX, LINSERT, LPOS.

**Design**: Index-based storage (not linked-list). Elements stored at `list/<key, i64_index>`. ObjectMeta tracks `list_head` (inclusive), `list_tail` (inclusive), `list_length`. LPUSH decrements head, RPUSH increments tail, giving O(1) push/pop and O(1) random access via LINDEX. LINSERT/LREM use compact-rewrite (read all, splice, clear range, rewrite at [0, N-1]) — O(n) same as Redis. See `docs/M9_LIST_PLAN.md` for full design.

**`just test`**: LPUSH/RPUSH/LPOP/RPOP round-trip, LRANGE returns correct order, LINDEX with positive and negative indices, LINSERT BEFORE/AFTER, LREM with positive/negative/zero count, LPOS with RANK/COUNT, LTRIM boundary cases.

**`just accept`**: Property tests — LLEN(LPUSH(k, v)) == LLEN(k) + 1, LPUSH then LPOP returns same element. Randomized interleaved LPUSH/RPUSH/LPOP/RPOP sequences verified against VecDeque model. LRANGE on lists with 5K elements. LREM/LTRIM correctness after random insertions. TTL expiry integration. Cross-type WRONGTYPE matrix.

**Exit criteria for M6-M8**: Each milestone's acceptance tests pass via `just accept`. All prior milestones' acceptance tests still pass. WRONGTYPE enforcement works across all types. `just test` still under 2 seconds. `just accept` under 15 seconds.

---

### Milestone 9: Sorted Set Commands

**Commands**: ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZPOPMIN, ZPOPMAX, ZMSCORE, ZRANDMEMBER.

**`just test`**: ZADD/ZSCORE/ZRANK round-trip, ZRANGE returns members in score order, ZADD with negative scores sorts correctly, ZADD NX/XX flags, ZRANGEBYSCORE with -inf and +inf, ZREVRANGE reverse order.

**`just accept`**: Property tests — ZRANK(k, m) + ZREVRANK(k, m) == ZCARD(k) - 1, ZRANGE always sorted, dual index consistency (ZSCORE matches ZRANGE position). Randomized ZADD/ZREM sequences verified against BTreeMap model. Scores at IEEE 754 edge cases (-0, NaN, ±inf, subnormals). Large sorted sets (10K members).

### Milestone 9.5: Cross-Key List Commands

**Commands**: LMOVE, LMPOP.

**Why separate**: These commands involve cross-key atomicity (LMOVE pops from one list and pushes to another; LMPOP scans multiple keys). FDB makes this trivially atomic within a single transaction, but the commands are more complex to implement and were added to Redis relatively late (6.2+). Separating them keeps M8 focused on the core list data structure.

**`just test`**: LMOVE between two lists (LEFT/RIGHT x LEFT/RIGHT), LMOVE same-key rotation, LMOVE from nonexistent key returns Nil, LMPOP pops from first non-empty list in order, LMPOP with COUNT, LMPOP all keys empty returns Nil.

**`just accept`**: LMOVE preserves element values across source/destination, LMOVE source deletion when emptied, LMPOP ordering determinism (always picks first non-empty key).

**Exit criteria**: All M8 + M9.5 acceptance tests pass. Prior milestones unaffected. `just test` under 2 seconds. `just accept` under 15 seconds.

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
 ├── M6  Hash Commands ✓          |
 ├── M7  Set Commands ✓           |
 ├── M8  List Commands ✓          |
 └── M9  Sorted Set Commands      |
       |                          |
       M10 Transactions ──────────┘
       |
       M11 Pub/Sub
       |
       M12 Server Commands & Observability
       |
       M13 Compatibility Testing
```

M0-M4 are strictly sequential. After M4, milestones M5-M9 can be worked in parallel (they share the storage layer but don't depend on each other). M10 depends on all data structures being in place. M11-M13 are sequential. Lists were promoted to M8 and sorted sets moved to M9 for implementation-order reasons.

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
