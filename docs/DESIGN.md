# Redis-on-FoundationDB Design Document

**Project**: Production-grade Redis-compatible database atop FoundationDB  
**Language**: Rust  
**Quality Bar**: 10+ year production lifespan

---

## Executive Summary

This document outlines the architecture for building a Redis-compatible database layer on FoundationDB using Rust, prioritizing correctness, performance, and observability.

### Critical Requirements

- **RESP3 Protocol**: **REQUIRED** - Full RESP3 implementation for modern type semantics and client compatibility
- **RESP2 Backward Compatibility**: **REQUIRED** - Must support RESP2 clients via automatic detection/fallback
- **Testing First**: Property testing, fuzzing, chaos engineering from day one
- **Production Quality**: 10-year support horizon, enterprise-grade reliability
- **Comprehensive Observability**: Prometheus, OpenTelemetry, Tracy profiling built-in

---

## Table of Contents

1. Architecture Overview
2. Protocol Implementation (RESP3 Required)
3. Data Model and Key Layout
4. Command Implementation Strategy
5. Transaction Boundaries
6. Performance Architecture
7. Observability and Instrumentation  
8. Testing Strategy (Day One Priority)
9. Technology Stack
10. Implementation Phases
11. Operational Considerations

---

## 1. Architecture Overview

See accompanying research documents for detailed information:
- REDIS_ANALYSIS.md: Complete Redis command analysis
- fdb_research.md: FoundationDB capabilities and patterns
- RUST_ECOSYSTEM_GUIDE.md: Rust tooling and libraries
- TESTING_ROADMAP.md: Comprehensive testing strategy

### Key Design Principles

1. **RESP3 First**: Full RESP3 implementation with RESP2 backward compatibility
2. **Testing from Day One**: Every component tested with multiple strategies
3. **Zero-Copy**: Use bytes crate for efficient protocol handling
4. **FDB-Native**: Leverage strict serializability (stronger than Redis!), ordered keys, atomic operations
5. **Observable**: Metrics, traces, and profiling instrumentation throughout
6. **Production-Grade**: Built for 10+ year operational lifespan

---

## 2. Protocol Implementation

**RESP3 Protocol (REQUIRED)**:
- All RESP3 types (Null, Boolean, Double, BigNumber, BulkError, VerbatimString, Map, Set, Push, Attribute)
- All RESP2 types for backward compatibility (SimpleString, Error, Integer, BulkString, Array)
- HELLO command for protocol negotiation
- Automatic RESP2/RESP3 detection and fallback
- Zero-copy parsing using bytes crate
- Pipelining support (critical for performance)

**Design Rationale**: 
- RESP3 provides superior type semantics and is the future of Redis protocol
- RESP2 backward compatibility ensures existing clients work seamlessly
- Automatic protocol detection eliminates client migration complexity

---

## 3. Data Model and Key Layout

### FDB Directory Structure

Each namespace (Redis database 0-15) is organized using FDB's directory layer into separate subspaces. This provides clean isolation and efficient prefix-based operations. The layout follows the pattern established in the original Go kvdb implementation:

```
kvdb/
  <ns>/                         # namespace (0-15, maps to SELECT)
    meta/                       # per-key metadata (type, TTL, chunk count)
      <key>                     # -> ObjectMeta (protobuf or bincode)
    obj/                        # raw object data (chunked)
      <key, chunk_offset>       # -> chunk bytes (max 100,000 bytes each)
    hash/                       # hash field storage
      <key, field>              # -> field value (chunked if needed)
    set/                        # set members
      <key, member>             # -> empty
    zset/                       # sorted set score index
      <key, score, member>      # -> empty (score uses tuple layer float encoding)
    zset_idx/                   # sorted set reverse index
      <key, member>             # -> score
    list/                       # list elements
      <key, index>              # -> element (chunked if needed)
    stream/                     # stream entries
      <key, entry_id>           # -> field data
    expire/                     # expiry bookkeeping
      <key>                     # -> expiry timestamp (ms)
```

### Namespace Mapping

The namespace maps directly to Redis `SELECT` (databases 0-15). By default, `ns = 0`. After `SELECT 3`, all subsequent commands operate on `ns = 3`. The 16-database limit from Redis is enforced for compatibility. FLUSHDB clears all keys within the current namespace directory; FLUSHALL clears all namespace directories.

### ObjectMeta — Type Enforcement and Metadata

Every Redis key has a corresponding entry in the `meta/` directory. ObjectMeta is read on every command and serves triple duty:

1. **Type enforcement**: A `type` field (enum: String, Hash, Set, SortedSet, List, Stream) prevents WRONGTYPE errors. If a client runs `SET foo bar` and then `HSET foo field value`, the second command reads ObjectMeta, sees type=String, and returns `WRONGTYPE Operation against a key holding the wrong kind of value`.
2. **Chunk tracking**: `num_chunks` and `size_bytes` fields enable transparent value chunking.
3. **TTL tracking**: `expires_at_ms` field (millisecond timestamp, or 0 for no expiry) enables lazy expiration on every read.

This design mirrors the original Go kvdb (`ObjectMeta` protobuf with `oneof type`). The metadata read is unavoidable — every command needs to know the type, chunk count, and TTL — so we pay for one metadata read per command but get all three checks for free.

### Value Chunking (Transparent)

Values exceeding 100,000 bytes are automatically split into chunks stored at `obj/<key, 0>`, `obj/<key, 1>`, etc. This is transparent to the command layer:

- **Write path**: `write_object(key, data)` splits into 100,000-byte chunks, updates `ObjectMeta.num_chunks`
- **Read path**: `read_object(key)` reads `ObjectMeta.num_chunks`, fires all chunk `get()` futures in parallel, concatenates results
- **All data types use chunking**: string values, hash field values, list elements, serialized set data — anything that could exceed 100KB

This follows the original Go kvdb pattern (`object.go:116-196`) but adds parallel chunk reads from day one (the Go version has a TODO for this).

### Data Structure Mappings

**Strings**:
- Meta: `meta/<key>` -> ObjectMeta { type: String, num_chunks, size_bytes, expires_at_ms }
- Data: `obj/<key, chunk_offset>` -> chunk bytes
- Simple key-value. Chunked transparently for values > 100KB.

**Hashes**:
- Meta: `meta/<key>` -> ObjectMeta { type: Hash, cardinality, expires_at_ms }
- Fields: `hash/<key, field>` -> field value (chunked if field value > 100KB)
- Cardinality maintained via FDB atomic `add` for conflict-free HLEN.

**Sets**:
- Meta: `meta/<key>` -> ObjectMeta { type: Set, cardinality, expires_at_ms }
- Members: `set/<key, member>` -> empty (existence = membership)
- Cardinality maintained via FDB atomic `add` for conflict-free SCARD.

**Sorted Sets (Dual Index)**:
- Meta: `meta/<key>` -> ObjectMeta { type: SortedSet, cardinality, expires_at_ms }
- Score index: `zset/<key, score, member>` -> empty
- Reverse index: `zset_idx/<key, member>` -> score bytes
- Cardinality maintained via FDB atomic `add` for conflict-free ZCARD.
- **Score encoding**: Scores MUST use FDB tuple layer float encoding (order-preserving XOR transform on IEEE 754 doubles). Never use raw `struct.pack` — it produces incorrect ordering for negative scores, NaN, -0, and ±inf.

**Lists (Gapped Index Array)**:
- Meta: `meta/<key>` -> ObjectMeta { type: List, head_index, tail_index, length, expires_at_ms }
- Elements: `list/<key, index>` -> element (chunked if > 100KB)
- Head/tail operations use read-modify-write on ObjectMeta (do NOT use atomic add — the result of an atomic op cannot be read in the same transaction)
- Length maintained via FDB atomic `add` for conflict-free LLEN
- Indices spaced by a configurable gap (e.g., 1000) for LINSERT at midpoints

**Streams**:
- Meta: `meta/<key>` -> ObjectMeta { type: Stream, length, first_entry_id, last_entry_id, expires_at_ms }
- Entries: `stream/<key, entry_id>` -> field data
- Consumer Groups: separate metadata keyspace
- Use versionstamps for ordering

### TTL and Expiration

FDB has no built-in key expiration. We implement a hybrid lazy + active approach (following the original Go kvdb pattern):

**Lazy expiration (on every read)**:
- Every command reads ObjectMeta first. If `expires_at_ms > 0 && now_ms > expires_at_ms`, the key is treated as non-existent and the data is cleaned up opportunistically.

**Active expiration (background worker)**:
- A background task runs every 250ms, scanning the `expire/` directory for keys past their TTL
- Processes up to 1000 keys per batch to respect FDB transaction limits
- Each expired key: delete ObjectMeta, delete all data keys (obj/, hash/, set/, etc.), delete expire/ entry
- For large keys (many chunks, large collections), deletion is done via async tombstone (see §5)

**Expiry bookkeeping**:
- When EXPIRE/PEXPIRE is called, the expiry timestamp (milliseconds) is written to both `ObjectMeta.expires_at_ms` and `expire/<key>` (so the background worker can find expiring keys without scanning all metadata)
- PERSIST removes both entries
- Timestamps are always milliseconds for PEXPIRE compatibility

### Eviction Policies

Eviction (LRU/LFU/maxmemory) is distinct from TTL expiration. It is implemented as a later phase but designed for from day one:

- ObjectMeta includes an `last_accessed_ms` field updated on reads (using FDB atomic `max` to avoid write conflicts)
- `CONFIG SET maxmemory <bytes>` sets a storage limit tracked by periodic DBSIZE estimation
- When storage exceeds maxmemory, a background eviction worker samples keys and evicts according to the configured policy (noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl)
- Eviction is sampling-based (like Redis) — not exact LRU. Sample 5-10 keys, evict the best candidate.
- Phase 1: noeviction only (reject writes when limit reached). Full eviction policies in Phase 3+.

---

## 4. Command Implementation Strategy

### Phase 1: MVP (~70 commands)

- Strings (15): GET, SET, MGET, MSET, INCR, DECR, etc.
- Hashes (13): HGET, HSET, HGETALL, HINCRBY, etc.
- Lists (10): LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc. (non-blocking only)
- Sets (9): SADD, SREM, SISMEMBER, SINTER, etc.
- Sorted Sets (10): ZADD, ZREM, ZRANGE, ZRANK, etc.
- Keys (8): EXISTS, DEL, EXPIRE, TTL, etc.
- Transactions (3): MULTI, EXEC, WATCH
- Server (2): PING, INFO
- Pub/Sub (3): SUBSCRIBE, PUBLISH, UNSUBSCRIBE (via FDB watches + versionstamp queue)
- **Explicitly deferred**: blocking operations (BLPOP, BRPOP, BLMOVE), full eviction policies

### Phase 2: Advanced Commands (~60 commands)

- Blocking operations (BLPOP, BRPOP, BLMOVE) — requires parked connection architecture
- Scan operations (SCAN, HSCAN, SSCAN, ZSCAN)
- Bitmaps, HyperLogLog, Geospatial
- Basic streams (no consumer groups)
- Full eviction policies (LRU, LFU, volatile-ttl)

### Phase 3-5: Streams, Pub/Sub, Scripting

See detailed breakdown in Implementation Phases section.

---

## 5. Transaction Boundaries

### General Rule

One Redis command = One FDB transaction (for simplicity and atomicity)

### Exceptions

1. **MULTI/EXEC**: Multiple commands in single FDB transaction
2. **Large operations**: Split across multiple transactions (see below)
3. **Blocking operations (Phase 2)**: Multiple transactions with watches

### Handling FDB Transaction Limits for Large Operations

FDB enforces hard limits (5-second timeout, 10MB transaction size) that can be exceeded by legitimate Redis operations on large collections. Strategy by operation type:

**Large reads (HGETALL, SMEMBERS, ZRANGE 0 -1 on big collections)**:
- Use **snapshot reads** — they don't add to the conflict set and tolerate longer execution
- If the collection exceeds what can be read in a single transaction, paginate across multiple transactions with continuation tokens
- Stream results back to the client as they're read (RESP arrays support incremental encoding)
- Trade-off: pagination breaks strict atomicity (another write could interleave between pages). This is acceptable — even Redis HSCAN/SSCAN/ZSCAN don't guarantee atomicity across cursor iterations.

**Large writes (MSET with thousands of keys)**:
- Split into batches of ~500 keys per FDB transaction
- Execute batches sequentially
- Document that MSET is not atomic beyond FDB transaction limits (Redis cluster mode has similar limitations)

**Large deletes (DEL on a key with millions of sub-keys)**:
- Mark ObjectMeta as deleted (tombstone) and return immediately — O(1) for the client
- Background worker cleans up actual data in batches (1000 keys per transaction)
- Tombstoned keys are invisible to all commands (treated as non-existent)
- This is critical for sorted sets, hashes, and lists that can grow unbounded

**MULTI/EXEC with many commands**:
- If the queued commands would exceed 10MB of writes, abort at EXEC time with an error rather than silently failing mid-commit
- Pre-flight estimation: sum approximate write sizes during queuing, reject if over ~8MB (leaving headroom)

### MULTI/EXEC Mapping

FDB's strict serializability provides **stronger** guarantees than Redis:
- Redis MULTI/EXEC: Serializable within single instance, but NO cross-key atomicity in cluster mode
- FDB transactions: Strict serializability across entire cluster, true ACID semantics

**No-Rollback Emulation (REQUIRED)**: Redis MULTI/EXEC explicitly provides **no rollback** on execution errors. If command 3 of 5 fails with a WRONGTYPE error, commands 1, 2, 4, and 5 still execute and their effects persist. Many Redis applications rely on this partial-execution behavior. FDB transactions are all-or-nothing by default, so the implementation must emulate Redis semantics:
- Queue all commands client-side during MULTI
- Execute each command within a single FDB transaction, collecting individual results (including per-command errors)
- Always commit the FDB transaction regardless of per-command errors — only abort if the FDB transaction itself conflicts
- Syntax errors detected during queuing (before EXEC) abort the entire transaction, matching Redis behavior

**WATCH Emulation**: FDB's conflict detection adds ALL keys read within a transaction to the read conflict set, not just explicitly WATCHed keys. To faithfully emulate Redis WATCH:
1. Read WATCHed key versions in a preliminary read before the MULTI transaction begins
2. Within the MULTI/EXEC transaction, use `add_read_conflict_key()` only for WATCHed keys
3. Use **snapshot reads** for all other reads within the transaction to avoid adding them to the conflict set
4. This ensures only modifications to explicitly WATCHed keys cause transaction failure, matching Redis behavior

**Design Implication**: We provide stronger consistency than Redis (cross-cluster atomicity), but emulate Redis's no-rollback and explicit-WATCH-only semantics for compatibility.

---

## 6. Performance Architecture

### Optimization Strategies

1. **Atomic Operations**: Use FDB atomic ops for conflict-free counters (INCR, HINCRBY) — note: this requires storing counter values as raw little-endian i64 bytes rather than Redis's decimal string encoding. A dual-representation strategy is needed to handle `SET counter "42"` followed by `INCR counter` (read-modify-write for string-encoded values, atomic add for values written exclusively via INCR/DECR).
2. **Zero-Copy Parsing**: bytes crate for protocol handling
3. **Connection Pooling**: Tokio-based async with semaphore limits (though FDB Database is cheap to clone)
4. **Pipelining**: Batch command execution where safe
5. **Hot Key Sharding**: **CRITICAL** - Manual sharding required for keys exceeding ~100K reads/sec (FDB triple-replication limit)

### FDB Constraints (Hard Limits - Not Configurable!)

- **5-second transaction time limit**: Architectural constraint due to 5-second MVCC window in storage servers
- **10MB transaction size limit**: Brief cluster-wide availability impact possible with multi-MB transactions
- **100,000-byte value size limit** (exactly 100,000 bytes, not 100 KiB/102,400): Requires chunking/blob pattern for larger values
- **~100K reads/sec per key**: Triple replication limit - hot keys MUST be manually sharded
- **Optimal concurrency**: 200+ concurrent transactions for maximum throughput (Little's Law: throughput = concurrency / latency)

---

## 7. Observability and Instrumentation

### Metrics (Prometheus)

- Command duration, count, errors (per command)
- Transaction duration, conflicts, retries
- Connection count, pool utilization
- TTL expiry rate, backlog
- FDB-specific metrics

### Tracing (OpenTelemetry)

- End-to-end request tracing
- FDB transaction spans
- Command execution spans
- Distributed context propagation

### Profiling (Tracy)

- CPU flamegraphs
- Memory allocation tracking
- Frame-time analysis
- Integration with Tracy profiler GUI

---

## 8. Testing Strategy (Day One Priority)

### Testing is NOT an Afterthought

Every component is built with comprehensive testing from the start:

### 1. Unit Testing

- Every function tested
- Mock FDB for isolated testing
- 100% coverage target for critical paths

### 2. Property-Based Testing (proptest)

**String Properties**:
- `GET(SET(k, v)) = v`
- `DEL(k); APPEND(k, a); APPEND(k, b); GET(k) = a||b` (concatenation on empty key)
- `Given GET(k) = v: APPEND(k, a); GET(k) = v||a` (append preserves existing value)
- `INCR commutativity and associativity`

**Sorted Set Properties**:
- `ZRANGE is sorted by score`
- `ZRANK + ZREVRANK = ZCARD - 1`
- `ZADD idempotence`

**Transaction Properties**:
- `MULTI/EXEC atomicity`
- `WATCH optimistic locking`

### 3. Fuzzing (cargo-fuzz)

- **Protocol Fuzzing**: RESP3 parser with arbitrary inputs
- **Command Fuzzing**: Structured command generation
- **Continuous Fuzzing**: 24h campaigns, corpus management

### 4. Integration Testing

- **Redis TCL Test Suite**: Official Redis tests (target 90%+ Phase 2, 95%+ production readiness)
- **Client Library Tests**: redis-py, go-redis, node-redis
- **Compatibility Matrix**: Test all supported commands

### 5. Performance Testing (Criterion)

- Latency percentiles (p50, p99, p99.9, p99.99)
- Throughput under load
- Concurrent client stress tests
- Comparison against Redis baseline

### 6. Chaos Engineering

- Network partitions (toxiproxy)
- FDB failure injection (buggify mode)
- Long-running stability tests (72h+)
- Concurrent stress with random failures

### 7. Linearizability Testing

- Property-based concurrent testing
- Porcupine for linearizability verification
- Detect race conditions and atomicity violations

### Testing Phases

**Phase 1 (Weeks 1-2)**:
- Unit test framework setup
- Property test infrastructure
- Parser fuzzing

**Phase 2 (Weeks 3-8)**:
- Property tests for all data structures
- Redis TCL suite integration
- Baseline performance benchmarks

**Phase 3+ (Ongoing)**:
- Chaos engineering
- Extended compatibility testing
- Performance regression detection
- Security fuzzing

---

## 9. Technology Stack

### Core Dependencies

| Component | Crate | Purpose |
|-----------|-------|---------|
| Async Runtime | `tokio` | Multi-threaded async I/O |
| Database | `foundationdb` | FDB Rust bindings |
| Networking | `bytes` | Zero-copy buffers |
| Metrics | `prometheus` | Metrics collection |
| Tracing | `tracing` + `opentelemetry` | Observability |
| Profiling | `tracy-client` | Performance profiling |
| Errors | `thiserror` + `anyhow` | Error handling |

### Testing Dependencies

| Component | Crate | Purpose |
|-----------|-------|---------|
| Property Testing | `proptest` | Property-based tests |
| Fuzzing | `cargo-fuzz` | Coverage-guided fuzzing |
| Benchmarking | `criterion` | Statistical benchmarks |
| Mocking | `mockall` | Mock FDB for unit tests |

---

## 10. Implementation Phases

### Phase 1: Foundation & Testing Infrastructure

**Deliverables**:
- Project structure, build system, CI/CD
- **Testing infrastructure first**: Unit test framework, property test harness, fuzzing setup
- RESP3 parser (full spec, zero-copy) with RESP2 backward compatibility
- HELLO command for protocol negotiation
- FDB transaction wrapper with retry logic and idempotency handling
- Metrics infrastructure (Prometheus)
- Basic tracing setup (OpenTelemetry)

**Testing**: 100% unit coverage for parser, fuzzing running continuously, property test framework validated

**Critical Success Factor**: Testing infrastructure MUST be ready before command implementation begins. This prevents technical debt and ensures quality from day one.

### Phase 2: MVP Commands

**Deliverables**: 70 core commands across all data structures

**Testing**:
- Property tests for all structures
- Redis TCL suite (90%+ target)
- redis-py compatibility
- Performance baseline

### Phase 3: Advanced Commands

**Deliverables**: Blocking ops, SCAN, bitmaps, HyperLogLog, geospatial, basic streams

**Testing**:
- Chaos engineering
- Extended compatibility
- Performance regression tests

### Phase 4-7: Streams, Pub/Sub, Scripting, Production Hardening

See detailed breakdown in research documents.

---

## 11. Operational Considerations

### Configuration

Key configuration areas:
- Server: listen address, max connections, timeouts
- FDB: cluster file, transaction limits, retry policy
- Performance: worker threads, FDB client threads
- TTL: expiry batch size, cleanup interval
- Observability: metrics port, tracing endpoint
- Security: TLS, authentication, authorization

### Monitoring

**Critical Metrics**:
- Latency (p50, p99, p99.9, p99.99) per command
- Throughput (ops/sec)
- Error rates and types
- Transaction conflicts and retries
- Connection pool utilization
- FDB health metrics
- TTL expiry backlog

**Alerts**:
- p99 latency > 50ms
- Error rate > 1%
- Conflict rate > 10%
- Connection pool exhaustion
- FDB transaction timeouts

### Capacity Planning

**Single Node** (estimate):
- 100K ops/sec mixed workload
- 10K concurrent connections
- Limited by FDB network thread (consider multi-process deployment)

**Scaling**:
- Horizontal: Multiple kvdb instances on same FDB cluster
- Vertical: Scale FDB cluster
- Hot keys: Application-level sharding

### Security

- Authentication required by default
- Per-namespace authorization
- TLS for client connections
- FDB network encryption
- Audit logging for authentication events

---

## 12. Success Criteria

### Correctness

- [ ] 90%+ pass rate on Redis TCL test suite for Phase 2; 95%+ for production readiness
- [ ] Compatible with major Redis clients
- [ ] Zero data loss under chaos testing
- [ ] No crashes after 24h fuzzing
- [ ] All property tests pass with 10K+ cases

### Performance

- [ ] p50 latency < 1ms for simple GET commands (FDB read latency)
- [ ] p99 latency < 5ms for simple GET commands
- [ ] p50 latency < 3ms for simple SET commands (FDB read + commit latency)
- [ ] p99 latency < 15ms for simple SET commands (FDB p50 commit is 1.5-2.5ms; p99 commit can be 5-15ms under load)
- [ ] p99 latency < 50ms for complex commands (multi-key operations, range queries)
- [ ] Throughput > 50-100K ops/sec per process (single FDB network thread bottleneck)
- [ ] Linear horizontal scaling to 1M+ ops/sec across multiple processes on FDB cluster
- [ ] < 5% transaction conflict rate under load
- [ ] Hot key reads < 100K/sec per key (before manual sharding required)

### Observability

- [ ] All commands instrumented
- [ ] Distributed tracing end-to-end
- [ ] Dashboards for key metrics
- [ ] Alerting on critical conditions
- [ ] Profiling integrated

### Production Readiness

- [ ] Comprehensive documentation
- [ ] Operations runbook
- [ ] Security audit complete
- [ ] Load testing complete (1M ops/sec, 72h stability)
- [ ] Disaster recovery tested
- [ ] TLS support implemented

---

## Next Steps

1. **Project Setup**: Initialize Rust project structure
2. **RESP3 Implementation**: Start with protocol parser (with tests!)
3. **FDB Transaction Layer**: Build robust transaction wrapper
4. **First Commands**: Implement MVP string commands
5. **Test, Test, Test**: Establish testing discipline from day one

---

## Key Takeaways

- **RESP3 is required with RESP2 backward compatibility** - modern protocol with broad client support
- **Testing is not an afterthought** - built in from day one, infrastructure before implementation
- **Quality over speed** - production-grade for 10+ year lifespan
- **Leverage FDB strengths** - strict serializability (stronger than Redis!), ordered keys, atomic ops
- **Understand FDB constraints** - 5-second and 10MB hard limits, ~100K reads/sec per key, manual hot key sharding
- **Emulate Redis semantics faithfully** - no-rollback MULTI/EXEC, explicit WATCH-only conflicts, binary-safe keys
- **Comprehensive observability** - metrics, traces, profiling integrated throughout
- **Phased approach** - Foundation first, then MVP, then advanced features, then production hardening

This design provides a roadmap for building a Redis-compatible database that is correct, performant, observable, and production-ready.
