# Redis Implementation Analysis for FoundationDB Backend

## Executive Summary

This document provides a comprehensive analysis of Redis protocols, commands, and features to guide implementation of a Redis-compatible layer atop FoundationDB. Focus is on command completeness, protocol correctness, and implementation complexity estimation.

---

## 1. Wire Protocol (RESP)

### RESP2 vs RESP3

**RESP2** (Redis 2.0+, current default):
- Simple types: String (+), Error (-), Integer (:), Bulk String ($), Array (*)
- Dual null representation: $-1\r\n (bulk) or *-1\r\n (array)
- Booleans represented as integers (1/0)
- No native floating-point, maps, or sets

**RESP3** (Redis 7.0+):
- All RESP2 types plus: Null (_), Boolean (#), Double (,), Big Number ((), Bulk Error (!), Verbatim String (=), Map (%), Set (~), Attributes (|), Push (>)
- Unified null representation
- Better semantic type mapping for modern clients
- Backward compatible: servers support both protocols

**Key Compatibility Requirement**: Must support RESP3 for modern clients and superior type semantics. RESP2 backward compatibility required for legacy client support.

### Protocol Mechanics

**Command Format**: Clients send arrays of bulk strings
```
*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n
```

**Response Types**: Command-specific (strings, integers, arrays, errors)

**Pipelining**: 
- Multiple commands sent without waiting for replies
- Server queues and processes sequentially
- All responses returned in order
- Performance benefit: eliminates RTT overhead
- NOT atomic (use MULTI/EXEC for atomicity)

**Error Format**: `-ERRORPREFIX message\r\n`
- Common prefixes: ERR, WRONGTYPE, NOPROTO, SYNTAX, MOVED, ASK, CLUSTERDOWN
- MOVED/ASK: Cluster redirection (even if not implementing cluster, should document)
- Clients should raise exceptions or handle redirections

**HELLO Command** (Protocol Negotiation):
```
HELLO [protover [AUTH username password] [SETNAME clientname]]
```
- Establishes protocol version (2 or 3)
- Optionally authenticates and sets client name in a single round-trip
- Returns server metadata map (server, version, proto, id, mode, role, modules)
- If protover is unsupported, returns NOPROTO error and connection stays in RESP2
- HELLO with no arguments returns server info without changing protocol

**Implementation Priority**: 
- RESP3: MUST implement (all types including RESP2 for backward compatibility)
- Pipelining: MUST implement (critical for performance)
- HELLO: MUST implement (enables protocol negotiation and version detection)
- Automatic RESP2/RESP3 detection: MUST implement

---

## 2. Command Set Analysis

### Command Categories & Implementation Complexity

#### 2.1 String Commands (26 commands)

**Easy to Implement** (O(1) operations):
- GET, SET, MGET, MSET, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT
- GETDEL, GETEX, SETNX, SETEX, PSETEX
- Atomicity: Single-key ops are naturally atomic; MGET/MSET require transaction in FDB

**Moderate Complexity**:
- GETBIT, SETBIT, BITCOUNT, BITPOS (requires bitfield manipulation)
- GETRANGE, SETRANGE (substring operations)

**Complex**:
- BITFIELD, BITFIELD_RO (arbitrary integer fields with overflow policies)
- BITOP (AND/OR/XOR/NOT across multiple keys - multi-key atomicity)

#### 2.2 Hash Commands (~15 core + ~15 field-expiry = ~30 total)

**Easy**: HGET, HSET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL, HMGET, HMSET, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD

**Moderate**:
- HSCAN (cursor-based iteration - requires stable iteration semantics)
- HGETDEL (atomic get+delete)

**Complex** (Redis 7.4+ field expiration - BLEEDING EDGE):
- HEXPIRE, HPEXPIRE, HEXPIREAT, HPEXPIREAT, HPERSIST, HTTL, HPTTL, HEXPIRETIME, HPEXPIRETIME
- **WARNING**: These are VERY new commands (Redis 7.4+, released 2024) and may not be stable
- Requires per-field TTL tracking (complex FDB implementation)
- **RECOMMENDATION**: Defer until after core hash commands are stable. Not widely used yet.

#### 2.3 List Commands (25 commands)

**Easy**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LTRIM, LREM, LPUSHX, RPUSHX, LINSERT

**Moderate**:
- LMOVE, LMPOP (atomic cross-list operations)
- LPOS (search with COUNT and RANK options)

**Complex** (blocking operations):
- BLPOP, BRPOP, BLMOVE, BLMPOP
- Requires client blocking/notification infrastructure
- Must handle timeouts and wake-on-data

#### 2.4 Set Commands (17 commands)

**Easy**: SADD, SREM, SISMEMBER, SMISMEMBER, SCARD, SMEMBERS, SPOP, SRANDMEMBER, SMOVE

**Moderate**:
- SSCAN (iteration)
- SINTER, SUNION, SDIFF (multi-key set operations)
- SINTERSTORE, SUNIONSTORE, SDIFFSTORE (result storage)
- SINTERCARD (cardinality without full materialization)

#### 2.5 Sorted Set Commands (44 commands)

**Complexity Source**: Dual ordering (score + lexicographic), range queries, conditional updates

**Easy**: ZADD (basic), ZCARD, ZSCORE, ZCOUNT, ZINCRBY, ZREM

**Moderate**:
- ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANGEBYLEX (range queries)
- ZRANK, ZREVRANK (rank lookups)
- ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX
- ZMPOP, ZPOPMIN, ZPOPMAX

**Complex**:
- ZADD with NX/XX/GT/LT/CH/INCR options (conditional logic)
- BZPOPMIN, BZPOPMAX, BZMPOP (blocking)
- ZINTER, ZUNION, ZDIFF with WEIGHTS and AGGREGATE (multi-set operations)
- ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE, ZRANGESTORE
- ZSCAN, ZMSCORE, ZRANDMEMBER

**Implementation Notes**:
- Requires skip list or equivalent (O(log N) operations)
- IEEE 754 double scores with lexicographic tie-breaking
- FDB's ordered keys suitable for score storage; need secondary index for member lookups

#### 2.6 Stream Commands (26 commands)

**Critical for Modern Redis**: Event streaming, consumer groups

**Easy**:
- XADD, XLEN, XRANGE, XREVRANGE, XDEL

**Moderate**:
- XTRIM (with MAXLEN/MINID strategies)
- XREAD (blocking capable)
- XINFO STREAM, XINFO GROUPS, XINFO CONSUMERS

**Complex** (Consumer Groups):
- XGROUP CREATE, XGROUP DESTROY, XGROUP SETID, XGROUP CREATECONSUMER, XGROUP DELCONSUMER
- XREADGROUP (blocking with PEL tracking)
- XACK (acknowledgment)
- XPENDING (PEL inspection)
- XCLAIM, XAUTOCLAIM (message ownership transfer)

**Implementation Notes**:
- Entry IDs: timestamp-sequence format (ms-seq)
- Must guarantee monotonically increasing IDs
- PEL (Pending Entries List) per consumer group
- Atomicity: XADD with trimming is atomic
- Blocking XREAD/XREADGROUP requires notification infrastructure

#### 2.7 Key Management Commands (29 commands)

**Easy**: EXISTS, DEL, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST, EXPIRETIME, PEXPIRETIME, TOUCH, UNLINK

**Moderate**:
- KEYS (pattern matching - can be slow)
- SCAN (cursor iteration - requires stable semantics)
- RENAME, RENAMENX (atomic key renaming)
- COPY (Redis 6.2+)
- DUMP, RESTORE (serialization)
- SORT, SORT_RO (complex sorting with BY, GET, STORE)

**Complex**:
- MIGRATE (atomic transfer between instances - cluster feature)
- RANDOMKEY (requires random sampling)

**Expiration Semantics**:
- Passive expiration: checked on key access
- Active expiration: background process (10x/sec default)
- Expired keys excluded from RDB snapshots
- FDB implementation: use TTL layer or custom expiration tracking

#### 2.8 Transaction Commands (5 commands)

**MULTI, EXEC, DISCARD, WATCH, UNWATCH**

**Redis Semantics**:
- Commands between MULTI and EXEC are queued (not executed)
- EXEC executes all commands atomically and sequentially *within a single Redis instance*
- WATCH implements optimistic locking: EXEC returns nil if watched keys modified by other clients
- **NO ROLLBACK**: Execution errors don't undo prior commands in transaction (partial execution possible)
- Syntax errors during queuing abort entire transaction
- **CLUSTER MODE LIMITATION**: Multi-key transactions require all keys in same hash slot (no cross-slot atomicity)

**Implementation with FDB - SIGNIFICANT ADVANTAGE**:
- FDB provides **STRICT SERIALIZABILITY** - stronger than Redis!
- FDB transactions are truly atomic across entire cluster (not just single instance)
- **FDB provides automatic rollback** on errors - better than Redis!
- WATCH is implicit in FDB (automatic read conflict detection)
- Queue commands client-side, execute in single FDB transaction
- **Design Decision**: We can provide better consistency than Redis. Document this difference:
  - FDB implementation: True ACID, full rollback, cross-cluster atomicity
  - Redis: Best-effort atomicity, no rollback, single-instance or same-hash-slot only

**Complexity**: Moderate (leverages FDB transactional model, but semantics are better than Redis)

#### 2.9 Pub/Sub Commands (13 commands)

**PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE**
**SPUBLISH, SSUBSCRIBE, SUNSUBSCRIBE** (Sharded Pub/Sub, Redis 7.0+)
**PUBSUB CHANNELS, PUBSUB NUMSUB, PUBSUB NUMPAT, PUBSUB SHARDCHANNELS, PUBSUB SHARDNUMSUB**

**Semantics**:
- Fire-and-forget: no persistence, no replay
- Subscribers receive messages only if connected at publish time
- Pattern matching: glob-style (*, ?, [...])
- Subscribed clients enter "pub/sub mode" (limited command set)
- Message ordering guaranteed per-channel

**Sharded Pub/Sub**: Messages routed to cluster shard, better scalability

**Implementation Complexity**: High
- Requires in-memory subscription registry
- Client connection management for push notifications
- Pattern matching engine
- Not suitable for FDB storage (transient by design)

#### 2.10 Scripting Commands (15 commands)

**Lua**: EVAL, EVAL_RO, EVALSHA, EVALSHA_RO, SCRIPT DEBUG, SCRIPT EXISTS, SCRIPT FLUSH, SCRIPT KILL, SCRIPT LOAD

**Functions** (Redis 7.0+): FCALL, FCALL_RO, FUNCTION DELETE, FUNCTION DUMP, FUNCTION FLUSH, FUNCTION KILL, FUNCTION LIST, FUNCTION LOAD, FUNCTION RESTORE, FUNCTION STATS

**Semantics**:
- Scripts execute atomically
- Block all other clients during execution
- EVAL sends full script; EVALSHA uses cached script by SHA1
- KEYS array (for cluster routing) + ARGV array
- redis.call() propagates errors; redis.pcall() returns errors as tables

**Implementation Complexity**: Very High
- Requires embedded Lua interpreter
- Sandboxing (limited command set, no blocking ops)
- Script caching and SHA1 tracking
- Atomicity enforcement
- Replication of script effects

**Recommendation**: Defer scripting for MVP; critical for full compatibility

#### 2.11 Geospatial Commands (9 commands)

**GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH, GEOSEARCHSTORE**
**GEORADIUS, GEORADIUS_RO, GEORADIUSBYMEMBER, GEORADIUSBYMEMBER_RO** (deprecated)

**Implementation**: Internally stored as sorted sets with geohash scores

**Complexity**: Moderate (implement atop sorted sets + geohashing library)

#### 2.12 HyperLogLog Commands (5 commands)

**PFADD, PFCOUNT, PFMERGE, PFDEBUG, PFSELFTEST**

**Purpose**: Probabilistic cardinality estimation (unique element counting)

**Complexity**: Moderate (requires HyperLogLog algorithm implementation)

#### 2.13 Bitmap Commands (5 commands)

**BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO**

**Implementation**: Operate on string values

**Complexity**: Moderate (bitwise operations with offset handling)

#### 2.14 Cluster Commands (40 commands)

**CLUSTER ADDSLOTS, CLUSTER NODES, CLUSTER SLOTS, CLUSTER INFO, ASKING, READONLY, READWRITE,** etc.

**Purpose**: Cluster topology management, slot allocation, failover

**Complexity**: Very High
- Hash slot routing (16,384 slots)
- MOVED/ASK redirections
- Multi-key operations require same hash slot
- Gossip protocol for cluster state
- Manual slot migration

**Recommendation**: Defer for MVP; FDB provides distributed consistency natively

#### 2.15 Server Management Commands (60+ commands)

**INFO, CONFIG GET/SET, SAVE, BGSAVE, SHUTDOWN, CLIENT LIST, MEMORY STATS, SLOWLOG,** etc.

**Complexity**: Varies
- Easy: INFO, PING, TIME, ECHO, DBSIZE
- Moderate: CONFIG GET/SET, CLIENT commands
- Complex: Persistence (SAVE, BGSAVE, AOF), replication (REPLCONF, SYNC)

**Recommendation**: Implement INFO, CONFIG GET/SET, basic CLIENT commands; defer persistence/replication (FDB handles durability)

#### 2.16 ACL Commands (12 commands)

**ACL SETUSER, ACL GETUSER, ACL LIST, ACL CAT, ACL WHOAMI,** etc.

**Purpose**: User authentication and authorization

**Complexity**: Moderate (permission enforcement per command)

**Recommendation**: Implement basic AUTH; defer fine-grained ACL for MVP

#### 2.17 Connection Commands (24 commands)

**AUTH, SELECT, PING, ECHO, QUIT, HELLO, CLIENT SETNAME, CLIENT GETNAME,** etc.

**Easy**: PING, ECHO, QUIT, SELECT, AUTH, CLIENT SETNAME/GETNAME

**Moderate**: CLIENT KILL, CLIENT PAUSE, CLIENT TRACKING (client-side caching)

#### 2.18 Module Commands (Bloom, Cuckoo, JSON, Search, TimeSeries, etc.)

**Status**: Redis Stack extensions, not core Redis

**Recommendation**: Defer; focus on core Redis commands

---

## 3. Data Structures

### 3.1 Strings
- Binary-safe byte sequences (up to 512 MB)
- Encoding: Raw, Int (64-bit signed), or Embstr (short strings)
- Use cases: Caching, counters, bitmaps

### 3.2 Lists
- Ordered collections, insertion-order preserved
- Implementation: Linked list (Redis <7.0) or quicklist (7.0+)
- O(1) head/tail operations, O(N) index access
- Use cases: Queues, stacks, timelines

### 3.3 Sets
- Unordered collections of unique strings
- Implementation: Hash table or intset (small integer sets)
- O(1) add, remove, membership test
- Use cases: Tagging, deduplication, membership

### 3.4 Hashes
- Field-value pairs (like objects/dictionaries)
- Implementation: Hash table or ziplist (small hashes)
- O(1) field access
- Use cases: Object storage, user profiles

### 3.5 Sorted Sets
- Unique strings ordered by score (double precision float)
- Secondary sort: lexicographic on ties
- Implementation: Skip list + hash table
- O(log N) add/remove/rank, O(log N + M) range queries
- Use cases: Leaderboards, priority queues, time-series

**FDB Implementation Strategy**:
- Primary index: (key, score, member) for range queries
- Secondary index: (key, member) -> score for O(1) lookup
- Lexicographic ordering: use binary comparison on member

### 3.6 Streams
- Append-only logs with entry IDs
- Entry ID format: `timestamp-sequence`
- Consumer groups for partitioned consumption
- Use cases: Event sourcing, message queues, logs

**FDB Implementation Strategy**:
- Entries: (stream_key, entry_id) -> fields
- Consumer groups: separate metadata keyspace
- PEL: (stream_key, group, consumer, entry_id) -> metadata

### 3.7 Bitmaps
- Bit-level operations on strings
- Not a separate type (use string commands)

### 3.8 HyperLogLog
- Probabilistic cardinality estimation
- 12 KB max per key (vs. actual set size)
- 0.81% standard error

### 3.9 Geospatial
- Internally stored as sorted sets with geohash scores
- Commands for radius/box queries

### 3.10 JSON (Module)
- Hierarchical JSON documents
- JSONPath query support

---

## 4. Critical Features

### 4.1 Transactions (MULTI/EXEC/WATCH)

**Guarantees**:
- Atomicity: All commands in EXEC execute sequentially without interruption
- Isolation: No other client commands interleave
- NO Rollback: Execution errors don't undo prior commands

**WATCH (Optimistic Locking)**:
- Monitors keys before transaction
- EXEC returns nil (not error) if watched keys modified by other clients
- Client must retry transaction

**FDB Mapping**:
- MULTI/EXEC: Single FDB transaction
- WATCH: FDB read conflict ranges
- Command queuing: Client-side buffering

### 4.2 Pub/Sub

**Delivery Semantics**: Fire-and-forget, no persistence
**Ordering**: Guaranteed per-channel
**Reliability**: Best-effort; disconnects lose messages

**Sharded Pub/Sub** (Redis 7.0+): Routes messages to cluster shards for better scalability

**Implementation Notes**:
- In-memory only (not FDB-backed)
- Requires connection state tracking
- Pattern matching: glob-style
- Subscription mode: limited command set

### 4.3 Key Expiration

**Mechanisms**:
- Passive: Checked on key access
- Active: Background process (10x/sec)

**Policies**:
- Expiration time set with EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT
- TTL removed with PERSIST
- Expired keys excluded from snapshots

**FDB Implementation**:
- Option 1: TTL layer (store expiration timestamp with value)
- Option 2: Background scanner with FDB watch triggers
- Option 3: Hybrid (passive check + periodic cleanup)

### 4.4 Eviction Policies

**maxmemory Policies**:
- noeviction: Reject writes when limit reached
- allkeys-lru: Evict least recently used (any key)
- volatile-lru: Evict LRU with TTL set
- allkeys-lfu: Evict least frequently used
- volatile-lfu: Evict LFU with TTL
- allkeys-lrm: Evict least recently modified (Redis 8.6+ — verify against actual release before implementing)
- volatile-lrm: Evict LRM with TTL (Redis 8.6+ — verify against actual release before implementing)
- allkeys-random, volatile-random: Random eviction
- volatile-ttl: Evict shortest TTL first

**Implementation**: Sampling-based approximation (not exact LRU)

**FDB Consideration**: Memory limits and eviction are application-level concerns (FDB provides durable storage, not cache semantics)

### 4.5 Persistence (RDB & AOF)

**RDB (Snapshots)**:
- Binary point-in-time snapshots
- Triggered by SAVE/BGSAVE or schedule
- Data loss window: up to snapshot interval

**AOF (Append-Only File)**:
- Write-ahead log of commands
- Fsync policies: always (slow), everysec (default), no (fast)
- Data loss: 0 (always) to ~30s (no)

**Redis 7.0+**: Multi-part AOF (base + incremental + manifest)

**FDB Consideration**: FDB provides ACID durability natively; Redis persistence semantics not needed for durability but may affect command latency expectations

### 4.6 Replication

**Semantics**: Asynchronous master-replica replication
**Commands**: REPLICAOF, REPLCONF, SYNC, PSYNC
**Consistency**: Eventually consistent (not strong consistency)

**FDB Consideration**: FDB handles replication; Redis replication protocol not needed unless building Redis Cluster compatibility

### 4.7 Cluster Mode

**Sharding**: 16,384 hash slots, CRC16(key) % 16384
**Hash Tags**: {tag} syntax forces keys to same slot for multi-key ops
**Redirections**: MOVED (permanent), ASK (temporary during migration)
**Multi-key Constraints**: All keys must be in same slot

**FDB Consideration**: FDB is globally consistent; hash slot partitioning unnecessary unless full Redis Cluster protocol required

### 4.8 Lua Scripting

**Atomicity**: Scripts execute atomically, block other clients
**API**: redis.call() (strict), redis.pcall() (error-tolerant)
**Caching**: EVALSHA uses SHA1 of script
**Replication**: Script effects replicated (not script itself in some modes)

**Complexity**: Very high (embedded Lua, sandboxing, atomicity)

**Recommendation**: Defer for MVP

### 4.9 Modules API

**Purpose**: Extend Redis with custom commands and data types
**Language**: C (bindings available for others)
**Capabilities**: Command registration, data type creation, memory management, replication hooks

**FDB Consideration**: Not critical for initial implementation; focus on core commands

---

## 5. Performance & Scalability

### 5.1 Pipelining

**Benefit**: Reduces RTT overhead by batching commands
**Performance**: 10-100x throughput improvement for bulk operations
**Implementation**: MUST support; critical for real-world performance

### 5.2 Command Complexity

**O(1) Commands**: GET, SET, HGET, HSET, SADD, ZADD (single element)
**O(log N) Commands**: ZADD, ZREM, ZRANK (sorted sets)
**O(N) Commands**: KEYS, SMEMBERS, HGETALL, ZRANGE (full scan)
**O(N log N)**: SORT

**FDB Mapping**:
- O(1): Single FDB read/write
- O(log N): FDB range read with limit
- O(N): FDB range scan

### 5.3 Blocking Operations

**Commands**: BLPOP, BRPOP, BLMOVE, BZPOPMIN, BZPOPMAX, XREAD, XREADGROUP
**Semantics**: Block until data available or timeout
**Complexity**: Requires client notification infrastructure (event loop, condition variables)

### 5.4 Scan-Based Iteration

**Commands**: SCAN, HSCAN, SSCAN, ZSCAN
**Purpose**: Cursor-based iteration without blocking server

**Requirements**: Best-effort iteration — guarantees all elements present throughout the full scan are returned at least once, but may return duplicates under concurrent modifications. This matches Redis SCAN semantics (Redis docs: "elements may be returned multiple times").

**FDB Implementation**: Range reads with continuation tokens

### 5.5 Benchmarking

**Redis Benchmark Tool**: redis-benchmark
**Metrics**: Throughput (ops/sec), latency (p50, p99, p999)
**Typical Redis**: 100K+ ops/sec on single instance

**FDB Considerations**:
- Network latency: FDB cluster adds ~1-5ms vs. in-memory Redis
- Throughput: FDB handles 10M+ ops/sec across cluster
- Pipelining critical to amortize latency

---

## 6. Implementation Roadmap

### Phase 1: MVP (Core Commands)
**Goal**: Basic Redis compatibility for common use cases

**Commands** (~70 total):
- Strings: GET, SET, MGET, MSET, INCR, DECR, DEL, EXISTS, EXPIRE, TTL, APPEND, STRLEN, INCRBYFLOAT, SETEX, PSETEX, GETDEL
- Hashes: HGET, HSET, HDEL, HEXISTS, HGETALL, HINCRBY, HLEN, HKEYS, HVALS, HMGET, HMSET, HSETNX, HINCRBYFLOAT
- Lists: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM, LREM
- Sets: SADD, SREM, SISMEMBER, SMEMBERS, SINTER, SUNION, SDIFF, SCARD, SPOP, SRANDMEMBER
- Sorted Sets: ZADD, ZREM, ZRANGE, ZRANK, ZSCORE, ZCARD, ZINCRBY, ZCOUNT, ZREVRANGE, ZREVRANK
- Keys: DEL, EXISTS, EXPIRE, TTL, TYPE, SCAN, KEYS, PERSIST, EXPIRETIME, PEXPIRETIME
- Transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH
- Server: PING, INFO, SELECT, DBSIZE, ECHO, TIME, HELLO

**Protocol**: RESP3 with RESP2 backward compatibility and pipelining (MUST have)

### Phase 2: Advanced Commands
**Commands**:
- Blocking: BLPOP, BRPOP
- Sorted Set operations: ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE
- Scan operations: HSCAN, SSCAN, ZSCAN
- Bitmap: GETBIT, SETBIT, BITCOUNT, BITPOS
- HyperLogLog: PFADD, PFCOUNT, PFMERGE
- Geospatial: GEOADD, GEORADIUS, GEOSEARCH
- Streams (basic): XADD, XRANGE, XREAD, XLEN

### Phase 3: Streams & Consumer Groups
**Commands**: Full stream support including XREADGROUP, XACK, XPENDING, XGROUP commands

### Phase 4: Pub/Sub
**Commands**: PUBLISH, SUBSCRIBE, PSUBSCRIBE, PUBSUB commands

**Infrastructure**: In-memory subscription registry, client notification

### Phase 5: Scripting
**Commands**: EVAL, EVALSHA, SCRIPT commands

**Infrastructure**: Embedded Lua, sandboxing, script cache

### Phase 6: Cluster Mode (Optional)
**Commands**: CLUSTER commands, ASKING, READONLY

**Infrastructure**: Hash slot routing, redirections, topology management

### Phase 7: Advanced Features (Optional)
- Modules API
- Fine-grained ACL
- Client-side caching (CLIENT TRACKING)
- RESP3 full support
- JSON, Search, TimeSeries modules

---

## 7. Command Categorization by Complexity

### Easy (Straightforward FDB Mapping)
**Strings**: GET, SET, MGET, MSET, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, GETDEL, GETEX
**Hashes**: HGET, HSET, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HGETALL, HMGET, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN
**Lists**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LTRIM, LREM, LPUSHX, RPUSHX
**Sets**: SADD, SREM, SISMEMBER, SMISMEMBER, SCARD, SMEMBERS, SPOP, SRANDMEMBER
**Sorted Sets**: ZADD (basic), ZREM, ZSCORE, ZCARD, ZINCRBY
**Keys**: EXISTS, DEL, TYPE, EXPIRE, TTL, PERSIST, TOUCH, UNLINK
**Transactions**: MULTI, EXEC, DISCARD
**Server**: PING, ECHO, DBSIZE, TIME

**Total**: ~70 commands

### Moderate (Some Complexity)
**Strings**: GETBIT, SETBIT, BITCOUNT, BITPOS, GETRANGE, SETRANGE
**Hashes**: HSCAN, HRANDFIELD, HGETDEL
**Lists**: LMOVE, LMPOP, LPOS, LINSERT
**Sets**: SSCAN, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SINTERCARD, SMOVE
**Sorted Sets**: ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZRANGEBYLEX, ZRANK, ZREVRANK, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX, ZMPOP, ZPOPMIN, ZPOPMAX, ZSCAN, ZCOUNT, ZLEXCOUNT
**Keys**: KEYS, SCAN, RENAME, RENAMENX, COPY, DUMP, RESTORE, RANDOMKEY
**Transactions**: WATCH, UNWATCH
**Geospatial**: GEOADD, GEODIST, GEOHASH, GEOPOS, GEOSEARCH
**HyperLogLog**: PFADD, PFCOUNT, PFMERGE
**Bitmap**: BITOP
**Server**: INFO, CONFIG GET/SET, CLIENT LIST/SETNAME/GETNAME

**Total**: ~60 commands

### Complex (Significant Implementation Effort)
**Strings**: BITFIELD, BITFIELD_RO
**Sorted Sets**: ZADD (NX/XX/GT/LT/CH/INCR), ZINTER, ZUNION, ZDIFF, ZINTERSTORE, ZUNIONSTORE, ZDIFFSTORE, ZRANGESTORE, ZMSCORE, ZRANDMEMBER
**Lists**: BLPOP, BRPOP, BLMOVE, BLMPOP (blocking)
**Sorted Sets**: BZPOPMIN, BZPOPMAX, BZMPOP (blocking)
**Streams**: XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XDEL, XINFO STREAM
**Keys**: SORT, SORT_RO, MIGRATE

**Total**: ~30 commands

### Very Complex (Major Features)
**Streams**: XGROUP commands, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO GROUPS/CONSUMERS
**Pub/Sub**: PUBLISH, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PUBSUB commands
**Scripting**: EVAL, EVALSHA, SCRIPT commands, FCALL, FUNCTION commands
**Cluster**: All CLUSTER commands, ASKING, READONLY, READWRITE
**Server**: Persistence (SAVE, BGSAVE, BGREWRITEAOF), Replication (REPLICAOF, SYNC)
**ACL**: Full ACL system
**Modules**: Modules API

**Total**: ~80 commands

---

## 8. Key Recommendations

### Must Implement (MVP)
1. **RESP3 protocol with RESP2 backward compatibility** and pipelining
2. **HELLO command** for protocol negotiation and version detection
3. Core string, hash, list, set, sorted set commands
4. Transactions (MULTI/EXEC/WATCH)
5. Key expiration (passive + active cleanup)
6. Basic server commands (PING, INFO, SELECT)
7. Scan-based iteration (SCAN, HSCAN, SSCAN, ZSCAN)

### Should Implement (High Value)
1. Blocking list operations (BLPOP, BRPOP)
2. Streams (basic XADD, XREAD, XRANGE)
3. Geospatial commands
4. HyperLogLog

### Consider Implementing (Nice to Have)
1. Pub/Sub (if real-time notifications needed)
2. Lua scripting (for complex atomicity requirements)
3. Streams with consumer groups (for message queue use cases)
4. BITFIELD operations
5. Advanced sorted set operations (ZINTER, ZUNION)

### Can Defer (Low Priority for FDB Backend)
1. Cluster mode (FDB provides global consistency)
2. Replication protocol (FDB handles replication)
3. Persistence commands (FDB is durable)
4. Modules API
5. Fine-grained ACL
6. Client-side caching protocol
7. Slow log and advanced debugging

### FDB-Specific Optimizations
1. Leverage FDB transactions for MULTI/EXEC
2. Use FDB watches for blocking operations
3. Map sorted sets to FDB's ordered keys
4. Implement TTL layer for expiration
5. Use FDB range reads for scan operations
6. Consider FDB directories for key namespacing

---

## 9. Compatibility Testing Strategy

### Test Suites
1. **Redis TCL Test Suite**: Official Redis test suite (~1000 tests)
2. **redis-py Tests**: Python client test suite
3. **redis-benchmark**: Performance baseline
4. **Custom Compatibility Tests**: Edge cases and FDB-specific scenarios

### Testing Approach
1. **Unit Tests**: Per-command correctness
2. **Integration Tests**: Multi-command workflows (transactions, pipelines)
3. **Compatibility Tests**: Against official Redis clients
4. **Performance Tests**: Latency and throughput benchmarks
5. **Concurrency Tests**: Multi-client race conditions
6. **Failure Tests**: Network partitions, crashes, recovery

### Success Criteria
- 90%+ pass rate on Redis TCL tests (excluding unsupported features)
- Compatible with redis-cli and major Redis clients
- Performance within 2-5x of in-memory Redis (accounting for FDB network latency)
- Correct atomicity and isolation guarantees

---

## 10. Estimated Total Effort

**MVP (Phase 1)**: Foundation + core commands (~70 commands)
**Advanced Commands (Phase 2)**: Extended command set (~60 commands)
**Streams & Consumer Groups (Phase 3)**: Full streaming support
**Pub/Sub (Phase 4)**: Message delivery infrastructure
**Scripting (Phase 5)**: Lua integration
**Cluster Mode (Phase 6)**: Optional Redis cluster protocol
**Advanced Features (Phase 7)**: Optional modules and extensions

**Phased Approach to Production-Ready System**:
- Comprehensive testing integrated at each phase, not bolted on afterward
- Foundation before features: testing infrastructure, observability, FDB patterns established early
- Iterative development with continuous quality verification
- Buffer for unexpected issues, performance tuning, and operational readiness

---

## Conclusion

Implementing a Redis-compatible layer atop FoundationDB is feasible with phased development. FDB's transactional semantics map well to Redis transactions, and ordered keys support sorted sets efficiently. Key challenges are blocking operations (requiring notification infrastructure), Pub/Sub (in-memory state), and Lua scripting (embedded interpreter). Deferring cluster mode and replication leverages FDB's native distributed consistency. The phased approach prioritizes foundation and testing infrastructure, followed by core commands, then advanced features, leading to a production-ready system with comprehensive quality assurance.
