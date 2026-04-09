# FoundationDB Deep Research: Implementation Guide for Redis-Compatible Layer

## Executive Summary

FoundationDB is a distributed ordered key-value store providing strict serializability with ACID transactions. It uses optimistic concurrency control, supports horizontal scaling to millions of ops/sec, and enforces hard limits (5-second transaction time, 10MB transaction size, 10KB keys, 100KB values). The minimal core is designed for extension through layers, making it an excellent foundation for building a Redis-compatible interface.

---

## 1. Architecture and Guarantees

### 1.1 ACID Transaction Model

**Core Guarantees:**
- **Atomicity**: All writes in a transaction succeed or none do
- **Consistency**: Database invariants maintained across concurrent modifications
- **Isolation**: Strict serializability - transactions appear to execute sequentially
- **Durability**: Committed writes survive failures
- **Causality**: Transactions see effects of all previously committed transactions

**Critical Insight**: FoundationDB provides the strongest possible consistency model - strict serializability. This is stronger than snapshot isolation and enables straightforward reasoning about concurrent operations.

### 1.2 Optimistic Concurrency Control

**How It Works:**
- Reads come from a database snapshot at a specific version
- Writes are cached locally during transaction execution
- At commit time, the system checks for conflicts rather than using locks
- Neither reads nor writes block other readers or writers

**Conflict Detection:**
A transaction is conflict-free if and only if no key it read was written between the transaction's read version and commit version. The system only checks read conflict sets - blind writes (writes without reads) never cause read conflicts but may cause future conflicts in other transactions.

**Version Management:**
- Each transaction receives a read version when it begins
- Master process generates commit versions monotonically
- Versions are assigned through proxies to avoid bottlenecks
- 5-second rolling buffer maintained in storage server memory

### 1.3 Transaction Constraints

**5-Second Time Limit:**
Transactions exceeding 5 seconds receive `transaction_too_old` error. Storage servers keep only 5 seconds of mutations in memory; older data requires disk reads. This is a fundamental architectural constraint, not a tunable parameter.

**Workarounds for Large Operations:**
1. **Decompose into multiple short transactions**: Break large operations into <1 second chunks
2. **Use indirection**: Write new data in separate transactions, then atomically switch a pointer/reference
   - Example: For large sorted set rebuild, write to `zset_new`, then atomically rename keys
   - **Critical**: The "switch" operation must be a single key write to be atomic
3. **For reads**: Use snapshot reads with pagination (limit parameter on range reads)
4. **For writes**: Batch into 1-second chunks with 50 concurrent transactions for throughput
5. **For large value updates**: Use blob pattern (split into 10KB chunks across multiple keys)

**10MB Size Limit:**
- Transaction cannot exceed 10,000,000 bytes of affected data
- Includes keys, values, ranges written, and ranges read
- **Transactions exceeding 1MB can cause performance issues**
- **CRITICAL WARNING**: Transactions >5MB can cause brief **cluster-wide** availability impact
  - Not just the client - affects ALL clients in the cluster
  - Can cause latency spikes for other transactions
  - Recommendation: Keep transactions well under 1MB, target 100-500KB for best performance
- For large operations (e.g., deleting thousands of keys), decompose into multiple transactions

### 1.4 Transaction Retry Strategies

**Standard Retry Loop Pattern:**
```python
while True:
    try:
        # Execute transaction logic
        tr.commit().wait()
        break
    except FDBError as e:
        tr.on_error(e.code).wait()  # Handles soft reset and backoff
```

The `on_error()` method:
- Determines if error is retryable
- Performs soft reset on retriable errors
- Applies exponential backoff
- Returns immediately for non-retriable errors

**Special Case - commit_unknown_result:**
Must handle separately by ensuring transactions are idempotent:
- Generate identifiers outside retry loops
- Check for completion markers before re-executing
- Design operations to produce identical results on replay

**Configuration Options:**
- `set_timeout(milliseconds)`: Auto-cancel after duration
- `set_retry_limit(count)`: Limit retry attempts (-1 for unlimited)
- `set_max_retry_delay(milliseconds)`: Cap backoff delay

---

## 2. Data Model

### 2.1 Ordered Key-Value Semantics

**Fundamental Model:**
- Keys and values are byte strings
- Keys treated as members of lexicographic order over underlying bytes
- Values require no interpretation by database
- Applications control serialization entirely

**Key Ordering Advantage:**
Enables efficient range operations for both transactional (OLTP) and analytical (OLAP) workloads. Adjacent keys in sort order are stored adjacently, minimizing seek operations.

### 2.2 Size Limits and Best Practices

**Hard Limits:**
- Keys: maximum 10,000 bytes (10 KB)
- Values: maximum 100,000 bytes (100 KB)

**Performance Guidelines:**
- Keys: aim below 1 KB; optimal under 32 bytes
- Values: target below 10 KB for best performance
- Structure keys to avoid single key updates exceeding 10-100/sec (causes conflicts)

**When to Split Values:**
If values exceed 1 KB but you use only partial content, split across multiple keys keyed by subfields to reduce unnecessary I/O.

### 2.3 Tuple Layer

**Purpose:** Encode composite data types into ordered keys while preserving sort order.

**Supported Elements:**
- Byte strings
- Unicode strings (UTF-8 encoded)
- Signed integers (variable-length encoding)
- Floating-point numbers (IEEE-based, order-preserving)
- Booleans, UUIDs, null values
- Nested tuples

**Key Benefit:** "The tuple layer preserves the ordering of tuples sorted by element from left to right," enabling prefix-based range reads on leftmost tuple elements.

**Example for Redis Keys:**
```
("redis", "string", user_key) -> string value
("redis", "hash", user_key, field) -> field value
("redis", "list", user_key, index) -> list element
("redis", "set", user_key, member) -> empty
("redis", "zset", user_key, score, member) -> empty
```

### 2.4 Directory Layer

**Purpose:** Logical groupings using prefixes, supporting hierarchical organization and nested structures (subspaces).

**Benefits:**
- Isolate different data types or applications
- Enable efficient prefix-based operations
- Simplify key management and migration
- Support nested namespaces

### 2.5 Versionstamps

**Structure:**
- 8 bytes: transaction commit version
- 2 bytes: transaction batch order  
- Optional 2 bytes: application-level ordering
- Total: 10 or 12 bytes

**Key Properties:**
- Generated at commit time by the database
- Monotonically increasing across the cluster
- **CRITICAL LIMITATION**: Cannot be read within the same transaction that writes them
  - Versionstamp is assigned during commit, not available earlier
  - This affects key design patterns significantly
  - For example: Can't use versionstamp as part of a key and then read that key in same transaction
  - **Workaround**: Use placeholder during write, retrieve versionstamp after commit, use in subsequent reads
- Enable conflict-free sequence generation without read conflicts
- Perfect for write-heavy workloads (no read conflicts on sequence generation)

**Use Cases:**
- Event sourcing with guaranteed ordering (append-only logs)
- Creating ordered logs or time-series data
- Implementing conflict-free counters and sequence generators
- Building change feeds (clients track last-seen versionstamp)
- Pub/sub message queues with total ordering
- **Anti-pattern**: Don't use if you need to read the versionstamp immediately after writing

**API Operations:**
- `set_versionstamped_key(key, param)`: Replaces 10 bytes at specified index
- `set_versionstamped_value(key, param)`: Embeds versionstamp in value
- `get_versionstamp()`: Returns versionstamp after commit

---

## 3. Performance Characteristics

### 3.1 Latency Expectations

**At 75% Load (milliseconds):**
- Read: 0.1 - 1 ms
- Transaction start: 0.3 - 1 ms
- Commit: 1.5 - 2.5 ms
- Set operations: Deferred until commit (no initial latency)

**Note:** Latencies increase as saturation approaches. System maintains low latencies even at moderate loads.

### 3.2 Throughput Capabilities

**Single Core (saturating workloads):**

| Workload | SSD Engine | Memory Engine |
|----------|-----------|---------------|
| Reads | 55,000/sec | 90,000/sec |
| Writes | 20,000/sec | 35,000/sec |

**Concurrency Requirements:**
Maximum throughput requires high concurrency. For typical 90/10 read-write ratios, performance peaks at approximately 200 concurrent operations. This follows Little's Law: throughput = outstanding requests / latency.

**Benchmark Results:**

*Single core with optimization:*
- 100 parallel clients with batched writes: 46,000 writes/sec
- 100 parallel clients reading: 305,000 reads/sec (0.6ms avg latency)
- 90/10 read-write mixed: 107,000 operations/sec
- Range reads (1,000-key scans): 3,600,000 keys/sec

*12-machine cluster (48 cores, 100M key-value pairs, 2x replication):*
- Random writes (3,200 clients): 720,000 writes/sec
- Random reads (3,200 clients): 5,540,000 reads/sec
- Mixed 90/10 workload: 2,390,000 operations/sec

*384-process cluster:*
- 90/10 read-write ratio with 16-byte keys: 8,200,000 operations/sec

### 3.3 Scaling Behavior

**Horizontal Scaling:**
- Linear scaling with number of cores
- Read operations scale by adding storage servers
- Write operations scale by adding commit proxies, resolvers, and log servers
- Tested up to 500 cores/processes; larger clusters may experience sub-linear scaling

**Vertical Scaling:**
- Scales from partial utilization of single core to full utilization of dozens of multicore machines
- Multi-version client supports multiple threads per process (CLIENT_THREADS_PER_VERSION option)

**Elasticity:**
- Immediate load balancing through replication
- Sub-millisecond response to hot spots
- Gradual redistribution (minutes) for major usage changes
- Automatic data movement between servers maintains balanced load

### 3.4 Key Performance Factors

**What Affects Performance:**
- Adjacent key writes execute much faster than scattered writes
- Larger keys/values reduce engine efficiency
- Caching substantially improves read performance
- Disk I/O requirements: slightly over 1 IOPS per write, 1 IOPS per uncached read
- OS-level network tuning critical for larger clusters

**Hot Key Handling:**
- Triple-replicated systems limited to ~100,000 reads/second per key
- No automatic replication increase for frequently accessed keys
- Workaround: Store hot data in multiple subspaces with client-side load balancing

### 3.5 Batch Operation Performance

**Best Practices:**
- Achieve high throughput through concurrency (thousands of concurrent transactions)
- Perform multiple reads in parallel within transactions
- Load data in small chunks (~10 KB per transaction)
- Use 50 concurrent transactions per process for optimal pipeline
- Keep transactions under one second latency for optimal throughput

**Atomic Operations for Conflict Avoidance:**
Use atomic ops (add, min, max, bitwise operations) for counters to avoid read-write conflicts.

---

## 4. API and Bindings

### 4.1 Transaction API Details

**Reading Data:**
- `get(key)`: Returns future value for specified key
- `get_key(key_selector)`: Resolves KeySelector to actual key
- `get_range(begin, end, limit, reverse, streaming_mode)`: Iterator for range
- `get_range_startswith(prefix)`: Convenience for prefix queries

**Writing Data:**
- `set(key, value)`: Associates key with value
- `clear(key)`: Removes key and value
- `clear_range(begin, end)`: Efficiently removes all keys in range
- `clear_range_startswith(prefix)`: Removes all keys with prefix

**Atomic Operations (conflict-free):**
- `add(key, param)`: Adds little-endian integers
- `bit_and(key, param)`: Bitwise AND
- `bit_or(key, param)`: Bitwise OR
- `bit_xor(key, param)`: Bitwise XOR
- `max(key, param)`: Sets to maximum of existing and new
- `min(key, param)`: Sets to minimum of existing and new
- `byte_max(key, param)`: Lexicographic maximum
- `byte_min(key, param)`: Lexicographic minimum
- `compare_and_clear(key, param)`: Clears only if matches provided value

### 4.2 Snapshot Reads

**Purpose:** Relax isolation to reduce conflicts for non-critical reads.

**Behavior:**
- Don't see concurrent modifications from other transactions
- Do reflect prior writes within same transaction
- Don't add to read conflict set
- Trade consistency for performance

**Available Methods:**
`snapshot.get()`, `snapshot.get_key()`, `snapshot.get_range()`, `snapshot.get_range_startswith()`

**Configuration:**
- `set_snapshot_ryw_enable()`: Snapshot reads see prior writes (default)
- `set_snapshot_ryw_disable()`: Snapshot reads don't see prior writes

### 4.3 Watches

**API:** `watch(key)` returns a future that becomes ready when key's value changes.

**Key Properties:**
- Watches persist after transaction commit
- Based on transaction's read version
- Must be cancelled via `Future.cancel()` to prevent leaks
- Useful for reactive/pub-sub patterns

**Use Cases:**
- Implementing Redis SUBSCRIBE/PSUBSCRIBE
- Building change notifications
- Coordinating distributed systems

### 4.4 Conflict Range Manipulation

**Manual Conflict Specification:**
- `add_read_conflict_range(begin, end)`: Mark range as read
- `add_read_conflict_key(key)`: Mark key as read
- `add_write_conflict_range(begin, end)`: Mark range as written
- `add_write_conflict_key(key)`: Mark key as written

**Use Cases:**
- Implementing custom locking semantics
- Fine-tuning conflict detection for complex operations
- Building coordination primitives

### 4.5 Version Management

**Setting Read Version:**
- `set_read_version(version)`: Specify database version to read from
- **Warning:** Disables causal consistency guarantees

**Getting Versions:**
- `get_read_version()`: Returns transaction's read point
- `get_committed_version()`: Returns version at which commit occurred
- `get_versionstamp()`: Returns versionstamp from versionstamp operations

### 4.6 Rust Binding Considerations

**Note:** Rust API documentation was not available (404), but based on Go patterns and general API concepts:

**Expected Patterns:**
- Async/await with futures for all database operations
- Result<T, Error> for error handling (similar to Go's explicit error returns)
- Transaction composition through references/borrows
- Explicit retry loops or retry decorators
- Type-safe tuple encoding/decoding

**Network Thread Constraint:**
Single network thread per process serializes all operations. For higher throughput:
- Spawn additional client processes, or
- Use multi-threaded client (CLIENT_THREADS_PER_VERSION option since 6.3)

**Error Handling:**
- Implement retry loops with `on_error()` method
- Handle `commit_unknown_result` specially with idempotency
- Use `cluster_version_changed` for multi-version client support

---

## 5. Advanced Features

### 5.1 Versionstamp Usage (detailed)

See section 2.5 for structure. Additional use cases:

**Event Sourcing:**
```
key: ("events", versionstamp, entity_id)
value: event_data
```
Enables chronologically ordered event retrieval without conflicts.

**Change Feeds:**
```
key: ("changes", versionstamp)
value: (entity_type, entity_id, operation)
```
Clients track last-seen versionstamp for incremental sync.

**Ordered Queues:**
```
key: ("queue", priority, versionstamp)
value: task_data
```
Conflict-free enqueue with total ordering.

### 5.2 Storage Engine Details

**Two Primary Engines:**

1. **SSD Engine (default):**
   - B-tree optimized for solid-state drives
   - Best for large datasets with reasonable cache-hit rates
   - Maintains 5 seconds of mutations in memory
   - On-disk copy as of 5 seconds ago
   - Uses SQLite-based B-tree storage

2. **Memory Engine:**
   - All data resident in memory at all times
   - Higher throughput (35,000 writes/sec vs 20,000 on SSD)
   - Default 1 GB per process limit (configurable)
   - Best for small datasets or high-performance requirements
   - Append-only logging for durability

**Alternative Engines:**
- `ssd-redwood-v1`: Improved B-tree variant for high-throughput
- `ssd-rocksdb-v1`: LSM-tree with compaction, good for compression

**Key Design Implication:**
The 5-second memory window creates the transaction time limit - clients must read within this window or receive `transaction_too_old` error.

### 5.3 Layers Architecture

**Philosophy:** FoundationDB intentionally maintains minimal core, exposing richer capabilities through layers.

**What Layers Can Provide:**
- New data models (SQL, document, graph)
- Encapsulated algorithms
- Entire frameworks
- Domain-specific abstractions

**For Redis Layer:**
The layer translates Redis commands to FDB operations, managing:
- Data type encoding in keys (using tuple layer)
- TTL implementation (separate index for expiration)
- Command semantics (atomicity, ordering)
- Pub/sub (using watches)

### 5.4 Backup and Disaster Recovery

**Continuous Backup Model:**
- Multiple distributed backup agents cooperate
- Creates inconsistent snapshot + mutation log
- Enables point-in-time recovery very close to "now"
- No downtime required during backup

**Point-in-Time Recovery:**
Restore to specific versions using commit versions or timestamps.

**Disaster Recovery Mode:**
Secondary database contains consistent copy of primary from past point in time, preserving ACID properties except durability for recent changes.

**Operational Patterns:**
- Distributed agents across machines
- Multiple tags for parallel backups
- Flexible destinations (filesystem, S3, secondary DB)
- Bidirectional replication support
- Incremental expiration with restorability guarantees

---

## 6. Key Design Patterns

### 6.1 Subspace Design

**Hierarchical Organization:**
Use tuple prefixes to create logical groupings:
```
("redis", namespace, data_type, user_key, ...)
```

**Benefits:**
- Isolate different data types
- Enable efficient range operations
- Simplify bulk operations (clear all keys in namespace)
- Support nested namespaces

### 6.2 Indexing Strategies

**Secondary Index Pattern:**
Maintain index entries within same transaction as primary data:

```python
@fdb.transactional
def set_with_index(tr, id, value, index_field):
    # Clear old index entry if exists
    old_value = tr.get(primary[id])
    if old_value:
        old_field = extract_field(old_value)
        del tr[index[old_field][id]]
    
    # Set new primary and index
    tr[primary[id]] = value
    tr[index[index_field][id]] = b''
```

**Consistency Guarantee:** ACID transactions ensure indexes and data remain synchronized - no stale index states possible.

**Query Pattern:**
Range query on index subspace returns all IDs matching indexed property via single range-read operation.

**Trade-offs:**
- Read optimization: faster queries without full scans
- Write cost: multiple writes per update
- Storage: additional space for indexes
- Covering indexes: store complete values in index for single-read retrieval (doubles storage)

### 6.3 Handling Large Values (Blob Pattern)

**Problem:** 100 KB value limit requires special handling for larger data.

**Solution:** Split into 10 KB chunks stored in adjacent keys:

```python
@fdb.transactional
def write_blob(tr, blob_id, data):
    blob_subspace = Subspace(("blobs", blob_id))
    offset = 0
    while offset < len(data):
        chunk = data[offset:offset + 10000]
        tr[blob_subspace[offset]] = chunk
        offset += 10000

@fdb.transactional
def read_blob(tr, blob_id):
    blob_subspace = Subspace(("blobs", blob_id))
    chunks = []
    for k, v in tr[blob_subspace.range()]:
        chunks.append(v)
    return b''.join(chunks)
```

**Performance:** Single range read efficiently retrieves all chunks.

**Extensions:**
- Sparse representation for random read/write
- Chunk joining to prevent fragmentation (join chunks < 200 bytes)

### 6.4 Multimap/Set Pattern

**Storage Strategy:** Use adjacent key-value pairs with empty or count values:

```python
# Set membership
tr[("set", user_key, member)] = b''

# Multimap with counts
tr.add(("multimap", index, value), struct.pack('<q', 1))
```

**Operations:**

| Operation | Implementation | Conflict Behavior |
|-----------|---------------|-------------------|
| Add | Atomic increment | Conflict-free |
| Remove | Read + conditional decrement | Requires read |
| Contains | Simple get | No conflicts |
| Members | Range read | No conflicts |

**For Redis Sets:** Store members as separate keys, retrieve with range read.

**For Redis Sorted Sets:** Use `(score, member)` tuple for ordering.

### 6.5 Queue Patterns

**FIFO with Minimal Conflicts:**

```python
# Enqueue with randomization to reduce conflicts
index = tr.snapshot[queue_subspace.range(reverse=True)].get_last_index() + 1
tr[queue_subspace[index][random.randint(0, 2**31)]] = value

# Dequeue
first_key = tr[queue_subspace.range(limit=1)].first_key()
value = tr[first_key]
del tr[first_key]
```

**Key Techniques:**
- Snapshot reads for enqueue (conflict-free)
- Random component spreads concurrent enqueues
- Limit=1 for efficient first/last item location

**High-Contention Extension:**
Use staging technique where dequeue operations register requests if initially unsuccessful, then retry fulfillment in loop. Decouples contention from queue items.

### 6.6 Table Patterns

**Dual-Storage for Flexibility:**
Store data in both row-oriented and column-oriented orders:

```python
# Row-oriented
tr[("table", row_id, column_id)] = value

# Column-oriented  
tr[("table_col", column_id, row_id)] = value
```

**Benefits:**
- Efficient retrieval of entire rows OR columns
- Sparse tables consume no storage for unassigned cells
- Single range read for either access pattern

**Trade-off:** Doubles write cost and storage.

### 6.7 Hierarchical Document Pattern

**Convert nested structures to tuples:**

```python
document = {
    "user": {
        "name": "Alice",
        "emails": ["alice@example.com", "alice@work.com"]
    }
}

# Becomes:
tr[("doc", doc_id, "user", "name")] = "Alice"
tr[("doc", doc_id, "user", "emails", 0)] = "alice@example.com"
tr[("doc", doc_id, "user", "emails", 1)] = "alice@work.com"
```

**Benefits:**
- Each tuple prefix corresponds to subdocument
- Retrieve subdocuments with prefix range read
- Lexicographic ordering groups related data
- Partial updates without full document reads

**Special Handling:**
- Sentinel values for empty objects (-2) and arrays (-1)
- Array indices in tuples preserve ordering
- Leaf values in value data respect size limits

### 6.8 Avoiding Hot Spots

**Distribution Strategies:**

1. **Split Hot Keys:**
```python
# Instead of single counter
counter_shard = hash(key) % NUM_SHARDS
tr.add(("counters", key, counter_shard), 1)
```

2. **Multiple Subspaces:**
For read-hot keys limited to ~100K reads/sec, replicate across subspaces with client-side load balancing.

3. **Atomic Operations:**
Use atomic ops (add, max, min) instead of read-modify-write to reduce conflicts.

4. **Snapshot Reads:**
For non-critical consistency, use snapshot reads to avoid adding to conflict set.

---

## 7. Operational Considerations

### 7.1 Monitoring and Observability

**Status Interface:**
- `fdbcli status`: Basic cluster health
- `fdbcli status details`: Per-process metrics (CPU, network, disk, RAM)
- `fdbcli status json`: Machine-readable comprehensive metrics

**Key Metrics:**
- Read/write rates (transactions per second)
- Transaction start and commit rates
- Conflict rates
- Ratekeeper limits (`cluster.qos.transactions_per_second_limit`)
- Storage queue sizes (`cluster.qos.worst_queue_bytes_storage_server`)
- Durability lag (versions and seconds of non-durable data)
- Transaction log queue metrics
- Per-process CPU, network, disk I/O, RAM utilization

**Latency Band Tracking:**
Configure via `\xff\x02/latencyBandConfig` key to track request counts in latency bands for get_read_version, reads, and commits. Enables SLO monitoring.

### 7.2 Configuration Options

**Redundancy Modes:**

| Mode | Copies | Min Machines | Best For |
|------|--------|-------------|----------|
| `single` | 1 | 1 | Development only |
| `double` | 2 | 3 | Testing |
| `triple` | 3 | 5+ | Production |
| `three_datacenter` | 3 across DCs | 9+ | HA production |

**Storage Engines:**
- `ssd`: Default, B-tree for SSDs
- `memory`: In-memory with 1GB default limit
- `ssd-redwood-v1`: High-throughput B-tree
- `ssd-rocksdb-v1`: LSM-tree with compression

**Performance Implications:**
- Commits wait for durability on configured number of copies
- More replicas = higher write latency but better read distribution
- Memory engine provides highest throughput but limited capacity

### 7.3 Failure Modes and Recovery

**Fault Tolerance:**
- Can tolerate single machine failure with few seconds interruption
- Multi-machine failures handled via team-based distribution
- Coordination servers (Paxos) determine partition to remain responsive
- Partition with majority of coordinators accepts transactions

**Recovery Process:**
- Recovery fast-forwards time by 90 seconds
- Aborts all in-flight transactions
- Clients discover new transaction system generation
- Automatic process restart via fdbmonitor

**CAP Theorem Position:**
Prioritizes Consistency over Availability during partitions. Achieves practical high availability by keeping majority partition responsive.

**Client Impacts:**
- `transaction_too_old`: Retry with newer timestamp
- `commit_unknown_result`: Ensure idempotency
- `cluster_version_changed`: Automatic retry in multi-version client

**Graceful Degradation:**
As storage space decreases to 100 MB, database refuses new non-SYSTEM_IMMEDIATE priority transactions, allowing administrative recovery.

### 7.4 Transaction Tagging and Throttling

**Tagging:**
- Up to 5 tags per transaction
- Each tag limited to 16 characters
- TAG: manual throttling only
- AUTO_THROTTLE_TAG: automatic and manual throttling

**Throttling Attributes:**
- Tag identifier
- Rate (transactions/second)
- Priority level
- Type (automatic or manual)
- Expiration time

**Automatic Throttling:**
Storage server congestion triggers automatic rate limits:
- Default priority transactions rate-limited
- Batch priority transactions stopped completely
- Persists for minutes, extends if congestion continues

**Manual Throttling:**
Operators use `fdbcli throttle` commands, supporting up to 40 tags simultaneously.

**Use Cases for Redis Layer:**
- Tag by namespace or user for QoS
- Identify expensive operations
- Implement rate limiting at application layer

---

## 8. Mapping Redis Operations to FDB Primitives

### 8.1 String Operations

**SET key value [EX seconds]:**
```python
tr[("string", key)] = value
if ex:
    expiry_time = current_time + ex
    tr[("expiry", expiry_time, "string", key)] = b''
```

**GET key:**
```python
value = tr[("string", key)]
# Check expiry index for TTL
```

**INCR/DECR:**

Redis stores counter values as string-encoded decimal integers (e.g., `"42"`), so INCR must use read-modify-write rather than FDB atomic add for correct interop with SET:

```python
# Read-modify-write (correct for Redis compatibility)
current = tr[("string", key)]
value = int(current or b'0') + 1
tr[("string", key)] = str(value).encode()
```

FDB atomic add (`tr.add(key, struct.pack('<q', 1))`) can be used as a future optimization for keys known to be exclusively incremented, but requires a dual-representation strategy to handle the transition when a client does `SET counter "42"` followed by `INCR counter`.

**APPEND:**
```python
# Requires read-modify-write
current = tr[("string", key)] or b''
tr[("string", key)] = current + value
```

### 8.2 Hash Operations

**HSET key field value:**
```python
tr[("hash", key, field)] = value
```

**HGET key field:**
```python
value = tr[("hash", key, field)]
```

**HGETALL key:**
```python
result = {}
for k, v in tr[("hash", key, slice_start, slice_end)]:
    field = extract_field(k)
    result[field] = v
```

**HINCRBY:**
```python
tr.add(("hash", key, field), struct.pack('<q', increment))
```

### 8.3 List Operations

**Challenge:** Lists require ordered index-based access, but FDB has no dense array primitive.

**Approach 1 - Sparse Array with Read-Modify-Write Metadata:**

Note: FDB atomic operations (`tr.add`) are blind writes — their result is NOT readable within the same transaction. LPUSH/RPUSH must use read-modify-write on the head/tail index instead. Concurrent pushes will conflict and retry, which is correct and acceptable for most workloads.

```python
# LPUSH - read-modify-write on head index (concurrent pushes conflict and retry)
meta = tr.get(("list_meta", key))
if meta:
    head, tail, length = unpack_list_meta(meta)
else:
    head, tail, length = 0, 0, 0

head -= 1
tr.set(("list_meta", key), pack_list_meta(head, tail, length + 1))
tr[("list", key, head)] = value

# RPUSH - same pattern on tail index
tail += 1
tr.set(("list_meta", key), pack_list_meta(head, tail, length + 1))
tr[("list", key, tail)] = value

# LINDEX - direct key lookup (O(1) if index maps to physical position)
tr[("list", key, head + index)]

# LRANGE - single range read (efficient when indices are sequential)
begin = pack(("list", key, head + start))
end = pack(("list", key, head + stop + 1))
for k, v in tr.get_range(begin, end):
    values.append(v)
```

Note: LRANGE via sequential range read only works when indices have no gaps. After deletions (LREM) or insertions (LINSERT), gaps or non-sequential indices appear. For lists that use LREM/LINSERT, a range scan with element counting is needed instead.

**Approach 2 - Linked Structure:**
Store next/prev pointers for doubly-linked list. More complex but supports efficient insertion/deletion in the middle.

```python
# Each node: (key, index) -> (value, prev_index, next_index)
# Metadata: (list_meta, key) -> (head_index, tail_index, length)
```

**Approach 3 - Versionstamp-Ordered:**
Use versionstamps for natural ordering, but this makes LINDEX O(N) traversal.

**Recommended Trade-off:**
- Sparse array for simple lists with LRANGE/LINDEX support
- Linked structure for lists with frequent middle insertion/deletion
- **Limit list size** to avoid transaction limit issues (max ~10K elements per list)
- For very large lists, consider splitting into multiple keys or warning users

### 8.4 Set Operations

**SADD key member:**
```python
tr[("set", key, member)] = b''
```

**SMEMBERS key:**
```python
members = [extract_member(k) for k, v in tr[("set", key, slice_start, slice_end)]]
```

**SISMEMBER:**
```python
exists = tr[("set", key, member)] is not None
```

**SREM:**
```python
del tr[("set", key, member)]
```

**SINTER/SUNION:**
Perform set operations client-side after fetching all members via range reads.

### 8.5 Sorted Set Operations

**ZADD key score member:**

Scores MUST be encoded using the FDB tuple layer, which applies an order-preserving XOR transform on IEEE 754 doubles. Raw `struct.pack('<d', score)` does NOT produce correct lexicographic ordering (negative scores, -0, NaN, ±inf all sort incorrectly). Always use `pack()` from the tuple layer for key components:

```python
# Primary: score -> member (tuple layer encodes score in order-preserving format)
tr[pack(("zset", key, score, member))] = b''
# Index: member -> score (value encoding doesn't need ordering)
tr[pack(("zset_idx", key, member))] = struct.pack('<d', score)
```

**ZRANGE key start stop (rank-based):**

`ZRANGE` takes rank positions (0-based indices), NOT score ranges. Scan members in score order and skip/take by positional index:

```python
# ZRANGE key start stop — rank-based, NOT score-based
results = []
count = 0
for k, v in tr.get_range_startswith(pack(("zset", key))):
    if count > stop:
        break
    if count >= start:
        results.append(extract_member(k))
    count += 1
```

For negative indices (e.g., `ZRANGE key 0 -1`), resolve against ZCARD first. For large sorted sets, this is O(stop) which may approach the 5-second transaction limit. Maintain a cached cardinality metadata key for O(1) ZCARD.

**ZRANGEBYSCORE key min max (score-based):**

This is the score-range variant — use tuple layer range boundaries:

```python
# ZRANGEBYSCORE — score-range scan
begin = pack(("zset", key, score_min))
end = pack(("zset", key, score_max + epsilon))  # or use key selector for inclusive
results = []
for k, v in tr.get_range(begin, end):
    results.append(extract_member(k))
```

**ZSCORE:**
```python
score_bytes = tr[("zset_idx", key, member)]
score = struct.unpack('<d', score_bytes)
```

**ZREM:**
```python
# Need to lookup score first
score_bytes = tr[("zset_idx", key, member)]
if score_bytes:
    score = struct.unpack('<d', score_bytes)
    del tr[("zset", key, struct.pack('<d', score), member)]
    del tr[("zset_idx", key, member)]
```

**Trade-off:** Maintaining dual indexes doubles write cost but enables efficient operations.

### 8.6 TTL Implementation

**Expiry Index Approach:**

```python
# SET with TTL (millisecond timestamps for PEXPIRE compatibility)
tr[("string", key)] = value
expiry_time_ms = current_time_ms + (ttl * 1000)
tr[pack(("expiry", expiry_time_ms, "string", key))] = b''

# Background expiry processor
@fdb.transactional
def expire_keys(tr):
    current_time_ms = int(time.time() * 1000)
    expired = []
    # Correct range: scan from ("expiry",) up to ("expiry", current_time_ms + 1)
    begin = pack(("expiry",))
    end = pack(("expiry", current_time_ms + 1))
    for k, v in tr.get_range(begin, end, limit=100):
        expiry_time, data_type, key = unpack(k)
        tr.clear(k)  # Remove expiry index entry
        delete_all_data_for_key(tr, data_type, key)  # Remove actual data
        expired.append(key)
    return expired
```

**Considerations:**
- Run background expiry process periodically
- Limit batch size to stay under transaction constraints
- Check TTL on read (lazy expiration)
- Use versionstamp for expiry order if needed

### 8.7 Pub/Sub Implementation

**Option 1: Using Watches (Real-time notifications)**

```python
# SUBSCRIBE channel
watch_future = tr.watch(("pubsub_notify", channel))
tr.commit()
# In separate thread/task:
watch_future.wait()  # Blocks until channel notified
# CRITICAL: Must cancel watch when done to prevent memory leaks!
# watch_future.cancel()

# PUBLISH channel message
tr[("pubsub_msg", channel, versionstamp)] = message
tr[("pubsub_notify", channel)] = struct.pack('<q', time.time())
```

**Watch Challenges:**
- Watches are per-key, not pattern-based (PSUBSCRIBE requires watching multiple keys)
- **Must cancel watches to prevent memory leaks** - FDB doesn't auto-clean them
- Messages need durable storage for reliability (watches only notify, don't deliver)
- Watch management complexity in clustered setups

**Option 2: Polling with versionstamp-ordered message queue (RECOMMENDED)**

```python
# Publish
tr[("pubsub_msg", channel, versionstamp)] = message
# Optionally increment counter for metadata
tr.add(("pubsub_count", channel), struct.pack('<q', 1))

# Subscribe (long-polling)
last_version = client_state.get("last_version", 0)
while True:
    messages = []
    for k, v in tr[("pubsub_msg", channel, last_version+1, MAX_VERSION)].limit(100):
        messages.append(parse_message(v))
        last_version = extract_versionstamp(k)
    
    if messages:
        for msg in messages:
            yield msg
    else:
        # No new messages, sleep briefly
        time.sleep(0.1)  # Adjust polling interval

# Pattern subscription (PSUBSCRIBE)
# Use pattern matching on channel names
for channel in list_of_channels_matching_pattern:
    for k, v in tr[("pubsub_msg", channel, last_versions[channel]+1, MAX_VERSION)]:
        yield parse_message(v)
```

**Recommended Approach: Hybrid Watch + Versionstamp Queue**

Pure polling with `time.sleep(0.1)` adds ~100ms average latency, which is too slow for Redis pub/sub compatibility (Redis delivers in single-digit milliseconds). The recommended hybrid approach:

1. **Publish**: Write message to versionstamp-ordered queue AND update a per-channel notification key
2. **Subscribe**: Watch the per-channel notification key for instant wakeup, then read messages from the versionstamp queue
3. This gives O(1ms) notification latency with the durability and ordering benefits of the queue
4. The watch only monitors a single per-channel counter key (not one per message), avoiding watch lifecycle complexity

**Benefits:**
- Near-instant delivery (watch-based notification)
- Message durability and ordering (versionstamp queue)
- Supports pattern matching (poll across matching channel queues)
- Can batch multiple messages per read
- Single watch per channel subscription (manageable lifecycle)

### 8.8 Transaction Boundaries

**Redis MULTI/EXEC:**
Map directly to FDB transaction:

```python
# MULTI
transaction = db.create_transaction()

# EXEC
@fdb.transactional
def execute_commands(tr, commands):
    results = []
    for cmd in commands:
        result = execute_redis_command(tr, cmd)
        results.append(result)
    return results
```

**Key Insight:** FDB's strict serializability provides stronger guarantees than Redis transactions (which are serializable but not across keys).

**No-Rollback Emulation:** Redis MULTI/EXEC does not roll back on per-command execution errors — partial execution is expected. The implementation must collect per-command errors but still commit the FDB transaction. See DESIGN.md §5 for the full emulation strategy.

**WATCH Emulation:** FDB's conflict detection adds ALL read keys to the conflict set, not just WATCHed keys. To match Redis WATCH semantics, use snapshot reads for non-WATCHed key access within the transaction and `add_read_conflict_key()` only for explicitly WATCHed keys. See DESIGN.md §5 for details.

---

## 9. Performance Optimization Patterns

### 9.1 Minimizing Conflicts

**Strategies:**

1. **Spread Updates:**
   - Distribute frequently updated keys across shards
   - Hash-based distribution: `("counter", key, hash(key) % shards)`

2. **Atomic Operations:**
   - Use `add`, `min`, `max`, `bit_*` for counters and accumulators
   - Avoids read-modify-write conflicts

3. **Snapshot Reads:**
   - For analytics or non-critical consistency
   - Doesn't add to read conflict set

4. **Limit Read Ranges:**
   - Narrow conflict ranges with specific limits
   - Use snapshot for exploratory reads

5. **Batch Operations:**
   - Group related updates in single transaction
   - Reduces total transaction overhead

### 9.2 Transaction Sizing

**Guidelines:**

- Target < 1 second execution time
- Keep under 1 MB size (well below 10 MB limit)
- Batch 50-100 concurrent transactions per process
- For large operations: decompose into multiple transactions

**Example - Bulk Loading:**
```python
# Bad: single huge transaction
for i in range(1_000_000):
    tr[("data", i)] = generate_data()

# Good: batched transactions
for batch in chunks(range(1_000_000), 1000):
    @fdb.transactional
    def load_batch(tr, batch):
        for i in batch:
            tr[("data", i)] = generate_data()
```

### 9.3 Range Read Optimization

**Streaming Modes:**
Different modes affect memory and latency (though specific modes not detailed in docs).

**Best Practices:**

1. **Use Limits:**
   ```python
   # Don't read entire range
   tr[subspace.range()].limit(100)
   ```

2. **Pagination:**
   ```python
   last_key = None
   while True:
       batch = tr[subspace.range(start=last_key)].limit(1000)
       if not batch:
           break
       process(batch)
       last_key = batch[-1].key
   ```

3. **Parallel Ranges:**
   ```python
   # Split range across multiple transactions
   ranges = split_range(("data", start), ("data", end), num_splits=10)
   with concurrent.futures.ThreadPoolExecutor() as executor:
       results = executor.map(lambda r: read_range(db, r), ranges)
   ```

### 9.4 Caching Strategies

**Client-Side Caching:**

- FDB already caches key locations client-side
- Recently-written values cached in memory
- Consider application-level cache for read-heavy workloads

**Read-Your-Writes:**
- Enabled by default: reads within transaction see prior writes
- Disable with `set_read_your_writes_disable()` if not needed
- Reduces memory footprint for large transactions

### 9.5 Monitoring Transaction Performance

**Instrumentation:**

```python
start = time.time()
version_start = tr.get_read_version().wait()

# Execute operations
result = execute_operations(tr)

commit_version = tr.get_committed_version()
duration = time.time() - start

log_metrics({
    "duration_ms": duration * 1000,
    "read_version": version_start,
    "commit_version": commit_version,
    "version_delta": commit_version - version_start,
})
```

**Key Metrics:**
- Transaction duration (aim for < 1s)
- Conflict rate (retry count)
- Read/write key counts
- Data size per transaction

---

## 10. Gotchas and Architectural Constraints

### 10.1 Critical Gotchas

1. **5-Second Transaction Limit is HARD:**
   - **NOT CONFIGURABLE** - this is a fundamental architectural constraint
   - Storage servers keep only 5 seconds of mutations in memory (MVCC window)
   - Older transactions get `transaction_too_old` error
   - **Must decompose any operation that might take >5 seconds**
   - **Design Impact**: Bulk operations, scans, migrations must be chunked

2. **Versionstamps Unavailable Until After Commit:**
   - **CRITICAL**: Can't read versionstamp within the same transaction that writes it
   - Versionstamp is assigned during commit, not available earlier
   - **Design Impact**: If you need the versionstamp for subsequent operations, requires multi-transaction pattern
   - Use placeholder during write, retrieve after commit, use in next transaction

3. **Large Transactions Impact ENTIRE Cluster:**
   - Transactions > 1 MB can cause brief **cluster-wide** availability issues
   - Not just your client - affects ALL clients
   - Transactions > 5 MB can cause significant latency spikes
   - **Best Practice**: Keep transactions under 500KB, definitely under 1MB

4. **Key Selector Offset Performance:**
   - O(offset) resolution time - linear in offset size
   - Avoid large offsets for pagination (e.g., KeySelector with +1000 offset)
   - **Use range reads with limits instead** for efficient pagination

5. **Hot Key Limits (~100K reads/sec):**
   - **HARD LIMIT**: ~100K reads/sec per key with triple replication
   - No automatic replication scaling for hot keys
   - **MUST manually shard hot keys** across multiple keys/subspaces
   - **Production Critical**: Monitor key access patterns, shard proactively

6. **No Built-in Security/Multi-tenancy:**
   - No user-level access control or authentication
   - Anyone with cluster connection can read/write all keys
   - No row-level security or key-level permissions
   - **Must implement authentication/authorization in your application layer**

7. **Watch Lifecycle Management:**
   - Watches persist after transaction commit
   - **MUST explicitly cancel watches** to prevent memory leaks
   - Uncanceled watches consume memory indefinitely
   - **Production Risk**: Memory leaks if watch cleanup is buggy

8. **Optimistic Concurrency - Retry Storms:**
   - High-conflict workloads cause exponential retry storms
   - **Design for low-conflict patterns from day one**
   - Use atomic operations (add, max, min) to avoid read-modify-write conflicts
   - Monitor transaction conflict rates in production

### 10.2 Redis-Specific Gotchas

1. **No Pattern Matching in Watches:**
   - FDB watches are single-key
   - PSUBSCRIBE requires polling or watch-per-channel

2. **List Operations Inefficient:**
   - No dense array primitive
   - LRANGE requires multiple gets or single large range read
   - Consider list size limits

3. **Set Operations Client-Side:**
   - SINTER/SUNION require fetching all members
   - Potentially large data transfer
   - Consider computed set materialization for repeated operations

4. **Sorted Set Dual Index:**
   - Maintaining score->member and member->score doubles writes
   - Consider whether ZSCORE is needed frequently

5. **TTL Background Processing:**
   - Passive expiration requires background process
   - Batch size limited by transaction constraints
   - Need lazy expiration on read path

6. **Transaction Semantics Differ:**
   - FDB provides stronger guarantees than Redis MULTI/EXEC
   - Redis WATCH not needed (implicit in FDB)
   - May need to adjust client expectations

### 10.3 Architectural Constraints

1. **Single Network Thread:**
   - Only one network thread per process
   - Can bottleneck throughput
   - Workaround: spawn additional processes or use multi-threaded client

2. **Cluster Size Limits:**
   - Tested up to 500 cores
   - Larger clusters may have sub-linear scaling
   - Plan capacity accordingly

3. **Database Size:**
   - Tested up to 100 TB
   - Actual disk requirements higher (replication, overhead)
   - Plan storage capacity

4. **No Query Language:**
   - Minimal key-value API only
   - Complex queries implemented in application layer
   - Can be advantage for flexibility

5. **Recovery Time:**
   - Fast-forwards time 90 seconds during recovery
   - Aborts all in-flight transactions
   - Design for transaction replay

6. **Minimum Machines:**
   - Production requires 5+ machines for triple redundancy
   - Development can use single mode
   - Plan infrastructure costs

---

## 11. Implementation Recommendations for Redis Layer

### 11.1 Key Layout Strategy

**Recommended Structure:**
```
("redis", namespace, data_type, user_key, subkey...)
```

**Benefits:**
- Clear isolation by namespace
- Efficient type-specific operations
- Natural range operations
- Easy bulk operations per namespace

**Example:**
```
("redis", "default", "string", "user:1:name") -> "Alice"
("redis", "default", "hash", "user:1", "email") -> "alice@example.com"
("redis", "default", "set", "users:active", "user:1") -> b''
("redis", "default", "zset", "leaderboard", 95.5, "user:1") -> b''
("redis", "default", "expiry", 1640000000, "string", "session:abc") -> b''
```

### 11.2 Transaction Boundary Decisions

**Single Command = Single Transaction (usually):**
Most Redis commands map to single FDB transaction for simplicity.

**Exceptions:**

1. **MULTI/EXEC:** Explicit transaction boundary
2. **Large Operations:** Split into multiple transactions
   - MGET with many keys
   - Large ZRANGE results
   - KEYS/SCAN operations

3. **Compound Operations:** Single transaction for atomicity
   - SET with GET option (replaces deprecated GETSET, removed in Redis 6.2.0)
   - LMOVE (replaces deprecated RPOPLPUSH, removed in Redis 6.2.0)
   - Operations with multiple keys if small

### 11.3 Handling Size Limits

**Strings:**
- Enforce 100 KB limit or implement blob pattern
- Consider warning at 10 KB threshold

**Lists:**
- Limit list length or use blob pattern for large lists
- Document maximum size

**Sorted Sets:**
- Large sorted sets may hit transaction limits on full ZRANGE
- Implement pagination for large results
- Consider materialized views for frequent queries

**Hashes:**
- Large hashes (many fields) may require multiple transactions for HGETALL
- Implement cursor-based iteration

### 11.4 TTL Implementation

**Hybrid Approach:**

1. **Lazy Expiration:**
   Check on every read, delete if expired
   
2. **Active Expiration:**
   Background process scans expiry index:
   - Run every 1-10 seconds
   - Process 100-1000 keys per transaction
   - Use versionstamps for ordered expiry queue

3. **Expiry Index:**
   ```
   ("redis", namespace, "expiry", expiry_timestamp, data_type, key) -> b''
   ```

**Implementation:**
```python
@fdb.transactional
def check_expiry(tr, data_type, key):
    # Check if key has expiry
    expiry = tr[("redis_meta", data_type, key, "expiry")]
    if expiry and time.time() > struct.unpack('<d', expiry)[0]:
        # Delete key and expiry index
        delete_key(tr, data_type, key)
        return None
    return tr[("redis", namespace, data_type, key)]

def background_expiry():
    while True:
        batch = expire_batch(db, limit=100)
        time.sleep(1)
```

### 11.5 Pub/Sub Strategy

**Recommendation:** Implement using versionstamp-ordered message queue with polling.

**Rationale:**
- Watches have management complexity (must cancel)
- Polling with versionstamps provides ordering
- Can batch multiple messages
- Works with pattern matching

**Implementation:**
```python
# Publish
@fdb.transactional
def publish(tr, channel, message):
    tr[("pubsub", channel, versionstamp)] = message
    tr.add(("pubsub_count", channel), struct.pack('<q', 1))

# Subscribe (long-polling)
def subscribe(db, channels, last_versions):
    while True:
        messages = poll_messages(db, channels, last_versions, timeout=1.0)
        for msg in messages:
            yield msg
            last_versions[msg.channel] = msg.version
        if not messages:
            time.sleep(0.1)  # Back off if no messages
```

### 11.6 Performance Tuning

**Configuration:**

1. **Connection Pool:**
   - Multiple FDB client processes for throughput
   - Or use multi-threaded client (CLIENT_THREADS_PER_VERSION)

2. **Transaction Options:**
   - Set reasonable timeouts (5-10 seconds)
   - Configure retry limits (50-100)
   - Use appropriate priorities (batch for background work)

3. **Batching:**
   - Batch MGET/MSET operations within single transaction
   - Pipeline independent commands

4. **Monitoring:**
   - Track transaction duration, conflicts, sizes
   - Monitor FDB cluster health via status json
   - Alert on high conflict rates or latency

**Optimization Checklist:**

- [ ] Use atomic operations for counters (INCR, HINCRBY)
- [ ] Implement snapshot reads for analytical queries
- [ ] Shard hot keys across multiple subspaces
- [ ] Limit range read sizes with pagination
- [ ] Batch background operations (expiry, compaction)
- [ ] Cache frequently accessed data client-side
- [ ] Use transaction tagging for monitoring
- [ ] Profile transaction sizes and durations

### 11.7 Testing Strategy

**Unit Tests:**
- Test each Redis command implementation
- Verify transaction boundaries
- Check error handling and retries

**Integration Tests:**
- Test against real FDB cluster
- Verify concurrency behavior
- Test failure scenarios with buggify

**Performance Tests:**
- Benchmark key operations (GET, SET, HGET, etc.)
- Test under load with many concurrent clients
- Measure conflict rates with contended keys

**Correctness Tests:**
- Use FDB simulator for deterministic testing
- Verify ACID guarantees
- Test recovery scenarios

---

## 12. Summary and Key Takeaways

### 12.1 Core Strengths for Redis Layer

1. **Strict Serializability:** Stronger guarantees than Redis
2. **Ordered Key-Value:** Natural fit for sorted sets, range queries
3. **Atomic Operations:** Efficient conflict-free counters
4. **Horizontal Scaling:** Millions of ops/sec with proper concurrency
5. **ACID Transactions:** Multi-key operations with guarantees

### 12.2 Key Challenges

1. **5-Second/10MB Limits:** Must decompose large operations
2. **No Dense Arrays:** Lists require creative encoding
3. **Hot Key Limits:** Manual sharding needed for >100K reads/sec
4. **Watch Management:** Pub/sub requires careful implementation
5. **No Native TTL:** Must build expiry mechanism

### 12.3 Critical Design Decisions

1. **Key Layout:** Use tuple layer with clear hierarchy
2. **Transaction Boundaries:** One command = one transaction (usually)
3. **TTL:** Hybrid lazy + active expiration (batch deletes respecting 5-second and 10MB limits, ~100-1000 keys per batch)
4. **Pub/Sub:** Versionstamp-ordered queue with polling (recommended over watches for simplicity)
5. **Large Values:** Enforce 100KB limit or implement blob pattern with 10KB chunks
6. **Indexing:** Dual indexes for sorted sets (score->member and member->score), single index for simple lookups
7. **Lists:** Sparse array or linked structure with size limits (document max 10K elements to avoid transaction limits)
8. **Hot Keys:** Monitor and shard proactively (CRITICAL - ~100K reads/sec hard limit per key)

### 12.4 Performance Targets

- **Single Operation Latency:** 1-3ms (FDB read + commit)
- **Throughput:** 100K+ ops/sec with proper concurrency
- **Transaction Success Rate:** >99% (low conflict rate)
- **Hot Key Performance:** 100K reads/sec per key (before sharding)

### 12.5 Next Steps

1. Define precise key encoding scheme using tuple layer
2. Implement core data types (string, hash, set) first
3. Build TTL mechanism with background expiry
4. Implement sorted sets with dual indexing
5. Add pub/sub with versionstamp queue
6. Build comprehensive test suite with simulator
7. Benchmark and optimize based on real workloads
8. Add monitoring and operational tooling

---

## Appendix: Quick Reference

### FDB API Essentials

```python
# Transaction execution
@fdb.transactional
def my_operation(tr, key, value):
    tr[key] = value
    return tr[key]

# Atomic operations
tr.add(key, struct.pack('<q', increment))
tr.max(key, struct.pack('<q', new_value))

# Range reads
for k, v in tr[subspace.range()]:
    process(k, v)

# Snapshot reads
value = tr.snapshot[key]

# Watches
watch = tr.watch(key)
tr.commit()
watch.wait()  # Blocks until key changes

# Versionstamps
tr.set_versionstamped_key(key_with_placeholder, param)
version = tr.get_versionstamp().wait()  # After commit
```

### Key Limits

- Transaction time: 5 seconds (hard limit)
- Transaction size: 10 MB (hard limit), target < 1 MB
- Key size: 10 KB max, target < 32 bytes
- Value size: 100 KB max, target < 10 KB
- Update frequency: < 100/sec per key to avoid conflicts

### Performance Numbers (rough guide)

- Read latency: 0.1-1ms
- Commit latency: 1.5-2.5ms
- Single core: 20K writes/sec, 55K reads/sec (SSD)
- Hot key limit: ~100K reads/sec
- Optimal concurrency: 200+ concurrent transactions

---

**Word Count: ~8,500 words (exceeded target for comprehensiveness, can trim if needed)**
