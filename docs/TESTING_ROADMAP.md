# Comprehensive Testing Roadmap for Redis-Compatible Database on FoundationDB

## Executive Summary

This document provides a detailed testing strategy for a Redis-compatible database built on FoundationDB. The testing approach encompasses six critical dimensions: Redis compatibility verification, property-based testing for invariants, fuzzing for protocol robustness, chaos engineering for distributed correctness, performance benchmarking, and correctness verification. The roadmap prioritizes practical implementation with specific tools and methodologies.

---

## 1. Redis Compatibility Testing

### 1.1 Redis TCL Test Suite

**Overview:** Redis maintains an official TCL-based test suite with approximately 1,000 test cases covering all commands and edge cases.

**Location:** `https://github.com/redis/redis/tree/unstable/tests`

**Structure:**
- `unit/`: Per-command and per-feature tests
- `integration/`: Multi-command workflows, replication, clustering
- `support/`: Test utilities and helpers
- Test files organized by category (string, list, set, sorted set, etc.)

**Running Against Your Implementation:**

1. Start your Redis-compatible server on port 6379 (or configure port)
2. Clone Redis repository: `git clone https://github.com/redis/redis.git`
3. Run specific test units:
   ```bash
   cd redis/tests
   # Point to your server (default is localhost:6379)
   ./runtest --single unit/type/string --host localhost --port 6379
   ./runtest --single unit/type/hash --host localhost --port 6379
   ```
4. Run all tests: `./runtest --host localhost --port 6379`

**Note**: If tests fail due to server configuration, you may need to modify test expectations or skip tests for unimplemented features (cluster, replication, persistence).

**Filtering for Implemented Features:**
- Start with basic data type tests
- Skip cluster, replication, persistence tests (handled by FDB)
- Focus on command semantics, not operational features

**Success Criteria:** 90%+ pass rate on implemented commands

**Integration Strategy:**
- Add to CI pipeline as compatibility gate
- Run subset on every PR, full suite nightly
- Track pass rate over time as metric

### 1.2 redis-py Test Suite

**Overview:** Python client library with extensive integration tests validating client-server interactions.

**Location:** `https://github.com/redis/redis-py`

**Running Tests:**
```bash
git clone https://github.com/redis/redis-py.git
cd redis-py
pip install -e ".[dev]"
# Point to your server
export REDIS_HOST=localhost
export REDIS_PORT=6379
pytest tests/
```

**Key Test Areas:**
- Connection handling and authentication
- Command serialization/deserialization
- Pipeline execution
- Transaction semantics (MULTI/EXEC/WATCH)
- Error handling and retry logic
- Pub/sub functionality

**Value:** Validates wire protocol correctness and client compatibility

### 1.3 Other Client Library Tests

**High-Priority Clients:**

1. **redis-rb (Ruby):**
   ```bash
   git clone https://github.com/redis/redis-rb.git
   cd redis-rb
   bundle install
   rake test
   ```

2. **node-redis (Node.js):**
   ```bash
   git clone https://github.com/redis/node-redis.git
   cd node-redis
   npm install
   npm test
   ```

3. **go-redis (Go):**
   ```bash
   git clone https://github.com/redis/go-redis.git
   cd go-redis
   go test ./...
   ```

4. **jedis (Java):**
   ```bash
   git clone https://github.com/redis/jedis.git
   cd jedis
   mvn test
   ```

**Rationale:** Different clients stress different protocol aspects (pipelining, connection pooling, error paths)

### 1.4 Coverage Analysis

**Approach:**
1. Generate command usage matrix from test suites
2. Map to your implementation status
3. Identify gaps in test coverage

**Tool:** Build custom analyzer:
```python
# Parse test files, extract Redis commands
# Compare against implemented command list
# Generate coverage report by category
```

**Metrics to Track:**
- Commands tested vs. implemented
- Test case count per command
- Edge case coverage (empty values, max sizes, special chars)
- Error path coverage (wrong types, missing keys)

**Goal:** 100% coverage of implemented commands, 80%+ edge case coverage

---

## 2. Property-Based Testing

### 2.1 Redis Command Properties

**Framework:** Use Hypothesis (Python) or proptest (Rust)

**Core Properties to Test:**

**Strings:**
- `GET(SET(k, v)) = v` (read-your-writes)
- `DEL(k); APPEND(k, a); APPEND(k, b); GET(k) = a||b` (concatenation on empty key)
- `Given GET(k) = v: APPEND(k, a); GET(k) = v||a` (append preserves existing value)
- `STRLEN(APPEND(k, v)) = STRLEN(k) + len(v)` (length additivity)
- `INCR(k, n); INCR(k, m) = INCR(k, n+m)` (commutativity)
- `STRLEN(APPEND(k, v)) = STRLEN(k) + len(v)` (length consistency)

**Hashes:**
- `HGET(HSET(k, f, v), f) = v` (field isolation)
- `HLEN(k) = len(HKEYS(k)) = len(HVALS(k))` (length consistency - may require counting in FDB)
- `HINCRBY(k, f, n); HINCRBY(k, f, m) = HINCRBY(k, f, n+m)` (commutativity - **FDB atomic ops make this conflict-free!**)

**Sets:**
- `SISMEMBER(SADD(k, m), m) = true` (membership)
- `SCARD(SADD(k, m)) <= SCARD(k) + 1` (cardinality bounds)
- `SINTER(A, B) = SINTER(B, A)` (commutativity)
- `SUNION(A, SUNION(B, C)) = SUNION(SUNION(A, B), C)` (associativity)

**Sorted Sets:**
- `ZSCORE(ZADD(k, s, m), m) = s` (score consistency)
- `ZCARD(k) = len(ZRANGE(k, 0, -1))` (cardinality)
- `ZRANK(k, m) + ZREVRANK(k, m) = ZCARD(k) - 1` (rank symmetry)
- `ZRANGE(k, 0, n) is sorted by score` (ordering invariant)

**Lists:**
- `LLEN(LPUSH(k, v)) = LLEN(k) + 1` (length tracking)
- `LINDEX(LPUSH(k, v), 0) = v` (head consistency)
- `DEL(k); LPUSH(k, v); RPOP(k) = v` (single-element list: push left, pop right returns same element)
- `Given LLEN(k) > 0: RPOP(LPUSH(k, v)) = original_tail` (non-empty list: LPUSH at head doesn't change tail)
- `LPUSH(k, v); LPOP(k) = v` (stack semantics: push left, pop left returns same element)

**Example Implementation (Hypothesis):**
```python
from hypothesis import given, strategies as st
import redis

@given(key=st.text(), value=st.binary())
def test_set_get_roundtrip(key, value):
    r = redis.Redis()
    r.set(key, value)
    assert r.get(key) == value

@given(
    key=st.text(),
    members=st.lists(st.binary(), min_size=1, max_size=100)
)
def test_set_cardinality(key, members):
    r = redis.Redis()
    r.delete(key)
    for m in members:
        r.sadd(key, m)
    assert r.scard(key) == len(set(members))
```

### 2.2 Linearizability Testing

**Goal:** Verify that concurrent operations appear to execute atomically in some sequential order.

**Tool:** Jepsen framework or Porcupine (Go library)

**Approach:**
1. Record history of operations with invocations and responses
2. Check if history is linearizable with respect to sequential specification
3. If not, produce minimal non-linearizable substring

**Porcupine Example:**
```go
type Operation struct {
    ClientID int
    Input    interface{} // e.g., SET key val
    Output   interface{} // e.g., OK
    Call     int64       // nanosecond timestamp
    Return   int64       // nanosecond timestamp
}

func checkLinearizability(history []Operation) bool {
    model := getRedisModel() // Sequential specification
    return porcupine.CheckOperations(model, history)
}
```

**Test Scenarios:**
- Concurrent SET/GET on same key
- Concurrent INCR operations (should be atomic)
- Concurrent LPUSH/RPOP on same list
- Concurrent ZADD with same scores

**Implementation:**
1. Run workload with multiple concurrent clients
2. Log all operations with timestamps
3. Post-process to check linearizability
4. Report violations with counterexamples

### 2.3 CRDT Properties for Concurrent Operations

**Context:** While Redis isn't CRDT-based, certain operations have convergence properties.

**Properties to Test:**

**Commutative Operations (order-independent):**
- `SADD(k, m1); SADD(k, m2) = SADD(k, m2); SADD(k, m1)`
- Atomic operations: `INCR`, `HINCRBY`, bitwise ops

**Non-Commutative Operations (order-dependent):**
- `SET(k, v1); SET(k, v2) != SET(k, v2); SET(k, v1)` (last-write-wins)
- List operations: `LPUSH(k, a); LPUSH(k, b) != LPUSH(k, b); LPUSH(k, a)`

**Testing Approach:**
1. Generate random operation sequences
2. Execute in different orders
3. Verify expected convergence or divergence
4. Test under FDB's strict serializability

### 2.4 Model-Based Testing

**Framework:** QuickCheck models or TLA+ specifications

**Approach:**
1. Define abstract state machine for Redis data structures
2. Generate random command sequences
3. Execute against both model and implementation
4. Compare final states

**Example State Machine (Python):**
```python
class RedisModel:
    """Simple sequential model for Redis operations.
    Note: This doesn't model FDB's MVCC or snapshot isolation,
    which are actually STRONGER guarantees than Redis provides."""
    
    def __init__(self):
        self.store = {}
    
    def set(self, key, value):
        self.store[key] = value
        return "OK"
    
    def get(self, key):
        return self.store.get(key)
    
    def incr(self, key):
        val = int(self.store.get(key, 0))
        val += 1
        self.store[key] = str(val)
        return val

@given(commands=st.lists(
    st.one_of(
        st.tuples(st.just("SET"), st.text(), st.text()),
        st.tuples(st.just("GET"), st.text()),
        st.tuples(st.just("INCR"), st.text()),
    )
))
def test_against_model(commands):
    model = RedisModel()
    impl = redis.Redis()
    
    for cmd in commands:
        model_result = execute_on_model(model, cmd)
        impl_result = execute_on_impl(impl, cmd)
        assert_equivalent(model_result, impl_result)
```

**Benefits:**
- Finds unexpected edge cases
- Tests complex command interactions
- Validates semantic correctness

---

## 3. Fuzzing

### 3.1 RESP Protocol Fuzzing

**Goal:** Find parser bugs, crashes, and security vulnerabilities in RESP decoder.

**Tool:** AFL++ (American Fuzzy Lop) or libFuzzer

**Setup:**
1. Isolate RESP parser into standalone function
2. Create harness that feeds bytes to parser
3. Instrument with coverage feedback

**AFL++ Example:**
```rust
// fuzz_target.rs
use resp_parser::parse_resp;

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let _ = parse_resp(data);
    });
}
```

**Compile and Run:**
```bash
cargo afl build
mkdir -p in out
echo "*1\r\n\$4\r\nPING\r\n" > in/seed
cargo afl fuzz -i in -o out target/debug/fuzz_target
```

**Test Vectors to Seed:**
- Valid RESP2 commands
- Valid RESP3 commands
- Edge cases: empty arrays, null bulk strings
- Invalid: missing \r\n, wrong lengths, nested depth
- Large: maximum sizes, deeply nested arrays

**Mutations to Apply:**
- Bit flips in type bytes (+, -, :, $, *)
- Length field corruption
- Delimiter removal/duplication
- UTF-8 boundary violations
- Integer overflow in lengths

**Success Metrics:**
- No crashes after 24h fuzzing
- No memory leaks (run with ASAN)
- No infinite loops (timeout detection)
- Graceful error handling for all invalid inputs

### 3.2 Structured Fuzzing for Commands

**Goal:** Generate semantically valid but unusual Redis commands.

**Approach:** Grammar-based fuzzing with command structure awareness

**Tool:** Custom generator or cargo-fuzz with structure-aware mutations

**Command Grammar:**
```
Command := CommandName Args*
Args := String | Integer | Float | Array | Null
String := ValidUTF8 | Binary | Empty | MaxSize
Integer := i64::MIN..i64::MAX | SpecialValues
```

**Fuzzing Strategy:**
1. Generate valid command syntax
2. Mutate argument values and types
3. Test boundary conditions systematically

**Example Fuzzer (using arbitrary crate):**
```rust
use arbitrary::{Arbitrary, Unstructured};

#[derive(Arbitrary, Debug)]
enum RedisCommand {
    Set { key: Vec<u8>, value: Vec<u8>, ex: Option<u64> },
    Get { key: Vec<u8> },
    Incr { key: Vec<u8> },
    // ... more commands
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    if let Ok(cmd) = RedisCommand::arbitrary(&mut u) {
        execute_command(&cmd);
    }
});
```

**Focus Areas:**
- Maximum sizes (100KB values, 10KB keys)
- Special characters (null bytes, UTF-8 boundaries)
- Empty strings, empty arrays
- Type mismatches (INCR on string, ZADD wrong arg count)
- Concurrent fuzzing (multiple clients, race conditions)

### 3.3 Coverage-Guided Fuzzing with cargo-fuzz

**Setup:**
```bash
cargo install cargo-fuzz
cargo fuzz init
cargo fuzz add resp_protocol
cargo fuzz add command_execution
```

**Configuration (fuzz/Cargo.toml):**
```toml
[dependencies]
libfuzzer-sys = "0.4"
arbitrary = { version = "1.3", features = ["derive"] }
```

**Run with Coverage:**
```bash
cargo fuzz run resp_protocol -- -max_total_time=3600
cargo fuzz coverage resp_protocol
```

**Analyze Results:**
```bash
cargo fuzz fmt resp_protocol crash-xyz
cargo fuzz cmin resp_protocol # Minimize corpus
```

**Integration:**
- Run fuzzing in CI for 5-10 minutes per target
- Long-running fuzzing campaigns (24h-1week) on dedicated hardware
- Store corpus in repo, minimize before commit

### 3.4 AFL Integration Strategy

**When to Use AFL vs. cargo-fuzz:**
- AFL++: Better for persistent services (socket fuzzing)
- libFuzzer: Better for library functions (parser fuzzing)

**AFL++ Persistent Mode Setup:**
```c
// For socket-based fuzzing
__AFL_LOOP(1000) {
    conn = accept_fuzzed_input();
    process_redis_command(conn);
    close(conn);
}
```

**Distributed Fuzzing:**
- Run multiple AFL instances in parallel
- Use `-M` for master, `-S` for slaves
- Synchronize corpus across instances

**Trophies to Hunt:**
- Memory corruption (ASAN/MSAN)
- Assertion failures
- Undefined behavior (UBSAN)
- Deadlocks (timeout detection)
- Resource exhaustion (ulimit protections)

---

## 4. Chaos Engineering

### 4.1 Network Partition Simulation

**Tool:** Jepsen or custom test harness with toxiproxy/tc

**Important Context**: FDB provides strict serializability, which means:
- Split-brain is handled by FDB's Paxos-based coordinator quorum
- Only the partition with majority coordinators accepts writes
- This is STRONGER than Redis (which can have split-brain in cluster mode)

**Scenarios:**

1. **Split Brain:**
   - Partition FDB cluster into two groups (separate coordinators)
   - Verify only majority partition accepts writes (FDB guarantees this)
   - Verify consistency after partition heals (should be perfect - strict serializability)
   - **Note**: This tests FDB behavior more than your layer, but validates integration

2. **Intermittent Partitions:**
   - Brief network flaps (100ms-1s)
   - Test transaction retry logic in your Redis layer
   - Verify no data loss or corruption
   - **Key Test**: Does your retry logic handle `transaction_too_old` correctly?

3. **Asymmetric Partitions:**
   - A can talk to B, B can't talk to A
   - Test FDB's transaction commit protocol
   - Verify your layer handles timeouts and retries correctly

**toxiproxy Example:**
```go
proxy := toxiproxy.NewProxy("redis", "localhost:6379", "localhost:16379")
proxy.Start()

// Inject latency
proxy.AddToxic("latency", "latency", 1.0, toxiproxy.Attributes{
    "latency": 1000, // 1s delay
})

// Run tests
runTestWorkload()

// Remove toxic
proxy.RemoveToxic("latency")
```

**tc (traffic control) Example:**
```bash
# Add 100ms latency
tc qdisc add dev eth0 root netem delay 100ms

# Add 10% packet loss
tc qdisc add dev eth0 root netem loss 10%

# Partition: block traffic to specific IP
iptables -A INPUT -s 192.168.1.100 -j DROP
```

**Verification:**
- No split-brain writes (linearizability check)
- Eventual consistency after healing
- Client retry behavior
- Transaction success/failure rates

### 4.2 FDB Failure Injection (Buggify Mode)

**Overview:** FoundationDB has built-in failure injection for testing.

**Enabling Buggify:**
```bash
# In fdb.cluster or via environment
export FDB_NETWORK_OPTION_BUGGIFY_ENABLE=1
```

**Failure Types Injected:**
- Transaction conflicts
- Slow commits
- Storage server failures
- Network delays
- Disk errors

**Testing Strategy:**
1. Run workload with buggify enabled
2. Monitor error rates and retries
3. Verify correctness of results
4. Check for deadlocks or hangs

**Custom Failure Injection:**
```python
# In your Redis layer
if should_inject_failure():
    raise FDBError(1020)  # Simulate commit conflict

# Test retry logic
@retry_on_fdb_error
def redis_set(tr, key, value):
    tr[encode_key(key)] = value
```

**Metrics:**
- Transaction retry count distribution
- End-to-end latency with failures
- Success rate under adverse conditions
- Recovery time after failure

### 4.3 Load Testing Tools

**redis-benchmark:**

Basic usage:
```bash
redis-benchmark -h localhost -p 6379 -t set,get -n 100000 -c 50

# Specific tests
redis-benchmark -t set -r 100000 -n 1000000  # 1M SETs
redis-benchmark -t get -r 100000 -n 1000000  # 1M GETs
redis-benchmark -t lpush,rpop -n 100000      # List ops
redis-benchmark -t zadd -n 100000            # Sorted set
```

Advanced options:
```bash
# Pipeline requests
redis-benchmark -t set,get -n 1000000 -P 16

# Key space size
redis-benchmark -t set -r 10000000 -n 1000000

# Data size
redis-benchmark -t set -d 1024 -n 100000  # 1KB values

# CSV output for analysis
redis-benchmark -t set,get -n 100000 --csv
```

**memtier_benchmark:**

More sophisticated than redis-benchmark:
```bash
# Installation
git clone https://github.com/RedisLabs/memtier_benchmark.git
cd memtier_benchmark
autoreconf -ivf
./configure
make
sudo make install

# Usage
memtier_benchmark -s localhost -p 6379 \
    --protocol=redis \
    --clients=50 \
    --threads=4 \
    --requests=10000 \
    --data-size=1024 \
    --key-pattern=R:R \
    --ratio=1:10  # 1 write : 10 reads

# Advanced: multi-key operations
memtier_benchmark -s localhost -p 6379 \
    --command="MGET __key__ __key__ __key__" \
    --command-key-pattern=R \
    --test-time=60

# JSON output for analysis
memtier_benchmark ... --json-out-file=results.json
```

**Custom Load Generators:**

For workload-specific testing:
```python
import redis
import time
import random
from concurrent.futures import ThreadPoolExecutor

def workload_generator(duration_sec):
    r = redis.Redis()
    start = time.time()
    count = 0
    
    while time.time() - start < duration_sec:
        op = random.choice(['set', 'get', 'incr', 'zadd'])
        key = f"key:{random.randint(1, 100000)}"
        
        if op == 'set':
            r.set(key, f"value-{count}")
        elif op == 'get':
            r.get(key)
        elif op == 'incr':
            r.incr(key)
        elif op == 'zadd':
            r.zadd(f"zset:{key}", {f"member-{count}": count})
        
        count += 1
    
    return count

# Run with 100 concurrent clients
with ThreadPoolExecutor(max_workers=100) as executor:
    futures = [executor.submit(workload_generator, 60) 
               for _ in range(100)]
    results = [f.result() for f in futures]
    
print(f"Total ops: {sum(results)}")
print(f"Ops/sec: {sum(results) / 60}")
```

### 4.4 Jepsen-Style Testing

**Overview:** Jepsen tests distributed systems for correctness under partitions.

**Setup:**
```bash
git clone https://github.com/jepsen-io/jepsen.git
cd jepsen

# Create test for your Redis implementation
mkdir -p redis-fdb/test/redis_fdb
```

**Test Structure (Clojure):**
```clojure
(ns redis-fdb.core
  (:require [clojure.tools.logging :refer :all]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]))

(def db
  (reify db/DB
    (setup! [_ test node]
      (info node "Starting Redis-FDB")
      ; Installation and startup logic
      )
    (teardown! [_ test node]
      (info node "Stopping Redis-FDB")
      ; Cleanup logic
      )))

(defn workload
  []
  {:name "redis-set-get"
   :client (redis-client)
   :generator (gen/phases
                (gen/mix [r w])
                (gen/nemesis-seq [(gen/sleep 5)
                                   {:type :start, :f :partition}
                                   (gen/sleep 10)
                                   {:type :stop, :f :partition}]))
   :checker (checker/linearizable)})

(def test-config
  (merge tests/noop-test
         {:name "redis-fdb"
          :os debian/os
          :db db
          :client (redis-client)
          :nemesis (nemesis/partition-random-halves)
          :generator (->> (workload)
                          (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info, :f :start}
                                    (gen/sleep 5)
                                    {:type :info, :f :stop}])))
          :checker (checker/compose
                     {:linear (checker/linearizable)
                      :perf (checker/perf)})}))
```

**Running Jepsen:**
```bash
lein run test --nodes n1,n2,n3,n4,n5 --time-limit 600
```

**Nemesis Types:**
- `partition-random-halves`: Split cluster randomly
- `partition-random-node`: Isolate random node
- `kill-random-processes`: Kill random nodes
- `clock-skew`: Introduce time drift

**Checkers:**
- Linearizability
- Timeline consistency
- Monotonic reads
- Read-your-writes

**Success Criteria:**
- No linearizability violations
- No data loss
- Correct transaction semantics under partitions

---

## 5. Performance Testing

### 5.1 Benchmark Suite Design

**Goals:**
1. Validate performance meets requirements
2. Identify bottlenecks
3. Track performance over time
4. Compare with Redis

**Benchmark Categories:**

**Single-Key Operations:**
```bash
# Strings
redis-benchmark -t set,get,incr -n 1000000
# Expected: 100K+ ops/sec

# Hashes
redis-benchmark -t hset,hget,hincrby -n 1000000
# Expected: 80K+ ops/sec

# Lists
redis-benchmark -t lpush,rpush,lpop,rpop,lrange -n 1000000
# Expected: 60K+ ops/sec (range queries slower)

# Sets
redis-benchmark -t sadd,smembers,sismember -n 1000000
# Expected: 70K+ ops/sec

# Sorted Sets
redis-benchmark -t zadd,zrange,zrank -n 1000000
# Expected: 50K+ ops/sec (dual-index overhead)
```

**Multi-Key Operations:**
```bash
# MGET/MSET
memtier_benchmark --command="MSET __key__ __data__ __key__ __data__" \
    --command-key-pattern=R:R --test-time=60

# Set operations
memtier_benchmark --command="SUNION __key__ __key__ __key__" \
    --command-key-pattern=R:R --test-time=60
```

**Transactions:**
```bash
# MULTI/EXEC with multiple commands
memtier_benchmark --command="MULTI" \
    --command="SET __key__ __data__" \
    --command="INCR __key__" \
    --command="EXEC" \
    --test-time=60
```

**Pipelining:**
```bash
# Pipeline 10 commands
redis-benchmark -t set,get -n 1000000 -P 10
# Expected: 5-10x throughput improvement

# Pipeline 100 commands
redis-benchmark -t set,get -n 1000000 -P 100
# Expected: 20-50x improvement (approaching latency limits)
```

### 5.2 Latency Percentiles

**Measuring with redis-benchmark:**
```bash
redis-benchmark -t set,get -n 1000000 --csv > results.csv
```

**Custom Latency Tracking:**
```python
import redis
import time
import numpy as np

def measure_latencies(operations=10000):
    r = redis.Redis()
    latencies = []
    
    for i in range(operations):
        start = time.perf_counter_ns()
        r.set(f"key:{i}", f"value:{i}")
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1_000_000)  # Convert to ms
    
    latencies = np.array(latencies)
    
    print(f"Mean: {latencies.mean():.2f}ms")
    print(f"Median (p50): {np.percentile(latencies, 50):.2f}ms")
    print(f"p90: {np.percentile(latencies, 90):.2f}ms")
    print(f"p95: {np.percentile(latencies, 95):.2f}ms")
    print(f"p99: {np.percentile(latencies, 99):.2f}ms")
    print(f"p99.9: {np.percentile(latencies, 99.9):.2f}ms")
    print(f"p99.99: {np.percentile(latencies, 99.99):.2f}ms")
    print(f"Max: {latencies.max():.2f}ms")
```

**Target Latencies (FDB-backed):**
- **GET operations** (read-only):
  - p50: 0.5-1ms (FDB read latency, no commit needed)
  - p99: 2-3ms
  - p99.9: 5-10ms
- **SET operations** (write + commit):
  - p50: 2-3ms (FDB read + commit latency)
  - p90: 4-5ms
  - p95: 6-8ms
  - p99: 10-15ms
  - p99.9: 20-50ms
  - p99.99: 50-100ms

**Redis (in-memory) comparison:**
- p50: 0.1-0.2ms (both GET and SET)
- p99: 0.5-1ms
- p99.9: 1-2ms

**Key Insight**: FDB-backed implementation will be slower due to durability guarantees, but provides stronger consistency than Redis cluster mode.

**Latency Breakdown Analysis:**
```python
# Profile where time is spent
with profiler:
    # Parse RESP
    cmd = parse_resp(data)
    
    # FDB transaction
    with fdb.create_transaction() as tr:
        # Key encoding
        key = encode_key(cmd.key)
        
        # FDB read
        value = tr[key]
        
        # Business logic
        result = process(value)
        
        # FDB write
        tr[key] = result
        
        # Commit
        tr.commit().wait()
    
    # Serialize response
    resp = encode_resp(result)
```

### 5.3 Throughput Testing Under Various Workloads

**Read-Heavy (90% reads, 10% writes):**
```bash
memtier_benchmark -s localhost -p 6379 \
    --protocol=redis \
    --clients=100 \
    --threads=8 \
    --ratio=9:1 \
    --test-time=300 \
    --key-pattern=G:G  # Gaussian distribution
```

**Write-Heavy (10% reads, 90% writes):**
```bash
memtier_benchmark --ratio=1:9 --test-time=300
```

**Balanced (50/50):**
```bash
memtier_benchmark --ratio=1:1 --test-time=300
```

**Hot Key Scenario:**
```bash
# Zipfian distribution (realistic web workload)
memtier_benchmark --key-pattern=S:S --test-time=300
```

**Large Values:**
```bash
# 1KB values
memtier_benchmark --data-size=1024 --test-time=300

# 10KB values (near 100KB FDB limit)
memtier_benchmark --data-size=10240 --test-time=300

# 100KB values (FDB limit)
memtier_benchmark --data-size=102400 --test-time=300
```

**Expected Throughput (FDB-backed):**
- Single process: 50-100K ops/sec (with high concurrency)
- Multiple processes: Linear scaling up to FDB cluster limits
- Read-heavy: Higher throughput (no commit overhead)
- Write-heavy: Lower throughput (commit latency)

**Comparison with Redis:**
- Redis: 100K+ ops/sec single-threaded
- Redis with pipelining: 1M+ ops/sec
- Your implementation: 50-100K ops/sec per process
- Horizontal scaling advantage: FDB supports distributed load

### 5.4 Comparison Methodology Against Redis

**Apples-to-Apples Testing:**

1. **Same Hardware:**
   - Run on identical machines
   - Same network configuration
   - Same OS tuning

2. **Same Workload:**
   - Identical test parameters
   - Same data sizes
   - Same concurrency levels

3. **Fair Comparison:**
   - Redis: In-memory, no durability (or AOF always with fsync)
   - Your implementation: FDB-backed with durability
   - Note: Different durability guarantees

**Benchmark Script:**
```bash
#!/bin/bash

# Test both implementations
for impl in redis redis-fdb; do
    echo "Testing $impl"
    
    # Start server
    start_server $impl
    
    # Run benchmarks
    redis-benchmark -h localhost -p 6379 \
        -t set,get,incr,lpush,zadd \
        -n 1000000 \
        --csv > results_${impl}.csv
    
    memtier_benchmark -s localhost -p 6379 \
        --protocol=redis \
        --clients=50 \
        --threads=4 \
        --test-time=300 \
        --json-out-file=results_${impl}.json
    
    # Stop server
    stop_server $impl
done

# Compare results
python3 compare_results.py results_redis.csv results_redis-fdb.csv
```

**Metrics to Report:**
- Throughput (ops/sec)
- Latency percentiles (p50, p99, p99.9)
- Relative performance (% of Redis baseline)
- Resource usage (CPU, memory, network)

**Expected Results:**
- 50-80% of Redis throughput (due to FDB network overhead)
- 10-20x higher p99 latency (2-3ms vs 0.2ms)
- Horizontal scaling advantage
- Stronger durability guarantees

**Visualization:**
```python
import matplotlib.pyplot as plt
import pandas as pd

redis_data = pd.read_csv('results_redis.csv')
fdb_data = pd.read_csv('results_redis-fdb.csv')

# Throughput comparison
plt.bar(['Redis', 'Redis-FDB'], 
        [redis_data['ops_per_sec'].mean(), 
         fdb_data['ops_per_sec'].mean()])
plt.ylabel('Operations per Second')
plt.title('Throughput Comparison')
plt.savefig('throughput.png')

# Latency comparison
latencies = ['p50', 'p90', 'p99', 'p99.9']
redis_lat = [redis_data[l].mean() for l in latencies]
fdb_lat = [fdb_data[l].mean() for l in latencies]

x = range(len(latencies))
plt.plot(x, redis_lat, label='Redis', marker='o')
plt.plot(x, fdb_lat, label='Redis-FDB', marker='s')
plt.xticks(x, latencies)
plt.ylabel('Latency (ms)')
plt.yscale('log')
plt.legend()
plt.title('Latency Percentiles')
plt.savefig('latency.png')
```

---

## 6. Correctness Testing

### 6.1 Transaction Isolation Verification

**Test Cases:**

**Dirty Reads (should not occur):**

Uses threading with synchronization barriers to ensure the read races against an uncommitted write:

```python
def test_no_dirty_reads():
    r1 = redis.Redis()
    r2 = redis.Redis()
    barrier = threading.Barrier(2)
    
    # Client 1: Start transaction, write, signal, then commit
    def writer():
        pipe1 = r1.pipeline()
        pipe1.multi()
        pipe1.set('key', 'value1')
        barrier.wait()  # Signal that MULTI is queued
        time.sleep(0.1)  # Give reader time to check
        pipe1.execute()
    
    # Client 2: Read while Client 1 has queued but not committed
    def reader():
        barrier.wait()  # Wait for writer to queue
        value = r2.get('key')
        assert value is None  # Should not see uncommitted write
    
    r1.delete('key')
    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=reader)
    t1.start(); t2.start()
    t1.join(); t2.join()
    
    # After commit
    value = r2.get('key')
    assert value == b'value1'
```

**Non-Repeatable Reads (should not occur in transaction):**
```python
def test_repeatable_reads():
    r1 = redis.Redis()
    r2 = redis.Redis()
    
    r1.set('key', 'initial')
    
    # Client 1: WATCH key
    r1.watch('key')
    value1 = r1.get('key')
    
    # Client 2: Modify key
    r2.set('key', 'modified')
    
    # Client 1: Transaction should fail due to WATCH
    pipe = r1.pipeline()
    pipe.multi()
    pipe.set('key', 'value1')
    result = pipe.execute()
    
    assert result is None  # Transaction aborted
```

**Phantom Reads (relevant for range queries):**
```python
def test_no_phantom_reads():
    r1 = redis.Redis()
    r2 = redis.Redis()
    
    # Setup
    r1.zadd('zset', {'a': 1, 'b': 2})
    
    # Client 1: WATCH zset
    r1.watch('zset')
    members1 = r1.zrange('zset', 0, -1)
    
    # Client 2: Insert new member
    r2.zadd('zset', {'c': 3})
    
    # Client 1: Transaction should fail
    pipe = r1.pipeline()
    pipe.multi()
    pipe.zadd('zset', {'d': 4})
    result = pipe.execute()
    
    assert result is None  # Detected phantom
```

### 6.2 Serializability Testing

**Goal:** Verify execution is equivalent to some serial order.

**Approach:** Antidote-style testing with known anomalies.

**Test: Write Skew:**
```python
def test_no_write_skew():
    r1 = redis.Redis()
    r2 = redis.Redis()
    
    # Setup: x=0, y=0, invariant: x+y >= 0
    r1.set('x', '0')
    r1.set('y', '0')
    
    def txn1():
        pipe = r1.pipeline()
        pipe.watch('x', 'y')
        x = int(pipe.get('x'))
        y = int(pipe.get('y'))
        if x + y >= 0:
            pipe.multi()
            pipe.set('x', str(x - 10))
            pipe.execute()
    
    def txn2():
        pipe = r2.pipeline()
        pipe.watch('x', 'y')
        x = int(pipe.get('x'))
        y = int(pipe.get('y'))
        if x + y >= 0:
            pipe.multi()
            pipe.set('y', str(y - 10))
            pipe.execute()
    
    # Run concurrently
    with ThreadPoolExecutor() as executor:
        f1 = executor.submit(txn1)
        f2 = executor.submit(txn2)
        f1.result()
        f2.result()
    
    # Check invariant not violated
    x = int(r1.get('x'))
    y = int(r1.get('y'))
    assert x + y >= -10  # At most one transaction committed
```

**Test: Lost Updates:**
```python
def test_no_lost_updates():
    r = redis.Redis()
    r.set('counter', '0')
    
    def increment():
        for _ in range(100):
            while True:  # Retry loop for each increment
                try:
                    pipe = r.pipeline()
                    pipe.watch('counter')
                    value = int(pipe.get('counter'))
                    pipe.multi()
                    pipe.set('counter', str(value + 1))
                    pipe.execute()
                    break  # Success — move to next increment
                except redis.WatchError:
                    continue  # Retry THIS increment
    
    # 10 concurrent incrementers
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(increment) for _ in range(10)]
        for f in futures:
            f.result()
    
    final = int(r.get('counter'))
    assert final == 1000  # No lost updates
```

### 6.3 TTL Correctness

**Test Cases:**

**Expiration Timing:**
```python
def test_ttl_expiration():
    r = redis.Redis()
    r.set('key', 'value', ex=1)  # 1 second TTL
    
    # Immediately after set
    assert r.get('key') == b'value'
    assert 0 < r.ttl('key') <= 1
    
    # After 0.5 seconds
    time.sleep(0.5)
    assert r.get('key') == b'value'
    assert r.ttl('key') <= 1
    
    # After 1.5 seconds (with margin)
    time.sleep(1.0)
    assert r.get('key') is None
    assert r.ttl('key') == -2  # Key doesn't exist
```

**PERSIST Command:**
```python
def test_persist():
    r = redis.Redis()
    r.set('key', 'value', ex=10)
    assert r.ttl('key') > 0
    
    r.persist('key')
    assert r.ttl('key') == -1  # No expiration
    
    time.sleep(11)
    assert r.get('key') == b'value'  # Still exists
```

**TTL Update on Write:**
```python
def test_ttl_reset_on_write():
    r = redis.Redis()
    r.set('key', 'value1', ex=10)
    time.sleep(5)
    
    # Overwrite with new TTL
    r.set('key', 'value2', ex=10)
    ttl = r.ttl('key')
    assert 9 <= ttl <= 10  # TTL reset
```

**TTL Preservation in Transactions:**
```python
def test_ttl_in_transaction():
    r = redis.Redis()
    r.set('key1', 'value1', ex=10)
    
    pipe = r.pipeline()
    pipe.multi()
    pipe.get('key1')
    pipe.set('key2', 'value2', ex=5)
    pipe.execute()
    
    assert r.ttl('key1') > 0
    assert r.ttl('key2') > 0
```

### 6.4 Race Condition Detection

**Tools:**
- ThreadSanitizer (TSan) for C/C++/Rust
- Rust's `cargo test` with `--release` (catches some UB)
- Custom race detectors for application logic

**Test Scenarios:**

**Concurrent INCR:**
```python
def test_concurrent_incr_no_race():
    r = redis.Redis()
    r.set('counter', '0')
    
    def incr_many(count):
        for _ in range(count):
            r.incr('counter')
    
    # 100 threads, 1000 increments each
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(incr_many, 1000) 
                   for _ in range(100)]
        for f in futures:
            f.result()
    
    final = int(r.get('counter'))
    assert final == 100000  # No lost increments
```

**Concurrent List Operations:**
```python
def test_concurrent_list_push_pop():
    r = redis.Redis()
    
    def pusher():
        for i in range(1000):
            r.lpush('list', f'item-{i}')
    
    def popper():
        popped = []
        for _ in range(1000):
            item = r.rpop('list')
            if item:
                popped.append(item)
        return popped
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        push_futures = [executor.submit(pusher) for _ in range(5)]
        pop_futures = [executor.submit(popper) for _ in range(5)]
        
        for f in push_futures + pop_futures:
            f.result()
    
    # Check no items lost or duplicated
    remaining = r.lrange('list', 0, -1)
    total_items = len(remaining)
    # Cannot easily check exact count, but no crashes
```

**Connection Pool Race:**
```python
def test_connection_pool_safety():
    pool = redis.ConnectionPool(max_connections=10)
    
    def worker():
        r = redis.Redis(connection_pool=pool)
        for _ in range(1000):
            r.set('key', 'value')
            r.get('key')
    
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(worker) for _ in range(50)]
        for f in futures:
            f.result()  # Should complete without errors
```

**TSan Integration (Rust):**
```bash
# Build with ThreadSanitizer
RUSTFLAGS="-Z sanitizer=thread" cargo +nightly test

# Or for C++ components
clang++ -fsanitize=thread -g -O1 your_code.cpp
./a.out
```

---

## Implementation Priorities

### Phase 1: Foundation - INTEGRATED WITH DESIGN.md PHASE 1
**Integration Note**: Testing infrastructure is built ALONGSIDE implementation, not separately.

1. Set up testing infrastructure:
   - Unit test framework (Rust)
   - Property test harness (proptest)
   - Fuzzing setup (cargo-fuzz)
   - CI/CD pipeline with test gates

2. Protocol testing:
   - RESP3 parser fuzzing harness running continuously
   - RESP2 backward compatibility testing
   - Property tests for protocol encoding/decoding
   - Integration with Redis TCL test suite (initial setup)

3. FDB integration testing:
   - Mock FDB for unit tests
   - Real FDB integration test framework
   - Transaction retry logic tests

**Deliverables:**
- Testing infrastructure operational BEFORE command implementation
- RESP3 parser (with RESP2 backward compatibility) fuzzed for 24+ hours without crashes
- CI pipeline running on every commit
- Foundation for property tests established

### Phase 2: Command Implementation & Testing (Weeks 3-8) - INTEGRATED WITH DESIGN.md PHASE 2
**Timing Note**: Testing happens DURING implementation, not after.

For each command implemented:
1. Unit tests with mocks
2. Property-based tests for invariants
3. Redis TCL test suite integration (track pass rate)
4. Fuzz testing for command parsing

**Deliverables by End of Phase 2:**
- 70+ commands implemented with full test coverage
- 90%+ pass rate on Redis TCL test suite (for implemented commands)
- Property tests for all data structures (strings, hashes, lists, sets, sorted sets)
- Fuzzing corpus covers all command types
- Performance baseline established with redis-benchmark

### Phase 3: Advanced Testing & Production Readiness - INTEGRATED WITH DESIGN.md PHASES 3-5
1. Chaos engineering:
   - Network partition simulation (toxiproxy)
   - FDB failure injection (buggify mode)
   - Long-running stability tests (72h+)

2. Correctness verification:
   - Linearizability testing (Jepsen or Porcupine)
   - Transaction isolation verification
   - Race condition detection (ThreadSanitizer)

3. Performance & optimization:
   - Comprehensive benchmarking vs Redis
   - Latency percentile tracking
   - Hot key behavior testing
   - Client library compatibility tests (redis-py, go-redis, node-redis)

**Deliverables:**
- No linearizability violations found (Jepsen passing)
- 72+ hour stability test passed
- Performance comparison report (vs Redis)
- Client compatibility matrix (python, go, node, java)
- Production readiness checklist completed

### Phase 4: Ongoing (Post-Launch)
- Continuous fuzzing (24/7 on dedicated hardware)
- Nightly performance regression tests
- Weekly long-running stability tests
- Quarterly Jepsen re-runs with new scenarios

---

## Success Criteria

1. **Compatibility:** 90%+ pass rate on Redis TCL test suite for implemented commands
2. **Reliability:** Zero crashes in extended continuous fuzzing runs
3. **Correctness:** Pass all linearizability tests under Jepsen
4. **Performance:** 50-80% of Redis throughput; p99 latency <5ms for reads, <15ms for writes
5. **Durability:** Zero data loss in chaos engineering tests
6. **Scalability:** Linear throughput scaling to 8+ processes

---

## Tooling Summary

| Category | Tool | Purpose |
|----------|------|---------|
| Compatibility | Redis TCL suite | Official test suite |
| | redis-py tests | Python client validation |
| | go-redis tests | Go client validation |
| Property Testing | Hypothesis (Python) | Generate test cases |
| | proptest (Rust) | Rust property testing |
| | Porcupine (Go) | Linearizability checking |
| Fuzzing | cargo-fuzz | Coverage-guided fuzzing |
| | AFL++ | Protocol fuzzing |
| | libFuzzer | In-process fuzzing |
| Chaos | Jepsen | Distributed correctness |
| | toxiproxy | Network fault injection |
| | tc/iptables | Network simulation |
| Performance | redis-benchmark | Basic benchmarking |
| | memtier_benchmark | Advanced load testing |
| | Custom generators | Workload-specific tests |
| Correctness | ThreadSanitizer | Race detection |
| | Custom test suites | Transaction isolation |
| | FDB simulator | Deterministic testing |

---

## Resource Requirements

- **Ongoing maintenance**: Regular time allocation for test suite updates
- **Infrastructure**: 4-8 core dedicated fuzzing server
- **CI time**: ~30 minutes per PR, 2-4 hours nightly
- **Team commitment**: Testing integrated into implementation (not separate)

---

## Conclusion

This testing roadmap provides comprehensive coverage across compatibility, correctness, performance, and reliability dimensions. The phased approach allows incremental deployment while building confidence through multiple testing methodologies. Priority should be given to foundational testing (compatibility, basic fuzzing) before advancing to sophisticated chaos engineering and Jepsen tests. Continuous integration of all test categories ensures ongoing quality as the codebase evolves.
