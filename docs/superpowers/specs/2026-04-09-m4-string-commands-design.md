# M4: String Commands — Design Spec

**Date**: 2026-04-09
**Milestone**: 4 of 13
**Depends on**: M3 (FDB Storage Layer) — complete
**Commands**: GET, SET, SETNX, SETEX, PSETEX, GETDEL, DEL, EXISTS, MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETRANGE, SETRANGE (19 total)

---

## 1. Goal

Wire FDB storage into the command dispatch layer and implement all Redis string commands with full flag support. After this milestone, kvdb is a functional string-value store that passes `redis-cli` interactive use and automated tests against real FDB.

## 2. Architecture Changes

### 2.1 Async Command Dispatch

The current `dispatch()` function is synchronous. Every string command needs FDB access, so dispatch must become async. The connection handler already runs inside a tokio task, so the change is mechanical:

```
// Before (M2)
fn dispatch(cmd: &RedisCommand, state: &mut ConnectionState) -> CommandResponse

// After (M4)
async fn dispatch(cmd: &RedisCommand, state: &mut ConnectionState) -> CommandResponse
```

All existing handlers (PING, ECHO, HELLO, QUIT, COMMAND, CLIENT) remain synchronous internally — they just gain an `async` signature with no `.await` points.

### 2.2 ConnectionState Gets Storage Handles

`ConnectionState` currently holds only `protocol_version: u8` and `selected_db: u8`. It needs access to FDB:

```rust
pub struct ConnectionState {
    pub protocol_version: u8,
    pub selected_db: u8,
    pub db: Database,
    pub dirs: Directories,           // For the current namespace
    pub client_name: Option<String>,
    pub connection_id: u64,
}
```

`Directories` is opened once per namespace. If `SELECT` changes the namespace (M5), a new `Directories` is opened lazily. For M4, all connections use namespace 0.

The `Database` handle is cheap to clone (it wraps `Arc<foundationdb::Database>`). Each connection gets its own clone.

### 2.3 New Module: `commands/strings.rs`

All 19 string command handlers live here. Each handler is an async function:

```rust
pub async fn handle_get(args: &[Bytes], state: &ConnectionState) -> RespValue
pub async fn handle_set(args: &[Bytes], state: &ConnectionState) -> RespValue
// ...etc
```

Command dispatch in `commands/mod.rs` routes by name:

```rust
b"GET" => CommandResponse::Reply(strings::handle_get(&cmd.args, state).await),
b"SET" => CommandResponse::Reply(strings::handle_set(&cmd.args, state).await),
```

### 2.4 Storage Helpers

Inspired by the bluesky-social/kvdb Go implementation's `getObject()`/`writeObject()`/`deleteObject()` pattern, we add high-level helpers that encapsulate the meta-read + type-check + chunk-read/write cycle. These live alongside the existing storage modules (not in a separate file — they're methods and free functions within the existing `meta.rs` and `chunking.rs` or a new thin `helpers.rs`).

```rust
/// Read a string value. Returns None if key doesn't exist or is expired.
/// Returns Err(WrongType) if key exists with a non-String type.
pub async fn get_string(
    tr: &Transaction, dirs: &Directories, key: &[u8], now_ms: u64,
) -> Result<Option<Vec<u8>>, CommandError>

/// Write a string value with optional expiry. Overwrites any existing key
/// (including non-String types — SET always overwrites per Redis semantics).
pub async fn write_string(
    tr: &Transaction, dirs: &Directories, key: &[u8], data: &[u8],
    expires_at_ms: u64, keep_ttl: bool,
) -> Result<Option<Vec<u8>>, CommandError>
// Returns the old value if the caller needs it (SET GET flag, GETDEL).

/// Delete a key of any type. Removes meta, all data chunks, and expiry entry.
/// Returns true if the key existed (was actually deleted).
pub async fn delete_object(
    tr: &Transaction, dirs: &Directories, key: &[u8], now_ms: u64,
) -> Result<bool, CommandError>
```

These helpers keep individual command handlers thin — they validate args, parse flags, call a helper, format the response.

## 3. Command Specifications

### 3.1 GET

```
GET key
```

- Read ObjectMeta with lazy expiry check
- If key doesn't exist or expired: return nil (BulkString(None))
- If key exists but wrong type: return WRONGTYPE error
- Read chunks, return as BulkString

### 3.2 SET

```
SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds] [KEEPTTL]
```

**Flag parsing**: Parse into a `SetOptions` struct. Validate mutual exclusions:
- NX and XX are mutually exclusive
- EX, PX, EXAT, PXAT are mutually exclusive (at most one)
- KEEPTTL is mutually exclusive with EX/PX/EXAT/PXAT

```rust
struct SetOptions {
    condition: Option<SetCondition>,  // NX or XX
    expiry: Option<Expiry>,           // EX/PX/EXAT/PXAT
    keep_ttl: bool,
    get: bool,                        // Return old value
}

enum SetCondition { Nx, Xx }

enum Expiry {
    Ex(u64),       // seconds from now
    Px(u64),       // milliseconds from now
    ExAt(u64),     // absolute seconds
    PxAt(u64),     // absolute milliseconds
}
```

**Logic within a single FDB transaction**:
1. Read existing ObjectMeta (with expiry check)
2. If NX and key exists: return nil (no-op). If XX and key doesn't exist: return nil.
3. If GET flag: read the old value (for the response). If old value exists but wrong type, return WRONGTYPE error.
4. If overwriting a non-String key: delete old data (different subspace — e.g., hash fields, set members)
5. Compute `expires_at_ms`:
   - If KEEPTTL and old meta exists: preserve `old_meta.expires_at_ms`
   - If EX: `now_ms + seconds * 1000`
   - If PX: `now_ms + milliseconds`
   - If EXAT: `seconds * 1000`
   - If PXAT: `milliseconds`
   - Otherwise: 0 (no expiry)
6. Write new ObjectMeta (type: String, num_chunks, size_bytes, expires_at_ms)
7. Write value chunks
8. If expires_at_ms > 0: write to expire/ directory. If expires_at_ms == 0 and old had expiry: clear expire/ entry.
9. Return: if GET flag, return old value (or nil). Otherwise, return OK.

**Note on SET GET + NX**: Per Redis 7.4+ semantics, `SET key value NX GET` returns the old value if the key already exists (the SET is a no-op but GET still reads). If the key does not exist, the SET succeeds and GET returns nil (there was no old value). Without GET, `SET key value NX` returns nil on no-op and OK on success.

**Note on SET overwriting non-String types**: Per Redis semantics, bare `SET key value` (without NX/XX) unconditionally overwrites any existing key regardless of type. If overwriting a Hash/Set/List/etc., the old data must be fully cleaned up (meta + type-specific data + expiry). Only GET-preflight and explicit reads (GET, INCR, APPEND, etc.) enforce WRONGTYPE.

### 3.3 SETNX / SETEX / PSETEX

Sugar commands that delegate to SET logic:

```
SETNX key value        → SET key value NX  (returns 1 if set, 0 if not)
SETEX key seconds val  → SET key val EX seconds  (returns OK or error)
PSETEX key ms val      → SET key val PX ms  (returns OK or error)
```

SETNX returns an integer (1/0), not OK/nil like SET NX. This matters for the redis-rs client.

### 3.4 GETDEL

```
GETDEL key
```

Single transaction: read value + delete. Returns the value or nil. WRONGTYPE if wrong type.

### 3.5 DEL

```
DEL key [key ...]
```

Type-agnostic. Deletes any key regardless of type. Single transaction for all keys. Returns integer count of keys actually deleted.

For each key: read ObjectMeta, if exists → delete meta + chunks (using `obj_key_range` clear_range) + expire entry. The delete helper must handle all key types — for String, clear obj/ chunks; for future types (Hash, Set, etc.), clear the appropriate subspace ranges.

### 3.6 EXISTS

```
EXISTS key [key ...]
```

Type-agnostic. Returns count of keys that exist (with expiry check). A key specified multiple times is counted multiple times (Redis behavior). Single read transaction.

### 3.7 MGET

```
MGET key [key ...]
```

Returns array of values (or nil for missing/expired/wrong-type keys). All reads in a single FDB transaction. Fire parallel `get()` futures for all keys within the transaction for performance.

Unlike GET, MGET does NOT return WRONGTYPE errors — non-string keys return nil in the array.

### 3.8 MSET

```
MSET key value [key value ...]
```

Requires even number of arguments after command name. Writes all key-value pairs in a single transaction. Always returns OK (never fails on key conflicts — overwrites unconditionally). No expiry support (use SET for that).

### 3.9 INCR / DECR / INCRBY / DECRBY

```
INCR key           → INCRBY key 1
DECR key           → DECRBY key 1
INCRBY key delta
DECRBY key delta
```

All route through a shared `incr_by(tr, dirs, key, delta: i64, now_ms)` helper:

1. Read existing value (or default to "0" if key missing)
2. WRONGTYPE check if key exists and isn't String
3. Parse value as i64: `str::from_utf8(bytes)?.parse::<i64>()?`
4. `checked_add(delta)` — return `ERR increment or decrement would overflow` on overflow (fixes Go v0 bug)
5. Write back as decimal string: `itoa::Buffer::new().format(result).as_bytes()`
6. Update ObjectMeta (num_chunks: 1, size_bytes: result_len)
7. Return new value as Integer

**Error on non-integer**: `ERR value is not an integer or out of range`

### 3.10 INCRBYFLOAT

```
INCRBYFLOAT key increment
```

Similar to INCRBY but with f64:

1. Read existing value (or default to "0")
2. Parse as f64. Accept both integer and float strings.
3. Parse increment as f64.
4. Add. If result is NaN or Inf: return `ERR increment would produce NaN or Infinity`
5. Format result using Redis rules: use `ryu` for formatting, then strip trailing zeros after decimal point. If no decimal point needed, omit it (e.g., `10.5 + 0.5 = "11"` not `"11.0"`).
6. Write back, return as BulkString.

### 3.11 APPEND

```
APPEND key value
```

1. Read existing value (or empty vec if key missing)
2. WRONGTYPE check
3. Concatenate: `existing.extend_from_slice(value)`
4. Write back (new meta + chunks)
5. Return new length as Integer

### 3.12 STRLEN

```
STRLEN key
```

1. Read ObjectMeta only (no chunk read needed)
2. Return `meta.size_bytes` as Integer, or 0 if key missing
3. WRONGTYPE check

### 3.13 GETRANGE

```
GETRANGE key start end
```

1. Read full value
2. Clamp indices to [0, len-1] using Redis rules:
   - Negative indices count from end (-1 = last byte)
   - If start > end after resolution: return empty string
   - If start > len-1: return empty string
   - Clamp end to len-1
3. Return `value[start..=end]` as BulkString

### 3.14 SETRANGE

```
SETRANGE key offset value
```

1. Read existing value (or empty if missing)
2. WRONGTYPE check
3. If offset > current length: zero-pad to offset length
4. Overwrite bytes at `[offset..offset+value.len()]`, extending if needed
5. Write back, return new length as Integer
6. Max string size check: if offset + value.len() > 512MB, return error

## 4. Error Handling

### 4.1 Error Message Table

All commands follow Redis error conventions:

| Error | Message | When |
|-------|---------|------|
| WRONGTYPE | `WRONGTYPE Operation against a key holding the wrong kind of value` | Key exists with non-String type (except DEL, EXISTS, MGET) |
| Wrong arity | `ERR wrong number of arguments for '<cmd>' command` | Bad argument count |
| Not integer | `ERR value is not an integer or out of range` | INCR/DECR on non-numeric value or arg |
| Overflow | `ERR increment or decrement would overflow` | i64 overflow in INCR/DECR |
| Not float | `ERR value is not a valid float` | INCRBYFLOAT parse failure (value or arg) |
| NaN/Inf | `ERR increment would produce NaN or Infinity` | INCRBYFLOAT result is NaN or Inf |
| Syntax | `ERR syntax error` | Invalid SET flag combination or unknown flag |
| Invalid expire | `ERR invalid expire time in 'set' command` | SET EX/PX/EXAT/PXAT with zero or negative value |
| Offset | `ERR offset is out of range` | SETRANGE with negative offset or result > 512MB |
| Not integer (arg) | `ERR value is not an integer or out of range` | Non-integer arg for GETRANGE, SETRANGE, SETEX, PSETEX |
| Key too large | `ERR key is too large (max 8192 bytes)` | Key exceeds `MAX_KEY_SIZE` |

Storage-level errors (FDB timeout, corruption) are logged and returned as `ERR internal error` to avoid leaking internals.

### 4.2 Arity Rules

Every command validates argument count before touching storage. The check must match Redis exactly:

| Command | Expected args | Error on |
|---------|--------------|----------|
| GET | exactly 1 | 0 or 2+ |
| SET | 2+ (key value [flags...]) | 0 or 1 |
| SETNX | exactly 2 | anything else |
| SETEX | exactly 3 (key seconds value) | anything else |
| PSETEX | exactly 3 (key ms value) | anything else |
| GETDEL | exactly 1 | 0 or 2+ |
| DEL | 1+ | 0 |
| EXISTS | 1+ | 0 |
| MGET | 1+ | 0 |
| MSET | 2+ and even count | 0, 1, or odd count |
| INCR | exactly 1 | 0 or 2+ |
| DECR | exactly 1 | 0 or 2+ |
| INCRBY | exactly 2 | anything else |
| DECRBY | exactly 2 | anything else |
| INCRBYFLOAT | exactly 2 | anything else |
| APPEND | exactly 2 | anything else |
| STRLEN | exactly 1 | 0 or 2+ |
| GETRANGE | exactly 3 | anything else |
| SETRANGE | exactly 3 | anything else |

### 4.3 WRONGTYPE Matrix

Commands that enforce String type (return WRONGTYPE if key exists as non-String):

| Enforces WRONGTYPE | Does NOT enforce WRONGTYPE |
|--------------------|---------------------------|
| GET | DEL (type-agnostic, deletes anything) |
| GETDEL | EXISTS (type-agnostic, checks any key) |
| APPEND | MGET (returns nil for non-String, no error) |
| STRLEN | SET without GET flag (overwrites any type) |
| GETRANGE | |
| SETRANGE | |
| INCR, DECR, INCRBY, DECRBY | |
| INCRBYFLOAT | |
| SET with GET flag (reading old value) | |

### 4.4 SET Flag Validation

**Mutually exclusive groups** (combining any two from the same group is `ERR syntax error`):
- Condition: NX, XX
- TTL: EX, PX, EXAT, PXAT, KEEPTTL

**TTL value validation** (checked after flag parsing):
- EX/PX: must be positive integer (> 0). Zero or negative → `ERR invalid expire time in 'set' command`
- EXAT/PXAT: must be positive integer (> 0). Zero or negative → same error
- Non-numeric value for any TTL flag → `ERR value is not an integer or out of range`

**Unknown flags**: Any unrecognized token after key and value → `ERR syntax error`

### 4.5 Integer/Float Parsing Rules

**Integer parsing** (INCR, DECR, INCRBY, DECRBY, GETRANGE indices, SETRANGE offset, SETEX/PSETEX TTL):
- Must be valid UTF-8
- Must parse as i64 with no leading/trailing whitespace
- No leading zeros (except "0" itself)
- Float strings like "3.14" are NOT valid integers
- Empty string is NOT a valid integer
- Values outside i64 range → `ERR value is not an integer or out of range`

**Float parsing** (INCRBYFLOAT):
- Must be valid UTF-8
- Must parse as f64
- Accepts integer strings (e.g., "42") and float strings (e.g., "3.14")
- "inf", "-inf", "infinity", "-infinity" as the increment → `ERR increment would produce NaN or Infinity`
- "nan" as the increment → `ERR increment would produce NaN or Infinity`
- If existing value is "inf"/"-inf" and increment is finite, result is still inf → allowed (Redis allows this)
- If result is NaN or ±Inf (from finite inputs): → `ERR increment would produce NaN or Infinity`

**Overflow boundaries** (INCR/DECRBY):
- `INCR` on value `"9223372036854775807"` (i64::MAX) → `ERR increment or decrement would overflow`
- `DECR` on value `"-9223372036854775808"` (i64::MIN) → same
- `INCRBY key 9223372036854775808` (exceeds i64 range as arg) → `ERR value is not an integer or out of range`
- `INCRBY key 5` on value `"9223372036854775805"` → overflow, error
- `DECRBY key 1` on value `"-9223372036854775808"` → overflow, error

### 4.6 Expiry Interaction with Error Cases

- `GET` on expired key → nil (lazy check deletes meta)
- `EXISTS` on expired key → 0 (lazy check)
- `INCR` on expired key → treats as missing, starts from 0
- `APPEND` on expired key → treats as missing, starts from ""
- `STRLEN` on expired key → 0
- `SET key val KEEPTTL` on expired key → expired key is gone, KEEPTTL has no existing TTL to keep, so no expiry is set
- `SET key val KEEPTTL` on non-existent key → same, no expiry
- `GETDEL` on expired key → nil (key already logically gone)

### 4.7 Empty and Edge Inputs

- Empty key (`""`, zero bytes) → Redis allows this; we allow it too (FDB tuple layer handles empty byte strings)
- Empty value (`SET key ""`) → allowed, creates a 0-byte string, STRLEN returns 0
- `APPEND key ""` → no-op, returns current length
- `SETRANGE key 0 ""` → no-op, returns current length (or 0 if key missing)
- `GETRANGE key 0 -1` on empty string → returns ""
- `MSET` with zero pairs → arity error
- Binary keys/values with null bytes, \r\n → must work correctly (binary-safe)

## 5. Testing Strategy

### 5.1 Integration Tests (`tests/integration/strings.rs` — `just test`)

Focused cases per command group. Each test uses `TestContext::new()` with real FDB.

**SET/GET group** (~6 tests):
- set_and_get: basic round-trip
- get_nonexistent_returns_nil
- set_overwrites_existing
- set_nx_respects_condition (exists and not-exists cases)
- set_xx_respects_condition
- set_get_returns_old_value

**SET TTL group** (~3 tests):
- set_ex_sets_expiry (verify value disappears on lazy expiry check)
- set_px_sets_expiry_milliseconds
- set_keepttl_preserves_existing

**SET error group** (~5 tests):
- set_nx_xx_mutually_exclusive (syntax error)
- set_ex_px_mutually_exclusive (syntax error)
- set_ex_zero_returns_error (invalid expire time)
- set_ex_negative_returns_error (invalid expire time)
- set_unknown_flag_returns_syntax_error

**DEL/EXISTS group** (~5 tests):
- del_removes_key
- del_multiple_keys_returns_count
- del_nonexistent_returns_zero
- exists_returns_count
- exists_counts_duplicates (EXISTS k k k returns 3 if k exists)

**MGET/MSET group** (~4 tests):
- mset_and_mget_roundtrip
- mget_returns_nil_for_missing
- mset_overwrites_existing
- mset_odd_args_returns_error

**INCR/DECR group** (~8 tests):
- incr_creates_key_from_zero
- incr_increments_existing
- incr_returns_error_on_non_integer
- incr_returns_error_on_float_string ("3.14" is not an integer)
- incr_overflow_returns_error (i64::MAX + 1)
- decr_underflow_returns_error (i64::MIN - 1)
- decrby_with_large_delta
- incrby_arg_overflow (arg exceeds i64 range)

**INCRBYFLOAT group** (~4 tests):
- incrbyfloat_basic
- incrbyfloat_on_integer_string
- incrbyfloat_nan_increment_returns_error
- incrbyfloat_inf_increment_returns_error

**String manipulation group** (~7 tests):
- append_to_existing
- append_creates_key
- strlen_returns_length
- strlen_nonexistent_returns_zero
- getrange_with_negative_indices
- getrange_start_greater_than_end_returns_empty
- setrange_pads_with_zeros

**SETRANGE error group** (~2 tests):
- setrange_negative_offset_returns_error
- setrange_noninteger_offset_returns_error

**WRONGTYPE group** (~4 tests):
- get_wrongtype_error (read Hash/Set/List/SortedSet-typed key with GET)
- incr_wrongtype_error
- append_wrongtype_error
- del_works_on_any_type (type-agnostic, no WRONGTYPE)

**Expiry interaction group** (~3 tests):
- get_expired_key_returns_nil
- exists_expired_key_returns_zero
- incr_expired_key_starts_from_zero

**Chunking group** (~2 tests):
- large_value_chunking (500KB value round-trips correctly)
- set_get_returns_old_value_for_large_values

**Arity error group** (~3 tests):
- get_wrong_arity
- set_wrong_arity
- incr_wrong_arity

**Sugar commands** (~3 tests):
- setnx_returns_integer
- setex_basic
- psetex_basic
- getdel_returns_and_deletes

**Total: ~60 tests in `just test`**

### 5.2 Acceptance Tests (`tests/accept/strings.rs` — `just accept`)

Property-based and randomized testing. Thousands of cases.

**Property tests (proptest)**:
- `set_get_roundtrip`: SET(k,v); GET(k) == v for arbitrary binary keys (1-100 bytes) and values (0-10KB)
- `incr_commutativity`: INCRBY(a) then INCRBY(b) == INCRBY(b) then INCRBY(a)
- `append_concatenation`: APPEND(k, a); APPEND(k, b); GET(k) == a || b
- `strlen_matches_value_length`: SET(k,v); STRLEN(k) == len(v)
- `getrange_substring`: GETRANGE matches Rust slice for in-bounds indices
- `mset_mget_consistency`: MSET(pairs); MGET(keys) == values in order

**Randomized command sequences**:
- Generate 1000 random operations (SET, GET, DEL, INCR, APPEND, GETRANGE, SETRANGE, STRLEN, EXISTS) on a key space of 10 keys
- Maintain an in-memory HashMap model
- After each operation, verify the server's response matches the model
- Covers interaction between commands (e.g., INCR after APPEND of numeric string)

**Chunking boundary tests**:
- Values at exactly 99,999, 100,000, 100,001, 200,000, 300,001 bytes
- SET, GET, verify byte-for-byte equality

**SET flag matrix**:
- All valid flag combinations: NX, XX, EX, PX, EXAT, PXAT, KEEPTTL, GET
- Invalid combinations return syntax error

**Error case matrix** (exhaustive, run in `just accept`):
- All arity errors for all 19 commands
- SET flag mutual exclusion: NX+XX, EX+PX, EX+EXAT, PX+PXAT, EX+KEEPTTL, etc. (all pairs)
- SET TTL with zero, negative, non-integer, very large values
- INCR/DECR on: float string, non-numeric string, empty string, whitespace-padded number, i64::MAX, i64::MIN
- INCRBYFLOAT with: "nan", "inf", "-inf", non-numeric, result producing NaN/Inf
- SETRANGE with: negative offset, very large offset, non-integer offset
- GETRANGE with: non-integer start/end
- WRONGTYPE for every string-specific command against every non-String key type (Hash, Set, SortedSet, List)

### 5.3 Benchmarks (`benches/commands.rs`)

Add string command benchmarks measuring full-stack latency (client → RESP parse → dispatch → FDB transaction → response):

- `string/set_get_small` — SET + GET of 64-byte value
- `string/set_get_1kb` — SET + GET of 1KB value
- `string/set_get_100kb` — SET + GET of 100KB value (1 chunk)
- `string/incr` — INCR on existing counter
- `string/mset_mget_10` — MSET + MGET of 10 keys

### 5.4 Smoke Test

`just smoke` starts the server, runs a redis-cli script, and verifies the output. The script exercises both happy paths and error paths to confirm the server handles bad input gracefully (returns RESP errors, doesn't crash, doesn't hang).

**Happy path coverage:**
```
SET greeting "hello world"        → OK
GET greeting                      → "hello world"
SET counter 0                     → OK
INCR counter                      → 1
INCRBY counter 41                 → 42
GET counter                       → "42"
DECR counter                      → 41
DECRBY counter 40                 → 1
APPEND greeting " from kvdb"      → 21
GET greeting                      → "hello world from kvdb"
STRLEN greeting                   → 21
GETRANGE greeting 0 4             → "hello"
SETRANGE greeting 0 "HELLO"       → 21
MSET a 1 b 2 c 3                  → OK
MGET a b c                        → "1" "2" "3"
SETNX newkey "value"              → 1
SETNX newkey "other"              → 0
DEL a b c newkey                  → 4
EXISTS a b c                      → 0
INCRBYFLOAT pi 3.14159            → "3.14159"
```

**Error path coverage** (server must return error, not crash):
```
GET                               → ERR wrong number of arguments
GET a b                           → ERR wrong number of arguments
SET                               → ERR wrong number of arguments
SET onlykey                       → ERR wrong number of arguments
SET key val NX XX                 → ERR syntax error
SET key val EX 0                  → ERR invalid expire time
SET key val EX -5                 → ERR invalid expire time
SET key val EX notanumber         → ERR value is not an integer
SET key val EX 10 PX 10000        → ERR syntax error
SET key val BADOPTION             → ERR syntax error
INCR greeting                     → ERR value is not an integer (greeting = "HELLO world...")
INCRBYFLOAT counter nan           → ERR increment would produce NaN or Infinity
INCRBYFLOAT counter inf           → ERR increment would produce NaN or Infinity
MSET a                            → ERR wrong number of arguments
DEL                               → ERR wrong number of arguments
SETRANGE greeting -1 "x"          → ERR offset is out of range
GETRANGE greeting a b             → ERR value is not an integer
```

**Crash resilience** (send garbage, server stays alive):
```
<send valid PING>                 → PONG (server still responsive)
<send FAKECMD>                    → ERR unknown command
<send valid PING>                 → PONG (server still responsive after error)
```

The smoke test script asserts both the content of expected responses and that the server process is still alive after each error case. If the server exits or hangs at any point, the smoke test fails.

### 5.5 Fuzz Target

Add `fuzz/fuzz_targets/resp_command.rs` — generates structured command sequences (valid command names with random args) and feeds them to dispatch. Asserts no panics.

## 6. Performance Considerations

- **One FDB transaction per command** (except MGET/MSET which batch). This is the simple, correct default.
- **Parallel chunk reads** in `read_chunks()` — already implemented in M3.
- **INCR uses read-modify-write**, not FDB atomic ops. This is correct for Redis compatibility (handles `SET counter "42"; INCR counter`). Concurrent INCRs on the same key will conflict and retry via FDB's optimistic concurrency. This is acceptable for M4; atomic ops can be added as a future optimization if profiling shows contention.
- **MGET fires parallel gets** within a single transaction for all keys.
- **STRLEN avoids reading chunks** — only reads ObjectMeta for `size_bytes`.
- **itoa/ryu for formatting** — avoids heap allocation in INCR/INCRBYFLOAT response formatting.

## 7. Forward-Looking Notes

- **M5 (Key Management & TTL)** will add EXPIRE, TTL, PERSIST, TYPE, RENAME, SELECT, FLUSHDB, and the background expiry worker. M4 writes expiry data to both ObjectMeta and expire/ directory, so M5 just needs to read it.
- **M7 (Sets)** will use RoaringBitmaps with UID indirection (member → u64 UID → bitmap position), following the bluesky-social/kvdb Go implementation's pattern. The `delete_object()` helper from M4 needs to be extensible for cleaning up bitmap + UID data in M7.
- **M10 (Transactions)** will wrap multiple commands in a single FDB transaction. The command handlers must be designed so they can operate on a caller-provided transaction rather than always creating their own via `run_transact()`. This means the core logic should accept a `&Transaction` parameter, with `run_transact()` as the outer wrapper for single-command use.

## 8. Exit Criteria

- [ ] All 19 string commands implemented and passing
- [ ] `just test` passes ~60 integration tests in < 2 seconds
- [ ] `just accept` passes all acceptance tests in < 15 seconds
- [ ] `redis-cli` interactive use works (SET, GET, INCR, DEL, etc.)
- [ ] WRONGTYPE enforcement for every string-specific command against every non-String type
- [ ] Chunking works for values up to 500KB+
- [ ] Integer overflow in INCR/DECR returns proper error (i64::MAX, i64::MIN boundaries)
- [ ] SET flag matrix fully tested — all valid combos and all invalid mutual exclusions
- [ ] SET TTL validation: zero, negative, non-integer all return proper errors
- [ ] Arity errors for all 19 commands
- [ ] Expiry interaction: expired keys return nil/0/start-from-zero as appropriate
- [ ] Error case acceptance matrix: all parsing, overflow, WRONGTYPE, flag combos
- [ ] Prior M0-M3 tests still pass (no regressions)
- [ ] `just lint` clean (zero warnings)
