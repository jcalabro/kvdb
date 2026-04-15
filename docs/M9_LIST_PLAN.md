# Milestone 9: List Commands — Implementation Plan

## Commands

LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LTRIM, LREM, LPUSHX, RPUSHX, LINSERT, LPOS.

**Deferred to M9.5:** LMOVE, LMPOP (cross-key atomicity commands).

## Design Decision: Index-Based Storage

The alpha (Go) version uses a **doubly-linked list** with UID-based pointers. Each list item gets a unique ID, and items store `next`/`previous` pointers. This requires O(n) traversal for LINDEX and LRANGE — every access chases pointers through FDB reads.

The Rust version already has the infrastructure for an **index-based** approach:
- `ObjectMeta` has `list_head: i64`, `list_tail: i64`, `list_length: u64` (meta.rs:73-75)
- FDB key layout: `list/<key, index>` where index is an i64 (directories.rs:12)
- Cleanup logic in `delete_data_for_meta` already handles `KeyType::List` (helpers.rs:164-168)

**Why index-based is better for FDB:**

| Operation | Linked-list (alpha) | Index-based (this plan) |
|-----------|-------------------|------------------------|
| LPUSH/RPUSH | O(1) but 3+ FDB reads (meta + head/tail + pointer updates) | O(1), 1 FDB write + meta update |
| LPOP/RPOP | O(1) but 3+ FDB reads | O(1), 1 FDB read + 1 clear + meta update |
| LINDEX | O(n) pointer traversal | O(1) direct key lookup |
| LRANGE | O(k+n) — traverse to start, then read k | O(k) range read from FDB |
| LLEN | O(1) meta read | O(1) meta read |
| LINSERT/LREM | O(n) traversal + pointer surgery | O(n) scan + shift (same asymptotic) |

The index-based approach is a strict improvement: O(1) random access, O(k) range reads, and the same O(n) worst case for the inherently-linear operations. It also uses fewer FDB reads per operation (no pointer chasing).

### Index Scheme

Elements are stored at integer indices in the `list/<key, index>` FDB keyspace, where `index` is a signed 64-bit integer. `list_head` and `list_tail` are both **inclusive** bounds.

```
list_head = -2, list_tail = 1, list_length = 4

  FDB index:  -2   -1    0    1
  Logical:     0    1    2    3
  Value:      "c"  "b"  "a"  "d"
```

**Conventions:**
- Fresh list, first element: `head = 0, tail = 0, length = 1`
- LPUSH: `head -= 1`, write at new head
- RPUSH: `tail += 1`, write at new tail
- LPOP: read + clear at head, `head += 1`, `length -= 1`
- RPOP: read + clear at tail, `tail -= 1`, `length -= 1`
- Logical index `i` maps to FDB index `head + i`
- Negative logical index `i` maps to FDB index `head + length + i`
- Empty list: delete the ObjectMeta entirely (Redis semantics: empty lists don't exist)

**Invariant:** `list_length == list_tail - list_head + 1` (when length > 0).

### LPUSH element ordering

Redis LPUSH pushes elements left-to-right, so `LPUSH key a b c` yields `[c, b, a]`:
- Push `a`: head=0, write index 0
- Push `b`: head=-1, write index -1
- Push `c`: head=-2, write index -2

Each element in the variadic args decrements head by 1.

---

## File Layout

| New file | Purpose |
|----------|---------|
| `src/commands/lists.rs` | All 14 list command handlers (LMOVE/LMPOP deferred to M9.5) |
| `tests/lists.rs` | Integration tests (just test) |
| `tests/accept_lists.rs` | Acceptance tests (just accept) |

| Modified file | Change |
|---------------|--------|
| `src/commands/mod.rs` | Add `pub mod lists;` + 14 dispatch entries |
| `src/storage/meta.rs` | Add `ObjectMeta::new_list()` constructor |
| `examples/smoke.rs` | Add list validation checks |

---

## Implementation Phases

### Phase 1: Core Push/Pop/Length (6 commands)

These are the fundamental operations — O(1) per element.

#### LPUSH key element [element ...]

```
Arity: >= 2 args (key + at least one element)
Returns: Integer (new list length)
```

1. Read meta (raw, no expiry filter) for type check
2. If live and wrong type -> WRONGTYPE
3. If expired or wrong type -> delete_all_data_and_meta
4. Get existing head/tail/length (or defaults: head=0, tail=-1, length=0 for new list)
5. For each element (left-to-right):
   - `head -= 1`
   - `tr.set(dirs.list.pack(&(key, head)), element)`
6. `length += num_elements`
7. Write updated ObjectMeta (type=List, list_head, list_tail, list_length)
8. Return new length

Note on fresh list defaults: `head=0, tail=-1` so that the first LPUSH sets `head=-1, tail=-1` (but see RPUSH below — we need to handle the "first element" case specially).

**Correction — fresh list handling:** When the list doesn't exist yet:
- LPUSH: first element goes at index 0. Subsequent elements in the same call decrement from there. So: `head = 1 - num_elements, tail = 0`.
- Actually simpler: start with `head = 0, tail = -1, length = 0` as sentinel. For LPUSH, each element does `head -= 1`, write at `head`. After all pushes on an empty list: head = -N+1... no that's wrong.

**Cleaner approach for fresh lists:**
- If list doesn't exist, treat as `head = 0, tail = -1, length = 0`
- LPUSH each element: `head -= 1`, write at `head`
- After LPUSH of N elements on empty: head = -N, tail = -1
- But then LINDEX 0 = element at head = -N, which is the LAST pushed element (correct — Redis pushes left-to-right, so last arg is at index 0)

Wait, that gives tail = -1 which means there's nothing at index -1. Let me reconsider.

**Final approach — use an explicit "is new" check:**
- If list is new: first element goes at index 0, set head=0, tail=0
  - Remaining LPUSH elements: head -= 1 for each
  - Remaining RPUSH elements: tail += 1 for each
- If list exists: LPUSH decrements head, RPUSH increments tail

For `LPUSH key a b c` on a new list:
- Push `a` (first): head=0, tail=0, write index 0
- Push `b`: head=-1, write index -1
- Push `c`: head=-2, write index -2
- Final: head=-2, tail=0, length=3
- LRANGE 0 -1 = indices -2,-1,0 = c,b,a (correct)

For `RPUSH key a b c` on a new list:
- Push `a` (first): head=0, tail=0, write index 0
- Push `b`: tail=1, write index 1
- Push `c`: tail=2, write index 2
- Final: head=0, tail=2, length=3
- LRANGE 0 -1 = indices 0,1,2 = a,b,c (correct)

#### RPUSH key element [element ...]

```
Arity: >= 2 args
Returns: Integer (new list length)
```

Mirror of LPUSH but increments tail instead of decrementing head.

#### LPOP key [count]

```
Arity: 1-2 args
Returns: BulkString (single pop) or Array (with count) or Nil (empty/missing)
```

1. Read meta, type check
2. If list doesn't exist -> Nil
3. If count arg: parse as positive integer, clamp to list_length
4. For each pop: read value at `head`, clear that key, `head += 1`, `length -= 1`
5. If length reaches 0: delete meta + expire entry (Redis: empty list = nonexistent key)
6. Return value(s)

#### RPOP key [count]

Mirror of LPOP but reads/clears at tail, decrements tail.

#### LLEN key

```
Arity: 1 arg
Returns: Integer (list length, 0 if key doesn't exist)
```

1. Read meta with expiry check
2. Type check (WRONGTYPE if exists but not list)
3. Return list_length (or 0 if key doesn't exist)

#### LINDEX key index

```
Arity: 2 args
Returns: BulkString or Nil (out of bounds)
```

1. Read meta with expiry check, type check
2. Parse index as i64 (supports negative)
3. Convert to FDB index: if index >= 0, fdb_idx = head + index; if < 0, fdb_idx = head + length + index
4. Bounds check: fdb_idx must be in [head, tail]
5. Single `tr.get(dirs.list.pack(&(key, fdb_idx)))` — O(1)
6. Return value or Nil

### Phase 2: Range & Conditional Operations (4 commands)

#### LRANGE key start stop

```
Arity: 3 args
Returns: Array of BulkStrings (may be empty)
```

1. Read meta, type check
2. Parse start/stop as i64 (supports negative)
3. Normalize indices (Redis clamping rules):
   - Negative indices: `idx += length`
   - Clamp start to `max(0, start)`, stop to `min(length-1, stop)`
   - If start > stop after clamping: return empty array
4. Convert to FDB indices: `fdb_start = head + start`, `fdb_stop = head + stop`
5. Range read from `dirs.list.pack(&(key, fdb_start))` to `dirs.list.pack(&(key, fdb_stop + 1))` (exclusive end per FDB range semantics — use `pack` with stop+1 or use `strinc`)
6. Return collected values in order

**FDB range read pattern:** Use the paginated range read pattern from `read_set_members()` in sets.rs. But since list indices are contiguous, we can also fire parallel point reads for small ranges and use range reads for large ones. Range read is simpler and sufficient.

#### LSET key index value

```
Arity: 3 args
Returns: OK or error (out of range, no such key)
```

1. Read meta, type check
2. If key doesn't exist: "ERR no such key"
3. Parse index, convert to FDB index
4. Bounds check: "ERR index out of range" if out of bounds
5. `tr.set(dirs.list.pack(&(key, fdb_idx)), value)`
6. Return OK

#### LTRIM key start stop

```
Arity: 3 args
Returns: OK (always succeeds)
```

1. Read meta, type check
2. If key doesn't exist: return OK (noop)
3. Normalize start/stop (same clamping as LRANGE)
4. If resulting range is empty (start > stop): delete entire list
5. Otherwise:
   - Clear range [head, head+start) — elements before the new start
   - Clear range [head+stop+1, tail+1) — elements after the new stop
   - Update meta: head = head+start, tail = head+stop, length = stop - start + 1
6. Return OK

#### LPUSHX / RPUSHX key element [element ...]

```
Arity: >= 2 args
Returns: Integer (new length, 0 if key doesn't exist)
```

Same as LPUSH/RPUSH but only if the key already exists AND is a list. Return 0 if the key doesn't exist.

### Phase 3: Scan & Modify Operations (3 commands)

These are O(n) operations that require reading/writing multiple elements.

#### LREM key count element

```
Arity: 3 args
Returns: Integer (number of removed elements)
```

Strategy: read the full list, identify elements to remove, compact remaining elements.

1. Read meta, type check
2. Parse count as i64
3. Read all elements via FDB range read (head to tail)
4. Determine which to remove:
   - count > 0: first `count` matches, scanning head-to-tail
   - count < 0: first `|count|` matches, scanning tail-to-head
   - count = 0: all matches
5. If nothing to remove: return 0
6. Build new element list (excluding removed elements), preserving order
7. Clear entire old range: `tr.clear_range(list_range)`
8. Write new elements at contiguous indices starting from 0 (re-index)
9. Update meta: head=0, tail=new_length-1, length=new_length
10. If new_length == 0: delete key entirely
11. Return count of removed elements

**Why re-index from 0:** After removal, we have gaps. Rather than complex gap management, we compact to [0, N-1]. This is clean and avoids index drift. The cost is O(n) writes, but LREM is already O(n).

#### LINSERT key BEFORE|AFTER pivot element

```
Arity: 4 args
Returns: Integer (new length, or -1 if pivot not found, 0 if key doesn't exist)
```

1. Read meta, type check
2. If key doesn't exist: return 0
3. Read all elements via range read
4. Find pivot value (linear scan)
5. If not found: return -1
6. Insert new element before/after pivot position in the collected vec
7. Clear old range, rewrite from 0 (same compact-rewrite as LREM)
8. Update meta
9. Return new length

#### LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]

```
Arity: >= 2 args (complex optional args)
Returns: Integer (single match) or Array (with COUNT) or Nil (not found)
```

1. Read meta, type check
2. Parse optional args: RANK (default 1), COUNT (default 1, 0=all), MAXLEN (default 0=no limit)
3. Read elements via range read
4. Scan elements (head-to-tail if RANK > 0, tail-to-head if RANK < 0)
5. Track match positions
6. Apply RANK (skip first `|rank|-1` matches) and COUNT (collect up to count matches)
7. Apply MAXLEN (stop scanning after maxlen elements)
8. Return position(s) or Nil

#### LMOVE, LMPOP — Deferred to M9.5

These cross-key commands (LMOVE pops from one list and pushes to another; LMPOP scans multiple keys for the first non-empty list) are deferred to M9.5. FDB makes them trivially atomic within a single transaction, but they're more complex to implement and were added to Redis relatively late (6.2+). The shared helpers built in M9 (meta reading, push/pop logic) will make M9.5 straightforward.

---

## Shared Helpers (in lists.rs)

These internal helpers reduce duplication across the 16 handlers:

```rust
/// Read meta for a list key, performing type check and expiry handling.
/// Returns (Option<ObjectMeta>, head, tail, length, expires_at_ms).
/// The ObjectMeta is None for nonexistent/expired keys.
async fn read_list_meta(tr, dirs, key) -> Result<ListMeta, FdbBindingError>

/// Convert a logical Redis index to an FDB index.
/// Handles negative indices and returns None for out-of-bounds.
fn resolve_index(head: i64, length: u64, index: i64) -> Option<i64>

/// Normalize a start..stop range per Redis clamping rules.
/// Returns (clamped_start, clamped_stop) as logical indices,
/// or None if the range is empty.
fn normalize_range(length: u64, start: i64, stop: i64) -> Option<(u64, u64)>

/// Read all elements from a list via paginated FDB range read.
/// Returns Vec<(i64, Vec<u8>)> of (fdb_index, value) pairs.
async fn read_all_elements(tr, dirs, key, head, tail) -> Result<Vec<...>>

/// Read a contiguous range of elements.
async fn read_element_range(tr, dirs, key, fdb_start, fdb_end_inclusive) -> Result<Vec<Vec<u8>>>

/// Rewrite a list from a Vec of elements, compacting indices to [0, N-1].
/// Clears the old range first.
fn compact_rewrite(tr, dirs, key, elements: &[Vec<u8>]) -> (i64, i64, u64)

/// Build and write ObjectMeta for a list.
fn write_list_meta(tr, dirs, key, head, tail, length, expires_at_ms) -> Result<()>

/// Delete list entirely (data + meta + expire).
fn delete_list(tr, dirs, key, meta) -> Result<()>
```

---

## Testing Strategy

### Integration Tests (`tests/lists.rs`, runs via `just test`)

Target: 3-5 tests per command, covering happy path, error path, one edge case.

**Core operations:**
- `lpush_rpush_basic` — LPUSH/RPUSH create list, LLEN confirms count
- `lpush_ordering` — LPUSH a b c yields [c, b, a] via LRANGE
- `rpush_ordering` — RPUSH a b c yields [a, b, c] via LRANGE
- `lpop_rpop_basic` — pop returns correct elements from each end
- `lpop_rpop_with_count` — pop with count argument
- `lpop_rpop_empty` — pop on nonexistent key returns Nil
- `llen_nonexistent` — returns 0
- `llen_wrongtype` — WRONGTYPE on string key
- `lindex_positive_and_negative` — both positive and negative indices work
- `lindex_out_of_bounds` — returns Nil

**Range & conditional:**
- `lrange_full` — LRANGE 0 -1 returns all elements
- `lrange_subset` — LRANGE with specific bounds
- `lrange_out_of_bounds_clamps` — oversized bounds clamp correctly
- `lrange_empty_range` — start > stop returns empty array
- `lset_basic` — overwrite an element, verify with LINDEX
- `lset_out_of_range` — error on invalid index
- `lset_negative_index` — negative index works
- `ltrim_basic` — trim to subset, verify length and content
- `ltrim_empty_deletes_key` — trim to nothing removes the key
- `lpushx_rpushx_existing` — pushes when key exists
- `lpushx_rpushx_nonexistent` — returns 0, key not created

**Scan & modify:**
- `lrem_positive_count` — removes first N matches from head
- `lrem_negative_count` — removes first N matches from tail
- `lrem_zero_count` — removes all matches
- `linsert_before_after` — insert before/after a pivot element
- `linsert_missing_pivot` — returns -1 when pivot not found
- `lpos_basic` — find element position
- `lpos_with_rank_and_count` — RANK and COUNT options
**Cross-type enforcement:**
- `wrongtype_on_string_key` — list commands on a string key return WRONGTYPE
- `wrongtype_on_hash_key` — list commands on a hash key return WRONGTYPE

### Acceptance Tests (`tests/accept_lists.rs`, runs via `just accept`)

Target: property-based invariant checks + randomized model comparison.

**Property tests (proptest):**

1. **LPUSH/RPUSH round-trip** (100 cases):
   - Push random binary data, verify LINDEX retrieval

2. **LLEN == LPUSH count** (100 cases):
   - LPUSH N random elements, verify LLEN == N

3. **LPUSH then LPOP returns same element** (100 cases):
   - LPUSH single element, LPOP returns it, list is empty

4. **RPUSH then RPOP returns same element** (100 cases)

5. **LRANGE returns all after RPUSH** (50 cases):
   - RPUSH N elements, LRANGE 0 -1 matches input order

**Model comparison (randomized sequences):**

6. **VecDeque model** (50 cases):
   - Generate random sequences of LPUSH/RPUSH/LPOP/RPOP/LLEN/LINDEX
   - Execute against both kvdb and an in-memory VecDeque
   - Assert all return values match
   - Include 5K+ operations per sequence

7. **LREM correctness** (50 cases):
   - RPUSH random elements (with duplicates), LREM with random count
   - Verify result matches equivalent Vec::retain logic

8. **LTRIM idempotence** (50 cases):
   - RPUSH elements, LTRIM to a range
   - Verify LRANGE matches expected subset
   - Second LTRIM to same range is idempotent

9. **LRANGE with 5K elements** (10 cases):
   - Verify FDB pagination works for large lists

### Smoke Tests (additions to `examples/smoke.rs`)

Add ~18 validation checks covering:
- LPUSH/RPUSH/LPOP/RPOP happy path
- LLEN/LINDEX/LRANGE basics
- LSET/LTRIM/LREM/LINSERT/LPOS basics
- LPUSHX/RPUSHX conditional behavior
- WRONGTYPE enforcement
- Empty list auto-deletion after final pop

---

## Potential Risks & Mitigations

### FDB transaction limits on LINSERT/LREM with large lists

LINSERT and LREM do compact-rewrite: read all elements, modify, clear range, rewrite. For a 50K-element list with 100-byte elements, that's ~5MB of reads + 5MB of writes = 10MB — right at FDB's transaction size limit.

**Mitigation:** This matches Redis behavior (LINSERT/LREM are O(n)). Users with very large lists should avoid these commands, same as with Redis. We could add a size check and return an error if the rewrite would exceed FDB limits, but that diverges from Redis semantics. Better to let FDB's own transaction_too_large error propagate with a clear error message.

### LPOP/RPOP with count=0

Redis quirk: `LPOP key 0` returns an empty array (not Nil). `LPOP key` (without count) returns a single bulk string or Nil. These are different return types. Handle by checking whether the count argument was provided.

### Empty list cleanup

Redis invariant: lists with zero elements don't exist. After any operation that might empty a list (LPOP, RPOP, LTRIM, LREM), check if length == 0 and delete the key entirely (meta + expire entry).

---

## Implementation Order

1. **`ObjectMeta::new_list()`** constructor in meta.rs
2. **`src/commands/lists.rs`** — shared helpers first, then commands in this order:
   - Phase 1: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX
   - Phase 2: LRANGE, LSET, LTRIM, LPUSHX, RPUSHX
   - Phase 3: LREM, LINSERT, LPOS
3. **`src/commands/mod.rs`** — wire up dispatch (after each phase)
4. **`tests/lists.rs`** — integration tests (after each phase)
5. **`tests/accept_lists.rs`** — acceptance tests (after all phases)
6. **`examples/smoke.rs`** — smoke test additions
7. Final: `just ci` passes, `just test` under 2 seconds, `just accept` under 15 seconds

---

## References

- Alpha (Go) implementation: `/Users/jcalabro/go/src/github.com/bluesky-social/kvdb/internal/server/redis/list.go`
  - Linked-list approach with UID allocation, pointer-chasing traversal
  - Only implements LPUSH, RPUSH, LLEN, LINDEX (4 of 14 M9 commands)
  - LINDEX is O(n) due to linked-list traversal — our index-based approach is O(1)
- Rust codebase patterns: `src/commands/hashes.rs` (type check + transact pattern), `src/commands/sets.rs` (range read pagination)
- FDB key layout: `src/storage/directories.rs` — `list/<key, index>` already defined
- ObjectMeta: `src/storage/meta.rs` — `list_head`, `list_tail`, `list_length` fields already present
