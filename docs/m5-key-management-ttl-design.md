# M5: Key Management & TTL — Design Spec

## Summary

18 commands plus a background expiry worker. Adds TTL manipulation (EXPIRE, TTL, PERSIST, etc.), key introspection (TYPE, RENAME, DBSIZE), database commands (SELECT, FLUSHDB, FLUSHALL), and active background expiration.

## Commands

### TTL Commands (9)

All TTL commands operate on ObjectMeta's `expires_at_ms` field and the `expire/<key>` directory entry. Timestamps are always stored as absolute milliseconds since Unix epoch.

| Command | Args | Behavior | Return |
|---------|------|----------|--------|
| EXPIRE | key seconds | Set TTL in seconds (relative) | 1 if set, 0 if key doesn't exist |
| PEXPIRE | key milliseconds | Set TTL in milliseconds (relative) | 1 if set, 0 if key doesn't exist |
| EXPIREAT | key timestamp | Set TTL as Unix timestamp (seconds) | 1 if set, 0 if key doesn't exist |
| PEXPIREAT | key ms-timestamp | Set TTL as Unix timestamp (ms) | 1 if set, 0 if key doesn't exist |
| TTL | key | Get remaining TTL in seconds | -2 if key doesn't exist, -1 if no expiry, else seconds remaining |
| PTTL | key | Get remaining TTL in milliseconds | -2 if key doesn't exist, -1 if no expiry, else ms remaining |
| PERSIST | key | Remove TTL | 1 if TTL was removed, 0 if key doesn't exist or had no TTL |
| EXPIRETIME | key | Get absolute expiry as Unix timestamp (seconds) | -2 if key doesn't exist, -1 if no expiry, else timestamp |
| PEXPIRETIME | key | Get absolute expiry as Unix timestamp (ms) | -2 if key doesn't exist, -1 if no expiry, else timestamp |

**Implementation pattern for EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT:**
1. Read ObjectMeta with `now_ms` for lazy expiry check (expired keys return 0, matching Redis)
2. If key doesn't exist (or is expired), return 0
3. Compute absolute `expires_at_ms` from the argument
4. Update `ObjectMeta.expires_at_ms`, write meta
5. Write `expire/<key>` entry with the timestamp
6. Return 1

**Implementation pattern for TTL/PTTL (read-only):**
1. Read ObjectMeta with `now_ms` for lazy expiry check
2. If key doesn't exist (or is expired), return -2
3. If `expires_at_ms == 0`, return -1
4. Compute remaining time: `expires_at_ms - now_ms` (convert to seconds for TTL)
5. Return the value (integer division for TTL: `remaining_ms / 1000`, so sub-second returns 0, matching Redis)

**Implementation pattern for PERSIST:**
1. Read ObjectMeta with `now_ms` for lazy expiry check
2. If key doesn't exist (or is expired), return 0
3. If `expires_at_ms == 0`, return 0 (no TTL to remove)
4. Set `ObjectMeta.expires_at_ms = 0`, write meta
5. Clear `expire/<key>` entry
6. Return 1

**Implementation pattern for EXPIRETIME/PEXPIRETIME (read-only):**
1. Read ObjectMeta with `now_ms` for lazy expiry check
2. If key doesn't exist (or is expired), return -2
3. If `expires_at_ms == 0`, return -1
4. Return `expires_at_ms` (convert to seconds for EXPIRETIME)

### Key Commands (6)

| Command | Args | Behavior | Return |
|---------|------|----------|--------|
| TYPE | key | Return the type of the value stored at key | Status string: "string", "hash", "set", "zset", "list", "none" |
| RENAME | source destination | Atomically rename key | OK, or error if source doesn't exist |
| RENAMENX | source destination | Rename key only if destination doesn't exist | 1 if renamed, 0 if destination exists |
| UNLINK | key [key ...] | Delete keys (async-safe) | Integer: count of deleted keys |
| TOUCH | key [key ...] | Check key existence | Integer: count of existing keys |
| DBSIZE | (none) | Count keys in current database | Integer: key count |

**TYPE implementation:**
1. Read ObjectMeta with `now_ms` for lazy expiry check
2. If key doesn't exist, return status string "none"
3. Return `KeyType::as_redis_type_str()` as a SimpleString (not BulkString — Redis TYPE returns a status reply)

**RENAME implementation:**
1. Read source ObjectMeta with `now_ms=0` (see expired keys for cleanup)
2. If source doesn't exist at all (no meta), return error `ERR no such key`
3. Read all source data based on type:
   - String: read chunks from `obj/`
   - Hash/Set/SortedSet/List: read all entries from type-specific subspace
4. If destination exists, delete all its data (via `delete_data_for_meta` + clear meta + clear expire)
5. Write source data to destination keys (re-key all FDB entries)
6. Delete source meta + data + expire entries
7. All within a single FDB transaction. Return OK.

**Constraint:** For large non-string types, RENAME may exceed FDB's 10MB transaction limit. This is acceptable for Phase 1 since only string types are implemented. Document the limitation.

**RENAMENX implementation:**
Same as RENAME but step 4 becomes: if destination exists (live, not expired), return 0 without modifying anything. If destination doesn't exist, proceed with rename, return 1.

**UNLINK implementation:**
For Phase 1, UNLINK behaves identically to DEL — synchronous deletion. Both return the count of keys that existed. When M6-M9 introduce large collections, UNLINK should be extended with tombstone-based async cleanup (mark meta as deleted, background worker cleans up data). The command handler already exists in DEL; UNLINK is a dispatch alias.

**TOUCH implementation:**
TOUCH returns the count of keys that exist (not expired). It is functionally identical to EXISTS for Phase 1. When eviction is added (Phase 2+), TOUCH will also update `last_accessed_ms` via FDB atomic max.

**DBSIZE implementation:**
1. Range-read all keys in the `meta/` subspace for the current namespace
2. For each entry, check expiry (exclude expired keys from count)
3. Return the count

This is a full scan, which is appropriate for DBSIZE (called rarely, for monitoring). An atomic counter optimization can be added later if needed.

### Database Commands (3)

| Command | Args | Behavior | Return |
|---------|------|----------|--------|
| SELECT | index | Switch to database 0-15 | OK, or error if index out of range |
| FLUSHDB | [ASYNC] | Delete all keys in current database | OK |
| FLUSHALL | [ASYNC] | Delete all keys in all databases | OK |

**SELECT implementation:**

Requires a shared namespace directory cache so directories are opened once and reused across connections.

1. Validate index is 0-15. Return error `ERR DB index is out of range` otherwise.
2. Look up `Directories` for the requested namespace in the shared cache.
3. If not cached, call `Directories::open(db, index, root_prefix)` to create the subspaces, then cache the result.
4. Update `state.dirs` and `state.selected_db`.
5. Return OK.

**Shared namespace cache:** `Arc<tokio::sync::RwLock<HashMap<u8, Directories>>>`. The listener initializes namespace 0 on startup and seeds the cache. SELECT opens additional namespaces lazily. `RwLock` allows concurrent reads (the common case — most connections use the same namespaces).

The `NamespaceCache` (or `DirectoryCache`) is passed to each connection alongside the `Database` handle. It lives in `src/storage/namespace.rs`.

**FLUSHDB implementation:**
1. For each of the 8 subspaces in the current namespace's `Directories`, compute the full range and `clear_range` it.
2. All 8 clears in a single FDB transaction.
3. Return OK.

The ASYNC flag is accepted but ignored (our implementation is already non-blocking since FDB range-clears are O(1) — they mark ranges for deletion, actual cleanup happens asynchronously in the storage engine).

**FLUSHALL implementation:**
1. For each namespace 0-15 that has been opened (in the namespace cache), FLUSHDB it.
2. For namespaces not in the cache, skip them (they may not exist yet and don't need clearing).
3. All clears in a single FDB transaction (up to 128 range-clears, well within limits).
4. Return OK.

Alternative: clear the entire `root_prefix` directory tree. This is simpler but would require re-opening all directories afterward. The per-namespace approach is safer.

## Background Expiry Worker

### Architecture

A tokio task spawned on server startup. Runs a loop:
1. Sleep for `scan_interval_ms` (default 250ms, from `ExpiryConfig`)
2. Range-read up to `batch_size` (default 1000) entries from `expire/` directory
3. For each entry, deserialize the timestamp and check if `now_ms >= expires_at_ms`
4. For expired entries: delete ObjectMeta, delete type-specific data (via `delete_data_for_meta`), delete expire entry
5. All deletions within a single FDB transaction per batch

### Scan Strategy

Simple full scan of `expire/` directory (Approach A from design discussion). Keys are ordered by key name, not timestamp, so we must check every entry. This is efficient for FDB — a range-read of 1000 small key-value pairs completes in single-digit milliseconds.

**Continuation:** If the scan finds `batch_size` entries, the next iteration continues from where it left off (using the last key as the range start). This ensures progress through large expiry backlogs without re-scanning already-processed entries. A full wrap-around resets after reaching the end of the keyspace.

### Namespace Awareness

The worker needs access to the namespace cache to scan all active namespaces, not just namespace 0. Each scan iteration picks one namespace (round-robin) and processes its expire directory.

### Shutdown

The worker accepts a `CancellationToken` (or watches the server's shutdown signal) and exits cleanly, draining any in-progress transaction before stopping.

### Metrics

- `kvdb_expired_keys_total` (counter): total keys expired by background worker
- `kvdb_expiry_scan_duration_seconds` (histogram): time per scan iteration

These complement the existing lazy-expiry path (which doesn't have its own counter since expired keys are simply treated as non-existent on read).

## File Organization

| File | Contents |
|------|----------|
| `src/commands/keys.rs` | All 18 M5 command handlers |
| `src/ttl/worker.rs` | Background expiry worker |
| `src/storage/namespace.rs` | `NamespaceCache` — shared directory cache for SELECT |
| `src/commands/mod.rs` | Updated dispatch table with all M5 commands |
| `tests/keys.rs` | Integration tests (runs in `just test`) |
| `tests/accept_keys.rs` | Acceptance tests (runs in `just accept`) |

## Testing

### Integration Tests (`tests/keys.rs`)

Focused cases, 3-5 per command:

- **EXPIRE/TTL**: Set a TTL, verify TTL returns correct remaining time
- **PEXPIRE/PTTL**: Same but millisecond precision
- **EXPIREAT/PEXPIREAT**: Set absolute timestamp, verify
- **EXPIRETIME/PEXPIRETIME**: Set expiry, read back absolute timestamp
- **PERSIST**: Set TTL, persist, verify TTL returns -1
- **TTL on non-existent key**: Returns -2
- **TTL on key with no expiry**: Returns -1
- **TYPE**: Returns correct type for string key, "none" for missing
- **RENAME**: Rename existing key, verify old is gone, new has value
- **RENAME non-existent**: Returns error
- **RENAMENX**: Returns 0 when destination exists, 1 when it doesn't
- **UNLINK**: Same as DEL — returns count of deleted keys
- **TOUCH**: Returns count of existing keys
- **DBSIZE**: Returns 0 on empty, correct count after inserts, decrements after deletes
- **SELECT**: Keys in DB 0 not visible in DB 1
- **FLUSHDB**: Clears current DB, doesn't affect other DBs
- **FLUSHALL**: Clears all DBs
- **Background expiry**: SET with PX 200, sleep 600ms, verify key is gone

### Acceptance Tests (`tests/accept_keys.rs`)

Property-based and randomized:

- **TTL round-trip**: EXPIRE then TTL returns value within tolerance (accounts for time passing during test)
- **PERSIST idempotence**: PERSIST on key with no TTL is a no-op (returns 0)
- **RENAME preserves value and TTL**: SET with EX, RENAME, verify value and remaining TTL at new key
- **SELECT isolation**: Operations in DB N don't affect DB M (randomized N, M)
- **FLUSHDB isolation**: FLUSHDB in one DB doesn't affect another
- **Randomized key lifecycle**: Mix of SET, EXPIRE, TTL, PERSIST, DEL, RENAME on overlapping key set, verify model consistency
- **DBSIZE accuracy**: Random sequence of SET/DEL, verify DBSIZE matches expected count

## Non-Goals for M5

- **Tombstone-based async deletion for UNLINK**: Deferred until M6-M9 when large collections exist. UNLINK is synchronous DEL for now.
- **OBJECT command** (OBJECT ENCODING, OBJECT FREQ, etc.): Not in scope.
- **COPY command**: Not in the M5 plan.
- **RANDOMKEY**: Listed in M12, not M5.
- **WAIT/KEYS/SCAN**: Not in scope.
- **Eviction (LRU/LFU/maxmemory)**: Phase 2+. TOUCH doesn't update access time yet.
