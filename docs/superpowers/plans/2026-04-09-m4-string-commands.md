# M4: String Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement all 19 Redis string commands (GET, SET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETRANGE, SETRANGE, DEL, EXISTS, SETNX, SETEX, PSETEX, GETDEL) with full SET flag support, wiring FDB storage into command dispatch.

**Architecture:** Command dispatch becomes async. `ConnectionState` gains `Database` + `Directories` handles. A new `commands/strings.rs` module holds all string handlers. A new `storage/helpers.rs` module provides high-level `get_string`/`write_string`/`delete_object` helpers that encapsulate the meta + type-check + chunk cycle. Each command runs inside a single FDB transaction via `run_transact()`.

**Tech Stack:** Rust, tokio, foundationdb crate, bytes, itoa, ryu, redis-rs (tests), proptest (acceptance), criterion (benchmarks)

**Spec:** `docs/superpowers/specs/2026-04-09-m4-string-commands-design.md`

---

## File Map

| Action | Path | Purpose |
|--------|------|---------|
| Modify | `src/server/connection.rs` | Make `ConnectionState` hold `Database` + `Directories`, make dispatch async |
| Modify | `src/server/listener.rs` | Initialize FDB `Database`, pass to connection handler |
| Modify | `src/commands/mod.rs` | Make `dispatch()` async, route string commands to `strings.rs` |
| Create | `src/commands/strings.rs` | All 19 string command handlers |
| Create | `src/storage/helpers.rs` | High-level `get_string`, `write_string`, `delete_object` |
| Modify | `src/storage/mod.rs` | Export `helpers` module |
| Modify | `src/error.rs` | Add `CommandError::WrongType` display, no structural changes needed |
| Modify | `src/config.rs` | No changes needed |
| Modify | `tests/harness/mod.rs` | Initialize FDB in test context |
| Create | `tests/strings.rs` | Integration tests (~60 tests) |
| Create | `tests/accept_strings.rs` | Acceptance tests (property-based + randomized) |
| Modify | `examples/smoke.rs` | Add string command + error smoke tests |
| Modify | `benches/commands.rs` | Add string command benchmarks |
| Modify | `fuzz/fuzz_targets/command_dispatch.rs` | Update for async dispatch |

---

### Task 1: Initialize FDB in Server and Test Harness

Wire FDB `Database` and `Directories` into the server startup path and test harness so that every connection has storage access.

**Files:**
- Modify: `src/server/listener.rs`
- Modify: `src/server/connection.rs`
- Modify: `tests/harness/mod.rs`

- [ ] **Step 1: Add `Database` and `Directories` to `ConnectionState`**

In `src/server/connection.rs`, add storage handles:

```rust
use crate::storage::{Database, Directories};

pub struct ConnectionState {
    pub protocol_version: u8,
    pub selected_db: u8,
    pub db: Database,
    pub dirs: Directories,
}
```

Remove the `Default` impl (it can no longer be default-constructed without FDB handles). Add a constructor:

```rust
impl ConnectionState {
    pub fn new(db: Database, dirs: Directories) -> Self {
        Self {
            protocol_version: 2,
            selected_db: 0,
            db,
            dirs,
        }
    }
}
```

Update `run_loop` to accept `db` and `dirs` parameters and construct `ConnectionState::new(db, dirs)` instead of `ConnectionState::default()`. Update `handle()` to accept and forward these parameters.

- [ ] **Step 2: Initialize FDB in listener and pass to connections**

In `src/server/listener.rs`, initialize FDB on startup and open `Directories` for namespace 0:

```rust
use crate::storage::{Database, Directories};

pub async fn run(config: ServerConfig, shutdown: tokio::sync::broadcast::Receiver<()>) -> anyhow::Result<()> {
    let db = Database::new(&config.fdb_cluster_file)?;
    let dirs = Directories::open(&db, 0, "kvdb").await?;

    // ... existing listener loop ...
    // Pass db.clone() and dirs.clone() to each connection::handle() call
}
```

Update the `tokio::spawn` inside the accept loop to pass `db.clone()` and `dirs.clone()` to the connection handler.

- [ ] **Step 3: Update test harness to initialize FDB**

In `tests/harness/mod.rs`, the `TestContext` must initialize FDB with an isolated namespace per test:

```rust
use kvdb::storage::{Database, Directories};
use uuid::Uuid;

pub struct TestContext {
    pub addr: SocketAddr,
    pub client: redis::Client,
    shutdown_tx: Option<broadcast::Sender<()>>,
    _server_handle: JoinHandle<()>,
    root_prefix: String,
    db: Database,
}
```

In `new_with_config`, initialize FDB before spawning the server:

```rust
let root_prefix = format!("kvdb_test_{}", Uuid::new_v4());
let db = Database::new(&config.fdb_cluster_file)
    .expect("FDB must be running for tests (run `just up`)");
let dirs = Directories::open(&db, 0, &root_prefix).await
    .expect("Failed to open test directories");
```

Update the listener to accept `Database` and `Directories` or pass them via config. The simplest approach: update `listener::run` to also accept `db: Option<Database>` and `root_prefix: Option<String>`, using them if provided (for tests) or creating fresh ones (for production). Alternatively, add `db` and `root_prefix` fields to `ServerConfig` wrapped in `Option`.

Add cleanup in `Drop` to remove the test namespace:

```rust
impl Drop for TestContext {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Schedule cleanup of test FDB namespace
        let db = self.db.clone();
        let prefix = self.root_prefix.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // Clear the test directory tree
                let _ = cleanup_test_dirs(&db, &prefix).await;
            });
        });
    }
}
```

- [ ] **Step 4: Fix compilation**

Update all call sites:
- `commands/mod.rs`: `dispatch()` — update `ConnectionState::default()` usage in tests to use a mock/stub. For unit tests that don't need FDB, add a `ConnectionState::default_stub()` method that creates a minimal state (or cfg(test) constructor).
- `fuzz/fuzz_targets/command_dispatch.rs`: Update `ConnectionState::default()` usage.

Run `cargo build` and fix all compilation errors.

- [ ] **Step 5: Verify existing tests still pass**

Run: `just test`
Expected: All existing tests pass (PING, ECHO, HELLO, etc.)

- [ ] **Step 6: Commit**

```
feat(m4): wire FDB storage into connection state and test harness
```

---

### Task 2: Make Command Dispatch Async

Convert `dispatch()` and `dispatch_one()` to async so string command handlers can call FDB.

**Files:**
- Modify: `src/commands/mod.rs`
- Modify: `src/server/connection.rs`

- [ ] **Step 1: Make `dispatch()` async**

In `src/commands/mod.rs`, change the signature:

```rust
pub async fn dispatch(cmd: &RedisCommand, state: &mut ConnectionState) -> CommandResponse {
    match cmd.name.as_ref() {
        b"PING" => CommandResponse::Reply(handle_ping(&cmd.args)),
        b"ECHO" => CommandResponse::Reply(handle_echo(&cmd.args)),
        b"HELLO" => CommandResponse::Reply(handle_hello(&cmd.args, state)),
        b"QUIT" => CommandResponse::Close(RespValue::ok()),
        b"COMMAND" => CommandResponse::Reply(handle_command(&cmd.args)),
        b"CLIENT" => CommandResponse::Reply(handle_client(&cmd.args)),
        _ => {
            let name_str = sanitize_for_error(&cmd.name);
            let mut msg = format!("ERR unknown command '{name_str}', with args beginning with:");
            for arg in cmd.args.iter().take(3) {
                msg.push_str(&format!(" '{}'", sanitize_for_error(arg)));
            }
            CommandResponse::Reply(RespValue::Error(Bytes::from(msg)))
        }
    }
}
```

All existing handlers remain synchronous — they just return immediately.

- [ ] **Step 2: Make `dispatch_one()` async**

In `src/server/connection.rs`, change `dispatch_one` to async:

```rust
async fn dispatch_one(value: RespValue, state: &mut ConnectionState, write_buf: &mut BytesMut) -> bool {
    match RedisCommand::from_resp(value) {
        Ok(cmd) => {
            let label = metric_label_for_command(&cmd.name);
            let timer = metrics::COMMAND_DURATION_SECONDS
                .with_label_values(&[label])
                .start_timer();

            let (response, close) = match commands::dispatch(&cmd, state).await {
                CommandResponse::Reply(resp) => (resp, false),
                CommandResponse::Close(resp) => (resp, true),
            };
            // ... rest unchanged
        }
        // ... rest unchanged
    }
}
```

Update the call in `run_loop` from `dispatch_one(value, &mut state, &mut write_buf)` to `dispatch_one(value, &mut state, &mut write_buf).await`.

- [ ] **Step 3: Update unit tests in `commands/mod.rs`**

The `dispatch_reply` test helper needs to become async, or use `tokio::runtime::Runtime::new().unwrap().block_on(...)` since these are `#[test]` (not `#[tokio::test]`). Simplest approach: switch them to `#[tokio::test]`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn make_cmd(name: &[u8], args: Vec<&[u8]>) -> RedisCommand {
        RedisCommand {
            name: Bytes::from(name.to_vec()),
            args: args.into_iter().map(|a| Bytes::from(a.to_vec())).collect(),
        }
    }

    async fn dispatch_reply(cmd: &RedisCommand, state: &mut ConnectionState) -> RespValue {
        match dispatch(cmd, state).await {
            CommandResponse::Reply(resp) => resp,
            CommandResponse::Close(resp) => resp,
        }
    }

    // Each test becomes #[tokio::test] and creates its own state
}
```

Since these tests don't need FDB, provide a test-only constructor for `ConnectionState` that doesn't require real storage handles (using `cfg(test)`).

- [ ] **Step 4: Update fuzz target**

In `fuzz/fuzz_targets/command_dispatch.rs`, the `fuzz_target!` closure is not async. Use a lightweight tokio runtime to call the async dispatch:

```rust
fuzz_target!(|input: FuzzCommand| {
    // ... existing setup ...

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut state = ConnectionState::default_for_test();
    let response = rt.block_on(async {
        match commands::dispatch(&cmd, &mut state).await {
            CommandResponse::Reply(resp) => resp,
            CommandResponse::Close(resp) => resp,
        }
    });

    // ... existing validation ...
});
```

- [ ] **Step 5: Verify everything compiles and passes**

Run: `just test`
Expected: All tests pass, no regressions.

- [ ] **Step 6: Commit**

```
refactor(m4): make command dispatch async for FDB access
```

---

### Task 3: Storage Helpers Module

Create `src/storage/helpers.rs` with high-level helpers that encapsulate the meta + type-check + chunk read/write cycle.

**Files:**
- Create: `src/storage/helpers.rs`
- Modify: `src/storage/mod.rs`

- [ ] **Step 1: Create `src/storage/helpers.rs` with `get_string`**

```rust
//! High-level storage helpers for command implementations.
//!
//! These functions encapsulate the ObjectMeta + type-check + chunk
//! read/write cycle that every command repeats. Inspired by the
//! bluesky-social/kvdb Go implementation's getObject/writeObject pattern.

use foundationdb::Transaction;

use super::chunking;
use super::directories::Directories;
use super::meta::{KeyType, ObjectMeta};
use crate::error::CommandError;

/// Read a string value from FDB.
///
/// Returns `Ok(None)` if the key doesn't exist or is expired.
/// Returns `Err(CommandError::WrongType)` if the key exists with a non-String type.
pub async fn get_string(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    now_ms: u64,
) -> Result<Option<Vec<u8>>, CommandError> {
    let meta = ObjectMeta::read(tr, dirs, key, now_ms, false)
        .await
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    match meta {
        None => Ok(None),
        Some(m) if m.key_type != KeyType::String => Err(CommandError::WrongType),
        Some(m) => {
            let data = chunking::read_chunks(tr, &dirs.obj, key, m.num_chunks, m.size_bytes, false)
                .await
                .map_err(|e| CommandError::Generic(e.to_string()))?;
            Ok(Some(data))
        }
    }
}

/// Write a string value to FDB.
///
/// Overwrites any existing key (including non-String types).
/// If `old_meta` is provided, cleans up old data first.
/// Writes expiry to both ObjectMeta and expire/ directory.
///
/// Returns the number of chunks written.
pub fn write_string(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    data: &[u8],
    expires_at_ms: u64,
    old_meta: Option<&ObjectMeta>,
) -> Result<(), CommandError> {
    // Clean up old data if overwriting a different type
    if let Some(old) = old_meta {
        delete_data_for_meta(tr, dirs, key, old);
    }

    let num_chunks = chunking::write_chunks(tr, &dirs.obj, key, data);
    let mut meta = ObjectMeta::new_string(num_chunks, data.len() as u64);
    meta.expires_at_ms = expires_at_ms;

    meta.write(tr, dirs, key)
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    // Write/clear expiry entry
    if expires_at_ms > 0 {
        let expire_key = dirs.expire_key(key);
        tr.set(&expire_key, &expires_at_ms.to_be_bytes());
    } else if old_meta.is_some_and(|m| m.expires_at_ms > 0) {
        let expire_key = dirs.expire_key(key);
        tr.clear(&expire_key);
    }

    Ok(())
}

/// Delete a key of any type. Removes meta, type-specific data, and expiry.
///
/// Returns `true` if the key existed (was actually deleted).
pub async fn delete_object(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    now_ms: u64,
) -> Result<bool, CommandError> {
    // Read meta without expiry check (we want to delete even expired keys'
    // leftover data — the background worker may not have cleaned up yet)
    let meta = ObjectMeta::read(tr, dirs, key, 0, false)
        .await
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    match meta {
        None => Ok(false),
        Some(ref m) => {
            // Check if logically expired (for correct return value)
            let existed = now_ms == 0 || !m.is_expired(now_ms);

            delete_data_for_meta(tr, dirs, key, m);
            ObjectMeta::delete(tr, dirs, key)
                .map_err(|e| CommandError::Generic(e.to_string()))?;

            // Clear expiry entry
            if m.expires_at_ms > 0 {
                let expire_key = dirs.expire_key(key);
                tr.clear(&expire_key);
            }

            Ok(existed)
        }
    }
}

/// Delete the type-specific data for a key (chunks, hash fields, set members, etc.)
/// without touching ObjectMeta or expiry. Used during overwrites.
fn delete_data_for_meta(tr: &Transaction, dirs: &Directories, key: &[u8], meta: &ObjectMeta) {
    match meta.key_type {
        KeyType::String => {
            chunking::delete_chunks(tr, &dirs.obj, key);
        }
        KeyType::Hash => {
            // Clear all hash fields: hash/<key, *>
            let sub = dirs.hash.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::Set => {
            let sub = dirs.set.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::SortedSet => {
            let sub = dirs.zset.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);
            let idx_sub = dirs.zset_idx.subspace(&(key,));
            let (begin, end) = idx_sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::List => {
            let sub = dirs.list.subspace(&(key,));
            let (begin, end) = sub.range();
            tr.clear_range(&begin, &end);
        }
        KeyType::Stream => {
            // Streams not yet implemented; no-op
        }
    }
}

/// Get current time in milliseconds (UTC).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
```

- [ ] **Step 2: Export from `src/storage/mod.rs`**

Add to `src/storage/mod.rs`:

```rust
pub mod helpers;
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build`
Expected: Compiles cleanly.

- [ ] **Step 4: Commit**

```
feat(m4): add storage helpers module (get_string, write_string, delete_object)
```

---

### Task 4: GET Command (First End-to-End Command)

Implement GET as the first command that touches FDB end-to-end. Write integration tests first.

**Files:**
- Create: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Create: `tests/strings.rs`

- [ ] **Step 1: Write failing integration tests for GET**

Create `tests/strings.rs`:

```rust
//! Integration tests for string commands (Milestone 4).

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;

// ── GET ───────────────────────────────────────────────────────────

#[tokio::test]
async fn get_nonexistent_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: Option<String> = redis::cmd("GET")
        .arg("nonexistent")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn get_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    // GET with no args
    let result: redis::RedisResult<String> = redis::cmd("GET").query_async(&mut con).await;
    assert!(result.is_err());
    // GET with too many args
    let result: redis::RedisResult<String> = redis::cmd("GET")
        .arg("a")
        .arg("b")
        .query_async(&mut con)
        .await;
    assert!(result.is_err());
}
```

Run: `just test`
Expected: FAIL — GET is not implemented yet.

- [ ] **Step 2: Create `src/commands/strings.rs` with GET handler**

```rust
//! String command handlers.
//!
//! All 19 Redis string commands. Each handler validates arguments,
//! runs an FDB transaction via `run_transact()`, and returns a `RespValue`.

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::{helpers, run_transact};

/// GET key
pub async fn handle_get(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(
            CommandError::WrongArity { name: "GET".into() }.to_string(),
        );
    }
    let key = &args[0];

    match run_transact(&state.db, "GET", |tr| {
        let dirs = state.dirs.clone();
        let k = key.to_vec();
        async move {
            let now = helpers::now_ms();
            helpers::get_string(&tr, &dirs, &k, now)
                .await
                .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))
        }
    })
    .await
    {
        Ok(Some(data)) => RespValue::BulkString(Some(Bytes::from(data))),
        Ok(None) => RespValue::BulkString(None),
        Err(e) => {
            // Check if it's a WRONGTYPE error
            let msg = e.to_string();
            if msg.contains("WRONGTYPE") {
                RespValue::Error(Bytes::from("WRONGTYPE Operation against a key holding the wrong kind of value"))
            } else {
                RespValue::err(format!("ERR {msg}"))
            }
        }
    }
}
```

Note: The error propagation pattern through `FdbBindingError::CustomError` is needed because `run_transact` expects `FdbBindingError`. We'll need to define a clean way to convert `CommandError` ↔ `FdbBindingError`. Consider adding a helper trait or function for this. Alternatively, refactor `run_transact` to support a more ergonomic error type — but that's a larger change. For now, use `CustomError(Box::new(e))` and extract on the other side.

A cleaner approach: add a dedicated error conversion helper:

```rust
// In src/storage/helpers.rs or a shared location
use foundationdb::FdbBindingError;

/// Convert a CommandError into an FdbBindingError for use inside run_transact closures.
pub fn cmd_err(e: CommandError) -> FdbBindingError {
    FdbBindingError::CustomError(Box::new(e))
}

/// Extract a CommandError from a StorageError if it was wrapped via cmd_err.
/// Returns the original StorageError message as a Generic CommandError otherwise.
pub fn extract_cmd_err(e: crate::error::StorageError) -> CommandError {
    // StorageError doesn't carry CommandError directly; convert to string
    let msg = e.to_string();
    if msg.contains("WRONGTYPE") {
        CommandError::WrongType
    } else {
        CommandError::Generic(msg)
    }
}
```

- [ ] **Step 3: Wire GET into dispatch**

In `src/commands/mod.rs`, add the module and route:

```rust
pub mod strings;

// In dispatch():
b"GET" => CommandResponse::Reply(strings::handle_get(&cmd.args, state).await),
```

Add `"GET"` to `metric_label_for_command()` in `connection.rs`.

- [ ] **Step 4: Run tests**

Run: `just test`
Expected: `get_nonexistent_returns_nil` and `get_wrong_arity` PASS. All prior tests still pass.

- [ ] **Step 5: Commit**

```
feat(m4): implement GET command with FDB storage
```

---

### Task 5: SET Command with Full Flag Support

Implement SET with all flags: NX, XX, EX, PX, EXAT, PXAT, KEEPTTL, GET.

**Files:**
- Modify: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/strings.rs`

- [ ] **Step 1: Write failing integration tests**

Add to `tests/strings.rs`:

```rust
use redis::AsyncCommands;

// ── SET / GET round-trip ──────────────────────────────────────────

#[tokio::test]
async fn set_and_get() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("foo").arg("bar")
        .query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("foo")
        .query_async(&mut con).await.unwrap();
    assert_eq!(val, "bar");
}

#[tokio::test]
async fn set_overwrites_existing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("k").arg("v1")
        .query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("k").arg("v2")
        .query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("k")
        .query_async(&mut con).await.unwrap();
    assert_eq!(val, "v2");
}

#[tokio::test]
async fn set_nx_respects_condition() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    // NX on non-existent key → succeeds
    let result: Option<String> = redis::cmd("SET").arg("k").arg("v1").arg("NX")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result, Some("OK".to_string()));
    // NX on existing key → nil (no-op)
    let result: Option<String> = redis::cmd("SET").arg("k").arg("v2").arg("NX")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result, None);
    // Value unchanged
    let val: String = redis::cmd("GET").arg("k")
        .query_async(&mut con).await.unwrap();
    assert_eq!(val, "v1");
}

#[tokio::test]
async fn set_xx_respects_condition() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    // XX on non-existent key → nil (no-op)
    let result: Option<String> = redis::cmd("SET").arg("k").arg("v1").arg("XX")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result, None);
    // SET normally first
    let _: () = redis::cmd("SET").arg("k").arg("v1")
        .query_async(&mut con).await.unwrap();
    // XX on existing key → succeeds
    let result: Option<String> = redis::cmd("SET").arg("k").arg("v2").arg("XX")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result, Some("OK".to_string()));
}

#[tokio::test]
async fn set_get_returns_old_value() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("k").arg("old")
        .query_async(&mut con).await.unwrap();
    let old: Option<String> = redis::cmd("SET").arg("k").arg("new").arg("GET")
        .query_async(&mut con).await.unwrap();
    assert_eq!(old, Some("old".to_string()));
}

#[tokio::test]
async fn set_wrong_arity() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .query_async(&mut con).await;
    assert!(result.is_err());
    let result: redis::RedisResult<String> = redis::cmd("SET").arg("onlykey")
        .query_async(&mut con).await;
    assert!(result.is_err());
}

// ── SET flag errors ───────────────────────────────────────────────

#[tokio::test]
async fn set_nx_xx_mutually_exclusive() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("k").arg("v").arg("NX").arg("XX")
        .query_async(&mut con).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn set_ex_zero_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("k").arg("v").arg("EX").arg("0")
        .query_async(&mut con).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn set_ex_negative_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("k").arg("v").arg("EX").arg("-5")
        .query_async(&mut con).await;
    assert!(result.is_err());
}
```

- [ ] **Step 2: Implement SET flag parsing**

Add to `src/commands/strings.rs`:

```rust
/// Parsed SET command options.
#[derive(Debug, Default)]
struct SetOptions {
    condition: Option<SetCondition>,
    expiry: Option<Expiry>,
    keep_ttl: bool,
    get: bool,
}

#[derive(Debug)]
enum SetCondition {
    Nx,
    Xx,
}

#[derive(Debug)]
enum Expiry {
    Ex(u64),
    Px(u64),
    ExAt(u64),
    PxAt(u64),
}

/// Parse SET flags from args starting at index 2 (after key and value).
fn parse_set_options(args: &[Bytes]) -> Result<SetOptions, RespValue> {
    let mut opts = SetOptions::default();
    let mut i = 2;

    while i < args.len() {
        let flag = args[i].to_ascii_uppercase();
        match flag.as_slice() {
            b"NX" => {
                if opts.condition.is_some() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                opts.condition = Some(SetCondition::Nx);
            }
            b"XX" => {
                if opts.condition.is_some() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                opts.condition = Some(SetCondition::Xx);
            }
            b"GET" => {
                opts.get = true;
            }
            b"KEEPTTL" => {
                if opts.expiry.is_some() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                opts.keep_ttl = true;
            }
            b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                if opts.expiry.is_some() || opts.keep_ttl {
                    return Err(RespValue::err("ERR syntax error"));
                }
                i += 1;
                if i >= args.len() {
                    return Err(RespValue::err("ERR syntax error"));
                }
                let val_str = std::str::from_utf8(&args[i])
                    .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
                let val: i64 = val_str.parse()
                    .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
                if val <= 0 {
                    return Err(RespValue::err("ERR invalid expire time in 'set' command"));
                }
                let val = val as u64;
                opts.expiry = Some(match flag.as_slice() {
                    b"EX" => Expiry::Ex(val),
                    b"PX" => Expiry::Px(val),
                    b"EXAT" => Expiry::ExAt(val),
                    b"PXAT" => Expiry::PxAt(val),
                    _ => unreachable!(),
                });
            }
            _ => {
                return Err(RespValue::err("ERR syntax error"));
            }
        }
        i += 1;
    }
    Ok(opts)
}

/// Compute absolute expiry timestamp in milliseconds.
fn resolve_expiry(opts: &SetOptions, now_ms: u64, old_meta: Option<&ObjectMeta>) -> u64 {
    if opts.keep_ttl {
        return old_meta.map_or(0, |m| m.expires_at_ms);
    }
    match &opts.expiry {
        Some(Expiry::Ex(secs)) => now_ms + secs * 1000,
        Some(Expiry::Px(ms)) => now_ms + ms,
        Some(Expiry::ExAt(secs)) => secs * 1000,
        Some(Expiry::PxAt(ms)) => *ms,
        None => 0,
    }
}
```

- [ ] **Step 3: Implement `handle_set`**

```rust
use crate::storage::meta::ObjectMeta;

/// SET key value [NX | XX] [GET] [EX s | PX ms | EXAT s | PXAT ms] [KEEPTTL]
pub async fn handle_set(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() < 2 {
        return RespValue::err(
            CommandError::WrongArity { name: "SET".into() }.to_string(),
        );
    }

    let key = &args[0];
    let value = &args[1];

    let opts = match parse_set_options(args) {
        Ok(o) => o,
        Err(e) => return e,
    };

    let k = key.to_vec();
    let v = value.to_vec();

    match run_transact(&state.db, "SET", |tr| {
        let dirs = state.dirs.clone();
        let k = k.clone();
        let v = v.clone();
        let get = opts.get;
        let condition = opts.condition.as_ref().map(|c| match c {
            SetCondition::Nx => 0u8,
            SetCondition::Xx => 1u8,
        });
        let keep_ttl = opts.keep_ttl;
        let expiry = opts.expiry.as_ref().map(|e| match e {
            Expiry::Ex(v) => (0u8, *v),
            Expiry::Px(v) => (1u8, *v),
            Expiry::ExAt(v) => (2u8, *v),
            Expiry::PxAt(v) => (3u8, *v),
        });

        async move {
            let now = helpers::now_ms();
            let old_meta = ObjectMeta::read(&tr, &dirs, &k, now, false)
                .await
                .map_err(helpers::storage_err)?;

            // Check condition
            match condition {
                Some(0) if old_meta.is_some() => {
                    // NX but key exists — return old value if GET, else nil
                    if get {
                        let old_val = if old_meta.as_ref().is_some_and(|m| m.key_type == KeyType::String) {
                            let m = old_meta.as_ref().unwrap();
                            Some(chunking::read_chunks(&tr, &dirs.obj, &k, m.num_chunks, m.size_bytes, false)
                                .await
                                .map_err(helpers::storage_err)?)
                        } else if old_meta.is_some() {
                            return Err(helpers::cmd_err(CommandError::WrongType));
                        } else {
                            None
                        };
                        return Ok((false, old_val));
                    }
                    return Ok((false, None));
                }
                Some(1) if old_meta.is_none() => {
                    // XX but key doesn't exist
                    return Ok((false, None));
                }
                _ => {}
            }

            // If GET flag, read old value
            let old_val = if get {
                match &old_meta {
                    Some(m) if m.key_type == KeyType::String => {
                        Some(chunking::read_chunks(&tr, &dirs.obj, &k, m.num_chunks, m.size_bytes, false)
                            .await
                            .map_err(helpers::storage_err)?)
                    }
                    Some(_) => return Err(helpers::cmd_err(CommandError::WrongType)),
                    None => None,
                }
            } else {
                None
            };

            // Compute expiry
            let expires_at_ms = if keep_ttl {
                old_meta.as_ref().map_or(0, |m| m.expires_at_ms)
            } else {
                match expiry {
                    Some((0, secs)) => now + secs * 1000,
                    Some((1, ms)) => now + ms,
                    Some((2, secs)) => secs * 1000,
                    Some((3, ms)) => ms,
                    _ => 0,
                }
            };

            // Write the new value
            helpers::write_string(&tr, &dirs, &k, &v, expires_at_ms, old_meta.as_ref())
                .map_err(helpers::cmd_err)?;

            Ok((true, old_val))
        }
    })
    .await
    {
        Ok((true, old_val)) => {
            if opts.get {
                match old_val {
                    Some(data) => RespValue::BulkString(Some(Bytes::from(data))),
                    None => RespValue::BulkString(None),
                }
            } else {
                RespValue::ok()
            }
        }
        Ok((false, old_val)) => {
            if opts.get {
                match old_val {
                    Some(data) => RespValue::BulkString(Some(Bytes::from(data))),
                    None => RespValue::BulkString(None),
                }
            } else {
                RespValue::BulkString(None) // NX/XX condition not met
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
```

Note: This is the most complex handler. The key pattern is that the `run_transact` closure returns `(bool, Option<Vec<u8>>)` — whether the SET happened and the optional old value for GET.

You'll need to add the `storage_err` and `cmd_err` and `storage_err_to_resp` helpers to `storage/helpers.rs`. Define them cleanly:

```rust
use foundationdb::FdbBindingError;
use crate::error::StorageError;

pub fn cmd_err(e: CommandError) -> FdbBindingError {
    FdbBindingError::CustomError(Box::new(e))
}

pub fn storage_err(e: StorageError) -> FdbBindingError {
    FdbBindingError::CustomError(Box::new(CommandError::Generic(e.to_string())))
}

pub fn storage_err_to_resp(e: StorageError) -> RespValue {
    let msg = e.to_string();
    if msg.contains("WRONGTYPE") {
        RespValue::Error(Bytes::from(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    } else {
        RespValue::err(format!("ERR {msg}"))
    }
}
```

- [ ] **Step 4: Wire SET into dispatch**

In `src/commands/mod.rs`:

```rust
b"SET" => CommandResponse::Reply(strings::handle_set(&cmd.args, state).await),
```

Add `"SET"` to `metric_label_for_command()`.

- [ ] **Step 5: Run tests**

Run: `just test`
Expected: All SET/GET tests pass.

- [ ] **Step 6: Commit**

```
feat(m4): implement SET command with full flag support (NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET)
```

---

### Task 6: DEL, EXISTS, SETNX, SETEX, PSETEX, GETDEL

Implement the remaining simple commands that build on the infrastructure from Tasks 3-5.

**Files:**
- Modify: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/strings.rs`

- [ ] **Step 1: Write failing tests**

Add to `tests/strings.rs` tests for: `del_removes_key`, `del_multiple_keys_returns_count`, `del_nonexistent_returns_zero`, `exists_returns_count`, `exists_counts_duplicates`, `setnx_returns_integer`, `setex_basic`, `psetex_basic`, `getdel_returns_and_deletes`, `del_wrong_arity`, `exists_wrong_arity`.

- [ ] **Step 2: Implement handlers**

In `src/commands/strings.rs`, implement:

```rust
/// DEL key [key ...]
pub async fn handle_del(args: &[Bytes], state: &ConnectionState) -> RespValue

/// EXISTS key [key ...]
pub async fn handle_exists(args: &[Bytes], state: &ConnectionState) -> RespValue

/// SETNX key value → returns 1 if set, 0 if not
pub async fn handle_setnx(args: &[Bytes], state: &ConnectionState) -> RespValue

/// SETEX key seconds value
pub async fn handle_setex(args: &[Bytes], state: &ConnectionState) -> RespValue

/// PSETEX key milliseconds value
pub async fn handle_psetex(args: &[Bytes], state: &ConnectionState) -> RespValue

/// GETDEL key
pub async fn handle_getdel(args: &[Bytes], state: &ConnectionState) -> RespValue
```

DEL iterates over all keys in a single transaction, calling `delete_object` for each. EXISTS reads ObjectMeta for each key (with expiry check), counts how many exist. SETNX/SETEX/PSETEX are thin wrappers that delegate to the SET logic. GETDEL reads the value and deletes in one transaction.

- [ ] **Step 3: Wire into dispatch**

Add all commands to `dispatch()` and `metric_label_for_command()`.

- [ ] **Step 4: Run tests**

Run: `just test`
Expected: All new tests pass, no regressions.

- [ ] **Step 5: Commit**

```
feat(m4): implement DEL, EXISTS, SETNX, SETEX, PSETEX, GETDEL
```

---

### Task 7: MGET and MSET

**Files:**
- Modify: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/strings.rs`

- [ ] **Step 1: Write failing tests**

Tests: `mset_and_mget_roundtrip`, `mget_returns_nil_for_missing`, `mset_overwrites_existing`, `mset_odd_args_returns_error`, `mget_wrong_arity`, `mset_wrong_arity`.

- [ ] **Step 2: Implement MGET**

MGET reads all keys in a single transaction, firing parallel gets. Non-string keys return nil (no WRONGTYPE error for MGET).

```rust
/// MGET key [key ...]
pub async fn handle_mget(args: &[Bytes], state: &ConnectionState) -> RespValue
```

Inside the transaction, for each key: read ObjectMeta, if String type read chunks, else return None. Collect into `Vec<Option<Vec<u8>>>`, return as RESP Array.

- [ ] **Step 3: Implement MSET**

MSET writes all key-value pairs in one transaction. Always returns OK.

```rust
/// MSET key value [key value ...]
pub async fn handle_mset(args: &[Bytes], state: &ConnectionState) -> RespValue
```

Validates even number of args. Inside transaction, for each pair: read old meta, write_string.

- [ ] **Step 4: Wire into dispatch, run tests**

- [ ] **Step 5: Commit**

```
feat(m4): implement MGET, MSET
```

---

### Task 8: INCR, DECR, INCRBY, DECRBY

**Files:**
- Modify: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/strings.rs`

- [ ] **Step 1: Write failing tests**

Tests: `incr_creates_key_from_zero`, `incr_increments_existing`, `incr_returns_error_on_non_integer`, `incr_returns_error_on_float_string`, `incr_overflow_returns_error`, `decr_underflow_returns_error`, `decrby_with_large_delta`, `incrby_arg_overflow`, `incr_wrong_arity`.

- [ ] **Step 2: Implement shared `incr_by` helper**

```rust
/// Shared core for INCR/DECR/INCRBY/DECRBY.
/// Reads the current value, parses as i64, adds delta, writes back.
async fn incr_by_impl(
    tr: &Transaction,
    dirs: &Directories,
    key: &[u8],
    delta: i64,
    now_ms: u64,
) -> Result<i64, CommandError> {
    let meta = ObjectMeta::read(tr, dirs, key, now_ms, false)
        .await
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    match &meta {
        Some(m) if m.key_type != KeyType::String => return Err(CommandError::WrongType),
        _ => {}
    }

    let current = if let Some(ref m) = meta {
        let data = chunking::read_chunks(tr, &dirs.obj, key, m.num_chunks, m.size_bytes, false)
            .await
            .map_err(|e| CommandError::Generic(e.to_string()))?;
        let s = std::str::from_utf8(&data)
            .map_err(|_| CommandError::Generic("value is not an integer or out of range".into()))?;
        s.parse::<i64>()
            .map_err(|_| CommandError::Generic("value is not an integer or out of range".into()))?
    } else {
        0
    };

    let result = current
        .checked_add(delta)
        .ok_or_else(|| CommandError::Generic("increment or decrement would overflow".into()))?;

    let result_str = itoa::Buffer::new().format(result).to_owned();
    let result_bytes = result_str.as_bytes();

    helpers::write_string(tr, dirs, key, result_bytes, 0, meta.as_ref())
        .map_err(|e| CommandError::Generic(e.to_string()))?;

    // Preserve TTL if key existed
    // (write_string set expires_at_ms to 0; we need to restore it)
    // Actually, INCR preserves the existing TTL per Redis semantics.
    // We should pass the old expires_at_ms to write_string.

    Ok(result)
}
```

Note: Redis INCR preserves existing TTL. The `write_string` call must pass the old `expires_at_ms` from the existing meta if the key already existed.

- [ ] **Step 3: Implement INCR, DECR, INCRBY, DECRBY handlers**

```rust
pub async fn handle_incr(args: &[Bytes], state: &ConnectionState) -> RespValue
pub async fn handle_decr(args: &[Bytes], state: &ConnectionState) -> RespValue
pub async fn handle_incrby(args: &[Bytes], state: &ConnectionState) -> RespValue
pub async fn handle_decrby(args: &[Bytes], state: &ConnectionState) -> RespValue
```

INCR delegates to `handle_incrby` with delta=1. DECR delegates to `handle_decrby` with delta=1. INCRBY/DECRBY parse the delta arg and call `incr_by_impl`.

- [ ] **Step 4: Wire into dispatch, run tests**

- [ ] **Step 5: Commit**

```
feat(m4): implement INCR, DECR, INCRBY, DECRBY with overflow detection
```

---

### Task 9: INCRBYFLOAT

**Files:**
- Modify: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/strings.rs`

- [ ] **Step 1: Write failing tests**

Tests: `incrbyfloat_basic`, `incrbyfloat_on_integer_string`, `incrbyfloat_nan_increment_returns_error`, `incrbyfloat_inf_increment_returns_error`, `incrbyfloat_wrong_arity`.

- [ ] **Step 2: Implement `handle_incrbyfloat`**

Similar to incr_by but with f64. Parse increment, check for NaN/Inf in increment, add to current value, check result for NaN/Inf, format using Redis rules (strip trailing zeros after decimal point).

Redis float formatting: use `ryu` to convert to string, then strip trailing zeros and unnecessary decimal point.

```rust
/// Format a float value following Redis conventions.
/// Strips trailing zeros after decimal point. Removes decimal point if no fraction.
fn format_redis_float(val: f64) -> String {
    if val.fract() == 0.0 && val.is_finite() {
        // Integer result — format without decimal point
        format!("{}", val as i64)
    } else {
        let mut s = ryu::Buffer::new().format(val).to_string();
        if s.contains('.') {
            // Strip trailing zeros
            let trimmed = s.trim_end_matches('0');
            let trimmed = trimmed.trim_end_matches('.');
            s = trimmed.to_string();
        }
        s
    }
}
```

- [ ] **Step 3: Wire into dispatch, run tests**

- [ ] **Step 4: Commit**

```
feat(m4): implement INCRBYFLOAT with NaN/Inf protection
```

---

### Task 10: APPEND, STRLEN

**Files:**
- Modify: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/strings.rs`

- [ ] **Step 1: Write failing tests**

Tests: `append_to_existing`, `append_creates_key`, `strlen_returns_length`, `strlen_nonexistent_returns_zero`, `append_wrong_arity`, `strlen_wrong_arity`.

- [ ] **Step 2: Implement APPEND**

Read existing value (or empty if key doesn't exist), concatenate, write back, return new length. WRONGTYPE check on existing key.

- [ ] **Step 3: Implement STRLEN**

Read ObjectMeta only (no chunk read), return `size_bytes`. Returns 0 for non-existent keys. WRONGTYPE check.

- [ ] **Step 4: Wire into dispatch, run tests**

- [ ] **Step 5: Commit**

```
feat(m4): implement APPEND, STRLEN
```

---

### Task 11: GETRANGE, SETRANGE

**Files:**
- Modify: `src/commands/strings.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/strings.rs`

- [ ] **Step 1: Write failing tests**

Tests: `getrange_with_negative_indices`, `getrange_start_greater_than_end_returns_empty`, `getrange_nonexistent_returns_empty`, `setrange_pads_with_zeros`, `setrange_negative_offset_returns_error`, `setrange_noninteger_offset_returns_error`, `getrange_wrong_arity`, `setrange_wrong_arity`.

- [ ] **Step 2: Implement GETRANGE**

Parse start/end as i64. Read value. Apply Redis index clamping rules:
- Negative indices count from end (`-1` = last byte)
- If start > end after resolution: return empty BulkString
- If start > len-1: return empty BulkString
- Clamp end to len-1
- Return `value[start..=end]`

- [ ] **Step 3: Implement SETRANGE**

Parse offset (must be non-negative). Read existing value. Zero-pad if offset > current length. Overwrite bytes at `[offset..offset+value.len()]`. Write back. Return new length.

Check: if offset + value.len() > 512MB (536_870_912), return error.

- [ ] **Step 4: Wire into dispatch, run tests**

- [ ] **Step 5: Commit**

```
feat(m4): implement GETRANGE, SETRANGE
```

---

### Task 12: WRONGTYPE and Expiry Edge Case Tests

Add integration tests that exercise WRONGTYPE enforcement and expiry interaction.

**Files:**
- Modify: `tests/strings.rs`
- Modify: `src/storage/helpers.rs` (if needed for test setup)

- [ ] **Step 1: Write WRONGTYPE tests**

To test WRONGTYPE, we need a non-String key. Since Hash/Set/etc. commands aren't implemented yet, write ObjectMeta directly in the test using FDB:

```rust
#[tokio::test]
async fn get_wrongtype_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    // Write a Hash-type ObjectMeta directly via FDB
    // (This requires access to the test's Database + Directories)
    ctx.write_meta("hash_key", KeyType::Hash).await;

    let result: redis::RedisResult<String> = redis::cmd("GET")
        .arg("hash_key")
        .query_async(&mut con).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("WRONGTYPE"), "expected WRONGTYPE, got: {err_msg}");
}
```

Add a `write_meta` helper to `TestContext` for this purpose (writes an ObjectMeta with the given KeyType directly to FDB).

- [ ] **Step 2: Write expiry interaction tests**

```rust
#[tokio::test]
async fn get_expired_key_returns_nil() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    // SET with PX 1 (1ms TTL)
    let _: () = redis::cmd("SET").arg("k").arg("v").arg("PX").arg("1")
        .query_async(&mut con).await.unwrap();
    // Wait for expiry
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let result: Option<String> = redis::cmd("GET").arg("k")
        .query_async(&mut con).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn exists_expired_key_returns_zero() {
    // Similar — SET with PX 1, sleep, EXISTS returns 0
}

#[tokio::test]
async fn incr_expired_key_starts_from_zero() {
    // SET counter 42 PX 1, sleep, INCR counter → 1
}
```

- [ ] **Step 3: Write chunking test**

```rust
#[tokio::test]
async fn large_value_chunking() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let big_val = "x".repeat(500_000); // 500KB = 5 chunks
    let _: () = redis::cmd("SET").arg("big").arg(&big_val)
        .query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("big")
        .query_async(&mut con).await.unwrap();
    assert_eq!(val, big_val);
}
```

- [ ] **Step 4: Run all tests**

Run: `just test`
Expected: All ~60 integration tests pass in < 2 seconds.

- [ ] **Step 5: Commit**

```
test(m4): add WRONGTYPE, expiry, and chunking integration tests
```

---

### Task 13: Acceptance Tests (Property-Based + Randomized)

**Files:**
- Create: `tests/accept_strings.rs`

- [ ] **Step 1: Create acceptance test file with property tests**

```rust
//! Acceptance tests for string commands — property-based and randomized.
//!
//! Run via `just accept`. These tests generate thousands of randomized
//! cases to find bugs that targeted integration tests miss.

#[path = "harness/mod.rs"]
#[allow(dead_code)]
mod harness;

use harness::TestContext;
use proptest::prelude::*;
use std::collections::HashMap;

// ── Property: SET/GET round-trip ──────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn accept_set_get_roundtrip(
        key in prop::collection::vec(any::<u8>(), 1..100),
        value in prop::collection::vec(any::<u8>(), 0..10_000),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;
            let mut con = ctx.connection().await;

            let _: () = redis::cmd("SET").arg(&key).arg(&value)
                .query_async(&mut con).await.unwrap();
            let got: Vec<u8> = redis::cmd("GET").arg(&key)
                .query_async(&mut con).await.unwrap();
            prop_assert_eq!(got, value);
            Ok(())
        }).unwrap();
    }
}

// ── Property: INCR commutativity ──────────────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn accept_incr_commutativity(a in 1i64..1000, b in 1i64..1000) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;
            let mut con = ctx.connection().await;

            // Path 1: INCRBY a then INCRBY b
            let _: i64 = redis::cmd("INCRBY").arg("k1").arg(a)
                .query_async(&mut con).await.unwrap();
            let r1: i64 = redis::cmd("INCRBY").arg("k1").arg(b)
                .query_async(&mut con).await.unwrap();

            // Path 2: INCRBY b then INCRBY a
            let _: i64 = redis::cmd("INCRBY").arg("k2").arg(b)
                .query_async(&mut con).await.unwrap();
            let r2: i64 = redis::cmd("INCRBY").arg("k2").arg(a)
                .query_async(&mut con).await.unwrap();

            prop_assert_eq!(r1, r2);
            prop_assert_eq!(r1, a + b);
            Ok(())
        }).unwrap();
    }
}

// ── Property: STRLEN matches value length ─────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn accept_strlen_matches(
        key in "[a-z]{1,10}",
        value in prop::collection::vec(any::<u8>(), 0..5_000),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;
            let mut con = ctx.connection().await;

            let _: () = redis::cmd("SET").arg(key.as_str()).arg(&value)
                .query_async(&mut con).await.unwrap();
            let len: i64 = redis::cmd("STRLEN").arg(key.as_str())
                .query_async(&mut con).await.unwrap();
            prop_assert_eq!(len as usize, value.len());
            Ok(())
        }).unwrap();
    }
}

// ── Randomized command sequences ──────────────────────────────────

#[tokio::test]
async fn accept_randomized_string_operations() {
    use rand::Rng;

    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let mut model: HashMap<String, Vec<u8>> = HashMap::new();
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let key = format!("k{}", rng.gen_range(0..10));
        match rng.gen_range(0..5) {
            0 => {
                // SET
                let val: Vec<u8> = (0..rng.gen_range(1..100)).map(|_| rng.gen()).collect();
                let _: () = redis::cmd("SET").arg(&key).arg(&val)
                    .query_async(&mut con).await.unwrap();
                model.insert(key, val);
            }
            1 => {
                // GET
                let result: Option<Vec<u8>> = redis::cmd("GET").arg(&key)
                    .query_async(&mut con).await.unwrap();
                let expected = model.get(&key).cloned();
                assert_eq!(result, expected, "GET {key} mismatch");
            }
            2 => {
                // DEL
                let result: i64 = redis::cmd("DEL").arg(&key)
                    .query_async(&mut con).await.unwrap();
                let expected = if model.remove(&key).is_some() { 1 } else { 0 };
                assert_eq!(result, expected, "DEL {key} mismatch");
            }
            3 => {
                // EXISTS
                let result: i64 = redis::cmd("EXISTS").arg(&key)
                    .query_async(&mut con).await.unwrap();
                let expected = if model.contains_key(&key) { 1 } else { 0 };
                assert_eq!(result, expected, "EXISTS {key} mismatch");
            }
            4 => {
                // APPEND
                let suffix: Vec<u8> = (0..rng.gen_range(1..20)).map(|_| rng.gen()).collect();
                let result: i64 = redis::cmd("APPEND").arg(&key).arg(&suffix)
                    .query_async(&mut con).await.unwrap();
                let entry = model.entry(key).or_insert_with(Vec::new);
                entry.extend_from_slice(&suffix);
                assert_eq!(result as usize, entry.len(), "APPEND length mismatch");
            }
            _ => unreachable!(),
        }
    }
}

// ── Chunking boundary tests ───────────────────────────────────────

#[tokio::test]
async fn accept_chunking_boundaries() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    for size in [99_999, 100_000, 100_001, 200_000, 300_001] {
        let val = vec![0xABu8; size];
        let key = format!("chunk_{size}");
        let _: () = redis::cmd("SET").arg(&key).arg(&val)
            .query_async(&mut con).await.unwrap();
        let got: Vec<u8> = redis::cmd("GET").arg(&key)
            .query_async(&mut con).await.unwrap();
        assert_eq!(got.len(), size, "size mismatch for {size}");
        assert_eq!(got, val, "content mismatch for {size}");
    }
}
```

- [ ] **Step 2: Run acceptance tests**

Run: `just accept`
Expected: All property tests and randomized sequences pass in < 15 seconds.

- [ ] **Step 3: Commit**

```
test(m4): add property-based and randomized acceptance tests for strings
```

---

### Task 14: Smoke Test Update

**Files:**
- Modify: `examples/smoke.rs`

- [ ] **Step 1: Add string command happy paths to smoke test**

Add sections for SET, GET, INCR/DECR, MSET/MGET, APPEND, STRLEN, GETRANGE, SETRANGE, DEL, EXISTS, SETNX, INCRBYFLOAT as described in the spec section 5.4.

- [ ] **Step 2: Add error path smoke tests**

Add all the error cases from the spec: wrong arity, SET flag conflicts, invalid TTLs, type mismatches, NaN/Inf, negative offsets. After each error, verify the server is still alive with a PING.

- [ ] **Step 3: Run smoke test**

Run: `just smoke`
Expected: All checks pass, server stays alive through all error cases.

- [ ] **Step 4: Commit**

```
test(m4): expand smoke tests with string commands and error cases
```

---

### Task 15: Benchmarks

**Files:**
- Modify: `benches/commands.rs`

- [ ] **Step 1: Add string command benchmarks**

Add benchmarks that measure full-stack latency (connect to real server, send command, receive response):

```rust
fn bench_set_get_small(c: &mut Criterion) {
    // Benchmark SET + GET of a 64-byte value against a real server
}

fn bench_set_get_1kb(c: &mut Criterion) { /* ... */ }
fn bench_incr(c: &mut Criterion) { /* ... */ }
fn bench_mset_mget_10(c: &mut Criterion) { /* ... */ }
```

These benchmarks need to start a server (like TestContext) and measure command latency. Use `criterion`'s async benchmarking support with `tokio`.

- [ ] **Step 2: Run benchmarks**

Run: `just bench`
Expected: Benchmarks complete and report latency numbers.

- [ ] **Step 3: Commit**

```
perf(m4): add string command benchmarks
```

---

### Task 16: Update Implementation Plan and Final Verification

**Files:**
- Modify: `docs/IMPLEMENTATION_PLAN.md`

- [ ] **Step 1: Run full CI pipeline**

Run: `just ci`
Expected: lint + test + accept + doc build all pass.

- [ ] **Step 2: Run smoke test**

Run: `just smoke`
Expected: All checks pass.

- [ ] **Step 3: Verify test timing**

Run: `just test` and verify it's under 2 seconds.
Run: `just accept` and verify it's under 15 seconds.

- [ ] **Step 4: Update implementation plan**

Update the M4 row in `docs/IMPLEMENTATION_PLAN.md`:

```markdown
| M4: String Commands | **Complete** | 19 commands (GET, SET, MGET, MSET, DEL, EXISTS, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETRANGE, SETRANGE, SETNX, SETEX, PSETEX, GETDEL). Full SET flags (NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET). ~60 integration tests, property-based acceptance tests, expanded smoke tests, string command benchmarks. |
```

Add a "What's been built (M4)" section documenting the architecture changes, command count, and test counts.

- [ ] **Step 5: Commit**

```
docs(m4): update implementation plan — M4 complete
```
