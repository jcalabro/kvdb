# M5: Key Management & TTL — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 18 key management and TTL commands (EXPIRE, TTL, PERSIST, TYPE, RENAME, SELECT, FLUSHDB, etc.) plus a background expiry worker that actively cleans up expired keys.

**Architecture:** New `src/commands/keys.rs` module for all 18 command handlers, following the established pattern from `strings.rs`. New `src/storage/namespace.rs` for a shared directory cache enabling SELECT. New `src/ttl/worker.rs` for the background expiry worker. Commands are wired into the existing dispatch table in `src/commands/mod.rs`.

**Tech Stack:** Rust, tokio, FoundationDB (via `foundationdb` crate), `bytes`, `redis` crate for tests.

**Spec:** `docs/m5-key-management-ttl-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `src/storage/namespace.rs` | Create | `NamespaceCache` — shared directory cache for SELECT |
| `src/storage/mod.rs` | Modify | Add `pub mod namespace;` and re-export |
| `src/commands/keys.rs` | Create | All 18 M5 command handlers |
| `src/commands/mod.rs` | Modify | Add `pub mod keys;`, dispatch entries, metric labels |
| `src/ttl/worker.rs` | Create | Background expiry worker |
| `src/ttl/mod.rs` | Modify | Add `pub mod worker;`, re-export worker entry point |
| `src/server/listener.rs` | Modify | Create NamespaceCache, pass to connections, spawn expiry worker |
| `src/server/connection.rs` | Modify | Add NamespaceCache to ConnectionState |
| `src/observability/metrics.rs` | Modify | Add expiry worker metrics |
| `tests/keys.rs` | Create | Integration tests for all 18 commands |
| `tests/accept_keys.rs` | Create | Acceptance tests (property-based, randomized) |
| `examples/loadgen.rs` | Modify | Add TTL commands to workload mix |

---

### Task 1: NamespaceCache for SELECT

**Files:**
- Create: `src/storage/namespace.rs`
- Modify: `src/storage/mod.rs`

This is a prerequisite for SELECT, the background worker, and FLUSHALL. It provides a shared cache of opened `Directories` keyed by namespace index.

- [ ] **Step 1: Create `src/storage/namespace.rs`**

```rust
//! Shared directory cache for multi-namespace support.
//!
//! The `NamespaceCache` holds opened `Directories` handles for each
//! namespace (0-15). Directories are opened lazily on first access
//! (via SELECT) and cached for reuse across all connections.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use super::database::Database;
use super::directories::Directories;
use crate::error::StorageError;

/// Shared cache of opened FDB directory handles, one per namespace.
///
/// Thread-safe via `Arc<RwLock<...>>`. The `RwLock` allows concurrent
/// reads (the common case — most connections use the same namespaces)
/// with exclusive access only when opening a new namespace.
#[derive(Clone)]
pub struct NamespaceCache {
    db: Database,
    root_prefix: String,
    cache: Arc<RwLock<HashMap<u8, Directories>>>,
}

impl NamespaceCache {
    /// Create a new cache, seeding it with the given initial namespace.
    pub fn new(db: Database, root_prefix: String, initial_dirs: Directories) -> Self {
        let mut map = HashMap::new();
        map.insert(initial_dirs.namespace, initial_dirs);
        Self {
            db,
            root_prefix,
            cache: Arc::new(RwLock::new(map)),
        }
    }

    /// Get the `Directories` for a namespace, opening them if not cached.
    pub async fn get(&self, namespace: u8) -> Result<Directories, StorageError> {
        // Fast path: read lock.
        {
            let cache = self.cache.read().await;
            if let Some(dirs) = cache.get(&namespace) {
                return Ok(dirs.clone());
            }
        }

        // Slow path: open directories and insert under write lock.
        let mut cache = self.cache.write().await;
        // Double-check after acquiring write lock (another task may have opened it).
        if let Some(dirs) = cache.get(&namespace) {
            return Ok(dirs.clone());
        }

        let dirs = Directories::open(&self.db, namespace, &self.root_prefix).await?;
        cache.insert(namespace, dirs.clone());
        Ok(dirs)
    }

    /// Return all currently cached namespaces (for FLUSHALL and expiry worker).
    pub async fn cached_namespaces(&self) -> Vec<Directories> {
        let cache = self.cache.read().await;
        cache.values().cloned().collect()
    }

    /// Return a reference to the underlying database handle.
    pub fn db(&self) -> &Database {
        &self.db
    }
}
```

- [ ] **Step 2: Export from `src/storage/mod.rs`**

Add to `src/storage/mod.rs`:
```rust
pub mod namespace;
pub use namespace::NamespaceCache;
```

- [ ] **Step 3: Verify it compiles**

Run: `cargo check`
Expected: compiles with no errors

- [ ] **Step 4: Commit**

```bash
git add src/storage/namespace.rs src/storage/mod.rs
git commit -m "feat(m5): add NamespaceCache for multi-namespace SELECT support"
```

---

### Task 2: Wire NamespaceCache into server and ConnectionState

**Files:**
- Modify: `src/server/connection.rs`
- Modify: `src/server/listener.rs`

- [ ] **Step 1: Add NamespaceCache to ConnectionState**

In `src/server/connection.rs`, add `NamespaceCache` to the struct and constructor:

```rust
use crate::storage::{Database, Directories, NamespaceCache};

pub struct ConnectionState {
    pub protocol_version: u8,
    pub selected_db: u8,
    pub db: Database,
    pub dirs: Directories,
    pub ns_cache: NamespaceCache,
}

impl ConnectionState {
    pub fn new(db: Database, dirs: Directories, ns_cache: NamespaceCache) -> Self {
        Self {
            protocol_version: 2,
            selected_db: 0,
            db,
            dirs,
            ns_cache,
        }
    }
}
```

Update `connection::handle` signature to accept `NamespaceCache`:

```rust
pub async fn handle(
    mut socket: TcpStream,
    addr: SocketAddr,
    db: Database,
    dirs: Directories,
    ns_cache: NamespaceCache,
) -> anyhow::Result<()>
```

And pass it through to `run_loop` and `ConnectionState::new`.

Update `default_for_test()` to also create a `NamespaceCache`.

- [ ] **Step 2: Update listener to create and pass NamespaceCache**

In `src/server/listener.rs`, after opening namespace 0 directories:

```rust
use crate::storage::NamespaceCache;

// After: let dirs = dirs.expect("directory open succeeded within retry limit");
let ns_cache = NamespaceCache::new(db.clone(), root_prefix.to_string(), dirs.clone());

// In the spawn closure, clone and pass ns_cache:
let conn_ns_cache = ns_cache.clone();
tokio::spawn(async move {
    if let Err(e) = connection::handle(socket, addr, conn_db, conn_dirs, conn_ns_cache).await {
        error!(%addr, error = %e, "connection error");
    }
    drop(permit);
});
```

- [ ] **Step 3: Verify everything compiles and tests pass**

Run: `just`
Expected: lint clean, all 212 tests pass

- [ ] **Step 4: Commit**

```bash
git add src/server/connection.rs src/server/listener.rs
git commit -m "feat(m5): wire NamespaceCache into server and ConnectionState"
```

---

### Task 3: TTL read commands — TTL, PTTL, EXPIRETIME, PEXPIRETIME

**Files:**
- Create: `src/commands/keys.rs`
- Modify: `src/commands/mod.rs`
- Create: `tests/keys.rs`

Start with read-only commands — simplest to implement and test.

- [ ] **Step 1: Create `src/commands/keys.rs` with TTL/PTTL/EXPIRETIME/PEXPIRETIME**

```rust
//! Key management and TTL command handlers.
//!
//! EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST,
//! EXPIRETIME, PEXPIRETIME, TYPE, RENAME, RENAMENX, UNLINK, TOUCH,
//! DBSIZE, SELECT, FLUSHDB, FLUSHALL.

use bytes::Bytes;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::ObjectMeta;
use crate::storage::{helpers, run_transact};

// ---------------------------------------------------------------------------
// TTL key
// ---------------------------------------------------------------------------

/// TTL key -- Returns the remaining time to live of a key in seconds.
/// Returns -2 if the key does not exist, -1 if no expiry is set.
pub async fn handle_ttl(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "TTL".into() }.to_string());
    }

    match run_transact(&state.db, "TTL", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => {
                    let remaining_ms = m.expires_at_ms.saturating_sub(now);
                    Ok((remaining_ms / 1000) as i64)
                }
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// PTTL key
// ---------------------------------------------------------------------------

/// PTTL key -- Returns the remaining time to live of a key in milliseconds.
/// Returns -2 if the key does not exist, -1 if no expiry is set.
pub async fn handle_pttl(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "PTTL".into() }.to_string());
    }

    match run_transact(&state.db, "PTTL", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => {
                    let remaining_ms = m.expires_at_ms.saturating_sub(now);
                    Ok(remaining_ms as i64)
                }
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// EXPIRETIME key
// ---------------------------------------------------------------------------

/// EXPIRETIME key -- Returns the absolute Unix timestamp (seconds) at which
/// the key will expire. Returns -2 if the key does not exist, -1 if no expiry.
pub async fn handle_expiretime(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "EXPIRETIME".into(),
            }
            .to_string(),
        );
    }

    match run_transact(&state.db, "EXPIRETIME", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => Ok((m.expires_at_ms / 1000) as i64),
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// PEXPIRETIME key
// ---------------------------------------------------------------------------

/// PEXPIRETIME key -- Returns the absolute Unix timestamp (milliseconds)
/// at which the key will expire. Returns -2 if key doesn't exist, -1 if no expiry.
pub async fn handle_pexpiretime(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(
            CommandError::WrongArity {
                name: "PEXPIRETIME".into(),
            }
            .to_string(),
        );
    }

    match run_transact(&state.db, "PEXPIRETIME", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            match meta {
                None => Ok(-2i64),
                Some(m) if m.expires_at_ms == 0 => Ok(-1i64),
                Some(m) => Ok(m.expires_at_ms as i64),
            }
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
```

- [ ] **Step 2: Add `pub mod keys;` to `src/commands/mod.rs` and add dispatch entries**

Add module declaration alongside `pub mod strings;`:
```rust
pub mod keys;
```

Add to the dispatch match block:
```rust
b"TTL" => CommandResponse::Reply(keys::handle_ttl(&cmd.args, state).await),
b"PTTL" => CommandResponse::Reply(keys::handle_pttl(&cmd.args, state).await),
b"EXPIRETIME" => CommandResponse::Reply(keys::handle_expiretime(&cmd.args, state).await),
b"PEXPIRETIME" => CommandResponse::Reply(keys::handle_pexpiretime(&cmd.args, state).await),
```

Add to `metric_label_for_command`:
```rust
b"TTL" => "TTL",
b"PTTL" => "PTTL",
b"EXPIRETIME" => "EXPIRETIME",
b"PEXPIRETIME" => "PEXPIRETIME",
```

- [ ] **Step 3: Write integration tests in `tests/keys.rs`**

```rust
mod harness;

use harness::TestContext;

// ---------------------------------------------------------------------------
// TTL / PTTL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ttl_nonexistent_returns_minus_two() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let ttl: i64 = redis::cmd("TTL").arg("nosuchkey").query_async(&mut con).await.unwrap();
    assert_eq!(ttl, -2);
}

#[tokio::test]
async fn ttl_no_expiry_returns_minus_one() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").query_async(&mut con).await.unwrap();
    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(ttl, -1);
}

#[tokio::test]
async fn ttl_with_expiry_returns_remaining() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET")
        .arg("mykey").arg("val").arg("EX").arg(100)
        .query_async(&mut con).await.unwrap();
    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(ttl > 0 && ttl <= 100, "TTL was {ttl}");
}

#[tokio::test]
async fn pttl_with_expiry_returns_remaining_ms() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET")
        .arg("mykey").arg("val").arg("PX").arg(50000)
        .query_async(&mut con).await.unwrap();
    let pttl: i64 = redis::cmd("PTTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(pttl > 0 && pttl <= 50000, "PTTL was {pttl}");
}

// ---------------------------------------------------------------------------
// EXPIRETIME / PEXPIRETIME
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expiretime_nonexistent_returns_minus_two() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let val: i64 = redis::cmd("EXPIRETIME").arg("nosuchkey").query_async(&mut con).await.unwrap();
    assert_eq!(val, -2);
}

#[tokio::test]
async fn expiretime_no_expiry_returns_minus_one() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").query_async(&mut con).await.unwrap();
    let val: i64 = redis::cmd("EXPIRETIME").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(val, -1);
}

#[tokio::test]
async fn pexpiretime_returns_absolute_ms() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET")
        .arg("mykey").arg("val").arg("PX").arg(60000)
        .query_async(&mut con).await.unwrap();
    let pet: i64 = redis::cmd("PEXPIRETIME").arg("mykey").query_async(&mut con).await.unwrap();
    // Should be roughly now + 60000ms (within 5s tolerance)
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
    assert!(pet > now_ms - 5000 && pet <= now_ms + 65000, "PEXPIRETIME was {pet}");
}
```

- [ ] **Step 4: Run tests**

Run: `just`
Expected: lint clean, all tests pass (existing 212 + new TTL tests)

- [ ] **Step 5: Commit**

```bash
git add src/commands/keys.rs src/commands/mod.rs tests/keys.rs
git commit -m "feat(m5): add TTL, PTTL, EXPIRETIME, PEXPIRETIME commands"
```

---

### Task 4: TTL write commands — EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST

**Files:**
- Modify: `src/commands/keys.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/keys.rs`

- [ ] **Step 1: Add shared expire helper and 5 command handlers to `src/commands/keys.rs`**

Add after the PEXPIRETIME handler:

```rust
// ---------------------------------------------------------------------------
// Shared helper for EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT
// ---------------------------------------------------------------------------

/// Set the expiry on a key. `expires_at_ms` is the absolute timestamp.
/// Returns 1 if the timeout was set, 0 if the key does not exist.
async fn set_expire(
    args: &[Bytes],
    state: &ConnectionState,
    cmd_name: &'static str,
    resolve_expiry: impl Fn(i64) -> u64 + Send + Sync + Clone + 'static,
) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: cmd_name.into() }.to_string());
    }

    let raw_val = match parse_i64_arg(&args[1]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    match run_transact(&state.db, cmd_name, |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        let resolve = resolve_expiry.clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            let mut meta = match meta {
                Some(m) => m,
                None => return Ok(0i64),
            };

            let expires_at_ms = resolve(raw_val);
            meta.expires_at_ms = expires_at_ms;
            meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;

            // Write expire directory entry.
            let expire_key = dirs.expire_key(&key);
            tr.set(&expire_key, &expires_at_ms.to_be_bytes());

            Ok(1i64)
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

/// Parse a byte slice as an i64.
fn parse_i64_arg(arg: &Bytes) -> Result<i64, RespValue> {
    let s = std::str::from_utf8(arg)
        .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))?;
    s.parse::<i64>()
        .map_err(|_| RespValue::err("ERR value is not an integer or out of range"))
}

// ---------------------------------------------------------------------------
// EXPIRE key seconds
// ---------------------------------------------------------------------------

pub async fn handle_expire(args: &[Bytes], state: &ConnectionState) -> RespValue {
    set_expire(args, state, "EXPIRE", |secs| {
        helpers::now_ms() + (secs as u64) * 1000
    })
    .await
}

// ---------------------------------------------------------------------------
// PEXPIRE key milliseconds
// ---------------------------------------------------------------------------

pub async fn handle_pexpire(args: &[Bytes], state: &ConnectionState) -> RespValue {
    set_expire(args, state, "PEXPIRE", |ms| helpers::now_ms() + ms as u64).await
}

// ---------------------------------------------------------------------------
// EXPIREAT key timestamp
// ---------------------------------------------------------------------------

pub async fn handle_expireat(args: &[Bytes], state: &ConnectionState) -> RespValue {
    set_expire(args, state, "EXPIREAT", |secs| (secs as u64) * 1000).await
}

// ---------------------------------------------------------------------------
// PEXPIREAT key ms-timestamp
// ---------------------------------------------------------------------------

pub async fn handle_pexpireat(args: &[Bytes], state: &ConnectionState) -> RespValue {
    set_expire(args, state, "PEXPIREAT", |ms| ms as u64).await
}

// ---------------------------------------------------------------------------
// PERSIST key
// ---------------------------------------------------------------------------

/// PERSIST key -- Remove the expiry from a key. Returns 1 if the timeout
/// was removed, 0 if the key doesn't exist or has no TTL.
pub async fn handle_persist(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "PERSIST".into() }.to_string());
    }

    match run_transact(&state.db, "PERSIST", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            let mut meta = match meta {
                Some(m) => m,
                None => return Ok(0i64),
            };

            if meta.expires_at_ms == 0 {
                return Ok(0i64);
            }

            meta.expires_at_ms = 0;
            meta.write(&tr, &dirs, &key).map_err(helpers::storage_err)?;

            let expire_key = dirs.expire_key(&key);
            tr.clear(&expire_key);

            Ok(1i64)
        }
    })
    .await
    {
        Ok(val) => RespValue::Integer(val),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
```

- [ ] **Step 2: Add dispatch entries and metric labels in `src/commands/mod.rs`**

Dispatch:
```rust
b"EXPIRE" => CommandResponse::Reply(keys::handle_expire(&cmd.args, state).await),
b"PEXPIRE" => CommandResponse::Reply(keys::handle_pexpire(&cmd.args, state).await),
b"EXPIREAT" => CommandResponse::Reply(keys::handle_expireat(&cmd.args, state).await),
b"PEXPIREAT" => CommandResponse::Reply(keys::handle_pexpireat(&cmd.args, state).await),
b"PERSIST" => CommandResponse::Reply(keys::handle_persist(&cmd.args, state).await),
```

Metric labels:
```rust
b"EXPIRE" => "EXPIRE",
b"PEXPIRE" => "PEXPIRE",
b"EXPIREAT" => "EXPIREAT",
b"PEXPIREAT" => "PEXPIREAT",
b"PERSIST" => "PERSIST",
```

- [ ] **Step 3: Add integration tests to `tests/keys.rs`**

```rust
// ---------------------------------------------------------------------------
// EXPIRE / PEXPIRE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expire_sets_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("EXPIRE").arg("mykey").arg(10).query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(ttl > 0 && ttl <= 10);
}

#[tokio::test]
async fn expire_nonexistent_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: i64 = redis::cmd("EXPIRE").arg("nosuchkey").arg(10).query_async(&mut con).await.unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn pexpire_sets_ttl_ms() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("PEXPIRE").arg("mykey").arg(50000).query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
    let pttl: i64 = redis::cmd("PTTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert!(pttl > 0 && pttl <= 50000);
}

// ---------------------------------------------------------------------------
// EXPIREAT / PEXPIREAT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn expireat_sets_absolute_expiry() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").query_async(&mut con).await.unwrap();
    let future_ts = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 100) as i64;
    let result: i64 = redis::cmd("EXPIREAT").arg("mykey").arg(future_ts).query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
    let et: i64 = redis::cmd("EXPIRETIME").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(et, future_ts);
}

// ---------------------------------------------------------------------------
// PERSIST
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persist_removes_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").arg("EX").arg(100)
        .query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("PERSIST").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
    let ttl: i64 = redis::cmd("TTL").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(ttl, -1);
}

#[tokio::test]
async fn persist_no_ttl_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("PERSIST").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(result, 0);
}

#[tokio::test]
async fn persist_nonexistent_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: i64 = redis::cmd("PERSIST").arg("nosuchkey").query_async(&mut con).await.unwrap();
    assert_eq!(result, 0);
}
```

- [ ] **Step 4: Run tests**

Run: `just`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add src/commands/keys.rs src/commands/mod.rs tests/keys.rs
git commit -m "feat(m5): add EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, PERSIST commands"
```

---

### Task 5: TYPE, TOUCH, UNLINK, DBSIZE

**Files:**
- Modify: `src/commands/keys.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/keys.rs`

- [ ] **Step 1: Add TYPE, TOUCH, UNLINK, DBSIZE handlers to `src/commands/keys.rs`**

```rust
// ---------------------------------------------------------------------------
// TYPE key
// ---------------------------------------------------------------------------

/// TYPE key -- Returns the type of value stored at key as a status string.
pub async fn handle_type(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "TYPE".into() }.to_string());
    }

    match run_transact(&state.db, "TYPE", |tr| {
        let dirs = state.dirs.clone();
        let key = args[0].clone();
        async move {
            let now = helpers::now_ms();
            let meta = ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)?;

            Ok(match meta {
                Some(m) => m.key_type.as_redis_type_str(),
                None => "none",
            })
        }
    })
    .await
    {
        // TYPE returns a status reply (+string\r\n), not a bulk string.
        Ok(type_str) => RespValue::SimpleString(Bytes::from(type_str)),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// UNLINK key [key ...]
// ---------------------------------------------------------------------------

/// UNLINK key [key ...] -- Delete keys. In Phase 1, identical to DEL
/// (synchronous). Will use tombstone-based async cleanup when large
/// collections are implemented (M6-M9).
pub async fn handle_unlink(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "UNLINK".into() }.to_string());
    }

    match run_transact(&state.db, "UNLINK", |tr| {
        let dirs = state.dirs.clone();
        let keys: Vec<Bytes> = args.to_vec();
        async move {
            let now = helpers::now_ms();
            let mut count: i64 = 0;
            for k in &keys {
                let deleted = helpers::delete_object(&tr, &dirs, k, now)
                    .await
                    .map_err(helpers::cmd_err)?;
                if deleted {
                    count += 1;
                }
            }
            Ok(count)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// TOUCH key [key ...]
// ---------------------------------------------------------------------------

/// TOUCH key [key ...] -- Returns the number of specified keys that exist.
/// Functionally identical to EXISTS for Phase 1.
pub async fn handle_touch(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "TOUCH".into() }.to_string());
    }

    match run_transact(&state.db, "TOUCH", |tr| {
        let dirs = state.dirs.clone();
        let keys: Vec<Bytes> = args.to_vec();
        async move {
            let now = helpers::now_ms();
            let mut count: i64 = 0;
            for k in &keys {
                let meta = ObjectMeta::read(&tr, &dirs, k, now, false)
                    .await
                    .map_err(helpers::storage_err)?;
                if meta.is_some() {
                    count += 1;
                }
            }
            Ok(count)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// DBSIZE
// ---------------------------------------------------------------------------

/// DBSIZE -- Returns the number of keys in the current database.
/// Performs a full scan of the meta directory, excluding expired keys.
pub async fn handle_dbsize(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if !args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "DBSIZE".into() }.to_string());
    }

    match run_transact(&state.db, "DBSIZE", |tr| {
        let dirs = state.dirs.clone();
        async move {
            let now = helpers::now_ms();
            let (begin, end) = dirs.meta.range();
            let range = tr.get_range(&foundationdb::RangeOption::from((begin.as_slice(), end.as_slice())), 0, true);
            let kvs = range.await.map_err(helpers::storage_err)?;

            let mut count: i64 = 0;
            for kv in kvs.iter() {
                if let Ok(meta) = ObjectMeta::deserialize(kv.value()) {
                    if !meta.is_expired(now) {
                        count += 1;
                    }
                }
            }
            Ok(count)
        }
    })
    .await
    {
        Ok(count) => RespValue::Integer(count),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}
```

Note: DBSIZE's `get_range` usage may need adjustment based on the exact FDB Rust API. The snapshot flag and iteration approach may differ. The implementer should check the `foundationdb` crate API for `get_range` and use snapshot reads with the correct iteration pattern. A simpler approach may be to use `tr.get_ranges()` which returns a stream, or iterate with `RangeOption` limits.

- [ ] **Step 2: Add dispatch entries and metric labels**

Dispatch:
```rust
b"TYPE" => CommandResponse::Reply(keys::handle_type(&cmd.args, state).await),
b"UNLINK" => CommandResponse::Reply(keys::handle_unlink(&cmd.args, state).await),
b"TOUCH" => CommandResponse::Reply(keys::handle_touch(&cmd.args, state).await),
b"DBSIZE" => CommandResponse::Reply(keys::handle_dbsize(&cmd.args, state).await),
```

Metric labels:
```rust
b"TYPE" => "TYPE",
b"UNLINK" => "UNLINK",
b"TOUCH" => "TOUCH",
b"DBSIZE" => "DBSIZE",
```

- [ ] **Step 3: Add integration tests**

```rust
// ---------------------------------------------------------------------------
// TYPE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn type_string_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("val").query_async(&mut con).await.unwrap();
    let t: String = redis::cmd("TYPE").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(t, "string");
}

#[tokio::test]
async fn type_nonexistent_returns_none() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let t: String = redis::cmd("TYPE").arg("nosuchkey").query_async(&mut con).await.unwrap();
    assert_eq!(t, "none");
}

// ---------------------------------------------------------------------------
// UNLINK
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unlink_deletes_keys() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let count: i64 = redis::cmd("UNLINK").arg("a").arg("b").arg("c")
        .query_async(&mut con).await.unwrap();
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------------
// TOUCH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn touch_returns_existing_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let count: i64 = redis::cmd("TOUCH").arg("a").arg("b").arg("c")
        .query_async(&mut con).await.unwrap();
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------------
// DBSIZE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dbsize_empty_returns_zero() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 0);
}

#[tokio::test]
async fn dbsize_reflects_key_count() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("c").arg("3").query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 3);
    let _: () = redis::cmd("DEL").arg("b").query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 2);
}
```

- [ ] **Step 4: Run tests**

Run: `just`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add src/commands/keys.rs src/commands/mod.rs tests/keys.rs
git commit -m "feat(m5): add TYPE, UNLINK, TOUCH, DBSIZE commands"
```

---

### Task 6: RENAME, RENAMENX

**Files:**
- Modify: `src/commands/keys.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/keys.rs`

- [ ] **Step 1: Add RENAME and RENAMENX handlers to `src/commands/keys.rs`**

```rust
// ---------------------------------------------------------------------------
// RENAME source destination
// ---------------------------------------------------------------------------

/// RENAME source destination -- Atomically renames a key.
/// Returns an error if source doesn't exist.
pub async fn handle_rename(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "RENAME".into() }.to_string());
    }

    match run_transact(&state.db, "RENAME", |tr| {
        let dirs = state.dirs.clone();
        let src = args[0].clone();
        let dst = args[1].clone();
        async move {
            rename_impl(&tr, &dirs, &src, &dst, false).await
        }
    })
    .await
    {
        Ok(RenameResult::Ok) => RespValue::ok(),
        Ok(RenameResult::DestExists) => RespValue::Integer(0), // only for RENAMENX
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// RENAMENX source destination
// ---------------------------------------------------------------------------

/// RENAMENX source destination -- Renames a key only if the destination
/// does not exist. Returns 1 if renamed, 0 if destination already exists.
pub async fn handle_renamenx(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err(CommandError::WrongArity { name: "RENAMENX".into() }.to_string());
    }

    match run_transact(&state.db, "RENAMENX", |tr| {
        let dirs = state.dirs.clone();
        let src = args[0].clone();
        let dst = args[1].clone();
        async move {
            rename_impl(&tr, &dirs, &src, &dst, true).await
        }
    })
    .await
    {
        Ok(RenameResult::Ok) => RespValue::Integer(1),
        Ok(RenameResult::DestExists) => RespValue::Integer(0),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

enum RenameResult {
    Ok,
    DestExists,
}

/// Shared implementation for RENAME/RENAMENX.
///
/// For Phase 1, only string keys are supported. The implementation reads
/// the source value, writes it to the destination, and deletes the source.
/// If `nx` is true, returns `DestExists` when the destination already exists.
async fn rename_impl(
    tr: &foundationdb::RetryableTransaction,
    dirs: &crate::storage::Directories,
    src: &[u8],
    dst: &[u8],
    nx: bool,
) -> Result<RenameResult, foundationdb::FdbBindingError> {
    let now = helpers::now_ms();

    // Read source meta (without expiry check — we want to see expired keys
    // so we can return "no such key" error, not silently succeed).
    let src_meta = ObjectMeta::read(tr, dirs, src, 0, false)
        .await
        .map_err(helpers::storage_err)?;

    let src_meta = match src_meta {
        Some(m) => m,
        None => {
            return Err(helpers::cmd_err(CommandError::Generic(
                "no such key".into(),
            )));
        }
    };

    // If source is expired, treat as non-existent.
    if src_meta.is_expired(now) {
        // Clean up the expired source while we're here.
        helpers::delete_object(tr, dirs, src, now)
            .await
            .map_err(helpers::cmd_err)?;
        return Err(helpers::cmd_err(CommandError::Generic(
            "no such key".into(),
        )));
    }

    // Check destination for RENAMENX.
    if nx {
        let dst_meta = ObjectMeta::read(tr, dirs, dst, now, false)
            .await
            .map_err(helpers::storage_err)?;
        if dst_meta.is_some() {
            return Ok(RenameResult::DestExists);
        }
    }

    // If src == dst, it's a no-op.
    if src == dst {
        return Ok(RenameResult::Ok);
    }

    // Read source data.
    let src_data = match src_meta.key_type {
        crate::storage::meta::KeyType::String => {
            crate::storage::chunking::read_chunks(
                tr,
                &dirs.obj,
                src,
                src_meta.num_chunks,
                src_meta.size_bytes,
                false,
            )
            .await
            .map_err(helpers::storage_err)?
        }
        _ => {
            // Non-string types: for Phase 1 this shouldn't happen since only
            // strings are implemented. Return error if it does.
            return Err(helpers::cmd_err(CommandError::Generic(
                "RENAME of non-string types not yet supported".into(),
            )));
        }
    };

    // Delete destination if it exists.
    helpers::delete_object(tr, dirs, dst, 0)
        .await
        .map_err(helpers::cmd_err)?;

    // Write source data to destination.
    helpers::write_string(tr, dirs, dst, &src_data, src_meta.expires_at_ms, None)
        .map_err(helpers::cmd_err)?;

    // Delete source.
    helpers::delete_object(tr, dirs, src, 0)
        .await
        .map_err(helpers::cmd_err)?;

    Ok(RenameResult::Ok)
}
```

- [ ] **Step 2: Add dispatch entries and metric labels**

Dispatch:
```rust
b"RENAME" => CommandResponse::Reply(keys::handle_rename(&cmd.args, state).await),
b"RENAMENX" => CommandResponse::Reply(keys::handle_renamenx(&cmd.args, state).await),
```

Metric labels:
```rust
b"RENAME" => "RENAME",
b"RENAMENX" => "RENAMENX",
```

- [ ] **Step 3: Add integration tests**

```rust
// ---------------------------------------------------------------------------
// RENAME
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rename_moves_key() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("src").arg("hello").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("RENAME").arg("src").arg("dst").query_async(&mut con).await.unwrap();
    let val: Option<String> = redis::cmd("GET").arg("src").query_async(&mut con).await.unwrap();
    assert_eq!(val, None);
    let val: String = redis::cmd("GET").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(val, "hello");
}

#[tokio::test]
async fn rename_nonexistent_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<()> = redis::cmd("RENAME")
        .arg("nosuchkey").arg("dst").query_async(&mut con).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn rename_preserves_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("src").arg("val").arg("EX").arg(100)
        .query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("RENAME").arg("src").arg("dst").query_async(&mut con).await.unwrap();
    let ttl: i64 = redis::cmd("TTL").arg("dst").query_async(&mut con).await.unwrap();
    assert!(ttl > 0 && ttl <= 100);
}

#[tokio::test]
async fn rename_overwrites_destination() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("src").arg("new").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("dst").arg("old").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("RENAME").arg("src").arg("dst").query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(val, "new");
}

// ---------------------------------------------------------------------------
// RENAMENX
// ---------------------------------------------------------------------------

#[tokio::test]
async fn renamenx_succeeds_when_dst_missing() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("src").arg("val").query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("RENAMENX").arg("src").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(result, 1);
}

#[tokio::test]
async fn renamenx_fails_when_dst_exists() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("src").arg("new").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("dst").arg("old").query_async(&mut con).await.unwrap();
    let result: i64 = redis::cmd("RENAMENX").arg("src").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(result, 0);
    // Source should still exist.
    let val: String = redis::cmd("GET").arg("src").query_async(&mut con).await.unwrap();
    assert_eq!(val, "new");
}
```

- [ ] **Step 4: Run tests**

Run: `just`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add src/commands/keys.rs src/commands/mod.rs tests/keys.rs
git commit -m "feat(m5): add RENAME and RENAMENX commands"
```

---

### Task 7: SELECT, FLUSHDB, FLUSHALL

**Files:**
- Modify: `src/commands/keys.rs`
- Modify: `src/commands/mod.rs`
- Modify: `tests/keys.rs`

These commands need `&mut ConnectionState` (SELECT modifies state) and access to `NamespaceCache`.

- [ ] **Step 1: Add SELECT, FLUSHDB, FLUSHALL handlers**

Note: SELECT requires `&mut ConnectionState` since it modifies `dirs` and `selected_db`. The dispatch function already passes `state: &mut ConnectionState`, so this works. Add to `src/commands/keys.rs`:

```rust
// ---------------------------------------------------------------------------
// SELECT index
// ---------------------------------------------------------------------------

/// SELECT index -- Switch to a different database (0-15).
pub async fn handle_select(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.len() != 1 {
        return RespValue::err(CommandError::WrongArity { name: "SELECT".into() }.to_string());
    }

    let index = match parse_i64_arg(&args[0]) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    if !(0..=15).contains(&index) {
        return RespValue::err("ERR DB index is out of range");
    }

    let ns = index as u8;

    match state.ns_cache.get(ns).await {
        Ok(dirs) => {
            state.dirs = dirs;
            state.selected_db = ns;
            RespValue::ok()
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// FLUSHDB [ASYNC]
// ---------------------------------------------------------------------------

/// FLUSHDB [ASYNC] -- Delete all keys in the current database.
pub async fn handle_flushdb(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Accept but ignore ASYNC flag.
    if args.len() > 1 {
        return RespValue::err(CommandError::WrongArity { name: "FLUSHDB".into() }.to_string());
    }

    match run_transact(&state.db, "FLUSHDB", |tr| {
        let dirs = state.dirs.clone();
        async move {
            flush_namespace(&tr, &dirs);
            Ok(())
        }
    })
    .await
    {
        Ok(()) => RespValue::ok(),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// FLUSHALL [ASYNC]
// ---------------------------------------------------------------------------

/// FLUSHALL [ASYNC] -- Delete all keys in all databases.
pub async fn handle_flushall(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() > 1 {
        return RespValue::err(CommandError::WrongArity { name: "FLUSHALL".into() }.to_string());
    }

    let all_dirs = state.ns_cache.cached_namespaces().await;

    match run_transact(&state.db, "FLUSHALL", |tr| {
        let dirs_list = all_dirs.clone();
        async move {
            for dirs in &dirs_list {
                flush_namespace(&tr, dirs);
            }
            Ok(())
        }
    })
    .await
    {
        Ok(()) => RespValue::ok(),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

/// Clear all 8 subspaces for a namespace within the current transaction.
fn flush_namespace(tr: &foundationdb::RetryableTransaction, dirs: &crate::storage::Directories) {
    for subspace in [
        &dirs.meta, &dirs.obj, &dirs.hash, &dirs.set,
        &dirs.zset, &dirs.zset_idx, &dirs.list, &dirs.expire,
    ] {
        let (begin, end) = subspace.range();
        tr.clear_range(&begin, &end);
    }
}
```

- [ ] **Step 2: Update dispatch — SELECT needs `&mut state`**

SELECT is special because it mutates ConnectionState. In `src/commands/mod.rs`, the dispatch match already receives `state: &mut ConnectionState`, so:

```rust
b"SELECT" => CommandResponse::Reply(keys::handle_select(&cmd.args, state).await),
b"FLUSHDB" => CommandResponse::Reply(keys::handle_flushdb(&cmd.args, state).await),
b"FLUSHALL" => CommandResponse::Reply(keys::handle_flushall(&cmd.args, state).await),
```

Metric labels:
```rust
b"SELECT" => "SELECT",
b"FLUSHDB" => "FLUSHDB",
b"FLUSHALL" => "FLUSHALL",
```

- [ ] **Step 3: Add integration tests**

```rust
// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_isolates_databases() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("mykey").arg("db0").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
    let val: Option<String> = redis::cmd("GET").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(val, None); // not visible in db 1
    let _: () = redis::cmd("SET").arg("mykey").arg("db1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("mykey").query_async(&mut con).await.unwrap();
    assert_eq!(val, "db0"); // original value in db 0
}

#[tokio::test]
async fn select_out_of_range_returns_error() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let result: redis::RedisResult<()> = redis::cmd("SELECT").arg(16).query_async(&mut con).await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// FLUSHDB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flushdb_clears_current_db() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 0);
}

#[tokio::test]
async fn flushdb_does_not_affect_other_db() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    // Set key in db 0
    let _: () = redis::cmd("SET").arg("keep").arg("val").query_async(&mut con).await.unwrap();
    // Switch to db 1, set key, flush
    let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("gone").arg("val").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();
    // Back to db 0 — key should still exist
    let _: () = redis::cmd("SELECT").arg(0).query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("keep").query_async(&mut con).await.unwrap();
    assert_eq!(val, "val");
}

// ---------------------------------------------------------------------------
// FLUSHALL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flushall_clears_all_dbs() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("a").arg("1").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("b").arg("2").query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SELECT").arg(0).query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("FLUSHALL").query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 0);
    let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 0);
}
```

- [ ] **Step 4: Run tests**

Run: `just`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add src/commands/keys.rs src/commands/mod.rs tests/keys.rs
git commit -m "feat(m5): add SELECT, FLUSHDB, FLUSHALL commands"
```

---

### Task 8: Background expiry worker

**Files:**
- Create: `src/ttl/worker.rs`
- Modify: `src/ttl/mod.rs`
- Modify: `src/server/listener.rs`
- Modify: `src/observability/metrics.rs`
- Modify: `tests/keys.rs`

- [ ] **Step 1: Add expiry worker metrics to `src/observability/metrics.rs`**

Add inside the `lazy_static!` block:

```rust
pub static ref EXPIRY_SCAN_DURATION_SECONDS: Histogram = register_histogram!(
    "kvdb_expiry_scan_duration_seconds",
    "Time spent per background expiry scan iteration"
)
.unwrap();
```

(The `EXPIRED_KEYS_TOTAL` counter already exists in the metrics file.)

- [ ] **Step 2: Create `src/ttl/worker.rs`**

```rust
//! Background expiry worker.
//!
//! Scans the `expire/` directory periodically and deletes keys whose
//! TTL has passed. Complements the lazy expiry check that runs on
//! every read.

use foundationdb::RangeOption;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::observability::metrics;
use crate::storage::helpers;
use crate::storage::meta::ObjectMeta;
use crate::storage::{Database, Directories, NamespaceCache};
use crate::ttl::ExpiryConfig;

/// Run the background expiry worker until the cancellation token is triggered.
pub async fn run(
    ns_cache: NamespaceCache,
    config: ExpiryConfig,
    cancel: CancellationToken,
) {
    info!(
        scan_interval_ms = config.scan_interval_ms,
        batch_size = config.batch_size,
        "background expiry worker started"
    );

    let interval = tokio::time::Duration::from_millis(config.scan_interval_ms);

    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = cancel.cancelled() => {
                info!("background expiry worker shutting down");
                return;
            }
        }

        let namespaces = ns_cache.cached_namespaces().await;
        for dirs in &namespaces {
            if cancel.is_cancelled() {
                return;
            }
            if let Err(e) = scan_one_namespace(ns_cache.db(), dirs, config.batch_size).await {
                error!(namespace = dirs.namespace, error = %e, "expiry scan failed");
            }
        }
    }
}

/// Scan expire/ directory for one namespace and delete expired keys.
async fn scan_one_namespace(
    db: &Database,
    dirs: &Directories,
    batch_size: usize,
) -> Result<(), crate::error::StorageError> {
    let timer = std::time::Instant::now();

    let (begin, end) = dirs.expire.range();
    let now = helpers::now_ms();

    // Read expire entries in a snapshot transaction (no conflict).
    let expire_entries: Vec<(Vec<u8>, u64)> = {
        let trx = db.inner().create_trx().map_err(crate::error::StorageError::Fdb)?;

        let range_opt = RangeOption::from((begin.as_slice(), end.as_slice()));
        let kvs = trx
            .get_range(&range_opt, batch_size as i32, true)
            .await
            .map_err(crate::error::StorageError::Fdb)?;

        kvs.iter()
            .filter_map(|kv| {
                if kv.value().len() == 8 {
                    let ts = u64::from_be_bytes(kv.value().try_into().ok()?);
                    if now >= ts {
                        Some((kv.key().to_vec(), ts))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    };

    if expire_entries.is_empty() {
        metrics::EXPIRY_SCAN_DURATION_SECONDS.observe(timer.elapsed().as_secs_f64());
        return Ok(());
    }

    let expired_count = expire_entries.len();
    debug!(namespace = dirs.namespace, expired_count, "cleaning expired keys");

    // Delete expired keys in a write transaction.
    // We need to unpack the Redis key from the expire FDB key.
    let dirs_clone = dirs.clone();
    crate::storage::run_transact(db, "EXPIRE_CLEANUP", |tr| {
        let dirs = dirs_clone.clone();
        let entries = expire_entries.clone();
        async move {
            for (fdb_expire_key, _ts) in &entries {
                // Extract the Redis key from the expire FDB key by unpacking.
                let redis_key: (Vec<u8>,) = dirs
                    .expire
                    .unpack(fdb_expire_key)
                    .map_err(|e| {
                        foundationdb::FdbBindingError::CustomError(Box::new(
                            crate::error::StorageError::DataCorruption(format!(
                                "failed to unpack expire key: {e:?}"
                            )),
                        ))
                    })?;

                // Delete the key's data, meta, and expire entry.
                helpers::delete_object(&tr, &dirs, &redis_key.0, 0)
                    .await
                    .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))?;
            }
            Ok(())
        }
    })
    .await?;

    metrics::EXPIRED_KEYS_TOTAL
        .with_label_values(&["active"])
        .inc_by(expired_count as u64);
    metrics::EXPIRY_SCAN_DURATION_SECONDS.observe(timer.elapsed().as_secs_f64());

    Ok(())
}
```

Note: The exact FDB API for `get_range`, `unpack`, and range iteration may need adjustment during implementation. The `foundationdb` crate's API for range reads returns `FdbValues` which can be iterated. The `DirectorySubspace::unpack` method extracts the tuple-encoded Redis key. The implementer should verify these APIs against the `foundationdb` crate docs.

- [ ] **Step 3: Update `src/ttl/mod.rs`**

```rust
//! TTL and expiration management.
//!
//! Implements a hybrid lazy + active expiration strategy:
//!
//! - **Lazy**: every read checks `ObjectMeta.expires_at_ms` and treats
//!   expired keys as non-existent.
//! - **Active**: a background worker scans the `expire/` directory every
//!   250ms and cleans up expired keys in batches.

pub mod worker;

/// Configuration for the background expiry worker.
#[derive(Debug, Clone)]
pub struct ExpiryConfig {
    /// How often the worker scans for expired keys.
    pub scan_interval_ms: u64,
    /// Maximum keys to process per scan batch.
    pub batch_size: usize,
}

impl Default for ExpiryConfig {
    fn default() -> Self {
        Self {
            scan_interval_ms: 250,
            batch_size: 1000,
        }
    }
}
```

- [ ] **Step 4: Spawn the worker in `src/server/listener.rs`**

Add after creating the NamespaceCache:

```rust
use tokio_util::sync::CancellationToken;
use crate::ttl;

// After: let ns_cache = NamespaceCache::new(...);

// Spawn background expiry worker.
let cancel_token = CancellationToken::new();
let worker_cancel = cancel_token.clone();
let worker_ns_cache = ns_cache.clone();
tokio::spawn(async move {
    ttl::worker::run(worker_ns_cache, ttl::ExpiryConfig::default(), worker_cancel).await;
});
```

Update the shutdown path to cancel the worker:

```rust
// In the shutdown select branch:
_ = shutdown.recv() => {
    info!("shutting down listener");
    cancel_token.cancel();
    break;
}
```

Add `tokio-util` to dependencies in `Cargo.toml`:
```toml
tokio-util = { version = "0.7", features = ["rt"] }
```

- [ ] **Step 5: Add background expiry integration test**

Add to `tests/keys.rs`:

```rust
#[tokio::test]
async fn background_expiry_cleans_up_keys() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    // Set key with 200ms TTL.
    let _: () = redis::cmd("SET")
        .arg("ephemeral").arg("val").arg("PX").arg(200)
        .query_async(&mut con).await.unwrap();
    // Verify it exists.
    let val: Option<String> = redis::cmd("GET").arg("ephemeral").query_async(&mut con).await.unwrap();
    assert!(val.is_some());
    // Wait for background worker (250ms interval + margin).
    tokio::time::sleep(tokio::time::Duration::from_millis(700)).await;
    // Key should be gone (either lazy or active expiry).
    let val: Option<String> = redis::cmd("GET").arg("ephemeral").query_async(&mut con).await.unwrap();
    assert_eq!(val, None);
}
```

- [ ] **Step 6: Run tests**

Run: `just`
Expected: all tests pass

- [ ] **Step 7: Commit**

```bash
git add src/ttl/ src/server/listener.rs src/observability/metrics.rs Cargo.toml tests/keys.rs
git commit -m "feat(m5): add background expiry worker"
```

---

### Task 9: Acceptance tests

**Files:**
- Create: `tests/accept_keys.rs`

- [ ] **Step 1: Write acceptance tests**

```rust
//! M5 acceptance tests — property-based and randomized.

mod harness;

use std::collections::HashMap;

use harness::TestContext;
use rand::Rng;

// ---------------------------------------------------------------------------
// TTL round-trip property
// ---------------------------------------------------------------------------

#[tokio::test]
async fn accept_ttl_roundtrip() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    for ttl_secs in [1i64, 5, 30, 100, 3600] {
        let key = format!("ttl_rt_{ttl_secs}");
        let _: () = redis::cmd("SET").arg(&key).arg("val").query_async(&mut con).await.unwrap();
        let _: i64 = redis::cmd("EXPIRE").arg(&key).arg(ttl_secs).query_async(&mut con).await.unwrap();
        let got: i64 = redis::cmd("TTL").arg(&key).query_async(&mut con).await.unwrap();
        // Allow 2 seconds of drift.
        assert!(
            got > 0 && got <= ttl_secs,
            "TTL for {ttl_secs}s was {got}"
        );
    }
}

// ---------------------------------------------------------------------------
// PERSIST idempotence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn accept_persist_idempotence() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("k").arg("v").query_async(&mut con).await.unwrap();
    // PERSIST on key with no TTL returns 0.
    let r: i64 = redis::cmd("PERSIST").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(r, 0);
    // Set TTL, PERSIST returns 1.
    let _: i64 = redis::cmd("EXPIRE").arg("k").arg(100).query_async(&mut con).await.unwrap();
    let r: i64 = redis::cmd("PERSIST").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(r, 1);
    // Second PERSIST returns 0.
    let r: i64 = redis::cmd("PERSIST").arg("k").query_async(&mut con).await.unwrap();
    assert_eq!(r, 0);
}

// ---------------------------------------------------------------------------
// RENAME preserves value and TTL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn accept_rename_preserves_value_and_ttl() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let _: () = redis::cmd("SET").arg("src").arg("hello").arg("EX").arg(100)
        .query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("RENAME").arg("src").arg("dst").query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("dst").query_async(&mut con).await.unwrap();
    assert_eq!(val, "hello");
    let ttl: i64 = redis::cmd("TTL").arg("dst").query_async(&mut con).await.unwrap();
    assert!(ttl > 0 && ttl <= 100);
}

// ---------------------------------------------------------------------------
// SELECT isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn accept_select_isolation() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    for db in 0..4u8 {
        let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();
        let _: () = redis::cmd("SET")
            .arg("shared_key")
            .arg(format!("db{db}"))
            .query_async(&mut con).await.unwrap();
    }

    // Read back — each DB should have its own value.
    for db in 0..4u8 {
        let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();
        let val: String = redis::cmd("GET").arg("shared_key").query_async(&mut con).await.unwrap();
        assert_eq!(val, format!("db{db}"));
    }

    // Clean up.
    for db in 0..4u8 {
        let _: () = redis::cmd("SELECT").arg(db).query_async(&mut con).await.unwrap();
        let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// FLUSHDB isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn accept_flushdb_isolation() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;

    // Set in db 0.
    let _: () = redis::cmd("SET").arg("safe").arg("val").query_async(&mut con).await.unwrap();
    // Set in db 1.
    let _: () = redis::cmd("SELECT").arg(1).query_async(&mut con).await.unwrap();
    let _: () = redis::cmd("SET").arg("doomed").arg("val").query_async(&mut con).await.unwrap();
    // Flush db 1.
    let _: () = redis::cmd("FLUSHDB").query_async(&mut con).await.unwrap();
    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 0);
    // db 0 should be intact.
    let _: () = redis::cmd("SELECT").arg(0).query_async(&mut con).await.unwrap();
    let val: String = redis::cmd("GET").arg("safe").query_async(&mut con).await.unwrap();
    assert_eq!(val, "val");
}

// ---------------------------------------------------------------------------
// Randomized key lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn accept_randomized_key_lifecycle() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let mut model: HashMap<String, String> = HashMap::new();
    let mut rng = rand::thread_rng();

    for _ in 0..500 {
        let key = format!("k{}", rng.gen_range(0..20));
        match rng.gen_range(0..6) {
            0 => {
                // SET
                let val = format!("v{}", rng.gen_range(0..1000));
                let _: () = redis::cmd("SET").arg(&key).arg(&val).query_async(&mut con).await.unwrap();
                model.insert(key, val);
            }
            1 => {
                // GET
                let got: Option<String> = redis::cmd("GET").arg(&key).query_async(&mut con).await.unwrap();
                assert_eq!(got.as_deref(), model.get(&key).map(|s| s.as_str()));
            }
            2 => {
                // DEL
                let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap();
                model.remove(&key);
            }
            3 => {
                // EXPIRE (short-lived, but won't expire during test)
                if model.contains_key(&key) {
                    let _: i64 = redis::cmd("EXPIRE").arg(&key).arg(3600).query_async(&mut con).await.unwrap();
                }
            }
            4 => {
                // PERSIST
                let _: i64 = redis::cmd("PERSIST").arg(&key).query_async(&mut con).await.unwrap();
            }
            5 => {
                // RENAME (within model)
                let dst = format!("k{}", rng.gen_range(0..20));
                if model.contains_key(&key) && key != dst {
                    let _: () = redis::cmd("RENAME").arg(&key).arg(&dst).query_async(&mut con).await.unwrap();
                    if let Some(val) = model.remove(&key) {
                        model.insert(dst, val);
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    // Final verification: all model keys match server.
    for (key, expected) in &model {
        let got: String = redis::cmd("GET").arg(key).query_async(&mut con).await.unwrap();
        assert_eq!(&got, expected, "mismatch for key {key}");
    }
}

// ---------------------------------------------------------------------------
// DBSIZE accuracy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn accept_dbsize_accuracy() {
    let ctx = TestContext::new().await;
    let mut con = ctx.connection().await;
    let mut rng = rand::thread_rng();
    let mut expected_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

    for _ in 0..200 {
        let key = format!("k{}", rng.gen_range(0..50));
        if rng.gen_bool(0.7) {
            let _: () = redis::cmd("SET").arg(&key).arg("v").query_async(&mut con).await.unwrap();
            expected_keys.insert(key);
        } else {
            let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut con).await.unwrap();
            expected_keys.remove(&key);
        }
    }

    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, expected_keys.len() as i64);
}
```

- [ ] **Step 2: Run all tests including acceptance**

Run: `just ci`
Expected: lint clean, all tests pass, all acceptance tests pass

- [ ] **Step 3: Commit**

```bash
git add tests/accept_keys.rs
git commit -m "test(m5): add acceptance tests for key management & TTL"
```

---

### Task 10: Update implementation plan status and loadgen

**Files:**
- Modify: `docs/IMPLEMENTATION_PLAN.md`
- Modify: `examples/loadgen.rs`

- [ ] **Step 1: Update M5 status in implementation plan**

Change M5 row to `**Complete**` with notes summarizing what was built.

- [ ] **Step 2: Add TTL commands to loadgen workload**

Add EXPIRE, TTL, PERSIST, TYPE to the loadgen command mix (small percentages, replacing some GET weight). This exercises the new commands under load.

- [ ] **Step 3: Run final verification**

Run: `just ci`
Run: `just loadgen 10`
Expected: all tests pass, loadgen shows 0 errors with the new commands

- [ ] **Step 4: Commit and push**

```bash
git add docs/IMPLEMENTATION_PLAN.md examples/loadgen.rs
git commit -m "docs(m5): mark M5 complete, add TTL commands to loadgen"
git push
```
