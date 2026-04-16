//! Transaction command handlers: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
//!
//! Implements Redis transaction semantics on top of FoundationDB:
//!
//! - **MULTI**: Enter queuing mode. Subsequent commands are queued, not executed.
//! - **EXEC**: Execute all queued commands in a single FDB transaction.
//! - **DISCARD**: Abort the transaction and clear the queue.
//! - **WATCH**: Optimistic locking — mark keys for conflict detection.
//! - **UNWATCH**: Clear all watched keys.
//!
//! # FDB mapping
//!
//! EXEC opens a single FDB transaction and uses a two-phase approach
//! for WATCH conflict detection:
//!
//! 1. **Byte comparison** — At WATCH time, each key's raw ObjectMeta
//!    bytes are snapshot-read and stored. At EXEC time, a non-snapshot
//!    re-read compares the current bytes against the snapshot. This
//!    catches modifications that happened before the EXEC transaction's
//!    read version (which FDB's conflict ranges alone cannot detect,
//!    since the EXEC transaction is created after potentially
//!    conflicting writes).
//! 2. **FDB conflict detection** — The non-snapshot re-read also adds
//!    WATCHed keys to the transaction's read conflict set, so FDB
//!    will abort the commit if another transaction modifies a WATCHed
//!    key between the EXEC transaction's read version and commit time.
//! 3. Executes each queued command, collecting per-command results
//!    into an array (no rollback on per-command errors — matching
//!    Redis semantics).
//! 4. Commits the transaction.
//!
//! # Limitations
//!
//! - FDB imposes a 5-second transaction timeout and 10MB transaction
//!   size limit. Large MULTI/EXEC blocks may hit these limits.
//! - All queued commands share a single FDB transaction, so
//!   read-your-writes works within the block (e.g., SET then GET
//!   returns the SET value).
//! - Commands inside EXEC use non-snapshot reads, which means
//!   concurrent modifications to ANY read key (not just WATCHed keys)
//!   can cause the FDB commit to fail. When this happens, EXEC returns
//!   nil (same as a WATCH conflict). Under low contention this is rare;
//!   under high contention, clients should retry failed EXEC. This is
//!   a known deviation from Redis, where MULTI/EXEC without WATCH
//!   always succeeds.

use std::sync::Arc;

use bytes::Bytes;
use foundationdb::options::TransactionOption;
use tracing::{Instrument, info_span};

use crate::error::CommandError;
use crate::observability::metrics::{FDB_TRANSACTION_DURATION_SECONDS, TRANSACTION_OUTCOMES_TOTAL};
use crate::protocol::types::{RedisCommand, RespValue};
use crate::server::connection::ConnectionState;

use super::CommandResponse;

/// MULTI — Enter transaction mode.
///
/// Returns an error if already inside a MULTI block.
/// Moves any pre-existing WATCHed keys into the transaction state.
pub fn handle_multi(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if !args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "MULTI".into() }.to_string());
    }

    if state.transaction.is_some() {
        return RespValue::err("ERR MULTI calls can not be nested");
    }

    // Move watched keys (with their snapshots) into the transaction state.
    let watched = std::mem::take(&mut state.watched_keys);
    state.transaction = Some(crate::server::connection::TransactionState::new(watched));

    TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["started"]).inc();
    RespValue::ok()
}

/// DISCARD — Abort the current transaction.
///
/// Clears the command queue and all watched keys.
/// Returns an error if not inside a MULTI block.
pub fn handle_discard(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if !args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "DISCARD".into() }.to_string());
    }

    if state.transaction.is_none() {
        return RespValue::err("ERR DISCARD without MULTI");
    }

    // Clear everything — queue, watched keys, transaction state.
    state.transaction = None;
    state.watched_keys.clear();

    TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["discarded"]).inc();
    RespValue::ok()
}

/// WATCH key [key ...] — Mark keys for optimistic locking.
///
/// Must be called before MULTI. Returns an error if called inside
/// a MULTI block (matching Redis behavior).
///
/// At WATCH time, we snapshot each key's ObjectMeta (raw bytes) so
/// that EXEC can compare and detect modifications by other clients.
pub async fn handle_watch(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "WATCH".into() }.to_string());
    }

    if state.transaction.is_some() {
        return RespValue::err("ERR WATCH inside MULTI is not allowed");
    }

    // Snapshot each key's meta bytes at WATCH time.
    for key in args {
        // Skip if already watching this key.
        if state.watched_keys.iter().any(|w| w.key == *key) {
            continue;
        }

        let meta_fdb_key = state.dirs.meta_key(key);
        let snapshot = match state.db.inner().create_trx() {
            Ok(tr) => {
                // Use snapshot read to avoid adding to any conflict set.
                match tr.get(&meta_fdb_key, true).await {
                    Ok(val) => val.map(|v| v.to_vec()),
                    Err(e) => {
                        return RespValue::err(format!("ERR WATCH failed: {e}"));
                    }
                }
            }
            Err(e) => {
                return RespValue::err(format!("ERR WATCH failed: {e}"));
            }
        };

        state.watched_keys.push(crate::server::connection::WatchedKey {
            key: key.clone(),
            meta_snapshot: snapshot,
        });
    }

    RespValue::ok()
}

/// UNWATCH — Clear all watched keys.
///
/// Can be called inside or outside a MULTI block.
pub fn handle_unwatch(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if !args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "UNWATCH".into() }.to_string());
    }

    state.watched_keys.clear();
    if let Some(tx) = &mut state.transaction {
        tx.watched.clear();
    }

    RespValue::ok()
}

/// EXEC — Execute all queued commands atomically.
///
/// Opens a single FDB transaction, adds read conflict ranges for
/// WATCHed keys, executes each queued command, and returns an array
/// of per-command results. Returns nil if a WATCHed key was modified
/// (optimistic lock failure). Returns EXECABORT if a syntax error
/// was detected during queuing.
///
/// Note: this function returns `Pin<Box<dyn Future>>` internally to
/// break the async recursion cycle (EXEC → dispatch → EXEC). The
/// dispatch interception prevents actual recursion, but the compiler
/// can't prove that statically.
pub fn handle_exec<'a>(
    args: &'a [Bytes],
    state: &'a mut ConnectionState,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = RespValue> + Send + 'a>> {
    Box::pin(handle_exec_inner(args, state))
}

async fn handle_exec_inner(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if !args.is_empty() {
        return RespValue::err(CommandError::WrongArity { name: "EXEC".into() }.to_string());
    }

    let tx_state = match state.transaction.take() {
        Some(tx) => tx,
        None => {
            return RespValue::err("ERR EXEC without MULTI");
        }
    };

    // Always clear watched keys after EXEC (even on error/abort).
    state.watched_keys.clear();

    // If a syntax error was flagged during queuing, abort.
    if tx_state.error_flag {
        TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["exec_aborted"]).inc();
        return RespValue::err("EXECABORT Transaction discarded because of previous errors.");
    }

    // Empty queue — return empty array.
    if tx_state.queued.is_empty() {
        TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["committed"]).inc();
        return RespValue::Array(Some(vec![]));
    }

    // Execute all queued commands in a single FDB transaction.
    exec_queued(tx_state, state).await
}

/// Execute the queued commands inside a single FDB transaction.
///
/// This is the core of MULTI/EXEC. It:
/// 1. Creates a raw FDB transaction (not using `db.run()`, because
///    we need manual control over conflict ranges and commit).
/// 2. Adds read conflict ranges for WATCHed keys' meta entries.
/// 3. Executes each queued command, collecting results.
/// 4. Commits. On conflict (WATCHed key changed), returns nil.
async fn exec_queued(tx_state: crate::server::connection::TransactionState, state: &mut ConnectionState) -> RespValue {
    let span = info_span!("exec_transaction", queued = tx_state.queued.len());

    async {
        let timer = std::time::Instant::now();

        // 1. Create a raw FDB transaction.
        let tr = match state.db.inner().create_trx() {
            Ok(tr) => tr,
            Err(e) => {
                return RespValue::err(format!("ERR transaction failed: {e}"));
            }
        };

        // Set 5-second timeout.
        if let Err(e) = tr.set_option(TransactionOption::Timeout(5000)) {
            return RespValue::err(format!("ERR transaction failed: {e}"));
        }

        // 2. Check WATCHed keys for modifications (two-phase detection).
        //
        // We use NON-snapshot reads here (false), which serves two purposes:
        // a) Byte comparison catches pre-read-version modifications (which
        //    FDB's conflict ranges can't detect, since this transaction was
        //    created after the conflicting writes committed).
        // b) The non-snapshot read adds WATCHed keys to the transaction's
        //    read conflict set, so FDB will abort the commit if another
        //    transaction modifies a WATCHed key between our read version
        //    and commit time.
        for watched in &tx_state.watched {
            let meta_fdb_key = state.dirs.meta_key(&watched.key);
            let current = match tr.get(&meta_fdb_key, false).await {
                Ok(val) => val.map(|v| v.to_vec()),
                Err(e) => {
                    return RespValue::err(format!("ERR transaction failed: {e}"));
                }
            };

            if current != watched.meta_snapshot {
                // Key was modified — abort.
                let elapsed = timer.elapsed().as_secs_f64();
                FDB_TRANSACTION_DURATION_SECONDS
                    .with_label_values(&["EXEC", "conflict"])
                    .observe(elapsed);
                TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["watch_aborted"]).inc();
                return RespValue::Array(None);
            }
        }

        let tr = Arc::new(tr);

        // 3. Execute each queued command, collecting results.
        // We inject the shared transaction into state so that handlers
        // use it instead of creating their own.
        state.active_transaction = Some(Arc::clone(&tr));

        let mut results = Vec::with_capacity(tx_state.queued.len());
        for cmd in &tx_state.queued {
            let resp = exec_single_command(cmd, state).await;
            results.push(resp);
        }

        // Clear the shared transaction before commit.
        state.active_transaction = None;

        // 4. Commit the transaction.
        // We need to unwrap the Arc to get ownership for commit.
        let tr = match Arc::try_unwrap(tr) {
            Ok(tr) => tr,
            Err(_) => {
                // This shouldn't happen — all handler references should be dropped.
                return RespValue::err("ERR internal error: transaction still referenced");
            }
        };

        let elapsed = timer.elapsed().as_secs_f64();

        match tr.commit().await {
            Ok(_) => {
                FDB_TRANSACTION_DURATION_SECONDS
                    .with_label_values(&["EXEC", "committed"])
                    .observe(elapsed);
                TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["committed"]).inc();
                RespValue::Array(Some(results))
            }
            Err(commit_err) => {
                // TransactionCommitError derefs to FdbError.
                let is_conflict = commit_err.is_retryable();

                if is_conflict {
                    // Conflict detected — return nil (transaction aborted).
                    // This fires for WATCHed key modifications, but can also
                    // fire if another client modifies ANY key read by queued
                    // commands (since handlers use non-snapshot reads). See
                    // module doc "Limitations" for details.
                    FDB_TRANSACTION_DURATION_SECONDS
                        .with_label_values(&["EXEC", "conflict"])
                        .observe(elapsed);
                    TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["watch_aborted"]).inc();
                    RespValue::Array(None)
                } else {
                    FDB_TRANSACTION_DURATION_SECONDS
                        .with_label_values(&["EXEC", "error"])
                        .observe(elapsed);
                    TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["error"]).inc();
                    RespValue::err(format!("ERR transaction commit failed: {commit_err}"))
                }
            }
        }
    }
    .instrument(span)
    .await
}

/// Execute a single queued command within the shared transaction.
///
/// This re-dispatches through the normal command handlers. Because
/// `state.active_transaction` is set, `run_transact` will use the
/// shared transaction instead of creating a new one.
///
/// Commands that return `Close` (like QUIT) are treated as `Reply`
/// inside a transaction — we don't close the connection mid-EXEC.
async fn exec_single_command(cmd: &RedisCommand, state: &mut ConnectionState) -> RespValue {
    // EXEC runs queued commands in a shared transaction. Pub/sub commands
    // don't make sense inside MULTI/EXEC (Redis rejects them at queue time),
    // so we pass a dummy pubsub_rx that's never used.
    let mut dummy_rx = None;
    match super::dispatch(cmd, state, &mut dummy_rx).await {
        CommandResponse::Reply(resp) => resp,
        CommandResponse::Close(resp) => resp,
        CommandResponse::MultiReply(resps) => {
            // Shouldn't happen inside EXEC, but handle gracefully.
            RespValue::Array(Some(resps))
        }
    }
}

/// Queue a command during MULTI mode, or return an error if the
/// command has a syntax issue (which sets the error flag).
///
/// Called from `dispatch()` when `state.transaction.is_some()` for
/// commands that should be queued (i.e., not MULTI/EXEC/DISCARD/etc.).
///
/// Returns `+QUEUED` on success, or an error response if the command
/// name is unknown. Note: we only check for unknown commands here;
/// arity/syntax errors within known commands are caught at execution
/// time (matching Redis behavior where only truly malformed commands
/// like unknown names or inline parse failures set the error flag).
pub fn queue_command(cmd: RedisCommand, state: &mut ConnectionState) -> RespValue {
    let tx = state.transaction.as_mut().expect("queue_command called outside MULTI");

    // Check if the command is known. If not, set the error flag.
    // Redis sets the error flag for unknown commands and wrong-arity
    // errors detected during queuing.
    if !is_known_command(&cmd.name) {
        tx.error_flag = true;
        let name = String::from_utf8_lossy(&cmd.name);
        return RespValue::err(format!("ERR unknown command '{name}', with args beginning with:"));
    }

    tx.queued.push(cmd);
    RespValue::SimpleString(Bytes::from_static(b"QUEUED"))
}

/// Check if a command name is one we support.
///
/// Driven by the central `commands::registry` table so it stays in
/// sync with the dispatcher and the COMMAND introspection commands.
fn is_known_command(name: &[u8]) -> bool {
    super::registry::is_known(name)
}
