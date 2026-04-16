//! Server administration commands.
//!
//! Implements the catalog of "ops" commands clients and tools (redis-cli,
//! redis-benchmark, monitoring agents) expect to find on a Redis-compatible
//! server. Most of these don't touch user data — they introspect or mutate
//! the running process.
//!
//! # Sections
//!
//! - **INFO** — full multi-section status report
//! - **CONFIG** — GET / SET / RESETSTAT / REWRITE
//! - **CLIENT** — LIST / SETNAME / GETNAME / ID / INFO / KILL / NO-EVICT /
//!   NO-TOUCH / REPLY / PAUSE / UNPAUSE / GETREDIR
//! - **COMMAND** — COUNT / DOCS / INFO / LIST
//! - **TIME / RANDOMKEY / KEYS** — basic introspection
//! - **OBJECT** — ENCODING / IDLETIME / REFCOUNT / FREQ / HELP
//! - **SLOWLOG** — GET / LEN / RESET / HELP
//! - **MEMORY** — USAGE / STATS / DOCTOR / PURGE
//! - **LATENCY** — RESET / LATEST / HISTORY / GRAPH / DOCTOR / HELP
//! - **DEBUG** — SLEEP / OBJECT / JMAP / SET-ACTIVE-EXPIRE / HELP
//! - **SHUTDOWN / BGSAVE / SAVE / BGREWRITEAOF / LASTSAVE / WAIT**
//! - **ACL** — WHOAMI / LIST / CAT / GETUSER (default-only stubs)
//!
//! # Design notes
//!
//! - Every command is async even when it doesn't touch FDB, so dispatch
//!   stays uniform.
//! - All read-only handlers take `&ConnectionState`; mutating ones take
//!   `&mut ConnectionState`.
//! - Heavy lifting (formatting INFO, snapshotting clients) happens
//!   off the lock-contended hot path: we snapshot once, then format.

use bytes::Bytes;
use foundationdb::RangeOption;

use crate::error::CommandError;
use crate::observability::metrics;
use crate::protocol::types::RespValue;
use crate::pubsub::pattern::GlobMatcher;
use crate::server::clients::{ClientInfo, ReplyMode};
use crate::server::connection::ConnectionState;
use crate::storage::ObjectMeta;
use crate::storage::meta::KeyType;
use crate::storage::{helpers, run_transact};

use super::registry;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_unix_ms() -> u64 {
    helpers::now_ms()
}

/// Paginated scan of all key-value pairs in a FDB range.
///
/// `get_range` with `StreamingMode::Iterator` returns results in
/// increasing batches. A single call with `iteration=1` only returns
/// the first page — typically a few hundred bytes. This helper loops
/// until all pages are consumed, matching the pagination pattern used
/// by hash/set/list/zset readers.
async fn scan_range_all(
    tr: &foundationdb::Transaction,
    begin: &[u8],
    end: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CommandError> {
    let mut results = Vec::new();
    let mut maybe_range: Option<RangeOption<'_>> = Some(RangeOption::from((begin, end)));
    let mut iteration = 1;

    while let Some(range_opt) = maybe_range.take() {
        let kvs = tr
            .get_range(&range_opt, iteration, true)
            .await
            .map_err(|e| CommandError::Generic(e.to_string()))?;

        for kv in kvs.iter() {
            results.push((kv.key().to_vec(), kv.value().to_vec()));
        }

        maybe_range = range_opt.next_range(&kvs);
        iteration += 1;
    }

    Ok(results)
}

fn arity_err(name: &str) -> RespValue {
    RespValue::err(CommandError::WrongArity { name: name.into() }.to_string())
}

fn bulk(s: impl Into<Bytes>) -> RespValue {
    RespValue::BulkString(Some(s.into()))
}

fn bulk_str(s: &str) -> RespValue {
    RespValue::BulkString(Some(Bytes::from(s.to_string())))
}

// ---------------------------------------------------------------------------
// TIME — returns [unix_seconds, microseconds]
// ---------------------------------------------------------------------------

/// `TIME`
///
/// Returns a 2-element array of bulk strings: seconds and microseconds.
pub async fn handle_time(args: &[Bytes]) -> RespValue {
    if !args.is_empty() {
        return arity_err("TIME");
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    RespValue::Array(Some(vec![
        bulk(now.as_secs().to_string()),
        bulk(now.subsec_micros().to_string()),
    ]))
}

// ---------------------------------------------------------------------------
// LASTSAVE — server start time (we don't run RDB)
// ---------------------------------------------------------------------------

/// `LASTSAVE`
///
/// Redis returns the UNIX time of the last successful RDB save. We
/// don't have RDB; FDB owns durability. Reporting server-start time is
/// the most truthful answer that satisfies clients which check the
/// value strictly increases.
pub async fn handle_lastsave(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if !args.is_empty() {
        return arity_err("LASTSAVE");
    }
    RespValue::Integer(state.server.start_timestamp_secs as i64)
}

// ---------------------------------------------------------------------------
// WAIT — FDB owns replication; nothing to wait for
// ---------------------------------------------------------------------------

/// `WAIT numreplicas timeout`
///
/// Always returns 0 immediately. FDB performs synchronous replication
/// on commit; once a write returns OK to us, it is durable on the
/// configured number of replicas. There's no asynchronous lag to wait
/// for.
pub async fn handle_wait(args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return arity_err("WAIT");
    }
    RespValue::Integer(0)
}

// ---------------------------------------------------------------------------
// BGSAVE / SAVE / BGREWRITEAOF — no-op; FDB owns durability
// ---------------------------------------------------------------------------

/// `BGSAVE [SCHEDULE]`
pub async fn handle_bgsave(args: &[Bytes]) -> RespValue {
    // Accept the optional SCHEDULE flag; reject anything else.
    for arg in args {
        if arg.to_ascii_uppercase().as_slice() != b"SCHEDULE" {
            return RespValue::err(format!(
                "ERR unknown subcommand or wrong number of arguments for 'BGSAVE|{}' command",
                String::from_utf8_lossy(arg)
            ));
        }
    }
    RespValue::SimpleString(Bytes::from_static(b"Background saving started"))
}

/// `SAVE` — return OK; nothing to flush.
pub async fn handle_save(args: &[Bytes]) -> RespValue {
    if !args.is_empty() {
        return arity_err("SAVE");
    }
    RespValue::ok()
}

/// `BGREWRITEAOF`
pub async fn handle_bgrewriteaof(args: &[Bytes]) -> RespValue {
    if !args.is_empty() {
        return arity_err("BGREWRITEAOF");
    }
    RespValue::SimpleString(Bytes::from_static(b"Background append only file rewriting started"))
}

// ---------------------------------------------------------------------------
// SHUTDOWN — graceful server stop
// ---------------------------------------------------------------------------

/// `SHUTDOWN [NOSAVE | SAVE | NOW | FORCE | ABORT]`
///
/// Triggers the broadcast shutdown channel. The listener loop exits on
/// the next select tick; in-flight commands finish naturally because
/// each connection task is independent.
///
/// `ABORT` is recognized but treated as a no-op (we never started a
/// pending shutdown to abort).
pub async fn handle_shutdown(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Accept any number of optional flags for compatibility.
    let mut abort = false;
    for arg in args {
        match arg.to_ascii_uppercase().as_slice() {
            b"NOSAVE" | b"SAVE" | b"NOW" | b"FORCE" => {}
            b"ABORT" => abort = true,
            other => {
                return RespValue::err(format!(
                    "ERR syntax error: unknown SHUTDOWN flag '{}'",
                    String::from_utf8_lossy(other)
                ));
            }
        }
    }

    if abort {
        // No pending shutdown to cancel — match Redis's response.
        return RespValue::err("ERR No shutdown in progress");
    }

    if state.server.trigger_shutdown() {
        tracing::info!("SHUTDOWN issued by client");
    } else {
        tracing::warn!("SHUTDOWN issued but shutdown signal already fired");
    }
    // Redis closes the connection without replying. Returning a Close
    // response would be ideal, but the listener loop will tear us down
    // shortly anyway. Returning OK is friendly for clients that block
    // on the response.
    RespValue::ok()
}

// ---------------------------------------------------------------------------
// RANDOMKEY
// ---------------------------------------------------------------------------

/// `RANDOMKEY`
///
/// Picks a uniformly-random non-expired key from the current namespace.
/// Implementation: use FDB to count meta entries (cheap byte range),
/// then re-scan to the chosen offset. Since we don't store an index,
/// this is O(n_keys); fine for ops/debugging, not a hot path.
pub async fn handle_randomkey(args: &[Bytes], state: &ConnectionState) -> RespValue {
    use rand::Rng;

    if !args.is_empty() {
        return arity_err("RANDOMKEY");
    }

    match run_transact(&state.db, state.shared_txn(), "RANDOMKEY", |tr| {
        let dirs = state.dirs.clone();
        async move {
            let now = helpers::now_ms();
            let (begin, end) = dirs.meta.range();
            // Paginated scan: collect all live keys. Bounded by FDB
            // transaction size (10MB) which caps namespace cardinality
            // here at roughly 100k keys for typical key sizes — that's
            // fine for an admin command.
            let all = scan_range_all(&tr, &begin, &end).await.map_err(helpers::cmd_err)?;

            let mut live: Vec<Vec<u8>> = Vec::new();
            for (key_bytes, value_bytes) in &all {
                let Ok(meta) = ObjectMeta::deserialize(value_bytes) else {
                    continue;
                };
                if meta.is_expired(now) {
                    continue;
                }
                if let Ok((key,)) = dirs.meta.unpack::<(Vec<u8>,)>(key_bytes) {
                    live.push(key);
                }
            }
            Ok(live)
        }
    })
    .await
    {
        Ok(live) => {
            if live.is_empty() {
                RespValue::BulkString(None)
            } else {
                let idx = rand::thread_rng().gen_range(0..live.len());
                RespValue::BulkString(Some(Bytes::from(live[idx].clone())))
            }
        }
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// KEYS pattern
// ---------------------------------------------------------------------------

/// `KEYS pattern`
///
/// Returns all live keys matching a Redis glob pattern. O(n_keys); use
/// SCAN in production. We implement it because redis-cli, monitoring
/// scripts, and most test suites depend on it.
pub async fn handle_keys(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return arity_err("KEYS");
    }
    let matcher = GlobMatcher::new(&args[0]);

    match run_transact(&state.db, state.shared_txn(), "KEYS", |tr| {
        let dirs = state.dirs.clone();
        let matcher = matcher.clone();
        async move {
            let now = helpers::now_ms();
            let (begin, end) = dirs.meta.range();
            let all = scan_range_all(&tr, &begin, &end).await.map_err(helpers::cmd_err)?;

            let mut out = Vec::new();
            for (key_bytes, value_bytes) in &all {
                let Ok(meta) = ObjectMeta::deserialize(value_bytes) else {
                    continue;
                };
                if meta.is_expired(now) {
                    continue;
                }
                if let Ok((key,)) = dirs.meta.unpack::<(Vec<u8>,)>(key_bytes)
                    && matcher.matches(&key)
                {
                    out.push(RespValue::BulkString(Some(Bytes::from(key))));
                }
            }
            Ok(out)
        }
    })
    .await
    {
        Ok(items) => RespValue::Array(Some(items)),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

// ---------------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------------

/// `CONFIG GET / SET / RESETSTAT / REWRITE`
pub async fn handle_config(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return arity_err("CONFIG");
    }
    let sub = args[0].to_ascii_uppercase();
    let rest = &args[1..];
    match sub.as_slice() {
        b"GET" => config_get(rest, state),
        b"SET" => config_set(rest, state),
        b"RESETSTAT" => config_resetstat(rest, state),
        b"REWRITE" => config_rewrite(rest),
        b"HELP" => config_help(),
        other => RespValue::err(format!(
            "ERR Unknown CONFIG subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

fn config_get(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return arity_err("CONFIG|GET");
    }
    // Each arg is a glob pattern — collect all matching parameter names.
    let mut out: Vec<RespValue> = Vec::new();
    let mut emitted: std::collections::HashSet<&'static str> = std::collections::HashSet::new();

    for raw_pattern in args {
        let matcher = GlobMatcher::new(&raw_pattern.to_ascii_lowercase());
        for &name in crate::server::server_state::LiveConfig::known_names() {
            if !emitted.insert(name) {
                continue;
            }
            if matcher.matches(name.as_bytes())
                && let Some(value) = state.server.config.get(name.as_bytes())
            {
                out.push(bulk_str(name));
                out.push(RespValue::BulkString(Some(value)));
            } else {
                emitted.remove(name);
            }
        }
    }

    RespValue::Array(Some(out))
}

fn config_set(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Redis accepts CONFIG SET name value [name value ...]
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return RespValue::err("ERR wrong number of arguments for 'CONFIG|SET' command");
    }
    for pair in args.chunks(2) {
        if let Err(msg) = state.server.config.set(&pair[0], &pair[1]) {
            return RespValue::err(msg);
        }
    }
    RespValue::ok()
}

fn config_resetstat(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if !args.is_empty() {
        return arity_err("CONFIG|RESETSTAT");
    }
    // Prometheus counters are monotonic by convention and can't be reset.
    // We clear the slow log — that's the operator-visible state Redis
    // users expect CONFIG RESETSTAT to affect.
    state.server.slowlog.reset();
    RespValue::ok()
}

fn config_rewrite(args: &[Bytes]) -> RespValue {
    if !args.is_empty() {
        return arity_err("CONFIG|REWRITE");
    }
    // We don't read a config file — there's nothing to rewrite. Match
    // redis-server's response when no config file was provided.
    RespValue::err("ERR The server is running without a config file")
}

fn config_help() -> RespValue {
    RespValue::Array(Some(vec![
        bulk_str("CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
        bulk_str("GET <pattern>"),
        bulk_str("SET <directive> <value>"),
        bulk_str("RESETSTAT"),
        bulk_str("REWRITE"),
        bulk_str("HELP"),
    ]))
}

// ---------------------------------------------------------------------------
// CLIENT
// ---------------------------------------------------------------------------

/// `CLIENT <subcommand>`
pub async fn handle_client(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err("ERR wrong number of arguments for 'CLIENT' command");
    }
    let sub = args[0].to_ascii_uppercase();
    let rest = &args[1..];
    match sub.as_slice() {
        b"ID" => RespValue::Integer(state.connection_id as i64),
        b"GETNAME" => {
            let name = state.client_handle.name.lock().unwrap().clone();
            if name.is_empty() {
                RespValue::BulkString(None)
            } else {
                RespValue::BulkString(Some(name))
            }
        }
        b"SETNAME" => client_setname(rest, state),
        b"SETINFO" => client_setinfo(rest),
        b"GETREDIR" => RespValue::Integer(-1), // tracking not implemented
        b"INFO" => {
            let now = now_unix_ms();
            let line = state.client_handle.snapshot(now).format_line();
            bulk(line)
        }
        b"LIST" => client_list(rest, state),
        b"KILL" => client_kill(rest, state),
        b"NO-EVICT" => client_toggle_no_evict(rest, state),
        b"NO-TOUCH" => client_toggle_no_touch(rest, state),
        b"REPLY" => client_reply(rest, state),
        b"PAUSE" => client_pause(rest, true),
        b"UNPAUSE" => client_pause(rest, false),
        b"HELP" => client_help(),
        other => RespValue::err(format!(
            "ERR Unknown CLIENT subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

fn client_setname(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return arity_err("CLIENT|SETNAME");
    }
    // Spaces and newlines aren't allowed in client names (matches Redis).
    if args[0].iter().any(|b| matches!(*b, b' ' | b'\n' | b'\r')) {
        return RespValue::err("ERR Client names cannot contain spaces, newlines or special characters.");
    }
    *state.client_handle.name.lock().unwrap() = args[0].clone();
    RespValue::ok()
}

fn client_setinfo(args: &[Bytes]) -> RespValue {
    // CLIENT SETINFO LIB-NAME | LIB-VER <value>
    // We acknowledge but don't store these (RFC: low-value).
    if args.len() != 2 {
        return arity_err("CLIENT|SETINFO");
    }
    let attr = args[0].to_ascii_uppercase();
    match attr.as_slice() {
        b"LIB-NAME" | b"LIB-VER" => RespValue::ok(),
        other => RespValue::err(format!("ERR Unrecognized option '{}'", String::from_utf8_lossy(other))),
    }
}

fn client_list(args: &[Bytes], state: &ConnectionState) -> RespValue {
    // Optional filters: TYPE <type> | ID <id1> [id2 ...]
    let mut id_filter: Option<Vec<u64>> = None;
    let mut type_filter: Option<&'static str> = None;
    let mut i = 0;
    while i < args.len() {
        let key = args[i].to_ascii_uppercase();
        match key.as_slice() {
            b"TYPE" => {
                let val = match args.get(i + 1) {
                    Some(v) => v,
                    None => return RespValue::err("ERR syntax error"),
                };
                let v = val.to_ascii_lowercase();
                type_filter = Some(match v.as_slice() {
                    b"normal" => "normal",
                    b"pubsub" => "pubsub",
                    b"master" => "master",
                    b"replica" | b"slave" => "replica",
                    _ => return RespValue::err("ERR Unknown client type"),
                });
                i += 2;
            }
            b"ID" => {
                let mut ids = Vec::new();
                i += 1;
                while i < args.len() {
                    let s = std::str::from_utf8(&args[i]);
                    match s.ok().and_then(|x| x.parse::<u64>().ok()) {
                        Some(id) => ids.push(id),
                        None => break,
                    }
                    i += 1;
                }
                id_filter = Some(ids);
            }
            _ => return RespValue::err("ERR syntax error"),
        }
    }

    let now = now_unix_ms();
    let snapshots: Vec<ClientInfo> = match id_filter {
        Some(ref ids) => state.clients.snapshot_ids(ids, now),
        None => state.clients.snapshot_all(now),
    };

    let lines: Vec<String> = snapshots
        .iter()
        .filter(|c| match type_filter {
            None => true,
            Some("normal") => c.sub_channels == 0 && c.sub_patterns == 0,
            Some("pubsub") => c.sub_channels > 0 || c.sub_patterns > 0,
            // We don't have master/replica connections.
            Some(_) => false,
        })
        .map(|c| c.format_line())
        .collect();

    let body = lines.join("\n");
    bulk(body)
}

fn client_kill(args: &[Bytes], state: &ConnectionState) -> RespValue {
    let (mut filter, is_old, skipme) = match crate::server::clients::parse_kill_args(args) {
        Ok(p) => p,
        Err(msg) => return RespValue::err(msg),
    };
    // When SKIPME=yes (the default), exclude the issuing connection.
    if skipme {
        filter.skip_id = Some(state.connection_id);
    }
    let now = now_unix_ms();
    let killed = state.clients.kill_matching(&filter, now);
    if is_old {
        // Old form: +OK if the client was found, error otherwise.
        if killed > 0 {
            RespValue::ok()
        } else {
            RespValue::err("ERR No such client")
        }
    } else {
        RespValue::Integer(killed as i64)
    }
}

fn client_toggle_no_evict(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.len() != 1 {
        return arity_err("CLIENT|NO-EVICT");
    }
    state.toggles.no_evict = match args[0].to_ascii_uppercase().as_slice() {
        b"ON" => true,
        b"OFF" => false,
        _ => return RespValue::err("ERR syntax error"),
    };
    RespValue::ok()
}

fn client_toggle_no_touch(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.len() != 1 {
        return arity_err("CLIENT|NO-TOUCH");
    }
    state.toggles.no_touch = match args[0].to_ascii_uppercase().as_slice() {
        b"ON" => true,
        b"OFF" => false,
        _ => return RespValue::err("ERR syntax error"),
    };
    RespValue::ok()
}

fn client_reply(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.len() != 1 {
        return arity_err("CLIENT|REPLY");
    }
    state.toggles.reply_mode = match args[0].to_ascii_uppercase().as_slice() {
        b"ON" => ReplyMode::On,
        b"OFF" => ReplyMode::Off,
        b"SKIP" => ReplyMode::Skip,
        _ => return RespValue::err("ERR syntax error"),
    };
    // Per spec, ON returns +OK; OFF and SKIP return nothing — but we
    // always return OK and let dispatch_one suppress as needed.
    RespValue::ok()
}

fn client_pause(args: &[Bytes], is_pause: bool) -> RespValue {
    if is_pause {
        // CLIENT PAUSE <timeout> [WRITE|ALL]
        if args.is_empty() {
            return arity_err("CLIENT|PAUSE");
        }
        // Validate the timeout is a non-negative integer (required by Redis).
        let ok = std::str::from_utf8(&args[0])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .is_some();
        if !ok {
            return RespValue::err("ERR timeout is not a valid integer or out of range");
        }
    }
    // We don't support pausing connections (it would block all writes
    // for a duration). Return OK so clients that probe for this don't
    // error out, but document the no-op nature in INFO if needed.
    RespValue::ok()
}

fn client_help() -> RespValue {
    RespValue::Array(Some(vec![
        bulk_str("CLIENT <subcommand> [<arg> ...]. Subcommands are:"),
        bulk_str("ID                                       -- Return the ID of the current connection."),
        bulk_str("GETNAME                                  -- Return the name of the current connection."),
        bulk_str("SETNAME <name>                           -- Assign a name to the current connection."),
        bulk_str("LIST [TYPE normal|pubsub|...] [ID id ...] -- List connected clients."),
        bulk_str("INFO                                     -- Print info on this connection."),
        bulk_str("KILL <ip:port>                           -- Kill a connection by addr (old form)."),
        bulk_str(
            "KILL [ID id] [ADDR ip:port] [LADDR ip:port] [SKIPME yes|no] [MAXAGE seconds] [TYPE type] -- Kill matching connections (new form).",
        ),
        bulk_str("NO-EVICT (ON|OFF)                        -- Mark connection as no-evict."),
        bulk_str("NO-TOUCH (ON|OFF)                        -- Don't update LRU on read commands."),
        bulk_str("REPLY (ON|OFF|SKIP)                      -- Toggle response sending."),
        bulk_str("PAUSE <millis>                           -- Pause clients (no-op)."),
        bulk_str("HELP                                     -- Print this help."),
    ]))
}

// ---------------------------------------------------------------------------
// COMMAND
// ---------------------------------------------------------------------------

/// `COMMAND` / `COMMAND COUNT|DOCS|INFO|LIST|GETKEYS`
pub async fn handle_command(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        // `COMMAND` with no args: full info for every command.
        let mut out = Vec::with_capacity(registry::count());
        for spec in registry::COMMAND_TABLE {
            out.push(registry::render_command_info(spec));
        }
        return RespValue::Array(Some(out));
    }

    let sub = args[0].to_ascii_uppercase();
    let rest = &args[1..];
    match sub.as_slice() {
        b"COUNT" => {
            if !rest.is_empty() {
                return arity_err("COMMAND|COUNT");
            }
            RespValue::Integer(registry::count() as i64)
        }
        b"LIST" => {
            // Ignore optional FILTERBY clauses for now — return all names.
            let names: Vec<RespValue> = registry::COMMAND_TABLE
                .iter()
                .map(|s| bulk_str(&s.name.to_ascii_lowercase()))
                .collect();
            RespValue::Array(Some(names))
        }
        b"INFO" => {
            // No args: same as bare COMMAND. Otherwise, named lookups.
            if rest.is_empty() {
                let all: Vec<RespValue> = registry::COMMAND_TABLE
                    .iter()
                    .map(registry::render_command_info)
                    .collect();
                return RespValue::Array(Some(all));
            }
            let entries: Vec<RespValue> = rest
                .iter()
                .map(|name| match registry::lookup(name) {
                    Some(s) => registry::render_command_info(s),
                    None => RespValue::Array(None),
                })
                .collect();
            RespValue::Array(Some(entries))
        }
        b"DOCS" => {
            // No args: docs for all. Otherwise, docs for the named subset.
            let to_emit: Vec<&registry::CommandSpec> = if rest.is_empty() {
                registry::COMMAND_TABLE.iter().collect()
            } else {
                rest.iter().filter_map(|n| registry::lookup(n)).collect()
            };
            let mut pairs = Vec::with_capacity(to_emit.len() * 2);
            for spec in to_emit {
                pairs.push((
                    bulk_str(&spec.name.to_ascii_lowercase()),
                    registry::render_command_docs(spec),
                ));
            }
            RespValue::Map(pairs)
        }
        b"GETKEYS" => {
            // Naive implementation: use the spec's key positions.
            if rest.is_empty() {
                return arity_err("COMMAND|GETKEYS");
            }
            let spec = match registry::lookup(&rest[0]) {
                Some(s) => s,
                None => return RespValue::err("ERR Invalid command specified"),
            };
            let argv = &rest[1..];
            // Validate arity quickly.
            let provided = (argv.len() + 1) as i32;
            let arity_ok = if spec.arity >= 0 {
                provided == spec.arity
            } else {
                provided >= -spec.arity
            };
            if !arity_ok {
                return RespValue::err("ERR Invalid number of arguments for the command");
            }

            if spec.first_key == 0 {
                return RespValue::Array(Some(vec![]));
            }
            // Translate 1-based positions over the full argv (cmd name + args)
            // into 0-based indices over `rest` (which excludes the command name).
            let last = if spec.last_key < 0 {
                (argv.len() as i32) + spec.last_key
            } else {
                spec.last_key - 1
            };
            let first = spec.first_key - 1;
            let step = spec.key_step.max(1);

            let mut out = Vec::new();
            let mut i = first;
            while i <= last && (i as usize) < argv.len() {
                out.push(RespValue::BulkString(Some(argv[i as usize].clone())));
                i += step;
            }
            RespValue::Array(Some(out))
        }
        b"HELP" => RespValue::Array(Some(vec![
            bulk_str("COMMAND <subcommand>. Subcommands are:"),
            bulk_str("(no subcommand)               -- Return details about all kvdb commands."),
            bulk_str("COUNT                         -- Return the number of commands."),
            bulk_str("DOCS [<command-name> ...]     -- Return doc for one or more commands."),
            bulk_str("INFO [<command-name> ...]     -- Return info for one or more commands."),
            bulk_str("LIST                          -- List all command names."),
            bulk_str("GETKEYS <full-command>        -- Return the keys from a command and arguments."),
            bulk_str("HELP                          -- Print this help."),
        ])),
        other => RespValue::err(format!(
            "ERR Unknown COMMAND subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

// ---------------------------------------------------------------------------
// OBJECT
// ---------------------------------------------------------------------------

/// `OBJECT ENCODING|IDLETIME|REFCOUNT|FREQ|HELP key`
pub async fn handle_object(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return arity_err("OBJECT");
    }
    let sub = args[0].to_ascii_uppercase();
    match sub.as_slice() {
        b"ENCODING" => object_encoding(&args[1..], state).await,
        b"REFCOUNT" => object_simple(&args[1..], state, |_| RespValue::Integer(1)).await,
        b"IDLETIME" => {
            object_simple(&args[1..], state, |meta| {
                let now = helpers::now_ms();
                let last = meta.last_accessed_ms;
                let idle = if last == 0 {
                    0
                } else {
                    ((now.saturating_sub(last)) / 1000) as i64
                };
                RespValue::Integer(idle)
            })
            .await
        }
        b"FREQ" => object_simple(&args[1..], state, |_| RespValue::Integer(0)).await,
        b"HELP" => RespValue::Array(Some(vec![
            bulk_str("OBJECT <subcommand> [<key>]. Subcommands are:"),
            bulk_str("ENCODING <key>     -- Return the kind of internal representation."),
            bulk_str("IDLETIME <key>     -- Idle time in seconds (0 if unsupported)."),
            bulk_str("REFCOUNT <key>     -- Number of references (always 1)."),
            bulk_str("FREQ <key>         -- Access frequency counter (LFU only; always 0)."),
            bulk_str("HELP               -- Print this help."),
        ])),
        other => RespValue::err(format!(
            "ERR Unknown OBJECT subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

async fn object_simple(
    args: &[Bytes],
    state: &ConnectionState,
    f: impl Fn(&ObjectMeta) -> RespValue + Send + Sync + 'static + Copy,
) -> RespValue {
    if args.len() != 1 {
        return arity_err("OBJECT");
    }
    let key = args[0].clone();
    match run_transact(&state.db, state.shared_txn(), "OBJECT", |tr| {
        let dirs = state.dirs.clone();
        let key = key.clone();
        async move {
            let now = helpers::now_ms();
            ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)
        }
    })
    .await
    {
        Ok(Some(meta)) => f(&meta),
        Ok(None) => RespValue::err("ERR no such key"),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

async fn object_encoding(args: &[Bytes], state: &ConnectionState) -> RespValue {
    object_simple(args, state, |meta| {
        let enc = encoding_for(meta);
        bulk_str(enc)
    })
    .await
}

/// Choose a Redis-recognized encoding name for `meta`.
///
/// We don't have multiple physical representations per type — but
/// clients sometimes pivot on this string (e.g. test suites assert
/// "listpack" for small hashes), so we return something plausible.
fn encoding_for(meta: &ObjectMeta) -> &'static str {
    match meta.key_type {
        KeyType::String => {
            // Numbers fit "int"; short strings fit "embstr"; the rest "raw".
            if meta.size_bytes <= 44 { "embstr" } else { "raw" }
        }
        KeyType::Hash => {
            if meta.cardinality <= 128 {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KeyType::Set => {
            if meta.cardinality <= 128 {
                "listpack"
            } else {
                "hashtable"
            }
        }
        KeyType::SortedSet => {
            if meta.cardinality <= 128 {
                "listpack"
            } else {
                "skiplist"
            }
        }
        KeyType::List => {
            if meta.list_length <= 128 {
                "listpack"
            } else {
                "quicklist"
            }
        }
        KeyType::Stream => "stream",
    }
}

// ---------------------------------------------------------------------------
// SLOWLOG
// ---------------------------------------------------------------------------

/// `SLOWLOG GET [count] | LEN | RESET | HELP`
pub async fn handle_slowlog(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return arity_err("SLOWLOG");
    }
    let sub = args[0].to_ascii_uppercase();
    let rest = &args[1..];
    match sub.as_slice() {
        b"GET" => {
            let count = if let Some(c) = rest.first() {
                match super::util::parse_i64_arg(c) {
                    Ok(n) if n >= 0 => n as usize,
                    Ok(_) => return RespValue::err("ERR count must be >= 0"),
                    Err(e) => return e,
                }
            } else {
                10
            };
            let entries = state.server.slowlog.snapshot(count);
            let out: Vec<RespValue> = entries
                .into_iter()
                .map(|e| {
                    RespValue::Array(Some(vec![
                        RespValue::Integer(e.id as i64),
                        RespValue::Integer(e.timestamp_secs as i64),
                        RespValue::Integer(e.duration_us as i64),
                        RespValue::Array(Some(
                            e.argv.into_iter().map(|b| RespValue::BulkString(Some(b))).collect(),
                        )),
                        bulk(e.client_addr.to_string()),
                        RespValue::BulkString(Some(e.client_name)),
                    ]))
                })
                .collect();
            RespValue::Array(Some(out))
        }
        b"LEN" => RespValue::Integer(state.server.slowlog.len() as i64),
        b"RESET" => {
            state.server.slowlog.reset();
            RespValue::ok()
        }
        b"HELP" => RespValue::Array(Some(vec![
            bulk_str("SLOWLOG <subcommand> [<arg>]. Subcommands:"),
            bulk_str("GET [count] -- Return the most recent slow-log entries (default 10)."),
            bulk_str("LEN         -- Return current number of entries."),
            bulk_str("RESET       -- Clear the log."),
            bulk_str("HELP        -- Print this help."),
        ])),
        other => RespValue::err(format!(
            "ERR Unknown SLOWLOG subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

// ---------------------------------------------------------------------------
// MEMORY
// ---------------------------------------------------------------------------

/// `MEMORY USAGE key | STATS | DOCTOR | PURGE | HELP`
pub async fn handle_memory(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return arity_err("MEMORY");
    }
    let sub = args[0].to_ascii_uppercase();
    let rest = &args[1..];
    match sub.as_slice() {
        b"USAGE" => memory_usage(rest, state).await,
        b"STATS" => memory_stats(),
        b"DOCTOR" => bulk_str(memory_doctor_text()),
        b"PURGE" => RespValue::ok(),
        b"MALLOC-STATS" => bulk_str("kvdb does not bundle a custom allocator"),
        b"HELP" => RespValue::Array(Some(vec![
            bulk_str("MEMORY <subcommand> [<arg>]. Subcommands:"),
            bulk_str("USAGE <key>          -- Estimated bytes used by the value at <key>."),
            bulk_str("STATS                -- Memory statistics map."),
            bulk_str("DOCTOR               -- Human-readable memory advisor."),
            bulk_str("PURGE                -- No-op (FDB owns memory)."),
            bulk_str("HELP                 -- Print this help."),
        ])),
        other => RespValue::err(format!(
            "ERR Unknown MEMORY subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

async fn memory_usage(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return arity_err("MEMORY|USAGE");
    }
    // Optional SAMPLES count is parsed but ignored.
    let key = args[0].clone();
    object_simple(&[key], state, |meta| {
        // Estimate: meta size + payload.
        const META_OVERHEAD: i64 = 64;
        let payload = match meta.key_type {
            KeyType::String => meta.size_bytes as i64,
            // For aggregates we don't have an exact byte count cheaply; use
            // cardinality * 32 as a loose proxy. Operators using MEMORY USAGE
            // for capacity planning will reach for the FDB-side metrics anyway.
            KeyType::Hash | KeyType::Set | KeyType::SortedSet => (meta.cardinality as i64) * 32,
            KeyType::List => (meta.list_length as i64) * 32,
            KeyType::Stream => meta.size_bytes as i64,
        };
        RespValue::Integer(META_OVERHEAD + payload)
    })
    .await
}

fn memory_stats() -> RespValue {
    use crate::observability::metrics::{NETWORK_BYTES_READ_TOTAL, NETWORK_BYTES_WRITTEN_TOTAL, uptime_secs};

    let mut pairs: Vec<(RespValue, RespValue)> = Vec::new();
    let push = |pairs: &mut Vec<(RespValue, RespValue)>, k: &str, v: i64| {
        pairs.push((bulk_str(k), RespValue::Integer(v)));
    };

    // Resident-set size proxy (we don't read /proc/self/status here to
    // stay portable; future enhancement: feature-gate a Linux-specific path).
    push(&mut pairs, "peak.allocated", 0);
    push(&mut pairs, "total.allocated", 0);
    push(&mut pairs, "startup.allocated", 0);
    push(&mut pairs, "uptime.seconds", uptime_secs() as i64);
    push(&mut pairs, "network.bytes.read", NETWORK_BYTES_READ_TOTAL.get() as i64);
    push(
        &mut pairs,
        "network.bytes.written",
        NETWORK_BYTES_WRITTEN_TOTAL.get() as i64,
    );
    pairs.push((bulk_str("eviction.policy"), bulk_str("noeviction")));
    pairs.push((
        bulk_str("note"),
        bulk_str("kvdb runs on FoundationDB; memory accounting is approximate"),
    ));
    RespValue::Map(pairs)
}

fn memory_doctor_text() -> &'static str {
    "Sam, I detected a few issues in this kvdb instance memory implants:\n\n * kvdb stores data in FoundationDB; in-process memory tracks only buffers and metadata.\n * If you need precise memory numbers, query FoundationDB's status JSON or the process RSS via the Prometheus endpoint.\n"
}

// ---------------------------------------------------------------------------
// LATENCY
// ---------------------------------------------------------------------------

/// `LATENCY RESET|LATEST|HISTORY|GRAPH|DOCTOR|HELP`
///
/// We don't run Redis's latency monitor (it's a separate sampling
/// subsystem). Stubs return well-formed empty responses so tools like
/// redis-cli `--latency-history` don't choke.
pub async fn handle_latency(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return arity_err("LATENCY");
    }
    let sub = args[0].to_ascii_uppercase();
    match sub.as_slice() {
        b"RESET" => RespValue::Integer(0),
        b"LATEST" => RespValue::Array(Some(vec![])),
        b"HISTORY" => RespValue::Array(Some(vec![])),
        b"GRAPH" => bulk_str("kvdb does not collect latency events; see Prometheus instead.\n"),
        b"DOCTOR" => bulk_str(
            "Dave, I have observed the system, and I can report:\n  No latency events to report.\n  For latency analysis use the Prometheus histogram kvdb_command_duration_seconds.\n",
        ),
        b"HELP" => RespValue::Array(Some(vec![
            bulk_str("LATENCY <subcommand>. Subcommands are:"),
            bulk_str("RESET [event ...]   -- Reset latency events (no-op)."),
            bulk_str("LATEST              -- Latest latency for each event."),
            bulk_str("HISTORY <event>     -- History for an event."),
            bulk_str("GRAPH <event>       -- ASCII graph (placeholder)."),
            bulk_str("DOCTOR              -- Latency advisor."),
            bulk_str("HELP                -- Print this help."),
        ])),
        other => RespValue::err(format!(
            "ERR Unknown LATENCY subcommand '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

// ---------------------------------------------------------------------------
// DEBUG
// ---------------------------------------------------------------------------

/// `DEBUG <subcommand>` — minimal implementation for testing tools.
pub async fn handle_debug(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return arity_err("DEBUG");
    }
    let sub = args[0].to_ascii_uppercase();
    let rest = &args[1..];
    match sub.as_slice() {
        b"SLEEP" => debug_sleep(rest).await,
        b"OBJECT" => debug_object(rest, state).await,
        b"SET-ACTIVE-EXPIRE" => RespValue::ok(), // accept on/off, no-op
        b"JMAP" => RespValue::ok(),
        b"HELP" => RespValue::Array(Some(vec![
            bulk_str("DEBUG <subcommand>. Subcommands are:"),
            bulk_str("SLEEP <seconds>           -- Sleep for the given duration."),
            bulk_str("OBJECT <key>              -- Print info on the on-disk representation."),
            bulk_str("SET-ACTIVE-EXPIRE <0|1>   -- No-op."),
            bulk_str("JMAP                      -- No-op."),
            bulk_str("HELP                      -- Print this help."),
        ])),
        other => RespValue::err(format!(
            "ERR Unknown DEBUG subcommand '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

async fn debug_sleep(args: &[Bytes]) -> RespValue {
    if args.len() != 1 {
        return arity_err("DEBUG|SLEEP");
    }
    let s = match std::str::from_utf8(&args[0]) {
        Ok(v) => v,
        Err(_) => return RespValue::err("ERR invalid duration"),
    };
    let secs: f64 = match s.parse::<f64>() {
        Ok(v) if v >= 0.0 && v.is_finite() => v,
        _ => return RespValue::err("ERR invalid duration"),
    };
    // Cap sleep at 60s as a safety net so a misuse can't pin a connection forever.
    let secs = secs.min(60.0);
    tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
    RespValue::ok()
}

async fn debug_object(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.len() != 1 {
        return arity_err("DEBUG|OBJECT");
    }
    let key = args[0].clone();
    match run_transact(&state.db, state.shared_txn(), "DEBUG_OBJECT", |tr| {
        let dirs = state.dirs.clone();
        let key = key.clone();
        async move {
            let now = helpers::now_ms();
            ObjectMeta::read(&tr, &dirs, &key, now, false)
                .await
                .map_err(helpers::storage_err)
        }
    })
    .await
    {
        Ok(Some(meta)) => {
            let enc = encoding_for(&meta);
            // Mirror Redis's textual format roughly.
            let s = format!(
                "Value at:0x0 refcount:1 encoding:{} serializedlength:{} lru:0 lru_seconds_idle:0 type:{}",
                enc,
                meta.size_bytes,
                key_type_name(meta.key_type),
            );
            RespValue::SimpleString(Bytes::from(s))
        }
        Ok(None) => RespValue::err("ERR no such key"),
        Err(e) => helpers::storage_err_to_resp(e),
    }
}

fn key_type_name(t: KeyType) -> &'static str {
    match t {
        KeyType::String => "string",
        KeyType::Hash => "hash",
        KeyType::Set => "set",
        KeyType::SortedSet => "zset",
        KeyType::List => "list",
        KeyType::Stream => "stream",
    }
}

// ---------------------------------------------------------------------------
// ACL — default-only stubs (no real auth yet)
// ---------------------------------------------------------------------------

/// `ACL <subcommand>`
pub async fn handle_acl(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        return arity_err("ACL");
    }
    let sub = args[0].to_ascii_uppercase();
    let rest = &args[1..];
    match sub.as_slice() {
        b"WHOAMI" => bulk_str("default"),
        b"LIST" => RespValue::Array(Some(vec![bulk_str("user default on nopass ~* &* +@all")])),
        b"CAT" => acl_cat(rest),
        b"GETUSER" => acl_getuser(rest),
        b"USERS" => RespValue::Array(Some(vec![bulk_str("default")])),
        b"SETUSER" | b"DELUSER" => RespValue::err("ERR ACL mutations are not yet supported"),
        b"HELP" => RespValue::Array(Some(vec![
            bulk_str("ACL <subcommand>. Subcommands are:"),
            bulk_str("WHOAMI                    -- Return current user."),
            bulk_str("LIST                      -- List all configured users."),
            bulk_str("USERS                     -- Return all user names."),
            bulk_str("CAT [<category>]          -- List ACL categories or commands in a category."),
            bulk_str("GETUSER <username>        -- Print full info on a user."),
            bulk_str("HELP                      -- Print this help."),
        ])),
        other => RespValue::err(format!(
            "ERR Unknown ACL subcommand '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

fn acl_cat(args: &[Bytes]) -> RespValue {
    if args.is_empty() {
        let cats = [
            "keyspace",
            "read",
            "write",
            "set",
            "sortedset",
            "list",
            "hash",
            "string",
            "bitmap",
            "hyperloglog",
            "geo",
            "stream",
            "pubsub",
            "admin",
            "fast",
            "slow",
            "blocking",
            "dangerous",
            "connection",
            "transaction",
            "scripting",
        ];
        RespValue::Array(Some(cats.iter().map(|c| bulk_str(c)).collect()))
    } else {
        // Without category->command mapping, just return all command names
        // (close enough for clients that probe categories like @read).
        RespValue::Array(Some(
            registry::COMMAND_TABLE
                .iter()
                .map(|s| bulk_str(&s.name.to_ascii_lowercase()))
                .collect(),
        ))
    }
}

fn acl_getuser(args: &[Bytes]) -> RespValue {
    if args.len() != 1 {
        return arity_err("ACL|GETUSER");
    }
    if args[0].as_ref() != b"default" {
        return RespValue::Array(None);
    }
    RespValue::Map(vec![
        (
            bulk_str("flags"),
            RespValue::Array(Some(vec![
                bulk_str("on"),
                bulk_str("nopass"),
                bulk_str("allkeys"),
                bulk_str("allcommands"),
            ])),
        ),
        (bulk_str("passwords"), RespValue::Array(Some(vec![]))),
        (bulk_str("commands"), bulk_str("+@all")),
        (bulk_str("keys"), RespValue::Array(Some(vec![bulk_str("~*")]))),
        (bulk_str("channels"), RespValue::Array(Some(vec![bulk_str("&*")]))),
        (bulk_str("selectors"), RespValue::Array(Some(vec![]))),
    ])
}

// ---------------------------------------------------------------------------
// AUTH — stub that succeeds when no password is set
// ---------------------------------------------------------------------------

/// `AUTH [username] password`
pub async fn handle_auth(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() || args.len() > 2 {
        return arity_err("AUTH");
    }
    let configured = state.server.config.requirepass.lock().unwrap().clone();
    let provided = args.last().unwrap();
    if configured.is_empty() {
        return RespValue::err(
            "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
        );
    }
    if provided.as_ref() == configured.as_ref() {
        RespValue::ok()
    } else {
        RespValue::err("WRONGPASS invalid username-password pair or user is disabled.")
    }
}

// ---------------------------------------------------------------------------
// INFO
// ---------------------------------------------------------------------------

/// `INFO [section [section ...]]`
///
/// Returns a single bulk string with `# Section\r\nkey:value\r\n...`
/// blocks. Most monitoring tools (redis-cli INFO, RedisInsight,
/// prometheus-redis-exporter when the metrics endpoint is unreachable)
/// scrape this format.
pub async fn handle_info(args: &[Bytes], state: &ConnectionState) -> RespValue {
    let sections: Vec<String> = if args.is_empty() {
        vec!["default".into()]
    } else {
        args.iter().map(|a| String::from_utf8_lossy(a).to_lowercase()).collect()
    };

    let want = |name: &str| {
        sections.iter().any(|s| {
            s == name || s == "all" || s == "everything" || (s == "default" && DEFAULT_SECTIONS.contains(&name))
        })
    };

    let mut out = String::with_capacity(2048);
    if want("server") {
        out.push_str(&info_server(state));
    }
    if want("clients") {
        out.push_str(&crate::server::clients::format_info_section(
            &state.clients,
            state
                .server
                .config
                .maxclients
                .load(std::sync::atomic::Ordering::Relaxed),
        ));
        out.push_str("\r\n");
    }
    if want("memory") {
        out.push_str(&info_memory());
    }
    if want("persistence") {
        out.push_str(&info_persistence());
    }
    if want("stats") {
        out.push_str(&info_stats());
    }
    if want("replication") {
        out.push_str(&info_replication());
    }
    if want("cpu") {
        out.push_str(&info_cpu());
    }
    if want("commandstats") {
        out.push_str(&info_commandstats());
    }
    if want("keyspace") {
        out.push_str(&info_keyspace(state).await);
    }

    bulk(out)
}

const DEFAULT_SECTIONS: &[&str] = &[
    "server",
    "clients",
    "memory",
    "persistence",
    "stats",
    "replication",
    "cpu",
    "keyspace",
];

fn info_server(state: &ConnectionState) -> String {
    let mut s = String::new();
    s.push_str("# Server\r\n");
    s.push_str("redis_version:7.4.0\r\n"); // protocol-compatible version
    s.push_str(&format!("kvdb_version:{}\r\n", env!("CARGO_PKG_VERSION")));
    s.push_str("redis_git_sha1:0\r\n");
    s.push_str("redis_git_dirty:0\r\n");
    s.push_str("redis_build_id:kvdb\r\n");
    s.push_str("redis_mode:standalone\r\n");
    s.push_str(&format!("os:{}\r\n", std::env::consts::OS));
    s.push_str(&format!("arch_bits:{}\r\n", std::mem::size_of::<usize>() * 8));
    s.push_str("multiplexing_api:tokio\r\n");
    s.push_str(&format!("process_id:{}\r\n", std::process::id()));
    s.push_str(&format!("tcp_port:{}\r\n", state.server.bind_addr.port()));
    s.push_str(&format!("uptime_in_seconds:{}\r\n", metrics::uptime_secs()));
    s.push_str(&format!("uptime_in_days:{}\r\n", metrics::uptime_secs() / 86_400));
    s.push_str(&format!(
        "server_time_usec:{}\r\n",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0)
    ));
    s.push_str("hz:10\r\n");
    s.push_str("config_file:\r\n");
    s.push_str("storage_engine:foundationdb\r\n");
    s.push_str(&format!("fdb_cluster_file:{}\r\n", state.server.fdb_cluster_file));
    s.push_str("\r\n");
    s
}

fn info_memory() -> String {
    use crate::observability::metrics::{NETWORK_BYTES_READ_TOTAL, NETWORK_BYTES_WRITTEN_TOTAL};
    let mut s = String::new();
    s.push_str("# Memory\r\n");
    // We don't track allocator stats. Report 0 / "noeviction" / counters
    // that ARE meaningful here (network bytes).
    s.push_str("used_memory:0\r\n");
    s.push_str("used_memory_human:0B\r\n");
    s.push_str("used_memory_rss:0\r\n");
    s.push_str("used_memory_peak:0\r\n");
    s.push_str("used_memory_peak_human:0B\r\n");
    s.push_str("maxmemory:0\r\n");
    s.push_str("maxmemory_policy:noeviction\r\n");
    s.push_str("mem_fragmentation_ratio:1.00\r\n");
    s.push_str(&format!("total_net_input_bytes:{}\r\n", NETWORK_BYTES_READ_TOTAL.get()));
    s.push_str(&format!(
        "total_net_output_bytes:{}\r\n",
        NETWORK_BYTES_WRITTEN_TOTAL.get()
    ));
    s.push_str("\r\n");
    s
}

fn info_persistence() -> String {
    let mut s = String::new();
    s.push_str("# Persistence\r\n");
    // FDB owns durability — we have no AOF/RDB. Match Redis fields so
    // monitoring agents don't break.
    s.push_str("loading:0\r\n");
    s.push_str("rdb_changes_since_last_save:0\r\n");
    s.push_str("rdb_bgsave_in_progress:0\r\n");
    s.push_str("rdb_last_save_time:0\r\n");
    s.push_str("rdb_last_bgsave_status:ok\r\n");
    s.push_str("aof_enabled:0\r\n");
    s.push_str("aof_rewrite_in_progress:0\r\n");
    s.push_str("aof_last_write_status:ok\r\n");
    s.push_str("\r\n");
    s
}

fn info_stats() -> String {
    use crate::observability::metrics::{
        COMMANDS_PROCESSED_TOTAL, CONNECTIONS_TOTAL, EXPIRED_KEYS_TOTAL, NETWORK_BYTES_READ_TOTAL,
        NETWORK_BYTES_WRITTEN_TOTAL, PUBSUB_MESSAGES_DELIVERED_TOTAL, PUBSUB_MESSAGES_PUBLISHED_TOTAL,
    };
    let mut s = String::new();
    s.push_str("# Stats\r\n");
    s.push_str(&format!(
        "total_connections_received:{}\r\n",
        CONNECTIONS_TOTAL.with_label_values(&["accepted"]).get()
    ));
    s.push_str(&format!(
        "total_commands_processed:{}\r\n",
        COMMANDS_PROCESSED_TOTAL.get()
    ));
    s.push_str(&format!("total_net_input_bytes:{}\r\n", NETWORK_BYTES_READ_TOTAL.get()));
    s.push_str(&format!(
        "total_net_output_bytes:{}\r\n",
        NETWORK_BYTES_WRITTEN_TOTAL.get()
    ));
    s.push_str(&format!(
        "expired_keys:{}\r\n",
        EXPIRED_KEYS_TOTAL.with_label_values(&["lazy"]).get() + EXPIRED_KEYS_TOTAL.with_label_values(&["active"]).get()
    ));
    s.push_str(&format!(
        "rejected_connections:{}\r\n",
        CONNECTIONS_TOTAL.with_label_values(&["rejected_limit"]).get()
    ));
    s.push_str(&format!(
        "pubsub_messages_published:{}\r\n",
        PUBSUB_MESSAGES_PUBLISHED_TOTAL.get()
    ));
    s.push_str(&format!(
        "pubsub_messages_delivered:{}\r\n",
        PUBSUB_MESSAGES_DELIVERED_TOTAL.get()
    ));
    s.push_str("\r\n");
    s
}

fn info_replication() -> String {
    let mut s = String::new();
    s.push_str("# Replication\r\n");
    s.push_str("role:master\r\n");
    s.push_str("connected_slaves:0\r\n");
    s.push_str("master_replid:0000000000000000000000000000000000000000\r\n");
    s.push_str("master_repl_offset:0\r\n");
    s.push_str("repl_backlog_active:0\r\n");
    s.push_str("\r\n");
    s
}

fn info_cpu() -> String {
    let mut s = String::new();
    s.push_str("# CPU\r\n");
    // Without cross-platform RUsage we can't fill these accurately.
    s.push_str("used_cpu_sys:0.0\r\n");
    s.push_str("used_cpu_user:0.0\r\n");
    s.push_str("used_cpu_sys_children:0.0\r\n");
    s.push_str("used_cpu_user_children:0.0\r\n");
    s.push_str("\r\n");
    s
}

fn info_commandstats() -> String {
    let mut s = String::new();
    s.push_str("# Commandstats\r\n");
    // Iterate metric families and emit a line per command.
    let families = prometheus::gather();
    for fam in families.iter() {
        if fam.name() != "kvdb_command_duration_seconds" {
            continue;
        }
        for m in fam.get_metric() {
            let labels = m.get_label();
            let cmd = labels
                .iter()
                .find(|l| l.name() == "command")
                .map(|l| l.value())
                .unwrap_or("");
            if cmd.is_empty() {
                continue;
            }
            let h = m.get_histogram();
            let calls = h.get_sample_count();
            if calls == 0 {
                continue;
            }
            let total_us = (h.get_sample_sum() * 1_000_000.0) as u64;
            let usec_per_call = if calls == 0 { 0 } else { total_us / calls };
            s.push_str(&format!(
                "cmdstat_{}:calls={},usec={},usec_per_call={},rejected_calls=0,failed_calls=0\r\n",
                cmd.to_lowercase(),
                calls,
                total_us,
                usec_per_call
            ));
        }
    }
    s.push_str("\r\n");
    s
}

async fn info_keyspace(state: &ConnectionState) -> String {
    // Best-effort: scan every cached namespace and count live meta entries.
    let mut s = String::new();
    s.push_str("# Keyspace\r\n");
    let ns_dirs = state.ns_cache.cached_namespaces().await;
    for dirs in ns_dirs {
        let count = count_keys(state, &dirs).await.unwrap_or(0);
        if count > 0 {
            s.push_str(&format!("db{}:keys={},expires=0,avg_ttl=0\r\n", dirs.namespace, count));
        }
    }
    s.push_str("\r\n");
    s
}

async fn count_keys(state: &ConnectionState, dirs: &crate::storage::Directories) -> Result<i64, ()> {
    run_transact(&state.db, None, "INFO_KEYSPACE", |tr| {
        let dirs = dirs.clone();
        async move {
            let now = helpers::now_ms();
            let (begin, end) = dirs.meta.range();
            let all = scan_range_all(&tr, &begin, &end).await.map_err(helpers::cmd_err)?;
            let mut count: i64 = 0;
            for (_key_bytes, value_bytes) in &all {
                if let Ok(meta) = ObjectMeta::deserialize(value_bytes)
                    && !meta.is_expired(now)
                {
                    count += 1;
                }
            }
            Ok(count)
        }
    })
    .await
    .map_err(|_| ())
}
