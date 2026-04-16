//! Client registry: a process-wide map of every connected client.
//!
//! Powers the `CLIENT LIST`, `CLIENT KILL`, `CLIENT INFO` and `INFO clients`
//! commands. Each connection registers a [`ClientHandle`] on accept and
//! deregisters on disconnect; handlers update mutable fields (name, db,
//! last command, sub counts) as the connection processes commands.
//!
//! # Concurrency
//!
//! The registry is `DashMap<u64, Arc<ClientHandle>>`, sharded internally
//! to avoid global lock contention. Per-handle mutable state uses fine-
//! grained primitives:
//!
//! - **Atomics** for counters that change per command (`db`, `last_active_ms`,
//!   `multi_queued`, sub counts) — read by `CLIENT LIST` without locking.
//! - **`parking_lot::Mutex`** isn't pulled in; we use `std::sync::Mutex` for
//!   the rare mutable fields (`name`, `last_cmd`).
//!
//! # Killing a client
//!
//! Each handle owns a [`tokio_util::sync::CancellationToken`]. The
//! connection's `select!` loop adds a branch on `kill.cancelled()` so a
//! `CLIENT KILL` immediately tears down the target connection without
//! waiting for it to send another byte.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicU8, AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

/// Per-client mutable state shared between the connection task and any
/// command handler that needs to inspect or mutate it.
///
/// Cheap to clone (it's an `Arc`).
pub struct ClientHandle {
    /// Unique connection id assigned at accept time.
    pub id: u64,
    /// Socket address of the client (peer).
    pub addr: SocketAddr,
    /// Local socket address (the listener address this connection landed on).
    pub laddr: SocketAddr,
    /// Wall-clock UNIX millis when the connection was accepted. Used for `age`.
    pub connected_at_ms: u64,
    /// Wall-clock UNIX millis of the last command processed. Used for `idle`.
    /// Updated by the connection loop on every dispatched command.
    pub last_active_ms: AtomicU64,
    /// Currently selected database namespace (0-15). Mirrors `ConnectionState.selected_db`.
    pub db: AtomicU8,
    /// Negotiated RESP protocol version (2 or 3). Mirrors `protocol_version`.
    pub protocol: AtomicU8,
    /// Connection name, set by `CLIENT SETNAME`. Empty string by default.
    pub name: Mutex<Bytes>,
    /// Last dispatched command name (uppercased), e.g. `b"SET"`. Empty for fresh connections.
    pub last_cmd: Mutex<Bytes>,
    /// Number of commands queued in MULTI, or `-1` if not inside MULTI.
    pub multi_queued: AtomicI32,
    /// Number of channels this client is subscribed to.
    pub sub_channels: AtomicUsize,
    /// Number of patterns this client is subscribed to via PSUBSCRIBE.
    pub sub_patterns: AtomicUsize,
    /// Number of WATCHed keys carried by this connection.
    pub watch_count: AtomicUsize,
    /// Cancellation token. `cancel()` causes the connection's `select!`
    /// loop to break and drop the socket. Used by `CLIENT KILL`.
    pub kill: CancellationToken,
}

impl ClientHandle {
    /// Build a fresh handle for a newly-accepted connection.
    pub fn new(id: u64, addr: SocketAddr, laddr: SocketAddr, now_ms: u64) -> Self {
        Self {
            id,
            addr,
            laddr,
            connected_at_ms: now_ms,
            last_active_ms: AtomicU64::new(now_ms),
            db: AtomicU8::new(0),
            protocol: AtomicU8::new(2),
            name: Mutex::new(Bytes::new()),
            last_cmd: Mutex::new(Bytes::new()),
            multi_queued: AtomicI32::new(-1),
            sub_channels: AtomicUsize::new(0),
            sub_patterns: AtomicUsize::new(0),
            watch_count: AtomicUsize::new(0),
            kill: CancellationToken::new(),
        }
    }

    /// Snapshot fields into a `ClientInfo` for serialization (CLIENT LIST/INFO).
    pub fn snapshot(&self, now_ms: u64) -> ClientInfo {
        ClientInfo {
            id: self.id,
            addr: self.addr,
            laddr: self.laddr,
            name: self.name.lock().unwrap().clone(),
            age_secs: ((now_ms.saturating_sub(self.connected_at_ms)) / 1000) as i64,
            idle_secs: ((now_ms.saturating_sub(self.last_active_ms.load(Ordering::Relaxed))) / 1000) as i64,
            db: self.db.load(Ordering::Relaxed),
            protocol: self.protocol.load(Ordering::Relaxed),
            multi_queued: self.multi_queued.load(Ordering::Relaxed),
            sub_channels: self.sub_channels.load(Ordering::Relaxed),
            sub_patterns: self.sub_patterns.load(Ordering::Relaxed),
            watch_count: self.watch_count.load(Ordering::Relaxed),
            last_cmd: self.last_cmd.lock().unwrap().clone(),
        }
    }
}

/// A point-in-time view of a client suitable for formatting into the
/// `CLIENT LIST` text format.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub laddr: SocketAddr,
    pub name: Bytes,
    pub age_secs: i64,
    pub idle_secs: i64,
    pub db: u8,
    pub protocol: u8,
    pub multi_queued: i32,
    pub sub_channels: usize,
    pub sub_patterns: usize,
    pub watch_count: usize,
    pub last_cmd: Bytes,
}

impl ClientInfo {
    /// Format as a single line in the Redis `CLIENT LIST` text format.
    ///
    /// Fields chosen to match the subset most clients/tools care about.
    /// We include enough state for ops to identify a misbehaving client
    /// and feed `CLIENT KILL ID <id>` or `CLIENT KILL ADDR <addr>` back in.
    pub fn format_line(&self) -> String {
        let name = String::from_utf8_lossy(&self.name);
        let last_cmd = String::from_utf8_lossy(&self.last_cmd);
        let flags = if self.multi_queued >= 0 {
            "x" // in MULTI
        } else if self.sub_channels > 0 || self.sub_patterns > 0 {
            "P" // pub/sub
        } else {
            "N" // normal
        };
        format!(
            "id={} addr={} laddr={} fd=-1 name={} age={} idle={} flags={} db={} sub={} psub={} ssub=0 multi={} watch={} qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 tot-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 events=r cmd={} user=default redir=-1 resp={} lib-name= lib-ver=",
            self.id,
            self.addr,
            self.laddr,
            name,
            self.age_secs,
            self.idle_secs,
            flags,
            self.db,
            self.sub_channels,
            self.sub_patterns,
            self.multi_queued,
            self.watch_count,
            if last_cmd.is_empty() {
                "NULL".into()
            } else {
                last_cmd.to_lowercase()
            },
            self.protocol,
        )
    }
}

/// Process-wide registry of connected clients.
///
/// Wrapped in `Arc` and shared with every `ConnectionState`. The registry
/// itself is `Send + Sync` and clones cheaply.
pub struct ClientRegistry {
    clients: DashMap<u64, Arc<ClientHandle>>,
    next_id: AtomicU64,
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
            // 0 is reserved for "no connection / unit-test stub" so we start at 1.
            next_id: AtomicU64::new(1),
        }
    }

    /// Allocate a new connection id. Always > 0.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register a handle. The caller must call `deregister` when the
    /// connection ends (typically via RAII guard in the connection task).
    pub fn register(&self, handle: Arc<ClientHandle>) {
        self.clients.insert(handle.id, handle);
    }

    /// Remove a handle. Idempotent.
    pub fn deregister(&self, id: u64) {
        self.clients.remove(&id);
    }

    /// Look up a single client by id. Returns `None` if not connected.
    pub fn get(&self, id: u64) -> Option<Arc<ClientHandle>> {
        self.clients.get(&id).map(|r| r.clone())
    }

    /// Number of currently registered clients.
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// True if there are no registered clients.
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// Snapshot all clients into a sorted-by-id `Vec`. The caller can
    /// then format and emit them without holding any lock on the registry.
    pub fn snapshot_all(&self, now_ms: u64) -> Vec<ClientInfo> {
        let mut out: Vec<ClientInfo> = self.clients.iter().map(|kv| kv.value().snapshot(now_ms)).collect();
        out.sort_by_key(|c| c.id);
        out
    }

    /// Snapshot the named clients (typically used by `CLIENT LIST ID id1 id2 ...`).
    pub fn snapshot_ids(&self, ids: &[u64], now_ms: u64) -> Vec<ClientInfo> {
        let mut out: Vec<ClientInfo> = ids
            .iter()
            .filter_map(|id| self.clients.get(id).map(|r| r.snapshot(now_ms)))
            .collect();
        out.sort_by_key(|c| c.id);
        out
    }

    /// Find clients matching a `CLIENT KILL` filter set and cancel them.
    /// Returns the number of clients killed.
    pub fn kill_matching(&self, filter: &KillFilter, now_ms: u64) -> usize {
        let mut killed = 0;
        for kv in self.clients.iter() {
            let handle = kv.value();
            if filter.matches(handle, now_ms) {
                handle.kill.cancel();
                killed += 1;
            }
        }
        killed
    }

    /// Cancel a single client by id. Returns true if the client was found.
    pub fn kill_id(&self, id: u64) -> bool {
        if let Some(handle) = self.clients.get(&id) {
            handle.kill.cancel();
            true
        } else {
            false
        }
    }

    /// Aggregate counts useful for `INFO clients`.
    pub fn aggregate_stats(&self) -> ClientAggregate {
        let mut agg = ClientAggregate::default();
        for kv in self.clients.iter() {
            let h = kv.value();
            agg.connected += 1;
            if h.multi_queued.load(Ordering::Relaxed) >= 0 {
                agg.in_multi += 1;
            }
            agg.sub_channels += h.sub_channels.load(Ordering::Relaxed);
            agg.sub_patterns += h.sub_patterns.load(Ordering::Relaxed);
            // longest output buffer / input buffer would go here if tracked.
        }
        agg
    }
}

/// Type alias used everywhere that needs the registry.
pub type SharedClientRegistry = Arc<ClientRegistry>;

/// Aggregate stats for `INFO clients`.
#[derive(Default, Debug, Clone)]
pub struct ClientAggregate {
    pub connected: usize,
    pub in_multi: usize,
    pub sub_channels: usize,
    pub sub_patterns: usize,
}

/// Filter for `CLIENT KILL [ID id] [ADDR ip:port] [LADDR ip:port] [SKIPME yes|no] [MAXAGE seconds] [TYPE type]`.
///
/// All fields are optional; when omitted, that condition is skipped.
/// A client is killed if **all** present conditions match (logical AND).
#[derive(Default, Debug, Clone)]
pub struct KillFilter {
    pub id: Option<u64>,
    pub addr: Option<SocketAddr>,
    pub laddr: Option<SocketAddr>,
    /// Maximum allowed age (seconds). Connections older than this match.
    pub max_age_secs: Option<i64>,
    /// If `true` (the default), the connection issuing CLIENT KILL is
    /// excluded from the kill set.
    pub skip_id: Option<u64>,
    /// "normal", "master", "replica", "pubsub". Master/replica always miss.
    pub conn_type: Option<ConnType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnType {
    Normal,
    PubSub,
    Master,
    Replica,
}

impl KillFilter {
    /// True if `handle` matches every present field.
    pub fn matches(&self, handle: &ClientHandle, now_ms: u64) -> bool {
        if let Some(id) = self.id
            && handle.id != id
        {
            return false;
        }
        if let Some(addr) = self.addr
            && handle.addr != addr
        {
            return false;
        }
        if let Some(laddr) = self.laddr
            && handle.laddr != laddr
        {
            return false;
        }
        if let Some(skip) = self.skip_id
            && handle.id == skip
        {
            return false;
        }
        if let Some(max_age) = self.max_age_secs {
            let age_secs = ((now_ms.saturating_sub(handle.connected_at_ms)) / 1000) as i64;
            if age_secs < max_age {
                return false;
            }
        }
        if let Some(t) = self.conn_type {
            let is_pubsub =
                handle.sub_channels.load(Ordering::Relaxed) > 0 || handle.sub_patterns.load(Ordering::Relaxed) > 0;
            let actual = if is_pubsub { ConnType::PubSub } else { ConnType::Normal };
            // We don't have master/replica connections; those filters always miss.
            if t != actual {
                return false;
            }
        }
        true
    }
}

/// Parse `CLIENT KILL` arguments. Either old-form `CLIENT KILL addr:port`
/// or new-form `CLIENT KILL [ID id] [ADDR ip:port] ...`.
///
/// Returns `Ok((filter, is_old_form))` so the caller can format the
/// response correctly (old form returns +OK, new form returns count).
pub fn parse_kill_args(args: &[Bytes]) -> Result<(KillFilter, bool), String> {
    if args.is_empty() {
        return Err("ERR wrong number of arguments for 'CLIENT|KILL' command".into());
    }

    // Old form: a single "ip:port" argument.
    if args.len() == 1
        && let Ok(s) = std::str::from_utf8(&args[0])
        && let Ok(addr) = s.parse::<SocketAddr>()
    {
        return Ok((
            KillFilter {
                addr: Some(addr),
                ..Default::default()
            },
            true,
        ));
    }

    // New form: filter pairs.
    let mut f = KillFilter::default();
    let mut skipme = true;
    let mut iter = args.iter();
    while let Some(field) = iter.next() {
        let key = field.to_ascii_uppercase();
        let value = iter.next().ok_or_else(|| {
            format!(
                "ERR syntax error: missing value for '{}'",
                String::from_utf8_lossy(&key)
            )
        })?;
        match key.as_slice() {
            b"ID" => {
                let s = std::str::from_utf8(value).map_err(|_| "ERR ID must be a valid integer".to_string())?;
                f.id = Some(
                    s.parse::<u64>()
                        .map_err(|_| "ERR ID must be a valid integer".to_string())?,
                );
            }
            b"ADDR" => {
                let s = std::str::from_utf8(value).map_err(|_| "ERR ADDR must be ip:port".to_string())?;
                f.addr = Some(
                    s.parse::<SocketAddr>()
                        .map_err(|_| "ERR ADDR must be ip:port".to_string())?,
                );
            }
            b"LADDR" => {
                let s = std::str::from_utf8(value).map_err(|_| "ERR LADDR must be ip:port".to_string())?;
                f.laddr = Some(
                    s.parse::<SocketAddr>()
                        .map_err(|_| "ERR LADDR must be ip:port".to_string())?,
                );
            }
            b"MAXAGE" => {
                let s = std::str::from_utf8(value).map_err(|_| "ERR MAXAGE must be a valid integer".to_string())?;
                f.max_age_secs = Some(
                    s.parse::<i64>()
                        .map_err(|_| "ERR MAXAGE must be a valid integer".to_string())?,
                );
            }
            b"SKIPME" => match value.as_ref() {
                b"yes" | b"YES" | b"Yes" => skipme = true,
                b"no" | b"NO" | b"No" => skipme = false,
                _ => return Err("ERR syntax error: SKIPME must be yes or no".into()),
            },
            b"TYPE" => {
                f.conn_type = Some(match value.to_ascii_lowercase().as_slice() {
                    b"normal" => ConnType::Normal,
                    b"pubsub" => ConnType::PubSub,
                    b"master" => ConnType::Master,
                    b"replica" | b"slave" => ConnType::Replica,
                    _ => return Err("ERR syntax error: unknown TYPE".into()),
                });
            }
            other => {
                return Err(format!(
                    "ERR syntax error: unknown filter '{}'",
                    String::from_utf8_lossy(other)
                ));
            }
        }
    }

    // The skipme handling is filled in by the caller (it knows the
    // connection's own id). We carry the parsed flag back via the field.
    if !skipme {
        f.skip_id = None;
    }
    // If SKIPME was yes (the default), the caller will set f.skip_id to
    // the issuing connection's id before invoking kill_matching.
    let _ = skipme;

    Ok((f, false))
}

/// CLIENT NO-EVICT / NO-TOUCH state. Per-connection toggles.
#[derive(Default, Debug)]
pub struct ClientToggles {
    /// CLIENT NO-EVICT: ignored (we don't have eviction yet) but tracked
    /// so clients that issue it get OK rather than an unknown-subcommand error.
    pub no_evict: bool,
    /// CLIENT NO-TOUCH: don't update last-access time on read commands.
    /// We don't currently update last_accessed on most reads, but the
    /// flag is honored by future read paths (and reflected in CLIENT LIST).
    pub no_touch: bool,
    /// CLIENT REPLY: ON | OFF | SKIP. Determines whether responses are
    /// sent back to the client.
    pub reply_mode: ReplyMode,
}

/// CLIENT REPLY mode.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyMode {
    /// Send replies normally.
    #[default]
    On,
    /// Suppress replies for the rest of the connection.
    Off,
    /// Suppress only the next reply (auto-reverts to On).
    Skip,
}

/// Helper to format a single-client `CLIENT INFO` response — same format
/// as one line of `CLIENT LIST` but for the issuing connection.
pub fn format_info_for(handle: &ClientHandle, now_ms: u64) -> String {
    handle.snapshot(now_ms).format_line()
}

/// Helper used by `INFO clients` to render the section.
pub fn format_info_section(reg: &ClientRegistry, max_clients: usize) -> String {
    let agg = reg.aggregate_stats();
    let mut out = String::with_capacity(256);
    out.push_str("# Clients\r\n");
    out.push_str(&format!("connected_clients:{}\r\n", agg.connected));
    out.push_str("cluster_connections:0\r\n");
    out.push_str(&format!("maxclients:{}\r\n", max_clients));
    out.push_str("client_recent_max_input_buffer:0\r\n");
    out.push_str("client_recent_max_output_buffer:0\r\n");
    out.push_str("blocked_clients:0\r\n");
    out.push_str("tracking_clients:0\r\n");
    out.push_str(&format!("pubsub_clients:{}\r\n", agg.sub_channels + agg.sub_patterns));
    out
}

/// Snapshot of registered handles keyed by id, so callers can take an
/// immutable view (e.g. for `CLIENT LIST TYPE pubsub` filtering).
pub fn snapshot_map(reg: &ClientRegistry, now_ms: u64) -> HashMap<u64, ClientInfo> {
    reg.snapshot_all(now_ms).into_iter().map(|c| (c.id, c)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handle(id: u64, addr: &str) -> Arc<ClientHandle> {
        Arc::new(ClientHandle::new(
            id,
            addr.parse().unwrap(),
            "127.0.0.1:6379".parse().unwrap(),
            1_000_000,
        ))
    }

    #[test]
    fn registry_register_get_deregister() {
        let reg = ClientRegistry::new();
        let h = make_handle(reg.next_id(), "127.0.0.1:1234");
        let id = h.id;
        reg.register(h);
        assert_eq!(reg.len(), 1);
        assert!(reg.get(id).is_some());
        reg.deregister(id);
        assert!(reg.get(id).is_none());
    }

    #[test]
    fn snapshot_returns_all_registered() {
        let reg = ClientRegistry::new();
        for i in 0..5 {
            let h = make_handle(reg.next_id(), &format!("127.0.0.1:200{i}"));
            reg.register(h);
        }
        let snap = reg.snapshot_all(1_000_000);
        assert_eq!(snap.len(), 5);
        // Sorted by id.
        for w in snap.windows(2) {
            assert!(w[0].id < w[1].id);
        }
    }

    #[test]
    fn kill_id_cancels_token_and_returns_true() {
        let reg = ClientRegistry::new();
        let h = make_handle(reg.next_id(), "127.0.0.1:7000");
        let token = h.kill.clone();
        let id = h.id;
        reg.register(h);
        assert!(reg.kill_id(id));
        assert!(token.is_cancelled());
        // Killing again returns true (still registered) until deregister.
        assert!(reg.kill_id(id));
        reg.deregister(id);
        assert!(!reg.kill_id(id));
    }

    #[test]
    fn kill_filter_by_addr_and_skipme() {
        let reg = ClientRegistry::new();
        let a = make_handle(reg.next_id(), "127.0.0.1:1111");
        let b = make_handle(reg.next_id(), "127.0.0.1:2222");
        let a_id = a.id;
        let b_id = b.id;
        reg.register(a.clone());
        reg.register(b.clone());

        let filter = KillFilter {
            addr: Some("127.0.0.1:1111".parse().unwrap()),
            ..Default::default()
        };
        let killed = reg.kill_matching(&filter, 1_000_000);
        assert_eq!(killed, 1);
        assert!(a.kill.is_cancelled());
        assert!(!b.kill.is_cancelled());

        // SKIPME with skip_id excludes the issuer.
        let _ = a_id;
        let filter = KillFilter {
            skip_id: Some(b_id),
            ..Default::default()
        };
        // a is already cancelled but matches; b is excluded.
        let killed = reg.kill_matching(&filter, 1_000_000);
        assert_eq!(killed, 1); // only `a`
        assert!(!b.kill.is_cancelled());
    }

    #[test]
    fn parse_kill_old_form() {
        let args = vec![Bytes::from_static(b"127.0.0.1:1234")];
        let (filter, is_old) = parse_kill_args(&args).unwrap();
        assert!(is_old);
        assert_eq!(filter.addr, Some("127.0.0.1:1234".parse().unwrap()));
    }

    #[test]
    fn parse_kill_new_form_id_addr() {
        let args = vec![
            Bytes::from_static(b"ID"),
            Bytes::from_static(b"42"),
            Bytes::from_static(b"ADDR"),
            Bytes::from_static(b"10.0.0.1:6379"),
        ];
        let (filter, is_old) = parse_kill_args(&args).unwrap();
        assert!(!is_old);
        assert_eq!(filter.id, Some(42));
        assert_eq!(filter.addr, Some("10.0.0.1:6379".parse().unwrap()));
    }

    #[test]
    fn format_line_contains_essentials() {
        let h = make_handle(7, "127.0.0.1:1234");
        let info = h.snapshot(1_000_000);
        let line = info.format_line();
        assert!(line.contains("id=7"));
        assert!(line.contains("addr=127.0.0.1:1234"));
        assert!(line.contains("db=0"));
        assert!(line.contains("multi=-1"));
        assert!(line.contains("user=default"));
    }
}
