//! Process-wide server state shared across all connections.
//!
//! Bundles together the things that admin commands (INFO, CLIENT, CONFIG,
//! SLOWLOG, SHUTDOWN, ...) need to inspect or mutate. One
//! `Arc<ServerState>` is allocated by the listener and threaded through
//! every connection.
//!
//! # What lives here
//!
//! - **`clients`**: registry of connected clients. Powers CLIENT LIST/KILL/INFO.
//! - **`slowlog`**: bounded ring of slow commands. Powers SLOWLOG.
//! - **`config`**: live, mutable subset of server config (CONFIG SET writes here).
//! - **`shutdown_tx`**: broadcast sender so SHUTDOWN can trigger the listener loop to exit.
//! - **`bind_addr`**: where the listener actually bound (for INFO / CLIENT INFO).
//! - **`max_connections`**: cap from CLI/config (for INFO clients section).
//!
//! # What does NOT live here
//!
//! - FDB handles (`Database`, `Directories`) — those are per-namespace
//!   and live on `ConnectionState`.
//! - Pub/sub manager — too domain-specific; passed separately for now.
//! - Per-connection state (RESP version, selected db, etc.) — those
//!   are connection-local.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use bytes::Bytes;
use tokio::sync::broadcast;

use super::clients::SharedClientRegistry;
use super::slowlog::SharedSlowLog;

/// Mutable runtime parameters that operators can tune via `CONFIG SET`.
///
/// Everything is stored as the smallest atomic that fits, so reads from
/// any handler are lock-free. A few text values (e.g. dir, requirepass)
/// live behind a `Mutex<Bytes>` since they're rarely read on the hot path.
pub struct LiveConfig {
    /// `maxclients` — also reflected to the listener semaphore in a future
    /// hot-reload path. For now the runtime value is reported by INFO.
    pub maxclients: AtomicUsize,
    /// `timeout` — idle-client timeout in seconds (0 = disabled).
    pub timeout_secs: AtomicI64,
    /// `tcp-keepalive` — TCP keepalive interval in seconds (0 = disabled).
    pub tcp_keepalive_secs: AtomicI64,
    /// `databases` — number of available db namespaces. Fixed at 16 for now.
    pub databases: AtomicUsize,
    /// `maxmemory` bytes (0 = unlimited; we don't actually enforce yet).
    pub maxmemory_bytes: AtomicI64,
    /// `maxmemory-policy` — eviction policy (we only support "noeviction").
    pub maxmemory_policy: Mutex<Bytes>,
    /// `requirepass` — set to non-empty to require AUTH on each connection.
    /// Currently advisory only (AUTH command exists but doesn't gate handlers).
    pub requirepass: Mutex<Bytes>,
    /// `dir` — working directory reported by INFO server.
    pub dir: Mutex<Bytes>,
    /// `appendonly` — AOF mode toggle. We have no AOF; reported as "no".
    pub appendonly: Mutex<Bytes>,
    /// `save` — RDB save schedule. Empty = persistence-via-FDB only.
    pub save: Mutex<Bytes>,
    /// `loglevel` — text representation. Initialized from CLI but mutable.
    pub loglevel: Mutex<Bytes>,
    /// `slowlog-log-slower-than` mirrors the slow log threshold (microseconds).
    pub slowlog_log_slower_than_us: AtomicI64,
    /// `slowlog-max-len` mirrors the slow log capacity.
    pub slowlog_max_len: AtomicUsize,
}

impl LiveConfig {
    pub fn from_server_config(cfg: &crate::config::ServerConfig) -> Self {
        Self {
            maxclients: AtomicUsize::new(cfg.max_connections),
            timeout_secs: AtomicI64::new(0),
            tcp_keepalive_secs: AtomicI64::new(300),
            databases: AtomicUsize::new(16),
            maxmemory_bytes: AtomicI64::new(0),
            maxmemory_policy: Mutex::new(Bytes::from_static(b"noeviction")),
            requirepass: Mutex::new(Bytes::new()),
            dir: Mutex::new(Bytes::from_static(b".")),
            appendonly: Mutex::new(Bytes::from_static(b"no")),
            save: Mutex::new(Bytes::new()),
            loglevel: Mutex::new(Bytes::from(cfg.log_level.clone().into_bytes())),
            slowlog_log_slower_than_us: AtomicI64::new(super::slowlog::SlowLogConfig::DEFAULT_THRESHOLD_US),
            slowlog_max_len: AtomicUsize::new(super::slowlog::SlowLogConfig::DEFAULT_MAX_LEN),
        }
    }

    /// Read a known parameter as a UTF-8 string. Returns `None` if the
    /// parameter name isn't recognized.
    ///
    /// Names match Redis CONFIG GET conventions (case-insensitive).
    pub fn get(&self, name: &[u8]) -> Option<Bytes> {
        let lower = name.to_ascii_lowercase();
        Some(match lower.as_slice() {
            b"maxclients" => Bytes::from(self.maxclients.load(Ordering::Relaxed).to_string()),
            b"timeout" => Bytes::from(self.timeout_secs.load(Ordering::Relaxed).to_string()),
            b"tcp-keepalive" => Bytes::from(self.tcp_keepalive_secs.load(Ordering::Relaxed).to_string()),
            b"databases" => Bytes::from(self.databases.load(Ordering::Relaxed).to_string()),
            b"maxmemory" => Bytes::from(self.maxmemory_bytes.load(Ordering::Relaxed).to_string()),
            b"maxmemory-policy" => self.maxmemory_policy.lock().unwrap().clone(),
            b"requirepass" => self.requirepass.lock().unwrap().clone(),
            b"dir" => self.dir.lock().unwrap().clone(),
            b"appendonly" => self.appendonly.lock().unwrap().clone(),
            b"save" => self.save.lock().unwrap().clone(),
            b"loglevel" => self.loglevel.lock().unwrap().clone(),
            b"slowlog-log-slower-than" => {
                Bytes::from(self.slowlog_log_slower_than_us.load(Ordering::Relaxed).to_string())
            }
            b"slowlog-max-len" => Bytes::from(self.slowlog_max_len.load(Ordering::Relaxed).to_string()),
            _ => return None,
        })
    }

    /// All known parameter names (lowercase). Used to expand `CONFIG GET <glob>`.
    pub fn known_names() -> &'static [&'static str] {
        &[
            "maxclients",
            "timeout",
            "tcp-keepalive",
            "databases",
            "maxmemory",
            "maxmemory-policy",
            "requirepass",
            "dir",
            "appendonly",
            "save",
            "loglevel",
            "slowlog-log-slower-than",
            "slowlog-max-len",
        ]
    }

    /// Set a known parameter from a string value. Returns `Ok(())` if
    /// the parameter is known and the value parses, otherwise an error
    /// suitable for returning as a Redis ERR reply.
    pub fn set(&self, name: &[u8], value: &[u8]) -> Result<(), String> {
        let lower = name.to_ascii_lowercase();
        let value_str = std::str::from_utf8(value)
            .map_err(|_| format!("ERR invalid value for '{}'", String::from_utf8_lossy(&lower)))?;

        fn parse_i64(name: &str, v: &str) -> Result<i64, String> {
            v.parse::<i64>()
                .map_err(|_| format!("ERR Invalid argument '{v}' for CONFIG SET '{name}'"))
        }

        fn parse_usize(name: &str, v: &str) -> Result<usize, String> {
            v.parse::<usize>()
                .map_err(|_| format!("ERR Invalid argument '{v}' for CONFIG SET '{name}'"))
        }

        match lower.as_slice() {
            b"maxclients" => {
                self.maxclients
                    .store(parse_usize("maxclients", value_str)?, Ordering::Relaxed);
            }
            b"timeout" => {
                self.timeout_secs
                    .store(parse_i64("timeout", value_str)?, Ordering::Relaxed);
            }
            b"tcp-keepalive" => {
                self.tcp_keepalive_secs
                    .store(parse_i64("tcp-keepalive", value_str)?, Ordering::Relaxed);
            }
            b"maxmemory" => {
                self.maxmemory_bytes
                    .store(parse_i64("maxmemory", value_str)?, Ordering::Relaxed);
            }
            b"maxmemory-policy" => {
                // Only "noeviction" is supported. Accept the value for compatibility but log otherwise.
                if value_str != "noeviction" {
                    return Err(format!(
                        "ERR Invalid argument '{value_str}' for CONFIG SET 'maxmemory-policy' (only 'noeviction' is supported)"
                    ));
                }
                *self.maxmemory_policy.lock().unwrap() = Bytes::copy_from_slice(value);
            }
            b"requirepass" => {
                *self.requirepass.lock().unwrap() = Bytes::copy_from_slice(value);
            }
            b"dir" => {
                *self.dir.lock().unwrap() = Bytes::copy_from_slice(value);
            }
            b"appendonly" => {
                *self.appendonly.lock().unwrap() = Bytes::copy_from_slice(value);
            }
            b"save" => {
                *self.save.lock().unwrap() = Bytes::copy_from_slice(value);
            }
            b"loglevel" => {
                *self.loglevel.lock().unwrap() = Bytes::copy_from_slice(value);
            }
            b"slowlog-log-slower-than" => {
                self.slowlog_log_slower_than_us
                    .store(parse_i64("slowlog-log-slower-than", value_str)?, Ordering::Relaxed);
            }
            b"slowlog-max-len" => {
                self.slowlog_max_len
                    .store(parse_usize("slowlog-max-len", value_str)?, Ordering::Relaxed);
            }
            // `databases` is fixed in this build; reject SET attempts.
            b"databases" => {
                return Err("ERR 'databases' is fixed at startup and can't be changed at runtime".into());
            }
            other => {
                return Err(format!(
                    "ERR Unknown option or number of arguments for CONFIG SET - '{}'",
                    String::from_utf8_lossy(other)
                ));
            }
        }
        Ok(())
    }
}

/// Server-wide state shared with every connection.
///
/// Cheap to clone (it's an `Arc`).
pub struct ServerState {
    pub clients: SharedClientRegistry,
    pub slowlog: SharedSlowLog,
    pub config: LiveConfig,
    /// Address the listener bound to. INFO server reports this.
    pub bind_addr: SocketAddr,
    /// Sender for the broadcast shutdown channel. Cloned by SHUTDOWN
    /// command so it can trigger the listener loop to exit.
    /// `Mutex` is just so we can take it out on first SHUTDOWN to
    /// guarantee at-most-once delivery; subsequent calls are no-ops.
    pub shutdown_tx: Mutex<Option<broadcast::Sender<()>>>,
    /// FDB cluster file path, reported by INFO server.
    pub fdb_cluster_file: String,
    /// Server start UNIX timestamp (seconds).
    pub start_timestamp_secs: u64,
}

impl ServerState {
    pub fn new(
        cfg: &crate::config::ServerConfig,
        clients: SharedClientRegistry,
        slowlog: SharedSlowLog,
        bind_addr: SocketAddr,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Self {
        Self {
            clients,
            slowlog,
            config: LiveConfig::from_server_config(cfg),
            bind_addr,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            fdb_cluster_file: cfg.fdb_cluster_file.clone(),
            start_timestamp_secs: crate::observability::metrics::server_start_unix_secs(),
        }
    }

    /// Trigger graceful shutdown. Returns false if shutdown was already
    /// triggered earlier (caller may still want to log this).
    pub fn trigger_shutdown(&self) -> bool {
        let tx = self.shutdown_tx.lock().unwrap().take();
        match tx {
            Some(tx) => {
                let _ = tx.send(());
                true
            }
            None => false,
        }
    }
}

/// Type alias used everywhere that needs the server state.
pub type SharedServerState = Arc<ServerState>;
