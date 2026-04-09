//! Per-connection handler.
//!
//! Each connection maintains its own state: read/write buffers, the
//! negotiated protocol version (RESP2 or RESP3), and the selected
//! database namespace (0-15, default 0).
//!
//! The handler implements a read/parse/dispatch/encode/write loop that
//! naturally supports pipelining: all complete RESP frames in the read
//! buffer are parsed and dispatched before flushing responses.

use std::net::SocketAddr;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, warn};

use crate::commands::{self, CommandResponse};
use crate::observability::metrics;
use crate::protocol::types::{RedisCommand, RespValue};
use crate::protocol::{encoder, parser};
use crate::storage::{Database, Directories, NamespaceCache};

/// Initial capacity for per-connection read and write buffers.
const INITIAL_BUF_CAPACITY: usize = 8 * 1024;

/// Maximum size of the per-connection read buffer before the server
/// closes the connection. Prevents a malicious client from sending
/// huge payloads (up to the parser's 512MB bulk string limit) to
/// exhaust server memory. 64MB is generous for any legitimate Redis
/// workload.
const MAX_READ_BUF_SIZE: usize = 64 * 1024 * 1024;

/// Per-connection state.
///
/// Tracks the negotiated protocol version, selected database, and FDB
/// handles. Passed to command handlers so they can read/modify connection
/// properties and access the storage layer.
pub struct ConnectionState {
    /// RESP protocol version (2 or 3). Starts at 2; upgraded via HELLO.
    pub protocol_version: u8,
    /// Currently selected database namespace (0-15).
    pub selected_db: u8,
    /// FDB database handle.
    pub db: Database,
    /// FDB directory subspaces for the current namespace.
    pub dirs: Directories,
    /// Shared cache of opened directory handles for all namespaces.
    pub ns_cache: NamespaceCache,
}

impl ConnectionState {
    /// Create a new connection state with the given FDB handles.
    pub fn new(db: Database, dirs: Directories, ns_cache: NamespaceCache) -> Self {
        Self {
            protocol_version: 2,
            selected_db: 0,
            db,
            dirs,
            ns_cache,
        }
    }

    /// Create a stub connection state for unit tests that don't need FDB.
    ///
    /// Lazily initializes a real FDB `Database` and `Directories` once per
    /// process using a shared `OnceLock`. This is safe because `boot()` is
    /// idempotent and tests share the cluster file.
    #[cfg(test)]
    pub fn default_for_test() -> Self {
        use std::sync::OnceLock;

        static TEST_FDB: OnceLock<(Database, Directories, NamespaceCache)> = OnceLock::new();

        let (db, dirs, ns_cache) = TEST_FDB.get_or_init(|| {
            // Spawn a dedicated thread for initialization to avoid
            // "cannot start a runtime from within a runtime" when
            // called inside #[tokio::test].
            std::thread::spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let db = Database::new("fdb.cluster").expect("failed to open FDB for tests");
                let dirs = rt
                    .block_on(Directories::open(&db, 0, "kvdb_test_unit"))
                    .expect("failed to open directories for tests");
                let ns_cache = NamespaceCache::new(db.clone(), "kvdb_test_unit".to_string(), dirs.clone());
                (db, dirs, ns_cache)
            })
            .join()
            .expect("test FDB init thread panicked")
        });

        Self {
            protocol_version: 2,
            selected_db: 0,
            db: db.clone(),
            dirs: dirs.clone(),
            ns_cache: ns_cache.clone(),
        }
    }
}

/// Handle a single client connection.
///
/// Reads RESP commands from the socket, dispatches them, and writes
/// responses back. Supports pipelining (multiple commands buffered
/// before flushing). Returns when the client disconnects or QUIT is
/// received.
pub async fn handle(
    mut socket: TcpStream,
    addr: SocketAddr,
    db: Database,
    dirs: Directories,
    ns_cache: NamespaceCache,
) -> anyhow::Result<()> {
    debug!(%addr, "new connection");
    metrics::ACTIVE_CONNECTIONS.inc();
    metrics::CONNECTIONS_TOTAL.with_label_values(&["accepted"]).inc();

    let result = run_loop(&mut socket, addr, db, dirs, ns_cache).await;

    metrics::ACTIVE_CONNECTIONS.dec();
    debug!(%addr, "connection closed");

    result
}

/// The core read/parse/dispatch/encode/write loop.
///
/// Separated from `handle()` so metrics bookkeeping happens exactly
/// once regardless of how the loop exits.
async fn run_loop(
    socket: &mut TcpStream,
    addr: SocketAddr,
    db: Database,
    dirs: Directories,
    ns_cache: NamespaceCache,
) -> anyhow::Result<()> {
    let mut state = ConnectionState::new(db, dirs, ns_cache);
    let mut read_buf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);
    let mut write_buf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);

    loop {
        let bytes_read = socket.read_buf(&mut read_buf).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        if read_buf.len() > MAX_READ_BUF_SIZE {
            warn!(%addr, buf_size = read_buf.len(), "read buffer exceeded limit, closing connection");
            return Ok(());
        }

        // Parse and execute all complete commands in the buffer.
        // This is what makes pipelining work.
        let mut should_close = false;
        loop {
            match parser::parse(&mut read_buf) {
                Ok(Some(value)) => {
                    if dispatch_one(value, &mut state, &mut write_buf).await {
                        should_close = true;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(%addr, error = %e, "protocol error");
                    let err_resp = RespValue::err(format!("ERR protocol error: {e}"));
                    encoder::encode_into(&mut write_buf, &err_resp, state.protocol_version);
                    should_close = true;
                    break;
                }
            }
        }

        // Flush all accumulated responses in one write syscall.
        if !write_buf.is_empty() {
            socket.write_all(&write_buf).await?;
            write_buf.clear();
        }

        if should_close {
            return Ok(());
        }
    }
}

/// Known command names for safe use as Prometheus metric labels.
/// Unknown commands are bucketed under "UNKNOWN" to prevent
/// label cardinality attacks (a client sending millions of unique
/// invalid command names would create unbounded metric series).
fn metric_label_for_command(name: &[u8]) -> &'static str {
    match name {
        b"PING" => "PING",
        b"ECHO" => "ECHO",
        b"HELLO" => "HELLO",
        b"QUIT" => "QUIT",
        b"COMMAND" => "COMMAND",
        b"CLIENT" => "CLIENT",
        b"GET" => "GET",
        b"SET" => "SET",
        b"DEL" => "DEL",
        b"EXISTS" => "EXISTS",
        b"SETNX" => "SETNX",
        b"SETEX" => "SETEX",
        b"PSETEX" => "PSETEX",
        b"GETDEL" => "GETDEL",
        b"MGET" => "MGET",
        b"MSET" => "MSET",
        b"INCR" => "INCR",
        b"DECR" => "DECR",
        b"INCRBY" => "INCRBY",
        b"DECRBY" => "DECRBY",
        b"INCRBYFLOAT" => "INCRBYFLOAT",
        b"APPEND" => "APPEND",
        b"STRLEN" => "STRLEN",
        b"GETRANGE" => "GETRANGE",
        b"SETRANGE" => "SETRANGE",
        _ => "UNKNOWN",
    }
}

/// Dispatch a single parsed RESP value as a command.
///
/// Returns `true` if the connection should be closed after flushing.
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

            let status = if matches!(response, RespValue::Error(_)) {
                "err"
            } else {
                "ok"
            };
            metrics::COMMANDS_TOTAL.with_label_values(&[label, status]).inc();

            encoder::encode_into(write_buf, &response, state.protocol_version);
            timer.observe_duration();

            close
        }
        Err(e) => {
            let err_resp = RespValue::err(e.to_string());
            encoder::encode_into(write_buf, &err_resp, state.protocol_version);
            false
        }
    }
}
