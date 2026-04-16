//! Per-connection handler.
//!
//! Each connection maintains its own state: read/write buffers, the
//! negotiated protocol version (RESP2 or RESP3), and the selected
//! database namespace (0-15, default 0).
//!
//! The handler implements a read/parse/dispatch/encode/write loop that
//! naturally supports pipelining: all complete RESP frames in the read
//! buffer are parsed and dispatched before flushing responses.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use foundationdb::Transaction;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::commands::{self, CommandResponse};
use crate::observability::metrics;
use crate::protocol::types::{RedisCommand, RespValue};
use crate::protocol::{encoder, parser};
use crate::pubsub::{PubSubMessage, SharedPubSubManager};
use crate::storage::{Database, Directories, NamespaceCache};

/// Initial capacity for per-connection read and write buffers.
const INITIAL_BUF_CAPACITY: usize = 8 * 1024;

/// Maximum size of the per-connection read buffer before the server
/// closes the connection. Prevents a malicious client from sending
/// huge payloads (up to the parser's 512MB bulk string limit) to
/// exhaust server memory. 64MB is generous for any legitimate Redis
/// workload.
const MAX_READ_BUF_SIZE: usize = 64 * 1024 * 1024;

/// A watched key and its meta snapshot taken at WATCH time.
///
/// At EXEC time we re-read the meta and compare — if it changed,
/// the transaction is aborted (optimistic lock failure).
#[derive(Debug, Clone)]
pub struct WatchedKey {
    /// The Redis key being watched.
    pub key: Bytes,
    /// The raw serialized ObjectMeta bytes at WATCH time, or `None`
    /// if the key did not exist.
    pub meta_snapshot: Option<Vec<u8>>,
}

/// State for a Redis MULTI/EXEC transaction.
///
/// Tracks queued commands, watched keys, and whether a syntax error
/// was encountered during queuing (which causes EXEC to abort).
pub struct TransactionState {
    /// Commands queued between MULTI and EXEC.
    pub queued: Vec<RedisCommand>,
    /// Keys being WATCHed with their meta snapshots.
    pub watched: Vec<WatchedKey>,
    /// Set `true` if any command queued during MULTI had a syntax error.
    /// When set, EXEC will refuse to execute and return EXECABORT.
    pub error_flag: bool,
}

impl TransactionState {
    /// Create a new transaction state, carrying over any previously
    /// WATCHed keys with their snapshots.
    pub fn new(watched: Vec<WatchedKey>) -> Self {
        Self {
            queued: Vec::new(),
            watched,
            error_flag: false,
        }
    }
}

/// Per-connection state.
///
/// Tracks the negotiated protocol version, selected database, FDB
/// handles, optional MULTI/EXEC transaction state, and pub/sub
/// subscription state. Passed to command handlers so they can
/// read/modify connection properties and access the storage layer.
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
    /// MULTI/EXEC transaction state. `Some` when inside a MULTI block.
    pub transaction: Option<TransactionState>,
    /// Keys being WATCHed outside of a MULTI block (with their meta
    /// snapshots taken at WATCH time). Moved into `TransactionState`
    /// when MULTI is issued, and cleared on EXEC / DISCARD / UNWATCH.
    pub watched_keys: Vec<WatchedKey>,
    /// When executing queued commands inside EXEC, this holds the
    /// shared FDB transaction. Handlers detect this and use it instead
    /// of creating their own transaction via `run_transact`.
    pub active_transaction: Option<Arc<Transaction>>,

    // ---- Pub/sub state ----
    /// Shared pub/sub manager for this server instance.
    pub pubsub: SharedPubSubManager,
    /// This connection's unique ID (for subscription tracking).
    pub connection_id: u64,
    /// Channels this connection is directly subscribed to.
    pub subscribed_channels: HashSet<Bytes>,
    /// Patterns this connection is subscribed to via PSUBSCRIBE.
    pub subscribed_patterns: HashSet<Bytes>,
    /// Sender for pub/sub messages to this connection. Created on
    /// first SUBSCRIBE/PSUBSCRIBE, paired with the receiver in the
    /// connection loop.
    pub pubsub_tx: Option<mpsc::UnboundedSender<PubSubMessage>>,
    /// Cancellation tokens for per-channel FDB watcher tasks.
    ///
    /// Keyed by channel name. Cancelling a token stops the corresponding
    /// watcher task spawned by SUBSCRIBE. Populated by SUBSCRIBE,
    /// drained by UNSUBSCRIBE and RESET.
    pub subscribed_channel_cancels: HashMap<Bytes, CancellationToken>,
    /// Cancellation tokens for per-pattern FDB watcher tasks.
    ///
    /// Keyed by pattern. Cancelling a token stops the corresponding
    /// watcher task spawned by PSUBSCRIBE. Populated by PSUBSCRIBE,
    /// drained by PUNSUBSCRIBE and RESET.
    pub subscribed_pattern_cancels: HashMap<Bytes, CancellationToken>,
}

impl ConnectionState {
    /// Returns the active shared FDB transaction, if inside MULTI/EXEC.
    ///
    /// Command handlers pass this to `run_transact` so that queued
    /// commands within an EXEC block share a single FDB transaction.
    pub fn shared_txn(&self) -> Option<&Arc<Transaction>> {
        self.active_transaction.as_ref()
    }

    /// Total number of active subscriptions (channels + patterns).
    pub fn subscription_count(&self) -> usize {
        self.subscribed_channels.len() + self.subscribed_patterns.len()
    }

    /// Create a new connection state with the given FDB handles.
    pub fn new(
        db: Database,
        dirs: Directories,
        ns_cache: NamespaceCache,
        pubsub: SharedPubSubManager,
        connection_id: u64,
    ) -> Self {
        Self {
            protocol_version: 2,
            selected_db: 0,
            db,
            dirs,
            ns_cache,
            transaction: None,
            watched_keys: Vec::new(),
            active_transaction: None,
            pubsub,
            connection_id,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
            pubsub_tx: None,
            subscribed_channel_cancels: HashMap::new(),
            subscribed_pattern_cancels: HashMap::new(),
        }
    }

    /// Create a stub connection state for unit tests that don't need FDB.
    ///
    /// Lazily initializes a real FDB `Database` and `Directories` once per
    /// process using a shared `OnceLock`. This is safe because `boot()` is
    /// idempotent and tests share the cluster file.
    #[cfg(test)]
    pub fn default_for_test() -> Self {
        use crate::pubsub::{PubSubDirectories, PubSubManager};
        use std::sync::OnceLock;

        static TEST_FDB: OnceLock<(Database, Directories, NamespaceCache, SharedPubSubManager)> = OnceLock::new();

        let (db, dirs, ns_cache, pubsub) = TEST_FDB.get_or_init(|| {
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
                let pubsub_dirs = rt
                    .block_on(PubSubDirectories::open(&db, "kvdb_test_unit"))
                    .expect("failed to open pubsub directories for tests");
                let pubsub = Arc::new(PubSubManager::new(db.clone(), pubsub_dirs));
                (db, dirs, ns_cache, pubsub)
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
            transaction: None,
            watched_keys: Vec::new(),
            active_transaction: None,
            pubsub: pubsub.clone(),
            connection_id: 0,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
            pubsub_tx: None,
            subscribed_channel_cancels: HashMap::new(),
            subscribed_pattern_cancels: HashMap::new(),
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
    pubsub: SharedPubSubManager,
    connection_id: u64,
) -> anyhow::Result<()> {
    debug!(%addr, "new connection");
    metrics::ACTIVE_CONNECTIONS.inc();
    metrics::CONNECTIONS_TOTAL.with_label_values(&["accepted"]).inc();

    let result = run_loop(&mut socket, addr, db, dirs, ns_cache, pubsub.clone(), connection_id).await;

    // Clean up any remaining pub/sub subscriptions on disconnect.
    pubsub.unsubscribe_all(connection_id);

    metrics::ACTIVE_CONNECTIONS.dec();
    debug!(%addr, "connection closed");

    result
}

/// The core read/parse/dispatch/encode/write loop.
///
/// Separated from `handle()` so metrics bookkeeping happens exactly
/// once regardless of how the loop exits.
///
/// Uses `tokio::select!` to concurrently handle:
/// 1. Socket reads (normal command processing + pipelining)
/// 2. Pub/sub message delivery (unsolicited Push messages from FDB watcher tasks)
///
/// When no pub/sub subscriptions are active, the pub/sub branch
/// returns `Pending` forever — zero overhead for non-subscriber
/// connections.
async fn run_loop(
    socket: &mut TcpStream,
    addr: SocketAddr,
    db: Database,
    dirs: Directories,
    ns_cache: NamespaceCache,
    pubsub: SharedPubSubManager,
    connection_id: u64,
) -> anyhow::Result<()> {
    let mut state = ConnectionState::new(db, dirs, ns_cache, pubsub, connection_id);
    let mut read_buf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);
    let mut write_buf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);

    // The receiver for pub/sub messages. Created lazily on first
    // SUBSCRIBE/PSUBSCRIBE. The sender half is stored in
    // `state.pubsub_tx` and registered with the PubSubManager.
    let mut pubsub_rx: Option<mpsc::UnboundedReceiver<PubSubMessage>> = None;

    loop {
        tokio::select! {
            // Branch 1: Socket data available (normal command processing).
            result = socket.read_buf(&mut read_buf) => {
                let bytes_read = result?;
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
                            if dispatch_one(value, &mut state, &mut write_buf, &mut pubsub_rx).await {
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

            // Branch 2: Pub/sub message to deliver.
            // recv_pubsub returns Pending when pubsub_rx is None,
            // so this branch is effectively disabled for non-subscribers.
            msg = recv_pubsub(&mut pubsub_rx) => {
                match msg {
                    Some(msg) => {
                        let resp = pubsub_message_to_resp(&msg);
                        encoder::encode_into(&mut write_buf, &resp, state.protocol_version);
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                    None => {
                        // All senders were dropped (the channel closed). This
                        // can happen if a RESET clears pubsub_tx while all
                        // watcher tasks are also exiting. Clear the receiver
                        // so recv_pubsub pends forever rather than spinning
                        // on the closed channel. New subscriptions will create
                        // a fresh channel via ensure_pubsub_channel.
                        pubsub_rx = None;
                    }
                }
            }
        }
    }
}

/// Receive a pub/sub message, or pend forever if not subscribed.
///
/// This is the key to zero-overhead for non-subscriber connections:
/// when `rx` is `None`, this future never resolves, so `select!`
/// only polls the socket branch.
async fn recv_pubsub(rx: &mut Option<mpsc::UnboundedReceiver<PubSubMessage>>) -> Option<PubSubMessage> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Convert a `PubSubMessage` to a RESP value for wire encoding.
///
/// Uses `RespValue::Push` for RESP3 clients and the encoder
/// automatically downgrades to `Array` for RESP2 clients.
fn pubsub_message_to_resp(msg: &PubSubMessage) -> RespValue {
    match msg {
        PubSubMessage::Message { channel, data } => RespValue::Push(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"message"))),
            RespValue::BulkString(Some(channel.clone())),
            RespValue::BulkString(Some(data.clone())),
        ]),
        PubSubMessage::PMessage { pattern, channel, data } => RespValue::Push(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"pmessage"))),
            RespValue::BulkString(Some(pattern.clone())),
            RespValue::BulkString(Some(channel.clone())),
            RespValue::BulkString(Some(data.clone())),
        ]),
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
        b"TTL" => "TTL",
        b"PTTL" => "PTTL",
        b"EXPIRETIME" => "EXPIRETIME",
        b"PEXPIRETIME" => "PEXPIRETIME",
        b"EXPIRE" => "EXPIRE",
        b"PEXPIRE" => "PEXPIRE",
        b"EXPIREAT" => "EXPIREAT",
        b"PEXPIREAT" => "PEXPIREAT",
        b"PERSIST" => "PERSIST",
        b"TYPE" => "TYPE",
        b"UNLINK" => "UNLINK",
        b"TOUCH" => "TOUCH",
        b"DBSIZE" => "DBSIZE",
        b"RENAME" => "RENAME",
        b"RENAMENX" => "RENAMENX",
        b"SELECT" => "SELECT",
        b"FLUSHDB" => "FLUSHDB",
        b"FLUSHALL" => "FLUSHALL",
        b"MULTI" => "MULTI",
        b"EXEC" => "EXEC",
        b"DISCARD" => "DISCARD",
        b"WATCH" => "WATCH",
        b"UNWATCH" => "UNWATCH",
        b"SUBSCRIBE" => "SUBSCRIBE",
        b"UNSUBSCRIBE" => "UNSUBSCRIBE",
        b"PSUBSCRIBE" => "PSUBSCRIBE",
        b"PUNSUBSCRIBE" => "PUNSUBSCRIBE",
        b"PUBLISH" => "PUBLISH",
        b"PUBSUB" => "PUBSUB",
        b"RESET" => "RESET",
        _ => "UNKNOWN",
    }
}

/// Dispatch a single parsed RESP value as a command.
///
/// Returns `true` if the connection should be closed after flushing.
///
/// `pubsub_rx` is passed through so that SUBSCRIBE/PSUBSCRIBE handlers
/// can lazily create the message channel and hand the receiver back
/// to the connection loop.
async fn dispatch_one(
    value: RespValue,
    state: &mut ConnectionState,
    write_buf: &mut BytesMut,
    pubsub_rx: &mut Option<mpsc::UnboundedReceiver<PubSubMessage>>,
) -> bool {
    match RedisCommand::from_resp(value) {
        Ok(cmd) => {
            let label = metric_label_for_command(&cmd.name);
            let timer = metrics::COMMAND_DURATION_SECONDS
                .with_label_values(&[label])
                .start_timer();

            let (response, close) = match commands::dispatch(&cmd, state, pubsub_rx).await {
                CommandResponse::Reply(resp) => (resp, false),
                CommandResponse::Close(resp) => (resp, true),
                CommandResponse::MultiReply(resps) => {
                    // Pub/sub commands return multiple responses (one per channel).
                    for resp in resps {
                        encoder::encode_into(write_buf, &resp, state.protocol_version);
                    }
                    timer.observe_duration();
                    let status = "ok";
                    metrics::COMMANDS_TOTAL.with_label_values(&[label, status]).inc();
                    return false;
                }
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
