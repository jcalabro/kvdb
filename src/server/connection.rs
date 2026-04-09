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

/// Initial capacity for per-connection read and write buffers.
const INITIAL_BUF_CAPACITY: usize = 8 * 1024;

/// Per-connection state.
///
/// Tracks the negotiated protocol version and selected database.
/// Passed to command handlers so they can read/modify connection
/// properties (e.g. HELLO upgrades protocol_version, SELECT
/// changes selected_db).
pub struct ConnectionState {
    /// RESP protocol version (2 or 3). Starts at 2; upgraded via HELLO.
    pub protocol_version: u8,
    /// Currently selected database namespace (0-15).
    pub selected_db: u8,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            protocol_version: 2,
            selected_db: 0,
        }
    }
}

/// Handle a single client connection.
///
/// Reads RESP commands from the socket, dispatches them, and writes
/// responses back. Supports pipelining (multiple commands buffered
/// before flushing). Returns when the client disconnects or QUIT is
/// received.
pub async fn handle(mut socket: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
    debug!(%addr, "new connection");
    metrics::ACTIVE_CONNECTIONS.inc();
    metrics::CONNECTIONS_TOTAL
        .with_label_values(&["accepted"])
        .inc();

    let result = run_loop(&mut socket, addr).await;

    metrics::ACTIVE_CONNECTIONS.dec();
    debug!(%addr, "connection closed");

    result
}

/// The core read/parse/dispatch/encode/write loop.
///
/// Separated from `handle()` so metrics bookkeeping happens exactly
/// once regardless of how the loop exits.
async fn run_loop(socket: &mut TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
    let mut state = ConnectionState::default();
    let mut read_buf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);
    let mut write_buf = BytesMut::with_capacity(INITIAL_BUF_CAPACITY);

    loop {
        let bytes_read = socket.read_buf(&mut read_buf).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        // Parse and execute all complete commands in the buffer.
        // This is what makes pipelining work.
        let mut should_close = false;
        loop {
            match parser::parse(&mut read_buf) {
                Ok(Some(value)) => {
                    if dispatch_one(value, &mut state, &mut write_buf) {
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

/// Dispatch a single parsed RESP value as a command.
///
/// Returns `true` if the connection should be closed after flushing.
fn dispatch_one(
    value: RespValue,
    state: &mut ConnectionState,
    write_buf: &mut BytesMut,
) -> bool {
    match RedisCommand::from_resp(value) {
        Ok(cmd) => {
            let cmd_name = String::from_utf8_lossy(&cmd.name).to_string();
            let timer = metrics::COMMAND_DURATION_SECONDS
                .with_label_values(&[&cmd_name])
                .start_timer();

            let (response, close) = match commands::dispatch(&cmd, state) {
                CommandResponse::Reply(resp) => (resp, false),
                CommandResponse::Close(resp) => (resp, true),
            };

            let status = if matches!(response, RespValue::Error(_)) {
                "err".to_owned()
            } else {
                "ok".to_owned()
            };
            metrics::COMMANDS_TOTAL
                .with_label_values(&[&cmd_name, &status])
                .inc();

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
