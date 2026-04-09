//! Per-connection handler.
//!
//! Each connection maintains its own state: read/write buffers, the
//! negotiated protocol version (RESP2 or RESP3), and the selected
//! database namespace (0-15, default 0).

use std::net::SocketAddr;

use tokio::net::TcpStream;
use tracing::debug;

/// Per-connection state.
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
/// before flushing).
///
/// M2 will implement the full read/parse/dispatch/encode/write loop.
/// For now this is a stub that immediately closes the connection.
pub async fn handle(socket: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
    debug!(%addr, "new connection");
    drop(socket);
    Ok(())
}
