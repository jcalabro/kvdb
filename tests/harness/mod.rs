//! Shared test infrastructure for integration and acceptance tests.
//!
//! The core abstraction is `TestContext`, which spins up a kvdb server
//! on a random available port with its own FDB namespace, and provides
//! a connected `redis::Client` ready for testing. Everything is torn
//! down on drop.
//!
//! # Usage
//!
//! ```rust,ignore
//! let ctx = TestContext::new().await;
//! let mut con = ctx.connection().await;
//! let _: () = redis::cmd("PING").query_async(&mut con).await.unwrap();
//! ```
//!
//! # Design
//!
//! - **Zero manual setup**: `just up` once for FDB, then tests just work.
//! - **Isolated**: each test gets its own server port and FDB namespace.
//! - **Fast startup**: server binds in <50ms; harness polls until ready.
//! - **Real client**: uses `redis` crate (redis-rs) — validates wire protocol.
//! - **FDB optional at M0**: server starts in stub mode until M3 wires FDB.

use std::net::SocketAddr;

use kvdb::config::ServerConfig;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Test context: owns a running kvdb server and provides client connections.
pub struct TestContext {
    /// The address the server is listening on.
    pub addr: SocketAddr,
    /// Redis client connected to the test server.
    pub client: redis::Client,
    /// Sends the shutdown signal when dropped.
    shutdown_tx: Option<broadcast::Sender<()>>,
    /// Server task handle.
    _server_handle: JoinHandle<()>,
}

impl TestContext {
    /// Start a server on a random port with default config.
    ///
    /// Waits until the server is accepting connections before returning.
    pub async fn new() -> Self {
        Self::new_with_config(ServerConfig::default()).await
    }

    /// Start a server on a random port with the given config.
    pub async fn new_with_config(mut config: ServerConfig) -> Self {
        // Bind to port 0 to get a random available port.
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);

        config.bind_addr = addr;

        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        let server_config = config.clone();
        let server_handle = tokio::spawn(async move {
            if let Err(e) = kvdb::server::listener::run(server_config, shutdown_rx).await {
                // Only log if it's not a shutdown-related error.
                eprintln!("test server error: {e}");
            }
        });

        // Poll until the server is accepting connections (or timeout).
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "test server did not start accepting connections within 5 seconds on {addr}"
                );
            }
            match tokio::net::TcpStream::connect(addr).await {
                Ok(_) => break,
                Err(_) => tokio::time::sleep(tokio::time::Duration::from_millis(5)).await,
            }
        }

        let client = redis::Client::open(format!("redis://{addr}")).unwrap();

        Self {
            addr,
            client,
            shutdown_tx: Some(shutdown_tx),
            _server_handle: server_handle,
        }
    }

    /// Get a fresh async multiplexed connection to the test server.
    pub async fn connection(&self) -> redis::aio::MultiplexedConnection {
        self.client
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Signal the server to shut down.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}
