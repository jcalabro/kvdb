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
//! - **FDB cleanup**: each test namespace is removed on drop.

use std::net::SocketAddr;

use foundationdb::directory::{Directory, DirectoryLayer};
use uuid::Uuid;

use kvdb::config::ServerConfig;
use kvdb::storage::database::Database;
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
    /// FDB database handle for cleanup.
    db: Database,
    /// The root prefix used for this test's FDB namespace.
    root_prefix: String,
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

        // Create an isolated FDB namespace for this test.
        let root_prefix = format!("kvdb_test_{}", Uuid::new_v4());
        let db = Database::new(&config.fdb_cluster_file).expect("failed to open FDB — is `just up` running?");

        config.db = Some(db.clone());
        config.root_prefix = Some(root_prefix.clone());

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
                panic!("test server did not start accepting connections within 5 seconds on {addr}");
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
            db,
            root_prefix,
        }
    }

    /// Get a fresh async multiplexed connection to the test server.
    pub async fn connection(&self) -> redis::aio::MultiplexedConnection {
        self.client.get_multiplexed_async_connection().await.unwrap()
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Signal the server to shut down.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Best-effort cleanup of the test FDB namespace.
        let db = self.db.clone();
        let root = self.root_prefix.clone();
        // Use a temporary runtime for cleanup since Drop is synchronous.
        // This is safe because the server task is on the main tokio runtime
        // and we only need a brief transaction to remove the directory.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let trx = db.inner().create_trx().unwrap();
                let dir_layer = DirectoryLayer::default();
                let _ = dir_layer.remove(&trx, &[root]).await;
                let _ = trx.commit().await;
            });
        });
    }
}
