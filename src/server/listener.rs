//! TCP listener with connection limiting.
//!
//! Binds to the configured address, accepts connections up to the
//! semaphore limit, and spawns a handler task for each.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::ServerConfig;
use crate::observability::metrics;
use crate::pubsub::{self, PubSubDirectories, PubSubManager, SharedPubSubManager};
use crate::server::connection;
use crate::storage::{Database, Directories, NamespaceCache};
use crate::ttl;

/// Run the main server loop: accept connections and dispatch handlers.
///
/// This function runs until the `shutdown` future resolves, at which
/// point it stops accepting new connections and returns.
///
/// If `pre_bound` is `Some`, the server uses that listener instead of
/// binding a new one. This eliminates the port-reuse TOCTOU race in
/// tests (bind port 0 → drop → rebind).
pub async fn run(
    config: ServerConfig,
    shutdown: tokio::sync::broadcast::Receiver<()>,
    pre_bound: Option<TcpListener>,
) -> anyhow::Result<()> {
    // Initialize FDB: use injected handles (tests) or create fresh ones.
    let db = match config.db {
        Some(ref db) => db.clone(),
        None => Database::new(&config.fdb_cluster_file)?,
    };

    let root_prefix = config.root_prefix.as_deref().unwrap_or("kvdb");

    // Directories::open() has built-in retry logic for FDB transaction
    // conflicts that occur when many processes open directories concurrently.
    let dirs = Directories::open(&db, 0, root_prefix).await?;

    let ns_cache = NamespaceCache::new(db.clone(), root_prefix.to_string(), dirs.clone());

    // Open pub/sub FDB directories (global, not per-namespace).
    let pubsub_dirs = PubSubDirectories::open(&db, root_prefix).await?;
    let pubsub_manager: SharedPubSubManager = Arc::new(PubSubManager::new(db.clone(), pubsub_dirs.clone()));

    // Spawn the background expiry worker.
    let cancel_token = CancellationToken::new();
    let worker_cancel = cancel_token.clone();
    let worker_ns_cache = ns_cache.clone();
    tokio::spawn(async move {
        ttl::worker::run(worker_ns_cache, ttl::ExpiryConfig::default(), worker_cancel).await;
    });

    // Spawn the pub/sub cleanup worker.
    let pubsub_cleanup_cancel = cancel_token.clone();
    let pubsub_cleanup_db = db.clone();
    tokio::spawn(async move {
        pubsub::cleanup::run(
            pubsub_cleanup_db,
            pubsub_dirs,
            pubsub::cleanup::PubSubCleanupConfig::default(),
            pubsub_cleanup_cancel,
        )
        .await;
    });

    let listener = match pre_bound {
        Some(l) => l,
        None => TcpListener::bind(config.bind_addr).await?,
    };
    let semaphore = Arc::new(Semaphore::new(config.max_connections));

    info!(addr = %config.bind_addr, "listening for connections");

    let mut shutdown = shutdown;

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (socket, addr) = result?;

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        metrics::CONNECTIONS_TOTAL
                            .with_label_values(&["rejected_limit"])
                            .inc();
                        // At connection limit — drop the socket immediately.
                        // The client sees a connection reset.
                        drop(socket);
                        tracing::warn!(%addr, "connection rejected: at limit");
                        continue;
                    }
                };

                let conn_db = db.clone();
                let conn_dirs = dirs.clone();
                let conn_ns_cache = ns_cache.clone();
                let conn_pubsub = pubsub_manager.clone();
                let conn_id = pubsub_manager.next_connection_id();
                tokio::spawn(async move {
                    if let Err(e) = connection::handle(
                        socket, addr, conn_db, conn_dirs, conn_ns_cache,
                        conn_pubsub, conn_id,
                    ).await {
                        error!(%addr, error = %e, "connection error");
                    }
                    drop(permit);
                });
            }
            _ = shutdown.recv() => {
                info!("shutting down listener");
                cancel_token.cancel();
                break;
            }
        }
    }

    Ok(())
}
