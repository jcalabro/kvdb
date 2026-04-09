//! TCP listener with connection limiting.
//!
//! Binds to the configured address, accepts connections up to the
//! semaphore limit, and spawns a handler task for each.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::config::ServerConfig;
use crate::observability::metrics;
use crate::server::connection;
use crate::storage::{Database, Directories};

/// Maximum number of retries for opening FDB directories.
///
/// Directory creation uses a regular FDB transaction that can conflict
/// when many processes (e.g. parallel tests) initialize simultaneously.
const DIR_OPEN_MAX_RETRIES: u32 = 5;

/// Run the main server loop: accept connections and dispatch handlers.
///
/// This function runs until the `shutdown` future resolves, at which
/// point it stops accepting new connections and returns.
pub async fn run(config: ServerConfig, shutdown: tokio::sync::broadcast::Receiver<()>) -> anyhow::Result<()> {
    // Initialize FDB: use injected handles (tests) or create fresh ones.
    let db = match config.db {
        Some(ref db) => db.clone(),
        None => Database::new(&config.fdb_cluster_file)?,
    };

    let root_prefix = config.root_prefix.as_deref().unwrap_or("kvdb");

    // Retry directory open — the underlying FDB transaction can conflict
    // when multiple servers (or tests) start concurrently.
    let mut dirs = None;
    for attempt in 0..DIR_OPEN_MAX_RETRIES {
        match Directories::open(&db, 0, root_prefix).await {
            Ok(d) => {
                dirs = Some(d);
                break;
            }
            Err(e) if attempt + 1 < DIR_OPEN_MAX_RETRIES => {
                warn!(attempt, error = %e, "directory open failed, retrying");
                tokio::time::sleep(tokio::time::Duration::from_millis(50 * (attempt as u64 + 1))).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    let dirs = dirs.expect("directory open succeeded within retry limit");

    let listener = TcpListener::bind(config.bind_addr).await?;
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
                tokio::spawn(async move {
                    if let Err(e) = connection::handle(socket, addr, conn_db, conn_dirs).await {
                        error!(%addr, error = %e, "connection error");
                    }
                    drop(permit);
                });
            }
            _ = shutdown.recv() => {
                info!("shutting down listener");
                break;
            }
        }
    }

    Ok(())
}
