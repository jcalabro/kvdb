//! TCP listener with connection limiting.
//!
//! Binds to the configured address, accepts connections up to the
//! semaphore limit, and spawns a handler task for each.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::config::ServerConfig;
use crate::server::connection;

/// Run the main server loop: accept connections and dispatch handlers.
///
/// This function runs until the `shutdown` future resolves, at which
/// point it stops accepting new connections and returns.
pub async fn run(config: ServerConfig, shutdown: tokio::sync::broadcast::Receiver<()>) -> anyhow::Result<()> {
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
                        // At connection limit — drop the socket immediately.
                        // The client sees a connection reset.
                        drop(socket);
                        tracing::warn!(%addr, "connection rejected: at limit");
                        continue;
                    }
                };

                tokio::spawn(async move {
                    if let Err(e) = connection::handle(socket, addr).await {
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
