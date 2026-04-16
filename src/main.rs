//! kvdb server entry point.

use clap::Parser;
use tracing::info;

use kvdb::config::ServerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ServerConfig::parse();

    // Initialize the full observability stack (tracing, metrics, profiling).
    // The guard must live for the program's lifetime — dropping it flushes
    // OTEL spans and shuts down the metrics server.
    let _observability_guard = kvdb::observability::init(&config.observability())?;

    info!(
        bind_addr = %config.bind_addr,
        max_connections = config.max_connections,
        tracy = config.tracy,
        otlp = ?config.otlp_endpoint,
        metrics = ?config.metrics_addr,
        "starting kvdb"
    );

    // Create a broadcast channel for coordinating graceful shutdown.
    // The Sender goes both to the listener (which subscribes internally
    // and exposes it via ServerState so the SHUTDOWN command can fire it)
    // and to the ctrl-c watcher.
    let (shutdown_tx, _initial_rx) = tokio::sync::broadcast::channel::<()>(1);
    let ctrl_c_tx = shutdown_tx.clone();

    // Spawn a task that listens for ctrl-c.
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
        info!("received ctrl-c, initiating shutdown");
        let _ = ctrl_c_tx.send(());
    });

    kvdb::server::listener::run(config, shutdown_tx, None).await?;

    info!("kvdb shut down cleanly");
    Ok(())
}
