//! Prometheus metrics definitions and HTTP scrape endpoint.
//!
//! Metrics are defined as `lazy_static` globals using the `prometheus`
//! crate. Command-specific metrics (per-command latency histograms,
//! etc.) will be added as commands are implemented in later milestones.
//!
//! The HTTP endpoint is intentionally minimal — a raw TCP listener that
//! serves the Prometheus text exposition format on any request. No HTTP
//! framework needed for a scrape target.

use std::net::SocketAddr;

use prometheus::{Encoder, Gauge, HistogramVec, IntCounterVec, IntGauge, TextEncoder};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{error, warn};

lazy_static::lazy_static! {
    // ---- Connection metrics ----

    /// Number of currently active client connections.
    pub static ref ACTIVE_CONNECTIONS: IntGauge = prometheus::register_int_gauge!(
        "kvdb_active_connections",
        "Number of active Redis client connections"
    ).unwrap();

    /// Total connections accepted since server start.
    pub static ref CONNECTIONS_TOTAL: IntCounterVec = prometheus::register_int_counter_vec!(
        "kvdb_connections_total",
        "Total connections accepted",
        &["status"] // "accepted", "rejected_limit"
    ).unwrap();

    // ---- Command metrics ----

    /// Total commands processed, by command name and result status.
    pub static ref COMMANDS_TOTAL: IntCounterVec = prometheus::register_int_counter_vec!(
        "kvdb_commands_total",
        "Total Redis commands processed",
        &["command", "status"] // status: "ok", "err"
    ).unwrap();

    /// Command execution duration in seconds, by command name.
    pub static ref COMMAND_DURATION_SECONDS: HistogramVec = prometheus::register_histogram_vec!(
        "kvdb_command_duration_seconds",
        "Redis command execution duration",
        &["command"],
        // Buckets tuned for FDB-backed latencies:
        // sub-ms for cache hits, 1-5ms for reads, 5-50ms for writes/complex ops
        vec![0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
    ).unwrap();

    // ---- FDB transaction metrics ----

    /// FDB transaction conflict count by operation type.
    pub static ref FDB_CONFLICTS_TOTAL: IntCounterVec = prometheus::register_int_counter_vec!(
        "kvdb_fdb_conflicts_total",
        "FoundationDB transaction conflicts",
        &["operation"]
    ).unwrap();

    /// FDB transaction retry count by operation type.
    pub static ref FDB_RETRIES_TOTAL: IntCounterVec = prometheus::register_int_counter_vec!(
        "kvdb_fdb_retries_total",
        "FoundationDB transaction retry attempts",
        &["operation"]
    ).unwrap();

    /// FDB transaction duration in seconds.
    pub static ref FDB_TRANSACTION_DURATION_SECONDS: HistogramVec = prometheus::register_histogram_vec!(
        "kvdb_fdb_transaction_duration_seconds",
        "FoundationDB transaction duration",
        &["operation", "status"], // status: "committed", "conflict", "error"
        vec![0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0]
    ).unwrap();

    // ---- TTL expiry metrics ----

    /// Total keys expired (lazy + active).
    pub static ref EXPIRED_KEYS_TOTAL: IntCounterVec = prometheus::register_int_counter_vec!(
        "kvdb_expired_keys_total",
        "Total keys expired",
        &["method"] // "lazy", "active"
    ).unwrap();

    /// Current backlog of keys pending active expiration.
    pub static ref EXPIRY_BACKLOG: Gauge = prometheus::register_gauge!(
        "kvdb_expiry_backlog",
        "Estimated keys pending active expiration"
    ).unwrap();
}

/// Register default process-level metrics (CPU, memory, file descriptors).
///
/// The `prometheus` crate's `process` feature provides these automatically
/// on Linux via `/proc/self/stat`.
pub fn register_default_metrics() {
    // Force evaluation of all lazy_static metrics so they appear in
    // output even before first use (Prometheus best practice: metrics
    // should exist with value 0 before the first event).
    let _ = ACTIVE_CONNECTIONS.get();
    let _ = CONNECTIONS_TOTAL.with_label_values(&["accepted"]).get();
    let _ = COMMANDS_TOTAL.with_label_values(&["PING", "ok"]).get();
}

/// Serve the Prometheus text exposition format over HTTP.
///
/// This is a minimal HTTP responder — it accepts any TCP connection,
/// reads (and discards) the request, and responds with the metrics
/// payload. Prometheus scrapers don't need a full HTTP framework.
pub async fn serve_metrics(addr: SocketAddr) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(%addr, error = %e, "failed to bind metrics endpoint");
            return;
        }
    };

    loop {
        let (mut stream, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(error = %e, "metrics endpoint accept error");
                continue;
            }
        };

        tokio::spawn(async move {
            // Read and discard the HTTP request. We don't need to parse it —
            // any connection to this port gets the metrics response.
            let mut request_buf = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut request_buf).await;

            let encoder = TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut body = Vec::new();
            if let Err(e) = encoder.encode(&metric_families, &mut body) {
                warn!(%peer, error = %e, "failed to encode metrics");
                return;
            }

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                encoder.format_type(),
                body.len()
            );

            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        });
    }
}
