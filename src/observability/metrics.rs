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
use std::sync::OnceLock;
use std::time::Instant;

use prometheus::{Encoder, Gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, TextEncoder};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::{error, warn};

/// Process start time, captured the first time it's queried.
///
/// Used both by the `kvdb_uptime_seconds` gauge and the `INFO server`
/// command. Initialized lazily so tests that never touch metrics still
/// report a sane uptime.
static SERVER_START: OnceLock<Instant> = OnceLock::new();

/// Wall-clock UNIX timestamp (seconds) of process start, captured once.
static SERVER_START_UNIX_SECS: OnceLock<u64> = OnceLock::new();

/// Initialize and return the process start `Instant`. Idempotent.
pub fn server_start_instant() -> Instant {
    *SERVER_START.get_or_init(Instant::now)
}

/// Initialize and return the process start UNIX timestamp in seconds. Idempotent.
pub fn server_start_unix_secs() -> u64 {
    *SERVER_START_UNIX_SECS.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    })
}

/// Server uptime in seconds. Cheap: derived from the cached start `Instant`.
pub fn uptime_secs() -> u64 {
    server_start_instant().elapsed().as_secs()
}

lazy_static::lazy_static! {
    // ---- Server lifecycle / build metadata ----

    /// UNIX timestamp (seconds) at which this process started.
    /// Cheap, set-once gauge — the value never changes after init.
    pub static ref SERVER_START_TIMESTAMP_SECONDS: IntGauge = prometheus::register_int_gauge!(
        "kvdb_start_timestamp_seconds",
        "UNIX timestamp at which the server process started"
    ).unwrap();

    /// Build-info gauge (always 1). Labels carry version + git metadata
    /// so dashboards can `count by (version)` across a fleet.
    pub static ref BUILD_INFO: IntGauge = {
        let g = prometheus::register_int_gauge_vec!(
            "kvdb_build_info",
            "Build information; labels carry version, rustc, and target",
            &["version", "rustc", "target", "profile"]
        ).unwrap();
        g.with_label_values(&[
            env!("CARGO_PKG_VERSION"),
            option_env!("RUSTC_VERSION").unwrap_or("unknown"),
            std::env::consts::ARCH,
            if cfg!(debug_assertions) { "debug" } else { "release" },
        ]).set(1);
        // Return a placeholder gauge so the static type is satisfied.
        // The labelled gauge above is what's actually exposed.
        prometheus::register_int_gauge!(
            "kvdb_build_info_initialized",
            "1 if build_info has been registered (internal sentinel)"
        ).unwrap()
    };

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
        &["status"] // "accepted", "rejected_limit", "killed"
    ).unwrap();

    /// Bytes read from client sockets since server start.
    pub static ref NETWORK_BYTES_READ_TOTAL: IntCounter = prometheus::register_int_counter!(
        "kvdb_network_bytes_read_total",
        "Total bytes read from client sockets"
    ).unwrap();

    /// Bytes written to client sockets since server start.
    pub static ref NETWORK_BYTES_WRITTEN_TOTAL: IntCounter = prometheus::register_int_counter!(
        "kvdb_network_bytes_written_total",
        "Total bytes written to client sockets"
    ).unwrap();

    // ---- Command metrics ----

    /// Total commands processed, by command name and result status.
    pub static ref COMMANDS_TOTAL: IntCounterVec = prometheus::register_int_counter_vec!(
        "kvdb_commands_total",
        "Total Redis commands processed",
        &["command", "status"] // status: "ok", "err"
    ).unwrap();

    /// Total commands processed (aggregate, no per-command label).
    /// Cheap to query for INFO totals — no need to sum a HistogramVec.
    pub static ref COMMANDS_PROCESSED_TOTAL: IntCounter = prometheus::register_int_counter!(
        "kvdb_commands_processed_total",
        "Total commands processed (all commands, all statuses)"
    ).unwrap();

    /// MULTI / EXEC outcomes. A small, bounded label set.
    pub static ref TRANSACTION_OUTCOMES_TOTAL: IntCounterVec =
        prometheus::register_int_counter_vec!(
            "kvdb_transaction_outcomes_total",
            "MULTI/EXEC outcomes",
            &["outcome"] // started, committed, discarded, exec_aborted, watch_aborted
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

    /// Server uptime in seconds. Updated on every Prometheus scrape so
    /// it always reflects current wall-clock time.
    pub static ref UPTIME_SECONDS: IntGauge = prometheus::register_int_gauge!(
        "kvdb_uptime_seconds",
        "Time since process start in seconds"
    ).unwrap();

    /// Duration of each background expiry scan iteration.
    pub static ref EXPIRY_SCAN_DURATION_SECONDS: Histogram = prometheus::register_histogram!(
        "kvdb_expiry_scan_duration_seconds",
        "Time spent per background expiry scan iteration",
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    ).unwrap();

    // ---- Pub/sub metrics ----

    /// Total messages published.
    ///
    /// Intentionally unlabeled: a per-channel label would be unbounded
    /// cardinality (any client can `PUBLISH` to any channel name),
    /// which is a denial-of-service vector against the metrics registry.
    /// Channel-level breakdown is available via PUBSUB CHANNELS / NUMSUB.
    pub static ref PUBSUB_MESSAGES_PUBLISHED_TOTAL: prometheus::IntCounter =
        prometheus::register_int_counter!(
            "kvdb_pubsub_messages_published_total",
            "Total pub/sub messages published"
        ).unwrap();

    /// Total messages delivered to local subscribers.
    pub static ref PUBSUB_MESSAGES_DELIVERED_TOTAL: prometheus::IntCounter =
        prometheus::register_int_counter!(
            "kvdb_pubsub_messages_delivered_total",
            "Total pub/sub messages delivered to local subscribers"
        ).unwrap();

    /// Current number of active channel subscriptions on this instance.
    pub static ref PUBSUB_ACTIVE_SUBSCRIPTIONS: IntGauge = prometheus::register_int_gauge!(
        "kvdb_pubsub_active_subscriptions",
        "Active channel subscriptions on this server instance"
    ).unwrap();

    /// Current number of active pattern subscriptions on this instance.
    pub static ref PUBSUB_ACTIVE_PATTERNS: IntGauge = prometheus::register_int_gauge!(
        "kvdb_pubsub_active_patterns",
        "Active PSUBSCRIBE pattern subscriptions on this server instance"
    ).unwrap();
}

/// Register default process-level metrics (CPU, memory, file descriptors).
///
/// The `prometheus` crate's `process` feature provides these automatically
/// on Linux via `/proc/self/stat`.
///
/// Also seeds the start-time metrics so dashboards can graph uptime
/// from the very first scrape.
pub fn register_default_metrics() {
    // Force evaluation of all lazy_static metrics so they appear in
    // output even before first use (Prometheus best practice: metrics
    // should exist with value 0 before the first event).
    let _ = ACTIVE_CONNECTIONS.get();
    let _ = CONNECTIONS_TOTAL.with_label_values(&["accepted"]).get();
    let _ = COMMANDS_TOTAL.with_label_values(&["PING", "ok"]).get();
    let _ = COMMANDS_PROCESSED_TOTAL.get();
    let _ = NETWORK_BYTES_READ_TOTAL.get();
    let _ = NETWORK_BYTES_WRITTEN_TOTAL.get();
    let _ = TRANSACTION_OUTCOMES_TOTAL.with_label_values(&["started"]).get();

    // Seed start-time metadata.
    SERVER_START_TIMESTAMP_SECONDS.set(server_start_unix_secs() as i64);
    let _ = server_start_instant(); // capture monotonic start
    let _ = BUILD_INFO.get(); // forces lazy_static evaluation, which registers labelled gauge
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

            // Refresh dynamically-derived gauges before encoding.
            UPTIME_SECONDS.set(uptime_secs() as i64);

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
