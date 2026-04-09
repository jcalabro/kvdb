//! Server configuration.
//!
//! Supports loading from CLI args with environment variable overrides.
//! Defaults are tuned for local development; production deployments
//! should override via config file or environment.

use std::net::SocketAddr;

use clap::Parser;

use crate::observability::{LogFormat, ObservabilityConfig};

/// kvdb — Redis-compatible database on FoundationDB
#[derive(Parser, Debug, Clone)]
#[command(name = "kvdb")]
#[command(about = "Redis-compatible database on FoundationDB")]
pub struct ServerConfig {
    /// Address to bind the Redis-compatible listener.
    #[arg(long, default_value = "127.0.0.1:6379", env = "KVDB_BIND_ADDR")]
    pub bind_addr: SocketAddr,

    /// Maximum number of concurrent client connections.
    #[arg(long, default_value_t = 10_000, env = "KVDB_MAX_CONNECTIONS")]
    pub max_connections: usize,

    /// Path to the FoundationDB cluster file.
    #[arg(long, default_value = "fdb.cluster", env = "KVDB_FDB_CLUSTER_FILE")]
    pub fdb_cluster_file: String,

    /// Log level filter (e.g. "info", "debug", "kvdb=debug,foundationdb=info").
    #[arg(long, default_value = "info", env = "KVDB_LOG_LEVEL")]
    pub log_level: String,

    /// Log format: "text" for human-readable, "json" for structured output.
    #[arg(long, default_value = "text", env = "KVDB_LOG_FORMAT")]
    pub log_format: String,

    /// Enable the Tracy profiler tracing layer. Connect the Tracy GUI
    /// to inspect spans, allocations, and CPU usage in real time.
    #[arg(long, env = "KVDB_TRACY")]
    pub tracy: bool,

    /// OpenTelemetry OTLP endpoint for distributed tracing
    /// (e.g. "http://localhost:4318"). Disabled if not set.
    #[arg(long, env = "KVDB_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,

    /// Address to serve the Prometheus metrics endpoint
    /// (e.g. "0.0.0.0:9090"). Disabled if not set.
    #[arg(long, env = "KVDB_METRICS_ADDR")]
    pub metrics_addr: Option<SocketAddr>,
}

impl ServerConfig {
    /// Build the observability config from the server config.
    pub fn observability(&self) -> ObservabilityConfig {
        let log_format = match self.log_format.as_str() {
            "json" => LogFormat::Json,
            _ => LogFormat::Text,
        };

        ObservabilityConfig {
            log_level: self.log_level.clone(),
            log_format,
            tracy: self.tracy,
            otlp_endpoint: self.otlp_endpoint.clone(),
            metrics_addr: self.metrics_addr,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379".parse().unwrap(),
            max_connections: 10_000,
            fdb_cluster_file: "fdb.cluster".into(),
            log_level: "info".into(),
            log_format: "text".into(),
            tracy: false,
            otlp_endpoint: None,
            metrics_addr: None,
        }
    }
}
