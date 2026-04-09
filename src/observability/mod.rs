//! Observability infrastructure: tracing, metrics, and profiling.
//!
//! Composes multiple tracing-subscriber layers into a single global
//! subscriber, and starts a Prometheus metrics HTTP endpoint. All
//! providers are optional and controlled via [`ObservabilityConfig`].
//!
//! # Providers
//!
//! - **fmt** (stdout/stderr): Always active. Text or JSON format.
//! - **Tracy**: Real-time frame profiler. Connect the Tracy GUI to
//!   inspect spans, allocations, and CPU usage live.
//! - **OpenTelemetry**: Distributed tracing via OTLP export (gRPC or HTTP).
//! - **Prometheus**: Metrics scraped via HTTP `/metrics` endpoint.
//!
//! # Usage
//!
//! ```rust,ignore
//! let _guard = observability::init(&config.observability)?;
//! // guard must live for the program's lifetime — dropping it
//! // flushes OTEL spans and shuts down the metrics server.
//! ```

pub mod metrics;
pub mod tracing_init;

use std::net::SocketAddr;

use tracing::info;

/// Configuration for the observability stack.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// tracing filter directive (e.g. "info", "kvdb=debug,tower=warn").
    pub log_level: String,

    /// Log output format.
    pub log_format: LogFormat,

    /// Enable the Tracy profiler tracing layer.
    /// Connect the Tracy GUI to inspect spans in real time.
    pub tracy: bool,

    /// OpenTelemetry OTLP endpoint (e.g. "http://localhost:4317").
    /// `None` disables OTEL export.
    pub otlp_endpoint: Option<String>,

    /// Address to serve the Prometheus `/metrics` endpoint.
    /// `None` disables the metrics HTTP server.
    pub metrics_addr: Option<SocketAddr>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            log_format: LogFormat::Text,
            tracy: false,
            otlp_endpoint: None,
            metrics_addr: None,
        }
    }
}

/// Log output format for the fmt layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable colored output (default for development).
    Text,
    /// Structured JSON (default for production / log aggregation).
    Json,
}

/// Guard that keeps observability resources alive.
///
/// Dropping this guard will:
/// - Flush and shut down the OpenTelemetry tracer provider (if active)
/// - The metrics HTTP server task is cancelled via tokio's task drop
///
/// Hold this in `main()` for the lifetime of the program.
pub struct ObservabilityGuard {
    _otel_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
    _metrics_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ObservabilityGuard {
    fn drop(&mut self) {
        if let Some(provider) = self._otel_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("error shutting down OTEL tracer provider: {e}");
        }
    }
}

/// Initialize the full observability stack.
///
/// Sets the global tracing subscriber (composing fmt, Tracy, and OTEL
/// layers), registers Prometheus metrics, and starts the metrics HTTP
/// server if configured.
///
/// Returns an [`ObservabilityGuard`] that must be held alive for the
/// duration of the program.
pub fn init(config: &ObservabilityConfig) -> anyhow::Result<ObservabilityGuard> {
    // Build and install the composed tracing subscriber.
    let otel_provider = tracing_init::init_tracing(config)?;

    // Register Prometheus process metrics (CPU, memory, fds on Linux).
    metrics::register_default_metrics();

    // Start the metrics HTTP endpoint if configured.
    let metrics_handle = if let Some(addr) = config.metrics_addr {
        info!(%addr, "starting prometheus metrics endpoint");
        Some(tokio::spawn(metrics::serve_metrics(addr)))
    } else {
        None
    };

    Ok(ObservabilityGuard {
        _otel_provider: otel_provider,
        _metrics_handle: metrics_handle,
    })
}
