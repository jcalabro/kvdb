//! Tracing subscriber composition.
//!
//! Builds a layered tracing subscriber from optional providers:
//!
//! ```text
//!  Registry
//!    ├── EnvFilter          (always: controls log levels)
//!    ├── fmt layer          (always: stdout/stderr, text or JSON)
//!    ├── TracyLayer         (optional: real-time profiler)
//!    └── OpenTelemetryLayer (optional: distributed tracing via OTLP)
//! ```
//!
//! All optional layers are collected into a `Vec<Box<dyn Layer<Registry>>>`
//! which implements `Layer<Registry>` itself, then applied in a single
//! `.with()` call. This avoids the deeply-nested `Layered<..., Layered<...>>`
//! types that break trait bounds when different layers are parameterized
//! on `Registry` specifically.

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

use super::{LogFormat, ObservabilityConfig};

/// Initialize the global tracing subscriber.
///
/// Returns the OTEL tracer provider (if configured) so the caller can
/// hold it alive and flush on shutdown.
pub fn init_tracing(
    config: &ObservabilityConfig,
) -> anyhow::Result<Option<opentelemetry_sdk::trace::SdkTracerProvider>> {
    // Collect ALL layers (including EnvFilter) into a single Vec<Box<dyn Layer<Registry>>>.
    // This is applied in one .with() call against bare Registry, avoiding the
    // nested Layered<..., Layered<...>> types that break trait bounds when
    // different layers are parameterized on Registry specifically.
    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();

    // EnvFilter controls which spans/events are recorded.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log_level));
    layers.push(Box::new(env_filter));

    // Always-on: structured log output to stdout/stderr.
    match config.log_format {
        LogFormat::Text => {
            layers.push(Box::new(
                tracing_subscriber::fmt::layer()
                    .with_target(true)
                    .with_thread_ids(false)
                    .with_ansi(true),
            ));
        }
        LogFormat::Json => {
            layers.push(Box::new(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(true)
                    .with_current_span(true)
                    .with_span_list(true),
            ));
        }
    }

    // Optional: Tracy profiler layer (requires `--features tracy` at compile time).
    #[cfg(feature = "tracy")]
    if config.tracy {
        layers.push(Box::new(tracing_tracy::TracyLayer::default()));
    }
    #[cfg(not(feature = "tracy"))]
    if config.tracy {
        tracing::warn!("--tracy requested but binary was compiled without the `tracy` feature; ignoring");
    }

    // Optional: OpenTelemetry OTLP export.
    let otel_provider = if let Some(ref endpoint) = config.otlp_endpoint {
        let (layer, provider) = build_otel_layer(endpoint)?;
        layers.push(Box::new(layer));
        Some(provider)
    } else {
        None
    };

    // Compose: Registry + all collected layers in a single .with() call.
    tracing_subscriber::registry().with(layers).init();

    #[cfg(feature = "tracy")]
    if config.tracy {
        tracing::info!("Tracy profiler layer enabled — connect the Tracy GUI to inspect spans");
    }

    Ok(otel_provider)
}

/// Build the OpenTelemetry tracing layer and its provider.
///
/// The provider must be kept alive (and eventually shut down) by the
/// caller — dropping it flushes pending spans.
fn build_otel_layer(
    endpoint: &str,
) -> anyhow::Result<(
    tracing_opentelemetry::OpenTelemetryLayer<Registry, opentelemetry_sdk::trace::Tracer>,
    opentelemetry_sdk::trace::SdkTracerProvider,
)> {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(opentelemetry_sdk::Resource::builder().with_service_name("kvdb").build())
        .build();

    let tracer = provider.tracer("kvdb");
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing::info!(%endpoint, "OpenTelemetry OTLP export enabled");

    Ok((layer, provider))
}
