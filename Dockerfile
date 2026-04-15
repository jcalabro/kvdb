# syntax=docker/dockerfile:1

# Multi-stage build: compile against the FDB client in a full Rust image,
# then copy only the stripped binary + libfdb_c into a slim runtime image.

# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
FROM rust:1.94-bookworm AS builder

ARG TARGETARCH
ARG FDB_VERSION=7.3.63

# Install:
# - libclang-dev: bindgen (used by foundationdb-sys) needs libclang
# - wget: fetch the FDB client deb
# - FDB client library: foundationdb-sys links against libfdb_c
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) fdb_arch="amd64" ;; \
        arm64) fdb_arch="aarch64" ;; \
        *)     echo "unsupported arch: ${TARGETARCH}"; exit 1 ;; \
    esac; \
    apt-get update \
    && apt-get install -y --no-install-recommends wget libclang-dev \
    && wget -q "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${fdb_arch}.deb" \
    && dpkg -i foundationdb-clients_*.deb \
    && rm -f foundationdb-clients_*.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# --- Dependency cache layer ---
# Copy only the manifests so a source-only change does not invalidate the
# (expensive) dependency build.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src benches \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && echo 'fn main() {}' > benches/commands.rs \
    && cargo build --release \
    && rm -rf src \
       target/release/deps/kvdb* \
       target/release/deps/libkvdb* \
       target/release/kvdb* \
       target/release/.fingerprint/kvdb* \
       target/release/incremental/kvdb*

# --- Build the real binary ---
COPY src/ src/
RUN cargo build --release \
    && strip target/release/kvdb

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim

ARG TARGETARCH
ARG FDB_VERSION=7.3.63

# Install only the FDB client runtime library (libfdb_c.so) and
# ca-certificates (needed if OTLP export targets an HTTPS endpoint).
RUN set -eux; \
    case "${TARGETARCH}" in \
        amd64) fdb_arch="amd64" ;; \
        arm64) fdb_arch="aarch64" ;; \
        *)     echo "unsupported arch: ${TARGETARCH}"; exit 1 ;; \
    esac; \
    apt-get update \
    && apt-get install -y --no-install-recommends wget ca-certificates \
    && wget -q "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_${fdb_arch}.deb" \
    && dpkg -i foundationdb-clients_*.deb \
    && rm -f foundationdb-clients_*.deb \
    && apt-get purge -y --auto-remove wget \
    && rm -rf /var/lib/apt/lists/*

# Run as non-root.
RUN useradd --system --no-create-home kvdb
USER kvdb

COPY --from=builder /build/target/release/kvdb /usr/local/bin/kvdb

# Inside a container the server must listen on all interfaces, not just
# loopback. Override the compiled-in default of 127.0.0.1:6379.
ENV KVDB_BIND_ADDR=0.0.0.0:6379

EXPOSE 6379

ENTRYPOINT ["kvdb"]
