set shell := ["bash", "-cu"]

# Default: lint + fast tests. Your inner loop — must stay under 2 seconds.
default: lint test

# Unit + integration tests against real FDB. Focused cases, not exhaustive.
# Requires `just up` first.
test:
    cargo nextest run -E 'not binary(accept_protocol) and not test(accept)'

# Exhaustive acceptance tests. Randomized inputs, property-based fuzzing,
# large corpus, chaos sequences against live server + FDB.
accept:
    cargo nextest run --no-tests=pass -E 'binary(accept_protocol) or test(accept)'

# Full CI pipeline — lint, all tests, doc build. Run before pushing.
ci: lint test accept
    cargo doc --no-deps

# Run clippy and format check
lint:
    cargo clippy --workspace --tests -- -D warnings
    cargo fmt --check

# Stand up local FDB cluster via docker compose, configure if first run
up:
    #!/usr/bin/env bash
    docker compose up -d

    if ! fdbcli -C fdb.cluster --exec status --timeout 1 2>/dev/null ; then
        echo "Configuring new FDB cluster..."
        if ! fdbcli -C fdb.cluster --exec "configure new single ssd-redwood-1 ; status" --timeout 10 ; then
            echo "Unable to configure new FDB cluster."
            exit 1
        fi
    fi

    echo "FDB cluster is ready"

# Tear down local FDB cluster
down:
    docker compose down --remove-orphans

# Run all fuzz targets (default 30s each, or pass duration)
fuzz duration="30":
    cargo +nightly fuzz run resp_parser -- -max_total_time={{duration}}
    cargo +nightly fuzz run resp_multi_frame -- -max_total_time={{duration}}
    cargo +nightly fuzz run resp_roundtrip -- -max_total_time={{duration}}
    cargo +nightly fuzz run resp_encoder -- -max_total_time={{duration}}

# Run benchmarks
bench:
    cargo bench

# Open FDB CLI shell connected to local cluster
fdbcli:
    fdbcli -C fdb.cluster

# Build release binary
build:
    cargo build --release

# Run smoke tests against a live server (starts one automatically)
smoke:
    #!/usr/bin/env bash
    set -e
    cargo build --example smoke 2>&1

    cargo run -- --bind-addr 127.0.0.1:6399 --log-level warn &
    SERVER_PID=$!

    # Wait for server to accept connections
    for i in $(seq 1 50); do
        if redis-cli -p 6399 PING >/dev/null 2>&1; then break; fi
        sleep 0.1
    done

    set +e
    cargo run --example smoke -- 127.0.0.1:6399
    STATUS=$?
    set -e

    kill $SERVER_PID 2>/dev/null
    wait $SERVER_PID 2>/dev/null || true
    exit $STATUS

# Run the server locally (debug mode)
run *ARGS:
    cargo run -- {{ARGS}}

# Clean everything
clean:
    cargo clean

# Format code
fmt:
    cargo fmt
