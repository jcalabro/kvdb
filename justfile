set shell := ["bash", "-cu"]

# FDB version — must match across Docker image, native install, and client library
fdb_version := "7.3.63"

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
    cargo clippy --workspace --tests --examples -- -D warnings
    cargo fmt --check

# Start FDB — native on macOS, Docker on Linux
up:
    #!/usr/bin/env bash
    set -euo pipefail

    case "$(uname -s)" in
        Darwin)
            fdb_server=/usr/local/libexec/fdbserver
            fdb_monitor=/usr/local/libexec/fdbmonitor
            if ! [ -x "$fdb_server" ] || ! [ -x "$fdb_monitor" ]; then
                echo "FDB binaries not found. Run 'just install-macos' first."
                exit 1
            fi

            installed=$("$fdb_server" --version 2>&1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)
            if [ "$installed" != "v{{fdb_version}}" ]; then
                echo "fdbserver $installed does not match required v{{fdb_version}}"
                echo "Run 'just install-macos' to install the correct version."
                exit 1
            fi

            if [ -f .fdb/fdbmonitor.pid ] && sudo kill -0 "$(cat .fdb/fdbmonitor.pid)" 2>/dev/null; then
                echo "FDB already running (pid $(cat .fdb/fdbmonitor.pid))"
            else
                mkdir -p .fdb/data/4500 .fdb/logs

                # Generate project-local fdbmonitor config with absolute paths
                cat > .fdb/foundationdb.conf <<CONF
    [general]
    restart-delay = 10
    cluster-file = $(pwd)/fdb.cluster

    [fdbserver]
    command = $fdb_server
    public-address = auto:\$ID
    listen-address = public
    datadir = $(pwd)/.fdb/data/\$ID
    logdir = $(pwd)/.fdb/logs
    logsize = 10MiB
    maxlogssize = 100MiB

    [fdbserver.4500]
    CONF

                # fdbserver requires root on macOS for shared memory (shm_open)
                sudo "$fdb_monitor" \
                    --conffile "$(pwd)/.fdb/foundationdb.conf" \
                    --lockfile "$(pwd)/.fdb/fdbmonitor.pid" \
                    --daemonize
                echo "Started fdbmonitor (pid $(cat .fdb/fdbmonitor.pid))"
            fi
            ;;
        Linux)
            docker compose up -d
            ;;
        *)
            echo "Unsupported OS: $(uname -s)"
            exit 1
            ;;
    esac

    # Wait for FDB to accept connections. On first run, configure the cluster.
    for attempt in $(seq 1 30); do
        if fdbcli -C fdb.cluster --exec "status" --timeout 2 2>/dev/null; then
            echo "FDB cluster is ready"
            exit 0
        fi
        if [ "$attempt" -eq 1 ]; then
            echo "Configuring new FDB cluster..."
        fi
        if fdbcli -C fdb.cluster --exec "configure new single ssd-redwood-1 ; status" --timeout 5 2>/dev/null; then
            echo "FDB cluster is ready"
            exit 0
        fi
        sleep 1
    done

    echo "FDB did not become ready after 30s"
    exit 1

# Stop FDB — kills native process on macOS, docker compose down on Linux
down:
    #!/usr/bin/env bash
    set -euo pipefail

    case "$(uname -s)" in
        Darwin)
            if [ -f .fdb/fdbmonitor.pid ]; then
                pid=$(cat .fdb/fdbmonitor.pid)
                if sudo kill -0 "$pid" 2>/dev/null; then
                    sudo kill "$pid"
                    for _ in $(seq 1 20); do
                        sudo kill -0 "$pid" 2>/dev/null || break
                        sleep 0.5
                    done
                    if sudo kill -0 "$pid" 2>/dev/null; then
                        echo "fdbmonitor did not exit cleanly, sending SIGKILL"
                        sudo kill -9 "$pid" 2>/dev/null || true
                    fi
                    echo "Stopped fdbmonitor (pid $pid)"
                else
                    echo "fdbmonitor (pid $pid) was not running"
                fi
                sudo rm -f .fdb/fdbmonitor.pid
            else
                echo "No FDB pid file found"
            fi
            ;;
        Linux)
            docker compose down --remove-orphans
            ;;
        *)
            echo "Unsupported OS: $(uname -s)"
            exit 1
            ;;
    esac

# Install FoundationDB on macOS (requires sudo for .pkg)
install-macos:
    #!/usr/bin/env bash
    set -euo pipefail

    if [ "$(uname -s)" != "Darwin" ]; then
        echo "This recipe is for macOS only."
        exit 1
    fi

    # Check if correct version is already installed
    if [ -x /usr/local/libexec/fdbserver ]; then
        installed=$(/usr/local/libexec/fdbserver --version 2>&1 | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)
        if [ "$installed" = "v{{fdb_version}}" ]; then
            echo "FoundationDB {{fdb_version}} is already installed."
            exit 0
        fi
        echo "Found FDB $installed, need v{{fdb_version}}. Upgrading..."
    fi

    arch=$(uname -m)
    case "$arch" in
        arm64)  pkg_arch="arm64" ;;
        x86_64) pkg_arch="x86_64" ;;
        *)      echo "Unsupported architecture: $arch"; exit 1 ;;
    esac

    pkg_name="FoundationDB-{{fdb_version}}_${pkg_arch}.pkg"
    url="https://github.com/apple/foundationdb/releases/download/{{fdb_version}}/${pkg_name}"

    echo "Downloading ${pkg_name}..."
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT
    curl -fSL --progress-bar -o "${tmpdir}/${pkg_name}" "$url"

    echo "Verifying SHA-256..."
    expected_sha=$(curl -fsSL "${url}.sha256" | awk '{print $1}')
    actual_sha=$(shasum -a 256 "${tmpdir}/${pkg_name}" | awk '{print $1}')
    if [ "$expected_sha" != "$actual_sha" ]; then
        echo "Checksum mismatch!"
        echo "  expected: $expected_sha"
        echo "  actual:   $actual_sha"
        exit 1
    fi
    echo "Checksum OK"

    echo "Installing FoundationDB {{fdb_version}} (requires sudo)..."
    sudo installer -pkg "${tmpdir}/${pkg_name}" -target /

    # The .pkg installs a launchd daemon that auto-starts fdbserver on port 4689.
    # We manage our own instance on port 4500 via 'just up', so unload it.
    if sudo launchctl list com.foundationdb.fdbmonitor 2>/dev/null; then
        echo "Unloading system FDB daemon (we use 'just up' instead)..."
        sudo launchctl unload /Library/LaunchDaemons/com.foundationdb.fdbmonitor.plist 2>/dev/null || true
    fi

    echo ""
    echo "FoundationDB {{fdb_version}} installed:"
    echo "  fdbserver:  /usr/local/libexec/fdbserver"
    echo "  fdbcli:     /usr/local/bin/fdbcli"
    echo "  libfdb_c:   /usr/local/lib/libfdb_c.dylib"
    echo ""
    echo "Run 'just up' to start the local cluster."

# Run all fuzz targets (default 30s each, or pass duration)
fuzz duration="30":
    cargo +nightly fuzz run resp_parser -- -max_total_time={{duration}}
    cargo +nightly fuzz run resp_multi_frame -- -max_total_time={{duration}}
    cargo +nightly fuzz run resp_roundtrip -- -max_total_time={{duration}}
    cargo +nightly fuzz run resp_encoder -- -max_total_time={{duration}}
    cargo +nightly fuzz run command_dispatch -- -max_total_time={{duration}}

# Run benchmarks
bench:
    cargo bench

# Open FDB CLI shell connected to local cluster
fdbcli:
    fdbcli -C fdb.cluster

# Build release binary
build:
    cargo build --release

# Run smoke tests (validate + load) against a live server (starts one automatically)
smoke seconds="10" connections="32":
    cargo build
    cargo run --example smoke -- --spawn-server --seconds {{seconds}} --connections {{connections}}

# Run the server locally (debug mode)
run *ARGS:
    cargo run {{ARGS}}

# Clean everything
clean:
    cargo clean

# Format code
fmt:
    cargo fmt
