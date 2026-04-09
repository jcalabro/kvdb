# Unified Smoke Test Design

## Problem

The project has two separate programs that exercise the server externally:

- **`examples/smoke.rs`** (469 lines) -- a sequential, single-connection correctness checker that runs 48 deterministic pass/fail checks. The `just smoke` recipe manages server lifecycle in bash.
- **`examples/loadgen.rs`** (547 lines) -- a multi-connection load generator with weighted command distribution and real-time throughput reporting. It requires a pre-started server and does no correctness validation.

These overlap significantly in scope (both connect via redis-rs, both exercise string commands, both are example binaries) but each lacks what the other has. The smoke test can't stress-test; the loadgen can't validate correctness. Managing server lifecycle lives in a bash script rather than the binary itself.

## Design

Merge both programs into a single `examples/smoke.rs` binary that runs two phases sequentially:

1. **Validate** -- all existing deterministic correctness checks (single connection, known expected results, exit on first failure)
2. **Load** -- the existing multi-connection randomized workload with throughput reporting

The binary optionally spawns and manages its own kvdb server process.

### CLI Interface

```
cargo run --example smoke -- [OPTIONS]

Options:
  -a, --addr <ADDR>              Target address [default: 127.0.0.1:6379]
  -s, --seconds <N>              Load phase duration [default: 10]
  -c, --connections <N>          Load phase worker count [default: 32]
      --spawn-server             Start a kvdb server, run both phases, then kill it
  -h, --help                     Print usage and exit
```

When `--spawn-server` is passed:
1. Build and spawn a kvdb child process bound to `127.0.0.1` on a random available port (using `TcpListener::bind("127.0.0.1:0")` to find a free port, then dropping the listener before spawning the server).
2. Override `--addr` with the spawned server's address.
3. Poll the address with a redis PING (up to 5 seconds, 100ms intervals) until the server is ready.
4. Run validate phase, then load phase.
5. Kill the child process and wait for it to exit.
6. Exit with code 0 if both phases succeeded, 1 otherwise.

### Phase 1: Validate

Structurally identical to the current `examples/smoke.rs`. The `SmokeTest` struct, `check_eq`, `check_err`, `pass`, `fail`, and `summary` methods are preserved. All 48 existing checks are kept. The section headers and output format remain the same:

```
=== Phase 1: Validate ===

PING
  PASS  PING returns PONG
  PASS  PING with message

ECHO
  PASS  ECHO returns argument
  ...

All 48 checks passed.
```

If any validation check fails, the program prints the summary and exits with code 1 immediately. The load phase does not run.

### Phase 2: Load

Structurally identical to the current `examples/loadgen.rs`. Preserves:

- The `Stats` struct with `AtomicU64` counters and `Ordering::Relaxed`.
- The `run_one_op` function with the full weighted command distribution (GET 31%, SET 15%, MGET 8%, etc.).
- The `random_value` function with its size distribution (60% small, 25% medium, 10% large, 5% huge).
- The key space partitioning (`lg:s:`, `lg:c:`, `lg:f:`, `lg:a:` prefixes, 5000 keys).
- The worker loop structure: one tokio task per connection, each with its own `StdRng`.
- The reporter task printing per-second throughput to stderr.
- The final summary block.

Output:

```
=== Phase 2: Load (10s, 32 connections) ===

[  1s]      5421 ops/s  (  12 errs/s)  total: 5421
[  2s]      5500 ops/s  (  10 errs/s)  total: 10921
...

--- smoke summary ---
  duration:    10.02s
  connections: 32
  total ops:   54210
  total errors: 120 (0.2%)
  throughput:  5415 ops/s
```

### Server Spawn

When `--spawn-server` is used, the binary manages the server process itself rather than relying on the justfile's bash script. The implementation:

1. Find a free port by binding `TcpListener` to `127.0.0.1:0`, reading the assigned port, then dropping the listener.
2. Spawn the kvdb binary as a child process: `cargo run -- --bind-addr 127.0.0.1:{port} --log-level warn`. Use `std::process::Command` (not tokio's) wrapped in a way that lets us kill it later.
3. Poll readiness by attempting a TCP connection (or redis PING) every 100ms, up to 50 attempts (5 seconds total). If the server doesn't become ready, print an error and exit 1.
4. After both phases complete, send SIGTERM (or kill on Windows) and wait for the child to exit.
5. If the child process exits unexpectedly during a phase, detect it and exit 1 with an error message.

### Code Organization

The merged file is organized into clearly separated sections with comment banners, following the existing style in both files:

```
// ── CLI ──────────────────────────────────────────
//   Args struct, parse_args()

// ── Server Spawn ─────────────────────────────────
//   find_free_port(), spawn_server(), wait_for_ready()

// ── Phase 1: Validate ────────────────────────────
//   SmokeTest struct, run_validate()

// ── Phase 2: Load ────────────────────────────────
//   Stats, run_one_op(), random_value(), worker(), run_load()

// ── Main ─────────────────────────────────────────
//   Orchestrates phases, manages server lifecycle
```

Each phase is wrapped in a top-level async function (`run_validate` and `run_load`) that takes the address and returns success/failure. Main orchestrates the sequence.

### Justfile Changes

The `smoke` recipe becomes a single cargo invocation:

```just
smoke seconds="10" connections="32":
    cargo run --example smoke -- --spawn-server --seconds {{seconds}} --connections {{connections}}
```

The `loadgen` recipe is deleted. Users who want to run load against an already-running server (e.g., for Tracy profiling with a `--release` build or custom server config) run the binary directly:

```bash
cargo run --release --example smoke -- --seconds 30 --connections 64 --addr 127.0.0.1:6379
```

### Files Changed

| File | Action |
|------|--------|
| `examples/smoke.rs` | Rewrite: merge both programs |
| `examples/loadgen.rs` | Delete |
| `justfile` | Update `smoke` recipe, delete `loadgen` recipe |

### Exit Codes

- **0** -- validation passed and load phase completed without fatal errors (per-op errors like WRONGTYPE in the load phase are counted, not fatal).
- **1** -- any validation check failed, server failed to start, connection failed, or unexpected server crash.

## What Is NOT Changing

- The `redis` dev-dependency and async multiplexed connection model.
- The specific validation checks (all 48 are preserved verbatim).
- The loadgen's workload weights, key space, value generation, and stats model.
- The `just ci` pipeline (it does not invoke `just smoke`; smoke is a separate concern).
- Integration tests in `tests/` -- those are unrelated to this change.
