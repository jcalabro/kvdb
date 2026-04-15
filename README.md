# kvdb

A Redis-compatible database built on [FoundationDB](https://apple.github.io/foundationdb/) in Rust. Speaks RESP3 (and RESP2), maps Redis data structures onto FDB's ordered key-value store. You get strict serializability for free.

## Prerequisites

- Rust (stable)
- [just](https://github.com/casey/just)
- [cargo-nextest](https://nexte.st/)
- FoundationDB 7.3.63

## Setup

FoundationDB runs in Docker on both Linux and macOS.

On macOS, first install the FDB client library (the `rust-foundationdb` crate links against `libfdb_c.dylib` at build time). Docker Desktop's Rosetta x86/64 emulation must be enabled (Settings → General → "Use Rosetta for x86/amd64 emulation on Apple Silicon").

```
just install-macos   # macOS only — installs libfdb_c + fdbcli
just up
```

On Linux, install the FDB client package (`foundationdb-clients_7.3.63-1_amd64.deb` from the FoundationDB releases page), then `just up`.

## Dev loop

```
just          # lint + tests (~2s)
just smoke    # validate + load test against a live server
just accept   # exhaustive acceptance tests (slow, randomized)
just ci       # everything: lint, test, accept, doc build
```

## Run the server

```
just run
```

Binds to `127.0.0.1:6379` by default. Talk to it with `redis-cli` or any Redis client.

## Tear down

```
just down
```
