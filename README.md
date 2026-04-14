# kvdb

A Redis-compatible database built on [FoundationDB](https://apple.github.io/foundationdb/) in Rust. Speaks RESP3 (and RESP2), maps Redis data structures onto FDB's ordered key-value store. You get strict serializability for free.

## Prerequisites

- Rust (stable)
- [just](https://github.com/casey/just)
- [cargo-nextest](https://nexte.st/)
- FoundationDB 7.3.63

## Setup

On macOS:

```
just install-macos
just up
```

On Linux, `just up` uses Docker Compose instead.

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
