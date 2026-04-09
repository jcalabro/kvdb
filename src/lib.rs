//! kvdb — A Redis-compatible database on FoundationDB.
//!
//! This crate implements a production-grade Redis-compatible server that
//! uses FoundationDB as its storage backend. It provides:
//!
//! - Full RESP3 protocol with RESP2 backward compatibility
//! - Core Redis data structures: strings, hashes, sets, sorted sets, lists
//! - Transparent value chunking for values exceeding FDB's 100KB limit
//! - Hybrid lazy + active TTL expiration
//! - Strict serializability via FDB (stronger than Redis)
//!
//! # Architecture
//!
//! ```text
//! Client ←RESP→ Server → Commands → Storage → FoundationDB
//!                  ↑                    ↑
//!              Protocol              ObjectMeta + Chunking
//! ```

pub mod commands;
pub mod config;
pub mod error;
pub mod observability;
pub mod protocol;
pub mod server;
pub mod storage;
pub mod ttl;
