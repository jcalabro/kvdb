//! Error types for kvdb.
//!
//! Uses `thiserror` for structured library errors that carry useful context
//! about what failed and why. Application-level code uses `anyhow` for
//! ad-hoc error propagation with context.

use thiserror::Error;

/// Top-level error type for kvdb operations.
#[derive(Error, Debug)]
pub enum Error {
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("command error: {0}")]
    Command(#[from] CommandError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors from the FDB storage layer.
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("FDB error: {0}")]
    Fdb(#[from] foundationdb::FdbError),

    #[error("FDB binding error: {0}")]
    FdbBinding(String),

    #[error("{0}")]
    Command(CommandError),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("data corruption: {0}")]
    DataCorruption(String),

    #[error("directory error: {0}")]
    Directory(String),

    #[error("key too large ({size} bytes, max {max})")]
    KeyTooLarge { size: usize, max: usize },
}

/// Errors during RESP protocol parsing or encoding.
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("invalid RESP format: unexpected byte {byte:#04x}")]
    InvalidFormat { byte: u8 },

    #[error("unexpected end of input")]
    UnexpectedEof,

    #[error("invalid integer: {0}")]
    InvalidInteger(String),

    #[error("invalid bulk string length: {0}")]
    InvalidLength(i64),

    #[error("line too long ({len} bytes, max {max})")]
    LineTooLong { len: usize, max: usize },

    #[error("nesting depth exceeded (max {max})")]
    NestingTooDeep { max: usize },

    #[error("aggregate element count too large ({count}, max {max})")]
    CountTooLarge { count: usize, max: usize },
}

/// Errors during command execution.
#[derive(Error, Debug)]
pub enum CommandError {
    #[error("ERR unknown command '{name}'")]
    UnknownCommand { name: String },

    #[error("ERR wrong number of arguments for '{name}' command")]
    WrongArity { name: String },

    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR {0}")]
    Generic(String),
}

/// Convenience type alias for Results using our Error type.
pub type Result<T> = std::result::Result<T, Error>;
