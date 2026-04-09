//! TCP server and connection handling.
//!
//! The listener accepts connections, enforces concurrency limits via a
//! semaphore, and spawns a tokio task per connection. Each connection
//! maintains its own read/write buffers, protocol version, and selected
//! database namespace.

pub mod connection;
pub mod listener;
