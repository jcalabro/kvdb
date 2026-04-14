//! Hash command handlers: HSET, HGET, HDEL, HEXISTS, HLEN, HKEYS, HVALS,
//! HGETALL, HMGET, HMSET, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD.
//!
//! Hash fields are stored as individual FDB key-value pairs:
//!   hash/<redis_key, field_name> -> field_value
//!
//! ObjectMeta tracks the key type (Hash) and cardinality (field count).
//! All reads/writes happen within a single FDB transaction per command.

use bytes::Bytes;
use foundationdb::RangeOption;

use crate::error::CommandError;
use crate::protocol::types::RespValue;
use crate::server::connection::ConnectionState;
use crate::storage::meta::{KeyType, ObjectMeta};
use crate::storage::{helpers, run_transact};
