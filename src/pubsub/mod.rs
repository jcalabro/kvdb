//! FDB-backed pub/sub for cross-instance message delivery.
//!
//! Uses a hybrid watch + versionstamp-ordered queue approach:
//!
//! - **PUBLISH**: writes message to `pubsub/msg/<channel, versionstamp>`
//!   (conflict-free via atomic op) and bumps `pubsub/notify/<channel>`
//!   (triggers FDB watches on subscribers).
//! - **SUBSCRIBE**: per-channel FDB watch loop detects new messages,
//!   reads from the versionstamp queue, and pushes to the connection.
//! - **PSUBSCRIBE**: watches a global `__all__` notification key,
//!   then scans matching channels for new messages.
//!
//! Pub/sub is global (not per-namespace), matching Redis behavior:
//! a PUBLISH in SELECT 5 reaches subscribers in SELECT 0.

pub mod cleanup;
pub mod pattern;
pub mod subscriber;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use foundationdb::directory::{Directory, DirectoryLayer, DirectorySubspace};
use foundationdb::options::MutationType;
use foundationdb::tuple::{Subspace, Versionstamp};
use tokio::sync::mpsc;
use tracing::debug;

use crate::error::StorageError;
use crate::observability::metrics;
use crate::storage::Database;

/// Maximum pub/sub message payload size in bytes.
///
/// FDB has a 100,000-byte hard value limit. Each message is stored as
/// `[8 bytes instance_id | payload]`, so the usable payload is
/// 100,000 − 8 = 99,992 bytes. We enforce this on PUBLISH to give a
/// clear error rather than an opaque FDB failure.
pub const MAX_MESSAGE_SIZE: usize = 100_000 - INSTANCE_ID_PREFIX_SIZE;

/// Size of the instance ID prefix prepended to every FDB message value.
///
/// Each message stored in `pubsub/msg/<channel, versionstamp>` starts
/// with this many bytes identifying which kvdb instance published it.
/// Watcher tasks use this to skip messages they already delivered locally.
pub const INSTANCE_ID_PREFIX_SIZE: usize = 8;

/// Sentinel channel name used as the global notification key for
/// pattern subscribers. Every PUBLISH bumps this in addition to
/// the per-channel notification key.
///
/// `pub(super)` so that `subscriber.rs` can import it directly,
/// preventing the two sides from silently diverging.
pub(super) const GLOBAL_NOTIFY_CHANNEL: &[u8] = b"__all__";

/// FDB directory handles for the pub/sub keyspace.
///
/// Lives under `<root>/pubsub/` — global, not per-namespace.
#[derive(Clone)]
pub struct PubSubDirectories {
    /// `<root>/pubsub/msg/<channel, versionstamp>` — message log.
    pub msg: DirectorySubspace,
    /// `<root>/pubsub/notify/<channel>` — per-channel notification counter.
    pub notify: DirectorySubspace,
}

impl PubSubDirectories {
    /// Open (or create) the pub/sub directories under `root_prefix`.
    /// Maximum number of retries for directory creation (matches
    /// `Directories::DIR_OPEN_MAX_RETRIES`).
    const DIR_OPEN_MAX_RETRIES: u32 = 10;

    /// Open (or create) the pub/sub FDB directory subspaces with
    /// automatic retry on transient FDB conflicts.
    ///
    /// Under heavy parallelism (e.g. nextest running hundreds of tests)
    /// the FDB directory layer transaction can conflict. Without retry
    /// the server crashes on startup — a silent, non-deterministic
    /// failure that looks like a flaky test.
    pub async fn open(db: &Database, root_prefix: &str) -> Result<Self, StorageError> {
        let mut last_err = None;

        for attempt in 0..Self::DIR_OPEN_MAX_RETRIES {
            match Self::try_open(db, root_prefix).await {
                Ok(dirs) => return Ok(dirs),
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "pubsub directory open conflict, retrying");
                    last_err = Some(e);
                    // Jittered backoff matching Directories::open pattern.
                    let base_ms = 30
                        + (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .subsec_nanos()
                            % 50) as u64;
                    let delay_ms = base_ms + (attempt as u64) * 20;
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }

        Err(last_err.unwrap())
    }

    /// Single attempt to open pub/sub directories.
    async fn try_open(db: &Database, root_prefix: &str) -> Result<Self, StorageError> {
        let trx = db.inner().create_trx().map_err(StorageError::Fdb)?;
        let dir_layer = DirectoryLayer::default();

        let msg_path = vec![root_prefix.to_string(), "pubsub".to_string(), "msg".to_string()];
        let notify_path = vec![root_prefix.to_string(), "pubsub".to_string(), "notify".to_string()];

        let msg = match dir_layer
            .create_or_open(&trx, &msg_path, None, None)
            .await
            .map_err(|e| StorageError::Directory(format!("{e:?}")))?
        {
            foundationdb::directory::DirectoryOutput::DirectorySubspace(ds) => ds,
            _ => {
                return Err(StorageError::Directory(
                    "expected DirectorySubspace for pubsub/msg".to_string(),
                ));
            }
        };

        let notify = match dir_layer
            .create_or_open(&trx, &notify_path, None, None)
            .await
            .map_err(|e| StorageError::Directory(format!("{e:?}")))?
        {
            foundationdb::directory::DirectoryOutput::DirectorySubspace(ds) => ds,
            _ => {
                return Err(StorageError::Directory(
                    "expected DirectorySubspace for pubsub/notify".to_string(),
                ));
            }
        };

        trx.commit().await.map_err(|e| StorageError::Fdb(e.into()))?;

        Ok(Self { msg, notify })
    }
}

/// A message delivered to a subscriber's connection.
#[derive(Debug, Clone)]
pub enum PubSubMessage {
    /// Channel subscription message: `["message", channel, data]`
    Message { channel: Bytes, data: Bytes },
    /// Pattern-matched message: `["pmessage", pattern, channel, data]`
    PMessage {
        pattern: Bytes,
        channel: Bytes,
        data: Bytes,
    },
}

/// A direct channel subscriber entry in the in-process registry.
struct ChannelSub {
    conn_id: u64,
    tx: mpsc::UnboundedSender<PubSubMessage>,
}

/// A pattern subscriber entry in the in-process registry.
struct PatternSub {
    conn_id: u64,
    tx: mpsc::UnboundedSender<PubSubMessage>,
    matcher: pattern::GlobMatcher,
}

/// Shared pub/sub state for one kvdb server instance.
///
/// Manages the FDB-backed message queue, in-process subscription
/// registry, and subscriber notification. One per server, shared
/// across all connections via `Arc`.
pub struct PubSubManager {
    /// FDB directory handles for the pub/sub keyspace.
    dirs: PubSubDirectories,
    /// FDB database handle.
    db: Database,
    /// In-process registry: channel name -> subscribers on this instance.
    channel_subs: DashMap<Bytes, Vec<ChannelSub>>,
    /// In-process registry: pattern string -> subscribers on this instance.
    pattern_subs: DashMap<Bytes, Vec<PatternSub>>,
    /// Monotonic connection ID generator.
    next_conn_id: AtomicU64,
    /// Unique identifier for this server instance.
    ///
    /// Prepended to every message written to FDB. Watcher tasks use
    /// this to skip messages published by this instance (which were
    /// already delivered locally via the mpsc channel).
    pub instance_id: u64,
}

impl PubSubManager {
    /// Create a new pub/sub manager.
    pub fn new(db: Database, dirs: PubSubDirectories) -> Self {
        // Generate a random instance ID using the current time and a counter.
        // We don't need cryptographic randomness here — just enough uniqueness
        // to distinguish concurrent kvdb instances on the same FDB cluster.
        use std::time::{SystemTime, UNIX_EPOCH};
        static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let counter = INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let instance_id = ts ^ (counter << 32);

        Self {
            dirs,
            db,
            channel_subs: DashMap::new(),
            pattern_subs: DashMap::new(),
            next_conn_id: AtomicU64::new(1),
            instance_id,
        }
    }

    /// Allocate a unique connection ID for subscription tracking.
    pub fn next_connection_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the FDB database handle.
    pub fn db(&self) -> &Database {
        &self.db
    }

    /// Get the pub/sub directory handles.
    pub fn dirs(&self) -> &PubSubDirectories {
        &self.dirs
    }

    /// Publish a message to a channel.
    ///
    /// Writes the message to the FDB versionstamp queue and bumps
    /// the per-channel + global notification keys. Returns the number
    /// of local subscribers that received the message (matching Redis
    /// standalone PUBLISH semantics).
    pub async fn publish(&self, channel: &[u8], message: &[u8]) -> Result<i64, StorageError> {
        // Write to FDB: versionstamp-keyed message + notification bump.
        let trx = self.db.inner().create_trx().map_err(StorageError::Fdb)?;

        // Versionstamped key: msg/<channel, VERSIONSTAMP> = [instance_id (8 bytes) | message]
        // Uses SetVersionstampedKey — no read conflicts, conflict-free.
        //
        // The value is prefixed with this instance's 8-byte ID so that
        // watcher tasks on this same instance can skip messages they
        // already delivered locally (avoiding double-delivery).
        //
        // DirectorySubspace doesn't expose pack_with_versionstamp, so we
        // construct a Subspace from its prefix bytes.
        let msg_subspace = Subspace::from_bytes(self.dirs.msg.bytes());
        let msg_key = msg_subspace.pack_with_versionstamp(&(channel, &Versionstamp::incomplete(0)));
        let mut fdb_value = Vec::with_capacity(INSTANCE_ID_PREFIX_SIZE + message.len());
        fdb_value.extend_from_slice(&self.instance_id.to_le_bytes());
        fdb_value.extend_from_slice(message);
        trx.atomic_op(&msg_key, &fdb_value, MutationType::SetVersionstampedKey);

        // Bump per-channel notification counter (atomic ADD, conflict-free).
        let notify_key = self.dirs.notify.pack(&(channel,));
        trx.atomic_op(&notify_key, &1i64.to_le_bytes(), MutationType::Add);

        // Bump global notification counter for pattern subscribers.
        let global_key = self.dirs.notify.pack(&(GLOBAL_NOTIFY_CHANNEL,));
        trx.atomic_op(&global_key, &1i64.to_le_bytes(), MutationType::Add);

        trx.commit().await.map_err(|e| StorageError::Fdb(e.into()))?;

        // Track publish metrics. The counter is intentionally unlabeled
        // (channel name would be unbounded cardinality — DoS vector).
        metrics::PUBSUB_MESSAGES_PUBLISHED_TOTAL.inc();

        // Count and deliver to local subscribers.
        let mut count = 0i64;

        // Direct channel subscribers on this instance.
        if let Some(subs) = self.channel_subs.get(channel) {
            for sub in subs.iter() {
                if sub
                    .tx
                    .send(PubSubMessage::Message {
                        channel: Bytes::copy_from_slice(channel),
                        data: Bytes::copy_from_slice(message),
                    })
                    .is_ok()
                {
                    count += 1;
                    metrics::PUBSUB_MESSAGES_DELIVERED_TOTAL.inc();
                }
            }
        }

        // Pattern subscribers whose pattern matches this channel.
        for entry in self.pattern_subs.iter() {
            for psub in entry.value().iter() {
                if psub.matcher.matches(channel)
                    && psub
                        .tx
                        .send(PubSubMessage::PMessage {
                            pattern: Bytes::copy_from_slice(entry.key()),
                            channel: Bytes::copy_from_slice(channel),
                            data: Bytes::copy_from_slice(message),
                        })
                        .is_ok()
                {
                    count += 1;
                    metrics::PUBSUB_MESSAGES_DELIVERED_TOTAL.inc();
                }
            }
        }

        Ok(count)
    }

    /// Register a direct channel subscriber. Returns the sender for
    /// the subscriber's message channel.
    ///
    /// The caller provides the `tx` side of an unbounded mpsc channel;
    /// messages will be sent through it when published to this channel.
    pub fn subscribe_channel(&self, channel: Bytes, conn_id: u64, tx: mpsc::UnboundedSender<PubSubMessage>) {
        self.channel_subs
            .entry(channel.clone())
            .or_default()
            .push(ChannelSub { conn_id, tx });

        metrics::PUBSUB_ACTIVE_SUBSCRIPTIONS.inc();
        debug!(conn_id, channel = %String::from_utf8_lossy(&channel), "subscribed to channel");
    }

    /// Unsubscribe a connection from a direct channel.
    ///
    /// Returns `true` if the connection was actually subscribed.
    pub fn unsubscribe_channel(&self, channel: &[u8], conn_id: u64) -> bool {
        let mut found = false;
        if let Some(mut subs) = self.channel_subs.get_mut(channel) {
            let before = subs.len();
            subs.retain(|s| s.conn_id != conn_id);
            found = subs.len() < before;

            // Clean up empty entries to avoid unbounded DashMap growth.
            // Use remove_if (atomic check-then-remove) to close the TOCTOU
            // window between drop(subs) and remove: if another thread
            // subscribes after we drop the guard, remove_if sees a non-empty
            // Vec and correctly skips the remove.
            if subs.is_empty() {
                drop(subs);
                self.channel_subs.remove_if(channel, |_, v| v.is_empty());
            }
        }
        if found {
            metrics::PUBSUB_ACTIVE_SUBSCRIPTIONS.dec();
            debug!(conn_id, channel = %String::from_utf8_lossy(channel), "unsubscribed from channel");
        }
        found
    }

    /// Register a pattern subscriber.
    pub fn subscribe_pattern(&self, pattern_bytes: Bytes, conn_id: u64, tx: mpsc::UnboundedSender<PubSubMessage>) {
        let matcher = pattern::GlobMatcher::new(&pattern_bytes);
        self.pattern_subs
            .entry(pattern_bytes.clone())
            .or_default()
            .push(PatternSub { conn_id, tx, matcher });

        metrics::PUBSUB_ACTIVE_PATTERNS.inc();
        debug!(conn_id, pattern = %String::from_utf8_lossy(&pattern_bytes), "subscribed to pattern");
    }

    /// Unsubscribe a connection from a pattern.
    ///
    /// Returns `true` if the connection was actually subscribed.
    pub fn unsubscribe_pattern(&self, pattern: &[u8], conn_id: u64) -> bool {
        let mut found = false;
        if let Some(mut subs) = self.pattern_subs.get_mut(pattern) {
            let before = subs.len();
            subs.retain(|s| s.conn_id != conn_id);
            found = subs.len() < before;

            if subs.is_empty() {
                drop(subs);
                self.pattern_subs.remove_if(pattern, |_, v| v.is_empty());
            }
        }
        if found {
            metrics::PUBSUB_ACTIVE_PATTERNS.dec();
            debug!(conn_id, pattern = %String::from_utf8_lossy(pattern), "unsubscribed from pattern");
        }
        found
    }

    /// Remove all subscriptions for a connection (channel + pattern).
    ///
    /// Called on connection close to clean up. Returns the lists of
    /// channels and patterns that were unsubscribed.
    pub fn unsubscribe_all(&self, conn_id: u64) -> (Vec<Bytes>, Vec<Bytes>) {
        let mut channels = Vec::new();
        let mut patterns = Vec::new();

        // Collect keys first to avoid holding DashMap shard locks.
        let channel_keys: Vec<Bytes> = self.channel_subs.iter().map(|entry| entry.key().clone()).collect();
        for key in channel_keys {
            if self.unsubscribe_channel(&key, conn_id) {
                channels.push(key);
            }
        }

        let pattern_keys: Vec<Bytes> = self.pattern_subs.iter().map(|entry| entry.key().clone()).collect();
        for key in pattern_keys {
            if self.unsubscribe_pattern(&key, conn_id) {
                patterns.push(key);
            }
        }

        (channels, patterns)
    }

    /// List active channels, optionally filtered by a glob pattern.
    ///
    /// Returns channels that have at least one subscriber on this
    /// instance. (Redis PUBSUB CHANNELS is instance-local.)
    pub fn active_channels(&self, pattern: Option<&[u8]>) -> Vec<Bytes> {
        let matcher = pattern.map(pattern::GlobMatcher::new);
        self.channel_subs
            .iter()
            .filter(|entry| !entry.value().is_empty())
            .filter(|entry| match &matcher {
                Some(m) => m.matches(entry.key()),
                None => true,
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get subscriber counts for specific channels.
    ///
    /// Returns (channel, count) pairs. Count is instance-local.
    pub fn numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .channel_subs
                    .get(ch.as_ref())
                    .map(|subs| subs.len() as i64)
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Get the total number of pattern subscriptions on this instance.
    pub fn numpat(&self) -> i64 {
        self.pattern_subs.iter().map(|entry| entry.value().len() as i64).sum()
    }
}

/// Arc-wrapped pub/sub manager for sharing across connections.
pub type SharedPubSubManager = Arc<PubSubManager>;
