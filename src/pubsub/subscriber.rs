//! Per-subscription FDB watch loops for cross-instance message delivery.
//!
//! Each SUBSCRIBE creates a watcher task that:
//! 1. Sets an FDB watch on the channel's notification key
//! 2. When the watch fires, reads new messages from the versionstamp queue
//! 3. Sends messages through the mpsc channel to the connection handler
//! 4. Repeats until cancelled
//!
//! PSUBSCRIBE creates a similar task that watches the global `__all__`
//! notification key and reads new messages from the entire msg/ subspace,
//! filtered by pattern.
//!
//! Both watchers use a **key cursor** to track progress: they remember the
//! last FDB key they read and fetch only keys after that point.  This is
//! correct and efficient because messages are keyed by versionstamp (which
//! is monotonically increasing with commit order), so "keys after cursor"
//! means "messages published after the cursor".
//!
//! On the first watch wakeup the cursor is initialised to the current tail
//! of the queue so that historical messages are never replayed — matching
//! Redis pub/sub semantics (you only receive messages published after you
//! subscribe).

use bytes::Bytes;
use foundationdb::RangeOption;
use foundationdb::tuple::Versionstamp;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::{GLOBAL_NOTIFY_CHANNEL, INSTANCE_ID_PREFIX_SIZE, PubSubDirectories, PubSubMessage};
use crate::storage::Database;

/// Maximum number of messages to read per watch wakeup.
const READ_BATCH_SIZE: usize = 100;

/// Run a watcher loop for a direct channel subscription.
///
/// Spawned as a tokio task per SUBSCRIBE channel. Watches the
/// per-channel FDB notification key for changes, reads new messages
/// from the versionstamp queue, and forwards them to the connection.
///
/// Messages with `local_instance_id` in their prefix are skipped —
/// they were already delivered locally by `PubSubManager::publish()`.
pub async fn channel_watcher(
    db: Database,
    dirs: PubSubDirectories,
    channel: Bytes,
    tx: mpsc::UnboundedSender<PubSubMessage>,
    cancel: CancellationToken,
    local_instance_id: u64,
) {
    // Cursor: the last FDB key we read. Everything after this is "new."
    // Starts at None, meaning we skip historical messages (Redis pub/sub
    // has no replay — you only get messages published after subscribing).
    //
    // On the first watch wakeup, we read the latest batch and set the cursor.
    let mut cursor: Option<Vec<u8>> = None;
    let mut initialized = false;

    loop {
        // 1. Set up FDB watch on the channel notification key.
        let notify_key = dirs.notify.pack(&(channel.as_ref(),));

        let watch_result = match setup_watch(&db, &notify_key).await {
            Ok(watch_fut) => watch_fut,
            Err(e) => {
                warn!(error = %e, "failed to set up channel watch, retrying");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }
        };

        // 2. Wait for watch to fire or cancellation.
        tokio::select! {
            _ = watch_result => {}
            _ = cancel.cancelled() => {
                debug!(channel = %String::from_utf8_lossy(&channel), "channel watcher cancelled");
                return;
            }
        }

        // 3. On first wakeup, initialize cursor to the end of the queue
        //    so we don't replay old messages. On subsequent wakeups,
        //    read from cursor forward.
        if !initialized {
            if let Some(last_key) = find_queue_tail(&db, &dirs, &channel).await {
                cursor = Some(last_key);
            }
            initialized = true;
            // Don't deliver messages from before subscription.
            // Wait for the next watch wakeup which will carry new messages.
            continue;
        }

        // 4. Read new messages from the versionstamp queue.
        match read_new_messages(&db, &dirs, &channel, &cursor).await {
            Ok(messages) => {
                for (key, fdb_value) in messages {
                    cursor = Some(key);

                    // Skip messages published by this instance — they were
                    // already delivered locally by PubSubManager::publish().
                    if fdb_value.len() >= INSTANCE_ID_PREFIX_SIZE {
                        let origin = u64::from_le_bytes(fdb_value[..INSTANCE_ID_PREFIX_SIZE].try_into().unwrap());
                        if origin == local_instance_id {
                            continue;
                        }
                        let payload = Bytes::copy_from_slice(&fdb_value[INSTANCE_ID_PREFIX_SIZE..]);
                        if tx
                            .send(PubSubMessage::Message {
                                channel: channel.clone(),
                                data: payload,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to read pub/sub messages");
            }
        }
    }
}

/// Run a watcher loop for a pattern subscription.
///
/// Watches the global `__all__` notification key. When it fires, scans
/// the entire msg/ subspace for messages not yet seen (using a key
/// cursor), filters by pattern, and delivers matches.
///
/// Uses the same cursor-based approach as `channel_watcher`: on the
/// first wakeup we advance the cursor to the current tail of msg/ so
/// that historical messages are never replayed.
pub async fn pattern_watcher(
    db: Database,
    dirs: PubSubDirectories,
    pattern: Bytes,
    matcher: super::pattern::GlobMatcher,
    tx: mpsc::UnboundedSender<PubSubMessage>,
    cancel: CancellationToken,
    local_instance_id: u64,
) {
    // Key cursor across the entire msg/ subspace.
    // None means "start from the end of whatever exists at first wakeup".
    let mut cursor: Option<Vec<u8>> = None;
    let mut initialized = false;

    loop {
        // Watch the global notification key.
        let notify_key = dirs.notify.pack(&(GLOBAL_NOTIFY_CHANNEL,));

        let watch_result = match setup_watch(&db, &notify_key).await {
            Ok(watch_fut) => watch_fut,
            Err(e) => {
                warn!(error = %e, "failed to set up pattern watch, retrying");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }
        };

        tokio::select! {
            _ = watch_result => {}
            _ = cancel.cancelled() => {
                debug!(pattern = %String::from_utf8_lossy(&pattern), "pattern watcher cancelled");
                return;
            }
        }

        // On first wakeup: initialise cursor to the tail of msg/ so we
        // skip all historical messages (matching channel_watcher behaviour).
        if !initialized {
            if let Some(last_key) = find_msg_subspace_tail(&db, &dirs).await {
                cursor = Some(last_key);
            }
            initialized = true;
            continue;
        }

        // Scan the entire msg/ subspace for new messages (after cursor),
        // filtered by pattern.
        match read_new_pattern_messages(&db, &dirs, &matcher, &cursor).await {
            Ok((messages, new_cursor)) => {
                // Advance cursor even if no matching messages were found,
                // so we don't re-scan the same non-matching keys next time.
                if let Some(k) = new_cursor {
                    cursor = Some(k);
                }
                for (channel, fdb_value) in messages {
                    // Skip messages published by this instance.
                    if fdb_value.len() < INSTANCE_ID_PREFIX_SIZE {
                        continue;
                    }
                    let origin = u64::from_le_bytes(fdb_value[..INSTANCE_ID_PREFIX_SIZE].try_into().unwrap());
                    if origin == local_instance_id {
                        continue;
                    }
                    let data = Bytes::copy_from_slice(&fdb_value[INSTANCE_ID_PREFIX_SIZE..]);
                    if tx
                        .send(PubSubMessage::PMessage {
                            pattern: pattern.clone(),
                            channel: Bytes::from(channel),
                            data,
                        })
                        .is_err()
                    {
                        return; // Connection closed.
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to scan pattern messages");
            }
        }
    }
}

/// Set up an FDB watch on a key and return the watch future.
///
/// Creates a transaction, reads the current value (establishing the
/// watch baseline), commits, and returns the watch future.
async fn setup_watch(
    db: &Database,
    key: &[u8],
) -> Result<impl std::future::Future<Output = foundationdb::FdbResult<()>>, foundationdb::FdbError> {
    let trx = db.inner().create_trx()?;
    // Read to establish baseline (watch fires when value changes *after* this).
    // Ignore the read result — we only care about establishing the conflict range
    // and baseline for the watch. A failed get just means the commit will likely
    // fail too, which is handled by the caller's retry loop.
    let _ = trx.get(key, false).await;
    let watch_fut = trx.watch(key);
    trx.commit().await?;
    Ok(watch_fut)
}

/// Find the last key in the versionstamp queue for a specific channel.
///
/// Used by `channel_watcher` to initialise its cursor on first subscription
/// so historical messages are not replayed.
async fn find_queue_tail(db: &Database, dirs: &PubSubDirectories, channel: &[u8]) -> Option<Vec<u8>> {
    let trx = match db.inner().create_trx() {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "failed to create transaction for find_queue_tail");
            return None;
        }
    };

    let sub = dirs.msg.subspace(&(channel,));
    let (begin, end) = sub.range();

    let range = RangeOption {
        limit: Some(1),
        reverse: true,
        ..RangeOption::from((begin.as_slice(), end.as_slice()))
    };

    let mut stream = trx.get_ranges_keyvalues(range, true);
    if let Some(Ok(kv)) = stream.next().await {
        Some(kv.key().to_vec())
    } else {
        None
    }
}

/// Find the last key in the entire msg/ subspace (all channels).
///
/// Used by `pattern_watcher` to initialise its cursor so historical
/// messages across all channels are not replayed.
async fn find_msg_subspace_tail(db: &Database, dirs: &PubSubDirectories) -> Option<Vec<u8>> {
    let trx = match db.inner().create_trx() {
        Ok(t) => t,
        Err(e) => {
            warn!(error = %e, "failed to create transaction for find_msg_subspace_tail");
            return None;
        }
    };

    let (begin, end) = dirs.msg.range();
    let range = RangeOption {
        limit: Some(1),
        reverse: true,
        ..RangeOption::from((begin.as_slice(), end.as_slice()))
    };

    let mut stream = trx.get_ranges_keyvalues(range, true);
    if let Some(Ok(kv)) = stream.next().await {
        Some(kv.key().to_vec())
    } else {
        None
    }
}

/// Read new messages from a specific channel's versionstamp queue after `cursor`.
///
/// Returns `(fdb_key, message_data)` pairs in versionstamp order.
async fn read_new_messages(
    db: &Database,
    dirs: &PubSubDirectories,
    channel: &[u8],
    cursor: &Option<Vec<u8>>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, foundationdb::FdbError> {
    let trx = db.inner().create_trx()?;

    let sub = dirs.msg.subspace(&(channel,));
    let (subspace_begin, subspace_end) = sub.range();

    let begin = match cursor {
        Some(last_key) => {
            // Start after the last key we read.
            // Append \x00 to get the key strictly after `last_key`.
            let mut next = last_key.clone();
            next.push(0x00);
            next
        }
        None => subspace_begin,
    };

    let range = RangeOption {
        limit: Some(READ_BATCH_SIZE),
        reverse: false,
        ..RangeOption::from((begin.as_slice(), subspace_end.as_slice()))
    };

    let mut messages = Vec::new();
    let mut stream = trx.get_ranges_keyvalues(range, true);
    while let Some(result) = stream.next().await {
        let kv = result?;
        messages.push((kv.key().to_vec(), kv.value().to_vec()));
    }

    Ok(messages)
}

/// Read new messages from the entire msg/ subspace after `cursor`,
/// filtered by the given pattern matcher.
///
/// Returns:
/// - A vec of `(channel_bytes, fdb_value)` for matching messages
/// - The last key seen in the batch (new cursor position), whether or not
///   it matched the pattern — so non-matching keys are skipped on the
///   next call rather than re-scanned.
///
/// This is the cross-instance delivery path for PSUBSCRIBE. It's more
/// expensive than direct channel subscription because it scans all channels,
/// but PSUBSCRIBE is rare in practice and the batch limit caps the work.
async fn read_new_pattern_messages(
    db: &Database,
    dirs: &PubSubDirectories,
    matcher: &super::pattern::GlobMatcher,
    cursor: &Option<Vec<u8>>,
) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>), foundationdb::FdbError> {
    let trx = db.inner().create_trx()?;

    let (subspace_begin, subspace_end) = dirs.msg.range();

    let begin = match cursor {
        Some(last_key) => {
            let mut next = last_key.clone();
            next.push(0x00);
            next
        }
        None => subspace_begin,
    };

    let range = RangeOption {
        limit: Some(READ_BATCH_SIZE),
        reverse: false,
        ..RangeOption::from((begin.as_slice(), subspace_end.as_slice()))
    };

    let mut messages = Vec::new();
    let mut last_key_seen: Option<Vec<u8>> = None;

    let mut stream = trx.get_ranges_keyvalues(range, true);
    while let Some(result) = stream.next().await {
        let kv = result?;
        let key = kv.key().to_vec();
        // Always advance the cursor past every key we read, not just
        // matching ones, so non-matching keys are never re-scanned.
        last_key_seen = Some(key.clone());

        // Unpack key to extract channel name: msg/(channel, versionstamp)
        if let Ok((channel, _vs)) = dirs.msg.unpack::<(Vec<u8>, Versionstamp)>(&key)
            && matcher.matches(&channel)
        {
            messages.push((channel, kv.value().to_vec()));
        }
    }

    Ok((messages, last_key_seen))
}
