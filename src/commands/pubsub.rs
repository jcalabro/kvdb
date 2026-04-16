//! Pub/sub command handlers.
//!
//! Commands: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE,
//! PUBLISH, PUBSUB (CHANNELS/NUMSUB/NUMPAT).

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::protocol::types::RespValue;
use crate::pubsub::{self, MAX_MESSAGE_SIZE, PubSubMessage};
use crate::server::connection::ConnectionState;

use super::CommandResponse;

/// SUBSCRIBE channel [channel ...]
///
/// Subscribe to one or more channels. Each channel gets its own
/// confirmation response (a Push/Array with ["subscribe", channel, count]).
/// The connection enters pub/sub mode after the first subscription.
pub fn handle_subscribe(
    args: &[Bytes],
    state: &mut ConnectionState,
    pubsub_rx: &mut Option<mpsc::UnboundedReceiver<PubSubMessage>>,
) -> CommandResponse {
    if args.is_empty() {
        return CommandResponse::Reply(RespValue::err("ERR wrong number of arguments for 'subscribe' command"));
    }

    // Lazily create the mpsc channel on first subscription.
    let tx = ensure_pubsub_channel(state, pubsub_rx);

    let mut responses = Vec::with_capacity(args.len());

    for channel in args {
        if !state.subscribed_channels.contains(channel) {
            state.subscribed_channels.insert(channel.clone());

            // Register with the manager for local message delivery.
            state
                .pubsub
                .subscribe_channel(channel.clone(), state.connection_id, tx.clone());

            // Spawn an FDB watcher task for cross-instance delivery.
            // Store the cancel token so we can stop the task on UNSUBSCRIBE
            // or RESET without waiting for the connection to close.
            let cancel = CancellationToken::new();
            state.subscribed_channel_cancels.insert(channel.clone(), cancel.clone());

            let db = state.db.clone();
            let dirs = state.pubsub.dirs().clone();
            let ch = channel.clone();
            let watcher_tx = tx.clone();
            let instance_id = state.pubsub.instance_id;
            tokio::spawn(async move {
                pubsub::subscriber::channel_watcher(db, dirs, ch, watcher_tx, cancel, instance_id).await;
            });
        }

        let count = state.subscription_count() as i64;
        responses.push(subscribe_confirmation(b"subscribe", channel, count));
    }

    CommandResponse::MultiReply(responses)
}

/// UNSUBSCRIBE [channel ...]
///
/// Unsubscribe from one or more channels. If no channels given,
/// unsubscribe from all. Returns one confirmation per channel.
pub fn handle_unsubscribe(args: &[Bytes], state: &mut ConnectionState) -> CommandResponse {
    let channels: Vec<Bytes> = if args.is_empty() {
        // Unsubscribe from all channels.
        state.subscribed_channels.iter().cloned().collect()
    } else {
        args.to_vec()
    };

    // Edge case: UNSUBSCRIBE with no args and no subscriptions.
    if channels.is_empty() {
        return CommandResponse::MultiReply(vec![subscribe_confirmation(b"unsubscribe", &Bytes::new(), 0)]);
    }

    let mut responses = Vec::with_capacity(channels.len());

    for channel in &channels {
        state.subscribed_channels.remove(channel);
        state.pubsub.unsubscribe_channel(channel, state.connection_id);

        // Cancel the watcher task so it stops delivering messages
        // immediately rather than waiting for the connection to close.
        if let Some(cancel) = state.subscribed_channel_cancels.remove(channel) {
            cancel.cancel();
        }

        let count = state.subscription_count() as i64;
        responses.push(subscribe_confirmation(b"unsubscribe", channel, count));
    }

    CommandResponse::MultiReply(responses)
}

/// PSUBSCRIBE pattern [pattern ...]
///
/// Subscribe to one or more patterns. Like SUBSCRIBE, returns one
/// confirmation per pattern.
pub fn handle_psubscribe(
    args: &[Bytes],
    state: &mut ConnectionState,
    pubsub_rx: &mut Option<mpsc::UnboundedReceiver<PubSubMessage>>,
) -> CommandResponse {
    if args.is_empty() {
        return CommandResponse::Reply(RespValue::err("ERR wrong number of arguments for 'psubscribe' command"));
    }

    let tx = ensure_pubsub_channel(state, pubsub_rx);

    let mut responses = Vec::with_capacity(args.len());

    for pattern in args {
        if !state.subscribed_patterns.contains(pattern) {
            state.subscribed_patterns.insert(pattern.clone());

            // Register with the manager.
            state
                .pubsub
                .subscribe_pattern(pattern.clone(), state.connection_id, tx.clone());

            // Spawn a pattern watcher task with a stored cancel token.
            let cancel = CancellationToken::new();
            state.subscribed_pattern_cancels.insert(pattern.clone(), cancel.clone());

            let db = state.db.clone();
            let dirs = state.pubsub.dirs().clone();
            let pat = pattern.clone();
            let matcher = pubsub::pattern::GlobMatcher::new(&pat);
            let watcher_tx = tx.clone();
            let instance_id = state.pubsub.instance_id;
            tokio::spawn(async move {
                pubsub::subscriber::pattern_watcher(db, dirs, pat, matcher, watcher_tx, cancel, instance_id).await;
            });
        }

        let count = state.subscription_count() as i64;
        responses.push(subscribe_confirmation(b"psubscribe", pattern, count));
    }

    CommandResponse::MultiReply(responses)
}

/// PUNSUBSCRIBE [pattern ...]
///
/// Unsubscribe from one or more patterns. If no patterns given,
/// unsubscribe from all. Returns one confirmation per pattern.
pub fn handle_punsubscribe(args: &[Bytes], state: &mut ConnectionState) -> CommandResponse {
    let patterns: Vec<Bytes> = if args.is_empty() {
        state.subscribed_patterns.iter().cloned().collect()
    } else {
        args.to_vec()
    };

    // Edge case: PUNSUBSCRIBE with no args and no subscriptions.
    if patterns.is_empty() {
        return CommandResponse::MultiReply(vec![subscribe_confirmation(b"punsubscribe", &Bytes::new(), 0)]);
    }

    let mut responses = Vec::with_capacity(patterns.len());

    for pattern in &patterns {
        state.subscribed_patterns.remove(pattern);
        state.pubsub.unsubscribe_pattern(pattern, state.connection_id);

        // Cancel the watcher task.
        if let Some(cancel) = state.subscribed_pattern_cancels.remove(pattern) {
            cancel.cancel();
        }

        let count = state.subscription_count() as i64;
        responses.push(subscribe_confirmation(b"punsubscribe", pattern, count));
    }

    CommandResponse::MultiReply(responses)
}

/// PUBLISH channel message
///
/// Publish a message to a channel. Returns the number of clients
/// that received the message (local to this instance, matching
/// Redis standalone semantics).
pub async fn handle_publish(args: &[Bytes], state: &mut ConnectionState) -> RespValue {
    if args.len() != 2 {
        return RespValue::err("ERR wrong number of arguments for 'publish' command");
    }

    let channel = &args[0];
    let message = &args[1];

    if message.len() > MAX_MESSAGE_SIZE {
        return RespValue::err(format!(
            "ERR message exceeds maximum size of {} bytes",
            MAX_MESSAGE_SIZE
        ));
    }

    match state.pubsub.publish(channel, message).await {
        Ok(count) => RespValue::Integer(count),
        Err(e) => RespValue::err(format!("ERR pub/sub publish failed: {e}")),
    }
}

/// PUBSUB subcommand [args...]
///
/// Introspection commands for the pub/sub system.
pub fn handle_pubsub(args: &[Bytes], state: &ConnectionState) -> RespValue {
    if args.is_empty() {
        return RespValue::err("ERR wrong number of arguments for 'pubsub' command");
    }

    let subcmd = args[0].to_ascii_uppercase();
    match subcmd.as_slice() {
        b"CHANNELS" => {
            let pattern = args.get(1).map(|p| p.as_ref());
            let channels = state.pubsub.active_channels(pattern);
            RespValue::Array(Some(
                channels.into_iter().map(|ch| RespValue::BulkString(Some(ch))).collect(),
            ))
        }
        b"NUMSUB" => {
            let channels: Vec<Bytes> = args[1..].to_vec();
            let counts = state.pubsub.numsub(&channels);
            let mut result = Vec::with_capacity(counts.len() * 2);
            for (ch, count) in counts {
                result.push(RespValue::BulkString(Some(ch)));
                result.push(RespValue::Integer(count));
            }
            RespValue::Array(Some(result))
        }
        b"NUMPAT" => RespValue::Integer(state.pubsub.numpat()),
        _ => {
            let name = String::from_utf8_lossy(&subcmd);
            RespValue::err(format!(
                "ERR unknown subcommand or wrong number of arguments for 'PUBSUB|{name}' command"
            ))
        }
    }
}

/// Build a subscribe/unsubscribe confirmation response.
///
/// Format: Push/Array of [type, channel/pattern, subscription_count]
fn subscribe_confirmation(msg_type: &[u8], name: &[u8], count: i64) -> RespValue {
    // Use Push type — the encoder downgrades to Array for RESP2 clients.
    RespValue::Push(vec![
        RespValue::BulkString(Some(Bytes::copy_from_slice(msg_type))),
        RespValue::BulkString(Some(Bytes::copy_from_slice(name))),
        RespValue::Integer(count),
    ])
}

/// Ensure the pub/sub mpsc channel exists, creating it if needed.
///
/// The first SUBSCRIBE or PSUBSCRIBE on a connection creates the
/// channel. The receiver is handed to the connection loop via
/// `pubsub_rx`, and the sender is stored in `state.pubsub_tx`.
fn ensure_pubsub_channel(
    state: &mut ConnectionState,
    pubsub_rx: &mut Option<mpsc::UnboundedReceiver<PubSubMessage>>,
) -> mpsc::UnboundedSender<PubSubMessage> {
    if let Some(ref tx) = state.pubsub_tx {
        return tx.clone();
    }

    let (tx, rx) = mpsc::unbounded_channel();
    state.pubsub_tx = Some(tx.clone());
    *pubsub_rx = Some(rx);
    tx
}
