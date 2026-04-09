//! Fuzz target: full command pipeline (parse -> from_resp -> dispatch -> encode).
//!
//! Generates arbitrary Redis-like commands (random names and argument
//! payloads), feeds them through the entire command processing pipeline,
//! and verifies:
//!
//! 1. No panics anywhere in the chain
//! 2. The dispatch response encodes to valid, parseable RESP
//! 3. ConnectionState invariants hold after dispatch
//!
//! This covers all M2+ code that the parser/encoder fuzz targets don't
//! reach: `RedisCommand::from_resp()`, every command handler, error
//! formatting, and response encoding for dispatch-produced values.
//!
//! Run with `cargo +nightly fuzz run command_dispatch`.

#![no_main]

use arbitrary::Arbitrary;
use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;

use kvdb::commands::{self, CommandResponse};
use kvdb::protocol::types::{RedisCommand, RespValue};
use kvdb::protocol::{encoder, parser};
use kvdb::server::connection::ConnectionState;

/// A fuzz-friendly representation of a Redis command.
///
/// The fuzzer mutates the name and args to exercise every dispatch path:
/// known commands with valid/invalid args, unknown commands with arbitrary
/// payloads, empty names, binary data in arguments, etc.
#[derive(Debug, Arbitrary)]
struct FuzzCommand {
    name: Vec<u8>,
    args: Vec<Vec<u8>>,
}

fuzz_target!(|input: FuzzCommand| {
    // Bound sizes to keep individual executions fast. These limits are
    // well above anything a real Redis command would need — they only
    // exist to prevent the fuzzer from spending time on degenerate cases.
    if input.name.len() > 64 {
        return;
    }
    if input.args.len() > 16 {
        return;
    }
    if input.args.iter().any(|a| a.len() > 4096) {
        return;
    }

    // Build a RESP Array of BulkStrings — the wire format clients use.
    let mut elements = Vec::with_capacity(1 + input.args.len());
    elements.push(RespValue::BulkString(Some(Bytes::from(input.name))));
    for arg in input.args {
        elements.push(RespValue::BulkString(Some(Bytes::from(arg))));
    }
    let wire_value = RespValue::Array(Some(elements));

    // Encode to wire format, then parse back. This validates our encoder
    // produces valid RESP for command-shaped values (the existing encoder
    // fuzz target uses arbitrary shapes, not specifically commands).
    let encoded = encoder::encode(&wire_value, 2);
    let mut parse_buf = BytesMut::from(&encoded[..]);
    let parsed = parser::parse(&mut parse_buf)
        .expect("re-parsing our own encoded command must not error")
        .expect("re-parsing our own encoded command must be complete");
    assert_eq!(parse_buf.len(), 0, "unconsumed bytes after parsing command");

    // Convert to RedisCommand. This exercises from_resp() — although our
    // inputs are always valid Array-of-BulkStrings, so it should always
    // succeed. The only failure case would be an empty array, which can't
    // happen since we always push at least the name element.
    let cmd = match RedisCommand::from_resp(parsed) {
        Ok(cmd) => cmd,
        Err(_) => return,
    };

    // Dispatch the command against a fresh connection state.
    let mut state = ConnectionState::default();
    let response = match commands::dispatch(&cmd, &mut state) {
        CommandResponse::Reply(resp) => resp,
        CommandResponse::Close(resp) => resp,
    };

    // Encode the dispatch response in the connection's protocol version.
    // This exercises the encoder on real dispatch-produced values — including
    // the HELLO Map, error messages with format strings, etc.
    let mut resp_buf = BytesMut::new();
    encoder::encode_into(&mut resp_buf, &response, state.protocol_version);

    // The encoded response must be valid, parseable RESP.
    let resp_parsed = parser::parse(&mut resp_buf)
        .expect("dispatch response must be valid RESP")
        .expect("dispatch response must be a complete frame");
    assert_eq!(
        resp_buf.len(),
        0,
        "unconsumed bytes after parsing dispatch response"
    );

    // If the response was an error, verify the error message is non-empty.
    if let RespValue::Error(ref msg) = resp_parsed {
        assert!(!msg.is_empty(), "error response must have a message");
    }

    // ConnectionState invariants must hold after any command.
    assert!(
        state.protocol_version == 2 || state.protocol_version == 3,
        "protocol_version must be 2 or 3, got {}",
        state.protocol_version
    );
    assert!(
        state.selected_db <= 15,
        "selected_db must be 0-15, got {}",
        state.selected_db
    );

    // If the fuzzer happened to send a valid HELLO 3, also verify the
    // response encodes correctly in RESP3 mode (Map type, not downgraded).
    if state.protocol_version == 3 {
        let mut resp3_buf = BytesMut::new();
        encoder::encode_into(&mut resp3_buf, &response, 3);
        parser::parse(&mut resp3_buf)
            .expect("RESP3 response must be valid")
            .expect("RESP3 response must be complete");
        assert_eq!(resp3_buf.len(), 0, "unconsumed bytes in RESP3 response");
    }
});
