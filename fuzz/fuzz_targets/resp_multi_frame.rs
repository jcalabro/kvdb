//! Fuzz target: multi-frame parsing loop.
//!
//! Feeds arbitrary bytes into the parser in a loop until the buffer is
//! exhausted or an error occurs. This catches buffer management bugs
//! that single-frame parsing misses: off-by-one in split_to, leftover
//! bytes corrupting the next frame, etc.
//!
//! Run with `cargo +nightly fuzz run resp_multi_frame`.

#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let initial_len = buf.len();
    let mut total_consumed = 0;

    loop {
        let before = buf.len();
        match kvdb::protocol::parser::parse(&mut buf) {
            Ok(Some(value)) => {
                let consumed = before - buf.len();
                assert!(consumed > 0, "parse consumed zero bytes but returned a value");
                total_consumed += consumed;

                // Every successfully parsed value must be encodable
                let _ = kvdb::protocol::encoder::encode(&value, 3);
                let _ = kvdb::protocol::encoder::encode(&value, 2);
            }
            Ok(None) => {
                // Incomplete — buffer should be unchanged
                assert_eq!(buf.len(), before, "parse returned None but modified buffer");
                break;
            }
            Err(_) => {
                // Protocol error — stop parsing
                break;
            }
        }
    }

    // Sanity: we never consumed more than the input
    assert!(total_consumed <= initial_len);
});
