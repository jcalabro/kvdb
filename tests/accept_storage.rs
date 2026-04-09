//! Property-based acceptance tests for the FDB storage layer.
//!
//! Exercises chunking, ObjectMeta serialization, and FDB round-trips
//! with randomized inputs via proptest. Fewer cases than typical
//! proptests, but with large payloads (up to 2MB) to stress chunk
//! boundaries and multi-chunk reassembly.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;
use uuid::Uuid;

use kvdb::storage::chunking;
use kvdb::storage::database::Database;
use kvdb::storage::directories::Directories;
use kvdb::storage::meta::{KeyType, ObjectMeta};

/// Monotonic counter to generate unique keys within a single test binary.
static KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_key() -> Vec<u8> {
    let id = KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("prop_{id}").into_bytes()
}

/// Shared database + directories for FDB acceptance tests (opened once).
fn shared() -> &'static (Database, Directories, String) {
    static SHARED: OnceLock<(Database, Directories, String)> = OnceLock::new();
    SHARED.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let db = Database::new("fdb.cluster").unwrap();
        let root = format!("kvdb_test_{}", Uuid::new_v4());
        let dirs = rt.block_on(Directories::open(&db, 0, &root)).unwrap();
        (db, dirs, root)
    })
}

/// Generate a random `KeyType`.
fn arb_key_type() -> impl Strategy<Value = KeyType> {
    (0u8..6).prop_map(|v| match v {
        0 => KeyType::String,
        1 => KeyType::Hash,
        2 => KeyType::Set,
        3 => KeyType::SortedSet,
        4 => KeyType::List,
        _ => KeyType::Stream,
    })
}

/// Generate a random `ObjectMeta` with arbitrary field values.
fn arb_object_meta() -> impl Strategy<Value = ObjectMeta> {
    (
        arb_key_type(),
        any::<u32>(),
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        any::<i64>(),
        any::<i64>(),
        any::<u64>(),
    )
        .prop_map(
            |(key_type, num_chunks, size_bytes, cardinality, last_accessed_ms, list_head, list_tail, list_length)| {
                ObjectMeta {
                    key_type,
                    num_chunks,
                    size_bytes,
                    expires_at_ms: 0,
                    cardinality,
                    last_accessed_ms,
                    list_head,
                    list_tail,
                    list_length,
                }
            },
        )
}

mod accept {
    use super::*;

    // ─────────────────────────────────────────────────────────
    // Chunk roundtrip: 20 cases, 0-2MB random data
    // Fewer cases but larger payloads — the interesting bugs
    // live at chunk boundaries, not in volume of small writes.
    // ─────────────────────────────────────────────────────────

    #[test]
    fn chunk_roundtrip() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (db, dirs, _root) = shared();

        let config = ProptestConfig::with_cases(20);
        let strategy = proptest::collection::vec(any::<u8>(), 0..2_000_000);

        let mut runner = proptest::test_runner::TestRunner::new(config);
        let result = runner.run(&strategy, |data| {
            let key = unique_key();

            // Run FDB I/O inside block_on, then assert outside so
            // prop_assert! can drive proptest shrinking on failure.
            let (num_chunks, read_back) = rt.block_on(async {
                let trx = db.inner().create_trx().unwrap();
                let nc = chunking::write_chunks(&trx, dirs, &key, &data);
                trx.commit().await.unwrap();

                let trx = db.inner().create_trx().unwrap();
                let rb = chunking::read_chunks(&trx, dirs, &key, nc, false).await.unwrap();
                (nc, rb)
            });

            prop_assert_eq!(num_chunks, chunking::chunk_count(data.len()), "chunk count");
            prop_assert!(read_back == data, "data mismatch at len={}", data.len());
            Ok(())
        });

        if let Err(e) = result {
            panic!("{}\n{}", e, runner);
        }
    }

    // ─────────────────────────────────────────────────────────
    // ObjectMeta serde roundtrip: 200 cases (pure CPU, fast)
    // ─────────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        #[test]
        fn meta_serde_roundtrip(meta in arb_object_meta()) {
            let bytes = meta.serialize().unwrap();
            let decoded = ObjectMeta::deserialize(&bytes).unwrap();
            prop_assert_eq!(decoded, meta);
        }
    }

    // ─────────────────────────────────────────────────────────
    // ObjectMeta FDB roundtrip: 30 cases
    // ─────────────────────────────────────────────────────────

    #[test]
    fn meta_fdb_roundtrip() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (db, dirs, _root) = shared();

        let config = ProptestConfig::with_cases(30);
        let strategy = arb_object_meta();

        let mut runner = proptest::test_runner::TestRunner::new(config);
        let result = runner.run(&strategy, |meta| {
            let key = unique_key();

            let read_meta = rt.block_on(async {
                let trx = db.inner().create_trx().unwrap();
                meta.write(&trx, dirs, &key).unwrap();
                trx.commit().await.unwrap();

                let trx = db.inner().create_trx().unwrap();
                ObjectMeta::read(&trx, dirs, &key, 0, false)
                    .await
                    .unwrap()
                    .expect("key should exist after write")
            });

            prop_assert_eq!(read_meta, meta, "ObjectMeta FDB roundtrip mismatch");
            Ok(())
        });

        if let Err(e) = result {
            panic!("{}\n{}", e, runner);
        }
    }
}
