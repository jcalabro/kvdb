//! Property-based acceptance tests for the FDB storage layer.
//!
//! Exercises chunking, ObjectMeta serialization, and FDB round-trips
//! with randomized inputs via proptest.

use std::sync::atomic::{AtomicU64, Ordering};

use foundationdb::directory::{Directory, DirectoryLayer};
use proptest::prelude::*;
use uuid::Uuid;

use kvdb::storage::chunking;
use kvdb::storage::database::Database;
use kvdb::storage::directories::Directories;
use kvdb::storage::meta::{KeyType, ObjectMeta};

/// Monotonic counter to generate unique keys within a single test function.
static KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique key for each proptest case to avoid inter-case conflicts.
fn unique_key() -> Vec<u8> {
    let id = KEY_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("prop_{id}").into_bytes()
}

/// Create an isolated FDB test namespace.
async fn setup() -> (Database, Directories, String) {
    let root_prefix = format!("kvdb_test_{}", Uuid::new_v4());
    let db = Database::new("fdb.cluster").unwrap();
    let dirs = Directories::open(&db, 0, &root_prefix).await.unwrap();
    (db, dirs, root_prefix)
}

/// Remove the test namespace from FDB (best-effort).
async fn cleanup(db: &Database, root_prefix: &str) {
    let trx = db.inner().create_trx().unwrap();
    let dir_layer = DirectoryLayer::default();
    let _ = dir_layer.remove(&trx, &[root_prefix.to_string()]).await;
    let _ = trx.commit().await;
}

/// Generate a random `KeyType` by mapping 0..6 to the six variants.
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
/// `expires_at_ms` is always 0 to avoid lazy expiry interference.
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
    // 1. Chunk roundtrip property (100 cases)
    // ─────────────────────────────────────────────────────────

    /// Test that writing random data as chunks and reading it back
    /// produces byte-for-byte identical output. Uses a single shared
    /// FDB namespace with unique keys per case to avoid directory-layer
    /// transaction conflicts.
    #[test]
    fn chunk_roundtrip() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (db, dirs, root_prefix) = rt.block_on(setup());

        let config = ProptestConfig::with_cases(100);
        let strategy = proptest::collection::vec(any::<u8>(), 0..1_000_000);

        let mut runner = proptest::test_runner::TestRunner::new(config);
        let result = runner.run(&strategy, |data| {
            let key = unique_key();

            rt.block_on(async {
                // Write chunks.
                let dirs_c = dirs.clone();
                let data_c = data.clone();
                let key_c = key.clone();
                kvdb::storage::run_transact(&db, "test_chunk_write", |tr| {
                    let dirs = dirs_c.clone();
                    let data = data_c.clone();
                    let key = key_c.clone();
                    async move {
                        chunking::write_chunks(&tr, &dirs, &key, &data);
                        Ok(())
                    }
                })
                .await
                .unwrap();

                // Compute expected chunk count.
                let num_chunks = chunking::chunk_count(data.len());

                // Read chunks back.
                let dirs_c = dirs.clone();
                let key_c = key.clone();
                let result: Vec<u8> = kvdb::storage::run_transact(&db, "test_chunk_read", |tr| {
                    let dirs = dirs_c.clone();
                    let key = key_c.clone();
                    async move {
                        chunking::read_chunks(&tr, &dirs, &key, num_chunks, false)
                            .await
                            .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))
                    }
                })
                .await
                .unwrap();

                assert_eq!(result.len(), data.len(), "length mismatch");
                assert!(result == data, "data mismatch at len={}", data.len());
            });

            Ok(())
        });

        rt.block_on(cleanup(&db, &root_prefix));

        if let Err(e) = result {
            panic!("{}\n{}", e, runner);
        }
    }

    // ─────────────────────────────────────────────────────────
    // 2. ObjectMeta serde roundtrip property (200 cases)
    // ─────────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        /// ObjectMeta serde roundtrip: generate random ObjectMeta fields,
        /// serialize to bincode, deserialize, verify equality.
        #[test]
        fn meta_serde_roundtrip(meta in arb_object_meta()) {
            let bytes = meta.serialize().unwrap();
            let decoded = ObjectMeta::deserialize(&bytes).unwrap();
            prop_assert_eq!(decoded, meta);
        }
    }

    // ─────────────────────────────────────────────────────────
    // 3. ObjectMeta FDB roundtrip property (50 cases)
    // ─────────────────────────────────────────────────────────

    /// Test that writing random ObjectMeta to FDB and reading it back
    /// produces identical metadata.
    #[test]
    fn meta_fdb_roundtrip() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (db, dirs, root_prefix) = rt.block_on(setup());

        let config = ProptestConfig::with_cases(50);
        let strategy = arb_object_meta();

        let mut runner = proptest::test_runner::TestRunner::new(config);
        let result = runner.run(&strategy, |meta| {
            let key = unique_key();

            rt.block_on(async {
                // Write.
                let dirs_c = dirs.clone();
                let meta_c = meta.clone();
                let key_c = key.clone();
                kvdb::storage::run_transact(&db, "test_meta_write", |tr| {
                    let dirs = dirs_c.clone();
                    let meta = meta_c.clone();
                    let key = key_c.clone();
                    async move {
                        meta.write(&tr, &dirs, &key)
                            .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))
                    }
                })
                .await
                .unwrap();

                // Read back (now_ms = 0 skips expiry check).
                let dirs_c = dirs.clone();
                let key_c = key.clone();
                let read_meta: ObjectMeta = kvdb::storage::run_transact(&db, "test_meta_read", |tr| {
                    let dirs = dirs_c.clone();
                    let key = key_c.clone();
                    async move {
                        ObjectMeta::read(&tr, &dirs, &key, 0, false)
                            .await
                            .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))
                            .map(|opt| opt.expect("key should exist after write"))
                    }
                })
                .await
                .unwrap();

                assert_eq!(read_meta, meta, "ObjectMeta FDB roundtrip mismatch");
            });

            Ok(())
        });

        rt.block_on(cleanup(&db, &root_prefix));

        if let Err(e) = result {
            panic!("{}\n{}", e, runner);
        }
    }
}
