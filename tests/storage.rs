//! Integration tests for the FDB storage layer.
//!
//! Each test creates an isolated namespace using a UUID-based root prefix
//! to avoid interference between concurrent test runs.

use std::collections::HashSet;

use foundationdb::directory::{Directory, DirectoryLayer};
use uuid::Uuid;

use kvdb::storage::chunking::{self, CHUNK_SIZE};
use kvdb::storage::database::Database;
use kvdb::storage::directories::Directories;
use kvdb::storage::meta::{KeyType, ObjectMeta};
use kvdb::storage::run_transact;

/// Test context that provides an isolated FDB namespace per test.
///
/// On drop, the entire directory tree for this test's root prefix is
/// removed (best-effort).
struct StorageTestContext {
    db: Database,
    dirs: Directories,
    root_prefix: String,
}

impl StorageTestContext {
    async fn new() -> Self {
        let root_prefix = format!("kvdb_test_{}", Uuid::new_v4());
        let db = Database::new("fdb.cluster").unwrap();
        let dirs = Directories::open(&db, 0, &root_prefix).await.unwrap();
        Self { db, dirs, root_prefix }
    }
}

impl Drop for StorageTestContext {
    fn drop(&mut self) {
        let db = self.db.clone();
        let root_prefix = self.root_prefix.clone();

        // Async cleanup in a blocking thread — best-effort.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let trx = db.inner().create_trx().unwrap();
                let dir_layer = DirectoryLayer::default();
                let _ = dir_layer.remove(&trx, &[root_prefix]).await;
                let _ = trx.commit().await;
            });
        });
    }
}

// ─────────────────────────────────────────────────────────
// ObjectMeta tests
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn meta_write_read_roundtrip() {
    let ctx = StorageTestContext::new().await;
    let key = b"test_key";

    let trx = ctx.db.inner().create_trx().unwrap();
    let meta = ObjectMeta::new_string(1, 42);
    meta.write(&trx, &ctx.dirs, key).unwrap();
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    let read_meta = ObjectMeta::read(&trx, &ctx.dirs, key, 0, false).await.unwrap();
    assert_eq!(read_meta, Some(meta));
}

#[tokio::test]
async fn meta_read_nonexistent_returns_none() {
    let ctx = StorageTestContext::new().await;

    let trx = ctx.db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &ctx.dirs, b"no_such_key", 0, false)
        .await
        .unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn meta_lazy_expiry_returns_none() {
    let ctx = StorageTestContext::new().await;
    let key = b"expired_key";

    // Write with expiry in the past.
    let trx = ctx.db.inner().create_trx().unwrap();
    let mut meta = ObjectMeta::new_string(1, 10);
    meta.expires_at_ms = 1_000; // expired at 1 second
    meta.write(&trx, &ctx.dirs, key).unwrap();
    trx.commit().await.unwrap();

    // Read with now_ms > expires_at_ms => lazy expiry returns None.
    let trx = ctx.db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &ctx.dirs, key, 2_000, false).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn meta_non_expired_returns_some() {
    let ctx = StorageTestContext::new().await;
    let key = b"live_key";

    let trx = ctx.db.inner().create_trx().unwrap();
    let mut meta = ObjectMeta::new_string(1, 10);
    meta.expires_at_ms = 100_000; // expires far in the future
    meta.write(&trx, &ctx.dirs, key).unwrap();
    trx.commit().await.unwrap();

    // Read with now_ms < expires_at_ms => key is still alive.
    let trx = ctx.db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &ctx.dirs, key, 50_000, false).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().expires_at_ms, 100_000);
}

#[tokio::test]
async fn meta_delete_removes_key() {
    let ctx = StorageTestContext::new().await;
    let key = b"delete_me";

    // Write then delete.
    let trx = ctx.db.inner().create_trx().unwrap();
    let meta = ObjectMeta::new_string(1, 5);
    meta.write(&trx, &ctx.dirs, key).unwrap();
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    ObjectMeta::delete(&trx, &ctx.dirs, key);
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &ctx.dirs, key, 0, false).await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn meta_all_key_types_roundtrip() {
    let ctx = StorageTestContext::new().await;

    let types = [
        (KeyType::String, "string_key"),
        (KeyType::Hash, "hash_key"),
        (KeyType::Set, "set_key"),
        (KeyType::SortedSet, "zset_key"),
        (KeyType::List, "list_key"),
        (KeyType::Stream, "stream_key"),
    ];

    for (key_type, key_name) in &types {
        let trx = ctx.db.inner().create_trx().unwrap();
        let meta = ObjectMeta {
            key_type: *key_type,
            num_chunks: 1,
            size_bytes: 100,
            expires_at_ms: 0,
            cardinality: 42,
            last_accessed_ms: 0,
            list_head: 0,
            list_tail: 0,
            list_length: 0,
        };
        meta.write(&trx, &ctx.dirs, key_name.as_bytes()).unwrap();
        trx.commit().await.unwrap();

        let trx = ctx.db.inner().create_trx().unwrap();
        let read_meta = ObjectMeta::read(&trx, &ctx.dirs, key_name.as_bytes(), 0, false)
            .await
            .unwrap()
            .expect("key should exist");
        assert_eq!(read_meta.key_type, *key_type);
        assert_eq!(read_meta.cardinality, 42);
    }
}

// ─────────────────────────────────────────────────────────
// Chunking tests
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn chunking_small_value() {
    let ctx = StorageTestContext::new().await;
    let key = b"small";
    let data = b"hello world";

    let trx = ctx.db.inner().create_trx().unwrap();
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, data);
    assert_eq!(num_chunks, 1);
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &ctx.dirs, key, 1, false).await.unwrap();
    assert_eq!(result, data);
}

#[tokio::test]
async fn chunking_empty_value() {
    let ctx = StorageTestContext::new().await;
    let key = b"empty";
    let data = b"";

    let trx = ctx.db.inner().create_trx().unwrap();
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, data);
    assert_eq!(num_chunks, 0);
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &ctx.dirs, key, 0, false).await.unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn chunking_large_value_500kb() {
    let ctx = StorageTestContext::new().await;
    let key = b"large";
    let data = vec![0xABu8; 500_000]; // 500KB = 5 chunks

    let trx = ctx.db.inner().create_trx().unwrap();
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, &data);
    assert_eq!(num_chunks, 5);
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &ctx.dirs, key, 5, false).await.unwrap();
    assert_eq!(result.len(), 500_000);
    assert_eq!(result, data);
}

#[tokio::test]
async fn chunking_exact_boundary() {
    let ctx = StorageTestContext::new().await;
    let key = b"boundary";
    let data = vec![0x42u8; CHUNK_SIZE]; // exactly 100,000 bytes = 1 chunk

    let trx = ctx.db.inner().create_trx().unwrap();
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, &data);
    assert_eq!(num_chunks, 1);
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &ctx.dirs, key, 1, false).await.unwrap();
    assert_eq!(result.len(), CHUNK_SIZE);
    assert_eq!(result, data);
}

#[tokio::test]
async fn chunking_boundary_plus_one() {
    let ctx = StorageTestContext::new().await;
    let key = b"boundary_plus";
    let data = vec![0x77u8; CHUNK_SIZE + 1]; // 100,001 bytes = 2 chunks

    let trx = ctx.db.inner().create_trx().unwrap();
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, &data);
    assert_eq!(num_chunks, 2);
    trx.commit().await.unwrap();

    let trx = ctx.db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &ctx.dirs, key, 2, false).await.unwrap();
    assert_eq!(result.len(), CHUNK_SIZE + 1);
    assert_eq!(result, data);
}

#[tokio::test]
async fn chunking_delete_removes_all_chunks() {
    let ctx = StorageTestContext::new().await;
    let key = b"to_delete";
    let data = vec![0xFFu8; CHUNK_SIZE * 3]; // 3 chunks

    // Write 3 chunks.
    let trx = ctx.db.inner().create_trx().unwrap();
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, &data);
    assert_eq!(num_chunks, 3);
    trx.commit().await.unwrap();

    // Delete all chunks.
    let trx = ctx.db.inner().create_trx().unwrap();
    chunking::delete_chunks(&trx, &ctx.dirs, key);
    trx.commit().await.unwrap();

    // Reading should fail with DataCorruption (missing chunk 0).
    let trx = ctx.db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &ctx.dirs, key, 3, false).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn chunking_overwrite_different_size() {
    let ctx = StorageTestContext::new().await;
    let key = b"overwrite";

    // Write 3 chunks.
    let data_big = vec![0xAAu8; CHUNK_SIZE * 3];
    let trx = ctx.db.inner().create_trx().unwrap();
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, &data_big);
    assert_eq!(num_chunks, 3);
    trx.commit().await.unwrap();

    // Overwrite with 1 chunk and delete old chunks first.
    let data_small = vec![0xBBu8; 50];
    let trx = ctx.db.inner().create_trx().unwrap();
    chunking::delete_chunks(&trx, &ctx.dirs, key);
    let num_chunks = chunking::write_chunks(&trx, &ctx.dirs, key, &data_small);
    assert_eq!(num_chunks, 1);
    trx.commit().await.unwrap();

    // Read back — should get the small value.
    let trx = ctx.db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &ctx.dirs, key, 1, false).await.unwrap();
    assert_eq!(result, data_small);
}

// ─────────────────────────────────────────────────────────
// Directory tests
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn directories_all_subspaces_have_non_empty_prefixes() {
    let ctx = StorageTestContext::new().await;

    let prefixes = [
        ctx.dirs.meta.bytes(),
        ctx.dirs.obj.bytes(),
        ctx.dirs.hash.bytes(),
        ctx.dirs.set.bytes(),
        ctx.dirs.zset.bytes(),
        ctx.dirs.zset_idx.bytes(),
        ctx.dirs.list.bytes(),
        ctx.dirs.expire.bytes(),
    ];

    for (i, prefix) in prefixes.iter().enumerate() {
        assert!(!prefix.is_empty(), "subspace {i} has empty prefix");
    }
}

#[tokio::test]
async fn directories_all_subspace_prefixes_distinct() {
    let ctx = StorageTestContext::new().await;

    let prefixes: Vec<Vec<u8>> = vec![
        ctx.dirs.meta.bytes().to_vec(),
        ctx.dirs.obj.bytes().to_vec(),
        ctx.dirs.hash.bytes().to_vec(),
        ctx.dirs.set.bytes().to_vec(),
        ctx.dirs.zset.bytes().to_vec(),
        ctx.dirs.zset_idx.bytes().to_vec(),
        ctx.dirs.list.bytes().to_vec(),
        ctx.dirs.expire.bytes().to_vec(),
    ];

    let unique: HashSet<&Vec<u8>> = prefixes.iter().collect();
    assert_eq!(unique.len(), 8, "expected 8 distinct prefixes, got {}", unique.len());
}

#[tokio::test]
async fn directories_reopen_same_namespace_same_prefixes() {
    let root_prefix = format!("kvdb_test_{}", Uuid::new_v4());
    let db = Database::new("fdb.cluster").unwrap();

    let dirs1 = Directories::open(&db, 0, &root_prefix).await.unwrap();
    let dirs2 = Directories::open(&db, 0, &root_prefix).await.unwrap();

    assert_eq!(dirs1.meta.bytes(), dirs2.meta.bytes());
    assert_eq!(dirs1.obj.bytes(), dirs2.obj.bytes());
    assert_eq!(dirs1.hash.bytes(), dirs2.hash.bytes());
    assert_eq!(dirs1.set.bytes(), dirs2.set.bytes());
    assert_eq!(dirs1.zset.bytes(), dirs2.zset.bytes());
    assert_eq!(dirs1.zset_idx.bytes(), dirs2.zset_idx.bytes());
    assert_eq!(dirs1.list.bytes(), dirs2.list.bytes());
    assert_eq!(dirs1.expire.bytes(), dirs2.expire.bytes());

    // Cleanup.
    let trx = db.inner().create_trx().unwrap();
    let dir_layer = DirectoryLayer::default();
    let _ = dir_layer.remove(&trx, &[root_prefix]).await;
    let _ = trx.commit().await;
}

#[tokio::test]
async fn directories_different_namespaces_different_prefixes() {
    let root_prefix = format!("kvdb_test_{}", Uuid::new_v4());
    let db = Database::new("fdb.cluster").unwrap();

    let dirs_ns0 = Directories::open(&db, 0, &root_prefix).await.unwrap();
    let dirs_ns1 = Directories::open(&db, 1, &root_prefix).await.unwrap();

    // Every subspace prefix in ns0 should differ from the corresponding one in ns1.
    assert_ne!(dirs_ns0.meta.bytes(), dirs_ns1.meta.bytes());
    assert_ne!(dirs_ns0.obj.bytes(), dirs_ns1.obj.bytes());
    assert_ne!(dirs_ns0.hash.bytes(), dirs_ns1.hash.bytes());
    assert_ne!(dirs_ns0.set.bytes(), dirs_ns1.set.bytes());
    assert_ne!(dirs_ns0.zset.bytes(), dirs_ns1.zset.bytes());
    assert_ne!(dirs_ns0.zset_idx.bytes(), dirs_ns1.zset_idx.bytes());
    assert_ne!(dirs_ns0.list.bytes(), dirs_ns1.list.bytes());
    assert_ne!(dirs_ns0.expire.bytes(), dirs_ns1.expire.bytes());

    // Cleanup.
    let trx = db.inner().create_trx().unwrap();
    let dir_layer = DirectoryLayer::default();
    let _ = dir_layer.remove(&trx, &[root_prefix]).await;
    let _ = trx.commit().await;
}

// ─────────────────────────────────────────────────────────
// Transaction wrapper test
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn run_transact_write_then_read() {
    let ctx = StorageTestContext::new().await;
    let key = b"transact_key";
    let data = b"transact_value";

    // Write via run_transact.
    let dirs = ctx.dirs.clone();
    run_transact(&ctx.db, "test_write", |tr| {
        let dirs = dirs.clone();
        async move {
            let meta = ObjectMeta::new_string(1, data.len() as u64);
            meta.write(&tr, &dirs, key)
                .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))?;
            chunking::write_chunks(&tr, &dirs, key, data);
            Ok(())
        }
    })
    .await
    .unwrap();

    // Read via run_transact.
    let dirs = ctx.dirs.clone();
    let result = run_transact(&ctx.db, "test_read", |tr| {
        let dirs = dirs.clone();
        async move {
            let meta = ObjectMeta::read(&tr, &dirs, key, 0, false)
                .await
                .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))?;
            let meta = meta.expect("key should exist");
            let value = chunking::read_chunks(&tr, &dirs, key, meta.num_chunks, false)
                .await
                .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))?;
            Ok(value)
        }
    })
    .await
    .unwrap();

    assert_eq!(result, data);
}
