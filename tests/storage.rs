//! Integration tests for the FDB storage layer.
//!
//! Tests are grouped by component to minimize FDB directory setup
//! overhead — each `#[tokio::test]` function runs in its own nextest
//! process and pays ~50ms to open directories, so fewer functions =
//! faster total runtime. Within each function, tests use unique keys
//! to avoid interference.

use std::collections::HashSet;

use foundationdb::directory::{Directory, DirectoryLayer};
use uuid::Uuid;

use kvdb::storage::chunking::{self, CHUNK_SIZE};
use kvdb::storage::database::Database;
use kvdb::storage::directories::Directories;
use kvdb::storage::meta::{KeyType, ObjectMeta};
use kvdb::storage::run_transact;

/// Open an isolated Database + Directories pair for a test.
async fn setup() -> (Database, Directories, String) {
    let db = Database::new("fdb.cluster").expect("failed to open FDB — is `just up` running?");
    let root = format!("kvdb_test_{}", Uuid::new_v4());
    let dirs = Directories::open(&db, 0, &root)
        .await
        .expect("failed to open directories");
    (db, dirs, root)
}

/// Best-effort cleanup of a test namespace.
async fn cleanup(db: &Database, root: &str) {
    let trx = db.inner().create_trx().unwrap();
    let dir_layer = DirectoryLayer::default();
    let _ = dir_layer.remove(&trx, &[root.to_string()]).await;
    let _ = trx.commit().await;
}

// ─────────────────────────────────────────────────────────
// ObjectMeta: write, read, expiry, delete, all types
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn meta_operations() {
    let (db, dirs, root) = setup().await;

    // write/read roundtrip
    let trx = db.inner().create_trx().unwrap();
    let meta = ObjectMeta::new_string(1, 42);
    meta.write(&trx, &dirs, b"wr_key").unwrap();
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &dirs, b"wr_key", 0, false).await.unwrap();
    assert_eq!(result, Some(meta), "write/read roundtrip failed");

    // read nonexistent returns None
    let trx = db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &dirs, b"no_such_key", 0, false).await.unwrap();
    assert_eq!(result, None, "nonexistent key should return None");

    // lazy expiry returns None for expired key
    let trx = db.inner().create_trx().unwrap();
    let mut expired = ObjectMeta::new_string(1, 10);
    expired.expires_at_ms = 1_000;
    expired.write(&trx, &dirs, b"exp_key").unwrap();
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &dirs, b"exp_key", 2_000, false).await.unwrap();
    assert_eq!(result, None, "expired key should return None");

    // non-expired key returns Some
    let trx = db.inner().create_trx().unwrap();
    let mut live = ObjectMeta::new_string(1, 10);
    live.expires_at_ms = 100_000;
    live.write(&trx, &dirs, b"live_key").unwrap();
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &dirs, b"live_key", 50_000, false).await.unwrap();
    assert!(result.is_some(), "non-expired key should return Some");
    assert_eq!(result.unwrap().expires_at_ms, 100_000);

    // delete removes key
    let trx = db.inner().create_trx().unwrap();
    ObjectMeta::new_string(1, 5).write(&trx, &dirs, b"del_key").unwrap();
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    ObjectMeta::delete(&trx, &dirs, b"del_key");
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &dirs, b"del_key", 0, false).await.unwrap();
    assert_eq!(result, None, "deleted key should return None");

    // all key types roundtrip
    for (kt, key) in [
        (KeyType::String, b"kt_str" as &[u8]),
        (KeyType::Hash, b"kt_hash"),
        (KeyType::Set, b"kt_set"),
        (KeyType::SortedSet, b"kt_zset"),
        (KeyType::List, b"kt_list"),
        (KeyType::Stream, b"kt_stream"),
    ] {
        let trx = db.inner().create_trx().unwrap();
        let meta = ObjectMeta {
            key_type: kt,
            num_chunks: 1,
            size_bytes: 100,
            expires_at_ms: 0,
            cardinality: 42,
            last_accessed_ms: 0,
            list_head: 0,
            list_tail: 0,
            list_length: 0,
        };
        meta.write(&trx, &dirs, key).unwrap();
        trx.commit().await.unwrap();

        let trx = db.inner().create_trx().unwrap();
        let read = ObjectMeta::read(&trx, &dirs, key, 0, false)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("key type {kt:?} should exist"));
        assert_eq!(read.key_type, kt, "key type mismatch for {kt:?}");
        assert_eq!(read.cardinality, 42, "cardinality mismatch for {kt:?}");
    }

    // reading garbage bytes from the meta key returns a serialization error
    let garbage_key = dirs.meta_key(b"garbage_meta");
    let trx = db.inner().create_trx().unwrap();
    trx.set(&garbage_key, b"this is not valid bincode");
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = ObjectMeta::read(&trx, &dirs, b"garbage_meta", 0, false).await;
    assert!(result.is_err(), "reading garbage meta should return error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("serialization"),
        "error should mention serialization, got: {err_msg}"
    );

    cleanup(&db, &root).await;
}

// ─────────────────────────────────────────────────────────
// Chunking: 1-byte, small, empty, large, boundaries, delete, overwrite
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn chunk_operations() {
    let (db, dirs, root) = setup().await;

    // 1-byte value
    let trx = db.inner().create_trx().unwrap();
    let nc = chunking::write_chunks(&trx, &dirs, b"c_1byte", b"\x42");
    assert_eq!(nc, 1);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &dirs, b"c_1byte", 1, false).await.unwrap();
    assert_eq!(result, b"\x42", "1-byte value mismatch");

    // small value (< 1 chunk)
    let trx = db.inner().create_trx().unwrap();
    let nc = chunking::write_chunks(&trx, &dirs, b"c_small", b"hello world");
    assert_eq!(nc, 1);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &dirs, b"c_small", 1, false).await.unwrap();
    assert_eq!(result, b"hello world", "small value mismatch");

    // empty value
    let trx = db.inner().create_trx().unwrap();
    let nc = chunking::write_chunks(&trx, &dirs, b"c_empty", b"");
    assert_eq!(nc, 0);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &dirs, b"c_empty", 0, false).await.unwrap();
    assert!(result.is_empty(), "empty value should be empty");

    // large value (500KB = 5 chunks)
    let big = vec![0xABu8; 500_000];
    let trx = db.inner().create_trx().unwrap();
    let nc = chunking::write_chunks(&trx, &dirs, b"c_big", &big);
    assert_eq!(nc, 5);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &dirs, b"c_big", 5, false).await.unwrap();
    assert_eq!(result, big, "500KB value mismatch");

    // exact chunk boundary (100,000 bytes)
    let exact = vec![0x42u8; CHUNK_SIZE];
    let trx = db.inner().create_trx().unwrap();
    let nc = chunking::write_chunks(&trx, &dirs, b"c_exact", &exact);
    assert_eq!(nc, 1);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &dirs, b"c_exact", 1, false).await.unwrap();
    assert_eq!(result, exact, "exact boundary mismatch");

    // boundary + 1 (100,001 bytes = 2 chunks)
    let bplus = vec![0x77u8; CHUNK_SIZE + 1];
    let trx = db.inner().create_trx().unwrap();
    let nc = chunking::write_chunks(&trx, &dirs, b"c_bplus", &bplus);
    assert_eq!(nc, 2);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &dirs, b"c_bplus", 2, false).await.unwrap();
    assert_eq!(result, bplus, "boundary+1 mismatch");

    // delete removes all chunks
    let del_data = vec![0xFFu8; CHUNK_SIZE * 3];
    let trx = db.inner().create_trx().unwrap();
    chunking::write_chunks(&trx, &dirs, b"c_del", &del_data);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    chunking::delete_chunks(&trx, &dirs, b"c_del");
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    assert!(
        chunking::read_chunks(&trx, &dirs, b"c_del", 3, false).await.is_err(),
        "reading deleted chunks should fail"
    );

    // overwrite with different size (3 chunks → 1 chunk)
    let trx = db.inner().create_trx().unwrap();
    chunking::write_chunks(&trx, &dirs, b"c_over", &vec![0xAAu8; CHUNK_SIZE * 3]);
    trx.commit().await.unwrap();

    let small = vec![0xBBu8; 50];
    let trx = db.inner().create_trx().unwrap();
    chunking::delete_chunks(&trx, &dirs, b"c_over");
    chunking::write_chunks(&trx, &dirs, b"c_over", &small);
    trx.commit().await.unwrap();

    let trx = db.inner().create_trx().unwrap();
    let result = chunking::read_chunks(&trx, &dirs, b"c_over", 1, false).await.unwrap();
    assert_eq!(result, small, "overwrite mismatch");

    cleanup(&db, &root).await;
}

// ─────────────────────────────────────────────────────────
// Directories: prefixes non-empty, distinct, idempotent,
// namespace isolation
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn directory_properties() {
    let db = Database::new("fdb.cluster").unwrap();
    let root = format!("kvdb_test_{}", Uuid::new_v4());

    let dirs = Directories::open(&db, 0, &root).await.unwrap();

    // all 8 subspaces have non-empty prefixes
    for prefix in [
        dirs.meta.bytes(),
        dirs.obj.bytes(),
        dirs.hash.bytes(),
        dirs.set.bytes(),
        dirs.zset.bytes(),
        dirs.zset_idx.bytes(),
        dirs.list.bytes(),
        dirs.expire.bytes(),
    ] {
        assert!(!prefix.is_empty(), "subspace has empty prefix");
    }

    // all 8 are distinct
    let prefixes: Vec<Vec<u8>> = vec![
        dirs.meta.bytes().to_vec(),
        dirs.obj.bytes().to_vec(),
        dirs.hash.bytes().to_vec(),
        dirs.set.bytes().to_vec(),
        dirs.zset.bytes().to_vec(),
        dirs.zset_idx.bytes().to_vec(),
        dirs.list.bytes().to_vec(),
        dirs.expire.bytes().to_vec(),
    ];
    let unique: HashSet<&Vec<u8>> = prefixes.iter().collect();
    assert_eq!(unique.len(), 8, "subspace prefixes not all distinct");

    // reopening returns the same prefixes (idempotent)
    let dirs2 = Directories::open(&db, 0, &root).await.unwrap();
    assert_eq!(dirs.meta.bytes(), dirs2.meta.bytes(), "meta prefix changed on reopen");
    assert_eq!(dirs.obj.bytes(), dirs2.obj.bytes(), "obj prefix changed on reopen");
    assert_eq!(dirs.hash.bytes(), dirs2.hash.bytes(), "hash prefix changed on reopen");

    // different namespace → different prefixes
    let dirs_ns1 = Directories::open(&db, 1, &root).await.unwrap();
    assert_ne!(
        dirs.meta.bytes(),
        dirs_ns1.meta.bytes(),
        "ns0 and ns1 meta should differ"
    );
    assert_ne!(dirs.obj.bytes(), dirs_ns1.obj.bytes(), "ns0 and ns1 obj should differ");

    // cleanup
    let trx = db.inner().create_trx().unwrap();
    let dir_layer = DirectoryLayer::default();
    let _ = dir_layer.remove(&trx, &[root]).await;
    let _ = trx.commit().await;
}

// ─────────────────────────────────────────────────────────
// Transaction wrapper: write + read via run_transact
// ─────────────────────────────────────────────────────────

#[tokio::test]
async fn run_transact_write_then_read() {
    let (db, dirs, root) = setup().await;
    let key = b"trx_key";
    let data = b"trx_value";

    let d = dirs.clone();
    run_transact(&db, "test_write", |tr| {
        let d = d.clone();
        async move {
            let meta = ObjectMeta::new_string(1, data.len() as u64);
            meta.write(&tr, &d, key)
                .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))?;
            chunking::write_chunks(&tr, &d, key, data);
            Ok(())
        }
    })
    .await
    .unwrap();

    let d = dirs.clone();
    let result = run_transact(&db, "test_read", |tr| {
        let d = d.clone();
        async move {
            let meta = ObjectMeta::read(&tr, &d, key, 0, false)
                .await
                .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))?;
            let meta = meta.expect("key should exist");
            chunking::read_chunks(&tr, &d, key, meta.num_chunks, false)
                .await
                .map_err(|e| foundationdb::FdbBindingError::CustomError(Box::new(e)))
        }
    })
    .await
    .unwrap();

    assert_eq!(result, data);

    cleanup(&db, &root).await;
}
