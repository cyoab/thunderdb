//! Integration tests for Thunder database.
//! These tests only use the public API.

#![allow(clippy::drop_non_drop)] // Explicit drops for test clarity

use std::fs;
use thunder::{Database, Error};

fn test_db_path(name: &str) -> String {
    format!("/tmp/thunder_integration_test_{name}.db")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

// ==================== Core Database Operations ====================

#[test]
fn test_database_open_and_crud() {
    let path = test_db_path("core_crud");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Write multiple keys
    {
        let mut wtx = db.write_tx();
        wtx.put(b"key1", b"value1");
        wtx.put(b"key2", b"value2");
        wtx.put(b"key3", b"value3");
        wtx.commit().expect("commit should succeed");
    }

    // Read and verify
    {
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(rtx.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(rtx.get(b"key3"), Some(b"value3".to_vec()));
        assert!(rtx.get(b"nonexistent").is_none());
    }

    // Update and delete
    {
        let mut wtx = db.write_tx();
        wtx.put(b"key1", b"updated");
        wtx.delete(b"key2");
        wtx.commit().expect("commit should succeed");
    }

    // Verify updates
    {
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key1"), Some(b"updated".to_vec()));
        assert!(rtx.get(b"key2").is_none());
        assert_eq!(rtx.get(b"key3"), Some(b"value3".to_vec()));
    }

    cleanup(&path);
}

#[test]
fn test_persistence_across_reopen() {
    let path = test_db_path("persistence");
    cleanup(&path);

    // Create and populate database
    {
        let mut db = Database::open(&path).expect("open should succeed");
        let mut wtx = db.write_tx();
        for i in 0..100 {
            wtx.put(
                format!("key_{i:03}").as_bytes(),
                format!("val_{i}").as_bytes(),
            );
        }
        wtx.commit().expect("commit should succeed");
    }

    // Reopen and verify
    {
        let db = Database::open(&path).expect("reopen should succeed");
        let rtx = db.read_tx();
        for i in 0..100 {
            let key = format!("key_{i:03}");
            let expected = format!("val_{i}");
            assert_eq!(rtx.get(key.as_bytes()), Some(expected.into_bytes()));
        }
    }

    cleanup(&path);
}

// ==================== Copy-on-Write (COW) Tests ====================

#[test]
fn test_cow_rollback_on_drop() {
    let path = test_db_path("cow_rollback");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open");

    // Commit initial data
    {
        let mut wtx = db.write_tx();
        wtx.put(b"existing", b"original");
        wtx.commit().expect("commit");
    }

    // Start tx, modify, but don't commit (rollback)
    {
        let mut wtx = db.write_tx();
        wtx.put(b"new_key", b"new_value");
        wtx.delete(b"existing");
        wtx.put(b"existing", b"modified");
        // Drop without commit
    }

    // Verify original state preserved
    let rtx = db.read_tx();
    assert_eq!(rtx.get(b"existing"), Some(b"original".to_vec()));
    assert!(rtx.get(b"new_key").is_none());

    cleanup(&path);
}

#[test]
fn test_cow_meta_page_alternation() {
    let path = test_db_path("cow_meta");
    cleanup(&path);

    // Multiple commits should alternate meta pages
    {
        let mut db = Database::open(&path).expect("open");
        for i in 0..10 {
            let mut wtx = db.write_tx();
            wtx.put(
                format!("tx_{i}").as_bytes(),
                format!("data_{i}").as_bytes(),
            );
            wtx.commit().expect("commit");
        }
    }

    // Reopen and verify all transactions persisted
    {
        let db = Database::open(&path).expect("reopen");
        let rtx = db.read_tx();
        for i in 0..10 {
            assert_eq!(
                rtx.get(format!("tx_{i}").as_bytes()),
                Some(format!("data_{i}").into_bytes())
            );
        }
    }

    cleanup(&path);
}

// ==================== Page Split Tests ====================

#[test]
fn test_page_split_persistence() {
    let path = test_db_path("split_persist");
    cleanup(&path);

    // Insert enough keys to cause page splits (LEAF_MAX_KEYS = 32)
    let count = 32 * 3;

    // Insert enough to cause splits
    {
        let mut db = Database::open(&path).expect("open");
        let mut wtx = db.write_tx();
        for i in 0..count {
            wtx.put(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes());
        }
        wtx.commit().expect("commit");
    }

    // Reopen and verify all data survived
    {
        let db = Database::open(&path).expect("reopen");
        let rtx = db.read_tx();
        for i in 0..count {
            let key = format!("k{i:05}");
            let expected = format!("v{i}");
            assert_eq!(rtx.get(key.as_bytes()), Some(expected.into_bytes()));
        }
    }

    cleanup(&path);
}

// ==================== Bucket Tests ====================

#[test]
fn test_bucket_crud() {
    let path = test_db_path("bucket_crud");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Create bucket and add data
    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"users").unwrap();
        wtx.bucket_put(b"users", b"alice", b"admin").unwrap();
        wtx.bucket_put(b"users", b"bob", b"member").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    // Read bucket data
    {
        let rtx = db.read_tx();
        let bucket = rtx.bucket(b"users").unwrap();
        assert_eq!(bucket.get(b"alice"), Some(&b"admin"[..]));
        assert_eq!(bucket.get(b"bob"), Some(&b"member"[..]));
        assert!(bucket.get(b"charlie").is_none());
    }

    // Delete bucket
    {
        let mut wtx = db.write_tx();
        wtx.delete_bucket(b"users").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    // Verify bucket gone
    {
        let rtx = db.read_tx();
        assert!(!rtx.bucket_exists(b"users"));
    }

    cleanup(&path);
}

#[test]
fn test_bucket_isolation() {
    let path = test_db_path("bucket_isolation");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"bucket_a").unwrap();
        wtx.create_bucket(b"bucket_b").unwrap();
        wtx.bucket_put(b"bucket_a", b"key", b"value_a").unwrap();
        wtx.bucket_put(b"bucket_b", b"key", b"value_b").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    {
        let rtx = db.read_tx();
        let a = rtx.bucket(b"bucket_a").unwrap();
        let b = rtx.bucket(b"bucket_b").unwrap();
        assert_eq!(a.get(b"key"), Some(&b"value_a"[..]));
        assert_eq!(b.get(b"key"), Some(&b"value_b"[..]));
    }

    cleanup(&path);
}

#[test]
fn test_bucket_errors() {
    let path = test_db_path("bucket_errors");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();

        // Invalid empty name
        assert!(matches!(
            wtx.create_bucket(b"").unwrap_err(),
            Error::InvalidBucketName { .. }
        ));

        // Create bucket
        wtx.create_bucket(b"test").unwrap();

        // Duplicate bucket
        assert!(matches!(
            wtx.create_bucket(b"test").unwrap_err(),
            Error::BucketAlreadyExists { .. }
        ));

        // Access nonexistent bucket
        assert!(matches!(
            wtx.bucket_put(b"nonexistent", b"k", b"v").unwrap_err(),
            Error::BucketNotFound { .. }
        ));
    }

    cleanup(&path);
}

// ==================== Iterator Tests ====================

#[test]
fn test_iter_sorted_order() {
    let path = test_db_path("iter_sorted");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.put(b"cherry", b"3");
        wtx.put(b"apple", b"1");
        wtx.put(b"banana", b"2");
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    let items: Vec<_> = rtx.iter().collect();

    assert_eq!(items.len(), 3);
    assert_eq!(items[0].0, b"apple");
    assert_eq!(items[1].0, b"banana");
    assert_eq!(items[2].0, b"cherry");

    cleanup(&path);
}

#[test]
fn test_iter_many_keys() {
    let path = test_db_path("iter_many");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let count = 500;
    {
        let mut wtx = db.write_tx();
        for i in 0..count {
            wtx.put(
                format!("key_{i:05}").as_bytes(),
                format!("val_{i}").as_bytes(),
            );
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    let items: Vec<_> = rtx.iter().collect();

    assert_eq!(items.len(), count);
    for i in 1..items.len() {
        assert!(items[i - 1].0 < items[i].0);
    }

    cleanup(&path);
}

// ==================== Range Query Tests ====================

#[test]
fn test_range_queries() {
    let path = test_db_path("range_queries");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        for i in 0..100 {
            wtx.put(
                format!("key_{i:03}").as_bytes(),
                format!("val_{i}").as_bytes(),
            );
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Inclusive range
    let items: Vec<_> = rtx
        .range(b"key_020".as_slice()..=b"key_030".as_slice())
        .collect();
    assert_eq!(items.len(), 11);
    assert_eq!(items[0].0, b"key_020");
    assert_eq!(items[10].0, b"key_030");

    // Exclusive end
    let items: Vec<_> = rtx
        .range(b"key_050".as_slice()..b"key_055".as_slice())
        .collect();
    assert_eq!(items.len(), 5);

    // Unbounded start
    let items: Vec<_> = rtx.range(..b"key_005".as_slice()).collect();
    assert_eq!(items.len(), 5);

    cleanup(&path);
}

#[test]
fn test_bucket_range() {
    let path = test_db_path("bucket_range");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"users").unwrap();
        wtx.bucket_put(b"users", b"admin:alice", b"a").unwrap();
        wtx.bucket_put(b"users", b"admin:bob", b"b").unwrap();
        wtx.bucket_put(b"users", b"guest:charlie", b"c").unwrap();
        wtx.bucket_put(b"users", b"guest:david", b"d").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    let bucket = rtx.bucket(b"users").unwrap();

    // Scan admin users only
    let admins: Vec<_> = bucket
        .range(b"admin:".as_slice()..b"admin;".as_slice())
        .collect();
    assert_eq!(admins.len(), 2);
    assert_eq!(admins[0].0, b"admin:alice");
    assert_eq!(admins[1].0, b"admin:bob");

    cleanup(&path);
}

// ==================== Large Data Tests ====================

#[test]
fn test_large_values() {
    let path = test_db_path("large_values");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let large_value = vec![0xAB; 100_000]; // 100KB

    {
        let mut wtx = db.write_tx();
        wtx.put(b"large", &large_value);
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    assert_eq!(rtx.get(b"large"), Some(large_value));

    cleanup(&path);
}

#[test]
fn test_many_transactions() {
    let path = test_db_path("many_tx");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    for i in 0..100 {
        let mut wtx = db.write_tx();
        wtx.put(
            format!("tx_{i}").as_bytes(),
            format!("data_{i}").as_bytes(),
        );
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    for i in 0..100 {
        assert_eq!(
            rtx.get(format!("tx_{i}").as_bytes()),
            Some(format!("data_{i}").into_bytes())
        );
    }

    cleanup(&path);
}

// ==================== Edge Cases ====================

#[test]
fn test_binary_keys_and_values() {
    let path = test_db_path("binary_data");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let binary_key = vec![0x00, 0xFF, 0x7F, 0x80, 0x01];
    let binary_value = vec![0xDE, 0xAD, 0xBE, 0xEF];

    {
        let mut wtx = db.write_tx();
        wtx.put(&binary_key, &binary_value);
        wtx.put(b"", b"empty_key_value"); // Empty key
        wtx.put(b"empty_val", b""); // Empty value
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    assert_eq!(rtx.get(&binary_key), Some(binary_value));
    assert_eq!(rtx.get(b""), Some(b"empty_key_value".to_vec()));
    assert_eq!(rtx.get(b"empty_val"), Some(b"".to_vec()));

    cleanup(&path);
}

#[test]
fn test_interleaved_operations() {
    let path = test_db_path("interleaved");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert
    {
        let mut wtx = db.write_tx();
        for i in 0..50 {
            wtx.put(format!("key_{i}").as_bytes(), b"initial");
        }
        wtx.commit().expect("commit should succeed");
    }

    // Update even, delete divisible by 5
    {
        let mut wtx = db.write_tx();
        for i in (0..50).step_by(2) {
            wtx.put(format!("key_{i}").as_bytes(), b"updated");
        }
        for i in (0..50).step_by(5) {
            wtx.delete(format!("key_{i}").as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    // Verify
    let rtx = db.read_tx();
    for i in 0..50 {
        let key = format!("key_{i}");
        if i % 5 == 0 {
            assert!(rtx.get(key.as_bytes()).is_none());
        } else if i % 2 == 0 {
            assert_eq!(rtx.get(key.as_bytes()), Some(b"updated".to_vec()));
        } else {
            assert_eq!(rtx.get(key.as_bytes()), Some(b"initial".to_vec()));
        }
    }

    cleanup(&path);
}

// ==================== Stress & Robustness Tests ====================

/// Stress test: High-volume sequential writes.
/// Inserts 1,000,000 keys in a single transaction to stress the B+ tree splitting logic.
#[test]
fn stress_high_volume_sequential_writes() {
    let path = test_db_path("stress_high_volume");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let key_count = 1_000_000;

    // Insert 1 million keys in a single transaction
    {
        let mut wtx = db.write_tx();
        for i in 0..key_count {
            let key = format!("key_{i:08}");
            let value = format!("value_{i:08}_padding_to_make_value_larger");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    // Verify via sampling (checking all 1M would be slow)
    {
        let rtx = db.read_tx();
        // Check first, last, and random samples
        for i in [0, 1, 100, 1000, 10000, 100000, 500000, 999998, 999999].iter() {
            let key = format!("key_{i:08}");
            let expected = format!("value_{i:08}_padding_to_make_value_larger");
            assert_eq!(
                rtx.get(key.as_bytes()),
                Some(expected.into_bytes()),
                "key {key} missing or corrupted"
            );
        }

        // Verify count via iterator
        let count = rtx.iter().count();
        assert_eq!(count, key_count, "iterator count mismatch");
    }

    cleanup(&path);
}

/// Stress test: Rapid transaction churn.
/// Tests database stability under 10,000 rapid commit cycles.
#[test]
fn stress_rapid_transaction_churn() {
    let path = test_db_path("stress_tx_churn");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let tx_count = 10_000;

    // Rapid fire transactions
    for i in 0..tx_count {
        let mut wtx = db.write_tx();
        // Each transaction does multiple operations
        wtx.put(format!("rapid_{i:06}").as_bytes(), format!("v{i}").as_bytes());
        wtx.put(b"counter", format!("{i}").as_bytes());
        // Keep some keys, delete others to create churn
        if i > 10 && i % 3 == 0 {
            wtx.delete(format!("rapid_{:06}", i - 10).as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    // Verify final state
    let rtx = db.read_tx();
    assert_eq!(
        rtx.get(b"counter"),
        Some(format!("{}", tx_count - 1).into_bytes())
    );
    // Last rapid key should exist
    assert!(rtx.get(format!("rapid_{:06}", tx_count - 1).as_bytes()).is_some());

    cleanup(&path);
}

/// Robustness test: Crash recovery simulation.
/// Writes data, drops database handle abruptly, reopens and verifies integrity.
#[test]
fn robustness_crash_recovery_simulation() {
    let path = test_db_path("crash_recovery");
    cleanup(&path);

    let committed_keys: Vec<String> = (0..100).map(|i| format!("committed_{i:03}")).collect();

    // Phase 1: Write committed data
    {
        let mut db = Database::open(&path).expect("open should succeed");
        let mut wtx = db.write_tx();
        for key in &committed_keys {
            wtx.put(key.as_bytes(), b"committed_value");
        }
        wtx.commit().expect("commit should succeed");
    }

    // Phase 2: Write uncommitted data and "crash" (drop without commit)
    {
        let mut db = Database::open(&path).expect("reopen should succeed");
        let mut wtx = db.write_tx();
        for i in 0..50 {
            wtx.put(format!("uncommitted_{i}").as_bytes(), b"should_not_exist");
        }
        // Simulate crash: drop wtx without commit
        drop(wtx);
        // Drop db handle abruptly
        drop(db);
    }

    // Phase 3: Recover and verify only committed data exists
    {
        let db = Database::open(&path).expect("recovery open should succeed");
        let rtx = db.read_tx();

        // All committed keys must exist
        for key in &committed_keys {
            assert_eq!(
                rtx.get(key.as_bytes()),
                Some(b"committed_value".to_vec()),
                "committed key {key} missing after recovery"
            );
        }

        // No uncommitted keys should exist
        for i in 0..50 {
            assert!(
                rtx.get(format!("uncommitted_{i}").as_bytes()).is_none(),
                "uncommitted key should not exist after recovery"
            );
        }
    }

    cleanup(&path);
}

/// Robustness test: Repeated open/close cycles.
/// Opens and closes the database 500 times with writes to detect resource leaks.
#[test]
fn robustness_repeated_open_close_cycles() {
    let path = test_db_path("open_close_cycles");
    cleanup(&path);

    let cycles = 500;

    for cycle in 0..cycles {
        let mut db = Database::open(&path).expect("open should succeed");

        // Write in this cycle
        {
            let mut wtx = db.write_tx();
            wtx.put(
                format!("cycle_{cycle:03}").as_bytes(),
                format!("data_{cycle}").as_bytes(),
            );
            wtx.put(b"latest_cycle", format!("{cycle}").as_bytes());
            wtx.commit().expect("commit should succeed");
        }

        // Verify all previous cycles' data persists
        {
            let rtx = db.read_tx();
            for prev in 0..=cycle {
                let key = format!("cycle_{prev:03}");
                assert!(
                    rtx.get(key.as_bytes()).is_some(),
                    "cycle {prev} data missing at cycle {cycle}"
                );
            }
        }

        drop(db);
    }

    // Final verification after all cycles
    let db = Database::open(&path).expect("final open should succeed");
    let rtx = db.read_tx();
    let count = rtx.iter().count();
    // cycles keys + 1 "latest_cycle" key
    assert_eq!(count, cycles + 1, "final key count mismatch");

    cleanup(&path);
}

/// Correctness test: Data integrity under heavy churn.
/// Performs 500,000 random insert/update/delete operations and verifies final state.
#[test]
fn correctness_data_integrity_heavy_churn() {
    let path = test_db_path("integrity_churn");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Use a simple PRNG for deterministic "random" operations
    let mut state: u64 = 12345;
    let next_rand = |s: &mut u64| -> u64 {
        *s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        *s >> 33
    };

    // Track expected state in memory
    let mut expected: std::collections::HashMap<String, Vec<u8>> =
        std::collections::HashMap::new();

    let operations = 500_000;
    let key_space = 10_000; // Keys 0-9999

    {
        let mut wtx = db.write_tx();
        for _ in 0..operations {
            let key_id = (next_rand(&mut state) % key_space) as usize;
            let key = format!("key_{key_id:04}");
            let op = next_rand(&mut state) % 3;

            match op {
                0 | 1 => {
                    // Insert/Update (2/3 probability)
                    let value = format!("val_{}", next_rand(&mut state));
                    wtx.put(key.as_bytes(), value.as_bytes());
                    expected.insert(key, value.into_bytes());
                }
                _ => {
                    // Delete (1/3 probability)
                    wtx.delete(key.as_bytes());
                    expected.remove(&key);
                }
            }
        }
        wtx.commit().expect("commit should succeed");
    }

    // Verify database matches expected state
    let rtx = db.read_tx();
    for (key, value) in &expected {
        assert_eq!(
            rtx.get(key.as_bytes()),
            Some(value.clone()),
            "key {key} value mismatch"
        );
    }

    // Verify no extra keys exist
    let db_count = rtx.iter().count();
    assert_eq!(db_count, expected.len(), "key count mismatch");

    cleanup(&path);
}

/// Correctness test: Lexicographic ordering under stress.
/// Inserts 100,000 keys in random order and verifies iterator returns them sorted.
#[test]
fn correctness_ordering_guarantee() {
    let path = test_db_path("ordering_stress");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Generate keys that will have interesting sort orders
    let mut keys: Vec<String> = Vec::new();
    for i in 0..50_000 {
        keys.push(format!("{i:06}")); // Numeric strings
    }
    for c in b'A'..=b'Z' {
        for i in 0..1000 {
            keys.push(format!("{}{i:04}", c as char));
        }
    }
    for i in 0..24_000 {
        keys.push(format!("prefix_{i:05}_suffix"));
    }
    // Add edge cases - use raw bytes for non-UTF8 keys
    let mut raw_keys: Vec<Vec<u8>> = keys.iter().map(|s| s.as_bytes().to_vec()).collect();
    raw_keys.push(vec![]); // Empty key
    raw_keys.push(vec![0x00, 0x00]); // Null bytes
    raw_keys.push(vec![0xFF, 0xFF]); // High bytes

    // Shuffle using deterministic PRNG
    let mut state: u64 = 98765;
    for i in (1..raw_keys.len()).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (state >> 33) as usize % (i + 1);
        raw_keys.swap(i, j);
    }

    // Insert in shuffled order
    {
        let mut wtx = db.write_tx();
        for key in &raw_keys {
            wtx.put(key, b"v");
        }
        wtx.commit().expect("commit should succeed");
    }

    // Verify iterator returns sorted order
    let rtx = db.read_tx();
    let mut prev: Option<Vec<u8>> = None;
    for (key, _) in rtx.iter() {
        if let Some(p) = &prev {
            assert!(
                p.as_slice() < key,
                "ordering violation: {:?} should be < {:?}",
                p,
                key
            );
        }
        prev = Some(key.to_vec());
    }

    cleanup(&path);
}

/// Stress test: Large values up to 100MB.
/// Tests handling of values from tiny to massive.
#[test]
fn stress_large_values() {
    let path = test_db_path("large_values_stress");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let sizes = [
        1,              // Tiny
        100,            // Small
        32768,          // Page size
        32769,          // Just over page
        65_536,         // 64KB
        1_000_000,      // 1MB
        10_000_000,     // 10MB
        50_000_000,     // 50MB
        100_000_000,    // 100MB
    ];

    // Insert values of various sizes
    {
        let mut wtx = db.write_tx();
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("size_{size}");
            // Create value with verifiable pattern
            let value: Vec<u8> = (0..size).map(|j| ((i + j) % 256) as u8).collect();
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("commit should succeed");
    }

    // Verify all values
    {
        let rtx = db.read_tx();
        for (i, &size) in sizes.iter().enumerate() {
            let key = format!("size_{size}");
            let expected: Vec<u8> = (0..size).map(|j| ((i + j) % 256) as u8).collect();
            let actual = rtx.get(key.as_bytes());
            assert!(actual.is_some(), "key {key} missing");
            assert_eq!(actual.unwrap(), expected, "key {key} value corrupted");
        }
    }

    // Test persistence of large values
    drop(db);
    let db = Database::open(&path).expect("reopen should succeed");
    let rtx = db.read_tx();
    for (i, &size) in sizes.iter().enumerate() {
        let key = format!("size_{size}");
        let expected: Vec<u8> = (0..size).map(|j| ((i + j) % 256) as u8).collect();
        let actual = rtx.get(key.as_bytes());
        assert_eq!(
            actual,
            Some(expected),
            "key {key} corrupted after reopen"
        );
    }

    cleanup(&path);
}

/// Robustness test: Bucket isolation under stress.
/// Creates 200 buckets with 500 keys each to verify isolation.
#[test]
fn robustness_bucket_isolation_stress() {
    let path = test_db_path("bucket_isolation_stress");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let bucket_count = 200;
    let keys_per_bucket = 500;

    // Create many buckets with same key names but different values
    {
        let mut wtx = db.write_tx();
        for b in 0..bucket_count {
            let bucket_name = format!("bucket_{b:03}");
            wtx.create_bucket(bucket_name.as_bytes()).unwrap();

            for k in 0..keys_per_bucket {
                let key = format!("key_{k:03}");
                let value = format!("bucket{b}_key{k}");
                wtx.bucket_put(bucket_name.as_bytes(), key.as_bytes(), value.as_bytes())
                    .unwrap();
            }
        }
        wtx.commit().expect("commit should succeed");
    }

    // Verify complete isolation
    {
        let rtx = db.read_tx();
        for b in 0..bucket_count {
            let bucket_name = format!("bucket_{b:03}");
            let bucket = rtx.bucket(bucket_name.as_bytes()).unwrap();

            // Verify this bucket has exactly the right keys with right values
            let mut count = 0;
            for (key, value) in bucket.iter() {
                let key_str = std::str::from_utf8(key).unwrap();
                let value_str = std::str::from_utf8(value).unwrap();
                let expected_prefix = format!("bucket{b}_");
                assert!(
                    value_str.starts_with(&expected_prefix),
                    "bucket {bucket_name} key {key_str} has wrong value {value_str}"
                );
                count += 1;
            }
            assert_eq!(
                count, keys_per_bucket,
                "bucket {bucket_name} has wrong key count"
            );
        }
    }

    cleanup(&path);
}

/// Correctness test: Transaction rollback preserves exact state.
/// Verifies that failed transactions leave no trace.
#[test]
fn correctness_rollback_leaves_no_trace() {
    let path = test_db_path("rollback_no_trace");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Establish baseline state
    {
        let mut wtx = db.write_tx();
        for i in 0..100 {
            wtx.put(format!("base_{i:03}").as_bytes(), b"original");
        }
        wtx.create_bucket(b"stable_bucket").unwrap();
        wtx.bucket_put(b"stable_bucket", b"bkey", b"bvalue").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    // Capture exact state
    let baseline_keys: Vec<(Vec<u8>, Vec<u8>)> = {
        let rtx = db.read_tx();
        rtx.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
    };

    // Perform many operations but rollback
    for _ in 0..10 {
        let mut wtx = db.write_tx();
        // Modify existing
        for i in 0..100 {
            wtx.put(format!("base_{i:03}").as_bytes(), b"modified");
        }
        // Add new
        for i in 0..100 {
            wtx.put(format!("new_{i:03}").as_bytes(), b"new_value");
        }
        // Delete existing
        for i in 0..50 {
            wtx.delete(format!("base_{i:03}").as_bytes());
        }
        // Bucket operations
        wtx.create_bucket(b"temp_bucket").unwrap();
        wtx.bucket_put(b"temp_bucket", b"tk", b"tv").unwrap();
        wtx.bucket_delete(b"stable_bucket", b"bkey").unwrap();
        // Rollback (drop without commit)
    }

    // Verify exact baseline state is preserved
    let rtx = db.read_tx();
    let current_keys: Vec<(Vec<u8>, Vec<u8>)> =
        rtx.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();

    assert_eq!(
        baseline_keys.len(),
        current_keys.len(),
        "key count changed after rollbacks"
    );
    for ((bk, bv), (ck, cv)) in baseline_keys.iter().zip(current_keys.iter()) {
        assert_eq!(bk, ck, "key mismatch after rollback");
        assert_eq!(bv, cv, "value mismatch after rollback");
    }

    // Verify bucket state
    let bucket = rtx.bucket(b"stable_bucket").unwrap();
    assert_eq!(bucket.get(b"bkey"), Some(&b"bvalue"[..]));
    assert!(!rtx.bucket_exists(b"temp_bucket"));

    cleanup(&path);
}

/// Stress test: Mixed workload simulation.
/// Simulates a realistic mixed read/write workload with 10,000 sessions.
#[test]
fn stress_mixed_workload_simulation() {
    let path = test_db_path("mixed_workload");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    let mut state: u64 = 54321;
    let next_rand = |s: &mut u64| -> u64 {
        *s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
        *s >> 33
    };

    // Tracking for verification
    let mut expected: std::collections::HashMap<String, Vec<u8>> =
        std::collections::HashMap::new();

    // Simulate 10,000 "sessions" of activity
    for session in 0..10_000 {
        let session_ops = (next_rand(&mut state) % 50) + 1; // 1-50 ops per session

        let mut wtx = db.write_tx();

        for _ in 0..session_ops {
            let op_type = next_rand(&mut state) % 10;
            let key_id = next_rand(&mut state) % 5000;
            let key = format!("wl_key_{key_id:05}");

            match op_type {
                0..=5 => {
                    // 60% writes
                    let value = format!("session{session}_rand{}", next_rand(&mut state));
                    wtx.put(key.as_bytes(), value.as_bytes());
                    expected.insert(key, value.into_bytes());
                }
                6..=7 => {
                    // 20% deletes
                    wtx.delete(key.as_bytes());
                    expected.remove(&key);
                }
                _ => {
                    // 20% no-op (simulates reads which don't modify state)
                }
            }
        }

        wtx.commit().expect("commit should succeed");

        // Periodic verification (every 500 sessions)
        if session % 500 == 0 {
            let rtx = db.read_tx();
            for (key, value) in &expected {
                assert_eq!(
                    rtx.get(key.as_bytes()),
                    Some(value.clone()),
                    "verification failed at session {session} for key {key}"
                );
            }
        }
    }

    // Final complete verification
    let rtx = db.read_tx();
    let db_count = rtx.iter().count();
    assert_eq!(db_count, expected.len(), "final count mismatch");

    for (key, value) in &expected {
        assert_eq!(
            rtx.get(key.as_bytes()),
            Some(value.clone()),
            "final verification failed for key {key}"
        );
    }

    // Verify persistence
    drop(rtx);
    drop(db);

    let db = Database::open(&path).expect("reopen should succeed");
    let rtx = db.read_tx();
    assert_eq!(rtx.iter().count(), expected.len(), "count mismatch after reopen");

    cleanup(&path);
}

// ==================== Nested Bucket Tests ====================

#[test]
fn test_nested_bucket_basic_crud() {
    let path = test_db_path("nested_bucket_crud");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Create parent bucket and nested bucket with data
    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"parent").unwrap();
        wtx.create_nested_bucket(b"parent", b"child").unwrap();

        // Put data in parent bucket
        wtx.bucket_put(b"parent", b"parent_key", b"parent_value").unwrap();

        // Put data in nested bucket
        wtx.nested_bucket_put(b"parent", b"child", b"child_key", b"child_value").unwrap();

        wtx.commit().expect("commit should succeed");
    }

    // Read and verify data isolation
    {
        let rtx = db.read_tx();

        // Verify parent bucket data
        let parent = rtx.bucket(b"parent").unwrap();
        assert_eq!(parent.get(b"parent_key"), Some(&b"parent_value"[..]));

        // Verify nested bucket data
        let child = rtx.nested_bucket(b"parent", b"child").unwrap();
        assert_eq!(child.get(b"child_key"), Some(&b"child_value"[..]));

        // Nested bucket key should not be visible in parent
        assert!(parent.get(b"child_key").is_none());
    }

    // Update and delete in nested bucket
    {
        let mut wtx = db.write_tx();
        wtx.nested_bucket_put(b"parent", b"child", b"child_key", b"updated_value").unwrap();
        wtx.nested_bucket_put(b"parent", b"child", b"another_key", b"another_value").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    // Verify updates
    {
        let rtx = db.read_tx();
        let child = rtx.nested_bucket(b"parent", b"child").unwrap();
        assert_eq!(child.get(b"child_key"), Some(&b"updated_value"[..]));
        assert_eq!(child.get(b"another_key"), Some(&b"another_value"[..]));
    }

    // Delete nested bucket
    {
        let mut wtx = db.write_tx();
        wtx.delete_nested_bucket(b"parent", b"child").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    // Verify nested bucket is gone but parent remains
    {
        let rtx = db.read_tx();
        assert!(rtx.bucket_exists(b"parent"));
        assert!(!rtx.nested_bucket_exists(b"parent", b"child"));

        let parent = rtx.bucket(b"parent").unwrap();
        assert_eq!(parent.get(b"parent_key"), Some(&b"parent_value"[..]));
    }

    cleanup(&path);
}

#[test]
fn test_nested_bucket_multi_level_hierarchy() {
    let path = test_db_path("nested_bucket_hierarchy");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Create a deep hierarchy: root -> level1 -> level2 -> level3
    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"root").unwrap();
        wtx.create_nested_bucket(b"root", b"level1").unwrap();

        // Create level2 under level1 (path: root/level1/level2)
        wtx.create_nested_bucket_at_path(&[b"root", b"level1"], b"level2").unwrap();

        // Create level3 under level2 (path: root/level1/level2/level3)
        wtx.create_nested_bucket_at_path(&[b"root", b"level1", b"level2"], b"level3").unwrap();

        // Insert data at each level
        wtx.bucket_put(b"root", b"key", b"root_value").unwrap();
        wtx.nested_bucket_put(b"root", b"level1", b"key", b"level1_value").unwrap();
        wtx.nested_bucket_put_at_path(&[b"root", b"level1", b"level2"], b"key", b"level2_value").unwrap();
        wtx.nested_bucket_put_at_path(&[b"root", b"level1", b"level2", b"level3"], b"key", b"level3_value").unwrap();

        wtx.commit().expect("commit should succeed");
    }

    // Verify data at each level is isolated
    {
        let rtx = db.read_tx();

        let root = rtx.bucket(b"root").unwrap();
        assert_eq!(root.get(b"key"), Some(&b"root_value"[..]));

        let level1 = rtx.nested_bucket(b"root", b"level1").unwrap();
        assert_eq!(level1.get(b"key"), Some(&b"level1_value"[..]));

        let level2 = rtx.nested_bucket_at_path(&[b"root", b"level1", b"level2"]).unwrap();
        assert_eq!(level2.get(b"key"), Some(&b"level2_value"[..]));

        let level3 = rtx.nested_bucket_at_path(&[b"root", b"level1", b"level2", b"level3"]).unwrap();
        assert_eq!(level3.get(b"key"), Some(&b"level3_value"[..]));
    }

    // Delete intermediate level (level2) should delete level3 as well
    {
        let mut wtx = db.write_tx();
        wtx.delete_nested_bucket_at_path(&[b"root", b"level1"], b"level2").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    // Verify level2 and level3 are gone, but root and level1 remain
    {
        let rtx = db.read_tx();

        assert!(rtx.bucket_exists(b"root"));
        assert!(rtx.nested_bucket_exists(b"root", b"level1"));
        assert!(!rtx.nested_bucket_exists_at_path(&[b"root", b"level1", b"level2"]));
        assert!(!rtx.nested_bucket_exists_at_path(&[b"root", b"level1", b"level2", b"level3"]));

        // Data at remaining levels should be intact
        let root = rtx.bucket(b"root").unwrap();
        assert_eq!(root.get(b"key"), Some(&b"root_value"[..]));

        let level1 = rtx.nested_bucket(b"root", b"level1").unwrap();
        assert_eq!(level1.get(b"key"), Some(&b"level1_value"[..]));
    }

    cleanup(&path);
}

#[test]
fn test_nested_bucket_sibling_isolation() {
    let path = test_db_path("nested_bucket_siblings");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Create multiple sibling nested buckets under same parent
    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"parent").unwrap();
        wtx.create_nested_bucket(b"parent", b"child_a").unwrap();
        wtx.create_nested_bucket(b"parent", b"child_b").unwrap();
        wtx.create_nested_bucket(b"parent", b"child_c").unwrap();

        // Same key in each sibling bucket with different values
        wtx.nested_bucket_put(b"parent", b"child_a", b"shared_key", b"value_a").unwrap();
        wtx.nested_bucket_put(b"parent", b"child_b", b"shared_key", b"value_b").unwrap();
        wtx.nested_bucket_put(b"parent", b"child_c", b"shared_key", b"value_c").unwrap();

        wtx.commit().expect("commit should succeed");
    }

    // Verify each sibling has its own isolated value
    {
        let rtx = db.read_tx();

        let child_a = rtx.nested_bucket(b"parent", b"child_a").unwrap();
        let child_b = rtx.nested_bucket(b"parent", b"child_b").unwrap();
        let child_c = rtx.nested_bucket(b"parent", b"child_c").unwrap();

        assert_eq!(child_a.get(b"shared_key"), Some(&b"value_a"[..]));
        assert_eq!(child_b.get(b"shared_key"), Some(&b"value_b"[..]));
        assert_eq!(child_c.get(b"shared_key"), Some(&b"value_c"[..]));

        // Verify listing nested buckets
        let nested_buckets = rtx.list_nested_buckets(b"parent").unwrap();
        assert_eq!(nested_buckets.len(), 3);
        assert!(nested_buckets.contains(&b"child_a".to_vec()));
        assert!(nested_buckets.contains(&b"child_b".to_vec()));
        assert!(nested_buckets.contains(&b"child_c".to_vec()));
    }

    // Delete one sibling, others should remain
    {
        let mut wtx = db.write_tx();
        wtx.delete_nested_bucket(b"parent", b"child_b").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    // Verify deletion didn't affect siblings
    {
        let rtx = db.read_tx();

        assert!(rtx.nested_bucket_exists(b"parent", b"child_a"));
        assert!(!rtx.nested_bucket_exists(b"parent", b"child_b"));
        assert!(rtx.nested_bucket_exists(b"parent", b"child_c"));

        let child_a = rtx.nested_bucket(b"parent", b"child_a").unwrap();
        let child_c = rtx.nested_bucket(b"parent", b"child_c").unwrap();

        assert_eq!(child_a.get(b"shared_key"), Some(&b"value_a"[..]));
        assert_eq!(child_c.get(b"shared_key"), Some(&b"value_c"[..]));
    }

    cleanup(&path);
}

#[test]
fn test_nested_bucket_error_conditions() {
    let path = test_db_path("nested_bucket_errors");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.create_bucket(b"parent").unwrap();
        wtx.create_nested_bucket(b"parent", b"child").unwrap();

        // Error: Create nested bucket under non-existent parent
        assert!(matches!(
            wtx.create_nested_bucket(b"nonexistent", b"child").unwrap_err(),
            Error::BucketNotFound { .. }
        ));

        // Error: Create duplicate nested bucket
        assert!(matches!(
            wtx.create_nested_bucket(b"parent", b"child").unwrap_err(),
            Error::BucketAlreadyExists { .. }
        ));

        // Error: Invalid nested bucket name (empty)
        assert!(matches!(
            wtx.create_nested_bucket(b"parent", b"").unwrap_err(),
            Error::InvalidBucketName { .. }
        ));

        // Error: Access non-existent nested bucket
        assert!(matches!(
            wtx.nested_bucket_put(b"parent", b"nonexistent", b"k", b"v").unwrap_err(),
            Error::BucketNotFound { .. }
        ));

        // Error: Delete non-existent nested bucket
        assert!(matches!(
            wtx.delete_nested_bucket(b"parent", b"nonexistent").unwrap_err(),
            Error::BucketNotFound { .. }
        ));

        wtx.commit().expect("commit should succeed");
    }

    // Error: Try to access nested bucket after parent deletion
    {
        let mut wtx = db.write_tx();
        wtx.delete_bucket(b"parent").unwrap();
        wtx.commit().expect("commit should succeed");
    }

    {
        let rtx = db.read_tx();
        assert!(!rtx.bucket_exists(b"parent"));
        assert!(!rtx.nested_bucket_exists(b"parent", b"child"));
    }

    cleanup(&path);
}

#[test]
fn test_nested_bucket_persistence() {
    let path = test_db_path("nested_bucket_persist");
    cleanup(&path);

    // Create nested bucket structure and persist
    {
        let mut db = Database::open(&path).expect("open should succeed");
        let mut wtx = db.write_tx();

        wtx.create_bucket(b"config").unwrap();
        wtx.create_nested_bucket(b"config", b"network").unwrap();
        wtx.create_nested_bucket(b"config", b"storage").unwrap();

        wtx.bucket_put(b"config", b"version", b"1.0").unwrap();
        wtx.nested_bucket_put(b"config", b"network", b"host", b"localhost").unwrap();
        wtx.nested_bucket_put(b"config", b"network", b"port", b"8080").unwrap();
        wtx.nested_bucket_put(b"config", b"storage", b"path", b"/data").unwrap();

        // Create a deeper nesting
        wtx.create_nested_bucket_at_path(&[b"config", b"storage"], b"options").unwrap();
        wtx.nested_bucket_put_at_path(&[b"config", b"storage", b"options"], b"compress", b"true").unwrap();

        wtx.commit().expect("commit should succeed");
    }

    // Reopen database and verify all nested buckets and data persisted
    {
        let db = Database::open(&path).expect("reopen should succeed");
        let rtx = db.read_tx();

        // Check top-level bucket
        assert!(rtx.bucket_exists(b"config"));
        let config = rtx.bucket(b"config").unwrap();
        assert_eq!(config.get(b"version"), Some(&b"1.0"[..]));

        // Check nested buckets
        assert!(rtx.nested_bucket_exists(b"config", b"network"));
        assert!(rtx.nested_bucket_exists(b"config", b"storage"));

        let network = rtx.nested_bucket(b"config", b"network").unwrap();
        assert_eq!(network.get(b"host"), Some(&b"localhost"[..]));
        assert_eq!(network.get(b"port"), Some(&b"8080"[..]));

        let storage = rtx.nested_bucket(b"config", b"storage").unwrap();
        assert_eq!(storage.get(b"path"), Some(&b"/data"[..]));

        // Check deeply nested bucket
        assert!(rtx.nested_bucket_exists_at_path(&[b"config", b"storage", b"options"]));
        let options = rtx.nested_bucket_at_path(&[b"config", b"storage", b"options"]).unwrap();
        assert_eq!(options.get(b"compress"), Some(&b"true"[..]));

        // List nested buckets
        let config_children = rtx.list_nested_buckets(b"config").unwrap();
        assert_eq!(config_children.len(), 2);
        assert!(config_children.contains(&b"network".to_vec()));
        assert!(config_children.contains(&b"storage".to_vec()));
    }

    cleanup(&path);
}
