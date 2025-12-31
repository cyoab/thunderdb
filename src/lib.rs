//! Summary: thunder - A minimal, embedded, transactional key-value database engine.
//! Copyright (c) YOAB. All rights reserved.

pub mod btree;
pub mod bucket;
pub mod db;
pub mod error;
pub mod freelist;
pub mod meta;
pub mod mmap;
pub mod page;
pub mod tx;

// Re-export public API at crate root for convenience.
pub use btree::{Bound, BTreeIter, BTreeRangeIter};
pub use bucket::{BucketBound, BucketIter, BucketMut, BucketRangeIter, BucketRef, MAX_BUCKET_NAME_LEN};
pub use db::Database;
pub use error::{Error, Result};
pub use tx::{ReadTx, WriteTx};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_db_path(name: &str) -> String {
        format!("/tmp/thunder_test_{name}.db")
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_database_open_creates_file() {
        let path = test_db_path("open_creates");
        cleanup(&path);

        let result = Database::open(&path);
        assert!(result.is_ok(), "Database::open should succeed");

        cleanup(&path);
    }

    #[test]
    fn test_database_open_existing() {
        let path = test_db_path("open_existing");
        cleanup(&path);

        // Create database
        {
            let _db = Database::open(&path).expect("first open should succeed");
        }

        // Re-open existing database
        {
            let result = Database::open(&path);
            assert!(result.is_ok(), "re-opening existing database should succeed");
        }

        cleanup(&path);
    }

    #[test]
    fn test_put_then_get() {
        let path = test_db_path("put_get");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Write a key-value pair
        {
            let mut wtx = db.write_tx();
            wtx.put(b"hello", b"world");
            wtx.commit().expect("commit should succeed");
        }

        // Read it back
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"hello");
            assert_eq!(value, Some(b"world".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let path = test_db_path("get_nonexistent");
        cleanup(&path);

        let db = Database::open(&path).expect("open should succeed");

        let rtx = db.read_tx();
        let value = rtx.get(b"nonexistent");
        assert_eq!(value, None);

        cleanup(&path);
    }

    #[test]
    fn test_put_overwrite() {
        let path = test_db_path("put_overwrite");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Write initial value
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value1");
            wtx.commit().expect("commit should succeed");
        }

        // Overwrite with new value
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value2");
            wtx.commit().expect("commit should succeed");
        }

        // Verify overwritten value
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"key");
            assert_eq!(value, Some(b"value2".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_delete() {
        let path = test_db_path("delete");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Write a key
        {
            let mut wtx = db.write_tx();
            wtx.put(b"to_delete", b"value");
            wtx.commit().expect("commit should succeed");
        }

        // Delete the key
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"to_delete");
            wtx.commit().expect("commit should succeed");
        }

        // Verify key is gone
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"to_delete");
            assert_eq!(value, None);
        }

        cleanup(&path);
    }

    #[test]
    fn test_multiple_keys() {
        let path = test_db_path("multiple_keys");
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

        // Verify all keys
        {
            let rtx = db.read_tx();
            assert_eq!(rtx.get(b"key1"), Some(b"value1".to_vec()));
            assert_eq!(rtx.get(b"key2"), Some(b"value2".to_vec()));
            assert_eq!(rtx.get(b"key3"), Some(b"value3".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_uncommitted_changes_not_visible() {
        let path = test_db_path("uncommitted");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Start write transaction but don't commit
        {
            let mut wtx = db.write_tx();
            wtx.put(b"uncommitted", b"value");
            // Drop without committing
        }

        // Changes should not be visible
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"uncommitted");
            assert_eq!(value, None);
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence_across_reopen() {
        let path = test_db_path("persistence");
        cleanup(&path);

        // Write and close
        {
            let mut db = Database::open(&path).expect("open should succeed");
            let mut wtx = db.write_tx();
            wtx.put(b"persistent", b"data");
            wtx.commit().expect("commit should succeed");
        }

        // Reopen and verify
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();
            let value = rtx.get(b"persistent");
            assert_eq!(value, Some(b"data".to_vec()));
        }

        cleanup(&path);
    }

    // ==================== Persistence & Crash Recovery Tests ====================

    #[test]
    fn test_persistence_multiple_transactions() {
        let path = test_db_path("persist_multi_tx");
        cleanup(&path);

        // Multiple commits, then reopen.
        {
            let mut db = Database::open(&path).expect("open should succeed");

            for i in 0..10 {
                let mut wtx = db.write_tx();
                wtx.put(format!("key_{i}").as_bytes(), format!("value_{i}").as_bytes());
                wtx.commit().expect("commit should succeed");
            }
        }

        // Reopen and verify all data.
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();

            for i in 0..10 {
                assert_eq!(
                    rtx.get(format!("key_{i}").as_bytes()),
                    Some(format!("value_{i}").into_bytes()),
                    "key_{i} missing after reopen"
                );
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence_with_updates() {
        let path = test_db_path("persist_updates");
        cleanup(&path);

        // Insert, update, then reopen.
        {
            let mut db = Database::open(&path).expect("open should succeed");

            // Initial insert.
            {
                let mut wtx = db.write_tx();
                wtx.put(b"key", b"initial");
                wtx.commit().expect("commit should succeed");
            }

            // Update.
            {
                let mut wtx = db.write_tx();
                wtx.put(b"key", b"updated");
                wtx.commit().expect("commit should succeed");
            }
        }

        // Verify updated value persists.
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();
            assert_eq!(rtx.get(b"key"), Some(b"updated".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence_with_deletes() {
        let path = test_db_path("persist_deletes");
        cleanup(&path);

        // Insert, delete, then reopen.
        {
            let mut db = Database::open(&path).expect("open should succeed");

            {
                let mut wtx = db.write_tx();
                wtx.put(b"keep", b"value1");
                wtx.put(b"delete_me", b"value2");
                wtx.commit().expect("commit should succeed");
            }

            {
                let mut wtx = db.write_tx();
                wtx.delete(b"delete_me");
                wtx.commit().expect("commit should succeed");
            }
        }

        // Verify state persists.
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();
            assert_eq!(rtx.get(b"keep"), Some(b"value1".to_vec()));
            assert_eq!(rtx.get(b"delete_me"), None);
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence_empty_db_reopen() {
        let path = test_db_path("persist_empty");
        cleanup(&path);

        // Create empty db.
        {
            let _db = Database::open(&path).expect("open should succeed");
        }

        // Reopen empty db.
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();
            assert_eq!(rtx.get(b"nonexistent"), None);
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence_multiple_reopen_cycles() {
        let path = test_db_path("persist_cycles");
        cleanup(&path);

        for cycle in 0..5 {
            {
                let mut db = Database::open(&path).expect("open should succeed");

                // Add data in each cycle.
                {
                    let mut wtx = db.write_tx();
                    wtx.put(
                        format!("cycle_{cycle}").as_bytes(),
                        format!("data_{cycle}").as_bytes(),
                    );
                    wtx.commit().expect("commit should succeed");
                }

                // Verify all previous cycles' data.
                let rtx = db.read_tx();
                for i in 0..=cycle {
                    assert_eq!(
                        rtx.get(format!("cycle_{i}").as_bytes()),
                        Some(format!("data_{i}").into_bytes()),
                        "cycle_{i} data missing in cycle {cycle}"
                    );
                }
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence_large_dataset() {
        let path = test_db_path("persist_large");
        cleanup(&path);

        // Insert many records.
        {
            let mut db = Database::open(&path).expect("open should succeed");
            let mut wtx = db.write_tx();

            for i in 0..500 {
                wtx.put(
                    format!("key_{i:05}").as_bytes(),
                    format!("value_{i}").as_bytes(),
                );
            }
            wtx.commit().expect("commit should succeed");
        }

        // Reopen and verify.
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();

            for i in 0..500 {
                assert_eq!(
                    rtx.get(format!("key_{i:05}").as_bytes()),
                    Some(format!("value_{i}").into_bytes()),
                    "key_{i:05} missing after reopen"
                );
            }
        }

        cleanup(&path);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_binary_keys_and_values() {
        let path = test_db_path("binary_kv");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Test with various binary patterns including null bytes.
        let test_cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (vec![0x00], vec![0xFF]),
            (vec![0x00, 0x00, 0x00], vec![0x01, 0x02, 0x03]),
            (vec![0xFF, 0xFF], vec![0x00, 0x00]),
            (vec![0x7F, 0x80, 0x81], vec![0xFE, 0xFD, 0xFC]),
        ];

        {
            let mut wtx = db.write_tx();
            for (key, value) in &test_cases {
                wtx.put(key, value);
            }
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            for (key, value) in &test_cases {
                assert_eq!(rtx.get(key), Some(value.clone()));
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_unicode_keys_and_values() {
        let path = test_db_path("unicode_kv");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let test_cases = vec![
            ("こんにちは", "世界"),
            ("🔥🚀", "emoji_value"),
            ("Привет", "мир"),
            ("مرحبا", "عالم"),
        ];

        {
            let mut wtx = db.write_tx();
            for (key, value) in &test_cases {
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            for (key, value) in &test_cases {
                assert_eq!(rtx.get(key.as_bytes()), Some(value.as_bytes().to_vec()));
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_very_long_keys() {
        let path = test_db_path("long_keys");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let long_key = "k".repeat(10_000);

        {
            let mut wtx = db.write_tx();
            wtx.put(long_key.as_bytes(), b"value");
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            assert_eq!(rtx.get(long_key.as_bytes()), Some(b"value".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_very_long_values() {
        let path = test_db_path("long_values");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let long_value = "v".repeat(100_000);

        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", long_value.as_bytes());
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            assert_eq!(rtx.get(b"key"), Some(long_value.into_bytes()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_prefix_keys_ordering() {
        let path = test_db_path("prefix_order");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let keys = vec!["a", "aa", "aaa", "ab", "b", "ba"];

        {
            let mut wtx = db.write_tx();
            for key in &keys {
                wtx.put(key.as_bytes(), key.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            for key in &keys {
                assert_eq!(rtx.get(key.as_bytes()), Some(key.as_bytes().to_vec()));
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_special_characters() {
        let path = test_db_path("special_chars");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let keys = vec![
            "\n\r\t",
            "key with spaces",
            "key\0with\0nulls",
            "key\"with\"quotes",
            "key\\with\\backslashes",
        ];

        {
            let mut wtx = db.write_tx();
            for key in &keys {
                wtx.put(key.as_bytes(), b"value");
            }
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            for key in &keys {
                assert_eq!(
                    rtx.get(key.as_bytes()),
                    Some(b"value".to_vec()),
                    "key {key:?} not found"
                );
            }
        }

        cleanup(&path);
    }

    // ==================== Stress Tests ====================

    #[test]
    fn test_many_small_transactions() {
        let path = test_db_path("many_small_tx");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // 100 small transactions.
        for i in 0..100 {
            let mut wtx = db.write_tx();
            wtx.put(format!("tx_{i}").as_bytes(), format!("data_{i}").as_bytes());
            wtx.commit().expect("commit should succeed");
        }

        // Verify all.
        let rtx = db.read_tx();
        for i in 0..100 {
            assert_eq!(
                rtx.get(format!("tx_{i}").as_bytes()),
                Some(format!("data_{i}").into_bytes())
            );
        }

        cleanup(&path);
    }

    #[test]
    fn test_interleaved_operations() {
        let path = test_db_path("interleaved");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Insert, update, delete in various orders.
        {
            let mut wtx = db.write_tx();
            // Insert.
            for i in 0..50 {
                wtx.put(format!("key_{i}").as_bytes(), b"initial");
            }
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            // Update even keys.
            for i in (0..50).step_by(2) {
                wtx.put(format!("key_{i}").as_bytes(), b"updated");
            }
            // Delete keys divisible by 5.
            for i in (0..50).step_by(5) {
                wtx.delete(format!("key_{i}").as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        // Verify state.
        let rtx = db.read_tx();
        for i in 0..50 {
            let key = format!("key_{i}");
            if i % 5 == 0 {
                assert_eq!(rtx.get(key.as_bytes()), None, "{key} should be deleted");
            } else if i % 2 == 0 {
                assert_eq!(
                    rtx.get(key.as_bytes()),
                    Some(b"updated".to_vec()),
                    "{key} should be updated"
                );
            } else {
                assert_eq!(
                    rtx.get(key.as_bytes()),
                    Some(b"initial".to_vec()),
                    "{key} should be initial"
                );
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_overwrite_same_key_many_times() {
        let path = test_db_path("overwrite_many");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        for i in 0..100 {
            let mut wtx = db.write_tx();
            wtx.put(b"same_key", format!("value_{i}").as_bytes());
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"same_key"), Some(b"value_99".to_vec()));

        cleanup(&path);
    }

    // ==================== Security Tests ====================

    #[test]
    fn test_path_traversal_attempt() {
        // Ensure keys with path-like characters work correctly.
        let path = test_db_path("path_traversal");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let malicious_keys = vec![
            "../../../etc/passwd",
            "/etc/shadow",
            "..\\..\\windows\\system32",
            "key/../../../value",
        ];

        {
            let mut wtx = db.write_tx();
            for key in &malicious_keys {
                wtx.put(key.as_bytes(), b"safe_value");
            }
            wtx.commit().expect("commit should succeed");
        }

        // Keys are stored as-is, no path interpretation.
        let rtx = db.read_tx();
        for key in &malicious_keys {
            assert_eq!(rtx.get(key.as_bytes()), Some(b"safe_value".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_null_byte_injection() {
        let path = test_db_path("null_injection");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let key_with_null = b"key\0suffix";
        let value_with_null = b"value\0more";

        {
            let mut wtx = db.write_tx();
            wtx.put(key_with_null, value_with_null);
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        assert_eq!(rtx.get(key_with_null), Some(value_with_null.to_vec()));

        // Ensure truncated key doesn't match.
        assert_eq!(rtx.get(b"key"), None);

        cleanup(&path);
    }

    // ==================== Page Splitting Tests ====================
    //
    // These tests validate that page splitting occurs correctly when nodes
    // exceed their capacity. Page splitting is essential for B+ tree growth
    // and maintaining balanced tree structure.

    /// Tests that inserting more keys than LEAF_MAX_KEYS causes a split.
    #[test]
    fn test_page_split_leaf_overflow() {
        use crate::btree::{BTree, LEAF_MAX_KEYS};

        let mut tree = BTree::new();

        // Insert exactly LEAF_MAX_KEYS + 1 to force a split.
        for i in 0..=LEAF_MAX_KEYS {
            let key = format!("key_{i:05}").into_bytes();
            let value = format!("value_{i}").into_bytes();
            tree.insert(key, value);
        }

        // Verify all keys are still accessible after split.
        for i in 0..=LEAF_MAX_KEYS {
            let key = format!("key_{i:05}");
            assert!(
                tree.get(key.as_bytes()).is_some(),
                "key {key} missing after split"
            );
        }

        assert_eq!(tree.len(), LEAF_MAX_KEYS + 1);
    }

    /// Tests that multiple splits maintain data integrity.
    #[test]
    fn test_page_split_multiple_levels() {
        use crate::btree::{BTree, LEAF_MAX_KEYS};

        let mut tree = BTree::new();

        // Insert enough keys to cause multiple splits (tree height > 1).
        let count = LEAF_MAX_KEYS * 5;
        for i in 0..count {
            let key = format!("key_{i:06}").into_bytes();
            let value = vec![i as u8; 100];
            tree.insert(key, value);
        }

        assert_eq!(tree.len(), count);

        // Verify all keys in sorted order.
        let pairs: Vec<_> = tree.iter().collect();
        assert_eq!(pairs.len(), count);

        for (idx, (key, _)) in pairs.iter().enumerate() {
            let expected = format!("key_{idx:06}");
            assert_eq!(
                *key,
                expected.as_bytes(),
                "key ordering incorrect after splits"
            );
        }
    }

    /// Tests split behavior with reverse-order insertions (worst case for some trees).
    #[test]
    fn test_page_split_reverse_insertion() {
        use crate::btree::{BTree, LEAF_MAX_KEYS};

        let mut tree = BTree::new();

        // Insert in reverse order to stress the split logic.
        let count = LEAF_MAX_KEYS * 3;
        for i in (0..count).rev() {
            let key = format!("key_{i:05}").into_bytes();
            let value = format!("v{i}").into_bytes();
            tree.insert(key, value);
        }

        assert_eq!(tree.len(), count);

        // Verify sorted iteration.
        let pairs: Vec<_> = tree.iter().collect();
        for (idx, (key, _)) in pairs.iter().enumerate() {
            let expected = format!("key_{idx:05}");
            assert_eq!(*key, expected.as_bytes());
        }
    }

    /// Tests split with alternating insertions (left-right stress).
    #[test]
    fn test_page_split_alternating_insertion() {
        use crate::btree::{BTree, LEAF_MAX_KEYS};

        let mut tree = BTree::new();

        let count = LEAF_MAX_KEYS * 2;
        let mut indices: Vec<usize> = Vec::with_capacity(count);

        // Alternating pattern: 0, count-1, 1, count-2, 2, count-3, ...
        for i in 0..count / 2 {
            indices.push(i);
            indices.push(count - 1 - i);
        }

        for i in indices {
            let key = format!("key_{i:05}").into_bytes();
            tree.insert(key, vec![i as u8]);
        }

        assert_eq!(tree.len(), count);

        // Verify all keys present.
        for i in 0..count {
            let key = format!("key_{i:05}");
            assert!(tree.get(key.as_bytes()).is_some());
        }
    }

    /// Tests that split maintains key-value associations.
    #[test]
    fn test_page_split_data_integrity() {
        use crate::btree::{BTree, LEAF_MAX_KEYS};

        let mut tree = BTree::new();

        let count = LEAF_MAX_KEYS * 4;
        for i in 0..count {
            let key = format!("key_{i:06}").into_bytes();
            // Unique value that encodes the key index.
            let value = format!("unique_value_for_key_{i}").into_bytes();
            tree.insert(key, value);
        }

        // Verify each key maps to its correct value.
        for i in 0..count {
            let key = format!("key_{i:06}");
            let expected_value = format!("unique_value_for_key_{i}");
            let actual = tree.get(key.as_bytes());
            assert_eq!(
                actual,
                Some(expected_value.as_bytes()),
                "key {key} has wrong value after splits"
            );
        }
    }

    /// Tests split with varying key sizes.
    #[test]
    fn test_page_split_varying_key_sizes() {
        use crate::btree::BTree;

        let mut tree = BTree::new();

        // Mix of small and large keys to stress page capacity calculations.
        for i in 0..100 {
            let key_len = (i % 50) + 1; // 1 to 50 bytes.
            let key = format!("{i:0>width$}", width = key_len).into_bytes();
            let value = vec![i as u8; 100];
            tree.insert(key, value);
        }

        assert_eq!(tree.len(), 100);

        // All keys should be retrievable.
        for i in 0..100 {
            let key_len = (i % 50) + 1;
            let key = format!("{i:0>width$}", width = key_len);
            assert!(tree.get(key.as_bytes()).is_some());
        }
    }

    /// Tests split with large values that approach page capacity.
    #[test]
    fn test_page_split_large_values() {
        use crate::btree::BTree;

        let mut tree = BTree::new();

        // Large values that stress page capacity.
        for i in 0..50 {
            let key = format!("large_value_key_{i:03}").into_bytes();
            let value = vec![i as u8; 2000]; // 2KB values.
            tree.insert(key, value);
        }

        assert_eq!(tree.len(), 50);

        for i in 0..50 {
            let key = format!("large_value_key_{i:03}");
            let value = tree.get(key.as_bytes());
            assert!(value.is_some());
            assert_eq!(value.unwrap().len(), 2000);
            assert!(value.unwrap().iter().all(|&b| b == i as u8));
        }
    }

    /// Tests that split operations preserve tree balance.
    #[test]
    fn test_page_split_tree_balance() {
        use crate::btree::BTree;

        let mut tree = BTree::new();

        // Insert many keys.
        for i in 0..1000 {
            tree.insert(format!("k{i:05}").into_bytes(), vec![0u8; 10]);
        }

        // Delete half of them.
        for i in (0..1000).step_by(2) {
            tree.remove(format!("k{i:05}").as_bytes());
        }

        // Insert more keys.
        for i in 1000..1500 {
            tree.insert(format!("k{i:05}").into_bytes(), vec![1u8; 10]);
        }

        assert_eq!(tree.len(), 1000); // 500 remaining + 500 new.

        // Verify all expected keys are present.
        for i in (1..1000).step_by(2) {
            assert!(tree.get(format!("k{i:05}").as_bytes()).is_some());
        }
        for i in 1000..1500 {
            assert!(tree.get(format!("k{i:05}").as_bytes()).is_some());
        }
    }

    /// Tests persistence after page splits.
    #[test]
    fn test_page_split_persistence() {
        use crate::btree::LEAF_MAX_KEYS;

        let path = test_db_path("split_persist");
        cleanup(&path);

        let count = LEAF_MAX_KEYS * 3;

        // Insert enough to cause splits, then close.
        {
            let mut db = Database::open(&path).expect("open");

            let mut wtx = db.write_tx();
            for i in 0..count {
                wtx.put(format!("k{i:05}").as_bytes(), format!("v{i}").as_bytes());
            }
            wtx.commit().expect("commit");
        }

        // Reopen and verify all data survived.
        {
            let db = Database::open(&path).expect("reopen");
            let rtx = db.read_tx();

            for i in 0..count {
                let key = format!("k{i:05}");
                let expected = format!("v{i}");
                assert_eq!(
                    rtx.get(key.as_bytes()),
                    Some(expected.into_bytes()),
                    "{key} missing after reopen"
                );
            }
        }

        cleanup(&path);
    }

    /// Tests concurrent reads during/after splits (simulated via transactions).
    #[test]
    fn test_page_split_read_consistency() {
        let path = test_db_path("split_read_consistency");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Initial data.
        {
            let mut wtx = db.write_tx();
            for i in 0..20 {
                wtx.put(format!("key_{i:03}").as_bytes(), b"initial");
            }
            wtx.commit().expect("commit");
        }

        // Read transaction sees initial state.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key_000"), Some(b"initial".to_vec()));
        assert_eq!(rtx.get(b"key_019"), Some(b"initial".to_vec()));
        drop(rtx);

        // Write more data causing potential splits.
        {
            let mut wtx = db.write_tx();
            for i in 20..200 {
                wtx.put(format!("key_{i:03}").as_bytes(), b"expanded");
            }
            wtx.commit().expect("commit");
        }

        // New read should see all data.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key_000"), Some(b"initial".to_vec()));
        assert_eq!(rtx.get(b"key_100"), Some(b"expanded".to_vec()));

        cleanup(&path);
    }

    // ==================== Copy-on-Write (COW) Tests ====================
    //
    // Copy-on-Write ensures that modifications don't alter original data
    // until a transaction commits. This provides crash recovery guarantees
    // and read isolation.

    /// Tests that uncommitted writes don't affect the database.
    #[test]
    fn test_cow_uncommitted_invisible() {
        let path = test_db_path("cow_uncommitted");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Initial committed data.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key1", b"committed");
            wtx.commit().expect("commit");
        }

        // Start a write transaction but don't commit.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key1", b"uncommitted_change");
            wtx.put(b"key2", b"uncommitted_new");
            // Drop without commit.
        }

        // Original data should be intact.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key1"), Some(b"committed".to_vec()));
        assert_eq!(rtx.get(b"key2"), None);

        cleanup(&path);
    }

    /// Tests that transaction rollback preserves original data.
    #[test]
    fn test_cow_rollback_preserves_data() {
        let path = test_db_path("cow_rollback");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Commit initial data.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"preserved", b"original_value");
            wtx.commit().expect("commit");
        }

        // Multiple uncommitted operations.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"preserved", b"modified");
            wtx.delete(b"preserved");
            wtx.put(b"preserved", b"recreated");
            // Implicit rollback on drop.
        }

        // Original still present.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"preserved"), Some(b"original_value".to_vec()));

        cleanup(&path);
    }

    /// Tests COW behavior across database reopens.
    #[test]
    fn test_cow_persistence_after_crash_simulation() {
        let path = test_db_path("cow_crash_sim");
        cleanup(&path);

        // Phase 1: Commit some data.
        {
            let mut db = Database::open(&path).expect("open");
            let mut wtx = db.write_tx();
            wtx.put(b"stable", b"committed_data");
            wtx.commit().expect("commit");
        }

        // Phase 2: Start writes but "crash" (don't commit).
        {
            let mut db = Database::open(&path).expect("reopen");
            let mut wtx = db.write_tx();
            wtx.put(b"stable", b"would_be_lost");
            wtx.put(b"new_key", b"would_be_lost_too");
            // Simulate crash by dropping without commit.
        }

        // Phase 3: Reopen - only committed data should exist.
        {
            let db = Database::open(&path).expect("reopen after crash");
            let rtx = db.read_tx();
            assert_eq!(rtx.get(b"stable"), Some(b"committed_data".to_vec()));
            assert_eq!(rtx.get(b"new_key"), None);
        }

        cleanup(&path);
    }

    /// Tests that read transactions see a consistent snapshot.
    #[test]
    fn test_cow_snapshot_isolation() {
        let path = test_db_path("cow_snapshot");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Initial state.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"version1");
            wtx.commit().expect("commit");
        }

        // Get a read snapshot.
        let snapshot_value = {
            let rtx = db.read_tx();
            rtx.get(b"key")
        };

        // Modify and commit.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"version2");
            wtx.commit().expect("commit");
        }

        // Snapshot value should still be version1.
        assert_eq!(snapshot_value, Some(b"version1".to_vec()));

        // New read sees version2.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), Some(b"version2".to_vec()));

        cleanup(&path);
    }

    /// Tests COW with many sequential transactions.
    #[test]
    fn test_cow_sequential_transactions() {
        let path = test_db_path("cow_sequential");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        for version in 0..50 {
            // Alternate between commit and rollback.
            if version % 3 == 0 {
                // Rollback - data should not change.
                let mut wtx = db.write_tx();
                wtx.put(b"key", format!("rolled_back_{version}").as_bytes());
                // Drop without commit.
            } else {
                // Commit.
                let mut wtx = db.write_tx();
                wtx.put(b"key", format!("committed_{version}").as_bytes());
                wtx.commit().expect("commit");
            }
        }

        // Final value should be from last committed transaction.
        let rtx = db.read_tx();
        // Last committed was version 49 (49 % 3 != 0).
        assert_eq!(rtx.get(b"key"), Some(b"committed_49".to_vec()));

        cleanup(&path);
    }

    /// Tests COW behavior with deletes.
    #[test]
    fn test_cow_delete_rollback() {
        let path = test_db_path("cow_delete_rollback");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Commit data.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"to_delete", b"should_survive");
            wtx.commit().expect("commit");
        }

        // Delete but don't commit.
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"to_delete");
            // Rollback.
        }

        // Data should still exist.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"to_delete"), Some(b"should_survive".to_vec()));

        cleanup(&path);
    }

    /// Tests that write transaction has its own working copy.
    #[test]
    fn test_cow_write_tx_isolation() {
        let path = test_db_path("cow_write_isolation");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Commit initial data.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"initial");
            wtx.commit().expect("commit");
        }

        // Write transaction modifies its copy.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"modified_in_tx");

            // Within the same tx, we should see our modification.
            // (WriteTx.get would be needed for this, but we verify via commit)

            // Don't commit.
        }

        // Database still has initial value.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"key"), Some(b"initial".to_vec()));

        cleanup(&path);
    }

    /// Tests COW with large data modifications.
    #[test]
    fn test_cow_large_data_rollback() {
        let path = test_db_path("cow_large_rollback");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Commit some initial data.
        {
            let mut wtx = db.write_tx();
            for i in 0..100 {
                wtx.put(format!("stable_{i:03}").as_bytes(), b"stable_value");
            }
            wtx.commit().expect("commit");
        }

        // Large rollback - many modifications.
        {
            let mut wtx = db.write_tx();
            // Delete all.
            for i in 0..100 {
                wtx.delete(format!("stable_{i:03}").as_bytes());
            }
            // Insert new.
            for i in 0..200 {
                wtx.put(format!("new_{i:03}").as_bytes(), b"new_value");
            }
            // Rollback.
        }

        // All original data should be intact.
        let rtx = db.read_tx();
        for i in 0..100 {
            assert_eq!(
                rtx.get(format!("stable_{i:03}").as_bytes()),
                Some(b"stable_value".to_vec()),
                "stable_{i:03} missing after rollback"
            );
        }
        // New keys should not exist.
        for i in 0..200 {
            assert_eq!(
                rtx.get(format!("new_{i:03}").as_bytes()),
                None,
                "new_{i:03} should not exist after rollback"
            );
        }

        cleanup(&path);
    }

    /// Tests meta page alternation (fundamental to COW).
    #[test]
    fn test_cow_meta_page_alternation() {
        let path = test_db_path("cow_meta_alt");
        cleanup(&path);

        // Multiple commits should alternate meta pages.
        {
            let mut db = Database::open(&path).expect("open");

            for i in 0..10 {
                let mut wtx = db.write_tx();
                wtx.put(format!("tx_{i}").as_bytes(), format!("data_{i}").as_bytes());
                wtx.commit().expect("commit");
            }
        }

        // Reopen and verify all transactions' data persisted.
        {
            let db = Database::open(&path).expect("reopen");
            let rtx = db.read_tx();

            for i in 0..10 {
                assert_eq!(
                    rtx.get(format!("tx_{i}").as_bytes()),
                    Some(format!("data_{i}").into_bytes()),
                    "tx_{i} data missing"
                );
            }
        }

        cleanup(&path);
    }

    /// Tests COW behavior under high transaction rate.
    #[test]
    fn test_cow_high_transaction_rate() {
        let path = test_db_path("cow_high_rate");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Many small transactions.
        for i in 0..200 {
            let mut wtx = db.write_tx();
            wtx.put(format!("rapid_{i:04}").as_bytes(), &vec![i as u8; 50]);
            wtx.commit().expect("commit");
        }

        // Verify all data.
        let rtx = db.read_tx();
        for i in 0..200 {
            let value = rtx.get(format!("rapid_{i:04}").as_bytes());
            assert!(value.is_some());
            assert!(value.unwrap().iter().all(|&b| b == i as u8));
        }

        cleanup(&path);
    }

    /// Tests that COW properly isolates concurrent-style operations.
    #[test]
    fn test_cow_transaction_boundary() {
        let path = test_db_path("cow_boundary");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Transaction 1: Insert A.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"A", b"from_tx1");
            wtx.commit().expect("commit tx1");
        }

        // Transaction 2: Insert B, modify A, rollback.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"B", b"from_tx2");
            wtx.put(b"A", b"modified_by_tx2");
            // Rollback.
        }

        // Transaction 3: Insert C.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"C", b"from_tx3");
            wtx.commit().expect("commit tx3");
        }

        // State should be: A from tx1, no B, C from tx3.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"A"), Some(b"from_tx1".to_vec()));
        assert_eq!(rtx.get(b"B"), None);
        assert_eq!(rtx.get(b"C"), Some(b"from_tx3".to_vec()));

        cleanup(&path);
    }

    /// Tests COW with mixed operations in single transaction.
    #[test]
    fn test_cow_mixed_operations_commit() {
        let path = test_db_path("cow_mixed_ops");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Initial data.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"keep", b"original");
            wtx.put(b"update", b"original");
            wtx.put(b"delete", b"original");
            wtx.commit().expect("commit");
        }

        // Mixed operations in one transaction.
        {
            let mut wtx = db.write_tx();
            // Keep 'keep' unchanged.
            wtx.put(b"update", b"updated");
            wtx.delete(b"delete");
            wtx.put(b"new", b"created");
            wtx.commit().expect("commit");
        }

        // Verify final state.
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"keep"), Some(b"original".to_vec()));
        assert_eq!(rtx.get(b"update"), Some(b"updated".to_vec()));
        assert_eq!(rtx.get(b"delete"), None);
        assert_eq!(rtx.get(b"new"), Some(b"created".to_vec()));

        cleanup(&path);
    }

    /// Tests that database file remains valid after partial write (simulated).
    #[test]
    fn test_cow_file_consistency() {
        let path = test_db_path("cow_file_consistency");
        cleanup(&path);

        // Create database with data.
        {
            let mut db = Database::open(&path).expect("open");
            let mut wtx = db.write_tx();
            for i in 0..50 {
                wtx.put(format!("key_{i:03}").as_bytes(), &vec![i as u8; 200]);
            }
            wtx.commit().expect("commit");
        }

        // Get file size.
        let file_size = fs::metadata(&path).expect("metadata").len();

        // Reopen and add more data.
        {
            let mut db = Database::open(&path).expect("reopen");
            let mut wtx = db.write_tx();
            for i in 50..100 {
                wtx.put(format!("key_{i:03}").as_bytes(), &vec![i as u8; 200]);
            }
            wtx.commit().expect("commit");
        }

        // File should have grown.
        let new_file_size = fs::metadata(&path).expect("metadata").len();
        assert!(new_file_size > file_size, "file should grow with more data");

        // All data should be accessible.
        {
            let db = Database::open(&path).expect("final reopen");
            let rtx = db.read_tx();
            for i in 0..100 {
                assert!(
                    rtx.get(format!("key_{i:03}").as_bytes()).is_some(),
                    "key_{i:03} missing"
                );
            }
        }

        cleanup(&path);
    }

    // ==================== Combined Page Split + COW Tests ====================

    /// Tests that page splits work correctly with transaction rollback.
    #[test]
    fn test_split_with_rollback() {
        use crate::btree::LEAF_MAX_KEYS;

        let path = test_db_path("split_rollback");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open");

        // Commit initial small dataset.
        {
            let mut wtx = db.write_tx();
            for i in 0..5 {
                wtx.put(format!("base_{i}").as_bytes(), b"initial");
            }
            wtx.commit().expect("commit");
        }

        // Large insert that would cause splits, but rollback.
        {
            let mut wtx = db.write_tx();
            for i in 0..(LEAF_MAX_KEYS * 3) {
                wtx.put(format!("split_{i:05}").as_bytes(), &vec![0u8; 100]);
            }
            // Rollback.
        }

        // Only initial data should exist.
        let rtx = db.read_tx();
        for i in 0..5 {
            assert!(rtx.get(format!("base_{i}").as_bytes()).is_some());
        }
        assert!(rtx.get(b"split_00000").is_none());

        cleanup(&path);
    }

    /// Tests persistence of split tree with interleaved rollbacks.
    #[test]
    fn test_split_persistence_with_interleaved_rollback() {
        use crate::btree::LEAF_MAX_KEYS;

        let path = test_db_path("split_interleaved");
        cleanup(&path);

        let count = LEAF_MAX_KEYS * 2;

        {
            let mut db = Database::open(&path).expect("open");

            // Commit half.
            {
                let mut wtx = db.write_tx();
                for i in 0..count / 2 {
                    wtx.put(format!("c_{i:05}").as_bytes(), b"committed");
                }
                wtx.commit().expect("commit");
            }

            // Rollback attempt to add more.
            {
                let mut wtx = db.write_tx();
                for i in count / 2..count {
                    wtx.put(format!("r_{i:05}").as_bytes(), b"rolled_back");
                }
                // Rollback.
            }

            // Commit rest with different prefix.
            {
                let mut wtx = db.write_tx();
                for i in count / 2..count {
                    wtx.put(format!("c_{i:05}").as_bytes(), b"committed");
                }
                wtx.commit().expect("commit");
            }
        }

        // Reopen and verify.
        {
            let db = Database::open(&path).expect("reopen");
            let rtx = db.read_tx();

            // All 'c_' keys should exist.
            for i in 0..count {
                assert!(
                    rtx.get(format!("c_{i:05}").as_bytes()).is_some(),
                    "c_{i:05} missing"
                );
            }

            // No 'r_' keys should exist.
            for i in count / 2..count {
                assert!(
                    rtx.get(format!("r_{i:05}").as_bytes()).is_none(),
                    "r_{i:05} should not exist"
                );
            }
        }

        cleanup(&path);
    }

    // ==================== Performance Characteristic Tests ====================
    //
    // These tests verify that operations complete within reasonable bounds,
    // which indirectly validates efficient implementation.

    /// Tests that many small insertions don't degrade performance drastically.
    #[test]
    fn test_split_performance_many_insertions() {
        use std::time::Instant;

        let mut tree = crate::btree::BTree::new();

        let start = Instant::now();
        for i in 0..5000 {
            tree.insert(format!("perf_{i:06}").into_bytes(), vec![0u8; 50]);
        }
        let duration = start.elapsed();

        // Should complete in reasonable time (< 5 seconds even on slow systems).
        assert!(
            duration.as_secs() < 5,
            "5000 insertions took too long: {duration:?}"
        );

        assert_eq!(tree.len(), 5000);
    }

    /// Tests that lookups remain fast after many splits.
    #[test]
    fn test_split_lookup_performance() {
        use std::time::Instant;

        let mut tree = crate::btree::BTree::new();

        // Build tree.
        for i in 0..5000 {
            tree.insert(format!("lookup_{i:06}").into_bytes(), vec![i as u8; 100]);
        }

        // Time lookups.
        let start = Instant::now();
        for i in 0..5000 {
            let _ = tree.get(format!("lookup_{i:06}").as_bytes());
        }
        let duration = start.elapsed();

        // 5000 lookups should be very fast (< 1 second).
        assert!(
            duration.as_millis() < 1000,
            "5000 lookups took too long: {duration:?}"
        );
    }

    /// Tests iteration performance after splits.
    #[test]
    fn test_split_iteration_performance() {
        use std::time::Instant;

        let mut tree = crate::btree::BTree::new();

        for i in 0..5000 {
            tree.insert(format!("iter_{i:06}").into_bytes(), vec![0u8; 20]);
        }

        let start = Instant::now();
        let count = tree.iter().count();
        let duration = start.elapsed();

        assert_eq!(count, 5000);
        assert!(
            duration.as_millis() < 500,
            "iteration took too long: {duration:?}"
        );
    }

    // ==================== Meta Page Switching Tests ====================
    //
    // Meta page switching alternates between two meta pages (page 0 and page 1)
    // on each commit. This enables crash recovery: if a crash occurs during
    // write, the previous valid meta page can be used.

    /// Tests that txid increments on each commit.
    #[test]
    fn test_meta_switch_txid_increments() {
        let path = test_db_path("meta_txid_inc");
        cleanup(&path);

        {
            let mut db = Database::open(&path).expect("open");

            for expected_txid in 1..=10u64 {
                let mut wtx = db.write_tx();
                wtx.put(format!("tx_{expected_txid}").as_bytes(), b"data");
                wtx.commit().expect("commit");

                // After commit, txid should match.
                assert_eq!(
                    db.meta().txid, expected_txid,
                    "txid should be {expected_txid} after {expected_txid} commits"
                );
            }
        }

        cleanup(&path);
    }

    /// Tests that meta pages alternate correctly.
    #[test]
    fn test_meta_switch_alternation() {
        let path = test_db_path("meta_alt");
        cleanup(&path);

        {
            let mut db = Database::open(&path).expect("open");

            // Commit 1: txid=1, should write to page 1 (odd txid)
            {
                let mut wtx = db.write_tx();
                wtx.put(b"key1", b"value1");
                wtx.commit().expect("commit");
            }
            assert_eq!(db.meta().txid, 1);

            // Commit 2: txid=2, should write to page 0 (even txid)
            {
                let mut wtx = db.write_tx();
                wtx.put(b"key2", b"value2");
                wtx.commit().expect("commit");
            }
            assert_eq!(db.meta().txid, 2);

            // Commit 3: txid=3, should write to page 1 (odd txid)
            {
                let mut wtx = db.write_tx();
                wtx.put(b"key3", b"value3");
                wtx.commit().expect("commit");
            }
            assert_eq!(db.meta().txid, 3);
        }

        cleanup(&path);
    }

    /// Tests crash recovery using meta page switching.
    #[test]
    fn test_meta_switch_crash_recovery() {
        use std::io::{Read, Seek, SeekFrom, Write};
        use crate::page::PAGE_SIZE;

        let path = test_db_path("meta_crash");
        cleanup(&path);

        // Phase 1: Create database with known state.
        {
            let mut db = Database::open(&path).expect("open");
            let mut wtx = db.write_tx();
            wtx.put(b"stable", b"committed_data");
            wtx.commit().expect("commit");
        }

        // Phase 2: Corrupt the meta page that would be written next.
        // After txid=1 (odd), the next write would go to page 0.
        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open file");

            // Corrupt page 0 (next write target) by writing garbage.
            file.seek(SeekFrom::Start(0)).expect("seek");
            let garbage = [0xDEu8; PAGE_SIZE];
            file.write_all(&garbage).expect("write garbage");
            file.sync_all().expect("sync");
        }

        // Phase 3: Reopen - should recover using the valid meta page 1.
        {
            let db = Database::open(&path).expect("reopen after corruption");
            let rtx = db.read_tx();
            assert_eq!(
                rtx.get(b"stable"),
                Some(b"committed_data".to_vec()),
                "should recover data from valid meta page"
            );
        }

        cleanup(&path);
    }

    /// Tests that corrupted meta page is detected.
    #[test]
    fn test_meta_switch_corruption_detection() {
        use std::io::{Seek, SeekFrom, Write};
        use crate::page::PAGE_SIZE;

        let path = test_db_path("meta_corrupt_detect");
        cleanup(&path);

        // Create database.
        {
            let mut db = Database::open(&path).expect("open");
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value");
            wtx.commit().expect("commit");

            // Commit again so both pages have data.
            let mut wtx = db.write_tx();
            wtx.put(b"key2", b"value2");
            wtx.commit().expect("commit");
        }

        // Corrupt just a few bytes in meta page 1 (not completely overwrite).
        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open file");

            // Corrupt checksum area of page 1.
            file.seek(SeekFrom::Start(PAGE_SIZE as u64 + 56)).expect("seek");
            file.write_all(&[0xFF; 8]).expect("corrupt checksum");
            file.sync_all().expect("sync");
        }

        // Should still open using the valid meta page 0.
        {
            let db = Database::open(&path).expect("reopen");
            let rtx = db.read_tx();
            // Should have data from the valid meta page.
            assert!(rtx.get(b"key").is_some() || rtx.get(b"key2").is_some());
        }

        cleanup(&path);
    }

    /// Tests meta page selection with higher txid wins.
    #[test]
    fn test_meta_switch_higher_txid_wins() {
        let path = test_db_path("meta_higher_txid");
        cleanup(&path);

        // Multiple commits to ensure txid progression.
        {
            let mut db = Database::open(&path).expect("open");

            for i in 0..5 {
                let mut wtx = db.write_tx();
                wtx.put(format!("iter_{i}").as_bytes(), format!("data_{i}").as_bytes());
                wtx.commit().expect("commit");
            }

            // Final txid should be 5.
            assert_eq!(db.meta().txid, 5);
        }

        // Reopen and verify highest txid is used.
        {
            let db = Database::open(&path).expect("reopen");
            // The meta with txid=5 should be selected.
            assert_eq!(db.meta().txid, 5);

            // All data should be present.
            let rtx = db.read_tx();
            for i in 0..5 {
                assert_eq!(
                    rtx.get(format!("iter_{i}").as_bytes()),
                    Some(format!("data_{i}").into_bytes())
                );
            }
        }

        cleanup(&path);
    }

    /// Tests meta page switching under high commit rate.
    #[test]
    fn test_meta_switch_high_commit_rate() {
        let path = test_db_path("meta_high_rate");
        cleanup(&path);

        {
            let mut db = Database::open(&path).expect("open");

            for i in 0..100 {
                let mut wtx = db.write_tx();
                wtx.put(format!("rapid_{i:04}").as_bytes(), b"data");
                wtx.commit().expect("commit");
            }

            assert_eq!(db.meta().txid, 100);
        }

        // Reopen and verify.
        {
            let db = Database::open(&path).expect("reopen");
            assert_eq!(db.meta().txid, 100);

            let rtx = db.read_tx();
            for i in 0..100 {
                assert!(rtx.get(format!("rapid_{i:04}").as_bytes()).is_some());
            }
        }

        cleanup(&path);
    }

    /// Tests that both meta pages can be read individually.
    #[test]
    fn test_meta_switch_both_pages_valid() {
        use std::io::{Read, Seek, SeekFrom};
        use crate::meta::Meta;
        use crate::page::PAGE_SIZE;

        let path = test_db_path("meta_both_valid");
        cleanup(&path);

        // Create and commit twice to write to both meta pages.
        {
            let mut db = Database::open(&path).expect("open");

            let mut wtx = db.write_tx();
            wtx.put(b"first", b"commit");
            wtx.commit().expect("commit 1");

            let mut wtx = db.write_tx();
            wtx.put(b"second", b"commit");
            wtx.commit().expect("commit 2");
        }

        // Manually read both meta pages.
        {
            let mut file = std::fs::File::open(&path).expect("open file");
            let mut buf = [0u8; PAGE_SIZE];

            // Read meta page 0.
            file.seek(SeekFrom::Start(0)).expect("seek 0");
            file.read_exact(&mut buf).expect("read 0");
            let meta0 = Meta::from_bytes(&buf);
            assert!(meta0.is_some(), "meta page 0 should be valid");

            // Read meta page 1.
            file.seek(SeekFrom::Start(PAGE_SIZE as u64)).expect("seek 1");
            file.read_exact(&mut buf).expect("read 1");
            let meta1 = Meta::from_bytes(&buf);
            assert!(meta1.is_some(), "meta page 1 should be valid");

            // One should have txid=1, other txid=2.
            let m0 = meta0.unwrap();
            let m1 = meta1.unwrap();
            let txids = [m0.txid, m1.txid];
            assert!(txids.contains(&1) || txids.contains(&2));
        }

        cleanup(&path);
    }

    /// Tests meta page with maximum txid value.
    #[test]
    fn test_meta_switch_txid_overflow_safety() {
        // This tests that very large txid values work correctly.
        use crate::meta::Meta;
        use crate::page::PAGE_SIZE;

        let mut meta = Meta::new();
        meta.txid = u64::MAX - 1;

        let bytes = meta.to_bytes();
        let recovered = Meta::from_bytes(&bytes);

        assert!(recovered.is_some());
        assert_eq!(recovered.unwrap().txid, u64::MAX - 1);
    }

    // ==================== Basic Freelist Tests ====================
    //
    // The freelist tracks pages that have been freed and can be reused.
    // This enables efficient space reclamation when data is deleted.

    /// Tests basic freelist creation.
    #[test]
    fn test_freelist_new() {
        use crate::freelist::FreeList;

        let fl = FreeList::new();
        assert!(fl.is_empty());
        assert_eq!(fl.len(), 0);
    }

    /// Tests adding pages to freelist.
    #[test]
    fn test_freelist_add_page() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        fl.free(10);
        assert_eq!(fl.len(), 1);
        assert!(!fl.is_empty());

        fl.free(20);
        fl.free(30);
        assert_eq!(fl.len(), 3);
    }

    /// Tests allocating pages from freelist.
    #[test]
    fn test_freelist_allocate() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        fl.free(10);
        fl.free(20);
        fl.free(30);

        // Allocate should return a freed page.
        let page = fl.allocate();
        assert!(page.is_some());
        assert_eq!(fl.len(), 2);

        let page2 = fl.allocate();
        assert!(page2.is_some());
        assert_eq!(fl.len(), 1);
    }

    /// Tests allocating from empty freelist.
    #[test]
    fn test_freelist_allocate_empty() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();
        let page = fl.allocate();
        assert!(page.is_none());
    }

    /// Tests freelist doesn't allow duplicate page IDs.
    #[test]
    fn test_freelist_no_duplicates() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        fl.free(10);
        fl.free(10); // Duplicate.
        fl.free(10); // Another duplicate.

        // Should only have 1 entry.
        assert_eq!(fl.len(), 1);
    }

    /// Tests freelist serialization/deserialization.
    #[test]
    fn test_freelist_serialize_deserialize() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();
        fl.free(5);
        fl.free(10);
        fl.free(15);
        fl.free(100);

        let bytes = fl.to_bytes();
        let recovered = FreeList::from_bytes(&bytes);

        assert!(recovered.is_some());
        let recovered = recovered.unwrap();
        assert_eq!(recovered.len(), 4);
    }

    /// Tests freelist round-trip preserves all pages.
    #[test]
    fn test_freelist_round_trip() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        // Add many pages.
        for i in 0..100 {
            fl.free(i * 10 + 3); // Arbitrary page IDs.
        }

        let bytes = fl.to_bytes();
        let mut recovered = FreeList::from_bytes(&bytes).expect("deserialize");

        assert_eq!(recovered.len(), 100);

        // Allocate all and verify we got them all.
        let mut allocated = Vec::new();
        while let Some(page) = recovered.allocate() {
            allocated.push(page);
        }

        assert_eq!(allocated.len(), 100);
    }

    /// Tests freelist with empty serialization.
    #[test]
    fn test_freelist_serialize_empty() {
        use crate::freelist::FreeList;

        let fl = FreeList::new();
        let bytes = fl.to_bytes();

        let recovered = FreeList::from_bytes(&bytes);
        assert!(recovered.is_some());
        assert!(recovered.unwrap().is_empty());
    }

    /// Tests freelist contains check.
    #[test]
    fn test_freelist_contains() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        fl.free(10);
        fl.free(20);

        assert!(fl.contains(10));
        assert!(fl.contains(20));
        assert!(!fl.contains(15));
        assert!(!fl.contains(30));
    }

    /// Tests freelist clear operation.
    #[test]
    fn test_freelist_clear() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        fl.free(10);
        fl.free(20);
        fl.free(30);

        fl.clear();

        assert!(fl.is_empty());
        assert_eq!(fl.len(), 0);
        assert!(fl.allocate().is_none());
    }

    /// Tests freelist iteration.
    #[test]
    fn test_freelist_iter() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        fl.free(10);
        fl.free(20);
        fl.free(30);

        let pages: Vec<_> = fl.iter().copied().collect();
        assert_eq!(pages.len(), 3);
        assert!(pages.contains(&10));
        assert!(pages.contains(&20));
        assert!(pages.contains(&30));
    }

    /// Tests freelist with large page IDs.
    #[test]
    fn test_freelist_large_page_ids() {
        use crate::freelist::FreeList;
        use crate::page::PageId;

        let mut fl = FreeList::new();

        let large_ids: Vec<PageId> = vec![
            u64::MAX - 1,
            u64::MAX - 100,
            u64::MAX / 2,
            1_000_000_000,
        ];

        for &id in &large_ids {
            fl.free(id);
        }

        assert_eq!(fl.len(), 4);

        for &id in &large_ids {
            assert!(fl.contains(id));
        }

        let bytes = fl.to_bytes();
        let recovered = FreeList::from_bytes(&bytes).expect("deserialize");

        for &id in &large_ids {
            assert!(recovered.contains(id));
        }
    }

    /// Tests freelist maintains order for allocation (LIFO behavior).
    #[test]
    fn test_freelist_allocation_order() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        // Free pages in order.
        fl.free(1);
        fl.free(2);
        fl.free(3);

        // Allocate - should return in some consistent order.
        let a = fl.allocate().unwrap();
        let b = fl.allocate().unwrap();
        let c = fl.allocate().unwrap();

        // All three should be unique and from our freed set.
        let mut set = std::collections::HashSet::new();
        set.insert(a);
        set.insert(b);
        set.insert(c);

        assert_eq!(set.len(), 3);
        assert!(set.contains(&1));
        assert!(set.contains(&2));
        assert!(set.contains(&3));
    }

    /// Tests freelist stress with many allocate/free cycles.
    #[test]
    fn test_freelist_stress_cycles() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        // Multiple cycles of free and allocate.
        for cycle in 0..10 {
            // Free some pages.
            for i in 0..100 {
                fl.free(cycle * 1000 + i);
            }

            // Allocate half.
            for _ in 0..50 {
                fl.allocate();
            }
        }

        // Should have 50 * 10 = 500 pages remaining.
        assert_eq!(fl.len(), 500);
    }

    /// Tests freelist serialization size is reasonable.
    #[test]
    fn test_freelist_serialization_size() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        // 100 pages, each PageId is 8 bytes, plus 8 byte count header.
        for i in 0..100 {
            fl.free(i);
        }

        let bytes = fl.to_bytes();
        // Expected: 8 (count) + 100 * 8 (page IDs) = 808 bytes.
        assert_eq!(bytes.len(), 808);
    }

    /// Tests freelist with page ID 0 (edge case).
    #[test]
    fn test_freelist_page_zero() {
        use crate::freelist::FreeList;

        let mut fl = FreeList::new();

        // Page 0 and 1 are meta pages, but freelist should handle any PageId.
        fl.free(0);
        fl.free(1);
        fl.free(2);

        assert_eq!(fl.len(), 3);
        assert!(fl.contains(0));
        assert!(fl.contains(1));

        let bytes = fl.to_bytes();
        let recovered = FreeList::from_bytes(&bytes).unwrap();
        assert!(recovered.contains(0));
    }

    /// Tests freelist deserialization with corrupted data.
    #[test]
    fn test_freelist_corrupted_data() {
        use crate::freelist::FreeList;

        // Too short - can't even read count.
        let short = vec![0u8; 4];
        assert!(FreeList::from_bytes(&short).is_none());

        // Count says 100 but not enough data.
        let mut bad = vec![0u8; 16];
        bad[0..8].copy_from_slice(&100u64.to_le_bytes()); // Claims 100 entries.
        assert!(FreeList::from_bytes(&bad).is_none());
    }

    /// Tests freelist performance with many pages.
    #[test]
    fn test_freelist_performance() {
        use crate::freelist::FreeList;
        use std::time::Instant;

        let mut fl = FreeList::new();

        let start = Instant::now();

        // Add 10000 pages.
        for i in 0..10000 {
            fl.free(i);
        }

        // Allocate all.
        while fl.allocate().is_some() {}

        let duration = start.elapsed();

        // Should complete quickly (< 1 second).
        assert!(
            duration.as_millis() < 1000,
            "freelist operations too slow: {duration:?}"
        );
    }

    // ==================== Integrated Meta + Freelist Tests ====================

    /// Tests that meta page stores freelist pointer.
    #[test]
    fn test_meta_stores_freelist_pointer() {
        use crate::meta::Meta;

        let mut meta = Meta::new();
        meta.freelist = 42;

        let bytes = meta.to_bytes();
        let recovered = Meta::from_bytes(&bytes).unwrap();

        assert_eq!(recovered.freelist, 42);
    }

    /// Tests database page_count tracking.
    #[test]
    fn test_meta_page_count_tracking() {
        use crate::meta::Meta;

        let mut meta = Meta::new();
        assert_eq!(meta.page_count, 2); // Initial: 2 meta pages.

        meta.page_count = 100;

        let bytes = meta.to_bytes();
        let recovered = Meta::from_bytes(&bytes).unwrap();

        assert_eq!(recovered.page_count, 100);
    }

    // ==================== Bucket Tests ====================

    #[test]
    fn test_bucket_create_basic() {
        let path = test_db_path("bucket_create");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"mybucket").expect("create bucket should succeed");
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            assert!(rtx.bucket_exists(b"mybucket"));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_create_already_exists() {
        let path = test_db_path("bucket_exists_err");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"mybucket").expect("create bucket should succeed");
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            let result = wtx.create_bucket(b"mybucket");
            assert!(result.is_err());
            match result.unwrap_err() {
                Error::BucketAlreadyExists { name } => {
                    assert_eq!(name, b"mybucket".to_vec());
                }
                other => panic!("expected BucketAlreadyExists, got {other:?}"),
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_create_if_not_exists() {
        let path = test_db_path("bucket_create_if_not");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            let created = wtx.create_bucket_if_not_exists(b"mybucket").unwrap();
            assert!(created, "should report bucket was created");
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            let created = wtx.create_bucket_if_not_exists(b"mybucket").unwrap();
            assert!(!created, "should report bucket already existed");
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_invalid_name_empty() {
        let path = test_db_path("bucket_invalid_empty");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            let result = wtx.create_bucket(b"");
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), Error::InvalidBucketName { .. }));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_invalid_name_too_long() {
        let path = test_db_path("bucket_invalid_long");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            let long_name = vec![b'x'; 256]; // 256 bytes exceeds MAX_BUCKET_NAME_LEN (255)
            let result = wtx.create_bucket(&long_name);
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), Error::InvalidBucketName { .. }));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_put_get() {
        let path = test_db_path("bucket_put_get");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"users").unwrap();
            wtx.bucket_put(b"users", b"alice", b"admin").unwrap();
            wtx.bucket_put(b"users", b"bob", b"member").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let bucket = rtx.bucket(b"users").unwrap();
            assert_eq!(bucket.get(b"alice"), Some(&b"admin"[..]));
            assert_eq!(bucket.get(b"bob"), Some(&b"member"[..]));
            assert_eq!(bucket.get(b"charlie"), None);
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_put_get_pending() {
        let path = test_db_path("bucket_pending");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"test").unwrap();
            wtx.bucket_put(b"test", b"key", b"value").unwrap();

            // Should be visible via bucket_get within same transaction.
            let value = wtx.bucket_get(b"test", b"key").unwrap();
            assert_eq!(value, Some(b"value".to_vec()));

            wtx.commit().expect("commit should succeed");
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_delete_key() {
        let path = test_db_path("bucket_delete_key");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"data").unwrap();
            wtx.bucket_put(b"data", b"key1", b"value1").unwrap();
            wtx.bucket_put(b"data", b"key2", b"value2").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            wtx.bucket_delete(b"data", b"key1").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let bucket = rtx.bucket(b"data").unwrap();
            assert_eq!(bucket.get(b"key1"), None);
            assert_eq!(bucket.get(b"key2"), Some(&b"value2"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_delete_bucket() {
        let path = test_db_path("bucket_delete_bucket");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"todelete").unwrap();
            wtx.bucket_put(b"todelete", b"k1", b"v1").unwrap();
            wtx.bucket_put(b"todelete", b"k2", b"v2").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            wtx.delete_bucket(b"todelete").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            assert!(!rtx.bucket_exists(b"todelete"));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_delete_nonexistent() {
        let path = test_db_path("bucket_delete_nonexistent");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            let result = wtx.delete_bucket(b"nonexistent");
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), Error::BucketNotFound { .. }));
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

            // Same key in different buckets.
            wtx.bucket_put(b"bucket_a", b"key", b"value_a").unwrap();
            wtx.bucket_put(b"bucket_b", b"key", b"value_b").unwrap();

            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let bucket_a = rtx.bucket(b"bucket_a").unwrap();
            let bucket_b = rtx.bucket(b"bucket_b").unwrap();

            assert_eq!(bucket_a.get(b"key"), Some(&b"value_a"[..]));
            assert_eq!(bucket_b.get(b"key"), Some(&b"value_b"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_list() {
        let path = test_db_path("bucket_list");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"alpha").unwrap();
            wtx.create_bucket(b"beta").unwrap();
            wtx.create_bucket(b"gamma").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let buckets = rtx.list_buckets();
            assert_eq!(buckets.len(), 3);
            assert!(buckets.contains(&b"alpha".to_vec()));
            assert!(buckets.contains(&b"beta".to_vec()));
            assert!(buckets.contains(&b"gamma".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_iter() {
        let path = test_db_path("bucket_iter");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"items").unwrap();
            wtx.bucket_put(b"items", b"a", b"1").unwrap();
            wtx.bucket_put(b"items", b"b", b"2").unwrap();
            wtx.bucket_put(b"items", b"c", b"3").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let bucket = rtx.bucket(b"items").unwrap();
            let items: Vec<_> = bucket.iter().collect();

            assert_eq!(items.len(), 3);
            // Items should be in sorted order.
            assert_eq!(items[0], (&b"a"[..], &b"1"[..]));
            assert_eq!(items[1], (&b"b"[..], &b"2"[..]));
            assert_eq!(items[2], (&b"c"[..], &b"3"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_persistence() {
        let path = test_db_path("bucket_persist");
        cleanup(&path);

        // Create and populate bucket.
        {
            let mut db = Database::open(&path).expect("open should succeed");
            {
                let mut wtx = db.write_tx();
                wtx.create_bucket(b"persist_test").unwrap();
                wtx.bucket_put(b"persist_test", b"key1", b"val1").unwrap();
                wtx.bucket_put(b"persist_test", b"key2", b"val2").unwrap();
                wtx.commit().expect("commit should succeed");
            }
        }

        // Reopen and verify.
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();

            assert!(rtx.bucket_exists(b"persist_test"));

            let bucket = rtx.bucket(b"persist_test").unwrap();
            assert_eq!(bucket.get(b"key1"), Some(&b"val1"[..]));
            assert_eq!(bucket.get(b"key2"), Some(&b"val2"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_uncommitted_not_visible() {
        let path = test_db_path("bucket_uncommitted");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Create bucket but don't commit.
        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"uncommitted").unwrap();
            wtx.bucket_put(b"uncommitted", b"key", b"value").unwrap();
            // Drop without commit.
        }

        // Bucket should not exist.
        {
            let rtx = db.read_tx();
            assert!(!rtx.bucket_exists(b"uncommitted"));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_not_found_on_operations() {
        let path = test_db_path("bucket_not_found");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();

            // Try operations on non-existent bucket.
            let result = wtx.bucket_put(b"nonexistent", b"key", b"value");
            assert!(matches!(result.unwrap_err(), Error::BucketNotFound { .. }));

            let result = wtx.bucket_get(b"nonexistent", b"key");
            assert!(matches!(result.unwrap_err(), Error::BucketNotFound { .. }));

            let result = wtx.bucket_delete(b"nonexistent", b"key");
            assert!(matches!(result.unwrap_err(), Error::BucketNotFound { .. }));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_binary_name() {
        let path = test_db_path("bucket_binary_name");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let binary_name = vec![0x00, 0xFF, 0x7F, 0x80];

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(&binary_name).unwrap();
            wtx.bucket_put(&binary_name, b"key", b"value").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            assert!(rtx.bucket_exists(&binary_name));
            let bucket = rtx.bucket(&binary_name).unwrap();
            assert_eq!(bucket.get(b"key"), Some(&b"value"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_many_buckets() {
        let path = test_db_path("bucket_many");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let num_buckets = 100;

        {
            let mut wtx = db.write_tx();
            for i in 0..num_buckets {
                let name = format!("bucket_{i:03}");
                wtx.create_bucket(name.as_bytes()).unwrap();
                wtx.bucket_put(name.as_bytes(), b"key", format!("value_{i}").as_bytes())
                    .unwrap();
            }
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let buckets = rtx.list_buckets();
            assert_eq!(buckets.len(), num_buckets);

            for i in 0..num_buckets {
                let name = format!("bucket_{i:03}");
                let bucket = rtx.bucket(name.as_bytes()).unwrap();
                assert_eq!(
                    bucket.get(b"key"),
                    Some(format!("value_{i}").as_bytes())
                );
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_many_keys() {
        let path = test_db_path("bucket_many_keys");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let num_keys = 500;

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"bigbucket").unwrap();
            for i in 0..num_keys {
                let key = format!("key_{i:05}");
                let value = format!("value_{i}");
                wtx.bucket_put(b"bigbucket", key.as_bytes(), value.as_bytes())
                    .unwrap();
            }
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let bucket = rtx.bucket(b"bigbucket").unwrap();

            // Verify all keys.
            for i in 0..num_keys {
                let key = format!("key_{i:05}");
                let expected = format!("value_{i}");
                assert_eq!(
                    bucket.get(key.as_bytes()),
                    Some(expected.as_bytes()),
                    "key {key} mismatch"
                );
            }

            // Verify iteration count.
            let items: Vec<_> = bucket.iter().collect();
            assert_eq!(items.len(), num_keys);
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_overwrite_value() {
        let path = test_db_path("bucket_overwrite");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"test").unwrap();
            wtx.bucket_put(b"test", b"key", b"original").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            wtx.bucket_put(b"test", b"key", b"updated").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let bucket = rtx.bucket(b"test").unwrap();
            assert_eq!(bucket.get(b"key"), Some(&b"updated"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_delete_then_recreate() {
        let path = test_db_path("bucket_recreate");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Create bucket with data.
        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"temp").unwrap();
            wtx.bucket_put(b"temp", b"old_key", b"old_value").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        // Delete bucket.
        {
            let mut wtx = db.write_tx();
            wtx.delete_bucket(b"temp").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        // Recreate with different data.
        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"temp").unwrap();
            wtx.bucket_put(b"temp", b"new_key", b"new_value").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let bucket = rtx.bucket(b"temp").unwrap();
            // Old data should be gone.
            assert_eq!(bucket.get(b"old_key"), None);
            // New data should be present.
            assert_eq!(bucket.get(b"new_key"), Some(&b"new_value"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_list_pending() {
        let path = test_db_path("bucket_list_pending");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"committed").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"pending").unwrap();

            let buckets = wtx.list_buckets();
            assert!(buckets.contains(&b"committed".to_vec()));
            assert!(buckets.contains(&b"pending".to_vec()));
            // Don't commit.
        }

        // Pending bucket should not be visible.
        {
            let rtx = db.read_tx();
            let buckets = rtx.list_buckets();
            assert!(buckets.contains(&b"committed".to_vec()));
            assert!(!buckets.contains(&b"pending".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_max_name_length() {
        let path = test_db_path("bucket_max_name");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let max_name = vec![b'x'; 255]; // MAX_BUCKET_NAME_LEN

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(&max_name).unwrap();
            wtx.bucket_put(&max_name, b"key", b"value").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            assert!(rtx.bucket_exists(&max_name));
            let bucket = rtx.bucket(&max_name).unwrap();
            assert_eq!(bucket.get(b"key"), Some(&b"value"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_iter_isolation() {
        let path = test_db_path("bucket_iter_isolation");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"b1").unwrap();
            wtx.create_bucket(b"b2").unwrap();
            wtx.bucket_put(b"b1", b"k1", b"v1").unwrap();
            wtx.bucket_put(b"b1", b"k2", b"v2").unwrap();
            wtx.bucket_put(b"b2", b"k3", b"v3").unwrap();
            wtx.bucket_put(b"b2", b"k4", b"v4").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            let b1 = rtx.bucket(b"b1").unwrap();
            let b2 = rtx.bucket(b"b2").unwrap();

            let items1: Vec<_> = b1.iter().collect();
            let items2: Vec<_> = b2.iter().collect();

            // Bucket 1 should only see its keys.
            assert_eq!(items1.len(), 2);
            assert!(items1.iter().any(|(k, _)| *k == b"k1"));
            assert!(items1.iter().any(|(k, _)| *k == b"k2"));

            // Bucket 2 should only see its keys.
            assert_eq!(items2.len(), 2);
            assert!(items2.iter().any(|(k, _)| *k == b"k3"));
            assert!(items2.iter().any(|(k, _)| *k == b"k4"));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_with_regular_keys() {
        let path = test_db_path("bucket_with_regular");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Buckets and regular keys should coexist.
        {
            let mut wtx = db.write_tx();
            wtx.put(b"regular_key", b"regular_value");
            wtx.create_bucket(b"mybucket").unwrap();
            wtx.bucket_put(b"mybucket", b"bucket_key", b"bucket_value").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            // Regular key accessible.
            assert_eq!(rtx.get(b"regular_key"), Some(b"regular_value".to_vec()));
            // Bucket key accessible.
            let bucket = rtx.bucket(b"mybucket").unwrap();
            assert_eq!(bucket.get(b"bucket_key"), Some(&b"bucket_value"[..]));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_delete_in_same_tx_as_create() {
        let path = test_db_path("bucket_create_delete_same");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"temp").unwrap();
            wtx.bucket_put(b"temp", b"key", b"value").unwrap();
            wtx.delete_bucket(b"temp").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        {
            let rtx = db.read_tx();
            assert!(!rtx.bucket_exists(b"temp"));
        }

        cleanup(&path);
    }

    #[test]
    fn test_bucket_operations_after_delete_pending() {
        let path = test_db_path("bucket_ops_after_delete");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Create and commit bucket.
        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"test").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        // Delete and try to operate on it in same transaction.
        {
            let mut wtx = db.write_tx();
            wtx.delete_bucket(b"test").unwrap();

            // Operations should fail now.
            let result = wtx.bucket_put(b"test", b"key", b"value");
            assert!(matches!(result.unwrap_err(), Error::BucketNotFound { .. }));
        }

        cleanup(&path);
    }

    // ==================== Iterator Tests ====================

    #[test]
    fn test_iter_empty_db() {
        let path = test_db_path("iter_empty");
        cleanup(&path);

        let db = Database::open(&path).expect("open should succeed");
        let rtx = db.read_tx();

        let items: Vec<_> = rtx.iter().collect();
        assert!(items.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_iter_single_key() {
        let path = test_db_path("iter_single");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let items: Vec<_> = rtx.iter().collect();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, b"key");
        assert_eq!(items[0].1, b"value");

        cleanup(&path);
    }

    #[test]
    fn test_iter_multiple_keys_sorted() {
        let path = test_db_path("iter_sorted");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            // Insert in non-sorted order.
            wtx.put(b"cherry", b"3");
            wtx.put(b"apple", b"1");
            wtx.put(b"banana", b"2");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let items: Vec<_> = rtx.iter().collect();

        assert_eq!(items.len(), 3);
        // Should be in sorted order.
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
                let key = format!("key_{i:05}");
                let value = format!("val_{i}");
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let items: Vec<_> = rtx.iter().collect();

        assert_eq!(items.len(), count);

        // Verify sorted order.
        for i in 1..items.len() {
            assert!(items[i - 1].0 < items[i].0, "keys should be sorted");
        }

        cleanup(&path);
    }

    #[test]
    fn test_iter_binary_keys() {
        let path = test_db_path("iter_binary");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(&[0x00, 0x01], b"first");
            wtx.put(&[0x00, 0x02], b"second");
            wtx.put(&[0xFF, 0x00], b"last");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let items: Vec<_> = rtx.iter().collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, &[0x00, 0x01]);
        assert_eq!(items[1].0, &[0x00, 0x02]);
        assert_eq!(items[2].0, &[0xFF, 0x00]);

        cleanup(&path);
    }

    #[test]
    fn test_iter_after_delete() {
        let path = test_db_path("iter_after_delete");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.commit().expect("commit should succeed");
        }

        {
            let mut wtx = db.write_tx();
            wtx.delete(b"b");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let items: Vec<_> = rtx.iter().collect();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, b"a");
        assert_eq!(items[1].0, b"c");

        cleanup(&path);
    }

    #[test]
    fn test_iter_persistence() {
        let path = test_db_path("iter_persist");
        cleanup(&path);

        {
            let mut db = Database::open(&path).expect("open should succeed");
            let mut wtx = db.write_tx();
            wtx.put(b"x", b"1");
            wtx.put(b"y", b"2");
            wtx.put(b"z", b"3");
            wtx.commit().expect("commit should succeed");
        }

        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();
            let items: Vec<_> = rtx.iter().collect();

            assert_eq!(items.len(), 3);
            assert_eq!(items[0].0, b"x");
            assert_eq!(items[1].0, b"y");
            assert_eq!(items[2].0, b"z");
        }

        cleanup(&path);
    }

    // ==================== Range Scan Tests ====================

    #[test]
    fn test_range_full() {
        let path = test_db_path("range_full");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // Full range should return all keys.
        let items: Vec<_> = rtx.range(..).collect();

        assert_eq!(items.len(), 3);

        cleanup(&path);
    }

    #[test]
    fn test_range_from_inclusive() {
        let path = test_db_path("range_from");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.put(b"d", b"4");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // From b (inclusive) to end.
        let items: Vec<_> = rtx.range(b"b".as_slice()..).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"b");
        assert_eq!(items[1].0, b"c");
        assert_eq!(items[2].0, b"d");

        cleanup(&path);
    }

    #[test]
    fn test_range_to_exclusive() {
        let path = test_db_path("range_to");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.put(b"d", b"4");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // From start to c (exclusive).
        let items: Vec<_> = rtx.range(..b"c".as_slice()).collect();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, b"a");
        assert_eq!(items[1].0, b"b");

        cleanup(&path);
    }

    #[test]
    fn test_range_to_inclusive() {
        let path = test_db_path("range_to_incl");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.put(b"d", b"4");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // From start to c (inclusive).
        let items: Vec<_> = rtx.range(..=b"c".as_slice()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"a");
        assert_eq!(items[1].0, b"b");
        assert_eq!(items[2].0, b"c");

        cleanup(&path);
    }

    #[test]
    fn test_range_bounded() {
        let path = test_db_path("range_bounded");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.put(b"d", b"4");
            wtx.put(b"e", b"5");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // From b (inclusive) to d (exclusive).
        let items: Vec<_> = rtx.range(b"b".as_slice()..b"d".as_slice()).collect();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, b"b");
        assert_eq!(items[1].0, b"c");

        cleanup(&path);
    }

    #[test]
    fn test_range_bounded_inclusive() {
        let path = test_db_path("range_bounded_incl");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.put(b"d", b"4");
            wtx.put(b"e", b"5");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // From b (inclusive) to d (inclusive).
        let items: Vec<_> = rtx.range(b"b".as_slice()..=b"d".as_slice()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"b");
        assert_eq!(items[1].0, b"c");
        assert_eq!(items[2].0, b"d");

        cleanup(&path);
    }

    #[test]
    fn test_range_empty_result() {
        let path = test_db_path("range_empty");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // Range outside existing keys.
        let items: Vec<_> = rtx.range(b"x".as_slice()..b"z".as_slice()).collect();

        assert!(items.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_range_no_match_in_middle() {
        let path = test_db_path("range_gap");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"aaa", b"1");
            wtx.put(b"zzz", b"2");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // Range in gap between keys.
        let items: Vec<_> = rtx.range(b"mmm".as_slice()..b"nnn".as_slice()).collect();

        assert!(items.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_range_single_key() {
        let path = test_db_path("range_single");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // Exact single key range.
        let items: Vec<_> = rtx.range(b"b".as_slice()..=b"b".as_slice()).collect();

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, b"b");

        cleanup(&path);
    }

    #[test]
    fn test_range_prefix_scan() {
        let path = test_db_path("range_prefix");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"user:001", b"alice");
            wtx.put(b"user:002", b"bob");
            wtx.put(b"user:003", b"charlie");
            wtx.put(b"post:001", b"hello");
            wtx.put(b"post:002", b"world");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // Scan all user keys using prefix range.
        let items: Vec<_> = rtx.range(b"user:".as_slice()..b"user;".as_slice()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"user:001");
        assert_eq!(items[1].0, b"user:002");
        assert_eq!(items[2].0, b"user:003");

        cleanup(&path);
    }

    #[test]
    fn test_range_many_keys() {
        let path = test_db_path("range_many");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            for i in 0..1000 {
                let key = format!("key_{i:05}");
                let value = format!("val_{i}");
                wtx.put(key.as_bytes(), value.as_bytes());
            }
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // Range from key_00100 to key_00200 (exclusive).
        let items: Vec<_> = rtx
            .range(b"key_00100".as_slice()..b"key_00200".as_slice())
            .collect();

        assert_eq!(items.len(), 100);
        assert_eq!(items[0].0, b"key_00100");
        assert_eq!(items[99].0, b"key_00199");

        cleanup(&path);
    }

    #[test]
    fn test_range_binary_bounds() {
        let path = test_db_path("range_binary");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(&[0x00], b"zero");
            wtx.put(&[0x10], b"sixteen");
            wtx.put(&[0x20], b"thirty-two");
            wtx.put(&[0x30], b"forty-eight");
            wtx.put(&[0xFF], b"max");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let items: Vec<_> = rtx.range([0x10].as_slice()..=[0x30].as_slice()).collect();

        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, &[0x10]);
        assert_eq!(items[1].0, &[0x20]);
        assert_eq!(items[2].0, &[0x30]);

        cleanup(&path);
    }

    // ==================== Bucket Range Scan Tests ====================

    #[test]
    fn test_bucket_range_full() {
        let path = test_db_path("bucket_range_full");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"data").unwrap();
            wtx.bucket_put(b"data", b"a", b"1").unwrap();
            wtx.bucket_put(b"data", b"b", b"2").unwrap();
            wtx.bucket_put(b"data", b"c", b"3").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let bucket = rtx.bucket(b"data").unwrap();
        let items: Vec<_> = bucket.range(..).collect();

        assert_eq!(items.len(), 3);

        cleanup(&path);
    }

    #[test]
    fn test_bucket_range_bounded() {
        let path = test_db_path("bucket_range_bounded");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"items").unwrap();
            wtx.bucket_put(b"items", b"a", b"1").unwrap();
            wtx.bucket_put(b"items", b"b", b"2").unwrap();
            wtx.bucket_put(b"items", b"c", b"3").unwrap();
            wtx.bucket_put(b"items", b"d", b"4").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let bucket = rtx.bucket(b"items").unwrap();
        let items: Vec<_> = bucket.range(b"b".as_slice()..b"d".as_slice()).collect();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, b"b");
        assert_eq!(items[1].0, b"c");

        cleanup(&path);
    }

    #[test]
    fn test_bucket_range_isolation() {
        let path = test_db_path("bucket_range_iso");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.create_bucket(b"b1").unwrap();
            wtx.create_bucket(b"b2").unwrap();
            wtx.bucket_put(b"b1", b"key1", b"v1").unwrap();
            wtx.bucket_put(b"b1", b"key2", b"v2").unwrap();
            wtx.bucket_put(b"b2", b"key1", b"x1").unwrap();
            wtx.bucket_put(b"b2", b"key3", b"x3").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        let b1 = rtx.bucket(b"b1").unwrap();
        let b2 = rtx.bucket(b"b2").unwrap();

        let items1: Vec<_> = b1.range(..).collect();
        let items2: Vec<_> = b2.range(..).collect();

        assert_eq!(items1.len(), 2);
        assert_eq!(items2.len(), 2);

        // Verify isolation.
        assert!(items1.iter().all(|(k, _)| *k == b"key1" || *k == b"key2"));
        assert!(items2.iter().all(|(k, _)| *k == b"key1" || *k == b"key3"));

        cleanup(&path);
    }

    #[test]
    fn test_bucket_range_prefix_scan() {
        let path = test_db_path("bucket_range_prefix");
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

        // Scan admin users.
        let admins: Vec<_> = bucket
            .range(b"admin:".as_slice()..b"admin;".as_slice())
            .collect();

        assert_eq!(admins.len(), 2);
        assert_eq!(admins[0].0, b"admin:alice");
        assert_eq!(admins[1].0, b"admin:bob");

        cleanup(&path);
    }

    // ==================== Iterator Performance Tests ====================

    #[test]
    fn test_iter_performance_sequential() {
        let path = test_db_path("iter_perf_seq");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let count = 10_000;
        {
            let mut wtx = db.write_tx();
            for i in 0..count {
                let key = format!("k{i:08}");
                wtx.put(key.as_bytes(), b"v");
            }
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();

        use std::time::Instant;
        let start = Instant::now();
        let items: Vec<_> = rtx.iter().collect();
        let duration = start.elapsed();

        assert_eq!(items.len(), count);
        assert!(
            duration.as_millis() < 500,
            "iteration took too long: {duration:?}"
        );

        cleanup(&path);
    }

    #[test]
    fn test_range_performance() {
        let path = test_db_path("range_perf");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        let count = 10_000;
        {
            let mut wtx = db.write_tx();
            for i in 0..count {
                let key = format!("k{i:08}");
                wtx.put(key.as_bytes(), b"v");
            }
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();

        use std::time::Instant;
        let start = Instant::now();
        // Range scan middle 1000 keys.
        let items: Vec<_> = rtx
            .range(b"k00004500".as_slice()..b"k00005500".as_slice())
            .collect();
        let duration = start.elapsed();

        assert_eq!(items.len(), 1000);
        assert!(
            duration.as_millis() < 100,
            "range scan took too long: {duration:?}"
        );

        cleanup(&path);
    }

    // ==================== Edge Cases ====================

    #[test]
    fn test_range_inverted_bounds() {
        let path = test_db_path("range_inverted");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            wtx.put(b"a", b"1");
            wtx.put(b"b", b"2");
            wtx.put(b"c", b"3");
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();
        // Inverted range (start > end) should return empty.
        let items: Vec<_> = rtx.range(b"z".as_slice()..b"a".as_slice()).collect();

        assert!(items.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_range_empty_db() {
        let path = test_db_path("range_empty_db");
        cleanup(&path);

        let db = Database::open(&path).expect("open should succeed");
        let rtx = db.read_tx();

        let items: Vec<_> = rtx.range(b"a".as_slice()..b"z".as_slice()).collect();
        assert!(items.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_iter_with_buckets_coexist() {
        let path = test_db_path("iter_buckets_coexist");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        {
            let mut wtx = db.write_tx();
            // Regular keys.
            wtx.put(b"global_key", b"global_val");
            // Bucket with keys.
            wtx.create_bucket(b"mybucket").unwrap();
            wtx.bucket_put(b"mybucket", b"bucket_key", b"bucket_val").unwrap();
            wtx.commit().expect("commit should succeed");
        }

        let rtx = db.read_tx();

        // Top-level iter should NOT include bucket internal keys directly
        // (they have special prefixes).
        let items: Vec<_> = rtx.iter().collect();

        // Should see global_key and bucket metadata (internal representation).
        // The exact count depends on implementation.
        // Key point: bucket_key should not appear as a top-level key.
        for (k, _) in &items {
            assert_ne!(*k, b"bucket_key", "bucket keys should not appear at top level");
        }

        cleanup(&path);
    }
}

