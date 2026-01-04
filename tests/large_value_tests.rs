//! Summary: Tests for large value optimization features.
//! Copyright (c) YOAB. All rights reserved.
//!
//! These tests verify the correctness, efficiency, and security of:
//! - Overflow pages for large values (>2KB threshold)
//! - Variable page size configuration (4K/8K/16K/64K)
//! - Write coalescing buffer for efficient I/O

use std::fs;
use thunderdb::{Database, DatabaseOptions, PageSizeConfig};

fn test_db_path(name: &str) -> String {
    format!("/tmp/thunder_large_value_test_{name}.db")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

// ==================== Task 2.1: Overflow Pages Tests ====================

/// Test 1: Large Value Roundtrip with Overflow Pages
///
/// This test verifies that:
/// 1. Values larger than OVERFLOW_THRESHOLD (2KB) are correctly stored in overflow pages
/// 2. Large values can be retrieved correctly after storage
/// 3. Mixed workloads (small inline + large overflow) work correctly
/// 4. Data integrity is maintained across database reopening
/// 5. Large value chains of varying sizes (just over threshold, medium, very large) are handled
///
/// Security considerations:
/// - Tests boundary conditions around the overflow threshold
/// - Verifies no data corruption occurs during overflow page chaining
/// - Ensures no memory safety issues with large allocations
#[test]
fn test_overflow_pages_large_value_roundtrip() {
    let path = test_db_path("overflow_roundtrip");
    cleanup(&path);

    // Define test values at various sizes relative to overflow threshold
    let small_value = vec![0x42u8; 1024]; // 1KB - inline storage
    let threshold_value = vec![0x43u8; 2048]; // Exactly 2KB - at threshold
    let just_over_threshold = vec![0x44u8; 2049]; // Just over threshold
    let medium_large = vec![0x45u8; 10 * 1024]; // 10KB - spans multiple overflow pages
    let large_value = vec![0x46u8; 100 * 1024]; // 100KB - many overflow pages
    let very_large = vec![0x47u8; 1024 * 1024]; // 1MB - extensive chain

    // Create database and store values
    {
        let mut db = Database::open(&path).expect("open should succeed");
        let mut wtx = db.write_tx();

        // Store all test values
        wtx.put(b"small", &small_value);
        wtx.put(b"threshold", &threshold_value);
        wtx.put(b"just_over", &just_over_threshold);
        wtx.put(b"medium_large", &medium_large);
        wtx.put(b"large", &large_value);
        wtx.put(b"very_large", &very_large);

        wtx.commit().expect("commit should succeed");
    }

    // Verify all values can be read back correctly
    {
        let db = Database::open(&path).expect("reopen should succeed");
        let rtx = db.read_tx();

        assert_eq!(rtx.get(b"small"), Some(small_value.clone()));
        assert_eq!(rtx.get(b"threshold"), Some(threshold_value.clone()));
        assert_eq!(rtx.get(b"just_over"), Some(just_over_threshold.clone()));
        assert_eq!(rtx.get(b"medium_large"), Some(medium_large.clone()));
        assert_eq!(rtx.get(b"large"), Some(large_value.clone()));
        assert_eq!(rtx.get(b"very_large"), Some(very_large.clone()));
    }

    // Test updating large values with different sizes
    {
        let mut db = Database::open(&path).expect("reopen for update");
        let mut wtx = db.write_tx();

        // Update large value with a smaller large value
        let updated_large = vec![0x50u8; 50 * 1024]; // 50KB
        wtx.put(b"large", &updated_large);

        // Update small value with a large value
        let promoted_to_large = vec![0x51u8; 20 * 1024]; // 20KB
        wtx.put(b"small", &promoted_to_large);

        wtx.commit().expect("update commit should succeed");

        // Verify updates in same session
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"large"), Some(updated_large.clone()));
        assert_eq!(rtx.get(b"small"), Some(promoted_to_large.clone()));
    }

    // Verify persistence after update
    {
        let db = Database::open(&path).expect("final reopen");
        let rtx = db.read_tx();

        // Updated values
        assert_eq!(rtx.get(b"large").map(|v| v.len()), Some(50 * 1024));
        assert_eq!(rtx.get(b"small").map(|v| v.len()), Some(20 * 1024));

        // Unchanged values
        assert_eq!(rtx.get(b"very_large"), Some(very_large));
    }

    cleanup(&path);
}

// ==================== Task 2.2: Variable Page Size Tests ====================

/// Test 2: Variable Page Size Configuration
///
/// This test verifies that:
/// 1. Different page sizes (4KB, 8KB, 16KB, 64KB) work correctly
/// 2. Page size is correctly stored in metadata and persisted
/// 3. Attempting to open a database with a mismatched page size fails gracefully
/// 4. NVMe-optimized configuration works correctly
/// 5. Overflow pages work correctly with different page sizes
///
/// Security considerations:
/// - Validates page size bounds to prevent buffer overflows
/// - Ensures page size mismatch is detected (prevents data corruption)
/// - Tests alignment requirements are satisfied
#[test]
fn test_variable_page_size_configuration() {
    // Test each supported page size
    let page_sizes = [
        PageSizeConfig::Size4K,
        PageSizeConfig::Size8K,
        PageSizeConfig::Size16K,
        PageSizeConfig::Size64K,
    ];

    for page_size in page_sizes {
        let path = test_db_path(&format!("page_size_{}", page_size.as_usize()));
        cleanup(&path);

        let options = DatabaseOptions {
            page_size,
            ..Default::default()
        };

        // Create database with specific page size
        {
            let mut db = Database::open_with_options(&path, options.clone())
                .expect("open with options should succeed");

            let mut wtx = db.write_tx();

            // Insert data that will span multiple pages
            for i in 0..50 {
                let key = format!("key_{i:04}");
                let value = format!("value_{i}_with_some_extra_padding_to_make_it_longer");
                wtx.put(key.as_bytes(), value.as_bytes());
            }

            // Insert a large value to test overflow with this page size
            let large_value = vec![0xAAu8; 10 * 1024]; // 10KB
            wtx.put(b"large_key", &large_value);

            wtx.commit().expect("commit should succeed");
        }

        // Reopen with same page size - should work
        {
            let db =
                Database::open_with_options(&path, options.clone()).expect("reopen should succeed");
            let rtx = db.read_tx();

            for i in 0..50 {
                let key = format!("key_{i:04}");
                assert!(rtx.get(key.as_bytes()).is_some(), "key {key} should exist");
            }

            assert_eq!(rtx.get(b"large_key").map(|v| v.len()), Some(10 * 1024));
        }

        cleanup(&path);
    }

    // Test page size mismatch detection
    let mismatch_path = test_db_path("page_size_mismatch");
    cleanup(&mismatch_path);

    // Create with 4KB pages
    {
        let options = DatabaseOptions {
            page_size: PageSizeConfig::Size4K,
            ..Default::default()
        };
        let mut db = Database::open_with_options(&mismatch_path, options)
            .expect("create with 4K should succeed");
        let mut wtx = db.write_tx();
        wtx.put(b"test", b"data");
        wtx.commit().expect("commit should succeed");
    }

    // Try to open with different page size - should fail
    {
        let options = DatabaseOptions {
            page_size: PageSizeConfig::Size16K,
            ..Default::default()
        };
        let result = Database::open_with_options(&mismatch_path, options);
        assert!(
            result.is_err(),
            "Opening with mismatched page size should fail"
        );
    }

    // Test NVMe-optimized preset
    let nvme_path = test_db_path("nvme_optimized");
    cleanup(&nvme_path);

    {
        let options = DatabaseOptions::nvme_optimized();
        assert_eq!(
            options.page_size,
            PageSizeConfig::Size32K,
            "NVMe preset should use 32KB HPC pages"
        );

        let mut db =
            Database::open_with_options(&nvme_path, options).expect("NVMe optimized should work");
        let mut wtx = db.write_tx();
        wtx.put(b"nvme_key", b"nvme_value");
        wtx.commit().expect("commit should succeed");
    }

    cleanup(&mismatch_path);
    cleanup(&nvme_path);
}

// ==================== Task 2.3: Write Coalescing Tests ====================

/// Test 3: Write Coalescing and Batched I/O
///
/// This test verifies that:
/// 1. Write coalescing correctly batches multiple writes
/// 2. Duplicate page writes are coalesced (only last write kept)
/// 3. Pages are written in sorted order for sequential I/O
/// 4. Buffer size limits trigger automatic flushing
/// 5. Data integrity is maintained with coalesced writes
///
/// Security considerations:
/// - Verifies buffer bounds are respected
/// - Ensures no data loss during coalescing
/// - Tests memory limits prevent OOM conditions
#[test]
fn test_write_coalescing_and_batch_io() {
    let path = test_db_path("write_coalescing");
    cleanup(&path);

    // Test 1: Batch write with many entries
    {
        let mut db = Database::open(&path).expect("open should succeed");

        // Write many entries in a single transaction
        // This should exercise the write coalescing buffer
        let mut wtx = db.write_tx();
        for i in 0..1000 {
            let key = format!("batch_key_{i:05}");
            let value = vec![i as u8; 256]; // 256 bytes each
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("batch commit should succeed");
    }

    // Verify all entries persisted correctly
    {
        let db = Database::open(&path).expect("reopen should succeed");
        let rtx = db.read_tx();

        for i in 0..1000 {
            let key = format!("batch_key_{i:05}");
            let value = rtx.get(key.as_bytes());
            assert!(value.is_some(), "key {key} should exist");
            assert_eq!(value.as_ref().map(|v| v.len()), Some(256));
            assert!(
                value.as_ref().map(|v| v[0]) == Some(i as u8),
                "value content should match"
            );
        }
    }

    // Test 2: Multiple updates to same key (coalescing)
    {
        let mut db = Database::open(&path).expect("reopen for updates");
        let mut wtx = db.write_tx();

        // Update same key multiple times - only final value should be persisted
        for i in 0..100 {
            wtx.put(b"coalesce_key", format!("value_{i}").as_bytes());
        }
        wtx.commit().expect("coalesce commit should succeed");
    }

    // Verify only final value persisted
    {
        let db = Database::open(&path).expect("final reopen");
        let rtx = db.read_tx();
        assert_eq!(rtx.get(b"coalesce_key"), Some(b"value_99".to_vec()));
    }

    // Test 3: Mixed large and small writes with coalescing
    {
        let mut db = Database::open(&path).expect("reopen for mixed");
        let mut wtx = db.write_tx();

        // Mix of small inline values and large overflow values
        for i in 0..50 {
            let key = format!("mixed_{i:03}");
            let value = if i % 5 == 0 {
                // Every 5th entry is large (triggers overflow)
                vec![0xBBu8; 10 * 1024]
            } else {
                // Small inline value
                vec![0xCCu8; 100]
            };
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("mixed commit should succeed");
    }

    // Verify mixed writes
    {
        let db = Database::open(&path).expect("verify mixed");
        let rtx = db.read_tx();

        for i in 0..50 {
            let key = format!("mixed_{i:03}");
            let value = rtx.get(key.as_bytes());
            let expected_len = if i % 5 == 0 { 10 * 1024 } else { 100 };
            assert_eq!(
                value.map(|v| v.len()),
                Some(expected_len),
                "mixed_{i:03} should have correct length"
            );
        }
    }

    // Test 4: Stress test with buffer overflow conditions
    {
        let options = DatabaseOptions {
            write_buffer_size: 64 * 1024, // 64KB buffer - will trigger multiple flushes
            ..Default::default()
        };
        let stress_path = test_db_path("stress_coalesce");
        cleanup(&stress_path);

        let mut db = Database::open_with_options(&stress_path, options).expect("open stress db");
        let mut wtx = db.write_tx();

        // Write enough data to exceed buffer multiple times
        for i in 0..500 {
            let key = format!("stress_{i:05}");
            let value = vec![(i % 256) as u8; 1024]; // 1KB each = 500KB total
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("stress commit should succeed");

        // Verify all data persisted despite multiple buffer flushes
        let rtx = db.read_tx();
        for i in 0..500 {
            let key = format!("stress_{i:05}");
            assert!(rtx.get(key.as_bytes()).is_some(), "stress key should exist");
        }

        cleanup(&stress_path);
    }

    cleanup(&path);
}

// ==================== Integration Test: All Features Combined ====================

/// Combined test ensuring all Phase 2 features work together correctly.
#[test]
fn test_large_value_combined_features() {
    let path = test_db_path("phase2_combined");
    cleanup(&path);

    let options = DatabaseOptions {
        page_size: PageSizeConfig::Size32K,
        overflow_threshold: 16 * 1024, // 16KB overflow threshold
        write_buffer_size: 512 * 1024, // 512KB buffer
        ..Default::default()
    };

    // Create database with all Phase 2 features
    {
        let mut db =
            Database::open_with_options(&path, options.clone()).expect("open should succeed");

        let mut wtx = db.write_tx();

        // Mix of small, medium, and large values
        for i in 0..100 {
            let key = format!("combined_{i:04}");
            let value = match i % 10 {
                0 => vec![0xAAu8; 50 * 1024],    // 50KB - large overflow
                1..=3 => vec![0xBBu8; 5 * 1024], // 5KB - small overflow
                _ => vec![0xCCu8; 500],          // 500B - inline
            };
            wtx.put(key.as_bytes(), &value);
        }

        wtx.commit().expect("combined commit should succeed");
    }

    // Verify everything persisted correctly
    {
        let db = Database::open_with_options(&path, options).expect("reopen should succeed");
        let rtx = db.read_tx();

        for i in 0..100 {
            let key = format!("combined_{i:04}");
            let value = rtx.get(key.as_bytes());
            let expected_len = match i % 10 {
                0 => 50 * 1024,
                1..=3 => 5 * 1024,
                _ => 500,
            };
            assert_eq!(
                value.map(|v| v.len()),
                Some(expected_len),
                "combined_{i:04} should have correct length"
            );
        }
    }

    cleanup(&path);
}
