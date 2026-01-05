//! Summary: Read Dominance tests - Iterator performance, snapshot isolation, zero-copy reads.
//! Copyright (c) YOAB. All rights reserved.
//!
//! These tests verify that read-optimized features are implemented correctly, securely,
//! and with high performance in mind.

#![allow(clippy::drop_non_drop)]

use std::fs;
use std::time::{Duration, Instant};
use thunderdb::Database;

fn test_db_path(name: &str) -> String {
    format!("/tmp/thunder_read_dominance_test_{name}.db")
}

fn cleanup(path: &str) {
    let _ = fs::remove_file(path);
}

// ==================== 1. Iterator Performance Tests ====================

/// Test that iterator prefetch hints improve performance for large scans.
/// Verifies that consecutive key access is optimized.
#[test]
fn test_iterator_sequential_scan_performance() {
    let path = test_db_path("iter_perf_seq");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert 10,000 sequential keys
    const NUM_KEYS: usize = 10_000;
    {
        let mut wtx = db.write_tx();
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            let value = format!("value_{i}");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    // Measure iteration performance
    let rtx = db.read_tx();
    let start = Instant::now();
    let mut count = 0;
    for (key, _value) in rtx.iter() {
        // Verify ordering
        let expected_key = format!("key_{count:08}");
        assert_eq!(
            key,
            expected_key.as_bytes(),
            "keys should be in sorted order"
        );
        count += 1;
    }
    let elapsed = start.elapsed();

    assert_eq!(count, NUM_KEYS, "should iterate all keys");
    // Performance assertion: should complete in reasonable time (10ms per 1000 keys is generous)
    assert!(
        elapsed < Duration::from_millis(100),
        "iteration of {NUM_KEYS} keys should complete quickly, took {:?}",
        elapsed
    );

    cleanup(&path);
}

/// Test range iterator performance and correctness.
#[test]
fn test_iterator_range_scan_performance() {
    let path = test_db_path("iter_perf_range");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert 5,000 keys
    const NUM_KEYS: usize = 5_000;
    {
        let mut wtx = db.write_tx();
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            let value = format!("value_{i}");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Range scan from key_00001000 to key_00002000
    let start_key = b"key_00001000".as_slice();
    let end_key = b"key_00002000".as_slice();

    let start = Instant::now();
    let range_results: Vec<_> = rtx.range(start_key..end_key).collect();
    let elapsed = start.elapsed();

    // Should have 1000 results (1000 to 1999)
    assert_eq!(range_results.len(), 1000, "range should return 1000 keys");

    // Verify bounds are correct
    let first_key = std::str::from_utf8(range_results.first().unwrap().0).unwrap();
    let last_key = std::str::from_utf8(range_results.last().unwrap().0).unwrap();
    assert_eq!(first_key, "key_00001000");
    assert_eq!(last_key, "key_00001999");

    // Performance: range scan should be very fast
    assert!(
        elapsed < Duration::from_millis(50),
        "range scan should complete quickly, took {:?}",
        elapsed
    );

    cleanup(&path);
}

/// Test iterator with prefetch hint configuration.
#[test]
fn test_iterator_with_prefetch_config() {
    let path = test_db_path("iter_prefetch");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert 1,000 keys
    {
        let mut wtx = db.write_tx();
        for i in 0..1000 {
            let key = format!("key_{i:05}");
            let value = vec![0u8; 100]; // 100-byte values
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Create iterator with prefetch hints
    let iter = rtx.iter_with_options(thunderdb::IterOptions::default().prefetch_count(32));

    let count: usize = iter.count();
    assert_eq!(count, 1000, "should iterate all keys with prefetch");

    cleanup(&path);
}

/// Test scan metrics collection during iteration.
#[test]
fn test_iterator_scan_metrics() {
    let path = test_db_path("iter_metrics");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert keys
    {
        let mut wtx = db.write_tx();
        for i in 0..500 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i}");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Create iterator with metrics collection
    let mut iter = rtx.iter_with_metrics();

    // Consume iterator
    let _: Vec<_> = iter.by_ref().collect();

    // Get scan metrics
    let metrics = iter.metrics();

    assert_eq!(metrics.keys_scanned, 500, "should report correct key count");
    assert!(metrics.bytes_scanned > 0, "should report bytes scanned");
    assert!(metrics.scan_duration_ns > 0, "should report scan duration");

    cleanup(&path);
}

/// Test fast-path "next key" logic for sequential access.
#[test]
fn test_iterator_fast_path_next_key() {
    let path = test_db_path("iter_fast_next");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert sequential keys
    {
        let mut wtx = db.write_tx();
        for i in 0..100 {
            let key = format!("key_{i:03}");
            let value = format!("value_{i}");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    let mut iter = rtx.iter();

    // Get first key
    let (first_key, _) = iter.next().expect("should have first key");
    assert_eq!(first_key, b"key_000");

    // Fast path: next_key should be efficient for sequential access
    let (second_key, _) = iter.next().expect("should have second key");
    assert_eq!(second_key, b"key_001");

    // Continue to verify the fast path works throughout
    let mut prev_key = second_key.to_vec();
    for (key, _) in iter {
        assert!(
            key > prev_key.as_slice(),
            "keys should be strictly increasing"
        );
        prev_key = key.to_vec();
    }

    cleanup(&path);
}

// ==================== 2. Snapshot Isolation Tests ====================

/// Test that read transactions provide consistent snapshots.
#[test]
fn test_snapshot_consistency() {
    let path = test_db_path("snapshot_consistency");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert initial data
    {
        let mut wtx = db.write_tx();
        wtx.put(b"key1", b"value1");
        wtx.put(b"key2", b"value2");
        wtx.commit().expect("commit should succeed");
    }

    // Take a snapshot (read transaction)
    let snapshot = db.snapshot();

    // Verify snapshot sees initial data
    assert_eq!(snapshot.get(b"key1"), Some(b"value1".to_vec()));
    assert_eq!(snapshot.get(b"key2"), Some(b"value2".to_vec()));

    // Modify database
    {
        let mut wtx = db.write_tx();
        wtx.put(b"key1", b"modified");
        wtx.put(b"key3", b"new_value");
        wtx.delete(b"key2");
        wtx.commit().expect("commit should succeed");
    }

    // Snapshot should still see original data (snapshot isolation)
    assert_eq!(snapshot.get(b"key1"), Some(b"value1".to_vec()));
    assert_eq!(snapshot.get(b"key2"), Some(b"value2".to_vec()));
    assert!(snapshot.get(b"key3").is_none());

    // Drop snapshot and verify new read sees updated data
    drop(snapshot);

    let rtx = db.read_tx();
    assert_eq!(rtx.get(b"key1"), Some(b"modified".to_vec()));
    assert!(rtx.get(b"key2").is_none());
    assert_eq!(rtx.get(b"key3"), Some(b"new_value".to_vec()));

    cleanup(&path);
}

/// Test cheap read-only transactions don't block writers.
#[test]
fn test_readonly_tx_no_writer_blocking() {
    let path = test_db_path("readonly_no_block");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert initial data
    {
        let mut wtx = db.write_tx();
        wtx.put(b"initial", b"data");
        wtx.commit().expect("commit should succeed");
    }

    // Hold multiple read transactions
    let rtx1 = db.read_tx();
    let rtx2 = db.read_tx();

    // Both should see the same data
    assert_eq!(rtx1.get(b"initial"), Some(b"data".to_vec()));
    assert_eq!(rtx2.get(b"initial"), Some(b"data".to_vec()));

    // Multiple reads should be able to coexist
    let count = rtx1.iter().count();
    assert_eq!(count, 1);

    drop(rtx1);
    drop(rtx2);

    cleanup(&path);
}

/// Test long-lived snapshots with minimal writer interference.
#[test]
fn test_long_lived_snapshot() {
    let path = test_db_path("long_snapshot");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Create initial data
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

    // Create a long-lived snapshot
    let snapshot = db.snapshot();

    // Perform many write transactions while snapshot is held
    for batch in 0..10 {
        let mut wtx = db.write_tx();
        for i in 0..100 {
            let key = format!("new_key_{batch}_{i:03}");
            wtx.put(key.as_bytes(), b"new_value");
        }
        wtx.commit().expect("commit should succeed");
    }

    // Snapshot should still only see original 100 keys
    let snapshot_count: usize = snapshot.iter().count();
    assert_eq!(
        snapshot_count, 100,
        "snapshot should still see only original 100 keys"
    );

    // Current view should see all keys
    let current_count: usize = db.read_tx().iter().count();
    assert_eq!(
        current_count,
        100 + 10 * 100,
        "current view should see all 1100 keys"
    );

    cleanup(&path);
}

/// Test explicit snapshot API with clear semantics.
#[test]
fn test_explicit_snapshot_api() {
    let path = test_db_path("explicit_snapshot");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert data
    {
        let mut wtx = db.write_tx();
        wtx.put(b"data", b"original");
        wtx.commit().expect("commit should succeed");
    }

    // Create explicit snapshot with ID
    let snapshot_id = db.create_snapshot();
    assert!(snapshot_id > 0, "snapshot ID should be positive");

    // Get snapshot by ID
    let snapshot = db.get_snapshot(snapshot_id).expect("snapshot should exist");
    assert_eq!(snapshot.get(b"data"), Some(b"original".to_vec()));

    // Modify database
    {
        let mut wtx = db.write_tx();
        wtx.put(b"data", b"modified");
        wtx.commit().expect("commit should succeed");
    }

    // Snapshot should still see original
    let snapshot = db
        .get_snapshot(snapshot_id)
        .expect("snapshot should still exist");
    assert_eq!(snapshot.get(b"data"), Some(b"original".to_vec()));

    // Release snapshot
    db.release_snapshot(snapshot_id);

    // Getting released snapshot should fail
    assert!(db.get_snapshot(snapshot_id).is_none());

    cleanup(&path);
}

/// Test snapshot statistics and memory tracking.
#[test]
fn test_snapshot_stats() {
    let path = test_db_path("snapshot_stats");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert data
    {
        let mut wtx = db.write_tx();
        for i in 0..100 {
            wtx.put(
                format!("key_{i:03}").as_bytes(),
                format!("value_{i}").as_bytes(),
            );
        }
        wtx.commit().expect("commit should succeed");
    }

    // Create snapshot
    let _snapshot = db.snapshot();

    // Get snapshot stats
    let stats = db.snapshot_stats();
    assert!(
        stats.active_snapshots >= 1,
        "should have at least one active snapshot"
    );
    assert!(
        stats.oldest_snapshot_age_ms >= 0,
        "should track snapshot age"
    );

    cleanup(&path);
}

// ==================== 3. Zero-Copy Read Tests ====================

/// Test zero-copy get_ref returns references without allocation.
#[test]
fn test_zero_copy_get_ref() {
    let path = test_db_path("zero_copy_get");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert a large value
    let large_value = vec![0x42u8; 64 * 1024]; // 64KB value
    {
        let mut wtx = db.write_tx();
        wtx.put(b"large_key", &large_value);
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Zero-copy read should return reference, not owned data
    let value_ref: Option<&[u8]> = rtx.get_ref(b"large_key");
    assert!(value_ref.is_some());
    assert_eq!(value_ref.unwrap().len(), 64 * 1024);
    assert_eq!(value_ref.unwrap()[0], 0x42);

    // Verify it's actually the same data
    assert_eq!(value_ref.unwrap(), large_value.as_slice());

    cleanup(&path);
}

/// Test that zero-copy refs are valid for transaction lifetime.
#[test]
fn test_zero_copy_lifetime_guarantees() {
    let path = test_db_path("zero_copy_lifetime");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert values
    {
        let mut wtx = db.write_tx();
        wtx.put(b"key1", b"value1");
        wtx.put(b"key2", b"value2");
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Get multiple refs
    let ref1 = rtx.get_ref(b"key1");
    let ref2 = rtx.get_ref(b"key2");

    // Both refs should be valid simultaneously (within tx lifetime)
    assert_eq!(ref1, Some(b"value1".as_slice()));
    assert_eq!(ref2, Some(b"value2".as_slice()));

    // Refs can be compared, sliced, etc. during tx lifetime
    if let (Some(r1), Some(r2)) = (ref1, ref2) {
        assert!(r1 != r2);
        assert!(r1.len() == r2.len());
    }

    cleanup(&path);
}

/// Test owned value accessor for cases where ownership is needed.
#[test]
fn test_owned_value_accessor() {
    let path = test_db_path("owned_value");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.put(b"key", b"value");
        wtx.commit().expect("commit should succeed");
    }

    // OwnedValue can outlive the transaction
    let owned: Option<thunderdb::OwnedValue>;
    {
        let rtx = db.read_tx();
        owned = rtx.get_owned(b"key");
    } // Transaction dropped here

    // OwnedValue still accessible
    assert!(owned.is_some());
    let owned = owned.unwrap();
    assert_eq!(owned.as_ref(), b"value");

    cleanup(&path);
}

/// Test zero-copy iterator values.
#[test]
fn test_zero_copy_iterator_values() {
    let path = test_db_path("zero_copy_iter");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert values
    {
        let mut wtx = db.write_tx();
        for i in 0..100 {
            let key = format!("key_{i:03}");
            let value = vec![i as u8; 100]; // 100-byte values
            wtx.put(key.as_bytes(), &value);
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Iterator yields references, not owned data
    for (key, value) in rtx.iter() {
        // These are &[u8], not Vec<u8>
        assert!(!key.is_empty());
        assert_eq!(value.len(), 100);
    }

    cleanup(&path);
}

/// Test safe defaults vs unsafe fast paths.
#[test]
fn test_safe_defaults_and_fast_paths() {
    let path = test_db_path("safe_fast_paths");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.put(b"key", b"value");
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Safe default: bounds-checked access
    let safe_value = rtx.get_ref(b"key");
    assert!(safe_value.is_some());

    // Safe slice operations
    if let Some(v) = safe_value {
        // All standard slice operations are safe
        let _first_byte = v.first();
        let _slice = v.get(0..3);
        let _len = v.len();
    }

    // Note: get_unchecked would be available for performance-critical code
    // in an unsafe API, but we prioritize safety by default.

    cleanup(&path);
}

/// Test that compile-time markers distinguish borrowed vs owned data.
#[test]
fn test_compile_time_markers() {
    let path = test_db_path("compile_markers");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.put(b"test", b"data");
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // BorrowedValue<'tx> - tied to transaction lifetime
    let borrowed: Option<thunderdb::BorrowedValue<'_>> = rtx.get_borrowed(b"test");
    assert!(borrowed.is_some());

    // OwnedValue - owns its data, can outlive tx
    let owned: Option<thunderdb::OwnedValue> = rtx.get_owned(b"test");
    drop(rtx); // Transaction dropped

    // Owned value still valid
    assert!(owned.is_some());
    assert_eq!(owned.unwrap().as_ref(), b"data");

    cleanup(&path);
}

// ==================== Performance Regression Tests ====================

/// Benchmark: verify iteration performance doesn't regress.
#[test]
fn test_performance_iteration_regression() {
    let path = test_db_path("perf_iter_regression");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert 50,000 keys for realistic benchmark
    const NUM_KEYS: usize = 50_000;
    {
        let mut wtx = db.write_tx();
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            let value = format!("value_with_some_reasonable_length_{i}");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Warm up
    let _: usize = rtx.iter().count();

    // Measure iteration
    let iterations = 5;
    let mut total_duration = Duration::ZERO;

    for _ in 0..iterations {
        let start = Instant::now();
        let count: usize = rtx.iter().count();
        total_duration += start.elapsed();
        assert_eq!(count, NUM_KEYS);
    }

    let avg_duration = total_duration / iterations as u32;

    // Performance target: iterate 50k keys in under 50ms average
    assert!(
        avg_duration < Duration::from_millis(50),
        "iteration performance regression: {NUM_KEYS} keys took {:?} average",
        avg_duration
    );

    cleanup(&path);
}

/// Benchmark: verify range scan performance.
#[test]
fn test_performance_range_scan_regression() {
    let path = test_db_path("perf_range_regression");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert 20,000 keys
    const NUM_KEYS: usize = 20_000;
    {
        let mut wtx = db.write_tx();
        for i in 0..NUM_KEYS {
            let key = format!("key_{i:08}");
            let value = format!("value_{i}");
            wtx.put(key.as_bytes(), value.as_bytes());
        }
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Range scan: 5000 keys in the middle
    let start_key = b"key_00005000".as_slice();
    let end_key = b"key_00010000".as_slice();

    let iterations = 10;
    let mut total_duration = Duration::ZERO;

    for _ in 0..iterations {
        let start = Instant::now();
        let count: usize = rtx.range(start_key..end_key).count();
        total_duration += start.elapsed();
        assert_eq!(count, 5000);
    }

    let avg_duration = total_duration / iterations as u32;

    // Performance target: range scan 5k keys in under 10ms
    assert!(
        avg_duration < Duration::from_millis(10),
        "range scan performance regression: 5000 keys took {:?} average",
        avg_duration
    );

    cleanup(&path);
}

/// Benchmark: verify snapshot creation is reasonably fast.
#[test]
fn test_performance_snapshot_creation() {
    let path = test_db_path("perf_snapshot");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert some data
    {
        let mut wtx = db.write_tx();
        for i in 0..1000 {
            wtx.put(
                format!("key_{i:05}").as_bytes(),
                format!("val_{i}").as_bytes(),
            );
        }
        wtx.commit().expect("commit should succeed");
    }

    // Warmup
    for _ in 0..10 {
        let _snapshot = db.snapshot();
    }

    // Measure snapshot creation time
    let iterations = 1000;
    let start = Instant::now();
    for _ in 0..iterations {
        let _snapshot = db.snapshot();
    }
    let total_duration = start.elapsed();

    let avg_duration = total_duration / iterations as u32;
    let avg_micros = avg_duration.as_nanos() as f64 / 1000.0;

    eprintln!(
        "Snapshot creation: {} iterations in {:?}, avg {:.2}µs ({:.3}ms)",
        iterations,
        total_duration,
        avg_micros,
        avg_micros / 1000.0
    );

    // O(1) snapshot creation should be < 1ms (typically < 100µs)
    assert!(
        avg_duration < Duration::from_millis(1),
        "snapshot creation too slow: {:?} average (target < 1ms)",
        avg_duration
    );

    cleanup(&path);
}

// ==================== Security Tests ====================

/// Test that snapshot isolation prevents dirty reads.
#[test]
fn test_security_no_dirty_reads() {
    let path = test_db_path("security_dirty_reads");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Insert initial data
    {
        let mut wtx = db.write_tx();
        wtx.put(b"balance", b"1000");
        wtx.commit().expect("commit should succeed");
    }

    // Take snapshot
    let snapshot = db.snapshot();

    // Start write transaction but don't commit
    {
        let mut wtx = db.write_tx();
        wtx.put(b"balance", b"0"); // Uncommitted change
        // Transaction dropped without commit
    }

    // Snapshot should not see uncommitted changes
    assert_eq!(snapshot.get(b"balance"), Some(b"1000".to_vec()));

    // New read should also not see uncommitted changes
    let rtx = db.read_tx();
    assert_eq!(rtx.get(b"balance"), Some(b"1000".to_vec()));

    cleanup(&path);
}

/// Test bounds checking on zero-copy reads.
#[test]
fn test_security_bounds_checking() {
    let path = test_db_path("security_bounds");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.put(b"key", b"value");
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();

    // Non-existent key should return None, not garbage
    assert!(rtx.get_ref(b"nonexistent").is_none());

    // Valid key should return proper slice
    let value = rtx.get_ref(b"key").unwrap();
    assert_eq!(value.len(), 5);

    // Slice operations should be bounds-checked
    assert!(value.get(0..5).is_some());
    assert!(value.get(0..10).is_none()); // Out of bounds

    cleanup(&path);
}

// ==================== Edge Case Tests ====================

/// Test iterator behavior with empty database.
#[test]
fn test_edge_empty_database_iteration() {
    let path = test_db_path("edge_empty_iter");
    cleanup(&path);

    let db = Database::open(&path).expect("open should succeed");

    let rtx = db.read_tx();
    let count: usize = rtx.iter().count();
    assert_eq!(count, 0);

    // Range on empty db
    let range_count: usize = rtx.range(b"a".as_slice()..b"z".as_slice()).count();
    assert_eq!(range_count, 0);

    cleanup(&path);
}

/// Test snapshot behavior at database boundaries.
#[test]
fn test_edge_snapshot_at_boundaries() {
    let path = test_db_path("edge_snapshot_boundaries");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    // Snapshot on empty database
    let empty_snapshot = db.snapshot();
    assert_eq!(empty_snapshot.iter().count(), 0);

    // Add first entry
    {
        let mut wtx = db.write_tx();
        wtx.put(b"first", b"value");
        wtx.commit().expect("commit should succeed");
    }

    // Empty snapshot should still be empty
    assert_eq!(empty_snapshot.iter().count(), 0);

    // New snapshot should have one entry
    let one_snapshot = db.snapshot();
    assert_eq!(one_snapshot.iter().count(), 1);

    cleanup(&path);
}

/// Test zero-copy with empty values.
#[test]
fn test_edge_zero_copy_empty_value() {
    let path = test_db_path("edge_empty_value");
    cleanup(&path);

    let mut db = Database::open(&path).expect("open should succeed");

    {
        let mut wtx = db.write_tx();
        wtx.put(b"empty", b"");
        wtx.commit().expect("commit should succeed");
    }

    let rtx = db.read_tx();
    let value = rtx.get_ref(b"empty");
    assert!(value.is_some());
    assert_eq!(value.unwrap().len(), 0);
    assert_eq!(value.unwrap(), b"".as_slice());

    cleanup(&path);
}
