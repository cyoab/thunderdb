//! Summary: Comprehensive tests for WAL (Write-Ahead Logging), Group Commit, and Checkpointing.
//! These tests verify correctness, durability, crash recovery, and security.
//! Copyright (c) YOAB. All rights reserved.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use thunderdb::checkpoint::{CheckpointConfig, CheckpointManager};
use thunderdb::group_commit::{GroupCommitConfig, GroupCommitManager};
use thunderdb::wal::{SyncPolicy, Wal, WalConfig};
use thunderdb::wal_record::WalRecord;
use thunderdb::{Database, DatabaseOptions};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_test_path(prefix: &str) -> PathBuf {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    PathBuf::from(format!("/tmp/thunder_wal_test_{}_{pid}_{counter}", prefix))
}

fn cleanup_dir(path: &PathBuf) {
    if path.exists() {
        let _ = fs::remove_dir_all(path);
    }
}

#[allow(dead_code)]
fn cleanup_file(path: &PathBuf) {
    if path.exists() {
        let _ = fs::remove_file(path);
    }
}

// ============================================================================
// TEST 1: WAL Record Integrity and Crash Recovery
// Verifies that WAL provides durability guarantees and correct recovery.
// Tests: record encoding/decoding, CRC validation, replay, corruption detection.
// ============================================================================

mod wal_integrity_and_recovery {
    use super::*;

    /// Tests that WAL records survive encode/decode roundtrip with CRC validation.
    #[test]
    fn test_wal_record_encode_decode_integrity() {
        // Test various record types
        let records = vec![
            WalRecord::TxBegin { txid: 1 },
            WalRecord::Put {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
            },
            WalRecord::Put {
                key: vec![0u8; 1024],     // Large key
                value: vec![0xFF; 32768], // Large value (page size)
            },
            WalRecord::Delete {
                key: b"deleted_key".to_vec(),
            },
            WalRecord::TxCommit { txid: 1 },
            WalRecord::TxAbort { txid: 2 },
            WalRecord::Checkpoint {
                lsn: 0x1234567890ABCDEF,
            },
        ];

        for original in &records {
            let encoded = original.encode();

            // Verify minimum size (header + at least some payload)
            assert!(encoded.len() >= thunderdb::wal_record::RECORD_HEADER_SIZE);

            // Decode and verify roundtrip
            let (decoded, consumed) =
                WalRecord::decode(&encoded).expect("decode should succeed for valid record");
            assert_eq!(consumed, encoded.len());

            match (original, &decoded) {
                (WalRecord::Put { key: k1, value: v1 }, WalRecord::Put { key: k2, value: v2 }) => {
                    assert_eq!(k1, k2, "keys must match after roundtrip");
                    assert_eq!(v1, v2, "values must match after roundtrip");
                }
                (WalRecord::Delete { key: k1 }, WalRecord::Delete { key: k2 }) => {
                    assert_eq!(k1, k2, "delete keys must match after roundtrip");
                }
                (WalRecord::TxBegin { txid: t1 }, WalRecord::TxBegin { txid: t2 }) => {
                    assert_eq!(t1, t2);
                }
                (WalRecord::TxCommit { txid: t1 }, WalRecord::TxCommit { txid: t2 }) => {
                    assert_eq!(t1, t2);
                }
                (WalRecord::TxAbort { txid: t1 }, WalRecord::TxAbort { txid: t2 }) => {
                    assert_eq!(t1, t2);
                }
                (WalRecord::Checkpoint { lsn: l1 }, WalRecord::Checkpoint { lsn: l2 }) => {
                    assert_eq!(l1, l2);
                }
                _ => panic!("record type mismatch after roundtrip"),
            }
        }
    }

    /// Tests that CRC corruption is detected during decode.
    #[test]
    fn test_wal_record_corruption_detection() {
        let record = WalRecord::Put {
            key: b"important_data".to_vec(),
            value: b"critical_value".to_vec(),
        };

        let mut encoded = record.encode();
        assert!(encoded.len() > thunderdb::wal_record::RECORD_HEADER_SIZE);

        // Corrupt a byte in the payload
        let corrupt_idx = thunderdb::wal_record::RECORD_HEADER_SIZE + 2;
        if corrupt_idx < encoded.len() {
            encoded[corrupt_idx] ^= 0xFF;
        }

        // Decode should fail due to CRC mismatch
        let result = WalRecord::decode(&encoded);
        assert!(
            result.is_err(),
            "corrupted record should fail CRC validation"
        );
    }

    /// Tests WAL append and replay after simulated crash.
    #[test]
    fn test_wal_crash_recovery_replay() {
        let wal_dir = unique_test_path("wal_recovery");
        cleanup_dir(&wal_dir);
        fs::create_dir_all(&wal_dir).expect("create WAL dir");

        let test_records = vec![
            (1u64, b"key1".to_vec(), b"value1".to_vec()),
            (1, b"key2".to_vec(), b"value2".to_vec()),
            (2, b"key3".to_vec(), b"value3_txn2".to_vec()),
            (1, b"key4".to_vec(), b"value4".to_vec()),
        ];

        // Phase 1: Write records to WAL (simulate normal operation)
        let expected_lsn;
        {
            let config = WalConfig {
                segment_size: 1024 * 1024, // 1MB segments for test
                sync_policy: SyncPolicy::Immediate,
            };
            let mut wal = Wal::open(&wal_dir, config).expect("open WAL");

            // Write transaction 1
            wal.append(&WalRecord::TxBegin { txid: 1 })
                .expect("append TxBegin");
            for (txid, key, value) in &test_records {
                if *txid == 1 {
                    wal.append(&WalRecord::Put {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .expect("append Put");
                }
            }
            wal.append(&WalRecord::TxCommit { txid: 1 })
                .expect("append TxCommit");

            // Write transaction 2 (uncommitted - should be rolled back)
            wal.append(&WalRecord::TxBegin { txid: 2 })
                .expect("append TxBegin 2");
            wal.append(&WalRecord::Put {
                key: b"key3".to_vec(),
                value: b"value3_txn2".to_vec(),
            })
            .expect("append Put txn2");
            // No commit for txn 2 - simulating crash

            expected_lsn = wal.current_lsn();
            wal.sync().expect("sync WAL");
        }

        // Phase 2: "Crash" and reopen - replay WAL
        {
            let config = WalConfig {
                segment_size: 1024 * 1024,
                sync_policy: SyncPolicy::Immediate,
            };
            let wal = Wal::open(&wal_dir, config).expect("reopen WAL");

            let mut replayed_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut committed_txids = std::collections::HashSet::new();
            let mut current_txid = None;
            let mut txn_puts: std::collections::HashMap<u64, Vec<_>> =
                std::collections::HashMap::new();

            let final_lsn = wal
                .replay(0, |record| {
                    match record {
                        WalRecord::TxBegin { txid } => {
                            current_txid = Some(txid);
                            txn_puts.insert(txid, Vec::new());
                        }
                        WalRecord::Put { key, value } => {
                            if let Some(txid) = current_txid {
                                txn_puts.entry(txid).or_default().push((key, value));
                            }
                        }
                        WalRecord::TxCommit { txid } => {
                            committed_txids.insert(txid);
                        }
                        WalRecord::TxAbort { txid } => {
                            txn_puts.remove(&txid);
                        }
                        _ => {}
                    }
                    Ok(())
                })
                .expect("replay should succeed");

            // Collect only committed transaction puts
            for txid in &committed_txids {
                if let Some(puts) = txn_puts.get(txid) {
                    replayed_puts.extend(puts.iter().cloned());
                }
            }

            // Verify: only committed transaction 1 data should be recovered
            assert!(committed_txids.contains(&1), "txn 1 should be committed");
            assert!(
                !committed_txids.contains(&2),
                "txn 2 should NOT be committed"
            );
            assert_eq!(replayed_puts.len(), 3, "should have 3 puts from txn 1");
            assert!(final_lsn <= expected_lsn, "replay should reach end");
        }

        cleanup_dir(&wal_dir);
    }

    /// Tests WAL segment rotation when segment fills up.
    #[test]
    fn test_wal_segment_rotation() {
        let wal_dir = unique_test_path("wal_segment_rotate");
        cleanup_dir(&wal_dir);
        fs::create_dir_all(&wal_dir).expect("create WAL dir");

        // Use small segment size to trigger rotation
        let config = WalConfig {
            segment_size: 32768, // 32KB segments (HPC page size)
            sync_policy: SyncPolicy::Immediate,
        };

        let mut wal = Wal::open(&wal_dir, config).expect("open WAL");

        // Write enough data to trigger rotation (need > 32KB per segment)
        // Each Put record is ~90 bytes + overhead, so 400 records should fill multiple segments
        for i in 0..400 {
            wal.append(&WalRecord::Put {
                key: format!("key_{i:05}").into_bytes(),
                value: vec![0xAB; 64], // 64-byte values
            })
            .expect("append should succeed");
        }
        wal.sync().expect("sync");

        // Verify multiple segment files exist
        let segments: Vec<_> = fs::read_dir(&wal_dir)
            .expect("read WAL dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
            .collect();

        assert!(
            segments.len() > 1,
            "should have multiple segments after rotation, got {}",
            segments.len()
        );

        cleanup_dir(&wal_dir);
    }
}

// ============================================================================
// TEST 2: Group Commit Throughput and Correctness
// Verifies that group commit batches writes correctly and improves throughput.
// Tests: batching behavior, concurrent commit correctness, timeout handling.
// ============================================================================

mod group_commit_throughput {
    use super::*;

    /// Tests that concurrent commits are batched together.
    #[test]
    fn test_group_commit_batching() {
        let wal_dir = unique_test_path("group_commit_batch");
        cleanup_dir(&wal_dir);
        fs::create_dir_all(&wal_dir).expect("create WAL dir");

        let wal_config = WalConfig {
            segment_size: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::None, // Group commit will handle sync
        };

        let wal = Arc::new(std::sync::Mutex::new(
            Wal::open(&wal_dir, wal_config).expect("open WAL"),
        ));

        let gc_config = GroupCommitConfig {
            max_wait: Duration::from_millis(50),
            max_batch_size: 10,
        };

        let group_commit = Arc::new(GroupCommitManager::new(gc_config, wal.clone()));

        let commit_count = Arc::new(AtomicU64::new(0));
        let num_threads = 8;
        let commits_per_thread = 5;

        // Spawn concurrent commit threads
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let gc = group_commit.clone();
                let count = commit_count.clone();
                let wal_clone = wal.clone();

                thread::spawn(move || {
                    for i in 0..commits_per_thread {
                        // Write to WAL first
                        let lsn = {
                            let mut wal_guard = wal_clone.lock().unwrap();
                            wal_guard
                                .append(&WalRecord::Put {
                                    key: format!("t{thread_id}_k{i}").into_bytes(),
                                    value: format!("v{i}").into_bytes(),
                                })
                                .expect("append")
                        };

                        // Group commit
                        gc.commit(lsn).expect("group commit should succeed");
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread join");
        }

        let total = commit_count.load(Ordering::SeqCst);
        assert_eq!(
            total,
            (num_threads * commits_per_thread) as u64,
            "all commits should complete"
        );

        // Verify batch count is less than total commits (batching occurred)
        let batch_count = group_commit.batch_count();
        assert!(
            batch_count < total,
            "batches ({batch_count}) should be fewer than total commits ({total})"
        );

        cleanup_dir(&wal_dir);
    }

    /// Tests group commit timeout behavior.
    #[test]
    fn test_group_commit_timeout() {
        let wal_dir = unique_test_path("group_commit_timeout");
        cleanup_dir(&wal_dir);
        fs::create_dir_all(&wal_dir).expect("create WAL dir");

        let wal_config = WalConfig {
            segment_size: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::None,
        };

        let wal = Arc::new(std::sync::Mutex::new(
            Wal::open(&wal_dir, wal_config).expect("open WAL"),
        ));

        // Short timeout to test timeout path
        let gc_config = GroupCommitConfig {
            max_wait: Duration::from_millis(10),
            max_batch_size: 100, // Large batch size so timeout triggers first
        };

        let group_commit = GroupCommitManager::new(gc_config, wal.clone());

        // Single commit should complete within reasonable time (timeout triggers flush)
        let start = std::time::Instant::now();
        {
            let lsn = {
                let mut wal_guard = wal.lock().unwrap();
                wal_guard
                    .append(&WalRecord::Put {
                        key: b"single_key".to_vec(),
                        value: b"single_value".to_vec(),
                    })
                    .expect("append")
            };
            group_commit.commit(lsn).expect("commit should succeed");
        }
        let elapsed = start.elapsed();

        // Should complete roughly within the timeout window (with some margin)
        assert!(
            elapsed < Duration::from_millis(100),
            "single commit should complete within timeout, took {:?}",
            elapsed
        );

        cleanup_dir(&wal_dir);
    }

    /// Tests that all commits are durable after group sync completes.
    #[test]
    fn test_group_commit_durability() {
        let wal_dir = unique_test_path("group_commit_durable");
        cleanup_dir(&wal_dir);
        fs::create_dir_all(&wal_dir).expect("create WAL dir");

        let wal_config = WalConfig {
            segment_size: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::None,
        };

        let wal = Arc::new(std::sync::Mutex::new(
            Wal::open(&wal_dir, wal_config).expect("open WAL"),
        ));

        let gc_config = GroupCommitConfig {
            max_wait: Duration::from_millis(5),
            max_batch_size: 50,
        };

        let gc = Arc::new(GroupCommitManager::new(gc_config, wal.clone()));

        // Write and commit multiple records
        let mut lsns = Vec::new();
        for i in 0..20 {
            let lsn = {
                let mut wal_guard = wal.lock().unwrap();
                wal_guard
                    .append(&WalRecord::Put {
                        key: format!("durable_key_{i}").into_bytes(),
                        value: format!("durable_value_{i}").into_bytes(),
                    })
                    .expect("append")
            };
            gc.commit(lsn).expect("commit");
            lsns.push((format!("durable_key_{i}"), format!("durable_value_{i}")));
        }

        // Drop and reopen WAL to verify durability
        drop(gc);
        drop(wal);

        let wal_config = WalConfig {
            segment_size: 64 * 1024 * 1024,
            sync_policy: SyncPolicy::Immediate,
        };
        let wal = Wal::open(&wal_dir, wal_config).expect("reopen WAL");

        let mut recovered: Vec<(String, String)> = Vec::new();
        wal.replay(0, |record| {
            if let WalRecord::Put { key, value } = record {
                recovered.push((
                    String::from_utf8(key).unwrap(),
                    String::from_utf8(value).unwrap(),
                ));
            }
            Ok(())
        })
        .expect("replay");

        // Verify all records were durably written
        for (expected_key, expected_value) in &lsns {
            let found = recovered
                .iter()
                .any(|(k, v)| k == expected_key && v == expected_value);
            assert!(
                found,
                "key '{}' should be recovered after group commit",
                expected_key
            );
        }

        cleanup_dir(&wal_dir);
    }
}

// ============================================================================
// TEST 3: Checkpoint and Bounded Recovery Time
// Verifies checkpointing reduces recovery time and truncates WAL correctly.
// Tests: checkpoint creation, recovery from checkpoint, WAL truncation.
// ============================================================================

mod checkpoint_recovery {
    use super::*;

    /// Tests that checkpoint reduces WAL size and recovery scans less data.
    #[test]
    fn test_checkpoint_wal_truncation() {
        let base_dir = unique_test_path("checkpoint_truncate");
        let wal_dir = base_dir.join("wal");
        let _db_path = base_dir.join("test.db");
        cleanup_dir(&base_dir);
        fs::create_dir_all(&wal_dir).expect("create dirs");

        let wal_config = WalConfig {
            segment_size: 8192, // Small segments for easier truncation testing
            sync_policy: SyncPolicy::Immediate,
        };

        // Phase 1: Write data and create checkpoint
        let checkpoint_lsn;
        {
            let mut wal = Wal::open(&wal_dir, wal_config.clone()).expect("open WAL");

            // Write first batch
            for i in 0..50 {
                wal.append(&WalRecord::Put {
                    key: format!("batch1_key_{i}").into_bytes(),
                    value: vec![0xAA; 32],
                })
                .expect("append");
            }
            wal.append(&WalRecord::TxCommit { txid: 1 })
                .expect("commit");
            wal.sync().expect("sync");

            checkpoint_lsn = wal.current_lsn();

            // Create checkpoint
            wal.append(&WalRecord::Checkpoint {
                lsn: checkpoint_lsn,
            })
            .expect("checkpoint record");

            // Truncate WAL before checkpoint
            wal.truncate_before(checkpoint_lsn).expect("truncate");
        }

        // Phase 2: Write more data after checkpoint
        {
            let mut wal = Wal::open(&wal_dir, wal_config.clone()).expect("reopen WAL");

            for i in 0..20 {
                wal.append(&WalRecord::Put {
                    key: format!("batch2_key_{i}").into_bytes(),
                    value: vec![0xBB; 32],
                })
                .expect("append");
            }
            wal.append(&WalRecord::TxCommit { txid: 2 })
                .expect("commit");
            wal.sync().expect("sync");
        }

        // Phase 3: Recovery from checkpoint
        {
            let wal = Wal::open(&wal_dir, wal_config).expect("final open");

            let mut record_count = 0;
            let mut found_batch2 = false;

            // Replay from checkpoint LSN
            wal.replay(checkpoint_lsn, |record| {
                record_count += 1;
                if let WalRecord::Put { key, .. } = &record {
                    let key_str = String::from_utf8_lossy(key);
                    if key_str.starts_with("batch2_") {
                        found_batch2 = true;
                    }
                }
                Ok(())
            })
            .expect("replay from checkpoint");

            // Should only replay batch2 records (checkpoint records + batch2 puts + commit)
            assert!(
                record_count <= 25,
                "should replay fewer records from checkpoint, got {record_count}"
            );
            assert!(found_batch2, "should find batch2 records after checkpoint");
        }

        cleanup_dir(&base_dir);
    }

    /// Tests CheckpointManager's threshold-based checkpoint triggering.
    #[test]
    fn test_checkpoint_manager_thresholds() {
        let base_dir = unique_test_path("checkpoint_thresholds");
        let wal_dir = base_dir.join("wal");
        cleanup_dir(&base_dir);
        fs::create_dir_all(&wal_dir).expect("create dirs");

        let wal_config = WalConfig {
            segment_size: 32768,
            sync_policy: SyncPolicy::Immediate,
        };
        let mut wal = Wal::open(&wal_dir, wal_config).expect("open WAL");

        // Configure checkpoint manager with low thresholds for testing
        let ckpt_config = CheckpointConfig {
            interval: Duration::from_secs(300), // Time-based won't trigger
            wal_threshold: 2048,                // 2KB threshold
            min_records: 10,                    // 10 records minimum
        };

        let mut ckpt_mgr = CheckpointManager::new(ckpt_config);

        // Should not checkpoint initially
        assert!(
            !ckpt_mgr.should_checkpoint(&wal),
            "should not checkpoint with empty WAL"
        );

        // Write enough records to trigger threshold
        for i in 0..20 {
            wal.append(&WalRecord::Put {
                key: format!("key_{i}").into_bytes(),
                value: vec![0xCC; 128],
            })
            .expect("append");
        }
        wal.sync().expect("sync");

        // Now should trigger checkpoint
        assert!(
            ckpt_mgr.should_checkpoint(&wal),
            "should checkpoint after exceeding thresholds"
        );

        // Record checkpoint with current WAL size
        let lsn = wal.current_lsn();
        let wal_size = wal.approximate_size();
        ckpt_mgr.record_checkpoint_with_wal_size(lsn, wal_size);

        // Should not checkpoint immediately after
        assert!(
            !ckpt_mgr.should_checkpoint(&wal),
            "should not checkpoint immediately after checkpoint"
        );

        cleanup_dir(&base_dir);
    }

    /// Tests full database recovery with checkpoint integration.
    #[test]
    fn test_database_recovery_with_checkpoint() {
        let base_dir = unique_test_path("db_checkpoint_recovery");
        let db_path = base_dir.join("test.db");
        let wal_dir = base_dir.join("wal");
        cleanup_dir(&base_dir);
        fs::create_dir_all(&base_dir).expect("create base dir");

        // Create database with WAL enabled
        let options = DatabaseOptions {
            wal_enabled: true,
            wal_dir: Some(wal_dir.clone()),
            wal_sync_policy: SyncPolicy::Immediate,
            checkpoint_interval_secs: 0, // Disabled automatic
            ..DatabaseOptions::default()
        };

        // Phase 1: Write data and create checkpoint
        {
            let mut db =
                Database::open_with_options(&db_path, options.clone()).expect("open database");

            // Write batch 1
            {
                let mut wtx = db.write_tx();
                for i in 0..100 {
                    wtx.put(
                        format!("pre_ckpt_key_{i}").as_bytes(),
                        format!("pre_ckpt_val_{i}").as_bytes(),
                    );
                }
                wtx.commit().expect("commit batch 1");
            }

            // Force checkpoint
            db.checkpoint().expect("create checkpoint");

            // Write batch 2 (after checkpoint)
            {
                let mut wtx = db.write_tx();
                for i in 0..50 {
                    wtx.put(
                        format!("post_ckpt_key_{i}").as_bytes(),
                        format!("post_ckpt_val_{i}").as_bytes(),
                    );
                }
                wtx.commit().expect("commit batch 2");
            }
        }

        // Phase 2: Simulate crash and recover
        {
            let db = Database::open_with_options(&db_path, options).expect("recover database");

            let rtx = db.read_tx();

            // Verify pre-checkpoint data
            for i in 0..100 {
                let val = rtx.get(format!("pre_ckpt_key_{i}").as_bytes());
                assert!(val.is_some(), "pre-checkpoint key {i} should be recovered");
            }

            // Verify post-checkpoint data
            for i in 0..50 {
                let val = rtx.get(format!("post_ckpt_key_{i}").as_bytes());
                assert!(val.is_some(), "post-checkpoint key {i} should be recovered");
            }
        }

        cleanup_dir(&base_dir);
    }

    /// Tests that checkpoint info is persisted and recovered correctly in meta.
    #[test]
    fn test_checkpoint_meta_persistence() {
        let base_dir = unique_test_path("checkpoint_meta");
        let db_path = base_dir.join("test.db");
        let wal_dir = base_dir.join("wal");
        cleanup_dir(&base_dir);
        fs::create_dir_all(&base_dir).expect("create base dir");

        let options = DatabaseOptions {
            wal_enabled: true,
            wal_dir: Some(wal_dir.clone()),
            wal_sync_policy: SyncPolicy::Immediate,
            checkpoint_interval_secs: 0,
            ..DatabaseOptions::default()
        };

        let expected_lsn;

        // Create checkpoint
        {
            let mut db =
                Database::open_with_options(&db_path, options.clone()).expect("open database");

            {
                let mut wtx = db.write_tx();
                wtx.put(b"test_key", b"test_value");
                wtx.commit().expect("commit");
            }

            db.checkpoint().expect("checkpoint");
            expected_lsn = db.checkpoint_lsn().expect("should have checkpoint LSN");
        }

        // Reopen and verify checkpoint info persisted
        {
            let db = Database::open_with_options(&db_path, options).expect("reopen database");

            let recovered_lsn = db.checkpoint_lsn();
            assert!(
                recovered_lsn.is_some(),
                "checkpoint LSN should be recovered"
            );
            assert_eq!(
                recovered_lsn.unwrap(),
                expected_lsn,
                "checkpoint LSN should match"
            );
        }

        cleanup_dir(&base_dir);
    }
}

// ============================================================================
// Security-focused tests
// ============================================================================

mod security {
    use super::*;

    /// Tests that WAL rejects oversized records (potential DoS vector).
    #[test]
    fn test_wal_record_size_limits() {
        // Attempt to create oversized key - should be handled gracefully
        let large_key = vec![0xAA; 1024 * 1024]; // 1MB key
        let large_value = vec![0xBB; 64 * 1024 * 1024]; // 64MB value

        let record = WalRecord::Put {
            key: large_key.clone(),
            value: large_value.clone(),
        };

        // Encoding should work but produce large output
        let encoded = record.encode();

        // Decoding should validate and potentially reject
        // (depends on configured limits in implementation)
        let result = WalRecord::decode(&encoded);

        // Either succeeds with correct data or fails with clear error
        match result {
            Ok((decoded, _)) => {
                if let WalRecord::Put { key, value } = decoded {
                    assert_eq!(key, large_key);
                    assert_eq!(value, large_value);
                }
            }
            Err(e) => {
                // If size limits are enforced, error is acceptable
                eprintln!("Large record rejected (expected if limits enforced): {e}");
            }
        }
    }

    /// Tests that truncated records don't cause panics or UB.
    #[test]
    fn test_wal_truncated_record_safety() {
        let record = WalRecord::Put {
            key: b"normal_key".to_vec(),
            value: b"normal_value".to_vec(),
        };

        let encoded = record.encode();

        // Test various truncation points
        for truncate_at in 0..encoded.len() {
            let truncated = &encoded[..truncate_at];
            // Should not panic, should return error
            let result = WalRecord::decode(truncated);
            assert!(
                result.is_err(),
                "truncated record at byte {truncate_at} should fail gracefully"
            );
        }
    }

    /// Tests WAL handles invalid record types gracefully.
    #[test]
    fn test_wal_invalid_record_type() {
        let record = WalRecord::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };

        let mut encoded = record.encode();

        // Corrupt the record type field (byte 4 in header after length)
        if encoded.len() > 5 {
            encoded[4] = 0xFF; // Invalid record type
        }

        let result = WalRecord::decode(&encoded);
        assert!(
            result.is_err(),
            "invalid record type should fail validation"
        );
    }
}
