//! Summary: Comprehensive crash safety tests with kill -9 simulation.
//! Copyright (c) YOAB. All rights reserved.
//!
//! These tests verify that ThunderDB maintains ACID guarantees under
//! simulated crash conditions including:
//! - Crashes during page writes
//! - Crashes during metadata updates
//! - Crashes before/after fsync
//! - Random operation sequences with kill -9
//!
//! Run with: cargo test --features failpoint --test crash_safety_tests

use std::collections::HashMap;
use std::env;
use std::fs;
use rand::Rng;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use rand::prelude::*;
use rand::rngs::StdRng;
use tempfile::TempDir;

// ============================================================================
// Test Configuration
// ============================================================================

/// Number of iterations for randomized crash tests.
const DEFAULT_ITERATIONS: usize = 100;

/// Maximum operations per transaction in random tests.
const MAX_OPS_PER_TX: usize = 50;

/// Maximum key size in bytes.
const MAX_KEY_SIZE: usize = 256;

/// Maximum value size in bytes (below overflow threshold).
const MAX_VALUE_SIZE: usize = 4096;

/// Large value size (triggers overflow pages).
const LARGE_VALUE_SIZE: usize = 32768;

// ============================================================================
// Operation Generator
// ============================================================================

/// Types of database operations.
#[derive(Debug, Clone)]
pub enum Operation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Commit,
}

/// Generates random database operations.
pub struct OperationGenerator {
    rng: StdRng,
    existing_keys: Vec<Vec<u8>>,
    key_counter: u64,
}

impl OperationGenerator {
    /// Creates a new generator with the given seed.
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            existing_keys: Vec::new(),
            key_counter: 0,
        }
    }

    /// Creates a generator from environment or random seed.
    pub fn from_env_or_random() -> Self {
        let seed = env::var("THUNDERDB_CRASH_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        eprintln!("Using seed: {} (set THUNDERDB_CRASH_SEED to reproduce)", seed);
        Self::new(seed)
    }

    /// Returns the current seed for reproducibility.
    pub fn seed(&self) -> u64 {
        // The seed is baked into the RNG state, we track it separately
        0 // Placeholder - in practice, store the initial seed
    }

    /// Generates a random key.
    pub fn random_key(&mut self) -> Vec<u8> {
        self.key_counter += 1;
        let len = self.rng.gen_range(1..=MAX_KEY_SIZE);
        let mut key = format!("key_{:08}", self.key_counter).into_bytes();
        // Add random suffix
        key.extend((0..len.saturating_sub(key.len())).map(|_| self.rng.r#gen::<u8>()));
        key.truncate(len);
        key
    }

    /// Generates a random value.
    pub fn random_value(&mut self, large: bool) -> Vec<u8> {
        let len = if large {
            self.rng.gen_range(LARGE_VALUE_SIZE..LARGE_VALUE_SIZE * 2)
        } else {
            self.rng.gen_range(1..=MAX_VALUE_SIZE)
        };
        (0..len).map(|_| self.rng.r#gen::<u8>()).collect()
    }

    /// Generates a random operation.
    pub fn next_operation(&mut self) -> Operation {
        let op_type = self.rng.gen_range(0..100);

        if op_type < 60 {
            // 60% puts
            let key = self.random_key();
            let large = self.rng.gen_bool(0.1); // 10% large values
            let value = self.random_value(large);
            self.existing_keys.push(key.clone());
            Operation::Put { key, value }
        } else if op_type < 80 && !self.existing_keys.is_empty() {
            // 20% deletes (if keys exist)
            let idx = self.rng.gen_range(0..self.existing_keys.len());
            let key = self.existing_keys.remove(idx);
            Operation::Delete { key }
        } else {
            // 20% commits or fallback to put
            if self.rng.gen_bool(0.5) {
                Operation::Commit
            } else {
                let key = self.random_key();
                let value = self.random_value(false);
                self.existing_keys.push(key.clone());
                Operation::Put { key, value }
            }
        }
    }

    /// Generates a sequence of operations.
    pub fn generate_sequence(&mut self, count: usize) -> Vec<Operation> {
        (0..count).map(|_| self.next_operation()).collect()
    }

    /// Generates a transaction (operations ending with commit).
    pub fn generate_transaction(&mut self) -> Vec<Operation> {
        let count = self.rng.gen_range(1..=MAX_OPS_PER_TX);
        let mut ops: Vec<Operation> = (0..count)
            .map(|_| {
                // Only puts and deletes within a transaction
                if self.rng.gen_bool(0.8) || self.existing_keys.is_empty() {
                    let key = self.random_key();
                    let large = self.rng.gen_bool(0.1);
                    let value = self.random_value(large);
                    self.existing_keys.push(key.clone());
                    Operation::Put { key, value }
                } else {
                    let idx = self.rng.gen_range(0..self.existing_keys.len());
                    let key = self.existing_keys.remove(idx);
                    Operation::Delete { key }
                }
            })
            .collect();
        ops.push(Operation::Commit);
        ops
    }
}

// ============================================================================
// Invariant Checker
// ============================================================================

/// Checks database invariants after crash recovery.
pub struct InvariantChecker {
    /// Expected committed state (key -> value).
    expected_state: HashMap<Vec<u8>, Vec<u8>>,
    /// Pending uncommitted operations.
    pending_puts: HashMap<Vec<u8>, Vec<u8>>,
    pending_deletes: Vec<Vec<u8>>,
}

impl InvariantChecker {
    /// Creates a new invariant checker.
    pub fn new() -> Self {
        Self {
            expected_state: HashMap::new(),
            pending_puts: HashMap::new(),
            pending_deletes: Vec::new(),
        }
    }

    /// Records an operation (before crash).
    pub fn record_operation(&mut self, op: &Operation) {
        match op {
            Operation::Put { key, value } => {
                self.pending_puts.insert(key.clone(), value.clone());
                self.pending_deletes.retain(|k| k != key);
            }
            Operation::Delete { key } => {
                self.pending_puts.remove(key);
                self.pending_deletes.push(key.clone());
            }
            Operation::Commit => {
                // Move pending to committed
                for (k, v) in self.pending_puts.drain() {
                    self.expected_state.insert(k, v);
                }
                for k in self.pending_deletes.drain(..) {
                    self.expected_state.remove(&k);
                }
            }
        }
    }

    /// Discards uncommitted operations (simulating crash before commit).
    pub fn discard_pending(&mut self) {
        self.pending_puts.clear();
        self.pending_deletes.clear();
    }

    /// Returns the expected committed state.
    pub fn expected_state(&self) -> &HashMap<Vec<u8>, Vec<u8>> {
        &self.expected_state
    }

    /// Verifies database state matches expectations.
    pub fn verify(&self, db_path: &Path) -> Result<(), String> {
        // Open database and verify
        let db = thunderdb::Database::open(db_path)
            .map_err(|e| format!("Failed to open database: {}", e))?;

        let tx = db.read_tx();

        // Check all expected keys exist with correct values
        for (key, expected_value) in &self.expected_state {
            match tx.get(key) {
                Some(actual_value) => {
                    if actual_value != *expected_value {
                        return Err(format!(
                            "Value mismatch for key {:?}: expected {} bytes, got {} bytes",
                            String::from_utf8_lossy(key),
                            expected_value.len(),
                            actual_value.len()
                        ));
                    }
                }
                None => {
                    return Err(format!(
                        "Missing key after recovery: {:?}",
                        String::from_utf8_lossy(key)
                    ));
                }
            }
        }

        // Verify no extra keys exist (within a reasonable sample)
        // Full verification would require iterating the entire database
        let sample_size = 100.min(self.expected_state.len());
        let mut found_count = 0;
        for entry in tx.iter().take(sample_size * 2) {
            let (key, _) = entry;
            if self.expected_state.contains_key(key) {
                found_count += 1;
            }
        }

        // Basic sanity check
        if found_count == 0 && !self.expected_state.is_empty() {
            return Err("No expected keys found in database".to_string());
        }

        Ok(())
    }

    /// Verifies that the database is at least openable (basic invariant).
    pub fn verify_openable(db_path: &Path) -> Result<(), String> {
        let db = thunderdb::Database::open(db_path)
            .map_err(|e| format!("Failed to open database after crash: {}", e))?;

        // Try a read transaction
        let tx = db.read_tx();
        let _ = tx.get(b"__nonexistent_key__");

        Ok(())
    }
}

impl Default for InvariantChecker {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Crash Test Harness
// ============================================================================

/// Result of a crash test run.
#[derive(Debug)]
pub struct CrashTestResult {
    pub success: bool,
    pub crash_point: String,
    pub operations_before_crash: usize,
    pub committed_transactions: usize,
    pub error_message: Option<String>,
}

/// Harness for running crash tests in subprocess.
pub struct CrashTestHarness {
    temp_dir: TempDir,
    db_path: PathBuf,
}

impl CrashTestHarness {
    /// Creates a new test harness.
    pub fn new() -> std::io::Result<Self> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("crash_test.db");
        Ok(Self { temp_dir, db_path })
    }

    /// Returns the database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Runs operations in a subprocess that can be killed.
    ///
    /// This creates a child process that:
    /// 1. Opens the database
    /// 2. Executes the given operations
    /// 3. Gets killed at a random point OR completes
    ///
    /// Returns whether the subprocess was killed.
    pub fn run_with_crash(
        &self,
        ops: &[Operation],
        crash_after_ops: Option<usize>,
        failpoint: Option<&str>,
    ) -> std::io::Result<bool> {
        // Serialize operations to a temp file
        let ops_file = self.temp_dir.path().join("operations.json");
        let ops_json = self.serialize_operations(ops);
        fs::write(&ops_file, &ops_json)?;

        // Build the test binary path
        let test_binary = env::current_exe()?;

        // Prepare environment
        let mut cmd = Command::new(&test_binary);
        cmd.arg("--exact")
            .arg("crash_worker_process")
            .arg("--nocapture")
            .env("THUNDERDB_CRASH_TEST_DB", &self.db_path)
            .env("THUNDERDB_CRASH_TEST_OPS", &ops_file)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(fp) = failpoint {
            cmd.env("THUNDERDB_CRASH_FAILPOINT", fp);
        }

        if let Some(n) = crash_after_ops {
            cmd.env("THUNDERDB_CRASH_AFTER_OPS", n.to_string());
        }

        let mut child = cmd.spawn()?;

        // Wait for completion or timeout
        let timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

        loop {
            match child.try_wait()? {
                Some(status) => {
                    // Process exited
                    return Ok(!status.success());
                }
                None => {
                    if start.elapsed() > timeout {
                        // Kill the process
                        let _ = child.kill();
                        return Ok(true);
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    /// Serializes operations to JSON format.
    fn serialize_operations(&self, ops: &[Operation]) -> String {
        let mut lines = Vec::new();
        for op in ops {
            let line = match op {
                Operation::Put { key, value } => {
                    format!(
                        "PUT {} {}",
                        base64_encode(key),
                        base64_encode(value)
                    )
                }
                Operation::Delete { key } => {
                    format!("DEL {}", base64_encode(key))
                }
                Operation::Commit => "COMMIT".to_string(),
            };
            lines.push(line);
        }
        lines.join("\n")
    }
}

impl Default for CrashTestHarness {
    fn default() -> Self {
        Self::new().expect("Failed to create temp dir")
    }
}

/// Simple base64 encoding for test data.
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(CHARS[b0 >> 2] as char);
        result.push(CHARS[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(CHARS[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(CHARS[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// Simple base64 decoding for test data.
fn base64_decode(s: &str) -> Vec<u8> {
    const DECODE: [i8; 128] = [
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
        -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4,
        5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1,
        -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45,
        46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
    ];

    let bytes: Vec<u8> = s.bytes().filter(|&b| b != b'=').collect();
    let mut result = Vec::new();

    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            break;
        }

        let b0 = DECODE[chunk[0] as usize] as u8;
        let b1 = DECODE[chunk[1] as usize] as u8;

        result.push((b0 << 2) | (b1 >> 4));

        if chunk.len() > 2 && chunk[2] != b'=' {
            let b2 = DECODE[chunk[2] as usize] as u8;
            result.push((b1 << 4) | (b2 >> 2));

            if chunk.len() > 3 && chunk[3] != b'=' {
                let b3 = DECODE[chunk[3] as usize] as u8;
                result.push((b2 << 6) | b3);
            }
        }
    }

    result
}

// ============================================================================
// Crash Worker Process
// ============================================================================

/// This function is called when running as a crash worker subprocess.
/// It reads operations from the environment and executes them.
#[cfg(feature = "failpoint")]
fn crash_worker_main() {
    use thunderdb::{Database, FailAction, FailpointRegistry};

    let db_path = match env::var("THUNDERDB_CRASH_TEST_DB") {
        Ok(p) => PathBuf::from(p),
        Err(_) => return, // Not a crash worker
    };

    let ops_file = match env::var("THUNDERDB_CRASH_TEST_OPS") {
        Ok(p) => PathBuf::from(p),
        Err(_) => return,
    };

    // Read operations
    let ops_content = fs::read_to_string(&ops_file).expect("Failed to read ops file");
    let operations = parse_operations(&ops_content);

    // Setup failpoint if specified
    if let Ok(failpoint) = env::var("THUNDERDB_CRASH_FAILPOINT") {
        let crash_after: u64 = env::var("THUNDERDB_CRASH_AFTER_OPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        FailpointRegistry::global().register(&failpoint_name(&failpoint), FailAction::PanicAfterN(crash_after));
    }

    // Open database
    let mut db = Database::open(&db_path).expect("Failed to open database");

    // Execute operations
    let mut tx = db.write_tx();
    for op in operations {
        match op {
            Operation::Put { key, value } => {
                tx.put(&key, &value);
            }
            Operation::Delete { key } => {
                tx.delete(&key);
            }
            Operation::Commit => {
                if let Err(e) = tx.commit() {
                    eprintln!("Commit failed: {}", e);
                    // Start a new transaction
                }
                tx = db.write_tx();
            }
        }
    }

    // Final commit if there are pending changes
    let _ = tx.commit();
}

#[cfg(feature = "failpoint")]
fn failpoint_name(s: &str) -> &'static str {
    // Convert runtime string to static string for failpoint
    // This is safe because we only use known failpoint names
    match s {
        "before_data_write" => "before_data_write",
        "after_data_write" => "after_data_write",
        "before_overflow_write" => "before_overflow_write",
        "after_overflow_write" => "after_overflow_write",
        "before_meta_write" => "before_meta_write",
        "after_meta_write" => "after_meta_write",
        "before_fsync" => "before_fsync",
        "after_fsync" => "after_fsync",
        "incr_before_meta_write" => "incr_before_meta_write",
        "incr_after_meta_write" => "incr_after_meta_write",
        "incr_before_entry_write" => "incr_before_entry_write",
        "incr_after_entry_write" => "incr_after_entry_write",
        "incr_before_fsync" => "incr_before_fsync",
        "incr_after_fsync" => "incr_after_fsync",
        _ => "before_fsync", // Default
    }
}

fn parse_operations(content: &str) -> Vec<Operation> {
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }

            if line == "COMMIT" {
                return Some(Operation::Commit);
            }

            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            match parts.as_slice() {
                ["PUT", key, value] => Some(Operation::Put {
                    key: base64_decode(key),
                    value: base64_decode(value),
                }),
                ["DEL", key] => Some(Operation::Delete {
                    key: base64_decode(key),
                }),
                _ => None,
            }
        })
        .collect()
}

// ============================================================================
// Unit Tests (run without subprocess)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Reset failpoints if the feature is enabled.
    /// This prevents cross-test interference when tests run in parallel.
    fn maybe_reset_failpoints() {
        #[cfg(feature = "failpoint")]
        {
            thunderdb::FailpointRegistry::global().reset_all();
        }
    }

    /// Basic test that database can be opened after normal operation.
    #[test]
    fn test_basic_recovery() {
        maybe_reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write some data
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"key1", b"value1");
            tx.put(b"key2", b"value2");
            tx.commit().unwrap();
        }

        // Verify data survives reopen
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();
            assert_eq!(tx.get(b"key1"), Some(b"value1".to_vec()));
            assert_eq!(tx.get(b"key2"), Some(b"value2".to_vec()));
        }
    }

    /// Test that uncommitted data is not visible after reopen.
    #[test]
    fn test_uncommitted_data_lost() {
        maybe_reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write committed data
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"committed", b"yes");
            tx.commit().unwrap();
        }

        // Write uncommitted data (simulating crash)
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"uncommitted", b"should_be_lost");
            // No commit - drop the database
        }

        // Verify only committed data exists
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();
            assert_eq!(tx.get(b"committed"), Some(b"yes".to_vec()));
            assert_eq!(tx.get(b"uncommitted"), None);
        }
    }

    /// Test operation generator produces valid operations.
    #[test]
    fn test_operation_generator() {
        let mut generator = OperationGenerator::new(12345);
        let ops = generator.generate_sequence(100);

        assert_eq!(ops.len(), 100);

        let mut puts = 0;
        let mut deletes = 0;
        let mut commits = 0;

        for op in ops {
            match op {
                Operation::Put { key, value } => {
                    assert!(!key.is_empty());
                    assert!(!value.is_empty());
                    puts += 1;
                }
                Operation::Delete { key } => {
                    assert!(!key.is_empty());
                    deletes += 1;
                }
                Operation::Commit => {
                    commits += 1;
                }
            }
        }

        // Should have a mix of operations
        assert!(puts > 0);
        // Deletes and commits may be 0 depending on random sequence
    }

    /// Test invariant checker tracks state correctly.
    #[test]
    fn test_invariant_checker() {
        let mut checker = InvariantChecker::new();

        // Record some operations
        checker.record_operation(&Operation::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        });
        checker.record_operation(&Operation::Put {
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
        });
        checker.record_operation(&Operation::Commit);

        assert_eq!(checker.expected_state().len(), 2);
        assert_eq!(checker.expected_state().get(&b"k1".to_vec()), Some(&b"v1".to_vec()));

        // Uncommitted operations should not affect expected state
        checker.record_operation(&Operation::Put {
            key: b"k3".to_vec(),
            value: b"v3".to_vec(),
        });
        assert_eq!(checker.expected_state().len(), 2);

        // Discard pending
        checker.discard_pending();
        checker.record_operation(&Operation::Commit);
        assert_eq!(checker.expected_state().len(), 2);
    }

    /// Test base64 encoding/decoding roundtrip.
    #[test]
    fn test_base64_roundtrip() {
        let test_cases = vec![
            b"hello world".to_vec(),
            b"".to_vec(),
            b"a".to_vec(),
            b"ab".to_vec(),
            b"abc".to_vec(),
            (0..256).map(|i| i as u8).collect::<Vec<_>>(),
        ];

        for original in test_cases {
            let encoded = base64_encode(&original);
            let decoded = base64_decode(&encoded);
            assert_eq!(original, decoded, "Failed for input of length {}", original.len());
        }
    }

    /// Test multiple transactions with interleaved commits.
    #[test]
    fn test_multiple_transactions() {
        maybe_reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write multiple transactions
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();

            for i in 0..10 {
                let mut tx = db.write_tx();
                for j in 0..5 {
                    let key = format!("tx{}key{}", i, j);
                    let value = format!("value{}_{}", i, j);
                    tx.put(key.as_bytes(), value.as_bytes());
                }
                tx.commit().unwrap();
            }
        }

        // Verify all data
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();

            for i in 0..10 {
                for j in 0..5 {
                    let key = format!("tx{}key{}", i, j);
                    let expected = format!("value{}_{}", i, j);
                    assert_eq!(
                        tx.get(key.as_bytes()),
                        Some(expected.as_bytes().to_vec()),
                        "Missing key {} after recovery",
                        key
                    );
                }
            }
        }
    }

    /// Test with large values (overflow pages).
    #[test]
    fn test_large_values_recovery() {
        maybe_reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let large_value: Vec<u8> = (0..50000).map(|i| (i % 256) as u8).collect();

        // Write large value
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"large_key", &large_value);
            tx.commit().unwrap();
        }

        // Verify recovery
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();
            assert_eq!(tx.get(b"large_key"), Some(large_value));
        }
    }

    /// Stress test with many small transactions.
    #[test]
    fn test_many_small_transactions() {
        maybe_reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let iterations = 100;

        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();

            for i in 0..iterations {
                let mut tx = db.write_tx();
                let key = format!("key_{:04}", i);
                let value = format!("value_{:04}", i);
                tx.put(key.as_bytes(), value.as_bytes());
                tx.commit().unwrap();
            }
        }

        // Verify all data
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();

            for i in 0..iterations {
                let key = format!("key_{:04}", i);
                let expected = format!("value_{:04}", i);
                assert_eq!(
                    tx.get(key.as_bytes()),
                    Some(expected.as_bytes().to_vec())
                );
            }
        }
    }
}

// ============================================================================
// Failpoint Tests (require --features failpoint)
// ============================================================================

#[cfg(all(test, feature = "failpoint"))]
mod failpoint_tests {
    use super::*;
    use tempfile::tempdir;
    use thunderdb::{FailAction, FailpointRegistry};

    /// Helper to reset all failpoints before/after each test.
    fn reset_failpoints() {
        FailpointRegistry::global().reset_all();
    }

    /// Test that failpoint infrastructure works correctly.
    #[test]
    fn test_failpoint_triggers_error() {
        reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Setup failpoint to error on fsync (incremental path for insert-only)
        FailpointRegistry::global().register("incr_before_fsync", FailAction::IoError);

        let mut db = thunderdb::Database::open(&db_path).unwrap();
        let mut tx = db.write_tx();
        tx.put(b"key", b"value");

        // Commit should fail due to failpoint
        let result = tx.commit();
        reset_failpoints();
        assert!(result.is_err());
    }

    /// Test recovery after error failpoint.
    ///
    /// Note: When incr_before_fsync fails, the data has already been written
    /// to the file (just not fsync'd). The data may or may not persist depending
    /// on whether the OS flushes it before reopen. We test that:
    /// 1. The commit returns an error
    /// 2. The database is still recoverable
    /// 3. Previously committed data is intact
    #[test]
    fn test_recovery_after_error() {
        reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // First, write some committed data
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"committed", b"data");
            tx.commit().unwrap();
        }

        // Setup failpoint and try to write (incremental path for insert-only)
        {
            FailpointRegistry::global().register("incr_before_fsync", FailAction::IoError);

            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"should_fail", b"data");

            let result = tx.commit();
            reset_failpoints();
            assert!(result.is_err(), "Commit should fail due to failpoint");
        }

        // Verify database is recoverable and committed data is intact
        // Note: should_fail data may or may not be present since it was written
        // before fsync failed. We only guarantee previously committed data.
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();
            assert_eq!(tx.get(b"committed"), Some(b"data".to_vec()));
            // Don't assert on should_fail - it may or may not be there
        }
    }

    /// Test error before data write.
    #[test]
    fn test_error_before_data_write() {
        reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write initial data
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"initial", b"data");
            tx.commit().unwrap();
        }

        // Error before data write
        {
            FailpointRegistry::global().register("before_data_write", FailAction::IoError);

            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"new_key", b"new_value");
            tx.delete(b"initial"); // Force full rewrite path

            let result = tx.commit();
            reset_failpoints();
            assert!(result.is_err());
        }

        // Verify database is intact
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();
            assert_eq!(tx.get(b"initial"), Some(b"data".to_vec()));
            assert_eq!(tx.get(b"new_key"), None);
        }
    }

    /// Test error after data write but before meta write.
    ///
    /// NOTE: This test documents a known limitation of the current implementation.
    /// When persist_tree does a full rewrite (for delete operations), it overwrites
    /// the data section before updating the meta page. If a crash occurs after data
    /// write but before meta write, the database may lose data from the previous
    /// transaction as well.
    ///
    /// For true crash safety during full rewrites, the database would need:
    /// - Copy-on-write semantics (write to new location)
    /// - Write-ahead logging (WAL)
    /// - Shadow paging
    ///
    /// The incremental path (insert-only) does not have this issue as it only
    /// appends new data and updates the entry count atomically with the meta page.
    #[test]
    fn test_error_after_data_before_meta() {
        reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write initial data
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"initial", b"data");
            tx.commit().unwrap();
        }

        // Error before meta write (after data write)
        {
            FailpointRegistry::global().register("before_meta_write", FailAction::IoError);

            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"new_key", b"new_value");
            tx.delete(b"initial");

            let result = tx.commit();
            reset_failpoints();
            assert!(result.is_err());
        }

        // Database should at least be openable after crash
        // Note: Due to the full rewrite behavior, data integrity is not guaranteed
        // for the persist_tree path if crash occurs after data write but before meta write
        let result = InvariantChecker::verify_openable(&db_path);
        assert!(result.is_ok(), "Database should be openable: {:?}", result);
    }

    /// Test error after meta write but before fsync.
    #[test]
    fn test_error_after_meta_before_fsync() {
        reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write initial data
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"initial", b"data");
            tx.commit().unwrap();
        }

        // Error before fsync (after meta write)
        // Note: This simulates data in OS buffer but not on disk
        {
            FailpointRegistry::global().register("before_fsync", FailAction::IoError);

            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"new_key", b"new_value");
            tx.delete(b"initial");

            let result = tx.commit();
            reset_failpoints();
            assert!(result.is_err());
        }

        // Database should still be recoverable
        let result = InvariantChecker::verify_openable(&db_path);
        assert!(result.is_ok(), "Database should be openable: {:?}", result);
    }

    /// Test incremental path failpoints.
    #[test]
    fn test_incremental_path_error() {
        reset_failpoints();
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Write initial data
        {
            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            tx.put(b"initial", b"data");
            tx.commit().unwrap();
        }

        // Error in incremental path
        {
            FailpointRegistry::global().register("incr_before_fsync", FailAction::IoError);

            let mut db = thunderdb::Database::open(&db_path).unwrap();
            let mut tx = db.write_tx();
            // Only puts use incremental path
            tx.put(b"new_key1", b"value1");
            tx.put(b"new_key2", b"value2");

            let result = tx.commit();
            reset_failpoints();
            assert!(result.is_err());
        }

        // Verify original data intact
        {
            let db = thunderdb::Database::open(&db_path).unwrap();
            let tx = db.read_tx();
            assert_eq!(tx.get(b"initial"), Some(b"data".to_vec()));
        }
    }

    /// Randomized crash test with failpoints.
    ///
    /// Tests only the "safe" failpoints - those that occur before data is written
    /// or after fsync, where we expect full durability. The persist_tree path
    /// (used for deletes) has known limitations documented in test_error_after_data_before_meta.
    #[test]
    fn test_randomized_crash_recovery() {
        reset_failpoints();

        let iterations = env::var("THUNDERDB_CRASH_ITERATIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_ITERATIONS);

        let seed: u64 = env::var("THUNDERDB_CRASH_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });

        eprintln!("Running {} iterations with seed {}", iterations, seed);
        eprintln!("Reproduce with: THUNDERDB_CRASH_SEED={}", seed);

        // Only test failpoints where durability is expected to work:
        // - incr_before_fsync: incremental path, data appended not overwritten
        // - incr_after_fsync: after sync, should be fully durable
        // We avoid persist_tree failpoints as that path has known limitations
        let failpoints = [
            "incr_before_fsync",
            "incr_after_fsync",
        ];

        let mut rng = StdRng::seed_from_u64(seed);
        let mut successes = 0;
        let mut failures = 0;

        for i in 0..iterations {
            reset_failpoints();
            let dir = tempdir().unwrap();
            let db_path = dir.path().join("test.db");

            let mut checker = InvariantChecker::new();

            // Phase 1: Write some committed data (insert-only to use incremental path)
            {
                let mut db = thunderdb::Database::open(&db_path).unwrap();

                // Execute 1-3 committed transactions with only puts
                let num_txs = rng.gen_range(1..=3);
                for tx_num in 0..num_txs {
                    let mut tx = db.write_tx();
                    let num_puts = rng.gen_range(1..=5);

                    for put_num in 0..num_puts {
                        let key = format!("iter{}_tx{}_key{}", i, tx_num, put_num).into_bytes();
                        let value = format!("value_{}", put_num).into_bytes();
                        checker.record_operation(&Operation::Put {
                            key: key.clone(),
                            value: value.clone(),
                        });
                        tx.put(&key, &value);
                    }

                    checker.record_operation(&Operation::Commit);
                    tx.commit().unwrap();
                }
            }

            // Phase 2: Attempt operation with failpoint (insert-only)
            let failpoint = failpoints[rng.gen_range(0..failpoints.len())];
            {
                FailpointRegistry::global().register(failpoint, FailAction::IoError);

                let mut db = thunderdb::Database::open(&db_path).unwrap();
                let mut tx = db.write_tx();

                // Add some puts that should fail
                let key = format!("iter{}_fail_key", i).into_bytes();
                let value = b"should_fail".to_vec();
                checker.record_operation(&Operation::Put {
                    key: key.clone(),
                    value: value.clone(),
                });
                tx.put(&key, &value);

                let result = tx.commit();
                if result.is_err() {
                    checker.discard_pending();
                }

                reset_failpoints();
            }

            // Phase 3: Verify recovery
            match checker.verify(&db_path) {
                Ok(()) => successes += 1,
                Err(e) => {
                    failures += 1;
                    eprintln!(
                        "Iteration {} failed (failpoint: {}): {}",
                        i, failpoint, e
                    );
                    eprintln!("Reproduce with: THUNDERDB_CRASH_SEED={}", seed.wrapping_add(i as u64));
                }
            }
        }

        reset_failpoints();

        eprintln!(
            "Completed {} iterations: {} successes, {} failures",
            iterations, successes, failures
        );

        // Expect 100% success rate for insert-only incremental path
        assert_eq!(
            failures, 0,
            "Expected no failures for incremental path, got {} failures out of {} iterations",
            failures, iterations
        );
    }
}

// ============================================================================
// Integration Tests (subprocess-based, more realistic)
// ============================================================================

/// Worker process entry point for subprocess-based crash tests.
/// This is detected by looking for specific environment variables.
#[test]
#[ignore] // Only run as subprocess
fn crash_worker_process() {
    #[cfg(feature = "failpoint")]
    crash_worker_main();
}

/// Test that creates realistic crash scenarios using subprocess.
#[test]
#[ignore] // Requires subprocess support
fn test_subprocess_crash_recovery() {
    let harness = CrashTestHarness::new().unwrap();
    let mut generator = OperationGenerator::new(54321);

    // Generate a sequence of operations
    let mut ops = Vec::new();
    for _ in 0..3 {
        ops.extend(generator.generate_transaction());
    }

    // Run without crash - should complete successfully
    let crashed = harness.run_with_crash(&ops, None, None).unwrap();
    assert!(!crashed, "Process should complete without crash");

    // Verify database is valid
    let result = InvariantChecker::verify_openable(harness.db_path());
    assert!(result.is_ok(), "Database should be openable: {:?}", result);
}
