//! Summary: Write-ahead logging for transaction durability.
//! Copyright (c) YOAB. All rights reserved.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::wal_record::{WalRecord, RECORD_HEADER_SIZE};

/// Default WAL segment size (64MB).
pub const WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Segment file header size.
const SEGMENT_HEADER_SIZE: u64 = 64;

/// Magic bytes for WAL segment files.
const WAL_MAGIC: u32 = 0x574C_4F47; // "WLOG" in little-endian

/// WAL format version.
const WAL_VERSION: u32 = 1;

/// Log Sequence Number - uniquely identifies a position in the WAL.
///
/// Format: `(segment_id << 32) | offset_in_segment`
pub type Lsn = u64;

/// Sync policy for WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// fsync after every write (safest, slowest).
    Immediate,
    /// fsync periodically or when buffer fills (balanced).
    Batched(Duration),
    /// No fsync (for testing only - data loss on crash).
    None,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Batched(Duration::from_millis(10))
    }
}

/// WAL configuration options.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Size of each WAL segment file in bytes.
    pub segment_size: u64,
    /// Sync policy for durability.
    pub sync_policy: SyncPolicy,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size: WAL_SEGMENT_SIZE,
            sync_policy: SyncPolicy::default(),
        }
    }
}

/// WAL segment file handle.
struct WalSegment {
    file: File,
    segment_id: u64,
    write_offset: u64,
    /// Path to segment file (kept for potential future operations like deletion).
    #[allow(dead_code)]
    path: PathBuf,
}

impl WalSegment {
    /// Creates a new segment file.
    fn create(dir: &Path, segment_id: u64, first_lsn: Lsn) -> Result<Self> {
        let path = segment_path(dir, segment_id);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::WalCorrupted {
                segment_id,
                offset: 0,
                reason: format!("failed to create segment: {e}"),
            })?;

        // Write segment header
        let header = SegmentHeader {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
            segment_id,
            first_lsn,
        };
        let header_bytes = header.to_bytes();
        file.write_all(&header_bytes).map_err(|e| Error::WalCorrupted {
            segment_id,
            offset: 0,
            reason: format!("failed to write header: {e}"),
        })?;

        Ok(Self {
            file,
            segment_id,
            write_offset: SEGMENT_HEADER_SIZE,
            path,
        })
    }

    /// Opens an existing segment file.
    fn open(dir: &Path, segment_id: u64) -> Result<Self> {
        let path = segment_path(dir, segment_id);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| Error::WalCorrupted {
                segment_id,
                offset: 0,
                reason: format!("failed to open segment: {e}"),
            })?;

        // Read and validate header
        let mut header_buf = [0u8; SEGMENT_HEADER_SIZE as usize];
        file.read_exact(&mut header_buf).map_err(|e| Error::WalCorrupted {
            segment_id,
            offset: 0,
            reason: format!("failed to read header: {e}"),
        })?;

        let header = SegmentHeader::from_bytes(&header_buf).ok_or_else(|| Error::WalCorrupted {
            segment_id,
            offset: 0,
            reason: "invalid segment header".to_string(),
        })?;

        if header.segment_id != segment_id {
            return Err(Error::WalCorrupted {
                segment_id,
                offset: 0,
                reason: format!(
                    "segment ID mismatch: expected {segment_id}, got {}",
                    header.segment_id
                ),
            });
        }

        // Find write offset (end of valid data)
        let file_len = file.metadata().map_err(|e| Error::WalCorrupted {
            segment_id,
            offset: 0,
            reason: format!("failed to get metadata: {e}"),
        })?.len();

        let write_offset = Self::find_write_offset(&mut file, segment_id, file_len)?;

        // Seek to write position
        file.seek(SeekFrom::Start(write_offset)).map_err(|e| Error::WalCorrupted {
            segment_id,
            offset: write_offset,
            reason: format!("failed to seek: {e}"),
        })?;

        Ok(Self {
            file,
            segment_id,
            write_offset,
            path,
        })
    }

    /// Finds the write offset by scanning for the end of valid records.
    fn find_write_offset(file: &mut File, segment_id: u64, file_len: u64) -> Result<u64> {
        let mut offset = SEGMENT_HEADER_SIZE;

        while offset < file_len {
            file.seek(SeekFrom::Start(offset)).map_err(|e| Error::WalCorrupted {
                segment_id,
                offset,
                reason: format!("seek error: {e}"),
            })?;

            // Try to read record length
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(Error::WalCorrupted {
                        segment_id,
                        offset,
                        reason: format!("read error: {e}"),
                    });
                }
            }

            let record_len = u32::from_le_bytes(len_buf) as u64;

            // Validate record length
            if record_len < RECORD_HEADER_SIZE as u64 || offset + record_len > file_len {
                // Likely corrupted or incomplete record - stop here
                break;
            }

            // Read full record and validate CRC
            file.seek(SeekFrom::Start(offset)).map_err(|e| Error::WalCorrupted {
                segment_id,
                offset,
                reason: format!("seek error: {e}"),
            })?;

            let mut record_buf = vec![0u8; record_len as usize];
            match file.read_exact(&mut record_buf) {
                Ok(()) => {}
                Err(_) => break,
            }

            // Validate record
            if WalRecord::decode(&record_buf).is_err() {
                // Invalid record - stop here
                break;
            }

            offset += record_len;
        }

        Ok(offset)
    }

    /// Appends a record to the segment.
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data).map_err(|e| Error::WalCorrupted {
            segment_id: self.segment_id,
            offset: self.write_offset,
            reason: format!("write error: {e}"),
        })?;
        self.write_offset += data.len() as u64;
        Ok(())
    }

    /// Syncs the segment to disk.
    fn sync(&mut self) -> Result<()> {
        self.file.sync_data().map_err(|e| Error::WalCorrupted {
            segment_id: self.segment_id,
            offset: self.write_offset,
            reason: format!("sync error: {e}"),
        })
    }

    /// Returns remaining space in segment.
    fn remaining(&self, segment_size: u64) -> u64 {
        segment_size.saturating_sub(self.write_offset)
    }
}

/// Segment file header.
struct SegmentHeader {
    magic: u32,
    version: u32,
    segment_id: u64,
    first_lsn: Lsn,
}

impl SegmentHeader {
    fn to_bytes(&self) -> [u8; SEGMENT_HEADER_SIZE as usize] {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE as usize];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.first_lsn.to_le_bytes());
        // Remaining bytes are reserved (zeroed)
        buf
    }

    fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < SEGMENT_HEADER_SIZE as usize {
            return None;
        }

        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != WAL_MAGIC {
            return None;
        }

        let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
        if version > WAL_VERSION {
            return None;
        }

        let segment_id = u64::from_le_bytes(buf[8..16].try_into().ok()?);
        let first_lsn = u64::from_le_bytes(buf[16..24].try_into().ok()?);

        Some(Self {
            magic,
            version,
            segment_id,
            first_lsn,
        })
    }
}

/// Generates the path for a segment file.
fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("wal_{segment_id:06}.log"))
}

/// Write-Ahead Log implementation.
///
/// Provides durable logging for database transactions with configurable
/// sync policies and automatic segment rotation.
pub struct Wal {
    dir: PathBuf,
    current_segment: WalSegment,
    config: WalConfig,
    /// Tracks pending bytes since last sync (for batched policy).
    pending_bytes: u64,
}

impl Wal {
    /// Opens or creates a WAL in the given directory.
    ///
    /// If the directory contains existing WAL segments, they will be
    /// opened for append. Otherwise, a new segment is created.
    pub fn open(dir: &Path, config: WalConfig) -> Result<Self> {
        // Ensure directory exists
        fs::create_dir_all(dir).map_err(|e| Error::WalCorrupted {
            segment_id: 0,
            offset: 0,
            reason: format!("failed to create WAL directory: {e}"),
        })?;

        // Find existing segments
        let segments = Self::list_segments(dir)?;

        let current_segment = if segments.is_empty() {
            // Create first segment
            WalSegment::create(dir, 0, 0)?
        } else {
            // Open the last segment
            let last_id = *segments.last().unwrap();
            WalSegment::open(dir, last_id)?
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            current_segment,
            config,
            pending_bytes: 0,
        })
    }

    /// Lists segment IDs in the WAL directory, sorted ascending.
    fn list_segments(dir: &Path) -> Result<Vec<u64>> {
        let mut segments = Vec::new();

        let entries = fs::read_dir(dir).map_err(|e| Error::WalCorrupted {
            segment_id: 0,
            offset: 0,
            reason: format!("failed to read WAL directory: {e}"),
        })?;

        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && name.starts_with("wal_")
                && name.ends_with(".log")
                && let Some(id_str) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log"))
                && let Ok(id) = id_str.parse::<u64>()
            {
                segments.push(id);
            }
        }

        segments.sort_unstable();
        Ok(segments)
    }

    /// Appends a record to the WAL.
    ///
    /// Returns the LSN of the appended record.
    pub fn append(&mut self, record: &WalRecord) -> Result<Lsn> {
        let data = record.encode();

        // Check if we need to rotate to a new segment
        if self.current_segment.remaining(self.config.segment_size) < data.len() as u64 {
            self.rotate_segment()?;
        }

        let lsn = self.make_lsn(self.current_segment.segment_id, self.current_segment.write_offset);

        self.current_segment.append(&data)?;
        self.pending_bytes += data.len() as u64;

        // Handle sync policy
        match self.config.sync_policy {
            SyncPolicy::Immediate => {
                self.current_segment.sync()?;
                self.pending_bytes = 0;
            }
            SyncPolicy::Batched(_) => {
                // Sync if we've accumulated significant data
                // The actual time-based batching is handled by GroupCommit
                if self.pending_bytes >= 32768 {
                    self.current_segment.sync()?;
                    self.pending_bytes = 0;
                }
            }
            SyncPolicy::None => {
                // No sync
            }
        }

        Ok(lsn)
    }

    /// Explicitly syncs the WAL to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.current_segment.sync()?;
        self.pending_bytes = 0;
        Ok(())
    }

    /// Returns the current LSN (next write position).
    pub fn current_lsn(&self) -> Lsn {
        self.make_lsn(self.current_segment.segment_id, self.current_segment.write_offset)
    }

    /// Truncates WAL segments before the given LSN.
    ///
    /// This is called after a checkpoint to reclaim space.
    pub fn truncate_before(&mut self, lsn: Lsn) -> Result<()> {
        let target_segment = self.segment_id_from_lsn(lsn);

        let segments = Self::list_segments(&self.dir)?;

        for segment_id in segments {
            if segment_id < target_segment {
                let path = segment_path(&self.dir, segment_id);
                let _ = fs::remove_file(&path);
            }
        }

        Ok(())
    }

    /// Replays records from the given LSN.
    ///
    /// The callback is invoked for each record in LSN order.
    /// Returns the final LSN after replay.
    pub fn replay<F>(&self, from_lsn: Lsn, mut callback: F) -> Result<Lsn>
    where
        F: FnMut(WalRecord) -> Result<()>,
    {
        let start_segment = self.segment_id_from_lsn(from_lsn);
        let start_offset = self.offset_from_lsn(from_lsn);

        let segments = Self::list_segments(&self.dir)?;
        let mut last_lsn = from_lsn;

        for segment_id in segments {
            if segment_id < start_segment {
                continue;
            }

            let path = segment_path(&self.dir, segment_id);
            let mut file = match File::open(&path) {
                Ok(f) => f,
                Err(_) => continue,
            };

            // Read header to validate
            let mut header_buf = [0u8; SEGMENT_HEADER_SIZE as usize];
            if file.read_exact(&mut header_buf).is_err() {
                continue;
            }

            if SegmentHeader::from_bytes(&header_buf).is_none() {
                continue;
            }

            // Determine start offset for this segment
            let read_offset = if segment_id == start_segment {
                start_offset.max(SEGMENT_HEADER_SIZE)
            } else {
                SEGMENT_HEADER_SIZE
            };

            let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
            let mut offset = read_offset;

            while offset < file_len {
                if file.seek(SeekFrom::Start(offset)).is_err() {
                    break;
                }

                // Read record length
                let mut len_buf = [0u8; 4];
                match file.read_exact(&mut len_buf) {
                    Ok(()) => {}
                    Err(_) => break,
                }

                let record_len = u32::from_le_bytes(len_buf) as u64;
                if record_len < RECORD_HEADER_SIZE as u64 || offset + record_len > file_len {
                    break;
                }

                // Read full record
                if file.seek(SeekFrom::Start(offset)).is_err() {
                    break;
                }

                let mut record_buf = vec![0u8; record_len as usize];
                if file.read_exact(&mut record_buf).is_err() {
                    break;
                }

                // Decode and invoke callback
                match WalRecord::decode(&record_buf) {
                    Ok((record, _)) => {
                        callback(record)?;
                        last_lsn = self.make_lsn(segment_id, offset + record_len);
                    }
                    Err(_) => break,
                }

                offset += record_len;
            }
        }

        Ok(last_lsn)
    }

    /// Returns total bytes written across all segments (approximate WAL size).
    pub fn approximate_size(&self) -> u64 {
        let segments = Self::list_segments(&self.dir).unwrap_or_default();
        let full_segments = segments.len().saturating_sub(1) as u64;
        full_segments * self.config.segment_size + self.current_segment.write_offset
    }

    /// Rotates to a new segment.
    fn rotate_segment(&mut self) -> Result<()> {
        // Sync current segment before rotating
        self.current_segment.sync()?;

        let new_segment_id = self.current_segment.segment_id + 1;
        let first_lsn = self.make_lsn(new_segment_id, SEGMENT_HEADER_SIZE);

        let new_segment = WalSegment::create(&self.dir, new_segment_id, first_lsn)?;

        self.current_segment = new_segment;
        self.pending_bytes = 0;

        Ok(())
    }

    /// Constructs an LSN from segment ID and offset.
    #[inline]
    fn make_lsn(&self, segment_id: u64, offset: u64) -> Lsn {
        (segment_id << 32) | (offset & 0xFFFF_FFFF)
    }

    /// Extracts segment ID from LSN.
    #[inline]
    fn segment_id_from_lsn(&self, lsn: Lsn) -> u64 {
        lsn >> 32
    }

    /// Extracts offset from LSN.
    #[inline]
    fn offset_from_lsn(&self, lsn: Lsn) -> u64 {
        lsn & 0xFFFF_FFFF
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_wal_dir(name: &str) -> PathBuf {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        PathBuf::from(format!("/tmp/thunder_wal_test_{name}_{pid}_{counter}"))
    }

    fn cleanup(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_wal_create_and_append() {
        let dir = test_wal_dir("create_append");
        cleanup(&dir);

        let config = WalConfig {
            segment_size: 1024 * 1024,
            sync_policy: SyncPolicy::Immediate,
        };

        let mut wal = Wal::open(&dir, config).expect("open");

        let lsn1 = wal
            .append(&WalRecord::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            })
            .expect("append 1");

        let lsn2 = wal
            .append(&WalRecord::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            })
            .expect("append 2");

        assert!(lsn2 > lsn1, "LSNs should be monotonically increasing");

        cleanup(&dir);
    }

    #[test]
    fn test_wal_replay() {
        let dir = test_wal_dir("replay");
        cleanup(&dir);

        let config = WalConfig {
            segment_size: 1024 * 1024,
            sync_policy: SyncPolicy::Immediate,
        };

        // Write records
        {
            let mut wal = Wal::open(&dir, config.clone()).expect("open");
            for i in 0..10 {
                wal.append(&WalRecord::Put {
                    key: format!("key_{i}").into_bytes(),
                    value: format!("value_{i}").into_bytes(),
                })
                .expect("append");
            }
            wal.sync().expect("sync");
        }

        // Replay
        {
            let wal = Wal::open(&dir, config).expect("reopen");
            let mut count = 0;
            wal.replay(0, |_record| {
                count += 1;
                Ok(())
            })
            .expect("replay");
            assert_eq!(count, 10);
        }

        cleanup(&dir);
    }

    #[test]
    fn test_wal_segment_rotation() {
        let dir = test_wal_dir("rotation");
        cleanup(&dir);

        let config = WalConfig {
            segment_size: 1024, // Small segments
            sync_policy: SyncPolicy::Immediate,
        };

        let mut wal = Wal::open(&dir, config).expect("open");

        // Write enough to trigger rotation
        for i in 0..50 {
            wal.append(&WalRecord::Put {
                key: format!("key_{i:05}").into_bytes(),
                value: vec![0xAA; 32],
            })
            .expect("append");
        }

        // Check multiple segments created
        let segments = Wal::list_segments(&dir).expect("list");
        assert!(segments.len() > 1, "should have multiple segments");

        cleanup(&dir);
    }

    #[test]
    fn test_wal_truncate() {
        let dir = test_wal_dir("truncate");
        cleanup(&dir);

        let config = WalConfig {
            segment_size: 512,
            sync_policy: SyncPolicy::Immediate,
        };

        let mut wal = Wal::open(&dir, config.clone()).expect("open");

        // Write records to create multiple segments
        for i in 0..100 {
            wal.append(&WalRecord::Put {
                key: format!("key_{i}").into_bytes(),
                value: vec![0xBB; 16],
            })
            .expect("append");
        }

        let checkpoint_lsn = wal.current_lsn();
        let segments_before = Wal::list_segments(&dir).expect("list").len();

        // Write more
        for i in 100..150 {
            wal.append(&WalRecord::Put {
                key: format!("key_{i}").into_bytes(),
                value: vec![0xCC; 16],
            })
            .expect("append");
        }

        // Truncate
        wal.truncate_before(checkpoint_lsn).expect("truncate");

        let segments_after = Wal::list_segments(&dir).expect("list").len();
        assert!(
            segments_after <= segments_before,
            "truncation should remove old segments"
        );

        cleanup(&dir);
    }

    #[test]
    fn test_wal_recovery_after_crash() {
        let dir = test_wal_dir("crash_recovery");
        cleanup(&dir);

        let config = WalConfig {
            segment_size: 1024 * 1024,
            sync_policy: SyncPolicy::Immediate,
        };

        let expected_keys: Vec<String> = (0..20).map(|i| format!("key_{i}")).collect();

        // Write records
        {
            let mut wal = Wal::open(&dir, config.clone()).expect("open");
            for key in &expected_keys {
                wal.append(&WalRecord::Put {
                    key: key.as_bytes().to_vec(),
                    value: b"value".to_vec(),
                })
                .expect("append");
            }
            wal.sync().expect("sync");
        }

        // "Crash" and reopen
        {
            let wal = Wal::open(&dir, config).expect("reopen");
            let mut recovered_keys = Vec::new();

            wal.replay(0, |record| {
                if let WalRecord::Put { key, .. } = record {
                    recovered_keys.push(String::from_utf8(key).unwrap());
                }
                Ok(())
            })
            .expect("replay");

            assert_eq!(recovered_keys, expected_keys);
        }

        cleanup(&dir);
    }
}
