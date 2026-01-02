//! Summary: Meta page handling for crash recovery and database state.
//! Copyright (c) YOAB. All rights reserved.

use crate::checkpoint::CheckpointInfo;
use crate::page::{PageId, PageSizeConfig, MAGIC, PAGE_SIZE, VERSION};
use crate::wal::Lsn;

/// Meta page structure stored at the beginning of the database file.
///
/// Two meta pages exist (at page 0 and page 1) for crash recovery.
/// The one with the higher transaction ID is considered current.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Meta {
    /// Magic number to identify valid thunder database files.
    pub magic: u32,
    /// Database file format version.
    pub version: u32,
    /// Page size in bytes.
    pub page_size: u32,
    /// Transaction ID (incremented on each commit).
    pub txid: u64,
    /// Root page ID of the B+ tree.
    pub root: PageId,
    /// Page ID of the freelist.
    pub freelist: PageId,
    /// Total number of pages in the database file.
    pub page_count: u64,
    /// Checksum for integrity validation.
    pub checksum: u64,
    // Phase 4: WAL checkpoint fields (bytes 64-88)
    /// LSN of last checkpoint.
    pub checkpoint_lsn: Lsn,
    /// Unix timestamp of last checkpoint.
    pub checkpoint_timestamp: u64,
    /// Entry count at last checkpoint.
    pub checkpoint_entry_count: u64,
}

impl Meta {
    /// Size of the meta structure in bytes (extended for checkpoint fields).
    pub const SIZE: usize = 88;

    /// Creates a new meta page with default values for a fresh database.
    pub fn new() -> Self {
        Self::with_page_size(PAGE_SIZE as u32)
    }

    /// Creates a new meta page with a specific page size.
    pub fn with_page_size(page_size: u32) -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            page_size,
            txid: 0,
            root: 0,
            freelist: 0,
            page_count: 2, // Two meta pages initially
            checksum: 0,
            checkpoint_lsn: 0,
            checkpoint_timestamp: 0,
            checkpoint_entry_count: 0,
        }
    }

    /// Returns checkpoint info from this meta page.
    pub fn checkpoint_info(&self) -> CheckpointInfo {
        CheckpointInfo {
            lsn: self.checkpoint_lsn,
            timestamp: self.checkpoint_timestamp,
            entry_count: self.checkpoint_entry_count,
        }
    }

    /// Sets checkpoint info on this meta page.
    pub fn set_checkpoint_info(&mut self, info: &CheckpointInfo) {
        self.checkpoint_lsn = info.lsn;
        self.checkpoint_timestamp = info.timestamp;
        self.checkpoint_entry_count = info.entry_count;
    }

    /// Validates the meta page.
    ///
    /// Checks magic number, version, and that page size is valid.
    pub fn validate(&self) -> bool {
        self.magic == MAGIC
            && self.version <= VERSION
            && PageSizeConfig::is_valid(self.page_size)
    }

    /// Validates the meta page against an expected page size.
    ///
    /// Used when opening an existing database to ensure the stored
    /// page size matches the configured page size.
    pub fn validate_with_page_size(&self, expected_page_size: u32) -> bool {
        self.validate() && self.page_size == expected_page_size
    }

    /// Serializes the meta page to bytes.
    ///
    /// Note: The output size is always 4KB (default PAGE_SIZE) regardless
    /// of the stored page_size, as meta pages use a fixed layout.
    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];

        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.txid.to_le_bytes());
        buf[24..32].copy_from_slice(&self.root.to_le_bytes());
        buf[32..40].copy_from_slice(&self.freelist.to_le_bytes());
        buf[40..48].copy_from_slice(&self.page_count.to_le_bytes());
        // Checkpoint fields (bytes 64-88)
        buf[64..72].copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf[72..80].copy_from_slice(&self.checkpoint_timestamp.to_le_bytes());
        buf[80..88].copy_from_slice(&self.checkpoint_entry_count.to_le_bytes());

        // Calculate checksum over bytes 0-56 and 64-88 (excluding checksum field at 56-64).
        let checksum = Self::compute_checksum_extended(&buf);
        buf[56..64].copy_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Deserializes a meta page from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        let version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
        let page_size = u32::from_le_bytes(buf[8..12].try_into().ok()?);
        let txid = u64::from_le_bytes(buf[16..24].try_into().ok()?);
        let root = u64::from_le_bytes(buf[24..32].try_into().ok()?);
        let freelist = u64::from_le_bytes(buf[32..40].try_into().ok()?);
        let page_count = u64::from_le_bytes(buf[40..48].try_into().ok()?);
        let checksum = u64::from_le_bytes(buf[56..64].try_into().ok()?);

        // Checkpoint fields (may be zero for older databases)
        let checkpoint_lsn = u64::from_le_bytes(buf[64..72].try_into().ok()?);
        let checkpoint_timestamp = u64::from_le_bytes(buf[72..80].try_into().ok()?);
        let checkpoint_entry_count = u64::from_le_bytes(buf[80..88].try_into().ok()?);

        // Verify checksum - try extended first, fall back to legacy
        let expected_checksum_ext = Self::compute_checksum_extended(buf);
        let expected_checksum_legacy = Self::compute_checksum(&buf[0..56]);

        if checksum != expected_checksum_ext && checksum != expected_checksum_legacy {
            return None;
        }

        Some(Self {
            magic,
            version,
            page_size,
            txid,
            root,
            freelist,
            page_count,
            checksum,
            checkpoint_lsn,
            checkpoint_timestamp,
            checkpoint_entry_count,
        })
    }

    /// Computes checksum over extended meta fields.
    fn compute_checksum_extended(buf: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0100_0000_01b3;

        let mut hash = FNV_OFFSET;
        // Hash bytes 0-56 (before checksum)
        for byte in &buf[0..56] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        // Hash bytes 64-88 (checkpoint fields)
        for byte in &buf[64..88] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    /// Simple FNV-1a hash for checksum.
    fn compute_checksum(data: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0100_0000_01b3;

        let mut hash = FNV_OFFSET;
        for byte in data {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
}

impl Default for Meta {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_new_and_validate() {
        let meta = Meta::new();
        assert_eq!(meta.magic, MAGIC);
        assert_eq!(meta.version, VERSION);
        assert_eq!(meta.page_size, PAGE_SIZE as u32);
        assert!(meta.validate());

        let mut bad_meta = meta;
        bad_meta.magic = 0xDEADBEEF;
        assert!(!bad_meta.validate());
    }

    #[test]
    fn test_meta_with_page_size() {
        let meta = Meta::with_page_size(16384);
        assert_eq!(meta.page_size, 16384);
        assert!(meta.validate());

        // Invalid page size should still create meta but fail validation
        let invalid_meta = Meta::with_page_size(1024);
        assert!(!invalid_meta.validate());
    }

    #[test]
    fn test_meta_validate_with_page_size() {
        let meta = Meta::with_page_size(8192);
        assert!(meta.validate_with_page_size(8192));
        assert!(!meta.validate_with_page_size(32768)); // Mismatch
    }

    #[test]
    fn test_meta_round_trip() {
        let mut original = Meta::new();
        original.txid = 12345;
        original.root = 100;
        original.freelist = 50;
        original.page_count = 200;

        let bytes = original.to_bytes();
        let restored = Meta::from_bytes(&bytes).expect("should parse");

        assert_eq!(original.txid, restored.txid);
        assert_eq!(original.root, restored.root);
        assert_eq!(original.freelist, restored.freelist);
        assert_eq!(original.page_count, restored.page_count);
    }

    #[test]
    fn test_meta_corruption_detected() {
        let meta = Meta::new();
        let mut bytes = meta.to_bytes();

        // Corrupt data
        bytes[20] ^= 0xFF;
        assert!(Meta::from_bytes(&bytes).is_none());

        // Corrupt checksum
        let mut bytes2 = meta.to_bytes();
        bytes2[56] ^= 0xFF;
        assert!(Meta::from_bytes(&bytes2).is_none());

        // Too short
        assert!(Meta::from_bytes(&[0u8; Meta::SIZE - 1]).is_none());
    }
}
