//! Summary: Meta page handling for crash recovery and database state.
//! Copyright (c) YOAB. All rights reserved.

use crate::page::{PageId, MAGIC, PAGE_SIZE, VERSION};

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
}

impl Meta {
    /// Size of the meta structure in bytes.
    pub const SIZE: usize = 64;

    /// Creates a new meta page with default values for a fresh database.
    pub fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            page_size: PAGE_SIZE as u32,
            txid: 0,
            root: 0,
            freelist: 0,
            page_count: 2, // Two meta pages initially
            checksum: 0,
        }
    }

    /// Validates the meta page.
    pub fn validate(&self) -> bool {
        self.magic == MAGIC && self.version == VERSION && self.page_size == PAGE_SIZE as u32
    }

    /// Serializes the meta page to bytes.
    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut buf = [0u8; PAGE_SIZE];

        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        buf[16..24].copy_from_slice(&self.txid.to_le_bytes());
        buf[24..32].copy_from_slice(&self.root.to_le_bytes());
        buf[32..40].copy_from_slice(&self.freelist.to_le_bytes());
        buf[40..48].copy_from_slice(&self.page_count.to_le_bytes());

        // Calculate checksum over the first 56 bytes (excluding checksum field).
        let checksum = Self::compute_checksum(&buf[0..56]);
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

        // Verify checksum.
        let expected_checksum = Self::compute_checksum(&buf[0..56]);
        if checksum != expected_checksum {
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
        })
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
