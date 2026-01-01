//! Summary: Page allocation and free page tracking.
//! Copyright (c) YOAB. All rights reserved.
//!
//! The freelist tracks pages that have been freed and can be reused.
//! This enables efficient space reclamation when data is deleted,
//! preventing unbounded file growth.

use crate::page::PageId;
use std::collections::BTreeSet;

/// A freelist for tracking available (freed) pages.
///
/// Pages added to the freelist can be reallocated for new data,
/// avoiding the need to grow the database file.
///
/// # Implementation
///
/// Uses a `BTreeSet` internally for:
/// - O(log n) insert, remove, and contains operations
/// - Deterministic iteration order (sorted by page ID)
/// - Automatic deduplication
#[derive(Debug, Clone)]
pub struct FreeList {
    /// Set of free page IDs.
    pages: BTreeSet<PageId>,
}

impl FreeList {
    /// Creates a new empty freelist.
    pub fn new() -> Self {
        Self {
            pages: BTreeSet::new(),
        }
    }

    /// Returns true if the freelist is empty.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Returns the number of free pages.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Marks a page as free and available for reuse.
    ///
    /// If the page is already in the freelist, this is a no-op
    /// (duplicates are automatically handled).
    pub fn free(&mut self, page_id: PageId) {
        self.pages.insert(page_id);
    }

    /// Allocates a page from the freelist.
    ///
    /// Returns `Some(page_id)` if a free page is available,
    /// or `None` if the freelist is empty.
    ///
    /// The allocated page is removed from the freelist.
    pub fn allocate(&mut self) -> Option<PageId> {
        // Pop the first (lowest) page ID for deterministic behavior.
        let page_id = self.pages.iter().next().copied();
        if let Some(id) = page_id {
            self.pages.remove(&id);
        }
        page_id
    }

    /// Checks if a page ID is in the freelist.
    pub fn contains(&self, page_id: PageId) -> bool {
        self.pages.contains(&page_id)
    }

    /// Clears all pages from the freelist.
    pub fn clear(&mut self) {
        self.pages.clear();
    }

    /// Returns an iterator over the free page IDs.
    pub fn iter(&self) -> impl Iterator<Item = &PageId> {
        self.pages.iter()
    }

    /// Serializes the freelist to bytes.
    ///
    /// Format:
    /// - 8 bytes: count (u64 little-endian)
    /// - count * 8 bytes: page IDs (u64 little-endian each)
    pub fn to_bytes(&self) -> Vec<u8> {
        let count = self.pages.len() as u64;
        let mut buf = Vec::with_capacity(8 + self.pages.len() * 8);

        buf.extend_from_slice(&count.to_le_bytes());

        for &page_id in &self.pages {
            buf.extend_from_slice(&page_id.to_le_bytes());
        }

        buf
    }

    /// Deserializes a freelist from bytes.
    ///
    /// Returns `None` if the data is corrupted or truncated.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 8 {
            return None;
        }

        let count = u64::from_le_bytes(buf[0..8].try_into().ok()?) as usize;

        // Validate buffer has enough data.
        let expected_len = 8 + count * 8;
        if buf.len() < expected_len {
            return None;
        }

        let mut pages = BTreeSet::new();
        for i in 0..count {
            let offset = 8 + i * 8;
            let page_id = u64::from_le_bytes(buf[offset..offset + 8].try_into().ok()?);
            pages.insert(page_id);
        }

        Some(Self { pages })
    }
}

impl Default for FreeList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_freelist_basic_operations() {
        let mut fl = FreeList::new();
        assert!(fl.is_empty());

        fl.free(10);
        fl.free(20);
        fl.free(10); // Duplicate should be ignored
        assert_eq!(fl.len(), 2);
        assert!(fl.contains(10));
        assert!(fl.contains(20));

        let page = fl.allocate().unwrap();
        assert!(page == 10 || page == 20);
        assert_eq!(fl.len(), 1);

        fl.clear();
        assert!(fl.is_empty());
    }

    #[test]
    fn test_freelist_serialization() {
        let mut fl = FreeList::new();
        fl.free(5);
        fl.free(10);
        fl.free(100);

        let bytes = fl.to_bytes();
        let recovered = FreeList::from_bytes(&bytes).unwrap();

        assert_eq!(recovered.len(), 3);
        assert!(recovered.contains(5));
        assert!(recovered.contains(10));
        assert!(recovered.contains(100));

        // Invalid data should return None
        assert!(FreeList::from_bytes(&[0u8; 4]).is_none());
    }

    #[test]
    fn test_freelist_deterministic_allocation() {
        let mut fl = FreeList::new();
        fl.free(30);
        fl.free(10);
        fl.free(20);

        // BTreeSet allocates in sorted order (lowest first)
        assert_eq!(fl.allocate(), Some(10));
        assert_eq!(fl.allocate(), Some(20));
        assert_eq!(fl.allocate(), Some(30));
        assert_eq!(fl.allocate(), None);
    }
}
