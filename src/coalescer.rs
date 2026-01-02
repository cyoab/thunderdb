//! Summary: Write coalescing for efficient batched I/O.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides a write coalescing buffer that batches dirty pages
//! and sequential data before flushing to disk. This reduces I/O operations
//! and improves write throughput, especially for workloads with many small writes.

use crate::page::PageId;
use std::collections::BTreeMap;

/// Default maximum coalescer buffer size (16MB).
pub const DEFAULT_MAX_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// Batches writes for efficient I/O.
///
/// Collects dirty pages and data in memory, then flushes them in optimal
/// order to minimize I/O operations.
///
/// # Features
///
/// - **Page deduplication**: Multiple writes to the same page keep only the last
/// - **Sorted writes**: Pages are written in page ID order for sequential I/O
/// - **Buffer limits**: Configurable max size with automatic flush trigger
pub struct WriteCoalescer {
    /// Pending page writes, keyed by page ID for deduplication.
    /// Using BTreeMap ensures pages are sorted by ID for sequential I/O.
    pages: BTreeMap<PageId, Vec<u8>>,
    /// Pending sequential data (entries).
    sequential_data: Vec<u8>,
    /// Current buffer size in bytes.
    buffer_size: usize,
    /// Maximum buffer size before forced flush.
    max_buffer_size: usize,
    /// Page size for this database.
    page_size: usize,
}

impl WriteCoalescer {
    /// Creates a new write coalescer.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The page size for this database.
    /// * `max_buffer_size` - Maximum buffer size before flush is triggered.
    pub fn new(page_size: usize, max_buffer_size: usize) -> Self {
        Self {
            pages: BTreeMap::new(),
            sequential_data: Vec::with_capacity(256 * 1024),
            buffer_size: 0,
            max_buffer_size,
            page_size,
        }
    }

    /// Queues a page write.
    ///
    /// If the same page is written multiple times, only the last write is kept
    /// (write coalescing). This is safe because only the final state matters.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page ID to write.
    /// * `data` - The page data (must be exactly `page_size` bytes).
    ///
    /// # Panics
    ///
    /// Debug builds will panic if `data.len() != page_size`.
    pub fn queue_page(&mut self, page_id: PageId, data: Vec<u8>) {
        debug_assert_eq!(
            data.len(),
            self.page_size,
            "page data must be exactly page_size bytes"
        );

        // Remove old entry's size if overwriting (coalescing)
        if let Some(old) = self.pages.get(&page_id) {
            self.buffer_size -= old.len();
        }

        self.buffer_size += data.len();
        self.pages.insert(page_id, data);
    }

    /// Queues sequential entry data.
    ///
    /// Sequential data is appended in order and written to the data section
    /// of the database file.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to append.
    pub fn queue_sequential(&mut self, data: &[u8]) {
        self.sequential_data.extend_from_slice(data);
        self.buffer_size += data.len();
    }

    /// Returns true if buffer exceeds max size and should be flushed.
    #[inline]
    pub fn should_flush(&self) -> bool {
        self.buffer_size >= self.max_buffer_size
    }

    /// Returns current buffer size in bytes.
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Returns true if there's nothing to write.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty() && self.sequential_data.is_empty()
    }

    /// Returns the page size for this coalescer.
    #[inline]
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Clears the buffer without writing.
    pub fn clear(&mut self) {
        self.pages.clear();
        self.sequential_data.clear();
        self.buffer_size = 0;
    }

    /// Consumes the coalescer and returns data to write.
    ///
    /// Returns a `WriteBatch` containing sequential data and pages
    /// sorted by page ID for optimal sequential I/O.
    pub fn into_write_batch(self) -> WriteBatch {
        let pages: Vec<_> = self.pages.into_iter().collect();
        WriteBatch {
            sequential_data: self.sequential_data,
            pages,
        }
    }

    /// Takes the current batch and resets the coalescer.
    ///
    /// More efficient than `into_write_batch` when you need to continue using
    /// the coalescer after flushing.
    pub fn take_batch(&mut self) -> WriteBatch {
        let pages: Vec<_> = std::mem::take(&mut self.pages).into_iter().collect();
        let sequential_data = std::mem::take(&mut self.sequential_data);
        self.buffer_size = 0;

        WriteBatch {
            sequential_data,
            pages,
        }
    }
}

impl std::fmt::Debug for WriteCoalescer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteCoalescer")
            .field("page_count", &self.pages.len())
            .field("sequential_len", &self.sequential_data.len())
            .field("buffer_size", &self.buffer_size)
            .field("max_buffer_size", &self.max_buffer_size)
            .field("page_size", &self.page_size)
            .finish()
    }
}

/// A batch of data ready to be written.
///
/// Contains sequential data and pages sorted by page ID.
#[derive(Debug)]
pub struct WriteBatch {
    /// Sequential entry data to append.
    pub sequential_data: Vec<u8>,
    /// Pages to write, sorted by page ID.
    pub pages: Vec<(PageId, Vec<u8>)>,
}

impl WriteBatch {
    /// Creates an empty write batch.
    pub fn empty() -> Self {
        Self {
            sequential_data: Vec::new(),
            pages: Vec::new(),
        }
    }

    /// Returns true if there's nothing to write.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sequential_data.is_empty() && self.pages.is_empty()
    }

    /// Total bytes to write.
    pub fn total_size(&self) -> usize {
        self.sequential_data.len() + self.pages.iter().map(|(_, d)| d.len()).sum::<usize>()
    }

    /// Splits batch into contiguous page ranges for potentially vectored I/O.
    ///
    /// Groups consecutive pages together to maximize sequential write
    /// performance on block devices.
    pub fn into_contiguous_ranges(self, page_size: usize) -> (Vec<u8>, Vec<ContiguousRange>) {
        let mut ranges = Vec::new();
        let mut current_range: Option<ContiguousRange> = None;

        for (page_id, data) in self.pages {
            match &mut current_range {
                Some(range) if range.can_extend(page_id, page_size) => {
                    range.extend(data);
                }
                _ => {
                    if let Some(range) = current_range.take() {
                        ranges.push(range);
                    }
                    current_range = Some(ContiguousRange::new(page_id, data));
                }
            }
        }

        if let Some(range) = current_range {
            ranges.push(range);
        }

        (self.sequential_data, ranges)
    }
}

/// A contiguous range of pages for efficient I/O.
///
/// Represents multiple consecutive pages that can be written in a single
/// I/O operation for better performance.
#[derive(Debug)]
pub struct ContiguousRange {
    /// First page ID in this range.
    pub start_page: PageId,
    /// Combined data of all pages in this range.
    pub data: Vec<u8>,
    /// Number of pages in this range.
    pub page_count: usize,
}

impl ContiguousRange {
    /// Creates a new contiguous range starting with a single page.
    fn new(page_id: PageId, data: Vec<u8>) -> Self {
        Self {
            start_page: page_id,
            data,
            page_count: 1,
        }
    }

    /// Checks if the given page can be appended to this range.
    fn can_extend(&self, page_id: PageId, page_size: usize) -> bool {
        let expected_next = self.start_page + (self.data.len() / page_size) as u64;
        page_id == expected_next
    }

    /// Extends the range with another page's data.
    fn extend(&mut self, data: Vec<u8>) {
        self.data.extend(data);
        self.page_count += 1;
    }

    /// Returns the byte offset for this range given a page size.
    pub fn offset(&self, page_size: usize) -> u64 {
        self.start_page * page_size as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coalescer_basic() {
        let mut coalescer = WriteCoalescer::new(32768, 1024 * 1024);

        assert!(coalescer.is_empty());
        assert_eq!(coalescer.buffer_size(), 0);

        // Add a page
        coalescer.queue_page(10, vec![0xAAu8; 32768]);
        assert!(!coalescer.is_empty());
        assert_eq!(coalescer.buffer_size(), 32768);

        // Add sequential data
        coalescer.queue_sequential(&[0xBB; 1024]);
        assert_eq!(coalescer.buffer_size(), 32768 + 1024);
    }

    #[test]
    fn test_coalescer_page_deduplication() {
        let mut coalescer = WriteCoalescer::new(32768, 1024 * 1024);

        // Write same page multiple times
        coalescer.queue_page(5, vec![0x11u8; 32768]);
        coalescer.queue_page(5, vec![0x22u8; 32768]);
        coalescer.queue_page(5, vec![0x33u8; 32768]);

        // Should only have one page worth of buffer
        assert_eq!(coalescer.buffer_size(), 32768);

        let batch = coalescer.into_write_batch();
        assert_eq!(batch.pages.len(), 1);
        assert_eq!(batch.pages[0].0, 5);
        assert_eq!(batch.pages[0].1[0], 0x33); // Last write wins
    }

    #[test]
    fn test_coalescer_sorted_pages() {
        let mut coalescer = WriteCoalescer::new(32768, 1024 * 1024);

        // Add pages in random order
        coalescer.queue_page(30, vec![0x30u8; 32768]);
        coalescer.queue_page(10, vec![0x10u8; 32768]);
        coalescer.queue_page(20, vec![0x20u8; 32768]);

        let batch = coalescer.into_write_batch();

        // Pages should be sorted by ID
        assert_eq!(batch.pages.len(), 3);
        assert_eq!(batch.pages[0].0, 10);
        assert_eq!(batch.pages[1].0, 20);
        assert_eq!(batch.pages[2].0, 30);
    }

    #[test]
    fn test_coalescer_should_flush() {
        let mut coalescer = WriteCoalescer::new(32768, 80 * 1024);

        assert!(!coalescer.should_flush());

        // Add data below threshold
        coalescer.queue_page(1, vec![0u8; 32768]);
        coalescer.queue_page(2, vec![0u8; 32768]);
        assert!(!coalescer.should_flush());

        // Add data to exceed threshold
        coalescer.queue_page(3, vec![0u8; 32768]);
        assert!(coalescer.should_flush()); // 96KB > 80KB threshold
    }

    #[test]
    fn test_contiguous_ranges() {
        let mut coalescer = WriteCoalescer::new(32768, 1024 * 1024);

        // Add contiguous pages 10, 11, 12
        coalescer.queue_page(10, vec![0x10u8; 32768]);
        coalescer.queue_page(11, vec![0x11u8; 32768]);
        coalescer.queue_page(12, vec![0x12u8; 32768]);

        // Add non-contiguous page 20
        coalescer.queue_page(20, vec![0x20u8; 32768]);

        // Add contiguous pages 21, 22
        coalescer.queue_page(21, vec![0x21u8; 32768]);
        coalescer.queue_page(22, vec![0x22u8; 32768]);

        let batch = coalescer.into_write_batch();
        let (_, ranges) = batch.into_contiguous_ranges(32768);

        // Should have 2 ranges: [10-12] and [20-22]
        assert_eq!(ranges.len(), 2);

        assert_eq!(ranges[0].start_page, 10);
        assert_eq!(ranges[0].page_count, 3);
        assert_eq!(ranges[0].data.len(), 3 * 32768);

        assert_eq!(ranges[1].start_page, 20);
        assert_eq!(ranges[1].page_count, 3);
        assert_eq!(ranges[1].data.len(), 3 * 32768);
    }

    #[test]
    fn test_write_batch_total_size() {
        let mut coalescer = WriteCoalescer::new(32768, 1024 * 1024);

        coalescer.queue_page(1, vec![0u8; 32768]);
        coalescer.queue_page(2, vec![0u8; 32768]);
        coalescer.queue_sequential(&[0u8; 1024]);

        let batch = coalescer.into_write_batch();
        assert_eq!(batch.total_size(), 32768 + 32768 + 1024);
    }

    #[test]
    fn test_take_batch() {
        let mut coalescer = WriteCoalescer::new(32768, 1024 * 1024);

        coalescer.queue_page(1, vec![0xAAu8; 32768]);
        coalescer.queue_sequential(&[0xBB; 512]);

        let batch = coalescer.take_batch();
        assert_eq!(batch.pages.len(), 1);
        assert_eq!(batch.sequential_data.len(), 512);

        // Coalescer should be reset
        assert!(coalescer.is_empty());
        assert_eq!(coalescer.buffer_size(), 0);
    }
}
