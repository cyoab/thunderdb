//! Summary: Overflow page management for large values.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module implements overflow page chains for values exceeding
//! the overflow threshold. Large values are stored in linked chains
//! of overflow pages, keeping leaf nodes compact.
//!
//! # Performance Features
//!
//! - Uses crc32fast with SIMD acceleration for checksums (~10 GB/s)
//! - Optional no_checksum feature for maximum performance
//! - Zero-copy buffer operations where possible
//! - Contiguous page allocation for sequential I/O

use crate::mmap::Mmap;
use crate::page::PageId;

/// Default threshold for storing values in overflow pages.
/// Values larger than this are stored in overflow chains.
/// 
/// Set to 16KB by default - optimized for 32KB HPC page size.
/// Most "large" values will be stored inline, avoiding overflow
/// page chain overhead. Only truly large values use overflow storage.
pub const DEFAULT_OVERFLOW_THRESHOLD: usize = 16 * 1024; // 16KB

/// Overflow page header size in bytes.
pub const OVERFLOW_HEADER_SIZE: usize = 24;

/// Overflow page header structure.
///
/// Layout (24 bytes total):
/// ```text
/// [0]       page_type (u8) - PageType::Overflow = 5
/// [1..8]    reserved for alignment
/// [8..16]   next_page (u64) - next overflow page ID (0 = end of chain)
/// [16..20]  data_len (u32) - length of data in this page
/// [20..24]  checksum (u32) - CRC32 of data for integrity verification
/// ```
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct OverflowHeader {
    /// Page type marker (PageType::Overflow = 5).
    pub page_type: u8,
    /// Reserved for alignment.
    pub _reserved: [u8; 7],
    /// Next overflow page ID (0 = end of chain).
    pub next_page: PageId,
    /// Length of data in this page.
    pub data_len: u32,
    /// CRC32 checksum of data for integrity verification.
    pub checksum: u32,
}

impl OverflowHeader {
    /// Size of the overflow header in bytes.
    pub const SIZE: usize = OVERFLOW_HEADER_SIZE;

    /// Creates a new overflow header.
    pub fn new(next_page: PageId, data_len: u32, checksum: u32) -> Self {
        Self {
            page_type: 5, // PageType::Overflow
            _reserved: [0; 7],
            next_page,
            data_len,
            checksum,
        }
    }

    /// Serializes the header to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0] = self.page_type;
        buf[8..16].copy_from_slice(&self.next_page.to_le_bytes());
        buf[16..20].copy_from_slice(&self.data_len.to_le_bytes());
        buf[20..24].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Writes the header directly to a buffer using unsafe pointer operations.
    /// 
    /// This is faster than `to_bytes()` + copy because it avoids an intermediate buffer.
    /// 
    /// # Safety
    /// 
    /// - `buffer` must have at least `Self::SIZE` bytes available
    /// - `buffer` should be properly aligned (though this function handles misalignment)
    /// 
    /// # Arguments
    /// 
    /// * `buffer` - The buffer to write the header into
    #[inline]
    pub unsafe fn write_to_buffer_unchecked(&self, buffer: &mut [u8]) {
        // SAFETY: All operations below are safe because:
        // - Caller guarantees buffer has at least Self::SIZE bytes
        // - All offsets are within bounds (0, 8, 16, 20 are all < 24 = Self::SIZE)
        // - We use copy_nonoverlapping which is safe for non-overlapping regions
        unsafe {
            // Write page_type at offset 0
            *buffer.get_unchecked_mut(0) = self.page_type;
            
            // Write next_page as little-endian u64 at offset 8
            let next_page_bytes = self.next_page.to_le_bytes();
            std::ptr::copy_nonoverlapping(
                next_page_bytes.as_ptr(),
                buffer.as_mut_ptr().add(8),
                8
            );
            
            // Write data_len as little-endian u32 at offset 16
            let data_len_bytes = self.data_len.to_le_bytes();
            std::ptr::copy_nonoverlapping(
                data_len_bytes.as_ptr(),
                buffer.as_mut_ptr().add(16),
                4
            );
            
            // Write checksum as little-endian u32 at offset 20
            let checksum_bytes = self.checksum.to_le_bytes();
            std::ptr::copy_nonoverlapping(
                checksum_bytes.as_ptr(),
                buffer.as_mut_ptr().add(20),
                4
            );
        }
    }

    /// Deserializes a header from bytes.
    ///
    /// Returns `None` if the buffer is too small or page type is invalid.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let page_type = buf[0];
        if page_type != 5 {
            return None; // Not an overflow page
        }

        Some(Self {
            page_type,
            _reserved: [0; 7],
            next_page: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            data_len: u32::from_le_bytes(buf[16..20].try_into().ok()?),
            checksum: u32::from_le_bytes(buf[20..24].try_into().ok()?),
        })
    }
}

/// Reference to a value stored in overflow pages.
///
/// This is stored in the leaf node entry instead of the actual value data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverflowRef {
    /// First page of the overflow chain.
    pub start_page: PageId,
    /// Total length of the value in bytes.
    pub total_len: u32,
}

impl OverflowRef {
    /// Marker value indicating an overflow reference (0xFFFFFFFF).
    /// This value is used in place of value_len to indicate overflow storage.
    pub const MARKER: u32 = 0xFFFF_FFFF;

    /// Serialized size of an overflow reference (after the marker).
    /// Format: [marker:4][start_page:8][total_len:4] = 16 bytes total
    /// The SIZE here is just the page_id + total_len = 12 bytes (marker written separately)
    pub const SIZE: usize = 12;

    /// Creates a new overflow reference.
    pub fn new(start_page: PageId, total_len: u32) -> Self {
        Self {
            start_page,
            total_len,
        }
    }

    /// Serializes the overflow reference to bytes (without marker).
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.start_page.to_le_bytes());
        buf[8..12].copy_from_slice(&self.total_len.to_le_bytes());
        buf
    }

    /// Deserializes an overflow reference from bytes.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        Some(Self {
            start_page: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            total_len: u32::from_le_bytes(buf[8..12].try_into().ok()?),
        })
    }
}

/// Manages overflow page allocation and retrieval.
///
/// Handles the creation of overflow page chains for large values
/// and provides methods to read values back from overflow chains.
pub struct OverflowManager {
    /// Next available page ID for overflow allocation.
    next_page_id: PageId,
    /// Free overflow pages available for reuse.
    free_pages: Vec<PageId>,
    /// Page size for this database.
    page_size: usize,
    /// Usable data per overflow page (page_size - header).
    overflow_data_size: usize,
}

impl OverflowManager {
    /// Creates a new overflow manager.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The page size for this database.
    /// * `next_page_id` - The next available page ID.
    pub fn new(page_size: usize, next_page_id: PageId) -> Self {
        Self {
            next_page_id,
            free_pages: Vec::new(),
            page_size,
            overflow_data_size: page_size - OVERFLOW_HEADER_SIZE,
        }
    }

    /// Returns the overflow data size for this manager.
    #[inline]
    pub fn overflow_data_size(&self) -> usize {
        self.overflow_data_size
    }

    /// Returns the next page ID that will be allocated.
    #[inline]
    pub fn next_page_id(&self) -> PageId {
        self.next_page_id
    }

    /// Sets the next page ID (used when loading from disk).
    #[inline]
    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.next_page_id = page_id;
    }

    /// Allocates overflow pages for a large value.
    ///
    /// Returns the `OverflowRef` and the pages to write.
    ///
    /// # Arguments
    ///
    /// * `value` - The value data to store in overflow pages.
    ///
    /// # Returns
    ///
    /// A tuple of (OverflowRef, Vec<(PageId, page_data)>).
    pub fn allocate_overflow(&mut self, value: &[u8]) -> (OverflowRef, Vec<(PageId, Vec<u8>)>) {
        if value.is_empty() {
            // Edge case: empty value shouldn't use overflow, but handle gracefully
            return (
                OverflowRef::new(0, 0),
                Vec::new(),
            );
        }

        let mut pages = Vec::new();
        let mut remaining = value;
        let first_page = self.alloc_page();
        let mut current_page_id = first_page;

        while !remaining.is_empty() {
            let chunk_len = remaining.len().min(self.overflow_data_size);
            let chunk = &remaining[..chunk_len];
            remaining = &remaining[chunk_len..];

            // Determine next page (0 if this is the last chunk)
            let next_page = if remaining.is_empty() {
                0
            } else {
                self.alloc_page()
            };

            // Calculate checksum for data integrity
            let checksum = Self::compute_checksum(chunk);

            let header = OverflowHeader::new(next_page, chunk_len as u32, checksum);

            let mut page_data = vec![0u8; self.page_size];
            page_data[..OVERFLOW_HEADER_SIZE].copy_from_slice(&header.to_bytes());
            page_data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk_len]
                .copy_from_slice(chunk);

            pages.push((current_page_id, page_data));
            current_page_id = next_page;
        }

        let overflow_ref = OverflowRef::new(first_page, value.len() as u32);
        (overflow_ref, pages)
    }

    /// Reads a value from direct format storage using mmap.
    ///
    /// Direct format is: [magic:4][len:4][data:N][crc:4]
    ///
    /// # Arguments
    ///
    /// * `overflow_ref` - Reference to the overflow data.
    ///   For direct format, start_page contains the raw byte offset (not page number).
    /// * `mmap` - Memory-mapped file for reading data.
    ///
    /// # Returns
    ///
    /// The reconstructed value, or `None` if reading fails or checksum mismatch.
    #[cfg(unix)]
    pub fn read_direct(&self, overflow_ref: OverflowRef, mmap: &Mmap) -> Option<Vec<u8>> {
        if overflow_ref.start_page == 0 {
            return Some(Vec::new());
        }

        let expected_len = overflow_ref.total_len as usize;
        let total_size = Self::direct_buffer_size(expected_len);

        // For direct format, start_page IS the byte offset (not a page number)
        let byte_offset = overflow_ref.start_page as usize;
        let data = mmap.slice(byte_offset, total_size)?;

        // Parse: [magic:4][len:4][data:N][crc:4]
        if data.len() < 12 {
            return None;
        }

        // Verify magic
        let stored_magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if stored_magic != Self::DIRECT_FORMAT_MAGIC {
            return None; // Not direct format
        }

        let stored_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        if stored_len != expected_len {
            return None; // Length mismatch
        }

        let value = &data[8..8 + stored_len];
        let crc_offset = 8 + stored_len;
        let stored_crc =
            u32::from_le_bytes([data[crc_offset], data[crc_offset + 1], data[crc_offset + 2], data[crc_offset + 3]]);

        // Verify checksum
        if !Self::verify_checksum(value, stored_crc) {
            return None; // Data corruption
        }

        Some(value.to_vec())
    }

    /// Reads a value from overflow pages using mmap.
    ///
    /// This function now handles both the direct format and the legacy page chain format.
    /// Direct format is detected by checking for the magic marker.
    ///
    /// # Arguments
    ///
    /// * `overflow_ref` - Reference to the overflow chain.
    /// * `mmap` - Memory-mapped file for reading pages.
    ///
    /// # Returns
    ///
    /// The reconstructed value, or `None` if reading fails.
    #[cfg(unix)]
    pub fn read_overflow(&self, overflow_ref: OverflowRef, mmap: &Mmap) -> Option<Vec<u8>> {
        if overflow_ref.start_page == 0 {
            return Some(Vec::new());
        }

        // For direct format, start_page stores byte offset
        // For legacy format, start_page stores page number
        // Try to detect which format by reading at both possible locations

        // First try direct format - treat start_page as byte offset
        let byte_offset = overflow_ref.start_page as usize;

        if byte_offset < mmap.len() {
            if let Some(header_data) = mmap.slice(byte_offset, 4) {
                let stored_magic = u32::from_le_bytes([header_data[0], header_data[1], header_data[2], header_data[3]]);
                if stored_magic == Self::DIRECT_FORMAT_MAGIC {
                    // Direct format detected - use fast read
                    if let Some(result) = self.read_direct(overflow_ref, mmap) {
                        return Some(result);
                    }
                }
            }
        }

        // Fall back to legacy page chain format (start_page is page number)
        self.read_overflow_legacy(overflow_ref, mmap)
    }

    /// Reads a value from overflow pages using legacy page chain format.
    #[cfg(unix)]
    fn read_overflow_legacy(&self, overflow_ref: OverflowRef, mmap: &Mmap) -> Option<Vec<u8>> {
        let mut result = Vec::with_capacity(overflow_ref.total_len as usize);
        let mut current_page = overflow_ref.start_page;
        let mut pages_read = 0;
        let mut stored_checksum: Option<u32> = None;

        // Safety limit to prevent infinite loops on corrupted chains
        const MAX_CHAIN_LENGTH: usize = 1_000_000;

        while current_page != 0 && pages_read < MAX_CHAIN_LENGTH {
            let page_data = mmap.page_with_size(current_page, self.page_size)?;
            let header = OverflowHeader::from_bytes(page_data)?;

            // Validate data length
            let data_end = OVERFLOW_HEADER_SIZE + header.data_len as usize;
            if data_end > page_data.len() {
                return None; // Corrupted
            }

            let chunk = &page_data[OVERFLOW_HEADER_SIZE..data_end];

            // Store checksum from first page (the only one with the value checksum)
            if pages_read == 0 {
                stored_checksum = Some(header.checksum);
            }

            result.extend_from_slice(chunk);
            current_page = header.next_page;
            pages_read += 1;
        }

        // Verify we got the expected amount of data
        if result.len() != overflow_ref.total_len as usize {
            return None;
        }

        // Verify checksum of entire value (stored in first page header)
        if let Some(checksum) = stored_checksum {
            if !Self::verify_checksum(&result, checksum) {
                return None; // Data corruption detected
            }
        }

        Some(result)
    }

    /// Reads a value from overflow pages by reading from file directly.
    ///
    /// Used when mmap is not available or for non-Unix platforms.
    /// Handles both direct format and legacy page chain format.
    #[allow(dead_code)]
    pub fn read_overflow_from_file(
        &self,
        overflow_ref: OverflowRef,
        file: &mut std::fs::File,
    ) -> Option<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        if overflow_ref.start_page == 0 {
            return Some(Vec::new());
        }

        // For direct format, start_page stores byte offset directly
        let byte_offset = overflow_ref.start_page;
        file.seek(SeekFrom::Start(byte_offset)).ok()?;

        // Try to detect format by reading first 4 bytes (magic)
        let mut magic_buf = [0u8; 4];
        file.read_exact(&mut magic_buf).ok()?;
        let magic = u32::from_le_bytes(magic_buf);

        if magic == Self::DIRECT_FORMAT_MAGIC {
            // Direct format: [magic:4][len:4][data:N][crc:4]
            self.read_direct_from_file(overflow_ref, file, byte_offset)
        } else {
            // Legacy format: start_page is page number, reinterpret and seek
            let legacy_offset = overflow_ref.start_page * self.page_size as u64;
            file.seek(SeekFrom::Start(legacy_offset)).ok()?;
            self.read_overflow_from_file_legacy(overflow_ref, file)
        }
    }

    /// Reads a value from direct format by reading from file directly.
    fn read_direct_from_file(
        &self,
        overflow_ref: OverflowRef,
        file: &mut std::fs::File,
        byte_offset: u64,
    ) -> Option<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let expected_len = overflow_ref.total_len as usize;

        // Seek to length field (after magic)
        file.seek(SeekFrom::Start(byte_offset + 4)).ok()?;

        // Read length
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf).ok()?;
        let stored_len = u32::from_le_bytes(len_buf) as usize;

        if stored_len != expected_len {
            return None; // Length mismatch
        }

        // Read value data
        let mut value = vec![0u8; stored_len];
        file.read_exact(&mut value).ok()?;

        // Read CRC
        let mut crc_buf = [0u8; 4];
        file.read_exact(&mut crc_buf).ok()?;
        let stored_crc = u32::from_le_bytes(crc_buf);

        // Verify checksum
        if !Self::verify_checksum(&value, stored_crc) {
            return None; // Data corruption
        }

        Some(value)
    }

    /// Reads a value from legacy page chain format by reading from file directly.
    fn read_overflow_from_file_legacy(
        &self,
        overflow_ref: OverflowRef,
        file: &mut std::fs::File,
    ) -> Option<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut result = Vec::with_capacity(overflow_ref.total_len as usize);
        let mut current_page = overflow_ref.start_page;
        let mut pages_read = 0;
        let mut stored_checksum: Option<u32> = None;

        const MAX_CHAIN_LENGTH: usize = 1_000_000;

        while current_page != 0 && pages_read < MAX_CHAIN_LENGTH {
            let offset = current_page * self.page_size as u64;
            file.seek(SeekFrom::Start(offset)).ok()?;

            let mut page_data = vec![0u8; self.page_size];
            file.read_exact(&mut page_data).ok()?;

            let header = OverflowHeader::from_bytes(&page_data)?;

            let data_end = OVERFLOW_HEADER_SIZE + header.data_len as usize;
            if data_end > page_data.len() {
                return None;
            }

            let chunk = &page_data[OVERFLOW_HEADER_SIZE..data_end];

            // Store checksum from first page
            if pages_read == 0 {
                stored_checksum = Some(header.checksum);
            }

            result.extend_from_slice(chunk);
            current_page = header.next_page;
            pages_read += 1;
        }

        if result.len() != overflow_ref.total_len as usize {
            return None;
        }

        // Verify checksum of entire value
        if let Some(checksum) = stored_checksum {
            if !Self::verify_checksum(&result, checksum) {
                return None;
            }
        }

        Some(result)
    }

    /// Frees an overflow chain for reuse.
    ///
    /// Traverses the chain and adds all pages to the free list.
    #[cfg(unix)]
    pub fn free_overflow(&mut self, overflow_ref: OverflowRef, mmap: &Mmap) {
        let mut current_page = overflow_ref.start_page;
        let mut pages_freed = 0;

        const MAX_CHAIN_LENGTH: usize = 1_000_000;

        while current_page != 0 && pages_freed < MAX_CHAIN_LENGTH {
            if let Some(page_data) = mmap.page_with_size(current_page, self.page_size)
                && let Some(header) = OverflowHeader::from_bytes(page_data)
            {
                self.free_pages.push(current_page);
                current_page = header.next_page;
                pages_freed += 1;
                continue;
            }
            break;
        }
    }

    /// Allocates a page ID, either from the free list or by incrementing.
    fn alloc_page(&mut self) -> PageId {
        self.free_pages.pop().unwrap_or_else(|| {
            let id = self.next_page_id;
            self.next_page_id += 1;
            id
        })
    }

    /// Allocates contiguous pages for a large value.
    ///
    /// Always allocates at the end to ensure contiguity (ignores free list).
    ///
    /// # Arguments
    ///
    /// * `count` - Number of contiguous pages to allocate.
    ///
    /// # Returns
    ///
    /// The starting page ID of the contiguous block.
    fn alloc_contiguous_pages(&mut self, count: usize) -> PageId {
        let start = self.next_page_id;
        self.next_page_id += count as PageId;
        start
    }

    /// Allocates overflow pages for a large value as a single contiguous buffer.
    ///
    /// This is more efficient than `allocate_overflow` because:
    /// - Pages are always contiguous (enables single write syscall)
    /// - Single buffer allocation reduces memory overhead
    /// - Better cache locality during data preparation
    ///
    /// # Arguments
    ///
    /// * `value` - The value data to store in overflow pages.
    ///
    /// # Returns
    ///
    /// A tuple of (OverflowRef, contiguous buffer containing all pages).
    pub fn allocate_overflow_contiguous(&mut self, value: &[u8]) -> (OverflowRef, Vec<u8>) {
        if value.is_empty() {
            return (OverflowRef::new(0, 0), Vec::new());
        }

        let pages_needed = value.len().div_ceil(self.overflow_data_size);
        let start_page = self.alloc_contiguous_pages(pages_needed);

        // Allocate single buffer for ALL overflow pages
        let total_size = pages_needed * self.page_size;
        let mut buffer = vec![0u8; total_size];

        // Compute single checksum for entire value (stored in first page)
        #[cfg(not(feature = "no_checksum"))]
        let value_checksum = crc32fast::hash(value);
        #[cfg(feature = "no_checksum")]
        let value_checksum = 0u32;

        let mut remaining = value;
        for page_idx in 0..pages_needed {
            let chunk_len = remaining.len().min(self.overflow_data_size);
            let chunk = &remaining[..chunk_len];
            remaining = &remaining[chunk_len..];

            let next_page = if remaining.is_empty() {
                0
            } else {
                start_page + page_idx as u64 + 1
            };

            // Store value checksum only in first page
            let checksum = if page_idx == 0 { value_checksum } else { 0 };
            let header = OverflowHeader::new(next_page, chunk_len as u32, checksum);

            let page_offset = page_idx * self.page_size;
            buffer[page_offset..page_offset + OVERFLOW_HEADER_SIZE]
                .copy_from_slice(&header.to_bytes());
            buffer[page_offset + OVERFLOW_HEADER_SIZE..page_offset + OVERFLOW_HEADER_SIZE + chunk_len]
                .copy_from_slice(chunk);
        }

        let overflow_ref = OverflowRef::new(start_page, value.len() as u32);
        (overflow_ref, buffer)
    }

    /// Writes overflow pages directly into a pre-allocated buffer.
    ///
    /// This is the most efficient method for batch operations because:
    /// - No intermediate allocations
    /// - Direct write into caller-provided buffer
    /// - Single buffer for all values
    ///
    /// # Arguments
    ///
    /// * `value` - The value data to store in overflow pages.
    /// * `buffer` - Pre-allocated buffer to write into (must have sufficient space).
    /// * `offset` - Starting offset in the buffer.
    ///
    /// # Returns
    ///
    /// A tuple of (OverflowRef, number of bytes written).
    pub fn write_overflow_to_buffer(
        &mut self,
        value: &[u8],
        buffer: &mut [u8],
        offset: usize,
    ) -> (OverflowRef, usize) {
        if value.is_empty() {
            return (OverflowRef::new(0, 0), 0);
        }

        let pages_needed = value.len().div_ceil(self.overflow_data_size);
        let start_page = self.alloc_contiguous_pages(pages_needed);
        let total_size = pages_needed * self.page_size;

        // Compute a single checksum for the entire value (stored in first page header).
        // This is much faster than per-page checksums for large values.
        #[cfg(not(feature = "no_checksum"))]
        let value_checksum = crc32fast::hash(value);
        #[cfg(feature = "no_checksum")]
        let value_checksum = 0u32;

        let mut remaining = value;
        for page_idx in 0..pages_needed {
            let chunk_len = remaining.len().min(self.overflow_data_size);
            let chunk = &remaining[..chunk_len];
            remaining = &remaining[chunk_len..];

            let next_page = if remaining.is_empty() {
                0
            } else {
                start_page + page_idx as u64 + 1
            };

            // Store value checksum only in first page, 0 for subsequent pages.
            // This reduces checksum computation from O(pages) to O(1).
            let checksum = if page_idx == 0 { value_checksum } else { 0 };
            let header = OverflowHeader::new(next_page, chunk_len as u32, checksum);

            let page_offset = offset + page_idx * self.page_size;
            
            // SAFETY: We allocated `total_size` bytes which guarantees sufficient space.
            // Each page_offset is page_size-aligned within the buffer.
            unsafe {
                header.write_to_buffer_unchecked(&mut buffer[page_offset..]);
                
                // Use ptr::copy_nonoverlapping for the data copy (faster than slice copy for larger chunks)
                std::ptr::copy_nonoverlapping(
                    chunk.as_ptr(),
                    buffer.as_mut_ptr().add(page_offset + OVERFLOW_HEADER_SIZE),
                    chunk_len
                );
            }
        }

        let overflow_ref = OverflowRef::new(start_page, value.len() as u32);
        (overflow_ref, total_size)
    }

    /// Magic marker for direct format to distinguish from legacy page chain format.
    /// This is 0xDDDDDDDD which is >3GB so will never be a valid value length in practice.
    const DIRECT_FORMAT_MAGIC: u32 = 0xDDDD_DDDD;

    /// Writes a large value directly without page chain overhead.
    ///
    /// This is the fastest method for large values because:
    /// - No per-page headers (only one header for the entire value)
    /// - Single contiguous write
    /// - Minimal overhead (12 bytes: 4 magic, 4 length, 4 CRC at end)
    ///
    /// Format: [magic:4][length:4][data:N][crc32:4]
    ///
    /// # Arguments
    ///
    /// * `value` - The value data to store.
    /// * `buffer` - Pre-allocated buffer to write into.
    /// * `buffer_offset` - Starting offset within the buffer.
    /// * `file_base_offset` - The byte offset in the file where the buffer will be written.
    ///
    /// # Returns
    ///
    /// Tuple of (OverflowRef, bytes_written).
    /// Note: For direct format, start_page stores the raw byte offset (not page number).
    #[inline]
    pub fn write_direct_to_buffer(
        &mut self,
        value: &[u8],
        buffer: &mut [u8],
        buffer_offset: usize,
        file_base_offset: u64,
    ) -> (OverflowRef, usize) {
        if value.is_empty() {
            return (OverflowRef::new(0, 0), 0);
        }

        let total_size = value.len() + 12; // 4 bytes magic + 4 bytes length + 4 bytes CRC
        
        // For direct format, store the exact byte offset where data is written
        // (This differs from legacy format which stores page numbers)
        let file_offset = file_base_offset + buffer_offset as u64;
        
        // Write magic marker
        buffer[buffer_offset..buffer_offset + 4].copy_from_slice(&Self::DIRECT_FORMAT_MAGIC.to_le_bytes());
        
        // Write length header (at buffer_offset+4)
        let len_bytes = (value.len() as u32).to_le_bytes();
        buffer[buffer_offset + 4..buffer_offset + 8].copy_from_slice(&len_bytes);
        
        // Write value data using fast copy (at buffer_offset+8)
        unsafe {
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                buffer.as_mut_ptr().add(buffer_offset + 8),
                value.len(),
            );
        }
        
        // Write CRC32 trailer
        #[cfg(not(feature = "no_checksum"))]
        let checksum = crc32fast::hash(value);
        #[cfg(feature = "no_checksum")]
        let checksum = 0u32;
        
        let crc_offset = buffer_offset + 8 + value.len();
        buffer[crc_offset..crc_offset + 4].copy_from_slice(&checksum.to_le_bytes());
        
        // Store byte offset in start_page (repurposed for direct format)
        let oref = OverflowRef::new(file_offset, value.len() as u32);
        (oref, total_size)
    }

    /// Calculates the buffer size for direct write (no page overhead).
    #[inline]
    pub fn direct_buffer_size(value_len: usize) -> usize {
        if value_len == 0 {
            return 0;
        }
        value_len + 12 // 4 bytes magic + 4 bytes length + 4 bytes CRC
    }

    /// Calculates the buffer size needed for a value's overflow pages.
    #[inline]
    pub fn overflow_buffer_size(&self, value_len: usize) -> usize {
        if value_len == 0 {
            return 0;
        }
        let pages_needed = value_len.div_ceil(self.overflow_data_size);
        pages_needed * self.page_size
    }

    /// Computes a fast checksum using SIMD-accelerated CRC32.
    ///
    /// Uses the crc32fast crate which automatically detects and uses:
    /// - SSE 4.2 hardware CRC32 instructions on x86_64
    /// - ARM CRC32 instructions on aarch64
    /// - Fallback software implementation otherwise
    ///
    /// Achieves ~10+ GB/s on modern CPUs with hardware support.
    ///
    /// When compiled with `no_checksum` feature, returns 0 for maximum
    /// performance (trusting filesystem for integrity).
    #[inline]
    fn compute_checksum(data: &[u8]) -> u32 {
        #[cfg(feature = "no_checksum")]
        {
            // Skip checksum computation for maximum performance
            // Useful when filesystem already provides integrity (ZFS, btrfs)
            let _ = data;
            0
        }
        
        #[cfg(not(feature = "no_checksum"))]
        {
            // Use SIMD-accelerated CRC32 from crc32fast
            crc32fast::hash(data)
        }
    }

    /// Verifies a checksum against stored value.
    ///
    /// When compiled with `no_checksum` feature, always returns true.
    #[inline]
    fn verify_checksum(data: &[u8], expected: u32) -> bool {
        #[cfg(feature = "no_checksum")]
        {
            let _ = data;
            let _ = expected;
            true
        }
        
        #[cfg(not(feature = "no_checksum"))]
        {
            crc32fast::hash(data) == expected
        }
    }

    /// Returns whether a value should use overflow storage.
    #[inline]
    pub fn should_overflow(value_len: usize, threshold: usize) -> bool {
        value_len > threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overflow_header_roundtrip() {
        let header = OverflowHeader::new(42, 1024, 0xDEADBEEF);
        let bytes = header.to_bytes();
        let recovered = OverflowHeader::from_bytes(&bytes).expect("should parse");

        assert_eq!(recovered.page_type, 5);
        assert_eq!(recovered.next_page, 42);
        assert_eq!(recovered.data_len, 1024);
        assert_eq!(recovered.checksum, 0xDEADBEEF);
    }

    #[test]
    fn test_overflow_ref_roundtrip() {
        let oref = OverflowRef::new(100, 50000);
        let bytes = oref.to_bytes();
        let recovered = OverflowRef::from_bytes(&bytes).expect("should parse");

        assert_eq!(recovered.start_page, 100);
        assert_eq!(recovered.total_len, 50000);
    }

    #[test]
    fn test_overflow_manager_allocation() {
        let mut mgr = OverflowManager::new(32768, 10);

        // Allocate space for a 80KB value (3 pages needed with 32KB page size)
        let value = vec![0xABu8; 80 * 1024];
        let (oref, pages) = mgr.allocate_overflow(&value);

        assert_eq!(oref.total_len, 80 * 1024);
        assert_eq!(oref.start_page, 10);
        assert_eq!(pages.len(), 3); // 80KB needs 3 pages with 32KB page size

        // Verify chain linkage
        let header0 = OverflowHeader::from_bytes(&pages[0].1).unwrap();
        let header1 = OverflowHeader::from_bytes(&pages[1].1).unwrap();
        let header2 = OverflowHeader::from_bytes(&pages[2].1).unwrap();

        assert_eq!(header0.next_page, pages[1].0);
        assert_eq!(header1.next_page, pages[2].0);
        assert_eq!(header2.next_page, 0); // End of chain
    }

    #[test]
    fn test_checksum_computation() {
        let data = b"Hello, World!";
        let checksum1 = OverflowManager::compute_checksum(data);
        let checksum2 = OverflowManager::compute_checksum(data);

        assert_eq!(checksum1, checksum2);

        // Different data should have different checksum
        let other_data = b"Hello, World?";
        let checksum3 = OverflowManager::compute_checksum(other_data);
        assert_ne!(checksum1, checksum3);
    }

    #[test]
    fn test_should_overflow() {
        assert!(!OverflowManager::should_overflow(1024, 2048));
        assert!(!OverflowManager::should_overflow(2048, 2048));
        assert!(OverflowManager::should_overflow(2049, 2048));
        assert!(OverflowManager::should_overflow(10000, 2048));
    }

    #[test]
    fn test_overflow_contiguous_allocation() {
        let mut mgr = OverflowManager::new(32768, 10);

        // Allocate space for a 80KB value (3 pages needed with 32KB page size)
        let value = vec![0xABu8; 80 * 1024];
        let (oref, buffer) = mgr.allocate_overflow_contiguous(&value);

        assert_eq!(oref.total_len, 80 * 1024);
        assert_eq!(oref.start_page, 10);
        // Buffer should contain all 3 pages contiguously
        assert_eq!(buffer.len(), 3 * 32768);

        // Verify chain linkage in contiguous buffer
        let header0 = OverflowHeader::from_bytes(&buffer[0..]).unwrap();
        let header1 = OverflowHeader::from_bytes(&buffer[32768..]).unwrap();
        let header2 = OverflowHeader::from_bytes(&buffer[65536..]).unwrap();

        assert_eq!(header0.next_page, 11); // start_page + 1
        assert_eq!(header1.next_page, 12); // start_page + 2
        assert_eq!(header2.next_page, 0); // End of chain

        // Verify data integrity
        let data_size = 32768 - OVERFLOW_HEADER_SIZE;
        assert_eq!(header0.data_len as usize, data_size);
        assert_eq!(header1.data_len as usize, data_size);
        // Last page has remaining data: 80*1024 - 2*data_size
        let expected_last = 80 * 1024 - 2 * data_size;
        assert_eq!(header2.data_len as usize, expected_last);

        // Next allocation should start after contiguous pages
        assert_eq!(mgr.next_page_id(), 13);
    }

    #[test]
    fn test_overflow_contiguous_empty_value() {
        let mut mgr = OverflowManager::new(32768, 10);

        let (oref, buffer) = mgr.allocate_overflow_contiguous(&[]);

        assert_eq!(oref.start_page, 0);
        assert_eq!(oref.total_len, 0);
        assert!(buffer.is_empty());
        // Next page should not advance
        assert_eq!(mgr.next_page_id(), 10);
    }

    #[test]
    fn test_overflow_contiguous_single_page() {
        let mut mgr = OverflowManager::new(32768, 10);
        let data_size = 32768 - OVERFLOW_HEADER_SIZE;

        // Value that fits exactly in one page
        let value = vec![0xCDu8; data_size];
        let (oref, buffer) = mgr.allocate_overflow_contiguous(&value);

        assert_eq!(oref.start_page, 10);
        assert_eq!(oref.total_len as usize, data_size);
        assert_eq!(buffer.len(), 32768);

        let header = OverflowHeader::from_bytes(&buffer).unwrap();
        assert_eq!(header.next_page, 0); // Single page, no chain
        assert_eq!(header.data_len as usize, data_size);

        // Verify data content
        assert_eq!(
            &buffer[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + data_size],
            &value[..]
        );
    }
}
