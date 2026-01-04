//! Summary: Memory-mapped file I/O for efficient page access.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides optimized memory mapping with support for:
//! - Access pattern hints (`madvise` SEQUENTIAL/RANDOM)
//! - Prefetching (`MADV_WILLNEED`)
//! - Memory reclamation hints (`MADV_DONTNEED`)
//! - Pre-faulting via `MAP_POPULATE`

use std::fmt;
use std::fs::File;
use std::io;
use std::ptr::NonNull;

use crate::page::PAGE_SIZE;

/// Access pattern hint for memory-mapped regions.
///
/// These hints inform the kernel's readahead and caching strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessPattern {
    /// Normal access pattern (kernel default behavior).
    #[default]
    Normal,
    /// Sequential access - kernel will aggressively readahead.
    /// Use for full scans, sequential iteration, compaction.
    Sequential,
    /// Random access - kernel will minimize readahead.
    /// Use for point lookups, B-tree traversal.
    Random,
    /// Pages will be accessed only once then not needed.
    /// Kernel may deprioritize caching.
    WillNeedOnce,
}

/// Options for creating memory mappings.
///
/// Allows configuring access patterns, pre-faulting, and other
/// kernel hints for optimal performance.
///
/// # Example
///
/// ```ignore
/// use thunderdb::mmap::{MmapOptions, AccessPattern};
///
/// let options = MmapOptions::new()
///     .with_access_pattern(AccessPattern::Sequential)
///     .with_populate(true);
/// ```
#[derive(Debug, Clone, Default)]
pub struct MmapOptions {
    /// Access pattern hint for madvise.
    access_pattern: AccessPattern,
    /// Whether to pre-fault all pages (MAP_POPULATE).
    populate: bool,
}

impl MmapOptions {
    /// Creates default mmap options.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the expected access pattern.
    ///
    /// This informs the kernel's readahead and caching behavior.
    #[inline]
    pub fn with_access_pattern(mut self, pattern: AccessPattern) -> Self {
        self.access_pattern = pattern;
        self
    }

    /// Enables pre-faulting of pages via MAP_POPULATE.
    ///
    /// When enabled, the kernel will read all pages into memory
    /// during the mmap call, eliminating later page faults.
    /// Useful for small databases that fit in memory.
    #[inline]
    pub fn with_populate(mut self, populate: bool) -> Self {
        self.populate = populate;
        self
    }
}

/// A memory-mapped region of a database file.
///
/// Provides read-only access to pages via memory mapping, avoiding
/// explicit read syscalls for page access.
///
/// # Optimizations
///
/// The mapping supports several kernel hints for performance:
/// - **Access patterns**: Sequential/Random hints via `madvise`
/// - **Prefetching**: `prefetch()` method for warming pages
/// - **Memory release**: `dontneed()` to hint page eviction
/// - **Pre-faulting**: `MAP_POPULATE` to eliminate page faults
///
/// # Safety
///
/// The mapped region must remain valid for the lifetime of this struct.
/// The underlying file must not be truncated while mapped.
pub struct Mmap {
    /// Pointer to the mapped memory region.
    ptr: NonNull<u8>,
    /// Length of the mapped region in bytes.
    len: usize,
}

impl fmt::Debug for Mmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mmap")
            .field("ptr", &self.ptr)
            .field("len", &self.len)
            .finish()
    }
}

// SAFETY: The memory mapping is read-only and the pointer is valid
// for the lifetime of the Mmap struct. Multiple threads can safely
// read from the same memory region.
unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl Mmap {
    /// Creates a new memory mapping for the given file.
    ///
    /// # Arguments
    ///
    /// * `file` - The file to map (must be open for reading).
    /// * `len` - The length in bytes to map (must be > 0).
    ///
    /// # Errors
    ///
    /// Returns an error if the mapping fails.
    ///
    /// # Safety
    ///
    /// The file must not be truncated or modified in ways that invalidate
    /// the mapped pages while this mapping exists.
    pub fn new(file: &File, len: usize) -> io::Result<Self> {
        Self::with_options(file, len, MmapOptions::default())
    }

    /// Creates a new memory mapping with custom options.
    ///
    /// # Arguments
    ///
    /// * `file` - The file to map (must be open for reading).
    /// * `len` - The length in bytes to map (must be > 0).
    /// * `options` - Configuration for access patterns and pre-faulting.
    ///
    /// # Errors
    ///
    /// Returns an error if the mapping fails.
    pub fn with_options(file: &File, len: usize, options: MmapOptions) -> io::Result<Self> {
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot map zero-length region",
            ));
        }

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;

            // Build mmap flags
            let mut flags = libc::MAP_SHARED;
            if options.populate {
                flags |= libc::MAP_POPULATE;
            }

            // SAFETY: We are calling mmap with valid parameters.
            // - PROT_READ: read-only access
            // - MAP_SHARED: changes to file are visible (though we don't write)
            // - fd from file handle is valid
            // - offset 0, len is the size we want
            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    flags,
                    file.as_raw_fd(),
                    0,
                )
            };

            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            // SAFETY: mmap succeeded, so ptr is valid and non-null.
            let ptr = unsafe { NonNull::new_unchecked(ptr as *mut u8) };

            let mmap = Self { ptr, len };

            // Apply access pattern hint via madvise
            mmap.apply_access_hint(options.access_pattern);

            Ok(mmap)
        }

        #[cfg(not(unix))]
        {
            let _ = file;
            let _ = options;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "memory mapping not supported on this platform",
            ))
        }
    }

    /// Applies an access pattern hint to the entire mapping.
    ///
    /// # Arguments
    ///
    /// * `pattern` - The expected access pattern.
    #[cfg(unix)]
    fn apply_access_hint(&self, pattern: AccessPattern) {
        let advice = match pattern {
            AccessPattern::Normal => libc::MADV_NORMAL,
            AccessPattern::Sequential => libc::MADV_SEQUENTIAL,
            AccessPattern::Random => libc::MADV_RANDOM,
            AccessPattern::WillNeedOnce => libc::MADV_SEQUENTIAL, // Best approximation
        };

        // SAFETY: ptr and len are valid from successful mmap.
        // madvise is advisory and failure is non-fatal.
        unsafe {
            libc::madvise(self.ptr.as_ptr() as *mut libc::c_void, self.len, advice);
        }
    }

    /// Hints to the kernel that specified pages will be needed soon.
    ///
    /// This triggers asynchronous readahead for the specified range,
    /// reducing latency when the pages are actually accessed.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset into the mapped region.
    /// * `len` - Number of bytes to prefetch.
    ///
    /// # Performance
    ///
    /// - Call before sequential scans to warm the page cache
    /// - Effective for ranges 64KB to 4MB
    /// - No-op if range is invalid or on non-Unix
    #[inline]
    pub fn prefetch(&self, offset: usize, len: usize) {
        #[cfg(unix)]
        {
            if offset >= self.len {
                return;
            }
            let actual_len = len.min(self.len - offset);
            if actual_len == 0 {
                return;
            }

            // SAFETY: We bounds-check offset and len above.
            // madvise WILLNEED is advisory and non-fatal on failure.
            unsafe {
                let addr = self.ptr.as_ptr().add(offset) as *mut libc::c_void;
                libc::madvise(addr, actual_len, libc::MADV_WILLNEED);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (offset, len);
        }
    }

    /// Hints to the kernel that specified pages are no longer needed.
    ///
    /// This allows the kernel to free physical pages backing this range.
    /// The pages will be re-read from the file if accessed again.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset into the mapped region.
    /// * `len` - Number of bytes to mark as unneeded.
    ///
    /// # Use Cases
    ///
    /// - After completing a large sequential scan
    /// - When memory pressure is high
    /// - Before process fork to reduce copy-on-write overhead
    #[inline]
    pub fn dontneed(&self, offset: usize, len: usize) {
        #[cfg(unix)]
        {
            if offset >= self.len {
                return;
            }
            let actual_len = len.min(self.len - offset);
            if actual_len == 0 {
                return;
            }

            // SAFETY: We bounds-check offset and len above.
            // madvise DONTNEED is advisory and non-fatal on failure.
            unsafe {
                let addr = self.ptr.as_ptr().add(offset) as *mut libc::c_void;
                libc::madvise(addr, actual_len, libc::MADV_DONTNEED);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (offset, len);
        }
    }

    /// Changes the access pattern hint for a region.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset into the mapped region.
    /// * `len` - Length of the region to hint.
    /// * `pattern` - The expected access pattern for this region.
    #[inline]
    pub fn advise_region(&self, offset: usize, len: usize, pattern: AccessPattern) {
        #[cfg(unix)]
        {
            if offset >= self.len {
                return;
            }
            let actual_len = len.min(self.len - offset);
            if actual_len == 0 {
                return;
            }

            let advice = match pattern {
                AccessPattern::Normal => libc::MADV_NORMAL,
                AccessPattern::Sequential => libc::MADV_SEQUENTIAL,
                AccessPattern::Random => libc::MADV_RANDOM,
                AccessPattern::WillNeedOnce => libc::MADV_SEQUENTIAL,
            };

            // SAFETY: We bounds-check offset and len above.
            unsafe {
                let addr = self.ptr.as_ptr().add(offset) as *mut libc::c_void;
                libc::madvise(addr, actual_len, advice);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = (offset, len, pattern);
        }
    }

    /// Returns the length of the mapped region in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the mapped region is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a slice of the entire mapped region.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: The pointer is valid for `len` bytes and properly aligned.
        // The memory is read-only and valid for the lifetime of self.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns a slice of a specific page using the default page size.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page number (0-indexed).
    ///
    /// # Returns
    ///
    /// `None` if the page is out of bounds.
    #[inline]
    pub fn page(&self, page_id: u64) -> Option<&[u8]> {
        self.page_with_size(page_id, PAGE_SIZE)
    }

    /// Returns a slice of a specific page with a given page size.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page number (0-indexed).
    /// * `page_size` - The page size in bytes.
    ///
    /// # Returns
    ///
    /// `None` if the page is out of bounds.
    #[inline]
    pub fn page_with_size(&self, page_id: u64, page_size: usize) -> Option<&[u8]> {
        let offset = page_id as usize * page_size;
        let end = offset.checked_add(page_size)?;
        if end > self.len {
            return None;
        }
        Some(&self.as_slice()[offset..end])
    }

    /// Returns a slice at a specific byte offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset into the mapped region.
    /// * `len` - Number of bytes to return.
    ///
    /// # Returns
    ///
    /// `None` if the range is out of bounds.
    #[inline]
    pub fn slice(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(len)?;
        if end > self.len {
            return None;
        }
        Some(&self.as_slice()[offset..end])
    }

    /// Returns the number of complete pages in the mapped region.
    #[inline]
    pub fn page_count(&self) -> u64 {
        (self.len / PAGE_SIZE) as u64
    }

    /// Returns the number of complete pages with a given page size.
    #[inline]
    pub fn page_count_with_size(&self, page_size: usize) -> u64 {
        (self.len / page_size) as u64
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            // SAFETY: ptr and len were set by a successful mmap call.
            unsafe {
                libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, self.len);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::io::Write;

    fn test_file_path(name: &str) -> String {
        format!("/tmp/thunder_mmap_test_{name}.db")
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    fn create_test_file(path: &str, pages: usize) -> File {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("failed to create test file");

        for page_num in 0..pages {
            let mut page = [0u8; PAGE_SIZE];
            page[0..8].copy_from_slice(&(page_num as u64).to_le_bytes());
            for (i, byte) in page[8..].iter_mut().enumerate() {
                *byte = ((page_num + i) % 256) as u8;
            }
            file.write_all(&page).expect("failed to write page");
        }
        file.sync_all().expect("failed to sync");

        OpenOptions::new()
            .read(true)
            .open(path)
            .expect("failed to reopen file")
    }

    #[test]
    fn test_mmap_creation_and_page_access() {
        let path = test_file_path("core");
        cleanup(&path);

        let file = create_test_file(&path, 4);
        let mmap = Mmap::new(&file, 4 * PAGE_SIZE).expect("mmap should succeed");

        assert_eq!(mmap.len(), 4 * PAGE_SIZE);
        assert_eq!(mmap.page_count(), 4);

        // Verify page content
        for page_num in 0..4u64 {
            let page = mmap.page(page_num).expect("page should exist");
            let stored_num = u64::from_le_bytes(page[0..8].try_into().unwrap());
            assert_eq!(stored_num, page_num);
        }

        // Out of bounds
        assert!(mmap.page(4).is_none());

        cleanup(&path);
    }

    #[test]
    fn test_mmap_page_with_size() {
        let path = test_file_path("page_size");
        cleanup(&path);

        let file = create_test_file(&path, 4); // 4 * 32KB = 128KB
        let mmap = Mmap::new(&file, 4 * PAGE_SIZE).expect("mmap should succeed");

        // Access as 64KB pages (2 pages)
        assert_eq!(mmap.page_count_with_size(65536), 2);
        let page0 = mmap.page_with_size(0, 65536).expect("page should exist");
        assert_eq!(page0.len(), 65536);

        // Access as 128KB pages (1 page)
        assert_eq!(mmap.page_count_with_size(131072), 1);
        let page0_large = mmap.page_with_size(0, 131072).expect("page should exist");
        assert_eq!(page0_large.len(), 131072);

        // Out of bounds with larger page size
        assert!(mmap.page_with_size(1, 131072).is_none());

        cleanup(&path);
    }

    #[test]
    fn test_mmap_slice() {
        let path = test_file_path("slice");
        cleanup(&path);

        let file = create_test_file(&path, 2);
        let mmap = Mmap::new(&file, 2 * PAGE_SIZE).expect("mmap should succeed");

        // Valid slice
        let slice = mmap.slice(100, 200).expect("slice should exist");
        assert_eq!(slice.len(), 200);

        // Out of bounds
        assert!(mmap.slice(2 * PAGE_SIZE - 50, 100).is_none());
        assert!(mmap.slice(usize::MAX, 10).is_none());

        cleanup(&path);
    }

    #[test]
    fn test_mmap_zero_length_fails() {
        let path = test_file_path("zero");
        cleanup(&path);

        let file = create_test_file(&path, 1);
        let result = Mmap::new(&file, 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        cleanup(&path);
    }
}
