//! Summary: Memory-mapped file I/O for efficient page access.
//! Copyright (c) YOAB. All rights reserved.

use std::fmt;
use std::fs::File;
use std::io;
use std::ptr::NonNull;

use crate::page::PAGE_SIZE;

/// A memory-mapped region of a database file.
///
/// Provides read-only access to pages via memory mapping, avoiding
/// explicit read syscalls for page access.
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
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot map zero-length region",
            ));
        }

        // SAFETY: We are calling mmap with valid parameters.
        // - PROT_READ: read-only access
        // - MAP_SHARED: changes to file are visible (though we don't write)
        // - fd from file handle is valid
        // - offset 0, len is the size we want
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;

            let ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };

            if ptr == libc::MAP_FAILED {
                return Err(io::Error::last_os_error());
            }

            // SAFETY: mmap succeeded, so ptr is valid and non-null.
            let ptr = unsafe { NonNull::new_unchecked(ptr as *mut u8) };

            Ok(Self { ptr, len })
        }

        #[cfg(not(unix))]
        {
            let _ = file;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "memory mapping not supported on this platform",
            ))
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

    /// Returns a slice of a specific page.
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
        let offset = page_id as usize * PAGE_SIZE;
        if offset + PAGE_SIZE > self.len {
            return None;
        }
        Some(&self.as_slice()[offset..offset + PAGE_SIZE])
    }

    /// Returns the number of complete pages in the mapped region.
    #[inline]
    pub fn page_count(&self) -> u64 {
        (self.len / PAGE_SIZE) as u64
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

        // Write known patterns to each page.
        for page_num in 0..pages {
            let mut page = [0u8; PAGE_SIZE];
            // Fill with page number as pattern.
            page[0..8].copy_from_slice(&(page_num as u64).to_le_bytes());
            // Fill rest with incrementing bytes.
            for (i, byte) in page[8..].iter_mut().enumerate() {
                *byte = ((page_num + i) % 256) as u8;
            }
            file.write_all(&page).expect("failed to write page");
        }
        file.sync_all().expect("failed to sync");

        // Reopen read-only for mapping.
        OpenOptions::new()
            .read(true)
            .open(path)
            .expect("failed to reopen file")
    }

    #[test]
    fn test_mmap_basic_creation() {
        let path = test_file_path("basic_creation");
        cleanup(&path);

        let file = create_test_file(&path, 4);
        let mmap = Mmap::new(&file, 4 * PAGE_SIZE).expect("mmap should succeed");

        assert_eq!(mmap.len(), 4 * PAGE_SIZE);
        assert!(!mmap.is_empty());
        assert_eq!(mmap.page_count(), 4);

        cleanup(&path);
    }

    #[test]
    fn test_mmap_zero_length_fails() {
        let path = test_file_path("zero_length");
        cleanup(&path);

        let file = create_test_file(&path, 1);
        let result = Mmap::new(&file, 0);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        cleanup(&path);
    }

    #[test]
    fn test_mmap_read_page_content() {
        let path = test_file_path("read_content");
        cleanup(&path);

        let file = create_test_file(&path, 4);
        let mmap = Mmap::new(&file, 4 * PAGE_SIZE).expect("mmap should succeed");

        // Verify each page has correct content.
        for page_num in 0..4u64 {
            let page = mmap.page(page_num).expect("page should exist");
            let stored_num = u64::from_le_bytes(page[0..8].try_into().unwrap());
            assert_eq!(stored_num, page_num, "page number mismatch");
        }

        cleanup(&path);
    }

    #[test]
    fn test_mmap_page_out_of_bounds() {
        let path = test_file_path("out_of_bounds");
        cleanup(&path);

        let file = create_test_file(&path, 2);
        let mmap = Mmap::new(&file, 2 * PAGE_SIZE).expect("mmap should succeed");

        assert!(mmap.page(0).is_some());
        assert!(mmap.page(1).is_some());
        assert!(mmap.page(2).is_none()); // Out of bounds.
        assert!(mmap.page(100).is_none()); // Way out of bounds.

        cleanup(&path);
    }

    #[test]
    fn test_mmap_as_slice() {
        let path = test_file_path("as_slice");
        cleanup(&path);

        let file = create_test_file(&path, 2);
        let mmap = Mmap::new(&file, 2 * PAGE_SIZE).expect("mmap should succeed");

        let slice = mmap.as_slice();
        assert_eq!(slice.len(), 2 * PAGE_SIZE);

        // Verify first page marker.
        let page0_num = u64::from_le_bytes(slice[0..8].try_into().unwrap());
        assert_eq!(page0_num, 0);

        // Verify second page marker.
        let page1_num =
            u64::from_le_bytes(slice[PAGE_SIZE..PAGE_SIZE + 8].try_into().unwrap());
        assert_eq!(page1_num, 1);

        cleanup(&path);
    }

    #[test]
    fn test_mmap_partial_file() {
        let path = test_file_path("partial");
        cleanup(&path);

        let file = create_test_file(&path, 4);
        // Only map first 2 pages.
        let mmap = Mmap::new(&file, 2 * PAGE_SIZE).expect("mmap should succeed");

        assert_eq!(mmap.page_count(), 2);
        assert!(mmap.page(0).is_some());
        assert!(mmap.page(1).is_some());
        assert!(mmap.page(2).is_none()); // Not mapped.

        cleanup(&path);
    }

    #[test]
    fn test_mmap_large_file() {
        let path = test_file_path("large");
        cleanup(&path);

        // Create a 100-page file (~400KB).
        let file = create_test_file(&path, 100);
        let mmap = Mmap::new(&file, 100 * PAGE_SIZE).expect("mmap should succeed");

        assert_eq!(mmap.page_count(), 100);

        // Spot check some pages.
        for &page_num in &[0u64, 50, 99] {
            let page = mmap.page(page_num).expect("page should exist");
            let stored_num = u64::from_le_bytes(page[0..8].try_into().unwrap());
            assert_eq!(stored_num, page_num);
        }

        cleanup(&path);
    }
}
