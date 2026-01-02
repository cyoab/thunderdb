//! Summary: Aligned memory allocation for direct I/O (O_DIRECT).
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides aligned buffer allocation required for O_DIRECT I/O,
//! where memory addresses must be aligned to filesystem block boundaries.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

/// Default alignment for direct I/O (32KB, HPC standard page size).
pub const DEFAULT_ALIGNMENT: usize = 32768;

/// A buffer with guaranteed memory alignment.
///
/// Required for O_DIRECT I/O operations where the kernel mandates
/// aligned memory addresses. The buffer manages its own memory
/// and ensures proper deallocation.
///
/// # Safety
///
/// This type uses raw memory allocation internally but provides
/// a safe interface. The memory is properly aligned and the buffer
/// tracks its own length separately from capacity.
pub struct AlignedBuffer {
    /// Pointer to the allocated memory.
    ptr: NonNull<u8>,
    /// Current number of valid bytes in the buffer.
    len: usize,
    /// Total allocated capacity (always aligned).
    capacity: usize,
    /// Alignment requirement for this buffer.
    alignment: usize,
}

// SAFETY: The buffer exclusively owns its memory and doesn't share it.
unsafe impl Send for AlignedBuffer {}
// SAFETY: No interior mutability without &mut access.
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Creates a new aligned buffer with the specified capacity and alignment.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Minimum capacity in bytes.
    /// * `alignment` - Required alignment (must be power of 2).
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - `alignment` is not a power of 2
    /// - Memory allocation fails
    ///
    /// # Example
    ///
    /// ```
    /// use thunder::aligned::AlignedBuffer;
    ///
    /// let buf = AlignedBuffer::new(8192, 4096);
    /// assert_eq!(buf.alignment(), 4096);
    /// assert!(buf.capacity() >= 8192);
    /// ```
    pub fn new(capacity: usize, alignment: usize) -> Self {
        assert!(
            alignment.is_power_of_two(),
            "alignment must be power of 2, got {alignment}"
        );

        // Round capacity up to alignment boundary
        let aligned_capacity = if capacity == 0 {
            alignment
        } else {
            (capacity + alignment - 1) & !(alignment - 1)
        };

        let layout = Layout::from_size_align(aligned_capacity, alignment)
            .expect("invalid layout parameters");

        // SAFETY: Layout is valid and non-zero sized.
        let ptr = unsafe { alloc_zeroed(layout) };

        let ptr = NonNull::new(ptr).expect("memory allocation failed");

        Self {
            ptr,
            len: 0,
            capacity: aligned_capacity,
            alignment,
        }
    }

    /// Creates a buffer with default 4KB alignment.
    ///
    /// This is the most common alignment for filesystem block devices.
    #[inline]
    pub fn with_default_alignment(capacity: usize) -> Self {
        Self::new(capacity, DEFAULT_ALIGNMENT)
    }

    /// Returns the alignment of this buffer.
    #[inline]
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    /// Returns the total allocated capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the current number of valid bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the buffer contains no data.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clears the buffer, setting length to zero without deallocating.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Extends the buffer with data from a slice.
    ///
    /// # Panics
    ///
    /// Panics if there isn't enough capacity for the additional data.
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        assert!(
            new_len <= self.capacity,
            "buffer overflow: need {new_len} bytes but capacity is {}",
            self.capacity
        );

        // SAFETY: We verified capacity and ptr is valid.
        unsafe {
            let dst = self.ptr.as_ptr().add(self.len);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
        self.len = new_len;
    }

    /// Pads the buffer to the next alignment boundary with zeros.
    ///
    /// This is required for O_DIRECT writes which must be aligned.
    pub fn pad_to_alignment(&mut self) {
        let aligned_len = (self.len + self.alignment - 1) & !(self.alignment - 1);

        if aligned_len > self.len && aligned_len <= self.capacity {
            // Zero-fill the padding bytes
            // SAFETY: We stay within allocated capacity.
            unsafe {
                let pad_start = self.ptr.as_ptr().add(self.len);
                let pad_len = aligned_len - self.len;
                std::ptr::write_bytes(pad_start, 0, pad_len);
            }
            self.len = aligned_len;
        }
    }

    /// Returns the buffer contents as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for len bytes.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns the buffer contents as a mutable slice.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for len bytes and we have exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Returns the raw pointer to the buffer (for I/O operations).
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Returns the raw mutable pointer to the buffer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Sets the length of valid data in the buffer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `new_len` bytes have been initialized.
    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity);
        self.len = new_len;
    }

    /// Returns true if the given offset and length are properly aligned.
    #[inline]
    pub fn is_aligned(&self, offset: u64, len: usize) -> bool {
        (offset as usize).is_multiple_of(self.alignment) && len.is_multiple_of(self.alignment)
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for AlignedBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: Layout matches the one used for allocation.
        let layout = Layout::from_size_align(self.capacity, self.alignment)
            .expect("layout should be valid");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Clone for AlignedBuffer {
    fn clone(&self) -> Self {
        let mut new_buf = Self::new(self.capacity, self.alignment);
        new_buf.extend_from_slice(self.as_slice());
        new_buf
    }
}

impl std::fmt::Debug for AlignedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuffer")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .field("alignment", &self.alignment)
            .field("ptr", &self.ptr)
            .finish()
    }
}

/// Pool of aligned buffers for efficient reuse.
///
/// Maintains a collection of pre-allocated buffers that can be
/// acquired and released to avoid repeated allocation overhead.
///
/// # Thread Safety
///
/// This pool is NOT thread-safe. Use external synchronization
/// for concurrent access, or use one pool per thread.
pub struct AlignedBufferPool {
    /// Available buffers ready for reuse.
    buffers: Vec<AlignedBuffer>,
    /// Standard buffer size for this pool.
    buffer_size: usize,
    /// Alignment for all buffers in this pool.
    alignment: usize,
    /// Maximum number of buffers to keep in the pool.
    max_pool_size: usize,
}

impl AlignedBufferPool {
    /// Creates a new buffer pool.
    ///
    /// # Arguments
    ///
    /// * `buffer_size` - Size of each buffer.
    /// * `alignment` - Alignment requirement for buffers.
    /// * `max_pool_size` - Maximum buffers to keep (others are dropped).
    pub fn new(buffer_size: usize, alignment: usize, max_pool_size: usize) -> Self {
        Self {
            buffers: Vec::with_capacity(max_pool_size),
            buffer_size,
            alignment,
            max_pool_size,
        }
    }

    /// Acquires a buffer from the pool or allocates a new one.
    ///
    /// The returned buffer is empty (len = 0) but has the pool's
    /// standard capacity.
    pub fn acquire(&mut self) -> AlignedBuffer {
        self.buffers
            .pop()
            .unwrap_or_else(|| AlignedBuffer::new(self.buffer_size, self.alignment))
    }

    /// Returns a buffer to the pool for reuse.
    ///
    /// The buffer is cleared before being stored. If the pool is
    /// at capacity, the buffer is dropped instead.
    pub fn release(&mut self, mut buffer: AlignedBuffer) {
        if self.buffers.len() < self.max_pool_size {
            buffer.clear();
            self.buffers.push(buffer);
        }
        // Otherwise buffer is dropped
    }

    /// Returns the number of buffers currently in the pool.
    #[inline]
    pub fn available(&self) -> usize {
        self.buffers.len()
    }

    /// Pre-allocates buffers up to the specified count.
    ///
    /// Useful for warming up the pool before heavy I/O.
    /// The total pool size is capped at `max_pool_size`.
    pub fn preallocate(&mut self, count: usize) {
        let target = count.min(self.max_pool_size);
        let to_add = target.saturating_sub(self.buffers.len());
        for _ in 0..to_add {
            self.buffers
                .push(AlignedBuffer::new(self.buffer_size, self.alignment));
        }
    }

    /// Clears all buffers from the pool, freeing memory.
    pub fn clear(&mut self) {
        self.buffers.clear();
    }
}

impl std::fmt::Debug for AlignedBufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBufferPool")
            .field("available", &self.buffers.len())
            .field("buffer_size", &self.buffer_size)
            .field("alignment", &self.alignment)
            .field("max_pool_size", &self.max_pool_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer_creation() {
        let buf = AlignedBuffer::new(1024, 32768);

        assert_eq!(buf.len(), 0);
        assert!(buf.capacity() >= 1024);
        assert_eq!(buf.capacity() % 32768, 0);
        assert_eq!(buf.alignment(), 32768);
        assert_eq!(buf.as_ptr() as usize % 32768, 0);
    }

    #[test]
    fn test_aligned_buffer_extend() {
        let mut buf = AlignedBuffer::new(32768, 32768);

        buf.extend_from_slice(&[1, 2, 3, 4]);
        assert_eq!(buf.len(), 4);
        assert_eq!(buf.as_slice(), &[1, 2, 3, 4]);

        buf.extend_from_slice(&[5, 6]);
        assert_eq!(buf.len(), 6);
        assert_eq!(buf.as_slice(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_aligned_buffer_padding() {
        let mut buf = AlignedBuffer::new(65536, 32768);

        buf.extend_from_slice(&[0xAA; 100]);
        assert_eq!(buf.len(), 100);

        buf.pad_to_alignment();
        assert_eq!(buf.len(), 32768);
        assert_eq!(buf.len() % 32768, 0);

        // Original data intact
        assert!(buf.as_slice()[..100].iter().all(|&b| b == 0xAA));
        // Padding is zeros
        assert!(buf.as_slice()[100..].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_aligned_buffer_clear() {
        let mut buf = AlignedBuffer::new(32768, 32768);

        buf.extend_from_slice(&[1, 2, 3, 4, 5]);
        assert_eq!(buf.len(), 5);

        buf.clear();
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert!(buf.capacity() >= 32768); // Capacity unchanged
    }

    #[test]
    fn test_aligned_buffer_clone() {
        let mut original = AlignedBuffer::new(32768, 32768);
        original.extend_from_slice(&[0x42; 100]);

        let clone = original.clone();

        assert_eq!(clone.len(), 100);
        assert_eq!(clone.as_slice(), original.as_slice());
        assert_eq!(clone.alignment(), original.alignment());

        // Should be independent allocations
        assert_ne!(clone.as_ptr(), original.as_ptr());
    }

    #[test]
    fn test_aligned_buffer_deref() {
        let mut buf = AlignedBuffer::new(32768, 32768);
        buf.extend_from_slice(&[1, 2, 3, 4, 5]);

        // Test Deref
        let slice: &[u8] = &buf;
        assert_eq!(slice, &[1, 2, 3, 4, 5]);

        // Test DerefMut
        let slice_mut: &mut [u8] = &mut buf;
        slice_mut[0] = 10;
        assert_eq!(buf.as_slice()[0], 10);
    }

    #[test]
    fn test_aligned_buffer_various_alignments() {
        for alignment in [512, 1024, 2048, 4096, 8192, 32768] {
            let buf = AlignedBuffer::new(1000, alignment);
            assert_eq!(buf.alignment(), alignment);
            assert_eq!(buf.as_ptr() as usize % alignment, 0);
            assert_eq!(buf.capacity() % alignment, 0);
        }
    }

    #[test]
    #[should_panic(expected = "alignment must be power of 2")]
    fn test_non_power_of_two_panics() {
        let _ = AlignedBuffer::new(1024, 1000);
    }

    #[test]
    fn test_buffer_pool_basic() {
        let mut pool = AlignedBufferPool::new(32768, 32768, 4);

        assert_eq!(pool.available(), 0);

        let buf1 = pool.acquire();
        assert_eq!(pool.available(), 0);
        assert!(buf1.capacity() >= 32768);

        pool.release(buf1);
        assert_eq!(pool.available(), 1);

        let buf2 = pool.acquire();
        assert_eq!(pool.available(), 0);
        assert!(buf2.is_empty()); // Should be cleared
    }

    #[test]
    fn test_buffer_pool_limit() {
        let mut pool = AlignedBufferPool::new(32768, 32768, 2);

        let bufs: Vec<_> = (0..3).map(|_| pool.acquire()).collect();
        assert_eq!(bufs.len(), 3);

        for buf in bufs {
            pool.release(buf);
        }

        // Only 2 should be kept (max_pool_size)
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_buffer_pool_preallocate() {
        let mut pool = AlignedBufferPool::new(32768, 32768, 8);

        pool.preallocate(5);
        assert_eq!(pool.available(), 5);

        pool.preallocate(10); // Should cap at max_pool_size
        assert_eq!(pool.available(), 8);
    }

    #[test]
    fn test_is_aligned() {
        let buf = AlignedBuffer::new(32768, 32768);

        assert!(buf.is_aligned(0, 32768));
        assert!(buf.is_aligned(32768, 65536));
        assert!(!buf.is_aligned(100, 32768)); // Offset not aligned
        assert!(!buf.is_aligned(0, 100)); // Length not aligned
        assert!(!buf.is_aligned(100, 100)); // Neither aligned
    }

    #[test]
    fn test_default_alignment() {
        assert_eq!(DEFAULT_ALIGNMENT, 32768);

        let buf = AlignedBuffer::with_default_alignment(1024);
        assert_eq!(buf.alignment(), 32768);
    }
}
