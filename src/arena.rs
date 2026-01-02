//! Summary: Bump allocator for transaction-scoped memory.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides a fast arena allocator optimized for transaction-scoped
//! allocations. Instead of calling the global allocator for each key/value copy,
//! transactions can bump-allocate from a contiguous chunk, dramatically reducing
//! allocation overhead.
//!
//! # Design
//!
//! - Allocations are bump-pointer style (O(1) allocation)
//! - Memory is organized in chunks that grow as needed
//! - Reset returns the arena to initial state without deallocating chunks
//! - All memory is zeroed on reset for security (prevents data leakage)
//!
//! # Performance
//!
//! - Allocation: O(1) typical, O(n) when new chunk needed
//! - Reset: O(n) where n is total allocated bytes (due to zeroing)
//! - Memory overhead: ~1x data size (vs ~2x with Vec per allocation)

use std::marker::PhantomData;

/// Default arena chunk size (64KB).
/// Chosen to balance memory efficiency with allocation frequency.
pub const DEFAULT_ARENA_SIZE: usize = 64 * 1024;

/// A bump allocator for transaction-scoped memory.
///
/// Provides fast allocation by simply incrementing a pointer. Memory is
/// organized in chunks that grow as needed. The arena can be reset for
/// reuse without deallocating the underlying memory.
///
/// # Security
///
/// The arena zeros all memory on reset to prevent data leakage between
/// transactions. This is critical when the arena may hold sensitive data
/// like encryption keys or user credentials.
///
/// # Example
///
/// ```
/// use thunder::arena::Arena;
///
/// let mut arena = Arena::new(1024);
///
/// // Fast bump allocation
/// let slice1 = arena.alloc(32);
/// slice1.fill(0xAA);
///
/// // Copy data into arena
/// let data = b"hello world";
/// let copied = arena.copy_slice(data);
/// assert_eq!(copied, data);
///
/// // Reset for reuse (memory is zeroed)
/// arena.reset();
/// assert_eq!(arena.bytes_used(), 0);
/// ```
#[derive(Debug)]
pub struct Arena {
    /// Memory chunks. Each chunk is a contiguous allocation.
    chunks: Vec<Vec<u8>>,
    /// Index of the current chunk being allocated from.
    current_chunk: usize,
    /// Offset within the current chunk.
    offset: usize,
    /// Size of each new chunk.
    chunk_size: usize,
    /// Total bytes allocated across all chunks.
    total_allocated: usize,
}

impl Arena {
    /// Creates a new arena with the specified chunk size.
    ///
    /// # Arguments
    ///
    /// * `chunk_size` - Size of each memory chunk in bytes.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is 0.
    #[inline]
    pub fn new(chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk_size must be greater than 0");
        Self {
            chunks: Vec::new(),
            current_chunk: 0,
            offset: 0,
            chunk_size,
            total_allocated: 0,
        }
    }

    /// Creates a new arena with default chunk size (64KB).
    #[inline]
    pub fn with_default_size() -> Self {
        Self::new(DEFAULT_ARENA_SIZE)
    }

    /// Creates a new arena with specified initial capacity.
    ///
    /// Pre-allocates a single chunk of the given size.
    #[inline]
    pub fn with_capacity(initial_capacity: usize) -> Self {
        let chunk_size = initial_capacity.max(1);
        let mut arena = Self::new(chunk_size);
        arena.ensure_capacity(0); // Pre-allocate first chunk
        arena
    }

    /// Allocates `size` bytes from the arena.
    ///
    /// Returns a mutable slice of zeroed memory. The allocation is
    /// bump-pointer style and very fast (O(1) typical).
    ///
    /// If the current chunk doesn't have enough space, a new chunk
    /// is allocated.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of bytes to allocate.
    ///
    /// # Returns
    ///
    /// A mutable slice of `size` bytes, initialized to zero.
    #[inline]
    pub fn alloc(&mut self, size: usize) -> &mut [u8] {
        if size == 0 {
            return &mut [];
        }

        self.ensure_capacity(size);

        let chunk = &mut self.chunks[self.current_chunk];
        let start = self.offset;
        let end = start + size;

        self.offset = end;
        self.total_allocated += size;

        &mut chunk[start..end]
    }

    /// Allocates `size` bytes with specified alignment.
    ///
    /// Useful for cache-line alignment (64 bytes) or page alignment
    /// to avoid false sharing and improve memory access patterns.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of bytes to allocate.
    /// * `align` - Required alignment (must be power of 2).
    ///
    /// # Returns
    ///
    /// A mutable slice of `size` bytes, with the starting address
    /// aligned to `align` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `align` is not a power of 2.
    ///
    /// # Performance
    ///
    /// Common alignments:
    /// - 64: Cache line (avoids false sharing)
    /// - 4096: Page alignment (for mmap-friendly buffers)
    /// - 32768: HPC page size (matches PAGE_SIZE)
    #[inline]
    pub fn alloc_aligned(&mut self, size: usize, align: usize) -> &mut [u8] {
        assert!(align.is_power_of_two(), "alignment must be power of 2");

        if size == 0 {
            return &mut [];
        }

        // Calculate padding needed for alignment
        // We need: (chunk_base + offset + padding) % align == 0
        // But since we don't know chunk_base at compile time, we need to
        // ensure allocation after getting the chunk.

        // First ensure we have enough space for size + potential padding
        let max_padding = align - 1;
        self.ensure_capacity(size + max_padding);

        let chunk = &mut self.chunks[self.current_chunk];
        let chunk_base = chunk.as_ptr() as usize;
        let current_addr = chunk_base + self.offset;

        // Calculate padding to achieve alignment
        let misalign = current_addr & (align - 1);
        let padding = if misalign == 0 { 0 } else { align - misalign };

        // Skip padding bytes
        if padding > 0 {
            self.offset += padding;
            self.total_allocated += padding;
        }

        // Now allocate the actual data
        let start = self.offset;
        let end = start + size;

        self.offset = end;
        self.total_allocated += size;

        &mut chunk[start..end]
    }

    /// Allocates and copies data into the arena.
    ///
    /// This is a convenience method that allocates space and copies
    /// the provided slice in one operation.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to copy into the arena.
    ///
    /// # Returns
    ///
    /// A slice pointing to the copied data within the arena.
    #[inline]
    pub fn copy_slice(&mut self, data: &[u8]) -> &[u8] {
        if data.is_empty() {
            return &[];
        }

        let dest = self.alloc(data.len());
        dest.copy_from_slice(data);

        // Return immutable reference
        // Safety: we just wrote to this memory and it won't be modified
        &self.chunks[self.current_chunk][self.offset - data.len()..self.offset]
    }

    /// Resets the arena for reuse.
    ///
    /// This clears all allocations but keeps the underlying memory
    /// for reuse. All memory is zeroed to prevent data leakage.
    ///
    /// # Security
    ///
    /// Memory is explicitly zeroed to ensure no sensitive data from
    /// previous transactions leaks to new allocations.
    pub fn reset(&mut self) {
        // Zero all used memory for security
        for (i, chunk) in self.chunks.iter_mut().enumerate() {
            if i < self.current_chunk {
                // Full chunk was used
                chunk.fill(0);
            } else if i == self.current_chunk {
                // Partial chunk
                chunk[..self.offset].fill(0);
            }
            // Chunks after current_chunk were never used
        }

        self.current_chunk = 0;
        self.offset = 0;
        self.total_allocated = 0;
    }

    /// Returns the total bytes currently allocated.
    #[inline]
    pub fn bytes_used(&self) -> usize {
        self.total_allocated
    }

    /// Returns the total capacity across all chunks.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }

    /// Returns the number of chunks allocated.
    #[inline]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Ensures there is capacity for at least `size` bytes.
    ///
    /// If the current chunk doesn't have enough space, allocates a new chunk.
    fn ensure_capacity(&mut self, size: usize) {
        if self.chunks.is_empty() {
            // Allocate first chunk
            let chunk_size = size.max(self.chunk_size);
            self.chunks.push(vec![0u8; chunk_size]);
            return;
        }

        let current_chunk_remaining = self.chunks[self.current_chunk].len() - self.offset;

        if size <= current_chunk_remaining {
            return; // Current chunk has enough space
        }

        // Need a new chunk
        // Check if there's a next chunk we can reuse
        if self.current_chunk + 1 < self.chunks.len() {
            self.current_chunk += 1;
            self.offset = 0;

            // Check if reused chunk is big enough
            if self.chunks[self.current_chunk].len() >= size {
                return;
            }
        }

        // Allocate new chunk (at least chunk_size or requested size)
        let new_chunk_size = size.max(self.chunk_size);
        let new_chunk = vec![0u8; new_chunk_size];

        if self.current_chunk + 1 < self.chunks.len() {
            // Replace undersized chunk
            self.chunks[self.current_chunk] = new_chunk;
        } else {
            // Add new chunk
            self.chunks.push(new_chunk);
            self.current_chunk = self.chunks.len() - 1;
        }
        self.offset = 0;
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::with_default_size()
    }
}

/// A typed arena for allocating values of a specific type.
///
/// This is a thin wrapper around `Arena` that provides type-safe
/// allocation for `Copy` types.
///
/// # Example
///
/// ```
/// use thunder::arena::TypedArena;
///
/// let mut arena: TypedArena<u64> = TypedArena::new(1024);
///
/// let value = arena.alloc(42u64);
/// assert_eq!(*value, 42);
///
/// let slice = arena.alloc_slice(&[1, 2, 3, 4, 5]);
/// assert_eq!(slice, &[1, 2, 3, 4, 5]);
/// ```
#[derive(Debug)]
pub struct TypedArena<T> {
    arena: Arena,
    _marker: PhantomData<T>,
}

impl<T: Copy> TypedArena<T> {
    /// Creates a new typed arena with the specified chunk size in bytes.
    #[inline]
    pub fn new(chunk_size: usize) -> Self {
        Self {
            arena: Arena::new(chunk_size),
            _marker: PhantomData,
        }
    }

    /// Creates a new typed arena with default chunk size.
    #[inline]
    pub fn with_default_size() -> Self {
        Self {
            arena: Arena::with_default_size(),
            _marker: PhantomData,
        }
    }

    /// Allocates space for a value and initializes it.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to store in the arena.
    ///
    /// # Returns
    ///
    /// A mutable reference to the allocated value.
    #[inline]
    pub fn alloc(&mut self, value: T) -> &mut T {
        let size = std::mem::size_of::<T>();
        let align = std::mem::align_of::<T>();

        // Align the allocation
        let padding = self.arena.offset % align;
        if padding != 0 {
            let _ = self.arena.alloc(align - padding);
        }

        let bytes = self.arena.alloc(size);

        // Safety: bytes is properly aligned and sized for T,
        // and T is Copy so there are no drop concerns.
        let ptr = bytes.as_mut_ptr() as *mut T;
        unsafe {
            ptr.write(value);
            &mut *ptr
        }
    }

    /// Allocates space for a slice and copies the values.
    ///
    /// # Arguments
    ///
    /// * `values` - The values to copy into the arena.
    ///
    /// # Returns
    ///
    /// A mutable slice containing the copied values.
    #[inline]
    pub fn alloc_slice(&mut self, values: &[T]) -> &mut [T] {
        if values.is_empty() {
            return &mut [];
        }

        let size = std::mem::size_of_val(values);
        let align = std::mem::align_of::<T>();

        // Align the allocation
        let padding = self.arena.offset % align;
        if padding != 0 {
            let _ = self.arena.alloc(align - padding);
        }

        let bytes = self.arena.alloc(size);

        // Safety: bytes is properly aligned and sized for [T],
        // and T is Copy so we can safely copy.
        let ptr = bytes.as_mut_ptr() as *mut T;
        unsafe {
            std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len());
            std::slice::from_raw_parts_mut(ptr, values.len())
        }
    }

    /// Resets the arena for reuse.
    #[inline]
    pub fn reset(&mut self) {
        self.arena.reset();
    }

    /// Returns total bytes used.
    #[inline]
    pub fn bytes_used(&self) -> usize {
        self.arena.bytes_used()
    }

    /// Returns total capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.arena.capacity()
    }
}

impl<T: Copy> Default for TypedArena<T> {
    fn default() -> Self {
        Self::with_default_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_basic_allocation() {
        let mut arena = Arena::new(1024);

        let slice1 = arena.alloc(32);
        assert_eq!(slice1.len(), 32);
        assert!(slice1.iter().all(|&b| b == 0));

        let slice2 = arena.alloc(64);
        assert_eq!(slice2.len(), 64);
        assert!(slice2.iter().all(|&b| b == 0));

        assert_eq!(arena.bytes_used(), 96);
    }

    #[test]
    fn test_arena_copy_slice() {
        let mut arena = Arena::new(1024);

        let data = b"hello world";
        let copied = arena.copy_slice(data);

        assert_eq!(copied, data);
        assert_eq!(arena.bytes_used(), data.len());
    }

    #[test]
    fn test_arena_reset_zeroes_memory() {
        let mut arena = Arena::new(1024);

        let slice = arena.alloc(256);
        slice.fill(0xDE);

        arena.reset();

        assert_eq!(arena.bytes_used(), 0);

        // New allocation should be zeroed
        let new_slice = arena.alloc(256);
        assert!(new_slice.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_arena_chunk_growth() {
        let mut arena = Arena::new(64);

        // Allocate more than one chunk
        let _ = arena.alloc(32);
        let _ = arena.alloc(32);
        let _ = arena.alloc(64); // Should create new chunk

        assert!(arena.capacity() >= 128);
        assert!(arena.chunk_count() >= 2);
    }

    #[test]
    fn test_arena_large_allocation() {
        let mut arena = Arena::new(64);

        let large = arena.alloc(512);
        assert_eq!(large.len(), 512);

        large.fill(0xFF);
        assert!(large.iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_arena_zero_size_allocation() {
        let mut arena = Arena::new(1024);

        let empty = arena.alloc(0);
        assert_eq!(empty.len(), 0);

        // Should not affect bytes_used
        assert_eq!(arena.bytes_used(), 0);
    }

    #[test]
    fn test_typed_arena_basic() {
        let mut arena: TypedArena<u64> = TypedArena::new(1024);

        let value = arena.alloc(42u64);
        assert_eq!(*value, 42);

        *value = 100;
        assert_eq!(*value, 100);
    }

    #[test]
    fn test_typed_arena_slice() {
        let mut arena: TypedArena<i32> = TypedArena::new(1024);

        let values = [1, 2, 3, 4, 5];
        let slice = arena.alloc_slice(&values);

        assert_eq!(slice, &values);

        slice[2] = 100;
        assert_eq!(slice[2], 100);
    }
}
