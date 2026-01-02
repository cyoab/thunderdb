//! Summary: I/O backend abstraction layer for flexible write strategies.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides a trait-based abstraction for I/O operations,
//! enabling different backends (sync, io_uring, direct I/O) to be swapped.

use crate::error::{Error, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

/// A write operation for batched I/O.
///
/// Represents a single write to a specific offset in the file.
#[derive(Debug, Clone)]
pub struct WriteOp {
    /// Byte offset in the file where the write begins.
    pub offset: u64,
    /// Data to write at the specified offset.
    pub data: Vec<u8>,
}

impl WriteOp {
    /// Creates a new write operation.
    #[inline]
    pub fn new(offset: u64, data: Vec<u8>) -> Self {
        Self { offset, data }
    }

    /// Returns the size of this write operation in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if this write operation has no data.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the end offset of this write (exclusive).
    #[inline]
    pub fn end_offset(&self) -> u64 {
        self.offset + self.data.len() as u64
    }
}

/// A read operation for batched I/O.
///
/// Represents a single read from a specific offset in the file.
#[derive(Debug, Clone)]
pub struct ReadOp {
    /// Byte offset in the file where the read begins.
    pub offset: u64,
    /// Number of bytes to read.
    pub len: usize,
}

impl ReadOp {
    /// Creates a new read operation.
    #[inline]
    pub fn new(offset: u64, len: usize) -> Self {
        Self { offset, len }
    }
}

/// Result of a read operation.
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// The data that was read.
    pub data: Vec<u8>,
    /// Actual number of bytes read (may be less than requested at EOF).
    pub bytes_read: usize,
}

impl ReadResult {
    /// Creates a new read result.
    #[inline]
    pub fn new(data: Vec<u8>) -> Self {
        let bytes_read = data.len();
        Self { data, bytes_read }
    }

    /// Returns true if no data was read.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes_read == 0
    }
}

/// Trait for I/O backend implementations.
///
/// This trait abstracts over different I/O strategies, allowing the database
/// to use standard synchronous I/O, io_uring, or direct I/O depending on
/// the platform and configuration.
///
/// # Safety
///
/// Implementations must ensure:
/// - Writes are visible to subsequent reads after `sync()` returns.
/// - No data corruption occurs under any combination of operations.
/// - Proper error handling for all I/O failures.
pub trait IoBackend: Send + Sync {
    /// Submits a batch of write operations.
    ///
    /// The writes may be reordered for efficiency unless explicitly ordered.
    /// Data is not guaranteed to be durable until `sync()` is called.
    ///
    /// # Arguments
    ///
    /// * `ops` - Vector of write operations to perform.
    ///
    /// # Errors
    ///
    /// Returns an error if any write operation fails.
    fn write_batch(&mut self, ops: Vec<WriteOp>) -> Result<()>;

    /// Submits a batch of read operations.
    ///
    /// Returns results in the same order as the input operations.
    ///
    /// # Arguments
    ///
    /// * `ops` - Vector of read operations to perform.
    ///
    /// # Errors
    ///
    /// Returns an error if any read operation fails.
    fn read_batch(&mut self, ops: Vec<ReadOp>) -> Result<Vec<ReadResult>>;

    /// Ensures all previous writes are durable (synced to storage).
    ///
    /// After this method returns successfully, all data written via
    /// `write_batch` is guaranteed to survive a system crash.
    ///
    /// # Errors
    ///
    /// Returns an error if the sync operation fails.
    fn sync(&mut self) -> Result<()>;

    /// Returns the name of this backend for logging and diagnostics.
    fn name(&self) -> &'static str;

    /// Returns true if this backend supports parallel operations.
    ///
    /// Backends that return true can have multiple write operations
    /// submitted concurrently for better performance.
    fn supports_parallel(&self) -> bool;

    /// Returns the optimal batch size for this backend.
    ///
    /// Callers should aim to batch this many operations together
    /// for best performance.
    fn optimal_batch_size(&self) -> usize;
}

/// Standard synchronous I/O backend using `pwrite`/`pread`.
///
/// This is the fallback backend that works on all platforms.
/// It uses standard file operations and is safe but not optimal
/// for high-throughput workloads.
pub struct SyncBackend {
    /// The file handle for I/O operations.
    file: File,
}

impl SyncBackend {
    /// Creates a new synchronous I/O backend.
    ///
    /// # Arguments
    ///
    /// * `file` - An open file handle with read/write access.
    pub fn new(file: File) -> Self {
        Self { file }
    }

    /// Returns a reference to the underlying file handle.
    #[inline]
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Returns a mutable reference to the underlying file handle.
    #[inline]
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Performs fdatasync on Unix, sync_all elsewhere.
    #[cfg(unix)]
    fn fdatasync_impl(&self) -> Result<()> {
        // SAFETY: fdatasync is a standard POSIX call with a valid fd.
        let ret = unsafe { libc::fdatasync(self.file.as_raw_fd()) };
        if ret != 0 {
            return Err(Error::FileSync {
                context: "fdatasync in SyncBackend",
                source: std::io::Error::last_os_error(),
            });
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn fdatasync_impl(&self) -> Result<()> {
        self.file.sync_all().map_err(|e| Error::FileSync {
            context: "sync_all in SyncBackend",
            source: e,
        })
    }
}

impl IoBackend for SyncBackend {
    fn write_batch(&mut self, ops: Vec<WriteOp>) -> Result<()> {
        for op in ops {
            if op.is_empty() {
                continue;
            }

            self.file
                .seek(SeekFrom::Start(op.offset))
                .map_err(|e| Error::FileSeek {
                    offset: op.offset,
                    context: "SyncBackend::write_batch seek",
                    source: e,
                })?;

            self.file.write_all(&op.data).map_err(|e| Error::FileWrite {
                offset: op.offset,
                len: op.data.len(),
                context: "SyncBackend::write_batch write",
                source: e,
            })?;
        }
        Ok(())
    }

    fn read_batch(&mut self, ops: Vec<ReadOp>) -> Result<Vec<ReadResult>> {
        let mut results = Vec::with_capacity(ops.len());

        for op in ops {
            self.file
                .seek(SeekFrom::Start(op.offset))
                .map_err(|e| Error::FileSeek {
                    offset: op.offset,
                    context: "SyncBackend::read_batch seek",
                    source: e,
                })?;

            let mut data = vec![0u8; op.len];
            let bytes_read = self.file.read(&mut data).map_err(|e| Error::FileRead {
                offset: op.offset,
                len: op.len,
                context: "SyncBackend::read_batch read",
                source: e,
            })?;

            data.truncate(bytes_read);
            results.push(ReadResult { data, bytes_read });
        }

        Ok(results)
    }

    fn sync(&mut self) -> Result<()> {
        self.fdatasync_impl()
    }

    fn name(&self) -> &'static str {
        "sync"
    }

    fn supports_parallel(&self) -> bool {
        false
    }

    fn optimal_batch_size(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn test_file(name: &str) -> String {
        format!("/tmp/thunder_io_backend_test_{name}.db")
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
    }

    fn create_file(path: &str, size: usize) -> File {
        cleanup(path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.write_all(&vec![0u8; size]).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file
    }

    #[test]
    fn test_write_op_methods() {
        let op = WriteOp::new(100, vec![0u8; 50]);
        assert_eq!(op.offset, 100);
        assert_eq!(op.len(), 50);
        assert!(!op.is_empty());
        assert_eq!(op.end_offset(), 150);

        let empty_op = WriteOp::new(0, Vec::new());
        assert!(empty_op.is_empty());
    }

    #[test]
    fn test_read_result_methods() {
        let result = ReadResult::new(vec![1, 2, 3, 4]);
        assert_eq!(result.bytes_read, 4);
        assert!(!result.is_empty());

        let empty_result = ReadResult::new(Vec::new());
        assert!(empty_result.is_empty());
    }

    #[test]
    fn test_sync_backend_roundtrip() {
        let path = test_file("roundtrip");
        let file = create_file(&path, 32768);
        let mut backend = SyncBackend::new(file);

        let test_data = b"Hello, thunder!";
        backend
            .write_batch(vec![WriteOp::new(0, test_data.to_vec())])
            .unwrap();
        backend.sync().unwrap();

        let results = backend
            .read_batch(vec![ReadOp::new(0, test_data.len())])
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].data, test_data);

        cleanup(&path);
    }

    #[test]
    fn test_sync_backend_empty_batch() {
        let path = test_file("empty");
        let file = create_file(&path, 1024);
        let mut backend = SyncBackend::new(file);

        // Empty batches should succeed
        backend.write_batch(Vec::new()).unwrap();
        let results = backend.read_batch(Vec::new()).unwrap();
        assert!(results.is_empty());

        cleanup(&path);
    }

    #[test]
    fn test_sync_backend_metadata() {
        let path = test_file("meta");
        let file = create_file(&path, 1024);
        let backend = SyncBackend::new(file);

        assert_eq!(backend.name(), "sync");
        assert!(!backend.supports_parallel());
        assert_eq!(backend.optimal_batch_size(), 1);

        cleanup(&path);
    }
}
