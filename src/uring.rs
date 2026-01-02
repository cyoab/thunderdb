//! Summary: io_uring-based asynchronous I/O backend for Linux.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides high-performance I/O using Linux io_uring (5.1+).
//! Only available on Linux with the `io_uring` feature enabled.

#![cfg(all(target_os = "linux", feature = "io_uring"))]

use crate::error::{Error, Result};
use crate::io_backend::{IoBackend, ReadOp, ReadResult, WriteOp};
use io_uring::{opcode, types, IoUring};
use std::fs::File;
use std::os::unix::io::{AsRawFd, RawFd};

/// Default io_uring submission queue depth.
pub const DEFAULT_QUEUE_DEPTH: u32 = 64;

/// Maximum operations per submission batch.
pub const MAX_OPS_PER_SUBMIT: usize = 32;

/// io_uring-based I/O backend for high-performance asynchronous I/O.
///
/// This backend batches operations and submits them through io_uring's
/// submission queue, providing significant throughput improvements on
/// NVMe storage.
///
/// # Platform Requirements
///
/// - Linux kernel 5.1 or later
/// - The `io_uring` feature must be enabled
pub struct UringBackend {
    /// The io_uring instance.
    ring: IoUring,
    /// File descriptor for the database file.
    fd: RawFd,
    /// File handle (kept for ownership).
    _file: File,
    /// Pending write buffers (kept alive during I/O).
    pending_writes: Vec<Vec<u8>>,
    /// Queue depth configuration.
    queue_depth: u32,
}

impl UringBackend {
    /// Creates a new io_uring backend with the specified queue depth.
    ///
    /// # Arguments
    ///
    /// * `file` - The database file handle.
    /// * `queue_depth` - Number of entries in submission/completion queues.
    ///
    /// # Errors
    ///
    /// Returns an error if io_uring initialization fails (e.g., kernel too old).
    pub fn new(file: File, queue_depth: u32) -> Result<Self> {
        let ring = IoUring::builder()
            .setup_sqpoll(1000) // Kernel-side polling with 1ms idle timeout
            .build(queue_depth)
            .map_err(|e| Error::IoUringInit {
                context: "creating io_uring instance",
                source: e,
            })?;

        let fd = file.as_raw_fd();

        Ok(Self {
            ring,
            fd,
            _file: file,
            pending_writes: Vec::new(),
            queue_depth,
        })
    }

    /// Creates a new io_uring backend with default settings.
    ///
    /// Uses `DEFAULT_QUEUE_DEPTH` (64) for the queue size.
    pub fn with_defaults(file: File) -> Result<Self> {
        Self::new(file, DEFAULT_QUEUE_DEPTH)
    }

    /// Checks if io_uring is supported on this system.
    ///
    /// # Returns
    ///
    /// `true` if io_uring is available and functional.
    pub fn is_supported() -> bool {
        // Try to create a minimal io_uring instance to test support
        IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
            .build(1)
            .is_ok()
    }

    /// Submits pending operations and waits for completions.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of completions to wait for.
    ///
    /// # Errors
    ///
    /// Returns an error if submission or any completion fails.
    fn submit_and_wait(&mut self, count: usize) -> Result<()> {
        if count == 0 {
            return Ok(());
        }

        self.ring
            .submit_and_wait(count)
            .map_err(|e| Error::IoUringSubmit {
                context: "submitting io_uring operations",
                source: e,
            })?;

        // Process completions and check for errors
        let mut completions = self.ring.completion();
        for cqe in &mut completions {
            let result = cqe.result();
            if result < 0 {
                return Err(Error::IoUringCompletion {
                    context: "io_uring operation completed with error",
                    errno: -result,
                });
            }
        }

        Ok(())
    }

    /// Builds a write entry for io_uring submission.
    fn build_write_entry(
        &self,
        data: &[u8],
        offset: u64,
        user_data: u64,
    ) -> io_uring::squeue::Entry {
        opcode::Write::new(types::Fd(self.fd), data.as_ptr(), data.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data)
    }

    /// Builds a read entry for io_uring submission.
    fn build_read_entry(
        &self,
        buf: &mut [u8],
        offset: u64,
        user_data: u64,
    ) -> io_uring::squeue::Entry {
        opcode::Read::new(types::Fd(self.fd), buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data)
    }
}

impl IoBackend for UringBackend {
    fn write_batch(&mut self, ops: Vec<WriteOp>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }

        // Store buffers to keep them alive during I/O
        self.pending_writes.clear();
        self.pending_writes.reserve(ops.len());

        let mut submitted = 0;

        for (idx, op) in ops.into_iter().enumerate() {
            self.pending_writes.push(op.data);
            let data = &self.pending_writes[idx];
            let entry = self.build_write_entry(data, op.offset, idx as u64);

            // SAFETY: Entry references data that is kept alive in pending_writes.
            unsafe {
                self.ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| Error::IoUringQueueFull)?;
            }
            submitted += 1;

            // Submit in batches to avoid queue overflow
            if submitted >= MAX_OPS_PER_SUBMIT {
                self.submit_and_wait(submitted)?;
                submitted = 0;
            }
        }

        // Submit remaining operations
        if submitted > 0 {
            self.submit_and_wait(submitted)?;
        }

        Ok(())
    }

    fn read_batch(&mut self, ops: Vec<ReadOp>) -> Result<Vec<ReadResult>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }

        let mut buffers: Vec<Vec<u8>> = ops.iter().map(|op| vec![0u8; op.len]).collect();

        let mut submitted = 0;

        for (idx, (op, buf)) in ops.iter().zip(buffers.iter_mut()).enumerate() {
            let entry = self.build_read_entry(buf, op.offset, idx as u64);

            // SAFETY: Entry references buf which stays alive for the duration.
            unsafe {
                self.ring
                    .submission()
                    .push(&entry)
                    .map_err(|_| Error::IoUringQueueFull)?;
            }
            submitted += 1;

            if submitted >= MAX_OPS_PER_SUBMIT {
                self.submit_and_wait(submitted)?;
                submitted = 0;
            }
        }

        if submitted > 0 {
            self.submit_and_wait(submitted)?;
        }

        // Convert buffers to results
        Ok(buffers
            .into_iter()
            .map(|data| {
                let bytes_read = data.len();
                ReadResult { data, bytes_read }
            })
            .collect())
    }

    fn sync(&mut self) -> Result<()> {
        // Submit fsync via io_uring for consistency
        let entry = opcode::Fsync::new(types::Fd(self.fd))
            .build()
            .user_data(u64::MAX);

        // SAFETY: Entry is valid and does not reference external memory.
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| Error::IoUringQueueFull)?;
        }

        self.submit_and_wait(1)?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "io_uring"
    }

    fn supports_parallel(&self) -> bool {
        true
    }

    fn optimal_batch_size(&self) -> usize {
        self.queue_depth as usize / 2
    }
}

impl Drop for UringBackend {
    fn drop(&mut self) {
        // Ensure all pending operations complete before dropping
        let _ = self.ring.submit_and_wait(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    fn test_file(name: &str) -> String {
        format!("/tmp/thunder_uring_test_{name}.db")
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
    fn test_uring_is_supported() {
        // This should return a valid boolean on Linux
        let supported = UringBackend::is_supported();
        println!("io_uring supported: {supported}");
    }

    #[test]
    fn test_uring_backend_basic() {
        if !UringBackend::is_supported() {
            println!("Skipping test: io_uring not supported");
            return;
        }

        let path = test_file("basic");
        let file = create_file(&path, 64 * 1024);

        let backend_result = UringBackend::with_defaults(file);
        if let Err(ref e) = backend_result {
            println!("io_uring init failed (expected on some systems): {e}");
            cleanup(&path);
            return;
        }

        let mut backend = backend_result.unwrap();

        assert_eq!(backend.name(), "io_uring");
        assert!(backend.supports_parallel());
        assert!(backend.optimal_batch_size() > 0);

        // Write test
        let test_data = vec![0xABu8; 32768];
        backend
            .write_batch(vec![WriteOp::new(0, test_data.clone())])
            .expect("write should succeed");
        backend.sync().expect("sync should succeed");

        // Read back
        let results = backend
            .read_batch(vec![ReadOp::new(0, 32768)])
            .expect("read should succeed");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].data, test_data);

        cleanup(&path);
    }

    #[test]
    fn test_uring_backend_batch() {
        if !UringBackend::is_supported() {
            return;
        }

        let path = test_file("batch");
        let file = create_file(&path, 1024 * 1024);

        let backend_result = UringBackend::with_defaults(file);
        if backend_result.is_err() {
            cleanup(&path);
            return;
        }

        let mut backend = backend_result.unwrap();

        // Write multiple pages
        let write_ops: Vec<WriteOp> = (0..16)
            .map(|i| WriteOp::new(i * 32768, vec![(i & 0xFF) as u8; 32768]))
            .collect();

        backend.write_batch(write_ops).expect("batch write");
        backend.sync().expect("sync");

        // Read them back
        let read_ops: Vec<ReadOp> = (0..16).map(|i| ReadOp::new(i * 32768, 32768)).collect();

        let results = backend.read_batch(read_ops).expect("batch read");
        assert_eq!(results.len(), 16);

        for (i, result) in results.iter().enumerate() {
            let expected = (i & 0xFF) as u8;
            assert!(
                result.data.iter().all(|&b| b == expected),
                "page {i} corrupted"
            );
        }

        cleanup(&path);
    }
}
