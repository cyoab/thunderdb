//! Summary: Parallel I/O coordination for high-throughput writes.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides utilities for parallelizing I/O operations
//! to saturate NVMe bandwidth through concurrent submissions.

use crate::coalescer::WriteBatch;
use crate::page::PageId;

/// Configuration for parallel write operations.
///
/// Controls how write batches are partitioned and distributed
/// across workers for parallel execution.
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of worker threads for parallel writes.
    /// Set to 0 to use the number of CPU cores.
    pub num_workers: usize,
    /// Maximum operations per worker batch.
    /// Smaller values give better load balancing, larger values reduce overhead.
    pub ops_per_batch: usize,
    /// Whether to use thread-local I/O backends.
    /// When true, each worker gets its own file handle.
    pub use_thread_local_backend: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        // Use a reasonable default based on typical NVMe capabilities
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);

        Self {
            num_workers: num_cpus.min(8), // Cap at 8 workers
            ops_per_batch: 32,
            use_thread_local_backend: false,
        }
    }
}

impl ParallelConfig {
    /// Creates a configuration optimized for NVMe storage.
    ///
    /// Uses higher worker counts and batch sizes for maximum throughput.
    pub fn nvme_optimized() -> Self {
        let num_cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4);

        Self {
            num_workers: num_cpus.min(16),
            ops_per_batch: 64,
            use_thread_local_backend: true,
        }
    }

    /// Creates a conservative configuration for compatibility.
    pub fn conservative() -> Self {
        Self {
            num_workers: 2,
            ops_per_batch: 16,
            use_thread_local_backend: false,
        }
    }
}

/// Parallel writer for distributing I/O across multiple workers.
///
/// Coordinates the partitioning and parallel execution of write
/// batches for improved throughput on multi-queue storage devices.
pub struct ParallelWriter {
    /// Configuration for parallel operations.
    config: ParallelConfig,
}

impl ParallelWriter {
    /// Creates a new parallel writer with the specified configuration.
    pub fn new(config: ParallelConfig) -> Self {
        Self { config }
    }

    /// Creates a parallel writer with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(ParallelConfig::default())
    }

    /// Returns the configuration for this writer.
    #[inline]
    pub fn config(&self) -> &ParallelConfig {
        &self.config
    }

    /// Returns the number of workers configured.
    #[inline]
    pub fn num_workers(&self) -> usize {
        self.config.num_workers
    }

    /// Determines if a batch should be written in parallel.
    ///
    /// Small batches have too much overhead for parallelism.
    #[inline]
    pub fn should_parallelize(&self, batch: &WriteBatch) -> bool {
        // Only parallelize if we have enough pages to distribute
        batch.pages.len() > self.config.num_workers * 2
    }
}

impl std::fmt::Debug for ParallelWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelWriter")
            .field("config", &self.config)
            .finish()
    }
}

/// Partitions a write batch for parallel execution.
///
/// Distributes pages across partitions while maintaining these invariants:
/// - All pages are accounted for exactly once
/// - Sequential data only appears in the first partition
/// - Partitions are roughly equal in size
///
/// # Arguments
///
/// * `batch` - The write batch to partition.
/// * `num_partitions` - Number of partitions to create.
///
/// # Returns
///
/// A vector of `WriteBatch` instances, one per partition.
/// Returns a single-element vector if partitioning is not beneficial.
pub fn partition_for_parallel(batch: WriteBatch, num_partitions: usize) -> Vec<WriteBatch> {
    // Don't partition if it's not beneficial
    if batch.pages.is_empty() || num_partitions <= 1 {
        return vec![batch];
    }

    // Need at least 2 pages per partition to be worthwhile
    if batch.pages.len() < num_partitions * 2 {
        return vec![batch];
    }

    let pages_per_partition = batch.pages.len().div_ceil(num_partitions);

    let mut partitions = Vec::with_capacity(num_partitions);
    let mut pages_iter = batch.pages.into_iter();

    for i in 0..num_partitions {
        let pages: Vec<(PageId, Vec<u8>)> = pages_iter.by_ref().take(pages_per_partition).collect();

        if pages.is_empty() && i > 0 {
            // No more pages to distribute
            break;
        }

        partitions.push(WriteBatch {
            // Sequential data only in first partition
            sequential_data: if i == 0 {
                batch.sequential_data.clone()
            } else {
                Vec::new()
            },
            pages,
        });
    }

    // Ensure we have at least one partition
    if partitions.is_empty() {
        partitions.push(WriteBatch {
            sequential_data: batch.sequential_data,
            pages: Vec::new(),
        });
    }

    partitions
}

/// Statistics about partition distribution.
#[derive(Debug, Clone, Default)]
pub struct PartitionStats {
    /// Number of partitions created.
    pub partition_count: usize,
    /// Total number of pages distributed.
    pub total_pages: usize,
    /// Minimum pages in any partition.
    pub min_pages: usize,
    /// Maximum pages in any partition.
    pub max_pages: usize,
    /// Size of sequential data (only in first partition).
    pub sequential_size: usize,
}

impl PartitionStats {
    /// Calculates statistics for a set of partitions.
    pub fn from_partitions(partitions: &[WriteBatch]) -> Self {
        if partitions.is_empty() {
            return Self::default();
        }

        let partition_count = partitions.len();
        let total_pages: usize = partitions.iter().map(|p| p.pages.len()).sum();
        let min_pages = partitions.iter().map(|p| p.pages.len()).min().unwrap_or(0);
        let max_pages = partitions.iter().map(|p| p.pages.len()).max().unwrap_or(0);
        let sequential_size = partitions
            .first()
            .map(|p| p.sequential_data.len())
            .unwrap_or(0);

        Self {
            partition_count,
            total_pages,
            min_pages,
            max_pages,
            sequential_size,
        }
    }

    /// Returns the load imbalance ratio (max/min pages).
    ///
    /// A value of 1.0 indicates perfect balance.
    pub fn imbalance_ratio(&self) -> f64 {
        if self.min_pages == 0 {
            if self.max_pages == 0 {
                1.0
            } else {
                f64::INFINITY
            }
        } else {
            self.max_pages as f64 / self.min_pages as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coalescer::WriteCoalescer;

    fn create_test_batch(num_pages: usize) -> WriteBatch {
        let mut coalescer = WriteCoalescer::new(32768, 16 * 1024 * 1024);

        for i in 0..num_pages {
            coalescer.queue_page(i as u64, vec![(i & 0xFF) as u8; 32768]);
        }
        coalescer.queue_sequential(&[0xEE; 1024]);

        coalescer.into_write_batch()
    }

    #[test]
    fn test_parallel_config_default() {
        let config = ParallelConfig::default();

        assert!(config.num_workers > 0);
        assert!(config.num_workers <= 8);
        assert!(config.ops_per_batch > 0);
    }

    #[test]
    fn test_parallel_config_nvme() {
        let config = ParallelConfig::nvme_optimized();

        assert!(config.num_workers > 0);
        assert!(config.ops_per_batch >= 64);
        assert!(config.use_thread_local_backend);
    }

    #[test]
    fn test_parallel_writer_creation() {
        let writer = ParallelWriter::with_defaults();
        assert!(writer.num_workers() > 0);
    }

    #[test]
    fn test_partition_empty_batch() {
        let batch = WriteBatch::empty();
        let partitions = partition_for_parallel(batch, 4);

        assert_eq!(partitions.len(), 1);
        assert!(partitions[0].pages.is_empty());
    }

    #[test]
    fn test_partition_single_page() {
        let batch = create_test_batch(1);
        let partitions = partition_for_parallel(batch, 4);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].pages.len(), 1);
    }

    #[test]
    fn test_partition_distribution() {
        let batch = create_test_batch(100);
        let seq_data = batch.sequential_data.clone();
        let partitions = partition_for_parallel(batch, 4);

        assert_eq!(partitions.len(), 4);

        // Check all pages accounted for
        let total_pages: usize = partitions.iter().map(|p| p.pages.len()).sum();
        assert_eq!(total_pages, 100);

        // Sequential data only in first partition
        assert_eq!(partitions[0].sequential_data, seq_data);
        for partition in &partitions[1..] {
            assert!(partition.sequential_data.is_empty());
        }

        // Check roughly equal distribution
        for partition in &partitions {
            assert!(partition.pages.len() >= 20); // 100/4 = 25, with some tolerance
            assert!(partition.pages.len() <= 30);
        }
    }

    #[test]
    fn test_partition_preserves_data_integrity() {
        let batch = create_test_batch(50);

        // Collect original pages
        let original_pages: std::collections::HashMap<u64, Vec<u8>> = batch
            .pages
            .iter()
            .map(|(id, data)| (*id, data.clone()))
            .collect();

        let partitions = partition_for_parallel(batch, 4);

        // Verify all pages present with correct data
        let mut seen = std::collections::HashSet::new();
        for partition in &partitions {
            for (page_id, data) in &partition.pages {
                assert!(seen.insert(*page_id), "duplicate page {page_id}");
                let original = original_pages.get(page_id).expect("page should exist");
                assert_eq!(data, original, "page {page_id} data mismatch");
            }
        }

        assert_eq!(seen.len(), 50);
    }

    #[test]
    fn test_partition_single_partition() {
        let batch = create_test_batch(20);
        let partitions = partition_for_parallel(batch, 1);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].pages.len(), 20);
    }

    #[test]
    fn test_partition_more_partitions_than_pages() {
        let batch = create_test_batch(3);
        let partitions = partition_for_parallel(batch, 10);

        // Should return original batch since 3 < 10*2
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].pages.len(), 3);
    }

    #[test]
    fn test_should_parallelize() {
        let writer = ParallelWriter::new(ParallelConfig {
            num_workers: 4,
            ops_per_batch: 32,
            use_thread_local_backend: false,
        });

        let small_batch = create_test_batch(5);
        assert!(!writer.should_parallelize(&small_batch));

        let large_batch = create_test_batch(50);
        assert!(writer.should_parallelize(&large_batch));
    }

    #[test]
    fn test_partition_stats() {
        let batch = create_test_batch(100);
        let partitions = partition_for_parallel(batch, 4);
        let stats = PartitionStats::from_partitions(&partitions);

        assert_eq!(stats.partition_count, 4);
        assert_eq!(stats.total_pages, 100);
        assert!(stats.min_pages > 0);
        assert!(stats.max_pages <= 30);
        assert_eq!(stats.sequential_size, 1024);

        // Imbalance should be low for evenly distributed pages
        assert!(stats.imbalance_ratio() < 2.0);
    }

    #[test]
    fn test_partition_stats_empty() {
        let stats = PartitionStats::from_partitions(&[]);

        assert_eq!(stats.partition_count, 0);
        assert_eq!(stats.total_pages, 0);
        assert_eq!(stats.imbalance_ratio(), 1.0);
    }

    #[test]
    fn test_partition_deterministic() {
        // Same input should produce same output
        let batch1 = create_test_batch(40);
        let batch2 = create_test_batch(40);

        let partitions1 = partition_for_parallel(batch1, 4);
        let partitions2 = partition_for_parallel(batch2, 4);

        assert_eq!(partitions1.len(), partitions2.len());

        for (p1, p2) in partitions1.iter().zip(partitions2.iter()) {
            assert_eq!(p1.pages.len(), p2.pages.len());
            assert_eq!(p1.sequential_data.len(), p2.sequential_data.len());
        }
    }
}
