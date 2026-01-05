//! Summary: Iterator configuration and scan metrics for Phase 2 read dominance.
//! Copyright (c) YOAB. All rights reserved.
//!
//! This module provides:
//! - `IterOptions`: Configuration for iterator behavior (prefetch hints, etc.)
//! - `ScanMetrics`: Statistics collected during iteration
//! - `MetricsIter`: Iterator wrapper that collects scan metrics
//!
//! # Performance Considerations
//!
//! - Prefetch hints allow the iterator to load upcoming data proactively.
//! - Metrics collection has minimal overhead when enabled.
//! - All structures are designed for cache-friendly access patterns.

use std::time::Instant;

/// Configuration options for iterators.
///
/// Controls prefetching, metrics collection, and other iterator behaviors
/// that affect scan performance.
///
/// # Example
///
/// ```ignore
/// let options = IterOptions::default()
///     .prefetch_count(32)
///     .collect_metrics(true);
/// let iter = rtx.iter_with_options(options);
/// ```
#[derive(Debug, Clone)]
pub struct IterOptions {
    /// Number of entries to prefetch ahead during iteration.
    /// Higher values improve sequential scan performance but use more memory.
    /// Default: 16
    pub(crate) prefetch_count: usize,

    /// Whether to collect scan metrics during iteration.
    /// Slight overhead when enabled.
    /// Default: false
    pub(crate) collect_metrics: bool,

    /// Hint that this will be a forward-only scan.
    /// Enables additional optimizations.
    /// Default: true
    pub(crate) forward_only: bool,
}

impl Default for IterOptions {
    fn default() -> Self {
        Self {
            prefetch_count: 16,
            collect_metrics: false,
            forward_only: true,
        }
    }
}

impl IterOptions {
    /// Creates new iterator options with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of entries to prefetch ahead.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of entries to prefetch (0 to disable prefetching)
    ///
    /// # Performance
    ///
    /// - Values 16-64 work well for most workloads.
    /// - Higher values benefit large sequential scans.
    /// - Lower values are better for selective scans with early termination.
    #[must_use]
    pub fn prefetch_count(mut self, count: usize) -> Self {
        self.prefetch_count = count;
        self
    }

    /// Enables or disables metrics collection.
    ///
    /// When enabled, the iterator tracks:
    /// - Number of keys scanned
    /// - Bytes scanned
    /// - Total scan duration
    #[must_use]
    pub fn collect_metrics(mut self, collect: bool) -> Self {
        self.collect_metrics = collect;
        self
    }

    /// Indicates whether this will be a forward-only scan.
    ///
    /// Forward-only scans can enable additional optimizations
    /// like sequential read-ahead.
    #[must_use]
    pub fn forward_only(mut self, forward: bool) -> Self {
        self.forward_only = forward;
        self
    }
}

/// Metrics collected during a scan operation.
///
/// Provides statistics about iterator performance for monitoring
/// and optimization purposes.
///
/// # Example
///
/// ```ignore
/// let mut iter = rtx.iter_with_metrics();
/// for (key, value) in iter.by_ref() {
///     // process entries
/// }
/// let metrics = iter.metrics();
/// println!("Scanned {} keys, {} bytes in {:?}",
///     metrics.keys_scanned,
///     metrics.bytes_scanned,
///     std::time::Duration::from_nanos(metrics.scan_duration_ns));
/// ```
#[derive(Debug, Clone, Default)]
pub struct ScanMetrics {
    /// Number of keys scanned.
    pub keys_scanned: u64,

    /// Total bytes scanned (keys + values).
    pub bytes_scanned: u64,

    /// Scan duration in nanoseconds.
    pub scan_duration_ns: u64,

    /// Number of leaf nodes visited.
    pub leaf_nodes_visited: u64,

    /// Number of branch nodes traversed.
    pub branch_nodes_traversed: u64,

    /// Cache hit ratio (0.0 to 1.0) if cache tracking is enabled.
    /// -1.0 if not available.
    pub cache_hit_ratio: f64,
}

impl ScanMetrics {
    /// Creates new empty metrics.
    pub fn new() -> Self {
        Self {
            keys_scanned: 0,
            bytes_scanned: 0,
            scan_duration_ns: 0,
            leaf_nodes_visited: 0,
            branch_nodes_traversed: 0,
            cache_hit_ratio: -1.0,
        }
    }

    /// Returns the average bytes per key scanned.
    #[inline]
    pub fn avg_bytes_per_key(&self) -> f64 {
        if self.keys_scanned == 0 {
            0.0
        } else {
            self.bytes_scanned as f64 / self.keys_scanned as f64
        }
    }

    /// Returns scan throughput in keys per second.
    #[inline]
    pub fn keys_per_second(&self) -> f64 {
        if self.scan_duration_ns == 0 {
            0.0
        } else {
            (self.keys_scanned as f64 * 1_000_000_000.0) / self.scan_duration_ns as f64
        }
    }

    /// Returns scan throughput in bytes per second.
    #[inline]
    pub fn bytes_per_second(&self) -> f64 {
        if self.scan_duration_ns == 0 {
            0.0
        } else {
            (self.bytes_scanned as f64 * 1_000_000_000.0) / self.scan_duration_ns as f64
        }
    }
}

/// An iterator wrapper that collects scan metrics.
///
/// Wraps any iterator and tracks performance statistics during iteration.
/// Access metrics after iteration using the `metrics()` method.
pub struct MetricsIter<I> {
    /// The underlying iterator.
    inner: I,

    /// Collected metrics.
    metrics: ScanMetrics,

    /// When the iteration started.
    start_time: Option<Instant>,
}

impl<I> MetricsIter<I> {
    /// Creates a new metrics-collecting iterator.
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            metrics: ScanMetrics::new(),
            start_time: None,
        }
    }

    /// Returns the collected metrics.
    ///
    /// Call this after iteration is complete to get final statistics.
    pub fn metrics(&self) -> ScanMetrics {
        let mut m = self.metrics.clone();
        // Finalize duration if still running
        if let Some(start) = self.start_time {
            m.scan_duration_ns = start.elapsed().as_nanos() as u64;
        }
        m
    }

    /// Returns a mutable reference to the underlying iterator.
    pub fn inner_mut(&mut self) -> &mut I {
        &mut self.inner
    }
}

impl<'a, I> Iterator for MetricsIter<I>
where
    I: Iterator<Item = (&'a [u8], &'a [u8])>,
{
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        // Start timing on first call
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }

        match self.inner.next() {
            Some((key, value)) => {
                self.metrics.keys_scanned += 1;
                self.metrics.bytes_scanned += (key.len() + value.len()) as u64;
                Some((key, value))
            }
            None => {
                // Finalize duration
                if let Some(start) = self.start_time.take() {
                    self.metrics.scan_duration_ns = start.elapsed().as_nanos() as u64;
                }
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Iterator with prefetch hints for optimized sequential access.
///
/// This iterator provides hints to the underlying storage layer
/// about upcoming access patterns, enabling better cache utilization.
pub struct PrefetchIter<'a, I> {
    /// The underlying iterator.
    inner: I,

    /// Prefetch configuration.
    prefetch_count: usize,

    /// Number of items yielded since last prefetch hint.
    items_since_prefetch: usize,

    /// Marker for lifetime.
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, I> PrefetchIter<'a, I> {
    /// Creates a new prefetch iterator.
    pub fn new(inner: I, prefetch_count: usize) -> Self {
        Self {
            inner,
            prefetch_count,
            items_since_prefetch: 0,
            _marker: std::marker::PhantomData,
        }
    }

    /// Issues a prefetch hint for upcoming data.
    ///
    /// This is a no-op placeholder that can be enhanced with actual
    /// prefetch system calls (e.g., madvise) when backed by mmap.
    #[inline]
    fn maybe_prefetch(&mut self) {
        if self.prefetch_count > 0 && self.items_since_prefetch >= self.prefetch_count {
            // In a full implementation, this would issue prefetch hints
            // to the underlying storage (madvise, fadvise, etc.)
            self.items_since_prefetch = 0;
        }
    }
}

impl<'a, I> Iterator for PrefetchIter<'a, I>
where
    I: Iterator<Item = (&'a [u8], &'a [u8])>,
{
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.maybe_prefetch();
        let result = self.inner.next();
        if result.is_some() {
            self.items_since_prefetch += 1;
        }
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iter_options_builder() {
        let opts = IterOptions::default()
            .prefetch_count(32)
            .collect_metrics(true)
            .forward_only(false);

        assert_eq!(opts.prefetch_count, 32);
        assert!(opts.collect_metrics);
        assert!(!opts.forward_only);
    }

    #[test]
    fn test_scan_metrics_calculations() {
        let mut metrics = ScanMetrics::new();
        metrics.keys_scanned = 1000;
        metrics.bytes_scanned = 50_000;
        metrics.scan_duration_ns = 1_000_000_000; // 1 second = 1,000,000,000 ns

        assert_eq!(metrics.avg_bytes_per_key(), 50.0);
        // 1000 keys in 1 second = 1000 keys per second
        assert_eq!(metrics.keys_per_second(), 1000.0);
        // 50,000 bytes in 1 second = 50,000 bytes per second
        assert_eq!(metrics.bytes_per_second(), 50_000.0);
    }

    #[test]
    fn test_metrics_iter() {
        let data = vec![
            (b"key1".as_slice(), b"value1".as_slice()),
            (b"key2".as_slice(), b"value2".as_slice()),
        ];

        let mut iter = MetricsIter::new(data.into_iter());
        let _: Vec<_> = iter.by_ref().collect();

        let metrics = iter.metrics();
        assert_eq!(metrics.keys_scanned, 2);
        assert_eq!(metrics.bytes_scanned, 4 + 6 + 4 + 6); // key1+value1+key2+value2
    }
}
