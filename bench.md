# Thunder vs BBolt Benchmark Results

## Raw Results

### Thunder (Rust)
| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 tx) | 506ms | 197,618 ops/sec |
| Sequential reads (100K keys) | 40ms | 2,517,241 ops/sec |
| Random reads (100K lookups) | 73ms | 1,360,771 ops/sec |
| Iterator scan (100K keys) | 1ms | 96,260,566 ops/sec |
| Mixed workload (10K ops, 70% read) | 1191s | 8 ops/sec |
| Batch writes (1K tx, 100 ops/tx) | 598s | 167 ops/sec (1.7 tx/sec) |
| Large values (1KB x 100) | 787ms | 0.1 MB/sec |
| Large values (10KB x 100) | 784ms | 1.2 MB/sec |
| Large values (100KB x 100) | 865ms | 11.3 MB/sec |
| Large values (1MB x 100) | 1.3s | 76.8 MB/sec |

### BBolt (Go)
| Benchmark | Time | Throughput |
|-----------|------|------------|
| Sequential writes (100K keys, 1 tx) | 251ms | 397,871 ops/sec |
| Sequential reads (100K keys) | 66ms | 1,517,306 ops/sec |
| Random reads (100K lookups) | 91ms | 1,101,069 ops/sec |
| Iterator scan (100K keys) | 2.7ms | 36,168,916 ops/sec |
| Mixed workload (10K ops, 70% read) | 1.8s | 5,448 ops/sec |
| Batch writes (1K tx, 100 ops/tx) | 822ms | 121,682 ops/sec (1,216.8 tx/sec) |
| Large values (1KB x 100) | 2.8ms | 34.2 MB/sec |
| Large values (10KB x 100) | 2.7ms | 366.9 MB/sec |
| Large values (100KB x 100) | 17.6ms | 555.0 MB/sec |
| Large values (1MB x 100) | 441ms | 226.8 MB/sec |

## Performance Comparison

| Benchmark | Thunder | BBolt | Ratio |
|-----------|---------|-------|-------|
| Sequential writes (1 tx) | 197K ops/sec | 397K ops/sec | 0.5x |
| Sequential reads | 2.5M ops/sec | 1.5M ops/sec | **1.7x** |
| Random reads | 1.4M ops/sec | 1.1M ops/sec | **1.2x** |
| Iterator scan | 96M ops/sec | 36M ops/sec | **2.7x** |
| Mixed workload | 8 ops/sec | 5,448 ops/sec | 0.001x |
| Batch writes (1K tx) | 1.7 tx/sec | 1,217 tx/sec | 0.001x |
| Large values (1KB) | 0.1 MB/sec | 34 MB/sec | 0.003x |
| Large values (1MB) | 77 MB/sec | 227 MB/sec | 0.3x |

## Analysis

### Where Thunder Excels
- **Read operations**: Thunder is 1.2-2.7x faster than BBolt for all read workloads
- **Iterator performance**: Thunder's iterator is 2.7x faster, suggesting efficient B+ tree traversal

### Critical Performance Issue
Thunder exhibits catastrophic performance degradation in any workload involving multiple transaction commits:

| Metric | Thunder | BBolt | Slowdown |
|--------|---------|-------|----------|
| Transactions per second | 1.7 tx/sec | 1,217 tx/sec | **716x slower** |
| Mixed workload ops/sec | 8 ops/sec | 5,448 ops/sec | **681x slower** |

### Root Cause Analysis

The performance pattern reveals a clear bottleneck in Thunder's **commit path**:

1. **Single large transaction**: Thunder is only 2x slower (506ms vs 251ms for 100K ops)
2. **Many small transactions**: Thunder is 716x slower (1.7 tx/sec vs 1,217 tx/sec)

This indicates the per-commit overhead is the problem, not the B+ tree operations themselves.

#### Likely Causes

1. **Excessive fsync calls**: Thunder likely calls `fsync()` on every commit, blocking until data is durably written to disk. Each fsync on Linux typically takes 5-15ms depending on storage.

2. **No write batching**: BBolt likely batches multiple pending transactions or uses more efficient sync strategies (fdatasync, write barriers, or async commits with group commit).

3. **Full file sync vs page sync**: Thunder may be syncing the entire file rather than just dirty pages.

4. **Memory mapping overhead**: The mmap-based design may require additional msync calls or page table manipulation on each commit.

#### Evidence from Large Value Tests

| Value Size | Thunder | BBolt | Ratio |
|------------|---------|-------|-------|
| 1KB | 0.1 MB/sec | 34 MB/sec | 340x slower |
| 10KB | 1.2 MB/sec | 367 MB/sec | 306x slower |
| 100KB | 11.3 MB/sec | 555 MB/sec | 49x slower |
| 1MB | 76.8 MB/sec | 227 MB/sec | 3x slower |

The performance gap narrows dramatically with larger values (340x → 3x), confirming that the overhead is **per-commit fixed cost**, not proportional to data size. When more data is written per commit, Thunder's throughput improves because the fixed commit overhead is amortized.

### Recommendations

1. **Implement group commit**: Batch multiple pending commits into a single fsync
2. **Use fdatasync instead of fsync**: Skip metadata sync when only data changed
3. **Add async/NoSync option**: Allow users to trade durability for performance
4. **Optimize mmap sync**: Use msync with MS_ASYNC for non-critical paths
5. **Investigate freelist management**: The freelist may be causing additional writes per commit
