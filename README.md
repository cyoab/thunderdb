# ⚡ ThunderDB

**ThunderDB** is a high-performance, embedded, transactional key-value database engine written in Rust. Keys and values are arbitrarily-sized byte streams. Optimized for fast, low-latency storage such as flash drives and high-speed disk drives, ThunderDB exploits the full potential of high read/write rates offered by flash or RAM.

Inspired by [BBolt](https://github.com/etcd-io/bbolt) and [RocksDB](https://rocksdb.org/), ThunderDB delivers **best-in-class read performance** — outperforming RocksDB, Sled, and BBolt on sequential reads, random reads, and iterator scans.

> ⚠️ **Work in Progress**: ThunderDB is still under active development and showing promising benchmark results. However, more comprehensive benchmarking is required and the project continues to evolve. For battle-tested, production-ready embedded databases, consider [SQLite](https://sqlite.org/), [RocksDB](https://rocksdb.org/), or [BBolt](https://github.com/etcd-io/bbolt).

## When to Use Thunder

✅ **ThunderDB is ideal for:**
- **Read-heavy workloads** — 2.6M sequential reads/sec, 1.1M random reads/sec
- **Range scans & analytics** — 78.6M iterator ops/sec (19× faster than RocksDB)
- **Document storage (10-100KB values)** — 484-642 MB/sec throughput
- **Embedded applications** — Single-file storage, minimal dependencies
- **Low-latency storage** — Optimized for flash drives and high-speed disks

❌ **Consider alternatives for:**
- **Write-heavy workloads** — RocksDB is 1.9× faster for bulk writes
- **Mixed read/write workloads** — Sled's lock-free architecture is 3.4× faster
- **Very large values (1MB+)** — Sled achieves 1.8× better throughput
- **Production-critical systems** — Use battle-tested solutions like SQLite or RocksDB

## Features

- **Embedded** — Runs in-process as a Rust library, no server required
- **Single-file storage** — Entire database in one file
- **ACID transactions** — Full durability with crash-safe commits
- **MVCC** — Multiple concurrent readers, single writer
- **Buckets** — Logical namespaces for organizing data
- **Nested buckets** — Hierarchical bucket organization (up to 16 levels deep)
- **Range queries** — Efficient iteration and range scans
- **Zero-copy reads** — `get_ref()` API returns references without allocation
- **Bloom filter** — Fast rejection of non-existent keys (8.5M ops/sec)
- **CRC32 checksums** — Data integrity verification with SIMD acceleration
- **Parallel writes** — Bulk operations use rayon for multi-core throughput (1M+ ops/sec)
- **Minimal dependencies** — Only `libc`, `crc32fast`, `nix`, and `rayon`

## Performance

Thunder delivers **best-in-class read performance** compared to RocksDB, Sled, and BBolt:

### Read Performance (Thunder's Strength)

| Benchmark | Thunder | RocksDB | Sled | BBolt | Winner |
|-----------|---------|---------|------|-------|--------|
| Sequential reads | **2.6M** | 624K | 214K | 1.5M | **Thunder 4.2×** |
| Random reads | **1.1M** | 577K | 539K | 955K | **Thunder 1.9×** |
| Iterator scan | **78.6M** | 4.1M | 957K | 27.1M | **Thunder 19×** |

### Write Performance

| Benchmark | Thunder | RocksDB | Sled | BBolt | Winner |
|-----------|---------|---------|------|-------|--------|
| Sequential writes | 590K | **1.1M** | 144K | 315K | RocksDB 1.9× |
| Mixed workload | 5.4K | 6.6K | **18.3K** | 5.1K | Sled 3.4× |
| Batch tx/sec | 1,129 | **1,663** | 1,044 | 1,214 | RocksDB |

### Large Value Throughput (MB/sec)

| Size | Thunder | RocksDB | Sled | BBolt | Winner |
|------|---------|---------|------|-------|--------|
| 10KB | **484** | 275 | 272 | 115 | **Thunder 1.8×** |
| 100KB | **642** | 416 | 434 | 244 | **Thunder 1.5×** |
| 1MB | 230 | 211 | **418** | 207 | Sled 1.8× |

See [bench.md](bench.md) for full benchmark details and methodology.

## Quick Start

```rust
use thunderdb::Database;

fn main() -> thunderdb::Result<()> {
    // Open or create a database
    let mut db = Database::open("my.db")?;

    // Write data
    {
        let mut tx = db.write_tx();
        tx.put(b"hello", b"world");
        tx.put(b"foo", b"bar");
        tx.commit()?;
    }

    // Read data
    {
        let tx = db.read_tx();
        assert_eq!(tx.get(b"hello"), Some(b"world".to_vec()));
    }

    Ok(())
}
```

## Buckets

Organize data into logical namespaces:

```rust
let mut tx = db.write_tx();

// Create buckets
tx.create_bucket(b"users")?;
tx.create_bucket(b"posts")?;

// Write to buckets
tx.bucket_put(b"users", b"alice", b"data")?;
tx.bucket_put(b"posts", b"post1", b"content")?;

tx.commit()?;
```

## Nested Buckets

Create hierarchical bucket structures for complex data organization:

```rust
let mut tx = db.write_tx();

// Create parent bucket
tx.create_bucket(b"config")?;

// Create nested buckets
tx.create_nested_bucket(b"config", b"network")?;
tx.create_nested_bucket(b"config", b"storage")?;

// Write to nested buckets
tx.nested_bucket_put(b"config", b"network", b"host", b"localhost")?;
tx.nested_bucket_put(b"config", b"network", b"port", b"8080")?;

// Create deeply nested buckets (up to 16 levels)
tx.create_nested_bucket_at_path(&[b"config", b"storage"], b"cache")?;
tx.nested_bucket_put_at_path(&[b"config", b"storage", b"cache"], b"size", b"1GB")?;

tx.commit()?;

// Read from nested buckets
let rtx = db.read_tx();
let network = rtx.nested_bucket(b"config", b"network")?;
assert_eq!(network.get(b"host"), Some(&b"localhost"[..]));

// List nested buckets
let children = rtx.list_nested_buckets(b"config")?;
assert!(children.contains(&b"network".to_vec()));
```

## Bulk Operations

For high-throughput writes, use the batch APIs which leverage parallel processing:

```rust
let mut tx = db.write_tx();

// Bulk insert (parallelized for batches >= 100 entries)
let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000)
    .map(|i| (format!("key_{i}").into_bytes(), format!("value_{i}").into_bytes()))
    .collect();

tx.batch_put(entries);
tx.commit()?;
```

**Bulk write throughput:**
| Batch Size | Throughput |
|------------|------------|
| 1,000 entries | ~720K ops/sec |
| 10,000 entries | ~910K ops/sec |
| 100,000 entries | ~1.08M ops/sec |

## Limitations

ThunderDB has some limitations compared to mature solutions:

- **No cursor API** — Only forward iteration is currently supported
- **No compaction** — Deleted data is not reclaimed until full rewrite
- **No encryption** — Data is stored in plaintext
- **No compression** — Values are stored as-is
- **Limited testing** — Not yet battle-tested in production environments

### Design Philosophy

Encryption and compression are intentional design choices. ThunderDB serves as a **storage primitive** — it is the responsibility of the application or a higher-level system to handle encryption, compression, and other data transformations. This keeps the core database lean and focused on performance.

### Roadmap

Planned features for future releases:
- **Cursor API** — Bidirectional iteration with seek support
- **Compaction** — Reclaim space from deleted entries

For production use cases requiring stability and robustness, please use established solutions like SQLite, RocksDB, or BBolt.

## Architecture

```
src/
├── lib.rs        # Public API
├── db.rs         # Database open/close, persistence
├── tx.rs         # Read and write transactions
├── btree.rs      # In-memory B+ tree
├── bucket.rs     # Bucket management
├── bloom.rs      # Bloom filter for fast negative lookups
├── overflow.rs   # Large value handling
├── page.rs       # Page layout constants
├── meta.rs       # Meta page handling
├── mmap.rs       # Memory-mapped I/O
├── ivec.rs       # Inline vector optimization
├── concurrent.rs # Parallel write support (rayon)
└── error.rs      # Error types
```

### Key Optimizations

1. **In-memory B+ tree** — The entire tree lives in memory for fast reads
2. **Append-only writes** — New entries are appended, enabling fast commits
3. **fdatasync** — Uses `fdatasync()` instead of `fsync()` to reduce latency
4. **Zero-copy reads** — `get_ref()` returns references without allocation
5. **Bloom filter** — Fast rejection of non-existent keys
6. **Direct overflow format** — Large values use compact storage (12 bytes overhead)
7. **SIMD checksums** — CRC32 with hardware acceleration (~10 GB/s)
8. **pwrite** — Positioned writes avoid seek syscalls
9. **Parallel serialization** — Bulk writes use rayon for multi-core data preparation

## Building

```bash
cargo build --release
```

## Testing

```bash
cargo test
```

## Running Benchmarks

```bash
# Build all benchmarks
cd bench
cargo build --release

# Run Thunder benchmark
./target/release/thunder_bench

# Run Sled benchmark
./target/release/sled_bench

# Run RocksDB benchmark
./target/release/rocksdb_bench

# Run BBolt benchmark (Go)
go run bbolt_bench.go
```

## License

MIT
