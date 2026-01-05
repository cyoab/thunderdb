# ThunderDB

ThunderDB is an embedded, transactional key-value database written in Rust. It provides ACID transactions with crash-safe commits, MVCC concurrency, and a simple API for storing arbitrary byte sequences.

## Status

ThunderDB is under active development. While the core functionality is implemented and tested, it has not yet been deployed in production environments. For mission-critical applications, consider established alternatives such as [SQLite](https://sqlite.org/), [RocksDB](https://rocksdb.org/), or [BBolt](https://github.com/etcd-io/bbolt).

## Design Goals

- **Correctness first** — ACID guarantees with verified crash recovery
- **Predictable performance** — Consistent latency over time
- **Minimal complexity** — Small codebase, few dependencies
- **Well-documented internals** — File format and durability semantics are specified

## Features

- **ACID transactions** — Atomic commits with crash recovery via dual meta pages
- **MVCC concurrency** — Multiple readers, single writer without blocking
- **Single-file storage** — Entire database in one file
- **Configurable durability** — Immediate fsync, batched, or application-controlled
- **Buckets** — Logical namespaces for data organization
- **Nested buckets** — Hierarchical structure up to 16 levels
- **Range queries** — Forward iteration with range bounds
- **Write-ahead log** — Optional WAL for group commits and faster durability
- **Checksums** — FNV-1a for metadata, CRC32 for data integrity

## Documentation

| Document | Description |
|----------|-------------|
| [docs/durability.md](docs/durability.md) | Durability guarantees, syscalls, crash recovery |
| [docs/file-format.md](docs/file-format.md) | Binary format specification, versioning |

## Quick Start

```rust
use thunderdb::Database;

fn main() -> thunderdb::Result<()> {
    let mut db = Database::open("my.db")?;

    // Write transaction
    {
        let mut tx = db.write_tx();
        tx.put(b"key", b"value");
        tx.commit()?;  // Data is durable when this returns Ok
    }

    // Read transaction
    {
        let tx = db.read_tx();
        assert_eq!(tx.get(b"key"), Some(b"value".to_vec()));
    }

    Ok(())
}
```

## Durability

When `commit()` returns `Ok(())`:

1. All data has been written to the file
2. `fdatasync()` has been called to flush to stable storage
3. The transaction is recoverable after power loss

ThunderDB uses dual meta pages for atomic commits. On recovery, the meta page with the higher transaction ID and valid checksum is selected. See [docs/durability.md](docs/durability.md) for details.

### Sync Policies

```rust
use thunderdb::{Database, DatabaseOptions, SyncPolicy};
use std::time::Duration;

// Maximum durability (default)
let db = Database::open("safe.db")?;

// With WAL and batched sync (higher throughput)
let opts = DatabaseOptions {
    wal_enabled: true,
    wal_sync_policy: SyncPolicy::Batched(Duration::from_millis(10)),
    ..Default::default()
};
let db = Database::open_with_options("batched.db", opts)?;
```

## File Format

ThunderDB uses a page-based format with these characteristics:

| Property | Value |
|----------|-------|
| Magic number | `0x54484E44` ("THND") |
| Format version | 3 |
| Default page size | 32 KB |
| Supported page sizes | 4K, 8K, 16K, 32K, 64K |
| Byte order | Little-endian |

The format is documented in [docs/file-format.md](docs/file-format.md).

## Buckets

```rust
let mut tx = db.write_tx();

tx.create_bucket(b"users")?;
tx.bucket_put(b"users", b"alice", b"data")?;
tx.commit()?;

let tx = db.read_tx();
let value = tx.bucket_get(b"users", b"alice");
```

### Nested Buckets

```rust
let mut tx = db.write_tx();

tx.create_bucket(b"config")?;
tx.create_nested_bucket(b"config", b"network")?;
tx.nested_bucket_put(b"config", b"network", b"host", b"localhost")?;
tx.commit()?;
```

## Bulk Operations

```rust
let mut tx = db.write_tx();

let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10_000)
    .map(|i| (format!("key_{i}").into_bytes(), format!("val_{i}").into_bytes()))
    .collect();

tx.batch_put(entries);
tx.commit()?;
```

Bulk operations use parallel serialization for batches ≥100 entries.

## Performance

Preliminary benchmarks show competitive read performance. Write performance varies by workload.

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Sequential reads | ~2.6M ops/sec | In-memory B+ tree |
| Random reads | ~1.1M ops/sec | — |
| Iterator scan | ~78M ops/sec | Zero-copy iteration |
| Sequential writes | ~590K ops/sec | Append-only path |
| Bulk writes | ~1M ops/sec | With parallel serialization |

These numbers are from a single machine and may not reflect your workload. See [bench.md](bench.md) for methodology.

## Limitations

- **No compaction** — Deleted data is not reclaimed automatically
- **No encryption** — Data stored in plaintext
- **No compression** — Values stored as-is
- **Forward-only iteration** — No reverse or bidirectional cursors
- **Single writer** — Write transactions are serialized

Encryption and compression are left to the application layer by design.

## Testing

```bash
# Unit and integration tests
cargo test

# With failpoint testing (crash simulation)
cargo test --features failpoint

# Clippy and formatting
cargo clippy --all-features
cargo fmt --check
```

The test suite includes crash safety tests with failpoint injection at:

- Before/after data writes
- Before/after meta page writes  
- Before/after fsync

## Building

```bash
cargo build --release
```

### Feature Flags

| Flag | Description |
|------|-------------|
| `failpoint` | Enable crash testing infrastructure |
| `io_uring` | Linux io_uring backend (experimental) |
| `no_checksum` | Disable data checksums for max throughput |

## Architecture

```
src/
├── db.rs         # Database core, persistence
├── tx.rs         # Transaction implementation
├── btree.rs      # In-memory B+ tree
├── bucket.rs     # Bucket management
├── meta.rs       # Meta page handling
├── wal.rs        # Write-ahead log
├── checkpoint.rs # WAL checkpointing
├── overflow.rs   # Large value storage
├── mmap.rs       # Memory-mapped I/O
├── failpoint.rs  # Crash testing (feature-gated)
└── error.rs      # Error types
```

## Dependencies

- `libc` — System calls
- `crc32fast` — SIMD-accelerated checksums
- `nix` — Unix file operations
- `rayon` — Parallel bulk operations

## License

MIT
