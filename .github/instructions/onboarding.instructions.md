# Thunder Onboarding

## What is Thunder?

Thunder is a minimal, embedded, transactional key-value database engine in Rust, inspired by bbolt. It prioritizes correctness over performance and clarity over cleverness.

## File Structure

```
src/
├── lib.rs      // Public API re-exports and integration tests
├── db.rs       // Database open/close, persistence
├── tx.rs       // ReadTx and WriteTx transactions
├── btree.rs    // In-memory B+ tree implementation
├── bucket.rs   // Bucket namespacing for key isolation
├── mmap.rs     // Memory-mapped file I/O
├── page.rs     // Page constants and types
├── meta.rs     // Meta page serialization/validation
├── freelist.rs // Page allocation and free page tracking
└── error.rs    // Error types with detailed context
```

## Key Components

### Database (`db.rs`)

- `Database::open(path)` — Opens or creates database file
- Initializes two meta pages on new database
- Loads existing data into in-memory B+ tree on open
- `persist_tree()` — Serializes tree to disk after meta pages
- Strict error handling with explicit context for all I/O operations

### Transactions (`tx.rs`)

- **`ReadTx`** — Immutable reference to database, reads from B+ tree
  - `get()`, `iter()`, `range()` — Key-value access and iteration
  - `bucket()`, `bucket_exists()`, `list_buckets()` — Bucket access
- **`WriteTx`** — Mutable reference, uses pending tree for uncommitted changes
  - Changes staged in separate `BTree` + deletion list
  - `commit()` applies changes and persists to disk
  - Dropping without commit = automatic rollback
  - `create_bucket()`, `delete_bucket()` — Bucket management
  - `bucket_put()`, `bucket_get()`, `bucket_delete()` — Bucket operations

### B+ Tree (`btree.rs`)

- In-memory tree with 32-key node capacity (`LEAF_MAX_KEYS`, `BRANCH_MAX_KEYS`)
- `get()`, `insert()`, `remove()`, `iter()`, `range()`
- `Bound` enum — Range bounds (Unbounded, Included, Excluded)
- `BTreeIter` — Iterator over all key-value pairs in sorted order
- `BTreeRangeIter` — Iterator for range scans with start/end bounds
- Automatic node splitting on overflow (page splitting)
- Automatic rebalancing (borrow/merge) on underflow
- Keys ordered lexicographically
- Copy-on-write semantics via pending tree isolation

### Buckets (`bucket.rs`)

- Logical namespacing for key isolation (similar to bbolt/etcd)
- `BucketRef` — Read-only view of a bucket
- `BucketMut` — Mutable view for write transactions
- `BucketIter` — Iterator over bucket's key-value pairs
- `BucketRangeIter` — Range iterator scoped to a bucket
- `MAX_BUCKET_NAME_LEN = 255` — Maximum bucket name size
- Internal key format: `[prefix][name_len][name][user_key]`
- Bucket metadata prefix: `0x00`, data prefix: `0x01`

### Memory Mapping (`mmap.rs`)

- `Mmap::new(file, len)` — Creates read-only mapping via `libc::mmap`
- `page(id)` — Returns slice for specific page
- Thread-safe (`Send + Sync`)
- Requires `libc` crate (only external dependency)

### Meta Page (`meta.rs`)

- Stores: magic, version, page_size, txid, root, freelist, page_count, checksum
- `to_bytes()` / `from_bytes()` — Serialization with FNV-1a checksum
- `validate()` — Checks magic, version, page size
- **Meta page switching** — Alternates between page 0/1 for crash recovery

### FreeList (`freelist.rs`)

- Tracks freed pages available for reuse
- `free(page_id)` — Marks page as available
- `allocate()` — Returns freed page (lowest ID first)
- O(log n) operations via `BTreeSet`
- Automatic deduplication
- `to_bytes()` / `from_bytes()` — Compact serialization

### Page Constants (`page.rs`)

- `PAGE_SIZE = 4096` (4KB)
- `MAGIC = 0x54484E44` ("THND")
- `VERSION = 1`
- `PageId = u64`
- `PageType` enum: Meta, Freelist, Branch, Leaf

### Error Handling (`error.rs`)

- 17 specific error variants with context
- `FileOpen`, `FileSeek`, `FileRead`, `FileWrite`, `FileSync` — I/O errors with path/offset
- `Corrupted`, `InvalidMetaPage`, `BothMetaPagesInvalid` — Data integrity errors
- `EntryReadFailed` — Per-entry load errors
- `TxClosed`, `TxCommitFailed` — Transaction errors
- `BucketNotFound`, `BucketAlreadyExists`, `InvalidBucketName` — Bucket errors
- All errors preserve source for debugging

## Storage Format

```
┌─────────────────┐  Offset 0
│   Meta Page 0   │  4KB - txid even writes here
├─────────────────┤  Offset 4096
│   Meta Page 1   │  4KB - txid odd writes here
├─────────────────┤  Offset 8192
│   Data Section  │  entry_count (8B) + entries
│   (key-values)  │  Each: key_len(4B) + key + val_len(4B) + val
└─────────────────┘
```

## Design Decisions

1. **In-memory B+ tree** — Full tree loaded on open, persisted on commit
2. **Simple serialization** — Length-prefixed key-value pairs (not page-based yet)
3. **Meta page switching** — txid % 2 determines which page to write (crash recovery)
4. **Pending changes isolation** — WriteTx uses separate tree until commit (COW)
5. **Strict error handling** — No `?` operator, explicit match with context
6. **FreeList for space reclaim** — BTreeSet-based tracking for page reuse
7. **libc for mmap** — Only external dependency, required for memory mapping

## Transaction Guarantees

- Uncommitted writes are invisible to readers (copy-on-write)
- Commit is atomic (meta page swap)
- Drop without commit = rollback (pending changes discarded)
- Single writer enforced by Rust's `&mut` borrow
- Crash recovery via dual meta pages

## Running Tests

```bash
cargo test        # Run all tests (291 total)
cargo clippy      # Lint check
```
