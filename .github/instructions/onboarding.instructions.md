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
├── mmap.rs     // Memory-mapped file I/O
├── page.rs     // Page constants and types
├── meta.rs     // Meta page serialization/validation
├── freelist.rs // Page allocation (stub)
└── error.rs    // Error types and Result alias
```

## Key Components

### Database (`db.rs`)

- `Database::open(path)` — Opens or creates database file
- Initializes two meta pages on new database
- Loads existing data into in-memory B+ tree on open
- `persist_tree()` — Serializes tree to disk after meta pages

### Transactions (`tx.rs`)

- **`ReadTx`** — Immutable reference to database, reads from B+ tree
- **`WriteTx`** — Mutable reference, uses pending tree for uncommitted changes
  - Changes staged in separate `BTree` + deletion list
  - `commit()` applies changes and persists to disk
  - Dropping without commit = automatic rollback

### B+ Tree (`btree.rs`)

- In-memory tree with 32-key node capacity
- `get()`, `insert()`, `remove()`, `iter()`
- Automatic node splitting on overflow
- Automatic rebalancing (borrow/merge) on underflow
- Keys ordered lexicographically

### Memory Mapping (`mmap.rs`)

- `Mmap::new(file, len)` — Creates read-only mapping via `libc::mmap`
- `page(id)` — Returns slice for specific page
- Thread-safe (`Send + Sync`)
- Requires `libc` crate (only external dependency)

### Meta Page (`meta.rs`)

- Stores: magic, version, page_size, txid, root, freelist, page_count, checksum
- `to_bytes()` / `from_bytes()` — Serialization with FNV-1a checksum
- `validate()` — Checks magic, version, page size

### Page Constants (`page.rs`)

- `PAGE_SIZE = 4096` (4KB)
- `MAGIC = 0x54484E44` ("THND")
- `VERSION = 1`
- `PageId = u64`
- `PageType` enum: Meta, Freelist, Branch, Leaf

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
3. **Alternating meta pages** — txid % 2 determines which page to write
4. **Pending changes isolation** — WriteTx uses separate tree until commit
5. **libc for mmap** — Only external dependency, required for memory mapping

## Transaction Guarantees

- Uncommitted writes are invisible to readers
- Commit is atomic (meta page swap)
- Drop without commit = rollback (pending changes discarded)
- Single writer enforced by Rust's `&mut` borrow

## Running Tests

```bash
cargo test        # Run all tests (43 total)
cargo clippy      # Lint check
```
