# ⚡ thunder

**thunder** is a minimal, embedded, transactional key–value database engine written in **Rust**.  
It is heavily inspired by **bbolt** (the BoltDB fork used by etcd) and exists primarily as a **learning-focused yet correct database engine**.

This project prioritizes:

- correctness over performance
- clarity over cleverness
- explicit architecture over hidden magic

thunder is **not** intended to be a drop-in replacement for bbolt or a production-grade database (yet).  
It is a foundational storage engine designed to teach and solidify database internals in Rust.

---

## Table of Contents

- [Design Goals](#design-goals)
- [Non-Goals](#non-goals)
- [Core Concepts](#core-concepts)
- [High-Level Architecture](#high-level-architecture)
- [Transaction Model](#transaction-model)
- [Storage Model](#storage-model)
- [Planned Rust Architecture](#planned-rust-architecture)
- [Public API (Planned)](#public-api-planned)
- [Testing Strategy](#testing-strategy)
- [Development Roadmap](#development-roadmap)

---

## Design Goals

thunder is designed around the following principles:

1. **Embedded**
   - Runs in-process as a Rust library
   - No client/server model
   - No network layer

2. **Single-file storage**
   - Entire database stored in one file
   - File is memory-mapped (mmap)

3. **Deterministic behavior**
   - No background threads
   - No hidden compaction
   - No asynchronous mutation

4. **Strong correctness guarantees**
   - ACID transactions
   - Crash safety
   - Snapshot isolation for readers

5. **Beginner-friendly Rust**
   - Avoid complex lifetimes early
   - Avoid `unsafe` unless absolutely required
   - Prefer explicit ownership and borrowing

---

## Non-Goals

The following are explicitly **out of scope** (at least initially):

- SQL or query language
- Distributed consensus or replication
- Background compaction or GC threads
- Compression or encryption
- High write throughput optimization
- Lock-free or highly concurrent writes

thunder is a **storage primitive**, not a full database system.

---

## Core Concepts

To work on thunder, you must understand the following concepts:

### Embedded Key–Value Store

- Keys and values are arbitrary byte slices
- Data is ordered lexicographically by key
- Supports point lookups and range scans

### B+ Tree

- Balanced tree optimized for disk/page access
- Internal nodes store keys and child pointers
- Leaf nodes store keys and values
- All data lives in leaf nodes

### Pages

- Fixed-size blocks (e.g. 4KB)
- Database file is a sequence of pages
- Pages are addressed by page ID (offset-based)

### Memory-Mapped I/O (mmap)

- Database file is mapped into memory
- Pages are accessed as memory, not via read/write syscalls
- OS manages caching and flushing

### Copy-on-Write (COW)

- Pages are never modified in-place during writes
- Modified pages are copied to new locations
- Atomic root pointer swap on commit

### MVCC (Multi-Version Concurrency Control)

- Multiple read transactions allowed concurrently
- Only one write transaction at a time
- Readers see a consistent snapshot

---

## High-Level Architecture

```yaml
+------------------------+
| Database |
| (single mmap file) |
+-----------+------------+
|
v
+------------------------+
| Transactions |
| ReadTx | WriteTx |
+-----------+------------+
|
v
+------------------------+
| B+ Tree |
| Root → Branch → Leaf |
+-----------+------------+
|
v
+------------------------+
| Pages |
| Fixed-size blocks |
+-----------+------------+
|
v
+------------------------+
| Meta Pages & Freelist|
| Safety & Allocation |
+------------------------+
```

---

## Transaction Model

| Transaction Type | Concurrency | Mutability |
|------------------|-------------|------------|
| Read Transaction | Many        | Read-only |
| Write Transaction| One         | Read-write |

### Properties

- Readers never block other readers
- Writers are exclusive
- Writers do not modify existing pages
- Commit is atomic and crash-safe

---

## Storage Model

### Page Types (Planned)

- **Meta pages**
  - Store root page ID
  - Store transaction ID
  - Two copies for crash recovery

- **Branch pages**
  - Internal B+ tree nodes
  - Keys + child page IDs

- **Leaf pages**
  - Store key–value pairs

- **Freelist pages**
  - Track unused pages
  - Prevent reuse while readers exist

---

## Planned Rust Architecture

```rust
src/
├── lib.rs // Public API surface
├── db.rs // Database open/close
├── tx.rs // Transactions
├── page.rs // Page layout & helpers
├── tree.rs // B+ tree logic
├── freelist.rs // Page allocation
├── meta.rs // Meta page handling
└── error.rs // Error types
```

The project is implemented as a **library crate**.

---

## Public API (Planned)

This API is intentionally minimal and mirrors the conceptual model.

```rust
pub struct Database;

impl Database {
    pub fn open(path: &str) -> Result<Self>;
    pub fn read_tx(&self) -> ReadTx;
    pub fn write_tx(&mut self) -> WriteTx;
}

pub struct ReadTx<'a>;

impl ReadTx<'_> {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
}

pub struct WriteTx<'a>;

impl WriteTx<'_> {
    pub fn put(&mut self, key: &[u8], value: &[u8]);
    pub fn delete(&mut self, key: &[u8]);
    pub fn commit(self) -> Result<()>;
}
```

Initial versions may:

- Support a single unnamed bucket

- Panic on page overflow

- Omit iterators

These limitations are intentional for simplicity.

## Testing Strategy

Correctness is validated via deterministic tests.

### Unit Tests

- Page encoding/decoding

- B+ tree insertion and lookup

- Meta page validation

### Integration Tests

```rust
#[test]
fn put_then_get() {
    let mut db = Database::open("test.db").unwrap();

    let mut wtx = db.write_tx();
    wtx.put(b"hello", b"world");
    wtx.commit().unwrap();

    let rtx = db.read_tx();
    assert_eq!(rtx.get(b"hello"), Some(b"world".to_vec()));
}
```

Crash-safety tests will be added later.

## Development Roadmap

### Phase 1 — Foundations

- Database open/close

- Memory mapping

- Fixed-size pages

- Single B+ tree

- Read & write transactions

### Phase 2 — Correctness

- Page splitting

- Copy-on-write

- Meta page switching

- Basic freelist

### Phase 3 — Usability

- Buckets

- Iterators

- Range scans

### Phase 4 — Robustness

- Crash recovery tests

- Stress testing

- Benchmarks vs bbolt
