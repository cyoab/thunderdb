# Durability & Crash Safety

This document describes ThunderDB's durability guarantees, crash recovery mechanisms, and the syscalls used to ensure data persistence.

## Table of Contents

- [Durability Contract](#durability-contract)
- [Syscalls Used](#syscalls-used)
- [Crash Recovery Mechanisms](#crash-recovery-mechanisms)
- [Power Loss Scenarios](#power-loss-scenarios)
- [Durability Modes](#durability-modes)
- [OS × Filesystem × Durability Matrix](#os--filesystem--durability-matrix)
- [Configuration Guide](#configuration-guide)
- [Comparison with bbolt](#comparison-with-bbolt)

---

## Durability Contract

### When `commit()` Returns `Ok(())`

When a write transaction's `commit()` method returns successfully, ThunderDB guarantees:

1. **All data is persisted to stable storage** - The data section containing your key-value pairs has been written and synced to disk.

2. **The meta page is updated and synced** - A new meta page with an incremented transaction ID has been written and synced.

3. **Crash safety is established** - If a power loss or system crash occurs immediately after `commit()` returns, the database will recover to this exact state upon restart.

4. **No partial transactions** - Either all changes from the transaction are visible, or none are. There is no intermediate state.

```rust
let mut tx = db.write_tx();
tx.put(b"key", b"value");
tx.commit()?;  // <-- When this returns Ok(()), data is durable

// Power loss here: data is safe
```

### When `commit()` Returns `Err(...)`

If `commit()` returns an error:

- No changes from the transaction are persisted
- The database remains in the state before the transaction began
- The transaction is implicitly rolled back

---

## Syscalls Used

ThunderDB uses the following system calls for durability:

### Unix Systems (Linux, macOS, BSD)

| Syscall | Purpose | Location |
|---------|---------|----------|
| `fdatasync(fd)` | Flush file data to disk (excluding metadata) | After data writes, after meta page writes |
| `pwrite(fd, buf, count, offset)` | Positioned write (no seek required) | Data section writes, overflow pages |
| `write(fd, buf, count)` | Sequential write | Meta page writes |
| `lseek(fd, offset, whence)` | Seek to position | Before meta page writes |

### Non-Unix Systems (Windows)

| Syscall | Purpose | Location |
|---------|---------|----------|
| `FlushFileBuffers()` via `sync_all()` | Flush all file data and metadata | Fallback when `fdatasync` unavailable |

### Why `fdatasync` Instead of `fsync`?

ThunderDB uses `fdatasync()` rather than `fsync()` for performance:

- **`fsync()`**: Flushes data AND file metadata (size, timestamps, permissions)
- **`fdatasync()`**: Flushes data only, skipping metadata unless required for data retrieval

Since ThunderDB's crash recovery does not depend on file metadata (only on data content and checksums), `fdatasync()` provides equivalent safety with better performance (~10-30% faster on typical workloads).

**Implementation** (`src/db.rs:1728-1749`):

```rust
fn fdatasync(file: &File) -> Result<()> {
    #[cfg(unix)]
    {
        let ret = unsafe { libc::fdatasync(file.as_raw_fd()) };
        if ret != 0 {
            return Err(Error::FileSync { ... });
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        file.sync_all()  // Fallback to fsync equivalent
    }
}
```

---

## Crash Recovery Mechanisms

ThunderDB employs multiple mechanisms to ensure crash safety:

### 1. Dual Meta Pages (Atomic Commit Pattern)

The database file contains two meta pages at fixed offsets:

| Meta Page | Offset | Purpose |
|-----------|--------|---------|
| Meta 0 | 0 (page 0) | Primary meta page |
| Meta 1 | 32KB (page 1) | Alternate meta page |

**Commit sequence:**

1. Write all data to the data section
2. Call `fdatasync()` to ensure data is on disk
3. Write new meta page to the **alternate** slot (not the current one)
4. Call `fdatasync()` to ensure meta page is on disk
5. The transaction is now committed

**Recovery on startup:**

1. Read both meta pages
2. Validate checksums (FNV-1a) for each
3. Select the valid meta page with the **higher transaction ID**
4. This meta page points to the consistent data state

**Why this works:**

- If crash occurs during step 1-2: Old meta page is still valid, points to old data
- If crash occurs during step 3-4: Either old or new meta is valid
- If crash occurs after step 4: New meta is valid, points to new data

**Meta page structure** (`src/meta.rs:14-38`):

```rust
pub struct Meta {
    pub magic: u32,          // 0x54_48_4E_44 ("THND")
    pub version: u32,        // Format version
    pub page_size: u32,      // Page size in bytes
    pub txid: u64,           // Transaction ID (incremented each commit)
    pub root: PageId,        // Root of B+ tree
    pub freelist: PageId,    // Freelist page
    pub page_count: u64,     // Total pages
    pub checksum: u64,       // FNV-1a checksum
    pub checkpoint_lsn: Lsn, // WAL checkpoint position
    pub checkpoint_timestamp: u64,
    pub checkpoint_entry_count: u64,
}
```

### 2. Write-Ahead Log (WAL) - Optional

When enabled, the WAL provides additional durability guarantees:

**WAL record types:**

- `TxBegin` - Transaction started
- `Put` - Key-value insertion
- `Delete` - Key deletion
- `TxCommit` - Transaction committed
- `TxAbort` - Transaction aborted
- `Checkpoint` - Checkpoint marker

**WAL structure:**

```
Segment file: wal_XXXXXX.log
├── Header (64 bytes)
│   ├── Magic: 0x574C_4F47 ("WLOG")
│   ├── Version: 1
│   ├── Segment ID
│   └── First LSN
└── Records
    └── [length:4][type:1][crc32:4][payload...]
```

**Recovery with WAL:**

1. Load the valid meta page (dual meta page recovery)
2. Read `checkpoint_lsn` from meta page
3. Replay all WAL records after `checkpoint_lsn`
4. Database is restored to the state at the last `TxCommit`

### 3. Checksums

All critical data structures are protected by checksums:

| Structure | Checksum Algorithm | Verified On |
|-----------|-------------------|-------------|
| Meta page | FNV-1a (64-bit) | Every database open |
| WAL records | CRC32 | Replay |
| Data integrity | CRC32 (optional) | Read operations |

---

## Power Loss Scenarios

### Scenario 1: Power Loss During Data Write

**State:** Transaction has modified in-memory tree, started writing to data section

**Outcome:**
- Meta page still points to old data section end
- Partially written data is beyond the recorded data boundary
- On recovery: Old meta page is valid, database returns to pre-transaction state

**Data loss:** Uncommitted transaction only (expected behavior)

### Scenario 2: Power Loss During Meta Page Write

**State:** Data section written and synced, meta page write in progress

**Outcome:**
- Old meta page remains valid (checksum intact)
- New meta page has corrupted checksum (partial write)
- On recovery: Old meta page selected, database returns to pre-transaction state

**Data loss:** Uncommitted transaction only (expected behavior)

### Scenario 3: Power Loss After fdatasync But Before Return

**State:** Meta page written and synced, commit() about to return

**Outcome:**
- New meta page is valid with higher txid
- On recovery: New meta page selected, transaction is preserved

**Data loss:** None (transaction committed)

### Scenario 4: Power Loss With WAL Enabled

**State:** WAL contains committed transaction, checkpoint not yet performed

**Outcome:**
- Meta page points to last checkpoint state
- WAL contains records since checkpoint
- On recovery: Checkpoint state loaded, WAL replayed to recover transactions

**Data loss:** None (WAL replayed)

---

## Durability Modes

ThunderDB supports multiple durability configurations through `SyncPolicy`:

### `SyncPolicy::Immediate` (Safest)

```rust
DatabaseOptions {
    wal_enabled: true,
    wal_sync_policy: SyncPolicy::Immediate,
    ..Default::default()
}
```

- `fdatasync()` after every WAL write
- Maximum durability, minimum performance
- **Recommended for:** Financial transactions, critical data

### `SyncPolicy::Batched(Duration)` (Balanced) - Default

```rust
DatabaseOptions {
    wal_enabled: true,
    wal_sync_policy: SyncPolicy::Batched(Duration::from_millis(10)),
    ..Default::default()
}
```

- Groups multiple commits into single `fdatasync()`
- Configurable batch window (default: 10ms)
- Good balance of durability and throughput
- **Recommended for:** Most production workloads

### `SyncPolicy::None` (Testing Only)

```rust
DatabaseOptions {
    wal_sync_policy: SyncPolicy::None,
    ..Default::default()
}
```

- No `fdatasync()` on WAL writes
- **WARNING:** Data loss on crash
- **Use only for:** Unit tests, benchmarks

### Without WAL (Default Configuration)

```rust
DatabaseOptions::default()  // wal_enabled: false
```

- Relies solely on dual meta pages
- `fdatasync()` after data writes and meta page writes
- Good durability without WAL overhead
- **Recommended for:** Read-heavy workloads, non-critical data

---

## OS × Filesystem × Durability Matrix

This matrix describes the expected durability behavior across different configurations:

### Linux

| Filesystem | fdatasync Behavior | Durability Notes |
|------------|-------------------|------------------|
| **ext4** (default) | Reliable | Full durability with default mount options |
| **ext4** (data=writeback) | Reliable | Data may be reordered, but fdatasync forces order |
| **ext4** (barrier=0) | **UNSAFE** | Barriers disabled, no durability guarantee |
| **XFS** | Reliable | Full durability |
| **Btrfs** | Reliable | Copy-on-write may have higher latency |
| **ZFS** | Reliable | Full durability, checksums redundant |
| **tmpfs** | **UNSAFE** | RAM-based, no persistence |
| **NFS** | Variable | Depends on server and mount options |

### macOS

| Filesystem | fdatasync Behavior | Durability Notes |
|------------|-------------------|------------------|
| **APFS** | Reliable | Full durability |
| **HFS+** | Reliable | Full durability |
| **Network volumes** | Variable | Depends on server |

### Windows

| Filesystem | sync_all Behavior | Durability Notes |
|------------|-------------------|------------------|
| **NTFS** | Reliable | Full durability |
| **ReFS** | Reliable | Full durability |
| **FAT32** | **Limited** | No journaling, higher risk |
| **Network drives** | Variable | Depends on server |

### Storage Hardware Considerations

| Storage Type | Notes |
|--------------|-------|
| **Enterprise SSD** | Reliable with power-loss protection |
| **Consumer SSD** | Generally reliable, some models lack PLP |
| **NVMe** | Reliable, use `nvme_optimized()` config |
| **HDD** | Reliable but slower fdatasync |
| **Battery-backed RAID** | Reliable, may report sync complete early |
| **Cloud block storage** | Reliable (AWS EBS, GCP PD, Azure Disk) |

### Configuration Recommendations

| Use Case | Configuration |
|----------|---------------|
| **Production (critical data)** | `with_wal()` + `SyncPolicy::Immediate` |
| **Production (balanced)** | `with_wal()` + `SyncPolicy::Batched(10ms)` |
| **Production (read-heavy)** | Default (no WAL) |
| **Development** | Default |
| **Testing** | `SyncPolicy::None` (temporary only) |
| **NVMe storage** | `nvme_optimized()` |
| **Large values (>10KB)** | `large_value_optimized()` |

---

## Configuration Guide

### Enabling Maximum Durability

```rust
use thunderdb::{Database, DatabaseOptions, SyncPolicy};

let options = DatabaseOptions {
    wal_enabled: true,
    wal_sync_policy: SyncPolicy::Immediate,
    checkpoint_interval_secs: 60,  // Frequent checkpoints
    ..Default::default()
};

let db = Database::open_with_options("critical.db", options)?;
```

### Enabling Balanced Durability (Recommended)

```rust
use thunderdb::{Database, DatabaseOptions};

let db = Database::open_with_options("app.db", DatabaseOptions::with_wal())?;
```

### High-Performance Configuration (Accept Some Risk)

```rust
use thunderdb::{Database, DatabaseOptions, SyncPolicy};
use std::time::Duration;

let options = DatabaseOptions {
    wal_enabled: true,
    wal_sync_policy: SyncPolicy::Batched(Duration::from_millis(100)),
    checkpoint_interval_secs: 600,
    ..DatabaseOptions::nvme_optimized()
};

let db = Database::open_with_options("perf.db", options)?;
```

---

## Comparison with bbolt

ThunderDB's durability model is inspired by and comparable to [bbolt](https://github.com/etcd-io/bbolt):

| Feature | ThunderDB | bbolt |
|---------|-----------|-------|
| **Dual meta pages** | Yes | Yes |
| **Atomic commits** | Yes | Yes |
| **Sync syscall** | `fdatasync` | `fdatasync` |
| **Meta checksum** | FNV-1a (64-bit) | FNV-1a (64-bit) |
| **WAL support** | Optional | No |
| **Group commit** | Yes (with WAL) | No |
| **Configurable sync** | Yes | Limited |

### Key Differences

1. **WAL Support**: ThunderDB optionally supports WAL for faster commits with equivalent durability. bbolt relies solely on dual meta pages.

2. **Group Commit**: ThunderDB can batch multiple transactions into a single fsync when WAL is enabled, improving throughput.

3. **Configurable Sync Policy**: ThunderDB allows tuning the sync behavior for different durability/performance tradeoffs.

4. **Data Checksums**: ThunderDB uses CRC32 for WAL records, providing additional corruption detection.

---

## Verifying Durability

### Manual Verification

You can verify that commits are durable by checking the transaction ID:

```rust
let mut tx = db.write_tx();
tx.put(b"key", b"value");
tx.commit()?;

// Re-open database (simulating crash recovery)
drop(db);
let db = Database::open("test.db")?;

let tx = db.read_tx();
assert_eq!(tx.get(b"key"), Some(b"value".to_vec()));
```

### Crash Testing

For production systems, consider using crash testing tools:

- **Linux**: `echo 1 > /proc/sys/kernel/sysrq` then `echo c > /proc/sysrq-trigger`
- **Virtualization**: VM snapshot/restore during writes
- **Power control**: Managed PDU for physical power cycling

---

## Technical References

- `src/db.rs:1728-1749` - fdatasync implementation
- `src/meta.rs` - Meta page structure and checksums
- `src/wal.rs` - Write-ahead log implementation
- `src/checkpoint.rs` - Checkpoint management
- `src/group_commit.rs` - Group commit batching

---

## Summary

ThunderDB provides production-grade durability through:

1. **Dual meta pages** with atomic transaction ID updates
2. **FNV-1a checksums** for corruption detection
3. **fdatasync** (not fsync) for efficient persistence
4. **Optional WAL** with configurable sync policies
5. **Group commit** for high-throughput workloads

When `commit()` returns `Ok(())`, your data is safe.
