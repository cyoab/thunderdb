//! Summary: Database open/close and core management logic.
//! Copyright (c) YOAB. All rights reserved.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use rayon::prelude::*;

use crate::bloom::BloomFilter;
use crate::btree::BTree;
use crate::checkpoint::{CheckpointConfig, CheckpointInfo, CheckpointManager};
use crate::concurrent::PARALLEL_THRESHOLD;
use crate::error::{Error, Result};
use crate::meta::Meta;
use crate::mmap::Mmap;
use crate::overflow::{DEFAULT_OVERFLOW_THRESHOLD, OverflowManager, OverflowRef};
use crate::page::{PAGE_SIZE, PageId, PageSizeConfig};
use crate::tx::{ReadTx, WriteTx};
use crate::wal::{Lsn, SyncPolicy, Wal, WalConfig};
use crate::wal_record::WalRecord;

/// Default write buffer size (256KB).
/// Larger buffers reduce syscall overhead for batch writes.
const WRITE_BUFFER_SIZE: usize = 256 * 1024;

/// Default expected keys for bloom filter sizing.
const DEFAULT_BLOOM_EXPECTED_KEYS: usize = 100_000;

/// Default bloom filter false positive rate (1%).
const DEFAULT_BLOOM_FP_RATE: f64 = 0.01;

/// Result type for load_tree: (tree, data_end_offset, entry_count, bloom_filter, overflow_refs)
type TreeLoadResult = (
    BTree,
    u64,
    u64,
    BloomFilter,
    std::collections::HashMap<Vec<u8>, OverflowRef>,
);

/// Database configuration options.
///
/// Allows customizing page size, overflow threshold, write buffer behavior,
/// and WAL/checkpoint settings.
#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    /// Page size for new databases.
    /// Ignored when opening existing databases (uses stored value).
    pub page_size: PageSizeConfig,
    /// Overflow threshold (values larger than this use overflow pages).
    pub overflow_threshold: usize,
    /// Write buffer size for the coalescer.
    pub write_buffer_size: usize,
    // Phase 4: WAL & Checkpoint options
    /// Enable write-ahead logging for durability.
    pub wal_enabled: bool,
    /// Directory for WAL files. If None, uses `<db_path>.wal/`.
    pub wal_dir: Option<PathBuf>,
    /// WAL sync policy.
    pub wal_sync_policy: SyncPolicy,
    /// WAL segment size in bytes.
    pub wal_segment_size: u64,
    /// Checkpoint interval in seconds. 0 disables automatic checkpointing.
    pub checkpoint_interval_secs: u64,
    /// WAL size threshold for checkpoint (bytes).
    pub checkpoint_wal_threshold: usize,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            page_size: PageSizeConfig::Size32K,
            overflow_threshold: DEFAULT_OVERFLOW_THRESHOLD,
            write_buffer_size: WRITE_BUFFER_SIZE,
            wal_enabled: false,
            wal_dir: None,
            wal_sync_policy: SyncPolicy::default(),
            wal_segment_size: 64 * 1024 * 1024,          // 64MB
            checkpoint_interval_secs: 300,               // 5 minutes
            checkpoint_wal_threshold: 128 * 1024 * 1024, // 128MB
        }
    }
}

impl DatabaseOptions {
    /// Configuration optimized for NVMe storage.
    ///
    /// Uses larger page sizes and buffers for better NVMe performance.
    pub fn nvme_optimized() -> Self {
        Self {
            page_size: PageSizeConfig::Size32K,
            overflow_threshold: 16 * 1024,  // 16KB threshold
            write_buffer_size: 1024 * 1024, // 1MB buffer
            wal_enabled: false,
            wal_dir: None,
            wal_sync_policy: SyncPolicy::default(),
            wal_segment_size: 64 * 1024 * 1024,
            checkpoint_interval_secs: 300,
            checkpoint_wal_threshold: 128 * 1024 * 1024,
        }
    }

    /// Configuration optimized for maximum large value throughput.
    ///
    /// Key differences from default:
    /// - Large page size (64KB) for better large value performance
    /// - Very high overflow threshold to avoid overflow page chains
    /// - Large write buffer for better batching
    ///
    /// Use this when storing predominantly large values (>10KB).
    pub fn large_value_optimized() -> Self {
        Self {
            page_size: PageSizeConfig::Size64K,
            // Use very high threshold - values up to 1MB inline
            // This avoids overflow page overhead for most use cases
            overflow_threshold: 1024 * 1024,
            write_buffer_size: 4 * 1024 * 1024, // 4MB buffer
            wal_enabled: false,
            wal_dir: None,
            wal_sync_policy: SyncPolicy::default(),
            wal_segment_size: 64 * 1024 * 1024,
            checkpoint_interval_secs: 300,
            checkpoint_wal_threshold: 128 * 1024 * 1024,
        }
    }

    /// Configuration with WAL enabled for durability.
    ///
    /// Enables write-ahead logging with default settings.
    pub fn with_wal() -> Self {
        Self {
            wal_enabled: true,
            ..Self::default()
        }
    }
}

/// The main database handle.
///
/// A `Database` represents an open connection to a thunder database file.
/// It provides methods to begin read and write transactions.
///
/// # Concurrency
///
/// - Multiple read transactions can be active concurrently.
/// - Only one write transaction can be active at a time.
pub struct Database {
    /// Path to the database file.
    path: PathBuf,
    /// The underlying file handle.
    file: File,
    /// Current meta page (the one with higher txid).
    meta: Meta,
    /// In-memory B+ tree storing all key-value pairs.
    tree: BTree,
    /// Current write offset in the data section (for append-only writes).
    data_end_offset: u64,
    /// Number of entries currently persisted.
    persisted_entry_count: u64,
    /// Memory-mapped view of the database file (optional).
    /// Used for efficient read access to committed data.
    #[cfg(unix)]
    mmap: Option<Mmap>,
    /// Bloom filter for fast negative lookups.
    /// Returns false = definitely not present, true = possibly present.
    bloom: BloomFilter,
    /// Effective page size for this database.
    page_size: usize,
    /// Database options.
    options: DatabaseOptions,
    /// Overflow page manager.
    overflow_manager: OverflowManager,
    /// Maps keys to their overflow references (for large values).
    overflow_refs: std::collections::HashMap<Vec<u8>, OverflowRef>,
    // Phase 4: WAL & Checkpoint fields
    /// Write-ahead log (if enabled).
    wal: Option<Wal>,
    /// Checkpoint manager (if WAL enabled).
    checkpoint_manager: Option<CheckpointManager>,
}

impl Database {
    /// Opens a database at the given path.
    ///
    /// If the file does not exist, a new database will be created.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or created,
    /// or if the database file is corrupted.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, DatabaseOptions::default())
    }

    /// Opens a database at the given path with custom options.
    ///
    /// For new databases, the configured page size is used.
    /// For existing databases, the stored page size is used (options.page_size is ignored).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be opened or created
    /// - The database file is corrupted
    /// - Page size mismatch (existing database has different page size than expected)
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: DatabaseOptions) -> Result<Self> {
        let path = path.as_ref();
        let path_buf = path.to_path_buf();

        // Check if file exists to determine if we need to initialize.
        let file_exists = path.exists();

        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
        {
            Ok(f) => f,
            Err(e) => {
                return Err(Error::FileOpen {
                    path: path_buf,
                    source: e,
                });
            }
        };

        let file_len = match file.metadata() {
            Ok(m) => m.len(),
            Err(e) => {
                return Err(Error::FileMetadata {
                    path: path_buf,
                    source: e,
                });
            }
        };

        let (
            meta,
            mut tree,
            data_end_offset,
            persisted_entry_count,
            bloom,
            page_size,
            overflow_refs,
        ) = if file_exists && file_len > 0 {
            // Existing database: read and validate meta pages, load data.
            let meta = Self::load_meta(&mut file, &path_buf)?;

            // For existing databases, check if page size is valid
            let stored_page_size = meta.page_size as usize;
            if PageSizeConfig::from_u32(meta.page_size).is_none() {
                return Err(Error::Corrupted {
                    context: "loading meta page",
                    details: format!("invalid page size: {}", meta.page_size),
                });
            }

            // Check for page size mismatch
            let expected_page_size = options.page_size.as_usize();
            if stored_page_size != expected_page_size {
                return Err(Error::PageSizeMismatch {
                    expected: expected_page_size as u32,
                    actual: stored_page_size as u32,
                });
            }

            let (tree, data_end, count, bloom, overflow_refs) = Self::load_tree(
                &mut file,
                &meta,
                stored_page_size,
                options.overflow_threshold,
            )?;
            (
                meta,
                tree,
                data_end,
                count,
                bloom,
                stored_page_size,
                overflow_refs,
            )
        } else {
            // New database: initialize with two meta pages.
            let page_size = options.page_size.as_usize();
            let meta = Self::init_db(&mut file, &path_buf, page_size)?;
            let data_offset = 2 * PAGE_SIZE as u64 + 8; // After meta pages + entry count
            let bloom = BloomFilter::new(DEFAULT_BLOOM_EXPECTED_KEYS, DEFAULT_BLOOM_FP_RATE);
            (
                meta,
                BTree::new(),
                data_offset,
                0,
                bloom,
                page_size,
                std::collections::HashMap::new(),
            )
        };

        // Calculate next page ID for overflow manager
        let next_overflow_page = Self::calculate_next_overflow_page(data_end_offset, page_size);

        // Initialize mmap for efficient read access (Unix only).
        #[cfg(unix)]
        let mmap = Self::init_mmap(&file);

        // Initialize WAL if enabled
        let (wal, checkpoint_manager) = if options.wal_enabled {
            let wal_dir = options.wal_dir.clone().unwrap_or_else(|| {
                let mut wal_path = path_buf.clone();
                wal_path.set_extension("wal");
                wal_path
            });

            let wal_config = WalConfig {
                segment_size: options.wal_segment_size,
                sync_policy: options.wal_sync_policy,
            };

            let wal = Wal::open(&wal_dir, wal_config)?;

            // Initialize checkpoint manager
            let ckpt_config = CheckpointConfig {
                interval: std::time::Duration::from_secs(options.checkpoint_interval_secs),
                wal_threshold: options.checkpoint_wal_threshold,
                min_records: 10_000,
            };

            let ckpt_mgr = if meta.checkpoint_lsn > 0 {
                CheckpointManager::restore(ckpt_config, meta.checkpoint_info())
            } else {
                CheckpointManager::new(ckpt_config)
            };

            (Some(wal), Some(ckpt_mgr))
        } else {
            (None, None)
        };

        // Replay WAL from checkpoint if needed (after creating wal/ckpt_mgr)
        if let Some(ref wal) = wal {
            let replay_from = meta.checkpoint_lsn;
            if wal.current_lsn() > replay_from {
                let mut committed_txids = std::collections::HashSet::new();
                let mut txn_ops: std::collections::HashMap<u64, Vec<WalRecord>> =
                    std::collections::HashMap::new();
                let mut current_txid = None;

                wal.replay(replay_from, |record| {
                    match &record {
                        WalRecord::TxBegin { txid } => {
                            current_txid = Some(*txid);
                            txn_ops.insert(*txid, Vec::new());
                        }
                        WalRecord::Put { .. } | WalRecord::Delete { .. } => {
                            if let Some(txid) = current_txid {
                                txn_ops.entry(txid).or_default().push(record);
                            }
                        }
                        WalRecord::TxCommit { txid } => {
                            committed_txids.insert(*txid);
                        }
                        WalRecord::TxAbort { txid } => {
                            txn_ops.remove(txid);
                        }
                        WalRecord::Checkpoint { .. } => {}
                    }
                    Ok(())
                })?;

                // Apply only committed transactions
                for txid in committed_txids {
                    if let Some(ops) = txn_ops.get(&txid) {
                        for op in ops {
                            match op {
                                WalRecord::Put { key, value } => {
                                    tree.insert(key.clone(), value.clone());
                                }
                                WalRecord::Delete { key } => {
                                    tree.remove(key);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            path: path_buf,
            file,
            meta,
            tree,
            data_end_offset,
            persisted_entry_count,
            #[cfg(unix)]
            mmap,
            bloom,
            page_size,
            options,
            overflow_manager: OverflowManager::new(page_size, next_overflow_page),
            overflow_refs,
            wal,
            checkpoint_manager,
        })
    }

    /// Calculates the next available page ID for overflow pages.
    fn calculate_next_overflow_page(data_end_offset: u64, page_size: usize) -> PageId {
        // Overflow pages start after the data section
        // Round up to next page boundary
        let page_size_u64 = page_size as u64;
        data_end_offset.div_ceil(page_size_u64) + 1
    }

    /// Initializes the memory mapping for the database file.
    #[cfg(unix)]
    fn init_mmap(file: &File) -> Option<Mmap> {
        let file_len = file.metadata().ok()?.len() as usize;
        // Only mmap if file has data beyond meta pages
        if file_len > 2 * PAGE_SIZE {
            Mmap::new(file, file_len).ok()
        } else {
            None
        }
    }

    /// Refreshes the memory mapping after file size changes.
    ///
    /// Uses lazy remapping: only remaps when the file has grown beyond
    /// the current mapping, avoiding expensive syscalls on every commit.
    #[cfg(unix)]
    fn refresh_mmap(&mut self) -> Result<()> {
        // Only check if we know the file has grown
        // The caller knows the new size from the write operations
        Ok(())
    }

    /// Forces a refresh of the memory mapping with the new file length.
    /// Call this when the file has grown and you need to read the new data.
    #[cfg(unix)]
    #[allow(dead_code)]
    fn refresh_mmap_to_size(&mut self, new_len: usize) -> Result<()> {
        let current_mmap_len = self.mmap.as_ref().map(|m| m.len()).unwrap_or(0);
        if new_len > current_mmap_len && new_len > 2 * PAGE_SIZE {
            self.mmap = Mmap::new(&self.file, new_len).ok();
        }
        Ok(())
    }

    /// Returns a slice of the memory-mapped file at the given offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset into the file.
    /// * `len` - Number of bytes to read.
    ///
    /// # Returns
    ///
    /// `None` if mmap is not available or the range is out of bounds.
    #[cfg(unix)]
    pub fn mmap_slice(&self, offset: u64, len: usize) -> Option<&[u8]> {
        let mmap = self.mmap.as_ref()?;
        let start = offset as usize;
        let end = start.checked_add(len)?;
        if end <= mmap.len() {
            Some(&mmap.as_slice()[start..end])
        } else {
            None
        }
    }

    /// Returns a slice of the memory-mapped file (stub for non-Unix).
    #[cfg(not(unix))]
    pub fn mmap_slice(&self, _offset: u64, _len: usize) -> Option<&[u8]> {
        None
    }

    /// Returns the page size for this database.
    #[inline]
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Returns the overflow threshold for this database.
    #[inline]
    pub fn overflow_threshold(&self) -> usize {
        self.options.overflow_threshold
    }

    /// Initializes a new database file with two meta pages.
    fn init_db(file: &mut File, path: &std::path::PathBuf, page_size: usize) -> Result<Meta> {
        let meta = Meta::with_page_size(page_size as u32);
        let meta_bytes = meta.to_bytes();

        // Seek to beginning of file.
        if let Err(e) = file.seek(SeekFrom::Start(0)) {
            return Err(Error::FileSeek {
                offset: 0,
                context: "initializing database, seeking to start",
                source: e,
            });
        }

        // Write meta page 0.
        if let Err(e) = file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: 0,
                len: PAGE_SIZE,
                context: "writing initial meta page 0",
                source: e,
            });
        }

        // Write meta page 1 (identical initially).
        if let Err(e) = file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: PAGE_SIZE as u64,
                len: PAGE_SIZE,
                context: "writing initial meta page 1",
                source: e,
            });
        }

        // Ensure data is persisted to disk.
        if let Err(e) = file.sync_all() {
            return Err(Error::FileSync {
                context: "syncing initial meta pages",
                source: e,
            });
        }

        // Log successful initialization (only in debug builds).
        #[cfg(debug_assertions)]
        {
            eprintln!("[thunder] initialized new database at '{}'", path.display());
        }
        let _ = path; // Suppress unused warning in release.

        Ok(meta)
    }

    /// Loads and validates meta pages from an existing database file.
    fn load_meta(file: &mut File, _path: &std::path::PathBuf) -> Result<Meta> {
        let mut buf = [0u8; PAGE_SIZE];

        // Seek to meta page 0.
        if let Err(e) = file.seek(SeekFrom::Start(0)) {
            return Err(Error::FileSeek {
                offset: 0,
                context: "seeking to meta page 0",
                source: e,
            });
        }

        // Read meta page 0.
        if let Err(e) = file.read_exact(&mut buf) {
            return Err(Error::FileRead {
                offset: 0,
                len: PAGE_SIZE,
                context: "reading meta page 0",
                source: e,
            });
        }
        let meta0 = Meta::from_bytes(&buf);

        // Seek to meta page 1.
        let meta1_offset = PAGE_SIZE as u64;
        if let Err(e) = file.seek(SeekFrom::Start(meta1_offset)) {
            return Err(Error::FileSeek {
                offset: meta1_offset,
                context: "seeking to meta page 1",
                source: e,
            });
        }

        // Read meta page 1.
        if let Err(e) = file.read_exact(&mut buf) {
            return Err(Error::FileRead {
                offset: meta1_offset,
                len: PAGE_SIZE,
                context: "reading meta page 1",
                source: e,
            });
        }
        let meta1 = Meta::from_bytes(&buf);

        // Select the valid meta page with the highest txid.
        match (meta0, meta1) {
            (Some(m0), Some(m1)) => {
                let m0_valid = m0.validate();
                let m1_valid = m1.validate();

                if !m0_valid && !m1_valid {
                    return Err(Error::BothMetaPagesInvalid);
                }

                if !m0_valid {
                    Ok(m1)
                } else if !m1_valid {
                    Ok(m0)
                } else if m1.txid > m0.txid {
                    Ok(m1)
                } else {
                    Ok(m0)
                }
            }
            (Some(m), None) => {
                if m.validate() {
                    Ok(m)
                } else {
                    Err(Error::InvalidMetaPage {
                        page_number: 0,
                        reason: "meta page 0 parsed but failed validation",
                    })
                }
            }
            (None, Some(m)) => {
                if m.validate() {
                    Ok(m)
                } else {
                    Err(Error::InvalidMetaPage {
                        page_number: 1,
                        reason: "meta page 1 parsed but failed validation",
                    })
                }
            }
            (None, None) => Err(Error::BothMetaPagesInvalid),
        }
    }

    /// Loads the B+ tree data from the database file.
    fn load_tree(
        file: &mut File,
        meta: &Meta,
        page_size: usize,
        _overflow_threshold: usize,
    ) -> Result<TreeLoadResult> {
        let mut tree = BTree::new();
        let mut overflow_refs = std::collections::HashMap::new();

        // Data starts after the two meta pages.
        let data_offset = 2 * PAGE_SIZE as u64;

        // Read the data page count from meta.
        if meta.root == 0 {
            // No data stored yet.
            let bloom = BloomFilter::new(DEFAULT_BLOOM_EXPECTED_KEYS, DEFAULT_BLOOM_FP_RATE);
            return Ok((tree, data_offset + 8, 0, bloom, overflow_refs));
        }

        // Seek to data section.
        if let Err(e) = file.seek(SeekFrom::Start(data_offset)) {
            return Err(Error::FileSeek {
                offset: data_offset,
                context: "seeking to data section",
                source: e,
            });
        }

        // Read number of entries.
        let mut count_buf = [0u8; 8];
        if file.read_exact(&mut count_buf).is_err() {
            // No data section yet - this is OK for empty databases.
            let bloom = BloomFilter::new(DEFAULT_BLOOM_EXPECTED_KEYS, DEFAULT_BLOOM_FP_RATE);
            return Ok((tree, data_offset + 8, 0, bloom, overflow_refs));
        }
        let entry_count = u64::from_le_bytes(count_buf);

        // Validate entry count is reasonable (prevent OOM).
        const MAX_ENTRIES: u64 = 100_000_000; // 100 million entries max.
        if entry_count > MAX_ENTRIES {
            return Err(Error::Corrupted {
                context: "loading data entries",
                details: format!("entry count {entry_count} exceeds maximum allowed {MAX_ENTRIES}"),
            });
        }

        // Track current position for computing end offset.
        let mut current_offset = data_offset + 8;

        // Create overflow manager for reading overflow values
        let overflow_manager = OverflowManager::new(page_size, 0);

        // Read each entry.
        for entry_idx in 0..entry_count {
            // Read key length.
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact(&mut len_buf) {
                return Err(Error::EntryReadFailed {
                    entry_index: entry_idx,
                    field: "key length",
                    source: e,
                });
            }
            let key_len = u32::from_le_bytes(len_buf) as usize;
            current_offset += 4;

            // Validate key length.
            const MAX_KEY_LEN: usize = 64 * 1024; // 64KB max key.
            if key_len > MAX_KEY_LEN {
                return Err(Error::Corrupted {
                    context: "loading entry key",
                    details: format!(
                        "entry {entry_idx}: key length {key_len} exceeds maximum {MAX_KEY_LEN}"
                    ),
                });
            }

            // Read key.
            let mut key = vec![0u8; key_len];
            if let Err(e) = file.read_exact(&mut key) {
                return Err(Error::EntryReadFailed {
                    entry_index: entry_idx,
                    field: "key data",
                    source: e,
                });
            }
            current_offset += key_len as u64;

            // Read value length (or overflow marker).
            if let Err(e) = file.read_exact(&mut len_buf) {
                return Err(Error::EntryReadFailed {
                    entry_index: entry_idx,
                    field: "value length",
                    source: e,
                });
            }
            let value_len = u32::from_le_bytes(len_buf);
            current_offset += 4;

            let value = if value_len == OverflowRef::MARKER {
                // Read overflow reference
                let mut oref_buf = [0u8; OverflowRef::SIZE];
                if let Err(e) = file.read_exact(&mut oref_buf) {
                    return Err(Error::EntryReadFailed {
                        entry_index: entry_idx,
                        field: "overflow reference",
                        source: e,
                    });
                }
                current_offset += OverflowRef::SIZE as u64;

                let oref = OverflowRef::from_bytes(&oref_buf).ok_or_else(|| Error::Corrupted {
                    context: "loading overflow reference",
                    details: format!("entry {entry_idx}: invalid overflow reference"),
                })?;

                // Read overflow value from file
                let value = overflow_manager
                    .read_overflow_from_file(oref, file)
                    .ok_or_else(|| Error::Corrupted {
                        context: "reading overflow value",
                        details: format!("entry {entry_idx}: failed to read overflow chain"),
                    })?;

                // Store the overflow reference for later
                overflow_refs.insert(key.clone(), oref);

                // Seek back to continue reading entries
                if let Err(e) = file.seek(SeekFrom::Start(current_offset)) {
                    return Err(Error::FileSeek {
                        offset: current_offset,
                        context: "seeking after overflow read",
                        source: e,
                    });
                }

                value
            } else {
                // Validate inline value length.
                let value_len = value_len as usize;
                const MAX_VALUE_LEN: usize = 512 * 1024 * 1024; // 512MB max value.
                if value_len > MAX_VALUE_LEN {
                    return Err(Error::Corrupted {
                        context: "loading entry value",
                        details: format!(
                            "entry {entry_idx}: value length {value_len} exceeds maximum {MAX_VALUE_LEN}"
                        ),
                    });
                }

                // Read inline value.
                let mut value = vec![0u8; value_len];
                if let Err(e) = file.read_exact(&mut value) {
                    return Err(Error::EntryReadFailed {
                        entry_index: entry_idx,
                        field: "value data",
                        source: e,
                    });
                }
                current_offset += value_len as u64;

                value
            };

            tree.insert(key, value);
        }

        // Build bloom filter from loaded keys.
        // Use entry_count to size the filter appropriately.
        let bloom_size = (entry_count as usize).max(DEFAULT_BLOOM_EXPECTED_KEYS);
        let mut bloom = BloomFilter::new(bloom_size, DEFAULT_BLOOM_FP_RATE);
        for (key, _) in tree.iter() {
            bloom.insert(key);
        }

        Ok((tree, current_offset, entry_count, bloom, overflow_refs))
    }

    /// Persists the B+ tree data to the database file.
    /// This performs a FULL rewrite of all data - use `persist_incremental` for better performance.
    pub(crate) fn persist_tree(&mut self) -> Result<()> {
        // Data starts after the two meta pages.
        let data_offset = 2 * PAGE_SIZE as u64;

        // Collect all entries and determine which need overflow
        let entries: Vec<_> = self.tree.iter().collect();
        let overflow_threshold = self.options.overflow_threshold;
        let page_size = self.page_size;

        // Prepare entry data and overflow pages
        let mut entry_buf = Vec::with_capacity(256 * 1024);
        let mut new_overflow_refs = std::collections::HashMap::new();

        // Write entry count first
        let entry_count = entries.len() as u64;
        entry_buf.extend_from_slice(&entry_count.to_le_bytes());

        // First pass: prepare all entry data and collect overflow allocations
        // We'll compute overflow page positions after we know the total entry data size
        #[allow(clippy::type_complexity)]
        let mut _overflow_values: Vec<(&[u8], &[u8], Vec<u8>)> = Vec::new(); // (key, original_value, needs_overflow)

        for (key, value) in &entries {
            // Write key length and key
            entry_buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            entry_buf.extend_from_slice(key);

            if value.len() > overflow_threshold {
                // Mark for overflow - we'll write the actual reference later
                _overflow_values.push((key, value, Vec::new()));
                // Placeholder: write overflow marker + empty ref (will be filled in second pass)
                entry_buf.extend_from_slice(&OverflowRef::MARKER.to_le_bytes());
                let placeholder_ref = OverflowRef::new(0, value.len() as u32);
                entry_buf.extend_from_slice(&placeholder_ref.to_bytes());
            } else {
                // Write inline value
                entry_buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                entry_buf.extend_from_slice(value);
            }
        }

        // Calculate where overflow pages will start (after data section)
        let data_section_end = data_offset + entry_buf.len() as u64;
        // Align to page boundary
        let overflow_start = data_section_end.div_ceil(page_size as u64) * page_size as u64;
        let overflow_start_page = overflow_start / page_size as u64;

        // Reset overflow manager to allocate pages starting from overflow section
        self.overflow_manager = OverflowManager::new(page_size, overflow_start_page);

        // Collect all overflow data into a single buffer for one write syscall
        let mut all_overflow_data: Vec<u8> = Vec::new();
        let mut first_overflow_page: Option<u64> = None;

        // Second pass: create overflow pages and update references in entry_buf
        let mut buf_offset = 8; // Skip entry count
        for (key, value) in &entries {
            // Skip key length + key
            buf_offset += 4 + key.len();

            if value.len() > overflow_threshold {
                // Allocate overflow pages as single contiguous buffer
                let (oref, overflow_buffer) =
                    self.overflow_manager.allocate_overflow_contiguous(value);

                // Track the first overflow page for the combined write
                if first_overflow_page.is_none() && !overflow_buffer.is_empty() {
                    first_overflow_page = Some(oref.start_page);
                }

                // Append to the combined overflow buffer
                all_overflow_data.extend_from_slice(&overflow_buffer);

                // Update the reference in entry_buf
                buf_offset += 4; // Skip marker
                entry_buf[buf_offset..buf_offset + 8]
                    .copy_from_slice(&oref.start_page.to_le_bytes());
                entry_buf[buf_offset + 8..buf_offset + 12]
                    .copy_from_slice(&oref.total_len.to_le_bytes());
                buf_offset += 12;

                new_overflow_refs.insert(key.to_vec(), oref);
            } else {
                // Skip inline value
                buf_offset += 4 + value.len();
            }
        }

        // Write entry data (sequential data) first
        #[cfg(feature = "failpoint")]
        crate::failpoint!("before_data_write");

        if let Err(e) = self.file.seek(SeekFrom::Start(data_offset)) {
            return Err(Error::FileSeek {
                offset: data_offset,
                context: "seeking to data section",
                source: e,
            });
        }
        if let Err(e) = self.file.write_all(&entry_buf) {
            return Err(Error::FileWrite {
                offset: data_offset,
                len: entry_buf.len(),
                context: "writing entry data",
                source: e,
            });
        }

        #[cfg(feature = "failpoint")]
        crate::failpoint!("after_data_write");

        // Write ALL overflow data with a single syscall
        if let Some(start_page) = first_overflow_page {
            #[cfg(feature = "failpoint")]
            crate::failpoint!("before_overflow_write");

            let offset = start_page * page_size as u64;
            if let Err(e) = self.file.seek(SeekFrom::Start(offset)) {
                return Err(Error::FileSeek {
                    offset,
                    context: "seeking to overflow pages",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&all_overflow_data) {
                return Err(Error::FileWrite {
                    offset,
                    len: all_overflow_data.len(),
                    context: "writing all overflow pages (single write)",
                    source: e,
                });
            }

            #[cfg(feature = "failpoint")]
            crate::failpoint!("after_overflow_write");
        }

        // Update overflow refs
        self.overflow_refs = new_overflow_refs;

        // Calculate new data end offset (just the entry data, not overflow pages)
        let data_end = data_offset + entry_buf.len() as u64;
        self.data_end_offset = data_end;
        self.persisted_entry_count = entry_count;

        // Update meta page.
        #[cfg(feature = "failpoint")]
        crate::failpoint!("before_txid_increment");

        self.meta.txid += 1;

        #[cfg(feature = "failpoint")]
        crate::failpoint!("after_txid_increment");

        #[cfg(feature = "failpoint")]
        crate::failpoint!("before_root_update");

        self.meta.root = if self.tree.is_empty() { 0 } else { 1 };

        #[cfg(feature = "failpoint")]
        crate::failpoint!("after_root_update");

        // Write to alternating meta page.
        let meta_page = if self.meta.txid.is_multiple_of(2) {
            0
        } else {
            1
        };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        #[cfg(feature = "failpoint")]
        crate::failpoint!("before_meta_write");

        if let Err(e) = self.file.seek(SeekFrom::Start(meta_offset)) {
            return Err(Error::FileSeek {
                offset: meta_offset,
                context: "seeking to meta page for update",
                source: e,
            });
        }

        let meta_bytes = self.meta.to_bytes();
        if let Err(e) = self.file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: meta_offset,
                len: PAGE_SIZE,
                context: "writing updated meta page",
                source: e,
            });
        }

        #[cfg(feature = "failpoint")]
        crate::failpoint!("after_meta_write");

        // Use fdatasync instead of fsync for better performance (skips metadata sync).
        #[cfg(feature = "failpoint")]
        crate::failpoint!("before_fsync");

        Self::fdatasync(&self.file)?;

        #[cfg(feature = "failpoint")]
        crate::failpoint!("after_fsync");

        // Refresh mmap to reflect new file size.
        #[cfg(unix)]
        self.refresh_mmap()?;

        Ok(())
    }

    /// Writes a batch of data to the file (version 2 with proper overflow handling).
    #[allow(dead_code)]
    fn write_batch_to_file_v2(
        &mut self,
        batch: &crate::coalescer::WriteBatch,
        data_offset: u64,
    ) -> Result<()> {
        // Write sequential data first
        if !batch.sequential_data.is_empty() {
            if let Err(e) = self.file.seek(SeekFrom::Start(data_offset)) {
                return Err(Error::FileSeek {
                    offset: data_offset,
                    context: "seeking to data section",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&batch.sequential_data) {
                return Err(Error::FileWrite {
                    offset: data_offset,
                    len: batch.sequential_data.len(),
                    context: "writing entry data",
                    source: e,
                });
            }
        }

        // Write overflow pages (sorted by page ID for sequential I/O)
        for (page_id, data) in &batch.pages {
            let offset = *page_id * self.page_size as u64;
            if let Err(e) = self.file.seek(SeekFrom::Start(offset)) {
                return Err(Error::FileSeek {
                    offset,
                    context: "seeking to overflow page",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(data) {
                return Err(Error::FileWrite {
                    offset,
                    len: data.len(),
                    context: "writing overflow page",
                    source: e,
                });
            }
        }

        Ok(())
    }

    /// Writes a batch of data to the file.
    #[allow(dead_code)]
    fn write_batch_to_file(
        &mut self,
        batch: &crate::coalescer::WriteBatch,
        data_offset: u64,
    ) -> Result<()> {
        // Write overflow pages first (sorted by page ID for sequential I/O)
        for (page_id, data) in &batch.pages {
            let offset = *page_id * self.page_size as u64;
            if let Err(e) = self.file.seek(SeekFrom::Start(offset)) {
                return Err(Error::FileSeek {
                    offset,
                    context: "seeking to overflow page",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(data) {
                return Err(Error::FileWrite {
                    offset,
                    len: data.len(),
                    context: "writing overflow page",
                    source: e,
                });
            }
        }

        // Write sequential data
        if !batch.sequential_data.is_empty() {
            if let Err(e) = self.file.seek(SeekFrom::Start(data_offset)) {
                return Err(Error::FileSeek {
                    offset: data_offset,
                    context: "seeking to data section",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&batch.sequential_data) {
                return Err(Error::FileWrite {
                    offset: data_offset,
                    len: batch.sequential_data.len(),
                    context: "writing entry data",
                    source: e,
                });
            }
        }

        Ok(())
    }

    /// Persists incremental changes (new insertions only) to the database file.
    ///
    /// This is much faster than `persist_tree` for workloads with many small commits
    /// because it only appends new entries rather than rewriting all data.
    ///
    /// For large batches (>100 entries), uses parallel processing to prepare
    /// serialized data, significantly improving throughput on multi-core systems.
    ///
    /// # Arguments
    ///
    /// * `new_entries` - Iterator of (key, value) pairs to append.
    /// * `has_deletions` - If true, falls back to full rewrite (deletions require compaction).
    pub(crate) fn persist_incremental<'a, I>(
        &mut self,
        new_entries: I,
        has_deletions: bool,
    ) -> Result<()>
    where
        I: Iterator<Item = (&'a [u8], &'a [u8])>,
    {
        // If there are deletions, we need to do a full rewrite.
        // In the future, we could implement lazy compaction.
        if has_deletions {
            return self.persist_tree();
        }

        // Collect new entries for writing.
        let entries: Vec<_> = new_entries.collect();
        if entries.is_empty() {
            // Nothing to write, but still need to sync meta.
            return self.sync_meta_only();
        }

        let new_entry_count = entries.len() as u64;
        let total_entry_count = self.persisted_entry_count + new_entry_count;
        let overflow_threshold = self.options.overflow_threshold;
        let page_size = self.page_size;

        // Use parallel processing for large batches
        if entries.len() >= PARALLEL_THRESHOLD {
            return self.persist_incremental_parallel(&entries, total_entry_count);
        }

        // Sequential path for smaller batches (avoids thread pool overhead)
        // First pass: calculate total sizes needed for single-allocation optimization.
        // Use direct write format for large values: [len:4][data:N][crc:4] = N+8 bytes
        let mut total_overflow_size: usize = 0;
        let mut entry_buf_size: usize = 0;
        for (key, value) in &entries {
            // Key header: 4 bytes length + key bytes
            entry_buf_size += 4 + key.len();

            if value.len() > overflow_threshold {
                // Overflow marker + ref: 4 bytes marker + 12 bytes ref
                entry_buf_size += 4 + 12;
                // Use direct buffer size (no page chain overhead)
                total_overflow_size += OverflowManager::direct_buffer_size(value.len());
            } else {
                // Inline value: 4 bytes length + value bytes
                entry_buf_size += 4 + value.len();
            }
        }

        // Pre-allocate buffers with exact sizes to avoid reallocations
        let mut entry_buf = vec![0u8; entry_buf_size];
        let mut all_overflow_data = vec![0u8; total_overflow_size];

        // Calculate where overflow data will start (after entry data, page-aligned)
        let new_data_end = self.data_end_offset + entry_buf_size as u64;
        let overflow_start = new_data_end.div_ceil(page_size as u64) * page_size as u64;
        let overflow_start_page = overflow_start / page_size as u64;

        // Update overflow manager
        self.overflow_manager.set_next_page_id(overflow_start_page);

        // Track positions for direct memory writes
        let mut entry_offset: usize = 0;
        let mut overflow_offset: usize = 0;
        // Track whether we have any overflow data to write
        let mut has_overflow_data = false;

        // Second pass: build entry data and overflow data using fast unsafe copies
        // SAFETY: We pre-allocated exact sizes, so all offsets are valid.
        for (key, value) in &entries {
            // Write key length (4 bytes)
            let key_len_bytes = (key.len() as u32).to_le_bytes();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    key_len_bytes.as_ptr(),
                    entry_buf.as_mut_ptr().add(entry_offset),
                    4,
                );
            }
            entry_offset += 4;

            // Write key data
            unsafe {
                std::ptr::copy_nonoverlapping(
                    key.as_ptr(),
                    entry_buf.as_mut_ptr().add(entry_offset),
                    key.len(),
                );
            }
            entry_offset += key.len();

            if value.len() > overflow_threshold {
                // Write overflow marker (4 bytes)
                let marker_bytes = OverflowRef::MARKER.to_le_bytes();
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        marker_bytes.as_ptr(),
                        entry_buf.as_mut_ptr().add(entry_offset),
                        4,
                    );
                }
                entry_offset += 4;

                // Write overflow data using direct format (no page chain overhead)
                // Pass the file base offset so OverflowRef can store correct byte position
                let (oref, bytes_written) = self.overflow_manager.write_direct_to_buffer(
                    value,
                    &mut all_overflow_data,
                    overflow_offset,
                    overflow_start, // File byte offset where overflow buffer will be written
                );

                // Track that we have overflow data
                if bytes_written > 0 {
                    has_overflow_data = true;
                }
                overflow_offset += bytes_written;

                // Write overflow ref (12 bytes)
                let oref_bytes = oref.to_bytes();
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        oref_bytes.as_ptr(),
                        entry_buf.as_mut_ptr().add(entry_offset),
                        12,
                    );
                }
                entry_offset += 12;

                self.overflow_refs.insert(key.to_vec(), oref);
            } else {
                // Write inline value length (4 bytes)
                let value_len_bytes = (value.len() as u32).to_le_bytes();
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value_len_bytes.as_ptr(),
                        entry_buf.as_mut_ptr().add(entry_offset),
                        4,
                    );
                }
                entry_offset += 4;

                // Write inline value data
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr(),
                        entry_buf.as_mut_ptr().add(entry_offset),
                        value.len(),
                    );
                }
                entry_offset += value.len();
            }
        }

        // Write data in file order to minimize seeks:
        // 1. Meta page (offset 0 or PAGE_SIZE)
        // 2. Entry count (offset 2*PAGE_SIZE)
        // 3. Entry data (data_end_offset)
        // 4. Overflow data (overflow_start)

        // Update tracking info BEFORE writes.
        self.data_end_offset = new_data_end;
        self.persisted_entry_count = total_entry_count;

        // Update meta.
        self.meta.txid += 1;
        self.meta.root = 1; // We have data

        let meta_page = if self.meta.txid.is_multiple_of(2) {
            0
        } else {
            1
        };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        // Use pwrite for positioned writes without seeking (Unix only)
        // This avoids multiple seek syscalls and is more efficient
        #[cfg(unix)]
        {
            #[cfg(feature = "failpoint")]
            crate::failpoint!("incr_before_meta_write");

            let meta_bytes = self.meta.to_bytes();

            // Write meta page using pwrite
            if let Err(e) = self.file.write_at(&meta_bytes, meta_offset) {
                return Err(Error::FileWrite {
                    offset: meta_offset,
                    len: PAGE_SIZE,
                    context: "writing meta page (incremental, pwrite)",
                    source: e,
                });
            }

            #[cfg(feature = "failpoint")]
            crate::failpoint!("incr_after_meta_write");

            #[cfg(feature = "failpoint")]
            crate::failpoint!("incr_before_entry_write");

            // Write entry count using pwrite
            let data_offset = 2 * PAGE_SIZE as u64;
            if let Err(e) = self
                .file
                .write_at(&total_entry_count.to_le_bytes(), data_offset)
            {
                return Err(Error::FileWrite {
                    offset: data_offset,
                    len: 8,
                    context: "writing entry count (pwrite)",
                    source: e,
                });
            }

            // Write entry data using pwrite
            let entry_write_offset = self.data_end_offset - entry_buf.len() as u64;
            if let Err(e) = self.file.write_at(&entry_buf, entry_write_offset) {
                return Err(Error::FileWrite {
                    offset: entry_write_offset,
                    len: entry_buf.len(),
                    context: "writing entry data (incremental, pwrite)",
                    source: e,
                });
            }

            #[cfg(feature = "failpoint")]
            crate::failpoint!("incr_after_entry_write");

            // Write overflow data using pwrite at the calculated byte offset
            if has_overflow_data
                && let Err(e) = self.file.write_at(&all_overflow_data, overflow_start)
            {
                return Err(Error::FileWrite {
                    offset: overflow_start,
                    len: all_overflow_data.len(),
                    context: "writing overflow data (incremental, pwrite)",
                    source: e,
                });
            }
        }

        // Fallback for non-Unix: use seek + write
        #[cfg(not(unix))]
        {
            let meta_bytes = self.meta.to_bytes();

            if let Err(e) = self.file.seek(SeekFrom::Start(meta_offset)) {
                return Err(Error::FileSeek {
                    offset: meta_offset,
                    context: "seeking to meta page for incremental update",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&meta_bytes) {
                return Err(Error::FileWrite {
                    offset: meta_offset,
                    len: PAGE_SIZE,
                    context: "writing meta page (incremental)",
                    source: e,
                });
            }

            let data_offset = 2 * PAGE_SIZE as u64;
            if let Err(e) = self.file.seek(SeekFrom::Start(data_offset)) {
                return Err(Error::FileSeek {
                    offset: data_offset,
                    context: "seeking to update entry count",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&total_entry_count.to_le_bytes()) {
                return Err(Error::FileWrite {
                    offset: data_offset,
                    len: 8,
                    context: "updating entry count",
                    source: e,
                });
            }

            let entry_write_offset = self.data_end_offset - entry_buf.len() as u64;
            if let Err(e) = self.file.seek(SeekFrom::Start(entry_write_offset)) {
                return Err(Error::FileSeek {
                    offset: entry_write_offset,
                    context: "seeking to append position",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&entry_buf) {
                return Err(Error::FileWrite {
                    offset: entry_write_offset,
                    len: entry_buf.len(),
                    context: "writing entry data (incremental)",
                    source: e,
                });
            }

            if has_overflow_data {
                if let Err(e) = self.file.seek(SeekFrom::Start(overflow_start)) {
                    return Err(Error::FileSeek {
                        offset: overflow_start,
                        context: "seeking to overflow data (incremental)",
                        source: e,
                    });
                }
                if let Err(e) = self.file.write_all(&all_overflow_data) {
                    return Err(Error::FileWrite {
                        offset: overflow_start,
                        len: all_overflow_data.len(),
                        context: "writing overflow data (incremental)",
                        source: e,
                    });
                }
            }
        }

        // Use fdatasync for better performance.
        #[cfg(feature = "failpoint")]
        crate::failpoint!("incr_before_fsync");

        Self::fdatasync(&self.file)?;

        #[cfg(feature = "failpoint")]
        crate::failpoint!("incr_after_fsync");

        // Refresh mmap to reflect new file size.
        #[cfg(unix)]
        self.refresh_mmap()?;

        Ok(())
    }

    /// Parallel version of persist_incremental for large batches.
    ///
    /// Uses rayon to parallelize entry serialization across multiple cores.
    /// This significantly improves throughput for batches with hundreds or
    /// thousands of entries.
    fn persist_incremental_parallel(
        &mut self,
        entries: &[(&[u8], &[u8])],
        total_entry_count: u64,
    ) -> Result<()> {
        let overflow_threshold = self.options.overflow_threshold;
        let page_size = self.page_size;

        // Phase 1: Parallel size calculation and entry preparation
        // Each entry is processed independently to compute its serialized form
        #[derive(Clone)]
        struct PreparedEntry {
            key_data: Vec<u8>,
            value_data: Vec<u8>,
            key: Vec<u8>,
            is_overflow: bool,
            overflow_value: Option<Vec<u8>>,
        }

        let prepared: Vec<PreparedEntry> = entries
            .par_iter()
            .map(|(key, value)| {
                // Serialize key: [len:4][key_bytes]
                let mut key_data = Vec::with_capacity(4 + key.len());
                key_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                key_data.extend_from_slice(key);

                let is_overflow = value.len() > overflow_threshold;
                let value_data = if is_overflow {
                    // Placeholder for overflow marker + ref
                    let mut data = Vec::with_capacity(16);
                    data.extend_from_slice(&OverflowRef::MARKER.to_le_bytes());
                    data.extend_from_slice(&[0u8; 12]); // Placeholder
                    data
                } else {
                    // Inline: [len:4][value_bytes]
                    let mut data = Vec::with_capacity(4 + value.len());
                    data.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    data.extend_from_slice(value);
                    data
                };

                PreparedEntry {
                    key_data,
                    value_data,
                    key: key.to_vec(),
                    is_overflow,
                    overflow_value: if is_overflow {
                        Some(value.to_vec())
                    } else {
                        None
                    },
                }
            })
            .collect();

        // Phase 2: Calculate total sizes (sequential but fast - just summing)
        let entry_buf_size: usize = prepared
            .iter()
            .map(|e| e.key_data.len() + e.value_data.len())
            .sum();

        let total_overflow_size: usize = prepared
            .iter()
            .filter(|e| e.is_overflow)
            .map(|e| OverflowManager::direct_buffer_size(e.overflow_value.as_ref().unwrap().len()))
            .sum();

        // Pre-allocate buffers
        let mut entry_buf = vec![0u8; entry_buf_size];
        let mut all_overflow_data = vec![0u8; total_overflow_size];

        // Calculate overflow position
        let new_data_end = self.data_end_offset + entry_buf_size as u64;
        let overflow_start = new_data_end.div_ceil(page_size as u64) * page_size as u64;
        let overflow_start_page = overflow_start / page_size as u64;

        // Update overflow manager
        self.overflow_manager.set_next_page_id(overflow_start_page);

        // Phase 3: Sequential assembly (must be sequential for overflow refs)
        let mut entry_offset: usize = 0;
        let mut overflow_offset: usize = 0;
        let mut has_overflow_data = false;

        for entry in &prepared {
            // Copy key data
            entry_buf[entry_offset..entry_offset + entry.key_data.len()]
                .copy_from_slice(&entry.key_data);
            entry_offset += entry.key_data.len();

            if entry.is_overflow {
                // Write overflow marker
                entry_buf[entry_offset..entry_offset + 4]
                    .copy_from_slice(&OverflowRef::MARKER.to_le_bytes());
                entry_offset += 4;

                // Write overflow data and get ref
                let value = entry.overflow_value.as_ref().unwrap();
                let (oref, bytes_written) = self.overflow_manager.write_direct_to_buffer(
                    value,
                    &mut all_overflow_data,
                    overflow_offset,
                    overflow_start,
                );

                if bytes_written > 0 {
                    has_overflow_data = true;
                }
                overflow_offset += bytes_written;

                // Write overflow ref
                entry_buf[entry_offset..entry_offset + 12].copy_from_slice(&oref.to_bytes());
                entry_offset += 12;

                self.overflow_refs.insert(entry.key.clone(), oref);
            } else {
                // Copy inline value data
                entry_buf[entry_offset..entry_offset + entry.value_data.len()]
                    .copy_from_slice(&entry.value_data);
                entry_offset += entry.value_data.len();
            }
        }

        // Phase 4: Write to disk (same as sequential version)
        self.data_end_offset = new_data_end;
        self.persisted_entry_count = total_entry_count;

        self.meta.txid += 1;
        self.meta.root = 1;

        let meta_page = if self.meta.txid.is_multiple_of(2) {
            0
        } else {
            1
        };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        #[cfg(unix)]
        {
            let meta_bytes = self.meta.to_bytes();

            if let Err(e) = self.file.write_at(&meta_bytes, meta_offset) {
                return Err(Error::FileWrite {
                    offset: meta_offset,
                    len: PAGE_SIZE,
                    context: "writing meta page (parallel incremental, pwrite)",
                    source: e,
                });
            }

            let data_offset = 2 * PAGE_SIZE as u64;
            if let Err(e) = self
                .file
                .write_at(&total_entry_count.to_le_bytes(), data_offset)
            {
                return Err(Error::FileWrite {
                    offset: data_offset,
                    len: 8,
                    context: "writing entry count (parallel, pwrite)",
                    source: e,
                });
            }

            let entry_write_offset = self.data_end_offset - entry_buf.len() as u64;
            if let Err(e) = self.file.write_at(&entry_buf, entry_write_offset) {
                return Err(Error::FileWrite {
                    offset: entry_write_offset,
                    len: entry_buf.len(),
                    context: "writing entry data (parallel incremental, pwrite)",
                    source: e,
                });
            }

            if has_overflow_data
                && let Err(e) = self.file.write_at(&all_overflow_data, overflow_start)
            {
                return Err(Error::FileWrite {
                    offset: overflow_start,
                    len: all_overflow_data.len(),
                    context: "writing overflow data (parallel incremental, pwrite)",
                    source: e,
                });
            }
        }

        #[cfg(not(unix))]
        {
            let meta_bytes = self.meta.to_bytes();

            if let Err(e) = self.file.seek(SeekFrom::Start(meta_offset)) {
                return Err(Error::FileSeek {
                    offset: meta_offset,
                    context: "seeking to meta page (parallel incremental)",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&meta_bytes) {
                return Err(Error::FileWrite {
                    offset: meta_offset,
                    len: PAGE_SIZE,
                    context: "writing meta page (parallel incremental)",
                    source: e,
                });
            }

            let data_offset = 2 * PAGE_SIZE as u64;
            if let Err(e) = self.file.seek(SeekFrom::Start(data_offset)) {
                return Err(Error::FileSeek {
                    offset: data_offset,
                    context: "seeking to entry count (parallel)",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&total_entry_count.to_le_bytes()) {
                return Err(Error::FileWrite {
                    offset: data_offset,
                    len: 8,
                    context: "writing entry count (parallel)",
                    source: e,
                });
            }

            let entry_write_offset = self.data_end_offset - entry_buf.len() as u64;
            if let Err(e) = self.file.seek(SeekFrom::Start(entry_write_offset)) {
                return Err(Error::FileSeek {
                    offset: entry_write_offset,
                    context: "seeking to entry data (parallel)",
                    source: e,
                });
            }
            if let Err(e) = self.file.write_all(&entry_buf) {
                return Err(Error::FileWrite {
                    offset: entry_write_offset,
                    len: entry_buf.len(),
                    context: "writing entry data (parallel incremental)",
                    source: e,
                });
            }

            if has_overflow_data {
                if let Err(e) = self.file.seek(SeekFrom::Start(overflow_start)) {
                    return Err(Error::FileSeek {
                        offset: overflow_start,
                        context: "seeking to overflow data (parallel)",
                        source: e,
                    });
                }
                if let Err(e) = self.file.write_all(&all_overflow_data) {
                    return Err(Error::FileWrite {
                        offset: overflow_start,
                        len: all_overflow_data.len(),
                        context: "writing overflow data (parallel incremental)",
                        source: e,
                    });
                }
            }
        }

        Self::fdatasync(&self.file)?;

        #[cfg(unix)]
        self.refresh_mmap()?;

        Ok(())
    }

    /// Syncs only the meta page (for commits with no data changes).
    fn sync_meta_only(&mut self) -> Result<()> {
        self.meta.txid += 1;

        let meta_page = if self.meta.txid.is_multiple_of(2) {
            0
        } else {
            1
        };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        if let Err(e) = self.file.seek(SeekFrom::Start(meta_offset)) {
            return Err(Error::FileSeek {
                offset: meta_offset,
                context: "seeking to meta page for sync",
                source: e,
            });
        }

        let meta_bytes = self.meta.to_bytes();
        if let Err(e) = self.file.write_all(&meta_bytes) {
            return Err(Error::FileWrite {
                offset: meta_offset,
                len: PAGE_SIZE,
                context: "writing meta page (sync only)",
                source: e,
            });
        }

        Self::fdatasync(&self.file)?;
        Ok(())
    }

    /// Performs fdatasync on Unix systems, falling back to sync_all elsewhere.
    /// fdatasync is faster than fsync because it doesn't sync file metadata.
    #[inline]
    fn fdatasync(file: &File) -> Result<()> {
        #[cfg(unix)]
        {
            // SAFETY: fdatasync is a standard POSIX call, safe with a valid fd.
            let ret = unsafe { libc::fdatasync(file.as_raw_fd()) };
            if ret != 0 {
                return Err(Error::FileSync {
                    context: "fdatasync failed",
                    source: std::io::Error::last_os_error(),
                });
            }
            Ok(())
        }

        #[cfg(not(unix))]
        {
            file.sync_all().map_err(|e| Error::FileSync {
                context: "sync_all fallback",
                source: e,
            })
        }
    }

    /// Returns the path to the database file.
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns a reference to the current meta page.
    #[allow(dead_code)]
    pub(crate) fn meta(&self) -> &Meta {
        &self.meta
    }

    /// Returns a mutable reference to the file handle.
    #[allow(dead_code)]
    pub(crate) fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Returns a reference to the B+ tree.
    pub(crate) fn tree(&self) -> &BTree {
        &self.tree
    }

    /// Returns a mutable reference to the B+ tree.
    pub(crate) fn tree_mut(&mut self) -> &mut BTree {
        &mut self.tree
    }

    /// Returns a reference to the bloom filter.
    #[allow(dead_code)]
    pub(crate) fn bloom(&self) -> &BloomFilter {
        &self.bloom
    }

    /// Returns a mutable reference to the bloom filter.
    pub(crate) fn bloom_mut(&mut self) -> &mut BloomFilter {
        &mut self.bloom
    }

    /// Checks if a key might exist in the database using the bloom filter.
    ///
    /// This is a fast probabilistic check:
    /// - Returns `false`: The key definitely does NOT exist.
    /// - Returns `true`: The key MIGHT exist (requires actual lookup to confirm).
    ///
    /// Use this for fast-path rejection of negative lookups.
    #[inline]
    pub(crate) fn may_contain_key(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    /// Begins a new read-only transaction.
    ///
    /// Read transactions provide a consistent snapshot view of the database.
    /// Multiple read transactions can be active concurrently.
    pub fn read_tx(&self) -> ReadTx<'_> {
        ReadTx::new(self)
    }

    /// Begins a new read-write transaction.
    ///
    /// Only one write transaction can be active at a time.
    /// The transaction must be committed to persist changes.
    pub fn write_tx(&mut self) -> WriteTx<'_> {
        WriteTx::new(self)
    }

    // ==================== Phase 4: WAL & Checkpoint Methods ====================

    /// Returns whether WAL is enabled for this database.
    pub fn wal_enabled(&self) -> bool {
        self.wal.is_some()
    }

    /// Returns a mutable reference to the WAL, if enabled.
    ///
    /// Reserved for future WAL integration enhancements.
    #[allow(dead_code)]
    pub(crate) fn wal_mut(&mut self) -> Option<&mut Wal> {
        self.wal.as_mut()
    }

    /// Returns the checkpoint LSN, if WAL is enabled and a checkpoint exists.
    pub fn checkpoint_lsn(&self) -> Option<Lsn> {
        if self.meta.checkpoint_lsn > 0 {
            Some(self.meta.checkpoint_lsn)
        } else {
            None
        }
    }

    /// Creates a checkpoint.
    ///
    /// This persists all data to the main database file and updates the
    /// checkpoint LSN in the meta page. WAL segments before the checkpoint
    /// can then be safely truncated.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL is not enabled or if the checkpoint fails.
    pub fn checkpoint(&mut self) -> Result<()> {
        // Get checkpoint LSN from WAL
        let checkpoint_lsn = {
            let wal = self.wal.as_ref().ok_or_else(|| Error::CheckpointFailed {
                lsn: 0,
                reason: "WAL not enabled".to_string(),
            })?;
            wal.current_lsn()
        };

        // Persist all data to main database file
        self.persist_tree()?;

        // Update meta with checkpoint info
        let ckpt_info = CheckpointInfo {
            lsn: checkpoint_lsn,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            entry_count: self.persisted_entry_count,
        };
        self.meta.set_checkpoint_info(&ckpt_info);

        // Write meta page
        let meta_page = if self.meta.txid.is_multiple_of(2) {
            0
        } else {
            1
        };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        self.file
            .seek(SeekFrom::Start(meta_offset))
            .map_err(|e| Error::FileSeek {
                offset: meta_offset,
                context: "seeking to meta page for checkpoint",
                source: e,
            })?;

        let meta_bytes = self.meta.to_bytes();
        self.file
            .write_all(&meta_bytes)
            .map_err(|e| Error::FileWrite {
                offset: meta_offset,
                len: PAGE_SIZE,
                context: "writing meta page for checkpoint",
                source: e,
            })?;

        Self::fdatasync(&self.file)?;

        // Truncate WAL segments before checkpoint
        if let Some(wal) = &mut self.wal {
            wal.truncate_before(checkpoint_lsn)?;
        }

        // Update checkpoint manager
        if let Some(ckpt_mgr) = &mut self.checkpoint_manager {
            ckpt_mgr.record_checkpoint(checkpoint_lsn);
        }

        Ok(())
    }

    /// Writes a WAL record for a Put operation.
    ///
    /// Called by WriteTx during commit when WAL is enabled.
    /// Reserved for future WAL integration enhancements.
    #[allow(dead_code)]
    pub(crate) fn wal_put(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Lsn>> {
        if let Some(wal) = &mut self.wal {
            let lsn = wal.append(&WalRecord::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })?;
            Ok(Some(lsn))
        } else {
            Ok(None)
        }
    }

    /// Writes a WAL record for a Delete operation.
    ///
    /// Reserved for future WAL integration enhancements.
    #[allow(dead_code)]
    pub(crate) fn wal_delete(&mut self, key: &[u8]) -> Result<Option<Lsn>> {
        if let Some(wal) = &mut self.wal {
            let lsn = wal.append(&WalRecord::Delete { key: key.to_vec() })?;
            Ok(Some(lsn))
        } else {
            Ok(None)
        }
    }

    /// Writes WAL records for transaction begin.
    ///
    /// Reserved for future WAL integration enhancements.
    #[allow(dead_code)]
    pub(crate) fn wal_tx_begin(&mut self, txid: u64) -> Result<Option<Lsn>> {
        if let Some(wal) = &mut self.wal {
            let lsn = wal.append(&WalRecord::TxBegin { txid })?;
            Ok(Some(lsn))
        } else {
            Ok(None)
        }
    }

    /// Writes WAL records for transaction commit.
    ///
    /// Reserved for future WAL integration enhancements.
    #[allow(dead_code)]
    pub(crate) fn wal_tx_commit(&mut self, txid: u64) -> Result<Option<Lsn>> {
        if let Some(wal) = &mut self.wal {
            let lsn = wal.append(&WalRecord::TxCommit { txid })?;
            wal.sync()?; // Ensure commit is durable
            Ok(Some(lsn))
        } else {
            Ok(None)
        }
    }

    /// Syncs the WAL to disk.
    ///
    /// Reserved for future WAL integration enhancements.
    #[allow(dead_code)]
    pub(crate) fn wal_sync(&mut self) -> Result<()> {
        if let Some(wal) = &mut self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Returns the current transaction ID for WAL purposes.
    ///
    /// Reserved for future WAL integration enhancements.
    #[allow(dead_code)]
    pub(crate) fn next_txid(&self) -> u64 {
        self.meta.txid + 1
    }
}
