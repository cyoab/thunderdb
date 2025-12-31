//! Summary: Database open/close and core management logic.
//! Copyright (c) YOAB. All rights reserved.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::btree::BTree;
use crate::error::{Error, Result};
use crate::meta::Meta;
use crate::page::PAGE_SIZE;
use crate::tx::{ReadTx, WriteTx};

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
    #[allow(dead_code)]
    path: std::path::PathBuf,
    /// The underlying file handle.
    file: File,
    /// Current meta page (the one with higher txid).
    meta: Meta,
    /// In-memory B+ tree storing all key-value pairs.
    tree: BTree,
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
        let path = path.as_ref();

        // Check if file exists to determine if we need to initialize.
        let file_exists = path.exists();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let (meta, tree) = if file_exists && file.metadata()?.len() > 0 {
            // Existing database: read and validate meta pages, load data.
            let meta = Self::load_meta(&mut file)?;
            let tree = Self::load_tree(&mut file, &meta)?;
            (meta, tree)
        } else {
            // New database: initialize with two meta pages.
            let meta = Self::init_db(&mut file)?;
            (meta, BTree::new())
        };

        Ok(Self {
            path: path.to_path_buf(),
            file,
            meta,
            tree,
        })
    }

    /// Initializes a new database file with two meta pages.
    fn init_db(file: &mut File) -> Result<Meta> {
        let meta = Meta::new();
        let meta_bytes = meta.to_bytes();

        // Write meta page 0.
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&meta_bytes)?;

        // Write meta page 1 (identical initially).
        file.write_all(&meta_bytes)?;

        // Ensure data is persisted to disk.
        file.sync_all()?;

        Ok(meta)
    }

    /// Loads and validates meta pages from an existing database file.
    fn load_meta(file: &mut File) -> Result<Meta> {
        let mut buf = [0u8; PAGE_SIZE];

        // Read meta page 0.
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut buf)?;
        let meta0 = Meta::from_bytes(&buf);

        // Read meta page 1.
        file.seek(SeekFrom::Start(PAGE_SIZE as u64))?;
        file.read_exact(&mut buf)?;
        let meta1 = Meta::from_bytes(&buf);

        // Select the valid meta page with the highest txid.
        match (meta0, meta1) {
            (Some(m0), Some(m1)) => {
                if !m0.validate() && !m1.validate() {
                    return Err(Error::Corrupted(
                        "both meta pages are invalid".to_string(),
                    ));
                }
                if !m0.validate() {
                    Ok(m1)
                } else if !m1.validate() {
                    Ok(m0)
                } else if m1.txid > m0.txid {
                    Ok(m1)
                } else {
                    Ok(m0)
                }
            }
            (Some(m), None) | (None, Some(m)) => {
                if m.validate() {
                    Ok(m)
                } else {
                    Err(Error::Corrupted("valid meta page is invalid".to_string()))
                }
            }
            (None, None) => Err(Error::Corrupted(
                "failed to read any meta page".to_string(),
            )),
        }
    }

    /// Loads the B+ tree data from the database file.
    fn load_tree(file: &mut File, meta: &Meta) -> Result<BTree> {
        let mut tree = BTree::new();

        // Data starts after the two meta pages.
        let data_offset = 2 * PAGE_SIZE as u64;

        // Read the data page count from meta.
        if meta.root == 0 {
            // No data stored yet.
            return Ok(tree);
        }

        // Seek to data section.
        file.seek(SeekFrom::Start(data_offset))?;

        // Read number of entries.
        let mut count_buf = [0u8; 8];
        if file.read_exact(&mut count_buf).is_err() {
            // No data section yet.
            return Ok(tree);
        }
        let entry_count = u64::from_le_bytes(count_buf);

        // Read each entry.
        for _ in 0..entry_count {
            // Read key length.
            let mut len_buf = [0u8; 4];
            file.read_exact(&mut len_buf)?;
            let key_len = u32::from_le_bytes(len_buf) as usize;

            // Read key.
            let mut key = vec![0u8; key_len];
            file.read_exact(&mut key)?;

            // Read value length.
            file.read_exact(&mut len_buf)?;
            let value_len = u32::from_le_bytes(len_buf) as usize;

            // Read value.
            let mut value = vec![0u8; value_len];
            file.read_exact(&mut value)?;

            tree.insert(key, value);
        }

        Ok(tree)
    }

    /// Persists the B+ tree data to the database file.
    pub(crate) fn persist_tree(&mut self) -> Result<()> {
        // Data starts after the two meta pages.
        let data_offset = 2 * PAGE_SIZE as u64;

        // Seek to data section.
        self.file.seek(SeekFrom::Start(data_offset))?;

        // Write number of entries.
        let entry_count = self.tree.len() as u64;
        self.file.write_all(&entry_count.to_le_bytes())?;

        // Write each entry.
        for (key, value) in self.tree.iter() {
            // Write key length and key.
            self.file.write_all(&(key.len() as u32).to_le_bytes())?;
            self.file.write_all(key)?;

            // Write value length and value.
            self.file.write_all(&(value.len() as u32).to_le_bytes())?;
            self.file.write_all(value)?;
        }

        // Update meta page.
        self.meta.txid += 1;
        self.meta.root = if self.tree.is_empty() { 0 } else { 1 };

        // Write to alternating meta page.
        let meta_page = if self.meta.txid.is_multiple_of(2) { 0 } else { 1 };
        let meta_offset = meta_page * PAGE_SIZE as u64;

        self.file.seek(SeekFrom::Start(meta_offset))?;
        self.file.write_all(&self.meta.to_bytes())?;

        // Sync to disk.
        self.file.sync_all()?;

        Ok(())
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
}

