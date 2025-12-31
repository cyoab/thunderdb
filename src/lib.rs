//! Summary: thunder - A minimal, embedded, transactional key-value database engine.
//! Copyright (c) YOAB. All rights reserved.

pub mod btree;
pub mod db;
pub mod error;
pub mod freelist;
pub mod meta;
pub mod mmap;
pub mod page;
pub mod tx;

// Re-export public API at crate root for convenience.
pub use db::Database;
pub use error::{Error, Result};
pub use tx::{ReadTx, WriteTx};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn test_db_path(name: &str) -> String {
        format!("/tmp/thunder_test_{name}.db")
    }

    fn cleanup(path: &str) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_database_open_creates_file() {
        let path = test_db_path("open_creates");
        cleanup(&path);

        let result = Database::open(&path);
        assert!(result.is_ok(), "Database::open should succeed");

        cleanup(&path);
    }

    #[test]
    fn test_database_open_existing() {
        let path = test_db_path("open_existing");
        cleanup(&path);

        // Create database
        {
            let _db = Database::open(&path).expect("first open should succeed");
        }

        // Re-open existing database
        {
            let result = Database::open(&path);
            assert!(result.is_ok(), "re-opening existing database should succeed");
        }

        cleanup(&path);
    }

    #[test]
    fn test_put_then_get() {
        let path = test_db_path("put_get");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Write a key-value pair
        {
            let mut wtx = db.write_tx();
            wtx.put(b"hello", b"world");
            wtx.commit().expect("commit should succeed");
        }

        // Read it back
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"hello");
            assert_eq!(value, Some(b"world".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let path = test_db_path("get_nonexistent");
        cleanup(&path);

        let db = Database::open(&path).expect("open should succeed");

        let rtx = db.read_tx();
        let value = rtx.get(b"nonexistent");
        assert_eq!(value, None);

        cleanup(&path);
    }

    #[test]
    fn test_put_overwrite() {
        let path = test_db_path("put_overwrite");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Write initial value
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value1");
            wtx.commit().expect("commit should succeed");
        }

        // Overwrite with new value
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key", b"value2");
            wtx.commit().expect("commit should succeed");
        }

        // Verify overwritten value
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"key");
            assert_eq!(value, Some(b"value2".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_delete() {
        let path = test_db_path("delete");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Write a key
        {
            let mut wtx = db.write_tx();
            wtx.put(b"to_delete", b"value");
            wtx.commit().expect("commit should succeed");
        }

        // Delete the key
        {
            let mut wtx = db.write_tx();
            wtx.delete(b"to_delete");
            wtx.commit().expect("commit should succeed");
        }

        // Verify key is gone
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"to_delete");
            assert_eq!(value, None);
        }

        cleanup(&path);
    }

    #[test]
    fn test_multiple_keys() {
        let path = test_db_path("multiple_keys");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Write multiple keys
        {
            let mut wtx = db.write_tx();
            wtx.put(b"key1", b"value1");
            wtx.put(b"key2", b"value2");
            wtx.put(b"key3", b"value3");
            wtx.commit().expect("commit should succeed");
        }

        // Verify all keys
        {
            let rtx = db.read_tx();
            assert_eq!(rtx.get(b"key1"), Some(b"value1".to_vec()));
            assert_eq!(rtx.get(b"key2"), Some(b"value2".to_vec()));
            assert_eq!(rtx.get(b"key3"), Some(b"value3".to_vec()));
        }

        cleanup(&path);
    }

    #[test]
    fn test_uncommitted_changes_not_visible() {
        let path = test_db_path("uncommitted");
        cleanup(&path);

        let mut db = Database::open(&path).expect("open should succeed");

        // Start write transaction but don't commit
        {
            let mut wtx = db.write_tx();
            wtx.put(b"uncommitted", b"value");
            // Drop without committing
        }

        // Changes should not be visible
        {
            let rtx = db.read_tx();
            let value = rtx.get(b"uncommitted");
            assert_eq!(value, None);
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence_across_reopen() {
        let path = test_db_path("persistence");
        cleanup(&path);

        // Write and close
        {
            let mut db = Database::open(&path).expect("open should succeed");
            let mut wtx = db.write_tx();
            wtx.put(b"persistent", b"data");
            wtx.commit().expect("commit should succeed");
        }

        // Reopen and verify
        {
            let db = Database::open(&path).expect("reopen should succeed");
            let rtx = db.read_tx();
            let value = rtx.get(b"persistent");
            assert_eq!(value, Some(b"data".to_vec()));
        }

        cleanup(&path);
    }
}
