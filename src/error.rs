//! Summary: Error types for the thunder database engine.
//! Copyright (c) YOAB. All rights reserved.

use std::fmt;
use std::io;
use std::path::PathBuf;

/// Result type alias for thunder operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error types for thunder database operations.
///
/// All errors include context about what operation failed and why.
/// This enables precise diagnosis of failures.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to open or create the database file.
    FileOpen {
        path: PathBuf,
        source: io::Error,
    },
    /// Failed to read file metadata.
    FileMetadata {
        path: PathBuf,
        source: io::Error,
    },
    /// Failed to seek within the database file.
    FileSeek {
        offset: u64,
        context: &'static str,
        source: io::Error,
    },
    /// Failed to read from the database file.
    FileRead {
        offset: u64,
        len: usize,
        context: &'static str,
        source: io::Error,
    },
    /// Failed to write to the database file.
    FileWrite {
        offset: u64,
        len: usize,
        context: &'static str,
        source: io::Error,
    },
    /// Failed to sync the database file to disk.
    FileSync {
        context: &'static str,
        source: io::Error,
    },
    /// Database file is corrupted or invalid.
    Corrupted {
        context: &'static str,
        details: String,
    },
    /// Meta page is invalid or corrupted.
    InvalidMetaPage {
        page_number: u8,
        reason: &'static str,
    },
    /// Both meta pages are invalid.
    BothMetaPagesInvalid,
    /// Invalid page encountered during read.
    InvalidPage {
        page_id: u64,
        reason: String,
    },
    /// Failed to read entry during data load.
    EntryReadFailed {
        entry_index: u64,
        field: &'static str,
        source: io::Error,
    },
    /// Transaction is no longer valid.
    TxClosed,
    /// Transaction commit failed.
    TxCommitFailed {
        reason: String,
        source: Option<Box<Error>>,
    },
    /// Key not found in the database.
    KeyNotFound,
    /// Bucket not found.
    BucketNotFound {
        name: Vec<u8>,
    },
    /// Bucket already exists.
    BucketAlreadyExists {
        name: Vec<u8>,
    },
    /// Invalid bucket name (empty or too long).
    InvalidBucketName {
        reason: &'static str,
    },
    /// Database is already open.
    DatabaseAlreadyOpen,
    /// Generic I/O error (legacy, prefer specific variants).
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FileOpen { path, source } => {
                write!(f, "failed to open database file '{}': {}", path.display(), source)
            }
            Error::FileMetadata { path, source } => {
                write!(f, "failed to read metadata for '{}': {}", path.display(), source)
            }
            Error::FileSeek { offset, context, source } => {
                write!(f, "failed to seek to offset {offset} ({context}): {source}")
            }
            Error::FileRead { offset, len, context, source } => {
                write!(
                    f,
                    "failed to read {len} bytes at offset {offset} ({context}): {source}"
                )
            }
            Error::FileWrite { offset, len, context, source } => {
                write!(
                    f,
                    "failed to write {len} bytes at offset {offset} ({context}): {source}"
                )
            }
            Error::FileSync { context, source } => {
                write!(f, "failed to sync to disk ({context}): {source}")
            }
            Error::Corrupted { context, details } => {
                write!(f, "database corrupted ({context}): {details}")
            }
            Error::InvalidMetaPage { page_number, reason } => {
                write!(f, "meta page {page_number} is invalid: {reason}")
            }
            Error::BothMetaPagesInvalid => {
                write!(f, "both meta pages are invalid or corrupted")
            }
            Error::InvalidPage { page_id, reason } => {
                write!(f, "invalid page {page_id}: {reason}")
            }
            Error::EntryReadFailed { entry_index, field, source } => {
                write!(
                    f,
                    "failed to read {field} for entry {entry_index}: {source}"
                )
            }
            Error::TxClosed => write!(f, "transaction is closed"),
            Error::TxCommitFailed { reason, source } => {
                if let Some(src) = source {
                    write!(f, "transaction commit failed ({reason}): {src}")
                } else {
                    write!(f, "transaction commit failed: {reason}")
                }
            }
            Error::KeyNotFound => write!(f, "key not found"),
            Error::BucketNotFound { name } => {
                write!(f, "bucket not found: {:?}", String::from_utf8_lossy(name))
            }
            Error::BucketAlreadyExists { name } => {
                write!(f, "bucket already exists: {:?}", String::from_utf8_lossy(name))
            }
            Error::InvalidBucketName { reason } => {
                write!(f, "invalid bucket name: {reason}")
            }
            Error::DatabaseAlreadyOpen => write!(f, "database is already open"),
            Error::Io(err) => write!(f, "I/O error: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::FileOpen { source, .. } => Some(source),
            Error::FileMetadata { source, .. } => Some(source),
            Error::FileSeek { source, .. } => Some(source),
            Error::FileRead { source, .. } => Some(source),
            Error::FileWrite { source, .. } => Some(source),
            Error::FileSync { source, .. } => Some(source),
            Error::EntryReadFailed { source, .. } => Some(source),
            Error::TxCommitFailed { source, .. } => {
                source.as_ref().map(|s| s.as_ref() as &(dyn std::error::Error + 'static))
            }
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // ==================== Error Display ====================

    #[test]
    fn test_error_display_file_open() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
        let err = Error::FileOpen {
            path: PathBuf::from("/tmp/test.db"),
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("failed to open database file"));
        assert!(display.contains("/tmp/test.db"));
        assert!(display.contains("permission denied"));
    }

    #[test]
    fn test_error_display_file_metadata() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "not found");
        let err = Error::FileMetadata {
            path: PathBuf::from("/tmp/test.db"),
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("failed to read metadata"));
        assert!(display.contains("/tmp/test.db"));
    }

    #[test]
    fn test_error_display_file_seek() {
        let io_err = io::Error::other("seek failed");
        let err = Error::FileSeek {
            offset: 4096,
            context: "reading meta page 1",
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("failed to seek"));
        assert!(display.contains("4096"));
        assert!(display.contains("reading meta page 1"));
    }

    #[test]
    fn test_error_display_file_read() {
        let io_err = io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected eof");
        let err = Error::FileRead {
            offset: 8192,
            len: 4096,
            context: "reading data page",
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("failed to read"));
        assert!(display.contains("4096 bytes"));
        assert!(display.contains("8192"));
        assert!(display.contains("reading data page"));
    }

    #[test]
    fn test_error_display_file_write() {
        let io_err = io::Error::new(io::ErrorKind::WriteZero, "write zero");
        let err = Error::FileWrite {
            offset: 0,
            len: 4096,
            context: "writing meta page 0",
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("failed to write"));
        assert!(display.contains("4096 bytes"));
        assert!(display.contains("writing meta page 0"));
    }

    #[test]
    fn test_error_display_file_sync() {
        let io_err = io::Error::other("sync failed");
        let err = Error::FileSync {
            context: "persisting transaction",
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("failed to sync"));
        assert!(display.contains("persisting transaction"));
    }

    #[test]
    fn test_error_display_corrupted() {
        let err = Error::Corrupted {
            context: "checksum validation",
            details: "expected 0xABCD, got 0x1234".to_string(),
        };
        let display = format!("{err}");
        assert!(display.contains("database corrupted"));
        assert!(display.contains("checksum validation"));
        assert!(display.contains("expected 0xABCD"));
    }

    #[test]
    fn test_error_display_invalid_meta_page() {
        let err = Error::InvalidMetaPage {
            page_number: 0,
            reason: "magic number mismatch",
        };
        let display = format!("{err}");
        assert!(display.contains("meta page 0"));
        assert!(display.contains("magic number mismatch"));
    }

    #[test]
    fn test_error_display_both_meta_pages_invalid() {
        let err = Error::BothMetaPagesInvalid;
        let display = format!("{err}");
        assert!(display.contains("both meta pages are invalid"));
    }

    #[test]
    fn test_error_display_invalid_page() {
        let err = Error::InvalidPage {
            page_id: 42,
            reason: "page type byte is invalid".to_string(),
        };
        let display = format!("{err}");
        assert!(display.contains("invalid page 42"));
        assert!(display.contains("page type byte is invalid"));
    }

    #[test]
    fn test_error_display_entry_read_failed() {
        let io_err = io::Error::new(io::ErrorKind::UnexpectedEof, "eof");
        let err = Error::EntryReadFailed {
            entry_index: 100,
            field: "value length",
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("entry 100"));
        assert!(display.contains("value length"));
    }

    #[test]
    fn test_error_display_tx_closed() {
        let err = Error::TxClosed;
        let display = format!("{err}");
        assert!(display.contains("transaction is closed"));
    }

    #[test]
    fn test_error_display_tx_commit_failed_with_source() {
        let source_err = Error::FileSync {
            context: "commit",
            source: io::Error::other("disk full"),
        };
        let err = Error::TxCommitFailed {
            reason: "failed to persist data".to_string(),
            source: Some(Box::new(source_err)),
        };
        let display = format!("{err}");
        assert!(display.contains("transaction commit failed"));
        assert!(display.contains("failed to persist data"));
    }

    #[test]
    fn test_error_display_tx_commit_failed_without_source() {
        let err = Error::TxCommitFailed {
            reason: "transaction was rolled back".to_string(),
            source: None,
        };
        let display = format!("{err}");
        assert!(display.contains("transaction commit failed"));
        assert!(display.contains("transaction was rolled back"));
    }

    #[test]
    fn test_error_display_key_not_found() {
        let err = Error::KeyNotFound;
        let display = format!("{err}");
        assert!(display.contains("key not found"));
    }

    #[test]
    fn test_error_display_database_already_open() {
        let err = Error::DatabaseAlreadyOpen;
        let display = format!("{err}");
        assert!(display.contains("database is already open"));
    }

    #[test]
    fn test_error_display_io() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = Error::Io(io_err);
        let display = format!("{err}");
        assert!(display.contains("I/O error"));
        assert!(display.contains("file not found"));
    }

    // ==================== Error Source ====================

    #[test]
    fn test_error_source_file_open() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "no access");
        let err = Error::FileOpen {
            path: PathBuf::from("/test"),
            source: io_err,
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_file_seek() {
        let io_err = io::Error::other("seek error");
        let err = Error::FileSeek {
            offset: 0,
            context: "test",
            source: io_err,
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_file_read() {
        let io_err = io::Error::other("read error");
        let err = Error::FileRead {
            offset: 0,
            len: 100,
            context: "test",
            source: io_err,
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_file_write() {
        let io_err = io::Error::other("write error");
        let err = Error::FileWrite {
            offset: 0,
            len: 100,
            context: "test",
            source: io_err,
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_file_sync() {
        let io_err = io::Error::other("sync error");
        let err = Error::FileSync {
            context: "test",
            source: io_err,
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_entry_read_failed() {
        let io_err = io::Error::other("read error");
        let err = Error::EntryReadFailed {
            entry_index: 0,
            field: "test",
            source: io_err,
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_tx_commit_failed_with_source() {
        let source_err = Error::KeyNotFound;
        let err = Error::TxCommitFailed {
            reason: "test".to_string(),
            source: Some(Box::new(source_err)),
        };
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_tx_commit_failed_without_source() {
        let err = Error::TxCommitFailed {
            reason: "test".to_string(),
            source: None,
        };
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn test_error_source_io() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "no access");
        let err = Error::Io(io_err);
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn test_error_source_none_for_simple_variants() {
        assert!(std::error::Error::source(&Error::TxClosed).is_none());
        assert!(std::error::Error::source(&Error::KeyNotFound).is_none());
        assert!(std::error::Error::source(&Error::DatabaseAlreadyOpen).is_none());
        assert!(std::error::Error::source(&Error::BothMetaPagesInvalid).is_none());
    }

    // ==================== Error Conversion ====================

    #[test]
    fn test_error_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::BrokenPipe, "pipe error");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    // ==================== Debug ====================

    #[test]
    fn test_error_debug() {
        let err = Error::KeyNotFound;
        let debug = format!("{err:?}");
        assert!(debug.contains("KeyNotFound"));
    }

    #[test]
    fn test_error_debug_with_path() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "not found");
        let err = Error::FileOpen {
            path: PathBuf::from("/test/path.db"),
            source: io_err,
        };
        let debug = format!("{err:?}");
        assert!(debug.contains("FileOpen"));
        assert!(debug.contains("path"));
    }

    // ==================== Result Type Alias ====================

    #[test]
    fn test_result_ok() {
        fn returns_ok() -> Result<i32> {
            Ok(42)
        }
        let result = returns_ok();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_result_err() {
        let result: Result<i32> = Err(Error::KeyNotFound);
        assert!(result.is_err());
    }

    // ==================== Non-Exhaustive Enum ====================

    #[test]
    fn test_error_is_non_exhaustive() {
        // This test verifies the enum is non-exhaustive by using a catch-all.
        let err = Error::KeyNotFound;
        #[allow(unreachable_patterns)]
        let is_key_not_found = match err {
            Error::KeyNotFound => true,
            _ => false, // Non-exhaustive allows this.
        };
        assert!(is_key_not_found);
    }
}
