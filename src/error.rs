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

    #[test]
    fn test_error_display() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
        let err = Error::FileOpen {
            path: PathBuf::from("/tmp/test.db"),
            source: io_err,
        };
        let display = format!("{err}");
        assert!(display.contains("failed to open database file"));
        assert!(display.contains("/tmp/test.db"));

        assert!(format!("{}", Error::KeyNotFound).contains("key not found"));
        assert!(format!("{}", Error::BothMetaPagesInvalid).contains("both meta pages"));
    }

    #[test]
    fn test_error_source() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "no access");
        let err = Error::FileOpen {
            path: PathBuf::from("/test"),
            source: io_err,
        };
        assert!(std::error::Error::source(&err).is_some());
        assert!(std::error::Error::source(&Error::KeyNotFound).is_none());
    }

    #[test]
    fn test_error_from_io() {
        let io_err = io::Error::new(io::ErrorKind::BrokenPipe, "pipe error");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }
}
