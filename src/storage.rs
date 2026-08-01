// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Storage abstraction with fault injection.
//!
//! All durable I/O goes through [`Storage`] so that WAL recovery (§4) can be
//! tested against injected torn writes and I/O errors instead of by killing
//! processes. A crash test that cannot be reproduced deterministically is not
//! a test.
//!
//! The trait is deliberately synchronous: the WAL batches writes on a dedicated
//! thread to control fsync grouping itself (§4), which async file I/O would
//! only obscure.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

/// Result of a storage operation.
pub type StorageResult<T> = Result<T, StorageError>;

/// Why a storage operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The underlying device or filesystem reported an error.
    #[error("i/o error on {path}: {source}")]
    Io {
        /// Object the operation targeted.
        path: PathBuf,
        /// Error as reported by the operating system.
        #[source]
        source: io::Error,
    },
    /// No object exists at this path.
    #[error("no such object: {0}")]
    NotFound(PathBuf),
}

impl StorageError {
    /// Wraps an [`io::Error`], mapping "not found" onto the dedicated variant
    /// so callers can distinguish an absent segment from a broken device.
    fn from_io(path: &Path, source: io::Error) -> Self {
        if source.kind() == io::ErrorKind::NotFound {
            Self::NotFound(path.to_path_buf())
        } else {
            Self::Io {
                path: path.to_path_buf(),
                source,
            }
        }
    }
}

/// Durable object store holding WAL segments and Parquet files.
///
/// Paths are relative to the store's root and use `/` separators.
pub trait Storage: fmt::Debug + Send + Sync {
    /// Opens `path` for appending, creating it if absent.
    ///
    /// Existing content is preserved — nothing in this crate truncates.
    fn open_append(&self, path: &Path) -> StorageResult<Box<dyn AppendFile>>;

    /// Reads an object in full.
    fn read(&self, path: &Path) -> StorageResult<Vec<u8>>;

    /// Lists objects directly beneath `prefix`, sorted by path.
    ///
    /// A missing prefix lists empty rather than failing: an archive with no
    /// WAL segments yet is a normal state, not an error.
    fn list(&self, prefix: &Path) -> StorageResult<Vec<PathBuf>>;

    /// Deletes an object. Removing an absent object succeeds.
    fn remove(&self, path: &Path) -> StorageResult<()>;
}

/// Append handle onto a single object.
pub trait AppendFile: fmt::Debug + Send {
    /// Appends `bytes`.
    ///
    /// Returning `Ok` does *not* mean the bytes are durable, nor even that all
    /// of them were written — a torn write reports success (§4). Only
    /// [`AppendFile::sync`] returning `Ok` establishes durability.
    fn append(&mut self, bytes: &[u8]) -> StorageResult<()>;

    /// Flushes everything appended so far to the device.
    fn sync(&mut self) -> StorageResult<()>;
}

/// A fault to apply to the next storage operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Keep only the first `keep` bytes of the next append, then report
    /// success — the shape a crash mid-write leaves behind.
    TornWrite {
        /// Number of leading bytes that reach storage.
        keep: usize,
    },
    /// Fail the next append or sync with this error kind.
    Fail(io::ErrorKind),
}

/// In-memory [`Storage`] test double with fault injection.
///
/// Cloning shares the same objects, so a handle can be kept beside the store
/// it writes to.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStorage {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    objects: BTreeMap<PathBuf, Vec<u8>>,
    faults: VecDeque<Fault>,
}

impl InMemoryStorage {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a fault for the next append or sync.
    ///
    /// Faults apply in the order injected, one operation each.
    pub fn inject(&self, fault: Fault) {
        self.lock().faults.push_back(fault);
    }

    /// Locks the shared state, recovering from a poisoned mutex.
    ///
    /// A panicking test must not cascade into unrelated failures, and the
    /// zero-panic policy rules out unwrapping the guard.
    fn lock(&self) -> MutexGuard<'_, MemoryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Storage for InMemoryStorage {
    fn open_append(&self, path: &Path) -> StorageResult<Box<dyn AppendFile>> {
        self.lock().objects.entry(path.to_path_buf()).or_default();
        Ok(Box::new(MemoryAppendFile {
            state: Arc::clone(&self.state),
            path: path.to_path_buf(),
        }))
    }

    fn read(&self, path: &Path) -> StorageResult<Vec<u8>> {
        self.lock()
            .objects
            .get(path)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))
    }

    fn list(&self, prefix: &Path) -> StorageResult<Vec<PathBuf>> {
        Ok(self
            .lock()
            .objects
            .keys()
            .filter(|path| path.parent() == Some(prefix))
            .cloned()
            .collect())
    }

    fn remove(&self, path: &Path) -> StorageResult<()> {
        self.lock().objects.remove(path);
        Ok(())
    }
}

/// Append handle onto an [`InMemoryStorage`] object.
#[derive(Debug)]
struct MemoryAppendFile {
    state: Arc<Mutex<MemoryState>>,
    path: PathBuf,
}

impl MemoryAppendFile {
    fn lock(&self) -> MutexGuard<'_, MemoryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl AppendFile for MemoryAppendFile {
    fn append(&mut self, bytes: &[u8]) -> StorageResult<()> {
        let mut state = self.lock();
        let written = match state.faults.pop_front() {
            Some(Fault::Fail(kind)) => {
                return Err(StorageError::Io {
                    path: self.path.clone(),
                    source: io::Error::from(kind),
                });
            }
            Some(Fault::TornWrite { keep }) => bytes.get(..keep.min(bytes.len())),
            None => Some(bytes),
        };
        if let Some(written) = written {
            state
                .objects
                .entry(self.path.clone())
                .or_default()
                .extend_from_slice(written);
        }
        Ok(())
    }

    fn sync(&mut self) -> StorageResult<()> {
        let mut state = self.lock();
        match state.faults.pop_front() {
            Some(Fault::Fail(kind)) => Err(StorageError::Io {
                path: self.path.clone(),
                source: io::Error::from(kind),
            }),
            Some(Fault::TornWrite { .. }) | None => Ok(()),
        }
    }
}

/// [`Storage`] backed by the local filesystem. The production implementation.
#[derive(Debug, Clone)]
pub struct FileStorage {
    root: PathBuf,
}

impl FileStorage {
    /// Creates a store rooted at `root`. Directories are created on demand.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Resolves a store-relative path against the root.
    fn resolve(&self, path: &Path) -> PathBuf {
        self.root.join(path)
    }
}

impl Storage for FileStorage {
    fn open_append(&self, path: &Path) -> StorageResult<Box<dyn AppendFile>> {
        let resolved = self.resolve(path);
        if let Some(parent) = resolved.parent() {
            fs::create_dir_all(parent).map_err(|e| StorageError::from_io(&resolved, e))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&resolved)
            .map_err(|e| StorageError::from_io(&resolved, e))?;
        Ok(Box::new(FileAppendFile {
            file,
            path: resolved,
        }))
    }

    fn read(&self, path: &Path) -> StorageResult<Vec<u8>> {
        let resolved = self.resolve(path);
        fs::read(&resolved).map_err(|e| StorageError::from_io(&resolved, e))
    }

    fn list(&self, prefix: &Path) -> StorageResult<Vec<PathBuf>> {
        let resolved = self.resolve(prefix);
        let entries = match fs::read_dir(&resolved) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(StorageError::from_io(&resolved, e)),
        };

        let mut listed = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| StorageError::from_io(&resolved, e))?;
            listed.push(prefix.join(entry.file_name()));
        }
        listed.sort();
        Ok(listed)
    }

    fn remove(&self, path: &Path) -> StorageResult<()> {
        let resolved = self.resolve(path);
        match fs::remove_file(&resolved) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::from_io(&resolved, e)),
        }
    }
}

/// Append handle onto a file on disk.
#[derive(Debug)]
struct FileAppendFile {
    file: File,
    path: PathBuf,
}

impl AppendFile for FileAppendFile {
    fn append(&mut self, bytes: &[u8]) -> StorageResult<()> {
        self.file
            .write_all(bytes)
            .map_err(|e| StorageError::from_io(&self.path, e))
    }

    fn sync(&mut self) -> StorageResult<()> {
        self.file
            .sync_all()
            .map_err(|e| StorageError::from_io(&self.path, e))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::io::ErrorKind;
    use std::path::Path;

    fn read_back(storage: &InMemoryStorage, path: &str) -> Vec<u8> {
        storage.read(Path::new(path)).expect("object should exist")
    }

    #[test]
    fn appended_bytes_read_back_in_order() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("wal/000001")).unwrap();
        file.append(b"hello ").unwrap();
        file.append(b"world").unwrap();
        file.sync().unwrap();

        assert_eq!(read_back(&storage, "wal/000001"), b"hello world");
    }

    /// §4: a crash mid-write leaves a prefix on disk. Reproducing that by
    /// killing a process is not a test — it has to be injectable.
    #[test]
    fn torn_write_persists_only_the_prefix() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("wal/000001")).unwrap();
        file.append(b"intact").unwrap();

        storage.inject(Fault::TornWrite { keep: 3 });
        file.append(b"truncated").unwrap();
        file.sync().unwrap();

        assert_eq!(read_back(&storage, "wal/000001"), b"intacttru");
    }

    /// A torn write reports success — the caller believed it wrote. That is
    /// exactly why WAL replay cannot trust the tail (§4).
    #[test]
    fn torn_write_does_not_report_an_error() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("wal/000001")).unwrap();
        storage.inject(Fault::TornWrite { keep: 1 });

        assert!(file.append(b"abc").is_ok());
    }

    #[test]
    fn injected_failure_surfaces_from_append() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("wal/000001")).unwrap();
        storage.inject(Fault::Fail(ErrorKind::StorageFull));

        let error = file.append(b"data").expect_err("append should fail");
        assert!(matches!(error, StorageError::Io { .. }));
    }

    #[test]
    fn injected_failure_surfaces_from_sync() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("wal/000001")).unwrap();
        file.append(b"data").unwrap();
        storage.inject(Fault::Fail(ErrorKind::Interrupted));

        assert!(file.sync().is_err());
    }

    #[test]
    fn a_fault_applies_only_to_the_next_operation() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("wal/000001")).unwrap();
        storage.inject(Fault::Fail(ErrorKind::Interrupted));

        assert!(file.append(b"lost").is_err());
        assert!(file.append(b"kept").is_ok());
        assert_eq!(read_back(&storage, "wal/000001"), b"kept");
    }

    #[test]
    fn reading_a_missing_object_reports_not_found() {
        let storage = InMemoryStorage::new();
        let error = storage
            .read(Path::new("wal/absent"))
            .expect_err("read should fail");
        assert!(matches!(error, StorageError::NotFound(_)));
    }

    #[test]
    fn list_reports_objects_under_a_prefix_in_order() {
        let storage = InMemoryStorage::new();
        for name in ["wal/000002", "wal/000001", "data/part-0"] {
            storage
                .open_append(Path::new(name))
                .unwrap()
                .sync()
                .unwrap();
        }

        let listed = storage.list(Path::new("wal")).unwrap();
        assert_eq!(
            listed,
            vec![PathBuf::from("wal/000001"), PathBuf::from("wal/000002")]
        );
    }

    #[test]
    fn removed_objects_disappear() {
        let storage = InMemoryStorage::new();
        storage
            .open_append(Path::new("wal/000001"))
            .unwrap()
            .sync()
            .unwrap();

        storage.remove(Path::new("wal/000001")).unwrap();
        assert!(storage.list(Path::new("wal")).unwrap().is_empty());
    }

    #[test]
    fn storage_is_object_safe() {
        let storage: Box<dyn Storage> = Box::new(InMemoryStorage::new());
        assert!(storage.list(Path::new("wal")).unwrap().is_empty());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod file_tests {
    use super::*;
    use std::path::Path;

    /// The in-memory implementation is only a useful test double if the real
    /// one behaves the same way.
    #[test]
    fn file_storage_round_trips_appends() {
        let root = tempfile::tempdir().expect("temp dir");
        let storage = FileStorage::new(root.path());

        let mut file = storage.open_append(Path::new("wal/000001")).unwrap();
        file.append(b"hello ").unwrap();
        file.append(b"world").unwrap();
        file.sync().unwrap();

        assert_eq!(
            storage.read(Path::new("wal/000001")).unwrap(),
            b"hello world"
        );
    }

    #[test]
    fn file_storage_reopens_for_append_without_truncating() {
        let root = tempfile::tempdir().expect("temp dir");
        let storage = FileStorage::new(root.path());

        storage
            .open_append(Path::new("wal/000001"))
            .unwrap()
            .append(b"first")
            .unwrap();
        storage
            .open_append(Path::new("wal/000001"))
            .unwrap()
            .append(b"second")
            .unwrap();

        assert_eq!(
            storage.read(Path::new("wal/000001")).unwrap(),
            b"firstsecond"
        );
    }

    #[test]
    fn file_storage_lists_and_removes() {
        let root = tempfile::tempdir().expect("temp dir");
        let storage = FileStorage::new(root.path());
        for name in ["wal/000002", "wal/000001"] {
            storage
                .open_append(Path::new(name))
                .unwrap()
                .sync()
                .unwrap();
        }

        assert_eq!(storage.list(Path::new("wal")).unwrap().len(), 2);
        storage.remove(Path::new("wal/000001")).unwrap();
        assert_eq!(
            storage.list(Path::new("wal")).unwrap(),
            vec![PathBuf::from("wal/000002")]
        );
    }

    #[test]
    fn file_storage_reports_missing_objects_as_not_found() {
        let root = tempfile::tempdir().expect("temp dir");
        let storage = FileStorage::new(root.path());

        let error = storage
            .read(Path::new("wal/absent"))
            .expect_err("read should fail");
        assert!(matches!(error, StorageError::NotFound(_)));
    }
}
