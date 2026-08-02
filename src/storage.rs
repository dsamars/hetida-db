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

    /// Reads `range` of an object, clamped to its end.
    ///
    /// A Parquet reader opens a file by fetching its footer and then only the
    /// column chunks the query needs (§5, §11). Serving those from a full read
    /// each time would make a scan quadratic in the file size, so this is the
    /// operation the query path uses and `read` is not.
    ///
    /// A range starting past the end yields no bytes rather than failing: that
    /// is what a reader asking beyond a shorter-than-expected object should see.
    fn read_range(&self, path: &Path, range: std::ops::Range<u64>) -> StorageResult<Vec<u8>>;

    /// Size of an object in bytes, or `None` if `path` is not one.
    ///
    /// `None` covers both an absent path and a directory, which is exactly the
    /// distinction a walk over the layout needs: an entry with a size is a file
    /// to scan, an entry without one is a level to descend into (§11).
    fn size(&self, path: &Path) -> StorageResult<Option<u64>>;

    /// Lists entries directly beneath `prefix`, sorted by path.
    ///
    /// Directory semantics, as `read_dir` has them: an object nested deeper is
    /// reported as the directory containing it. A caller can therefore tell an
    /// empty prefix from a populated one without walking the whole tree, which
    /// is what the startup layout check needs (§11).
    ///
    /// A missing prefix lists empty rather than failing: an archive with no
    /// WAL segments yet is a normal state, not an error.
    fn list(&self, prefix: &Path) -> StorageResult<Vec<PathBuf>>;

    /// Deletes an object. Removing an absent object succeeds.
    fn remove(&self, path: &Path) -> StorageResult<()>;

    /// Deletes `prefix` and everything beneath it.
    ///
    /// This is how retention removes expired data (§8): a whole partition
    /// directory at once, never rows out of a file. Removing an absent prefix
    /// succeeds — a reaper racing a concurrent pass finds the desired state
    /// already true.
    ///
    /// `prefix` matches on whole path components, so dropping `month=07` never
    /// takes a `month=07-old` beside it.
    fn remove_dir_all(&self, prefix: &Path) -> StorageResult<()>;

    /// Moves `from` onto `to`, replacing `to` if it exists.
    ///
    /// Atomic: a reader sees either the old object or the whole new one, never a
    /// partial write. This is what lets a Parquet file be published only once its
    /// footer is there — [`Storage::open_append`] makes a name visible
    /// immediately, so writing under the final name would expose a half file
    /// that a reader would try to open.
    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()>;
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
    /// Fail the next append, sync or directory removal with this error kind.
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
    syncs: usize,
    bytes_read: usize,
}

impl InMemoryStorage {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// How many times any handle onto this store has been synced.
    ///
    /// Lets a test observe fsync grouping (§4), which is otherwise invisible.
    pub fn syncs(&self) -> usize {
        self.lock().syncs
    }

    /// Bytes any handle has read out of this store through a range read.
    ///
    /// Lets a test measure pruning the way §11 asks it to be measured — by
    /// bytes scanned, not by wall time, which would be a benchmark and a flaky
    /// test. Whole-object reads are excluded: the query path uses ranges, and
    /// counting the compactor's reads would drown the signal.
    pub fn bytes_read(&self) -> usize {
        self.lock().bytes_read
    }

    /// Forgets what has been read so far, so a test can measure one query.
    pub fn reset_bytes_read(&self) {
        self.lock().bytes_read = 0;
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

    fn read_range(&self, path: &Path, range: std::ops::Range<u64>) -> StorageResult<Vec<u8>> {
        let bytes = self.read(path)?;
        let start = usize::try_from(range.start)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let end = usize::try_from(range.end)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let served = bytes
            .get(start..end.max(start))
            .unwrap_or_default()
            .to_vec();
        self.lock().bytes_read += served.len();
        Ok(served)
    }

    fn size(&self, path: &Path) -> StorageResult<Option<u64>> {
        Ok(self
            .lock()
            .objects
            .get(path)
            .map(|bytes| bytes.len() as u64))
    }

    fn list(&self, prefix: &Path) -> StorageResult<Vec<PathBuf>> {
        // Mirrors `read_dir`: an object nested deeper appears as the directory
        // that contains it, not as itself and not as nothing. A flat store that
        // reported only direct children would make a nested layout invisible
        // here and visible in production — the one thing this double must not do.
        let mut listed: Vec<PathBuf> = self
            .lock()
            .objects
            .keys()
            .filter_map(|path| {
                let below = path.strip_prefix(prefix).ok()?;
                Some(prefix.join(below.components().next()?))
            })
            .collect();

        listed.dedup();
        Ok(listed)
    }

    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()> {
        let mut guard = self.lock();
        let bytes = guard
            .objects
            .remove(from)
            .ok_or_else(|| StorageError::NotFound(from.to_path_buf()))?;
        guard.objects.insert(to.to_path_buf(), bytes);
        Ok(())
    }

    fn remove(&self, path: &Path) -> StorageResult<()> {
        self.lock().objects.remove(path);
        Ok(())
    }

    fn remove_dir_all(&self, prefix: &Path) -> StorageResult<()> {
        let mut state = self.lock();
        // Injectable, because §8's reaper must count a failed drop and carry on
        // rather than abandon the pass — untestable otherwise.
        if let Some(Fault::Fail(kind)) = state.faults.pop_front() {
            return Err(StorageError::Io {
                path: prefix.to_path_buf(),
                source: io::Error::from(kind),
            });
        }
        // `strip_prefix` matches whole components, so `year=2026` never takes
        // `year=2026-old` with it — the sibling the string form would eat.
        state
            .objects
            .retain(|path, _| path.strip_prefix(prefix).is_err());
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
        if let Some(Fault::Fail(kind)) = state.faults.pop_front() {
            return Err(StorageError::Io {
                path: self.path.clone(),
                source: io::Error::from(kind),
            });
        }
        state.syncs += 1;
        Ok(())
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

    fn read_range(&self, path: &Path, range: std::ops::Range<u64>) -> StorageResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let resolved = self.resolve(path);
        let mut file = File::open(&resolved).map_err(|e| StorageError::from_io(&resolved, e))?;
        file.seek(SeekFrom::Start(range.start))
            .map_err(|e| StorageError::from_io(&resolved, e))?;

        // `take` clamps at the end of the file, so a range reaching past it
        // yields what is there rather than failing on a short read.
        let mut bytes = Vec::new();
        file.take(range.end.saturating_sub(range.start))
            .read_to_end(&mut bytes)
            .map_err(|e| StorageError::from_io(&resolved, e))?;
        Ok(bytes)
    }

    fn size(&self, path: &Path) -> StorageResult<Option<u64>> {
        let resolved = self.resolve(path);
        match fs::metadata(&resolved) {
            Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
            // A directory is not an object, and neither is an absent path.
            Ok(_) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::from_io(&resolved, e)),
        }
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

    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()> {
        let source = self.resolve(from);
        let target = self.resolve(to);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|e| StorageError::from_io(parent, e))?;
        }
        fs::rename(&source, &target).map_err(|e| StorageError::from_io(&source, e))
    }

    fn remove(&self, path: &Path) -> StorageResult<()> {
        let resolved = self.resolve(path);
        match fs::remove_file(&resolved) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::from_io(&resolved, e)),
        }
    }

    fn remove_dir_all(&self, prefix: &Path) -> StorageResult<()> {
        let resolved = self.resolve(prefix);
        match fs::remove_dir_all(&resolved) {
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable
)]
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

    /// The double has to answer like `read_dir`, or a nested layout would be
    /// invisible under test and present in production. The startup layout check
    /// (§11) rests on exactly this.
    #[test]
    fn list_reports_a_nested_object_as_the_directory_holding_it() {
        let storage = InMemoryStorage::new();
        for name in [
            "data/year=2026/month=08/part-0.parquet",
            "data/year=2026/month=09/part-0.parquet",
            "data/year=2027/month=01/part-0.parquet",
        ] {
            storage
                .open_append(Path::new(name))
                .unwrap()
                .sync()
                .unwrap();
        }

        assert_eq!(
            storage.list(Path::new("data")).unwrap(),
            vec![
                PathBuf::from("data/year=2026"),
                PathBuf::from("data/year=2027")
            ],
            "each year must appear once, however many files sit below it"
        );
        assert_eq!(
            storage.list(Path::new("data/year=2026")).unwrap(),
            vec![
                PathBuf::from("data/year=2026/month=08"),
                PathBuf::from("data/year=2026/month=09")
            ]
        );
    }

    #[test]
    fn list_of_an_unpopulated_prefix_is_empty() {
        let storage = InMemoryStorage::new();
        storage
            .open_append(Path::new("wal/000001"))
            .unwrap()
            .sync()
            .unwrap();

        assert!(storage.list(Path::new("data")).unwrap().is_empty());
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

    /// §5: a Parquet reader opens a file by reading its footer and then the
    /// column chunks it needs. Reading the whole object for each of those would
    /// make a scan quadratic in the file size, so the store answers ranges.
    #[test]
    fn a_range_reads_only_the_bytes_asked_for() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("data/part-0")).unwrap();
        file.append(b"0123456789").unwrap();
        file.sync().unwrap();

        assert_eq!(
            storage.read_range(Path::new("data/part-0"), 2..5).unwrap(),
            b"234"
        );
        assert_eq!(
            storage.read_range(Path::new("data/part-0"), 8..10).unwrap(),
            b"89",
            "a range reaching the end must be whole"
        );
    }

    /// Clamped rather than failing: a reader asking past the end gets what is
    /// there, which is what a short object at the tail of a range looks like.
    #[test]
    fn a_range_past_the_end_is_clamped() {
        let storage = InMemoryStorage::new();
        let mut file = storage.open_append(Path::new("data/part-0")).unwrap();
        file.append(b"abc").unwrap();
        file.sync().unwrap();

        assert_eq!(
            storage.read_range(Path::new("data/part-0"), 1..99).unwrap(),
            b"bc"
        );
        assert!(
            storage
                .read_range(Path::new("data/part-0"), 9..99)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_range_of_a_missing_object_reports_not_found() {
        let storage = InMemoryStorage::new();
        assert!(matches!(
            storage.read_range(Path::new("absent"), 0..1),
            Err(StorageError::NotFound(_))
        ));
    }

    /// §5: the scan needs each file's size to plan its reads, and needs to tell
    /// a file from a directory while walking the layout. One answer does both:
    /// a directory has no size.
    #[test]
    fn size_reports_objects_and_denies_directories() {
        let storage = InMemoryStorage::new();
        let mut file = storage
            .open_append(Path::new("data/year=2026/part-0"))
            .unwrap();
        file.append(b"12345").unwrap();
        file.sync().unwrap();

        assert_eq!(
            storage.size(Path::new("data/year=2026/part-0")).unwrap(),
            Some(5)
        );
        assert_eq!(
            storage.size(Path::new("data/year=2026")).unwrap(),
            None,
            "a directory is not an object"
        );
        assert_eq!(storage.size(Path::new("absent")).unwrap(), None);
    }

    /// §8: retention drops a whole partition directory. Removing the files one
    /// by one would leave the directory behind, and the archive's shape is its
    /// directory listing (§11).
    #[test]
    fn remove_dir_all_drops_a_whole_partition() {
        let storage = InMemoryStorage::new();
        for name in [
            "data/readings/year=2026/month=07/part-0.parquet",
            "data/readings/year=2026/month=07/part-1.parquet",
            "data/readings/year=2026/month=08/part-0.parquet",
        ] {
            storage
                .open_append(Path::new(name))
                .unwrap()
                .sync()
                .unwrap();
        }

        storage
            .remove_dir_all(Path::new("data/readings/year=2026/month=07"))
            .unwrap();

        assert_eq!(
            storage.list(Path::new("data/readings/year=2026")).unwrap(),
            vec![PathBuf::from("data/readings/year=2026/month=08")],
            "the dropped partition must be gone, the neighbour untouched"
        );
    }

    /// The reaper may race a concurrent pass or a manual cleanup, so an absent
    /// directory is not an error — the desired state is already the case.
    #[test]
    fn removing_an_absent_directory_succeeds() {
        let storage = InMemoryStorage::new();
        assert!(
            storage
                .remove_dir_all(Path::new("data/readings/year=1999"))
                .is_ok()
        );
    }

    /// A prefix must be a whole path component, or dropping `month=07` would
    /// also take `month=07-old` with it.
    #[test]
    fn remove_dir_all_does_not_take_a_sibling_sharing_a_name_prefix() {
        let storage = InMemoryStorage::new();
        for name in ["data/t/year=2026/part-0", "data/t/year=2026-old/part-0"] {
            storage
                .open_append(Path::new(name))
                .unwrap()
                .sync()
                .unwrap();
        }

        storage
            .remove_dir_all(Path::new("data/t/year=2026"))
            .unwrap();

        assert_eq!(
            storage.list(Path::new("data/t")).unwrap(),
            vec![PathBuf::from("data/t/year=2026-old")]
        );
    }

    #[test]
    fn storage_is_object_safe() {
        let storage: Box<dyn Storage> = Box::new(InMemoryStorage::new());
        assert!(storage.list(Path::new("wal")).unwrap().is_empty());
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable
)]
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

    /// The double is only useful if the real store answers ranges and sizes the
    /// same way (§5).
    #[test]
    fn file_storage_reads_ranges_and_reports_sizes() {
        let root = tempfile::tempdir().expect("temp dir");
        let storage = FileStorage::new(root.path());
        let mut file = storage.open_append(Path::new("data/y=1/part-0")).unwrap();
        file.append(b"0123456789").unwrap();
        file.sync().unwrap();

        assert_eq!(
            storage
                .read_range(Path::new("data/y=1/part-0"), 2..5)
                .unwrap(),
            b"234"
        );
        assert_eq!(
            storage
                .read_range(Path::new("data/y=1/part-0"), 8..99)
                .unwrap(),
            b"89",
            "a range past the end must clamp, not fail"
        );
        assert_eq!(
            storage.size(Path::new("data/y=1/part-0")).unwrap(),
            Some(10)
        );
        assert_eq!(
            storage.size(Path::new("data/y=1")).unwrap(),
            None,
            "a directory is not an object"
        );
        assert_eq!(storage.size(Path::new("data/absent")).unwrap(), None);
    }

    /// The in-memory double is only useful if the real store drops a directory
    /// the same way (§8).
    #[test]
    fn file_storage_removes_a_whole_directory() {
        let root = tempfile::tempdir().expect("temp dir");
        let storage = FileStorage::new(root.path());
        for name in [
            "data/t/year=2026/month=07/part-0",
            "data/t/year=2026/month=08/part-0",
        ] {
            storage
                .open_append(Path::new(name))
                .unwrap()
                .sync()
                .unwrap();
        }

        storage
            .remove_dir_all(Path::new("data/t/year=2026/month=07"))
            .unwrap();

        assert_eq!(
            storage.list(Path::new("data/t/year=2026")).unwrap(),
            vec![PathBuf::from("data/t/year=2026/month=08")],
            "the directory itself must be gone, not just its files"
        );
        assert!(
            storage
                .remove_dir_all(Path::new("data/t/year=2026/month=07"))
                .is_ok(),
            "dropping it again must succeed"
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
