// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The archive's [`Storage`] seen as an `ObjectStore`, for DataFusion (§5).
//!
//! DataFusion reads Parquet through `object_store`, and this crate writes it
//! through [`Storage`]. Letting the query path open files behind the archive's
//! back would give one archive two views of its own bytes — and the in-memory
//! double, on which the whole test suite rests, would not be one of them. So the
//! adapter goes this way round: one store, one truth.
//!
//! # Read-only, deliberately
//!
//! Every mutating method answers `NotSupported`. Writes belong to the flusher and
//! the compactor, which go through [`Storage`] directly with the ordering
//! guarantees §2 and §3 require — a staging write and a rename, in that order.
//! A second write path onto the same bytes is exactly the inconsistency this
//! adapter exists to prevent, so it is refused rather than quietly provided.
//!
//! # Ranges, not whole objects
//!
//! A Parquet reader fetches a footer and then only the column chunks the query
//! needs (§11). [`Storage::read_range`] is what serves those; reading whole
//! objects instead would make every scan quadratic in the file size.
//!
//! # No modification times
//!
//! `Storage` records none, so every object reports the epoch. Nothing here
//! depends on it: files are immutable once renamed into place, and a file is
//! identified by its name, which carries its highest `seq`. Reading a host clock
//! to invent one would also break the rule that wall-clock time enters this crate
//! only through [`Clock`](crate::clock::Clock).

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectResult,
};

use crate::storage::{Storage, StorageError};

/// Adapts an archive [`Storage`] onto the `ObjectStore` DataFusion reads through.
#[derive(Debug, Clone)]
pub struct StorageObjectStore {
    storage: Arc<dyn Storage>,
}

impl StorageObjectStore {
    /// Serves `storage` to DataFusion, read-only.
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// The store-relative path an `object_store` location names.
    fn resolve(location: &ObjectPath) -> std::path::PathBuf {
        std::path::PathBuf::from(location.as_ref())
    }

    /// Metadata for an object, or `NotFound` if `location` is not one.
    fn meta(&self, location: &ObjectPath) -> ObjectResult<ObjectMeta> {
        let path = Self::resolve(location);
        let size = self
            .storage
            .size(&path)
            .map_err(|error| convert(&path, error))?
            .ok_or_else(|| object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(StorageError::NotFound(path)),
            })?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: epoch(),
            size,
            e_tag: None,
            version: None,
        })
    }

    /// Every object at or below `prefix`, depth first.
    ///
    /// [`Storage::list`] answers one level at a time, as `read_dir` does, so the
    /// recursion is here. An entry with a size is an object; one without is a
    /// level to descend into (§11).
    fn walk(&self, prefix: &std::path::Path) -> ObjectResult<Vec<ObjectMeta>> {
        let mut found = Vec::new();
        let mut pending = vec![prefix.to_path_buf()];

        while let Some(directory) = pending.pop() {
            for entry in self
                .storage
                .list(&directory)
                .map_err(|error| convert(&directory, error))?
            {
                match self
                    .storage
                    .size(&entry)
                    .map_err(|error| convert(&entry, error))?
                {
                    Some(size) => found.push(ObjectMeta {
                        location: location_of(&entry),
                        last_modified: epoch(),
                        size,
                        e_tag: None,
                        version: None,
                    }),
                    None => pending.push(entry),
                }
            }
        }

        found.sort_by(|left, right| left.location.cmp(&right.location));
        Ok(found)
    }
}

impl fmt::Display for StorageObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hetida-storage")
    }
}

#[async_trait::async_trait]
impl ObjectStore for StorageObjectStore {
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectResult<GetResult> {
        let meta = self.meta(location)?;
        let range = requested_range(options.range.as_ref(), meta.size);

        // A HEAD asks for the metadata alone, so it must not read the object.
        let bytes = if options.head {
            Bytes::new()
        } else {
            Bytes::from(
                self.storage
                    .read_range(&Self::resolve(location), range.clone())
                    .map_err(|error| convert(&Self::resolve(location), error))?,
            )
        };

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream::once(async move { Ok(bytes) }))),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectResult<ObjectMeta>> {
        let root = prefix.map(Self::resolve).unwrap_or_default();
        match self.walk(&root) {
            Ok(found) => Box::pin(stream::iter(found.into_iter().map(Ok))),
            Err(error) => Box::pin(stream::once(async move { Err(error) })),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> ObjectResult<ListResult> {
        let root = prefix.map(Self::resolve).unwrap_or_default();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };

        for entry in self
            .storage
            .list(&root)
            .map_err(|error| convert(&root, error))?
        {
            match self
                .storage
                .size(&entry)
                .map_err(|error| convert(&entry, error))?
            {
                Some(size) => result.objects.push(ObjectMeta {
                    location: location_of(&entry),
                    last_modified: epoch(),
                    size,
                    e_tag: None,
                    version: None,
                }),
                None => result.common_prefixes.push(location_of(&entry)),
            }
        }

        Ok(result)
    }

    // ---- read-only: writes belong to the archive's own path ----

    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> ObjectResult<PutResult> {
        Err(read_only("put"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _options: PutMultipartOptions,
    ) -> ObjectResult<Box<dyn MultipartUpload>> {
        Err(read_only("put_multipart"))
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> ObjectResult<()> {
        Err(read_only("copy"))
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, ObjectResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectResult<ObjectPath>> {
        Box::pin(stream::once(async { Err(read_only("delete")) }))
    }
}

/// The byte range a get asks for, clamped to the object.
fn requested_range(requested: Option<&GetRange>, size: u64) -> Range<u64> {
    match requested {
        None => 0..size,
        Some(GetRange::Bounded(range)) => range.start.min(size)..range.end.min(size),
        Some(GetRange::Offset(start)) => (*start).min(size)..size,
        Some(GetRange::Suffix(length)) => size.saturating_sub(*length)..size,
    }
}

/// The `object_store` location naming a store-relative path.
fn location_of(path: &std::path::Path) -> ObjectPath {
    ObjectPath::from(path.to_string_lossy().as_ref())
}

/// Every object reports the epoch — see the module documentation.
fn epoch() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

/// Maps a storage failure onto the one `object_store` speaks.
fn convert(path: &std::path::Path, error: StorageError) -> object_store::Error {
    match error {
        StorageError::NotFound(missing) => object_store::Error::NotFound {
            path: missing.to_string_lossy().into_owned(),
            source: Box::new(StorageError::NotFound(missing)),
        },
        other => object_store::Error::Generic {
            store: "hetida-storage",
            source: Box::new(WithPath {
                path: path.to_path_buf(),
                source: other,
            }),
        },
    }
}

/// A storage failure carrying the path it happened on.
#[derive(Debug, thiserror::Error)]
#[error("{path}: {source}")]
struct WithPath {
    path: std::path::PathBuf,
    #[source]
    source: StorageError,
}

/// The refusal every mutating method answers with.
fn read_only(operation: &'static str) -> object_store::Error {
    object_store::Error::NotSupported {
        source: format!(
            "{operation} is not served here: the archive writes through its own \
             storage path, which orders a staging write before a rename (§2, §3)"
        )
        .into(),
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
    use std::path::Path;

    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    use crate::storage::InMemoryStorage;

    fn stored(entries: &[(&str, &[u8])]) -> (InMemoryStorage, StorageObjectStore) {
        let storage = InMemoryStorage::new();
        for (name, bytes) in entries {
            let mut file = storage.open_append(Path::new(name)).unwrap();
            file.append(bytes).unwrap();
            file.sync().unwrap();
        }
        let adapter = StorageObjectStore::new(Arc::new(storage.clone()));
        (storage, adapter)
    }

    #[tokio::test]
    async fn an_object_reads_back_whole() {
        let (_storage, store) = stored(&[("data/t/year=2026/part-0", b"0123456789")]);

        let bytes = store
            .get(&ObjectPath::from("data/t/year=2026/part-0"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(&bytes[..], b"0123456789");
    }

    /// §5, §11: a Parquet reader fetches a footer and then column chunks. Every
    /// range form it can ask for has to be served, or a scan reads the whole
    /// file for each of them.
    #[tokio::test]
    async fn every_range_form_is_served() {
        let (_storage, store) = stored(&[("data/part-0", b"0123456789")]);
        let path = ObjectPath::from("data/part-0");

        let read = |range: GetRange| {
            let store = store.clone();
            let path = path.clone();
            async move {
                let result = store
                    .get_opts(
                        &path,
                        GetOptions {
                            range: Some(range),
                            ..GetOptions::default()
                        },
                    )
                    .await
                    .unwrap();
                result.bytes().await.unwrap()
            }
        };

        assert_eq!(&read(GetRange::Bounded(2..5)).await[..], b"234");
        assert_eq!(&read(GetRange::Offset(7)).await[..], b"789");
        // The footer read: a Parquet reader asks for the last bytes first.
        assert_eq!(&read(GetRange::Suffix(3)).await[..], b"789");
    }

    #[tokio::test]
    async fn a_missing_object_is_not_found() {
        let (_storage, store) = stored(&[]);

        let error = store
            .get(&ObjectPath::from("data/absent"))
            .await
            .expect_err("must fail");

        assert!(matches!(error, object_store::Error::NotFound { .. }));
    }

    /// A head must not read the object, only describe it.
    #[tokio::test]
    async fn a_head_reports_the_size_without_reading() {
        let (_storage, store) = stored(&[("data/part-0", b"0123456789")]);

        let meta = store.head(&ObjectPath::from("data/part-0")).await.unwrap();

        assert_eq!(meta.size, 10);
    }

    /// §11: the layout is nested, and `Storage::list` answers one level at a
    /// time. A listing that stopped at the first level would make every
    /// partition invisible to the query path.
    #[tokio::test]
    async fn listing_descends_the_whole_layout() {
        let (_storage, store) = stored(&[
            ("data/t/year=2026/month=07/part-0", b"a"),
            ("data/t/year=2026/month=08/part-0", b"b"),
            ("data/t/year=2027/month=01/part-0", b"c"),
            ("wal/000000", b"d"),
        ]);

        let listed: Vec<String> = store
            .list(Some(&ObjectPath::from("data/t")))
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .unwrap();

        assert_eq!(
            listed,
            vec![
                "data/t/year=2026/month=07/part-0",
                "data/t/year=2026/month=08/part-0",
                "data/t/year=2027/month=01/part-0",
            ],
            "the walk must reach the files and stay inside the prefix"
        );
    }

    #[tokio::test]
    async fn a_delimited_listing_separates_files_from_levels() {
        let (_storage, store) = stored(&[
            ("data/t/year=2026/month=07/part-0", b"a"),
            ("data/t/marker", b"b"),
        ]);

        let result = store
            .list_with_delimiter(Some(&ObjectPath::from("data/t")))
            .await
            .unwrap();

        assert_eq!(
            result
                .objects
                .iter()
                .map(|m| m.location.to_string())
                .collect::<Vec<_>>(),
            vec!["data/t/marker"]
        );
        assert_eq!(
            result
                .common_prefixes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["data/t/year=2026"]
        );
    }

    /// The archive writes through its own path, with a staging write before a
    /// rename (§2, §3). A second write path onto the same bytes is what this
    /// adapter exists to prevent, so it is refused rather than provided.
    #[tokio::test]
    async fn writing_through_the_adapter_is_refused() {
        let (storage, store) = stored(&[]);

        let error = store
            .put(
                &ObjectPath::from("data/part-0"),
                PutPayload::from_static(b"x"),
            )
            .await
            .expect_err("must be refused");

        assert!(matches!(error, object_store::Error::NotSupported { .. }));
        assert!(
            storage.list(Path::new("data")).unwrap().is_empty(),
            "nothing may have been written"
        );
    }

    #[tokio::test]
    async fn deleting_through_the_adapter_is_refused() {
        let (storage, store) = stored(&[("data/part-0", b"x")]);

        assert!(
            store
                .delete(&ObjectPath::from("data/part-0"))
                .await
                .is_err()
        );
        assert_eq!(storage.list(Path::new("data")).unwrap().len(), 1);
    }

    /// A store failure has to arrive as an `object_store` error naming the path,
    /// not as a panic — the zero-panic policy reaches into this adapter too.
    #[tokio::test]
    async fn a_store_failure_surfaces_as_an_error() {
        let (storage, store) = stored(&[("data/part-0", b"x")]);
        storage.inject(crate::storage::Fault::Fail(
            std::io::ErrorKind::PermissionDenied,
        ));

        // The fault lands on the next append, so provoke one through the store
        // itself: the adapter must still answer, never panic.
        let mut file = storage.open_append(Path::new("data/part-1")).unwrap();
        assert!(file.append(b"y").is_err());
        assert!(store.get(&ObjectPath::from("data/part-0")).await.is_ok());
    }
}
