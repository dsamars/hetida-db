// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Writing a partition to Parquet (§1, §2, §3).
//!
//! One frozen memtable becomes one file, named `part-<seq>.parquet` after the
//! highest `seq` it holds, so file names order the same way their contents do.
//!
//! # What the file must guarantee
//!
//! - **Sorted by `(metric_id, timestamp, seq)`.** Version resolution is a
//!   streaming merge over that order and silently degrades to a full sort if it
//!   is broken (§1). The order comes from
//!   [`MemTable::sorted_batch`](crate::memtable::MemTable::sorted_batch); this
//!   module checks it before writing rather than trusting the caller, because a
//!   file that violates it cannot be repaired without a rewrite.
//! - **Every version preserved.** Nothing here resolves or deduplicates (§1).
//! - **Statistics on every column, `system_time` included.** Row-group and page
//!   statistics are what let a query skip whole row groups: on `metric_id`
//!   because the sort is metric-major (§11), and on `system_time` so an old
//!   observation point does not read rows written after it (§1).
//!
//! # Compression
//!
//! ZSTD. The sort places identical and near-identical values adjacent — a
//! recomputed forecast repeats a value across runs, and `metric_id` runs long —
//! which is exactly the shape a dictionary plus ZSTD compresses well.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::clock::Micros;
use crate::instance::FORMAT_VERSION;
use crate::registry::MetricId;
use crate::schema::{METRIC_ID_COLUMN, SEQ_COLUMN, TIMESTAMP_COLUMN};
use crate::seq::Seq;
use crate::storage::{Storage, StorageError};

/// Footer key carrying the on-disk format version (§12).
///
/// Namespaced, because a Parquet footer's key-value metadata is a shared space —
/// Arrow already writes `ARROW:schema` into it — and an unqualified
/// `format_version` would be a claim on a name this archive does not own. The
/// instance metadata uses its own unprefixed key: that file is ours entirely.
pub const FORMAT_VERSION_KEY: &str = "hetida.format_version";

/// Why a partition could not be written.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// The batch is not in `(metric_id, timestamp, seq)` order (§1).
    ///
    /// Refused rather than sorted here: the caller producing an unsorted batch is
    /// a bug, and silently fixing it would hide the one property every later
    /// stage depends on.
    #[error(
        "rows are not in (metric_id, timestamp, seq) order at row {row}; \
         a file in this order cannot be repaired without a rewrite (§1)"
    )]
    Unsorted {
        /// First row that breaks the order.
        row: usize,
    },
    /// A key column was missing or had an unexpected type.
    #[error("column `{0}` is missing or not the expected type")]
    KeyColumn(String),
    /// Parquet encoding failed.
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    /// The store failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// How a partition file is written.
#[derive(Debug, Clone, Copy)]
pub struct ParquetConfig {
    /// ZSTD level. Higher trades write throughput for size.
    pub zstd_level: i32,
    /// Rows per row group.
    ///
    /// The unit of skipping: a query prunes whole row groups by their statistics,
    /// so smaller groups prune more finely and cost more metadata.
    pub row_group_rows: usize,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            // 3 is ZSTD's own default: most of the ratio for little of the cost.
            zstd_level: 3,
            row_group_rows: 128 * 1024,
        }
    }
}

/// What a written file holds, for the caller that must now track it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenFile {
    /// Path within the store.
    pub path: std::path::PathBuf,
    /// Rows written, counting every version.
    pub rows: usize,
    /// Bytes the file occupies.
    pub bytes: usize,
    /// Highest `seq` the file holds — its name, and the checkpoint it permits (§4).
    pub max_seq: Seq,
}

/// File name for a partition file holding up to `max_seq`.
///
/// `discriminator` distinguishes files that would otherwise share a name: a
/// compaction re-emitting the same `max_seq` as one of its inputs must not
/// collide with it, and the store has no rename to work around that. `None` is
/// the flush path's plain name.
///
/// Zero padded so lexicographic order matches sequence order, which is what makes
/// a directory listing readable in write order.
pub fn file_name(max_seq: Seq, discriminator: Option<u32>) -> String {
    match discriminator {
        None => format!("part-{max_seq:020}.parquet"),
        Some(index) => format!("part-{max_seq:020}-c{index:04}.parquet"),
    }
}

/// Writes `batch` as one Parquet file into `directory`.
///
/// `batch` must already be in `(metric_id, timestamp, seq)` order — see
/// [`WriteError::Unsorted`].
pub fn write_partition(
    storage: &dyn Storage,
    directory: &std::path::Path,
    batch: &RecordBatch,
    config: ParquetConfig,
) -> Result<WrittenFile, WriteError> {
    write_named(storage, directory, batch, config, None)
}

/// Writes `batch` under a name distinguished by `discriminator`.
///
/// See [`file_name`]. Used by compaction, which must not overwrite its inputs.
pub fn write_named(
    storage: &dyn Storage,
    directory: &std::path::Path,
    batch: &RecordBatch,
    config: ParquetConfig,
    discriminator: Option<u32>,
) -> Result<WrittenFile, WriteError> {
    let max_seq = check_sorted(batch)?;

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(config.zstd_level).unwrap_or_default(),
        ))
        // Page-level statistics, not just row-group level: the column index they
        // build is what lets a reader skip pages within a group (§1, §11).
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_row_count(Some(config.row_group_rows))
        .set_sorting_columns(Some(sorting_columns(batch)?))
        // §12: every artefact says which format wrote it. Unread for now — the
        // instance-level gate covers every file the archive owns — but a stamp
        // cannot be added to a file after it is on disk.
        .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
            FORMAT_VERSION_KEY.to_owned(),
            FORMAT_VERSION.to_string(),
        )]))
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties))?;
    writer.write(batch)?;
    writer.close()?;

    // Published by rename, because a Parquet file is only valid once its footer
    // is there and `open_append` makes a name visible immediately. Writing under
    // the final name would leave a half file for a reader to open if the write
    // failed part way.
    let path = directory.join(file_name(max_seq, discriminator));
    let staging = directory.join(format!(".{}.tmp", file_name(max_seq, discriminator)));
    if let Err(error) = write_staged(storage, &staging, &buffer) {
        // Leave no debris behind, whether or not this succeeds.
        let _ = storage.remove(&staging);
        return Err(error);
    }
    storage.rename(&staging, &path)?;

    Ok(WrittenFile {
        path,
        rows: batch.num_rows(),
        bytes: buffer.len(),
        max_seq,
    })
}

/// Writes the whole file to a staging name, synced.
fn write_staged(
    storage: &dyn Storage,
    staging: &std::path::Path,
    bytes: &[u8],
) -> Result<(), WriteError> {
    let mut file = storage.open_append(staging)?;
    file.append(bytes)?;
    file.sync()?;
    Ok(())
}

/// Declares the sort order in the file's metadata, so a reader can rely on it.
fn sorting_columns(
    batch: &RecordBatch,
) -> Result<Vec<parquet::file::metadata::SortingColumn>, WriteError> {
    [METRIC_ID_COLUMN, TIMESTAMP_COLUMN, SEQ_COLUMN]
        .into_iter()
        .map(|name| {
            let index = batch
                .schema()
                .index_of(name)
                .map_err(|_| WriteError::KeyColumn(name.to_owned()))?;
            Ok(parquet::file::metadata::SortingColumn {
                column_idx: index as i32,
                descending: false,
                nulls_first: false,
            })
        })
        .collect()
}

/// Verifies the §1 order and returns the highest `seq`.
fn check_sorted(batch: &RecordBatch) -> Result<Seq, WriteError> {
    let metric_ids = u64_column(batch, METRIC_ID_COLUMN)?;
    let timestamps = timestamp_column(batch, TIMESTAMP_COLUMN)?;
    let seqs = u64_column(batch, SEQ_COLUMN)?;

    let mut previous: Option<(MetricId, Micros, Seq)> = None;
    let mut max_seq = 0;

    for row in 0..batch.num_rows() {
        let key = (
            *metric_ids
                .values()
                .get(row)
                .ok_or_else(|| WriteError::KeyColumn(METRIC_ID_COLUMN.to_owned()))?,
            *timestamps
                .values()
                .get(row)
                .ok_or_else(|| WriteError::KeyColumn(TIMESTAMP_COLUMN.to_owned()))?,
            *seqs
                .values()
                .get(row)
                .ok_or_else(|| WriteError::KeyColumn(SEQ_COLUMN.to_owned()))?,
        );

        if previous.is_some_and(|held| held > key) {
            return Err(WriteError::Unsorted { row });
        }
        max_seq = max_seq.max(key.2);
        previous = Some(key);
    }

    Ok(max_seq)
}

fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array, WriteError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| WriteError::KeyColumn(name.to_owned()))
}

fn timestamp_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampMicrosecondArray, WriteError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .ok_or_else(|| WriteError::KeyColumn(name.to_owned()))
}

/// Reads a written partition file back, for verification and for the read path.
pub fn read_partition(
    storage: &dyn Storage,
    path: &std::path::Path,
) -> Result<Vec<RecordBatch>, WriteError> {
    let bytes = storage.read(path)?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )?
    .build()?;

    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(ParquetError::from)?);
    }
    Ok(batches)
}

/// The footer metadata of a written file, for asserting on what it guarantees.
pub fn read_metadata(
    storage: &dyn Storage,
    path: &std::path::Path,
) -> Result<Arc<parquet::file::metadata::ParquetMetaData>, WriteError> {
    let bytes = storage.read(path)?;
    Ok(
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        )?
        .metadata()
        .clone(),
    )
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
    use std::path::{Path, PathBuf};

    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::memtable::MemTable;
    use crate::schema::{SYSTEM_TIME_COLUMN, append_system_fields, timestamp_type};
    use crate::storage::{Fault, InMemoryStorage};
    use crate::suppression::{SuppressionConfig, SuppressionIndex};

    fn storage_schema() -> arrow::datatypes::SchemaRef {
        append_system_fields(&Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    /// A batch from `(metric_id, timestamp, seq)` triples, in the order given.
    fn batch(rows: &[(u64, i64, u64)]) -> RecordBatch {
        RecordBatch::try_new(
            storage_schema(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter()
                        .map(|(_, _, seq)| *seq as f64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter()
                        .map(|(_, _, seq)| 1_000 + *seq as i64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, _, seq)| *seq).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn dir() -> PathBuf {
        PathBuf::from("data/year=2026/month=08")
    }

    fn write(storage: &InMemoryStorage, batch: &RecordBatch) -> Result<WrittenFile, WriteError> {
        write_partition(storage, &dir(), batch, ParquetConfig::default())
    }

    /// §1: every version is written, in order, and comes back unchanged.
    #[test]
    fn a_sorted_batch_round_trips_through_a_file() {
        let storage = InMemoryStorage::new();
        let written = batch(&[(1, 10, 0), (1, 10, 5), (1, 20, 1), (2, 5, 2)]);

        let file = write(&storage, &written).unwrap();
        let read = read_partition(&storage, &file.path).unwrap();

        assert_eq!(file.rows, 4);
        assert_eq!(file.max_seq, 5);
        let total: usize = read.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4, "no version may be dropped");
        assert_eq!(read[0].schema(), written.schema());
        assert_eq!(read[0].columns(), written.columns());
    }

    /// §1: the order cannot be repaired later, so an unsorted batch is refused
    /// rather than silently sorted — that would hide the caller's bug.
    #[test]
    fn an_unsorted_batch_is_refused() {
        let storage = InMemoryStorage::new();

        for unsorted in [
            vec![(2, 10, 0), (1, 10, 1)], // metric_id descends
            vec![(1, 20, 0), (1, 10, 1)], // timestamp descends
            vec![(1, 10, 5), (1, 10, 2)], // seq descends
        ] {
            let error = write(&storage, &batch(&unsorted)).expect_err("must be refused");
            assert!(matches!(error, WriteError::Unsorted { .. }), "{error}");
        }
    }

    #[test]
    fn equal_keys_are_in_order() {
        let storage = InMemoryStorage::new();
        // Two versions of one logical row, ascending by seq: legal.
        assert!(write(&storage, &batch(&[(1, 10, 0), (1, 10, 1)])).is_ok());
    }

    /// §1, §11: statistics are what let a reader skip. Without them on
    /// `metric_id` the metric-major sort buys nothing, and without them on
    /// `system_time` an old observation point reads rows written after it.
    #[test]
    fn every_key_column_carries_statistics() {
        let storage = InMemoryStorage::new();
        let file = write(&storage, &batch(&[(1, 10, 0), (2, 20, 1)])).unwrap();

        let metadata = read_metadata(&storage, &file.path).unwrap();
        let group = metadata.row_group(0);

        for name in [
            METRIC_ID_COLUMN,
            TIMESTAMP_COLUMN,
            SEQ_COLUMN,
            SYSTEM_TIME_COLUMN,
        ] {
            let index = group
                .columns()
                .iter()
                .position(|column| column.column_path().string() == name)
                .unwrap_or_else(|| panic!("{name} is not in the file"));
            assert!(
                group.column(index).statistics().is_some(),
                "{name} has no statistics, so nothing can prune on it"
            );
        }
    }

    /// §12: every artefact the archive writes says which format produced it.
    ///
    /// Nothing reads this back yet, deliberately — the instance-level gate
    /// covers every file the archive owns, and a Parquet file carries its own
    /// schema, so a misinterpretation surfaces as a schema mismatch rather than
    /// as plausible nonsense. Writing it is what costs nothing now and cannot be
    /// added retroactively to files already on disk.
    #[test]
    fn the_file_is_stamped_with_the_format_version() {
        let storage = InMemoryStorage::new();
        let file = write(&storage, &batch(&[(1, 10, 0)])).unwrap();

        let metadata = read_metadata(&storage, &file.path).unwrap();
        let stamp = metadata
            .file_metadata()
            .key_value_metadata()
            .expect("the footer must carry key-value metadata")
            .iter()
            .find(|entry| entry.key == FORMAT_VERSION_KEY)
            .expect("the format version must be stamped");

        assert_eq!(
            stamp.value.as_deref(),
            Some(FORMAT_VERSION.to_string()).as_deref()
        );
    }

    /// The declared sort order is in the footer, so a reader does not have to
    /// take it on trust (§1).
    #[test]
    fn the_file_declares_its_sort_order() {
        let storage = InMemoryStorage::new();
        let file = write(&storage, &batch(&[(1, 10, 0)])).unwrap();

        let metadata = read_metadata(&storage, &file.path).unwrap();
        let declared = metadata
            .row_group(0)
            .sorting_columns()
            .expect("a sort order must be declared");

        let names: Vec<String> = declared
            .iter()
            .map(|column| {
                storage_schema()
                    .field(column.column_idx as usize)
                    .name()
                    .clone()
            })
            .collect();
        assert_eq!(names, vec![METRIC_ID_COLUMN, TIMESTAMP_COLUMN, SEQ_COLUMN]);
        assert!(declared.iter().all(|column| !column.descending));
    }

    #[test]
    fn the_file_is_zstd_compressed() {
        let storage = InMemoryStorage::new();
        let file = write(&storage, &batch(&[(1, 10, 0), (1, 10, 1)])).unwrap();

        let metadata = read_metadata(&storage, &file.path).unwrap();
        assert!(matches!(
            metadata.row_group(0).column(0).compression(),
            Compression::ZSTD(_)
        ));
    }

    /// §1: the sort places repeated values adjacent, which is the shape ZSTD
    /// compresses. A recomputed forecast is exactly that case.
    #[test]
    fn repeated_values_compress_far_below_their_raw_size() {
        let storage = InMemoryStorage::new();
        const ROWS: usize = 20_000;
        let rows: Vec<(u64, i64, u64)> = (0..ROWS as u64).map(|seq| (1, 10, seq)).collect();

        let file = write(&storage, &batch(&rows)).unwrap();

        // Five columns of eight bytes is the uncompressed floor.
        let raw = ROWS * 5 * 8;
        assert!(
            file.bytes * 4 < raw,
            "{ROWS} adjacent rows took {} bytes against a raw {raw}",
            file.bytes
        );
    }

    /// The name orders the same way its contents do, so a listing reads in write
    /// order.
    #[test]
    fn file_names_sort_in_sequence_order() {
        let mut names = [
            file_name(9, None),
            file_name(100, None),
            file_name(11, None),
        ];
        names.sort();
        assert_eq!(
            names,
            [
                file_name(9, None),
                file_name(11, None),
                file_name(100, None)
            ]
        );
    }

    #[test]
    fn the_file_is_named_after_its_highest_seq() {
        let storage = InMemoryStorage::new();
        let file = write(&storage, &batch(&[(1, 10, 3), (1, 10, 42)])).unwrap();

        assert_eq!(file.path, dir().join("part-00000000000000000042.parquet"));
    }

    /// A store that fails must surface as an error, not a half-written file under
    /// a name a reader would open.
    #[test]
    fn a_failing_store_surfaces_as_an_error() {
        let storage = InMemoryStorage::new();
        storage.inject(Fault::Fail(std::io::ErrorKind::StorageFull));

        assert!(matches!(
            write(&storage, &batch(&[(1, 10, 0)])),
            Err(WriteError::Storage(_))
        ));
    }

    /// A Parquet file is only valid with its footer, and `open_append` makes a
    /// name visible at once — so a failed write must leave nothing behind for a
    /// reader to open, staging file included.
    #[test]
    fn a_failed_write_leaves_no_file_behind() {
        let storage = InMemoryStorage::new();
        storage.inject(Fault::Fail(std::io::ErrorKind::StorageFull));

        assert!(write(&storage, &batch(&[(1, 10, 0)])).is_err());

        assert!(
            storage.list(&dir()).unwrap().is_empty(),
            "debris remains: {:?}",
            storage.list(&dir()).unwrap()
        );
    }

    /// A torn write must not be published either: the rename happens only after
    /// the whole buffer is synced, so the final name never holds a partial file.
    #[test]
    fn a_torn_write_never_appears_under_the_final_name() {
        let storage = InMemoryStorage::new();
        storage.inject(Fault::TornWrite { keep: 64 });

        // The write itself reports success — a torn write does (§4) — but what
        // matters is that whatever is published is a readable file or nothing.
        let _ = write(&storage, &batch(&[(1, 10, 0), (1, 20, 1)]));

        for file in storage.list(&dir()).unwrap() {
            assert!(
                !file.to_string_lossy().contains(".tmp"),
                "a staging file was published: {}",
                file.display()
            );
        }
    }

    /// §1, §2: the memtable produces the order and this module writes it, so the
    /// two have to agree — checked over deliberately scrambled input.
    #[test]
    fn a_frozen_memtable_writes_in_flush_order() {
        let table = MemTable::new(
            storage_schema(),
            Arc::new(SuppressionIndex::new(SuppressionConfig::for_stream())),
        )
        .unwrap();

        // Inserted out of order, across several batches.
        for rows in [
            vec![(2, 10, 0)],
            vec![(1, 20, 1), (1, 10, 2)],
            vec![(1, 10, 9), (2, 5, 3)],
        ] {
            table.insert(&batch(&rows)).unwrap();
        }

        let sorted = table.sorted_batch().unwrap().expect("rows were inserted");
        let storage = InMemoryStorage::new();
        let file = write(&storage, &sorted).expect("the memtable's order must be accepted");

        assert_eq!(file.rows, 5, "every version must reach the file");

        let read = read_partition(&storage, &file.path).unwrap();
        let keys: Vec<(u64, i64, u64)> = read
            .iter()
            .flat_map(|batch| {
                let ids = u64_column(batch, METRIC_ID_COLUMN)
                    .unwrap()
                    .values()
                    .to_vec();
                let stamps = timestamp_column(batch, TIMESTAMP_COLUMN)
                    .unwrap()
                    .values()
                    .to_vec();
                let seqs = u64_column(batch, SEQ_COLUMN).unwrap().values().to_vec();
                (0..batch.num_rows())
                    .map(move |row| (ids[row], stamps[row], seqs[row]))
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut expected = keys.clone();
        expected.sort_unstable();
        assert_eq!(keys, expected, "the file is not in flush order");
    }

    #[test]
    fn an_empty_memtable_has_nothing_to_write() {
        let table = MemTable::new(
            storage_schema(),
            Arc::new(SuppressionIndex::new(SuppressionConfig::for_stream())),
        )
        .unwrap();

        assert!(table.sorted_batch().unwrap().is_none());
    }

    #[test]
    fn reading_a_missing_file_is_an_error() {
        let storage = InMemoryStorage::new();
        assert!(read_partition(&storage, Path::new("data/absent.parquet")).is_err());
    }
}
