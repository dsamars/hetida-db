// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Compacting a partition's files (§1, §3, §11).
//!
//! A flush writes one file per freeze, so a partition accumulates small files.
//! Compaction merges them back into few, re-sorted into
//! `(metric_id, timestamp, seq)` — the order every later stage depends on (§1).
//!
//! # It never drops a version
//!
//! Compaction exists for file count and scan efficiency, not for version
//! resolution (§1, §3). Every row of every input appears in the output, including
//! rows that share a `(metric_id, timestamp)`. A test counts rows in and out for
//! exactly this reason.
//!
//! # Disjoint key ranges
//!
//! Output files never share a `(metric_id, timestamp)`, so a reader that has
//! located a key needs exactly one file for it. That is why a file is cut at the
//! next *key boundary* after reaching its target size, not at the target itself —
//! cutting mid-group would put one logical row's versions in two files and make
//! resolution read both.
//!
//! A single logical row with more versions than a target file holds therefore
//! produces one oversized file. That is deliberate: splitting it is the one thing
//! that would break the disjointness a reader relies on.
//!
//! # Ordering against a crash
//!
//! Outputs are written and synced *before* any input is removed. A crash between
//! the two leaves the rows present twice, under different names — and because the
//! duplicates carry identical `(metric_id, timestamp, seq)`, no as-of query
//! changes answer. The reverse order would lose rows outright. Duplicates are
//! recoverable; absence is not.
//!
//! The store offers no rename, so an output that re-emits an input's `max_seq`
//! would collide with it. Outputs are named one generation above their inputs
//! instead — see [`crate::parquet::Output`]. The generation is what makes a
//! second pass over an unchanged partition safe: without it that pass
//! reproduces its inputs' names exactly and deletes what it just wrote.

use std::sync::Arc;

use arrow::array::{RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};

use crate::clock::Micros;
use crate::instance::DATA_DIR;
use crate::parquet::{
    Output, ParquetConfig, WriteError, WrittenFile, generation_of, read_partition, write_named,
};
use crate::partition::PartitionInterval;
use crate::registry::MetricId;
use crate::schema::{METRIC_ID_COLUMN, SEQ_COLUMN, TIMESTAMP_COLUMN};
use crate::seq::Seq;
use crate::storage::{Storage, StorageError};

/// Why a compaction failed.
#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    /// A file could not be read or written.
    #[error(transparent)]
    Write(#[from] WriteError),
    /// The store failed.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// Merging the inputs failed.
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    /// A key column was missing or had an unexpected type.
    #[error("column `{0}` is missing or not the expected type")]
    KeyColumn(String),
    /// An output took the name of a file this pass is about to remove (§3).
    ///
    /// The generation in the name makes this unreachable. It is still checked,
    /// and checked before anything is removed, because the cost of the naming
    /// rule being wrong is every row in the partition.
    #[error(
        "output `{0}` has the name of one of its own inputs; \
         removing the inputs would delete it (§1, §3)"
    )]
    OutputCollidesWithInput(std::path::PathBuf),
}

/// When a partition is compacted, and into what (§3).
#[derive(Debug, Clone, Copy)]
pub struct CompactConfig {
    /// Compact once a partition holds this many files.
    pub max_files: usize,
    /// Compact once a partition's files total this many bytes.
    pub max_bytes: usize,
    /// Target size of an output file.
    ///
    /// A soft target: a file is cut at the next key boundary past it, so
    /// disjointness holds.
    pub target_bytes: usize,
}

impl Default for CompactConfig {
    fn default() -> Self {
        Self {
            // A partition answered from four files pays four footer reads; past
            // that the metadata overhead §11 warns about starts to dominate.
            max_files: 4,
            max_bytes: 256 * 1024 * 1024,
            target_bytes: 128 * 1024 * 1024,
        }
    }
}

/// Why a partition is due for compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactReason {
    /// Too many files, whatever their size.
    FileCount {
        /// Files present.
        files: usize,
    },
    /// The files total more than the threshold.
    TotalBytes {
        /// Bytes across all files.
        bytes: usize,
    },
}

/// Whether `files` are all the output of one compaction pass (§3).
///
/// Such a partition has nothing to gain from another: the same rows would merge
/// into the same slices under the same target size. Without this it would be
/// compacted on every pass for as long as it stayed above `max_files`, which a
/// partition big enough to slice does permanently.
///
/// Generation, not file count, is the test — a flush landing in a compacted
/// partition writes generation 0, so the mixture is due again, which is what
/// makes new rows reach a merged file at all. A partition of mixed generations
/// is likewise due: a pass was interrupted, and finishing it is real work.
///
/// The cost is that changing `target_bytes` does not re-slice a partition until
/// something is written to it. Re-slicing on a configuration change would mean
/// rewriting every partition in the archive on the next pass, which is the
/// larger surprise of the two.
pub fn is_settled(files: &[std::path::PathBuf]) -> bool {
    let mut generations = files.iter().map(|file| generation_of(file));
    let Some(first) = generations.next() else {
        return false;
    };
    first > 0 && generations.all(|generation| generation == first)
}

/// Whether a partition of `files` totalling `bytes` is due (§3).
///
/// A single file is never due: merging one file into one file changes nothing and
/// would rewrite the partition on every pass.
///
/// This counts; it does not look at the files. [`is_settled`] is the other half
/// of the decision and [`Compactor::compact_if_due`] applies both.
pub fn compaction_reason(
    config: CompactConfig,
    files: usize,
    bytes: usize,
) -> Option<CompactReason> {
    if files < 2 {
        return None;
    }
    if files >= config.max_files {
        return Some(CompactReason::FileCount { files });
    }
    (bytes >= config.max_bytes).then_some(CompactReason::TotalBytes { bytes })
}

/// What one compaction did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compaction {
    /// Files written.
    pub written: Vec<WrittenFile>,
    /// Files removed once the outputs were durable.
    pub removed: Vec<std::path::PathBuf>,
    /// Rows carried through, which must equal the rows read in (§1).
    pub rows: usize,
}

/// Merges a partition's files into few, re-sorted ones.
#[derive(Debug)]
pub struct Compactor {
    storage: Arc<dyn Storage>,
    config: CompactConfig,
    parquet: ParquetConfig,
}

impl Compactor {
    /// Compacts with `config`, writing through `parquet`.
    pub fn new(storage: Arc<dyn Storage>, config: CompactConfig, parquet: ParquetConfig) -> Self {
        Self {
            storage,
            config,
            parquet,
        }
    }

    /// Compacts `directory` if it is due, returning `None` if it is not.
    ///
    /// Due means over a threshold *and* not already settled — see
    /// [`compaction_reason`] and [`is_settled`]. A partition that has just been
    /// compacted stays above `max_files` whenever it was large enough to slice,
    /// so the file count alone would send it round for ever.
    ///
    /// Entries that cannot be read are skipped rather than fatal, and are never
    /// passed to [`Self::compact`] — so they are also never removed. That covers
    /// two cases with one rule: a directory handed in by mistake (a level above
    /// the partitions holds directories, not files), and a file an I/O error makes
    /// unreadable, which must not be merged away while it cannot be verified.
    pub fn compact_if_due(
        &self,
        directory: &std::path::Path,
    ) -> Result<Option<Compaction>, CompactError> {
        let mut files = Vec::new();
        let mut bytes = 0;
        for entry in self.storage.list(directory)? {
            if let Ok(contents) = self.storage.read(&entry) {
                bytes += contents.len();
                files.push(entry);
            }
        }

        if is_settled(&files) || compaction_reason(self.config, files.len(), bytes).is_none() {
            return Ok(None);
        }
        self.compact(directory, &files).map(Some)
    }

    /// Merges `inputs` into re-sorted outputs and removes the inputs.
    ///
    /// The inputs are removed only after every output is durable — see the module
    /// documentation on ordering against a crash.
    pub fn compact(
        &self,
        directory: &std::path::Path,
        inputs: &[std::path::PathBuf],
    ) -> Result<Compaction, CompactError> {
        let mut batches = Vec::new();
        for input in inputs {
            batches.extend(read_partition(self.storage.as_ref(), input)?);
        }
        let batches: Vec<RecordBatch> = batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();

        let Some(first) = batches.first() else {
            return Ok(Compaction {
                written: Vec::new(),
                removed: Vec::new(),
                rows: 0,
            });
        };

        let merged = concat_batches(&first.schema(), batches.iter())?;
        let rows_in = merged.num_rows();
        let sorted = sort_by_key(&merged)?;

        // One past the highest generation among the inputs, so no output can be
        // named after a file this pass removes — see [`Output`]. Deriving it
        // from the inputs rather than counting passes keeps it correct after a
        // restart, which has no memory of how often this partition was merged.
        let generation = inputs
            .iter()
            .map(|input| generation_of(input))
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        // Written and synced before anything is removed.
        let mut written = Vec::new();
        for (index, slice) in self
            .slice_at_key_boundaries(&sorted)?
            .into_iter()
            .enumerate()
        {
            written.push(write_named(
                self.storage.as_ref(),
                directory,
                &slice,
                self.parquet,
                Some(Output::new(generation, index as u32)),
            )?);
        }

        let rows_out: usize = written.iter().map(|file| file.rows).sum();
        debug_assert_eq!(rows_in, rows_out, "compaction changed the row count");

        // Before the first removal, not during: a pass that would delete its own
        // output must leave the partition exactly as it found it.
        for file in &written {
            if inputs.contains(&file.path) {
                return Err(CompactError::OutputCollidesWithInput(file.path.clone()));
            }
        }

        let mut removed = Vec::new();
        for input in inputs {
            self.storage.remove(input)?;
            removed.push(input.clone());
        }

        Ok(Compaction {
            written,
            removed,
            rows: rows_out,
        })
    }

    /// Cuts `sorted` into slices of about the target size, at key boundaries.
    ///
    /// The boundary rule is what keeps output ranges disjoint: a slice ends only
    /// where `(metric_id, timestamp)` changes, so one logical row's versions never
    /// span two files.
    fn slice_at_key_boundaries(
        &self,
        sorted: &RecordBatch,
    ) -> Result<Vec<RecordBatch>, CompactError> {
        let rows = sorted.num_rows();
        if rows == 0 {
            return Ok(Vec::new());
        }

        // Bytes per row, from the in-memory size: exact output sizes are only
        // known after compression, and the target is soft by design.
        let per_row = (sorted
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .sum::<usize>()
            / rows)
            .max(1);
        let rows_per_file = (self.config.target_bytes / per_row).max(1);

        let keys = row_keys(sorted)?;
        let mut slices = Vec::new();
        let mut start = 0usize;

        for row in 1..rows {
            let long_enough = row - start >= rows_per_file;
            let boundary = keys.get(row) != keys.get(row - 1);
            if long_enough && boundary {
                slices.push(sorted.slice(start, row - start));
                start = row;
            }
        }
        slices.push(sorted.slice(start, rows - start));

        Ok(slices)
    }
}

/// Sorts a batch into `(metric_id, timestamp, seq)` order (§1).
fn sort_by_key(batch: &RecordBatch) -> Result<RecordBatch, CompactError> {
    let columns = [METRIC_ID_COLUMN, TIMESTAMP_COLUMN, SEQ_COLUMN]
        .into_iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .cloned()
                .map(|values| SortColumn {
                    values,
                    options: None,
                })
                .ok_or_else(|| CompactError::KeyColumn(name.to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let indices = lexsort_to_indices(&columns, None)?;
    Ok(take_record_batch(batch, &indices)?)
}

/// Each row's `(metric_id, timestamp)`, for finding key boundaries.
fn row_keys(batch: &RecordBatch) -> Result<Vec<(MetricId, Micros)>, CompactError> {
    let metric_ids = batch
        .column_by_name(METRIC_ID_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| CompactError::KeyColumn(METRIC_ID_COLUMN.to_owned()))?;
    let timestamps = batch
        .column_by_name(TIMESTAMP_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .ok_or_else(|| CompactError::KeyColumn(TIMESTAMP_COLUMN.to_owned()))?;

    Ok(metric_ids
        .values()
        .iter()
        .zip(timestamps.values().iter())
        .map(|(metric_id, timestamp)| (*metric_id, *timestamp))
        .collect())
}

/// Every partition directory of `table`, in path order.
///
/// Walks exactly [`PartitionInterval::depth`] levels below the table, so it
/// returns partition directories rather than the year and month levels above
/// them. A layout deeper or shallower than the recorded interval yields nothing
/// at that depth, which is the shape the startup check exists to prevent (§11).
pub fn partition_dirs(
    storage: &dyn Storage,
    table: &str,
    interval: PartitionInterval,
) -> Result<Vec<std::path::PathBuf>, StorageError> {
    let mut level = vec![std::path::Path::new(DATA_DIR).join(table)];

    for _ in 0..interval.depth() {
        let mut next = Vec::new();
        for directory in &level {
            next.extend(storage.list(directory)?);
        }
        level = next;
    }

    level.sort();
    Ok(level)
}

/// The `(metric_id, timestamp, seq)` triples a file holds, in file order.
///
/// Public for verification: the invariant suite and the compaction tests both
/// need to see what actually landed on disk.
pub fn keys_of(
    storage: &dyn Storage,
    path: &std::path::Path,
) -> Result<Vec<(MetricId, Micros, Seq)>, CompactError> {
    let mut keys = Vec::new();
    for batch in read_partition(storage, path)? {
        let pairs = row_keys(&batch)?;
        let seqs = batch
            .column_by_name(SEQ_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| CompactError::KeyColumn(SEQ_COLUMN.to_owned()))?
            .values()
            .to_vec();
        for (index, (metric_id, timestamp)) in pairs.into_iter().enumerate() {
            keys.push((
                metric_id,
                timestamp,
                seqs.get(index).copied().unwrap_or_default(),
            ));
        }
    }
    Ok(keys)
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
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use crate::parquet::write_partition;
    use crate::schema::{append_system_fields, timestamp_type};
    use crate::storage::InMemoryStorage;

    fn storage_schema() -> SchemaRef {
        append_system_fields(&Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    /// A sorted batch from `(metric_id, timestamp, seq)` triples.
    fn batch(rows: &[(u64, i64, u64)]) -> RecordBatch {
        let mut rows = rows.to_vec();
        rows.sort_unstable();
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
        PathBuf::from("data/readings/year=2026/month=08")
    }

    /// Writes each group as its own file, as successive flushes would.
    fn with_files(groups: &[&[(u64, i64, u64)]]) -> InMemoryStorage {
        let storage = InMemoryStorage::new();
        for group in groups {
            write_partition(&storage, &dir(), &batch(group), ParquetConfig::default()).unwrap();
        }
        storage
    }

    fn compactor(storage: &InMemoryStorage, config: CompactConfig) -> Compactor {
        Compactor::new(Arc::new(storage.clone()), config, ParquetConfig::default())
    }

    fn eager() -> CompactConfig {
        CompactConfig {
            max_files: 2,
            max_bytes: usize::MAX,
            target_bytes: usize::MAX,
        }
    }

    /// Eager, and small enough a target that the output is cut into one file per
    /// key. A partition large enough to slice is the case a single-output
    /// compaction never reaches: it stays above `max_files` afterwards, so it
    /// comes round again with its inputs unchanged.
    fn slicing() -> CompactConfig {
        CompactConfig {
            max_files: 2,
            max_bytes: usize::MAX,
            target_bytes: 1,
        }
    }

    fn all_keys(storage: &InMemoryStorage) -> Vec<(MetricId, Micros, Seq)> {
        let mut keys = Vec::new();
        for file in storage.list(&dir()).unwrap() {
            keys.extend(keys_of(storage, &file).unwrap());
        }
        keys.sort_unstable();
        keys
    }

    // ---- the trigger (§3) ----

    #[test]
    fn a_single_file_is_never_due() {
        // Merging one file into one file changes nothing and would rewrite the
        // partition on every pass.
        assert_eq!(compaction_reason(eager(), 1, usize::MAX), None);
        assert_eq!(compaction_reason(eager(), 0, 0), None);
    }

    #[test]
    fn the_file_count_threshold_fires() {
        assert_eq!(
            compaction_reason(eager(), 2, 0),
            Some(CompactReason::FileCount { files: 2 })
        );
    }

    #[test]
    fn the_total_size_threshold_fires() {
        let config = CompactConfig {
            max_files: usize::MAX,
            max_bytes: 1_000,
            target_bytes: usize::MAX,
        };
        assert_eq!(
            compaction_reason(config, 2, 1_000),
            Some(CompactReason::TotalBytes { bytes: 1_000 })
        );
        assert_eq!(compaction_reason(config, 2, 999), None);
    }

    // ---- the merge (§1, §3) ----

    /// §1, §3: this is the property compaction must never violate. Every version
    /// of every input appears in the output, including versions sharing a
    /// `(metric_id, timestamp)`.
    #[test]
    fn no_version_is_dropped() {
        let storage = with_files(&[
            &[(1, 10, 0), (1, 20, 1)],
            &[(1, 10, 5), (2, 10, 2)], // another version of (1, 10)
            &[(1, 10, 9)],             // and a third
        ]);
        let before = all_keys(&storage);

        let result = compactor(&storage, eager())
            .compact_if_due(&dir())
            .unwrap()
            .expect("due");

        assert_eq!(result.rows, before.len());
        assert_eq!(all_keys(&storage), before, "a version changed or vanished");
    }

    /// §1: the output is re-sorted, whatever order the inputs arrived in.
    #[test]
    fn the_output_is_in_flush_order() {
        let storage = with_files(&[&[(2, 10, 0)], &[(1, 20, 1)], &[(1, 10, 2)]]);

        compactor(&storage, eager()).compact_if_due(&dir()).unwrap();

        for file in storage.list(&dir()).unwrap() {
            let keys = keys_of(&storage, &file).unwrap();
            let mut expected = keys.clone();
            expected.sort_unstable();
            assert_eq!(keys, expected, "{} is not sorted", file.display());
        }
    }

    #[test]
    fn the_inputs_are_removed() {
        let storage = with_files(&[&[(1, 10, 0)], &[(1, 20, 1)]]);
        let before = storage.list(&dir()).unwrap();

        let result = compactor(&storage, eager())
            .compact_if_due(&dir())
            .unwrap()
            .unwrap();

        assert_eq!(result.removed, before);
        let after = storage.list(&dir()).unwrap();
        assert!(
            after.iter().all(|file| !before.contains(file)),
            "an input survived: {after:?}"
        );
    }

    #[test]
    fn a_partition_that_is_not_due_is_left_alone() {
        let storage = with_files(&[&[(1, 10, 0)]]);
        let before = storage.list(&dir()).unwrap();

        assert!(
            compactor(&storage, CompactConfig::default())
                .compact_if_due(&dir())
                .unwrap()
                .is_none()
        );
        assert_eq!(storage.list(&dir()).unwrap(), before);
    }

    #[test]
    fn compacting_an_empty_directory_does_nothing() {
        let storage = InMemoryStorage::new();
        let result = compactor(&storage, eager()).compact(&dir(), &[]).unwrap();

        assert_eq!(result.rows, 0);
        assert!(result.written.is_empty());
    }

    // ---- disjoint ranges (§3) ----

    /// §3: output files must not share a `(metric_id, timestamp)`, so a reader
    /// that has located a key needs exactly one file. A cut mid-group would put
    /// one logical row's versions in two files.
    #[test]
    fn output_ranges_are_disjoint() {
        // Many logical rows, several versions each, and a tiny target so the
        // output is genuinely split.
        let mut rows = Vec::new();
        for metric in 1..=4u64 {
            for timestamp in 0..25i64 {
                for seq in 0..3u64 {
                    rows.push((
                        metric,
                        timestamp,
                        metric * 1000 + timestamp as u64 * 10 + seq,
                    ));
                }
            }
        }
        let storage = with_files(&[&rows[..rows.len() / 2], &rows[rows.len() / 2..]]);

        compactor(
            &storage,
            CompactConfig {
                max_files: 2,
                max_bytes: usize::MAX,
                target_bytes: 1, // forces a cut after every possible boundary
            },
        )
        .compact_if_due(&dir())
        .unwrap()
        .expect("due");

        let files = storage.list(&dir()).unwrap();
        assert!(files.len() > 1, "the target should have split the output");

        // No `(metric_id, timestamp)` may appear in two files.
        let mut seen: std::collections::HashMap<(MetricId, Micros), usize> =
            std::collections::HashMap::new();
        for (index, file) in files.iter().enumerate() {
            for (metric_id, timestamp, _) in keys_of(&storage, file).unwrap() {
                if let Some(other) = seen.insert((metric_id, timestamp), index) {
                    assert_eq!(other, index, "({metric_id}, {timestamp}) is in two files");
                }
            }
        }
    }

    /// The disjointness rule wins over the size target: one logical row with more
    /// versions than a file should hold stays whole.
    #[test]
    fn one_logical_row_is_never_split_across_files() {
        let rows: Vec<(u64, i64, u64)> = (0..500u64).map(|seq| (1, 10, seq)).collect();
        let storage = with_files(&[&rows[..250], &rows[250..]]);

        compactor(
            &storage,
            CompactConfig {
                max_files: 2,
                max_bytes: usize::MAX,
                target_bytes: 1,
            },
        )
        .compact_if_due(&dir())
        .unwrap()
        .expect("due");

        let files = storage.list(&dir()).unwrap();
        assert_eq!(
            files.len(),
            1,
            "a single logical row must stay in one file, even oversized"
        );
        assert_eq!(keys_of(&storage, &files[0]).unwrap().len(), 500);
    }

    /// The store has no rename, so an output re-emitting an input's `max_seq`
    /// must not collide with it — which would append to a live file.
    #[test]
    fn an_output_never_collides_with_an_input_name() {
        let storage = with_files(&[&[(1, 10, 42)], &[(1, 20, 41)]]);
        // Both inputs and the output share max_seq 42.
        let result = compactor(&storage, eager())
            .compact_if_due(&dir())
            .unwrap()
            .unwrap();

        assert_eq!(result.rows, 2, "a collision would have corrupted the file");
        assert!(
            result
                .written
                .iter()
                .all(|file| !result.removed.contains(&file.path)),
            "an output took an input's name"
        );
    }

    /// §1: a second pass over a partition nothing has written to since must not
    /// lose a row.
    ///
    /// [`compaction_is_idempotent_in_content`] means to cover this and cannot:
    /// its rows fit in one output file, and one file is never due, so its second
    /// and third passes do nothing at all. The case only exists once compaction
    /// emits several files — then the partition is still over `max_files`
    /// afterwards and comes round again with its inputs unchanged.
    /// Driven through [`Compactor::compact`] rather than `compact_if_due`, which
    /// [`is_settled`] would turn away: the safety of a pass may not rest on the
    /// policy deciding when to run one. A caller that merges a partition twice —
    /// a manual pass, a future scheduler — must still not lose a row.
    #[test]
    fn recompacting_a_sliced_partition_keeps_every_row() {
        let storage = with_files(&[&[(1, 10, 0)], &[(2, 20, 1)], &[(3, 30, 2)]]);
        let before = all_keys(&storage);

        let first = compactor(&storage, slicing())
            .compact_if_due(&dir())
            .unwrap()
            .expect("a three-file partition is due");
        // Without this the test passes for the wrong reason, which is exactly
        // how the idempotence test above came to be green.
        assert!(
            first.written.len() >= 2,
            "the setup must leave several files to merge again, got {}",
            first.written.len()
        );

        let again = storage.list(&dir()).unwrap();
        compactor(&storage, slicing()).compact(&dir(), &again).unwrap();

        assert_eq!(all_keys(&storage), before, "the second pass lost rows");
    }

    /// §3: once a partition holds nothing but one pass's output, another pass
    /// would merge the same rows into the same slices. The work is wasted, and
    /// it repeats for as long as the partition sits above `max_files`.
    #[test]
    fn a_partition_holding_only_one_passs_output_is_not_due_again() {
        let storage = with_files(&[&[(1, 10, 0)], &[(2, 20, 1)], &[(3, 30, 2)]]);
        let first = compactor(&storage, slicing())
            .compact_if_due(&dir())
            .unwrap()
            .expect("a three-file partition is due");
        assert!(
            first.written.len() >= slicing().max_files,
            "the setup must leave the partition over max_files"
        );

        let again = compactor(&storage, slicing()).compact_if_due(&dir()).unwrap();

        assert!(again.is_none(), "recompacted a partition with nothing new");
    }

    /// Compaction settles: it does not rewrite a partition on every pass.
    #[test]
    fn compaction_runs_once_for_a_partition_nothing_is_written_to() {
        let storage = with_files(&[&[(1, 10, 0)], &[(2, 20, 1)], &[(3, 30, 2)]]);

        let passes = (0..4)
            .filter(|_| {
                compactor(&storage, slicing())
                    .compact_if_due(&dir())
                    .unwrap()
                    .is_some()
            })
            .count();

        assert_eq!(passes, 1, "compaction did not settle");
    }

    /// A flush into a compacted partition makes it due again — that is the whole
    /// point of the rule being about generations rather than about file count.
    #[test]
    fn a_flush_into_a_compacted_partition_makes_it_due_again() {
        let storage = with_files(&[&[(1, 10, 0)], &[(2, 20, 1)], &[(3, 30, 2)]]);
        compactor(&storage, slicing()).compact_if_due(&dir()).unwrap();

        write_partition(
            &storage,
            &dir(),
            &batch(&[(4, 40, 3)]),
            ParquetConfig::default(),
        )
        .unwrap();

        let due = compactor(&storage, slicing()).compact_if_due(&dir()).unwrap();
        assert!(due.is_some(), "new rows must be merged in");
    }

    /// Files of differing generations mean a pass was interrupted, or a flush
    /// landed mid-merge. Either way there is real work to do.
    #[test]
    fn a_partition_of_mixed_generations_is_due() {
        let storage = InMemoryStorage::new();
        for (generation, rows) in [(1u32, &[(1u64, 10i64, 0u64)]), (2, &[(2, 20, 1)])] {
            write_named(
                &storage,
                &dir(),
                &batch(rows),
                ParquetConfig::default(),
                Some(Output::new(generation, 0)),
            )
            .unwrap();
        }

        let due = compactor(&storage, slicing()).compact_if_due(&dir()).unwrap();

        assert!(due.is_some(), "a half-merged partition must be finished");
    }

    /// §3: an output may never take the name of a file the same pass removes.
    ///
    /// [`an_output_never_collides_with_an_input_name`] asserts this over inputs
    /// a flush wrote, which use a name without a discriminator and so cannot
    /// collide. When the inputs are themselves compaction outputs, an unchanged
    /// input set yields the same slices, the same `max_seq` and the same indices
    /// — and therefore the very names being deleted.
    #[test]
    fn an_output_never_takes_the_name_of_an_input_it_replaces() {
        let storage = with_files(&[&[(1, 10, 0)], &[(2, 20, 1)], &[(3, 30, 2)]]);
        compactor(&storage, slicing()).compact_if_due(&dir()).unwrap();

        // Directly, for the reason given on the test above.
        let again = storage.list(&dir()).unwrap();
        let second = compactor(&storage, slicing())
            .compact(&dir(), &again)
            .unwrap();

        let clobbered: Vec<_> = second
            .written
            .iter()
            .filter(|file| second.removed.contains(&file.path))
            .map(|file| file.path.clone())
            .collect();
        assert!(
            clobbered.is_empty(),
            "outputs deleted as inputs of the same pass: {clobbered:?}"
        );
    }

    /// §1: compaction may run repeatedly, and must be a no-op in content each
    /// time — the same rows, still sorted.
    #[test]
    fn compaction_is_idempotent_in_content() {
        let storage = with_files(&[&[(1, 10, 0), (2, 20, 3)], &[(1, 10, 1)], &[(3, 5, 2)]]);
        let before = all_keys(&storage);

        for _ in 0..3 {
            let _ = compactor(&storage, eager()).compact_if_due(&dir()).unwrap();
        }

        assert_eq!(all_keys(&storage), before);
    }

    /// A failing store must not remove an input: the outputs are not durable, so
    /// removing them would lose rows outright.
    /// An unreadable file must not be merged away while it cannot be verified,
    /// and must not stop the readable ones from being compacted either.
    #[test]
    fn an_unreadable_entry_is_skipped_not_merged() {
        let storage = with_files(&[&[(1, 10, 0)], &[(1, 20, 1)], &[(1, 30, 2)]]);
        // A directory sitting among the files: unreadable, and not a file.
        write_partition(
            &storage,
            &dir().join("stray"),
            &batch(&[(9, 99, 9)]),
            ParquetConfig::default(),
        )
        .unwrap();

        let result = compactor(&storage, eager())
            .compact_if_due(&dir())
            .expect("must not fail")
            .expect("due");

        assert_eq!(result.rows, 3, "only the readable files were merged");
        assert!(
            result.removed.iter().all(|path| !path.ends_with("stray")),
            "the unreadable entry must not have been removed"
        );
    }

    #[test]
    fn a_failing_write_leaves_the_inputs_in_place() {
        let storage = with_files(&[&[(1, 10, 0)], &[(1, 20, 1)]]);
        let before = storage.list(&dir()).unwrap();
        storage.inject(crate::storage::Fault::Fail(std::io::ErrorKind::StorageFull));

        assert!(compactor(&storage, eager()).compact_if_due(&dir()).is_err());

        // Fault injection only affects writes, so the listing still shows them.
        assert_eq!(
            storage.list(&dir()).unwrap(),
            before,
            "inputs must survive a failed compaction"
        );
    }

    /// §11: the walk goes exactly as deep as the interval, so it finds partition
    /// directories rather than the levels above them.
    #[test]
    fn partition_dirs_finds_directories_at_the_interval_depth() {
        let storage = InMemoryStorage::new();
        for month in ["08", "09"] {
            write_partition(
                &storage,
                &PathBuf::from(format!("data/readings/year=2026/month={month}")),
                &batch(&[(1, 10, 0)]),
                ParquetConfig::default(),
            )
            .unwrap();
        }

        let found = partition_dirs(&storage, "readings", PartitionInterval::Month).unwrap();

        assert_eq!(
            found,
            vec![
                PathBuf::from("data/readings/year=2026/month=08"),
                PathBuf::from("data/readings/year=2026/month=09")
            ]
        );
    }

    /// A layout at a different depth yields nothing, which is why §11 validates
    /// the interval at startup rather than guessing here.
    #[test]
    fn a_walk_at_the_wrong_depth_finds_nothing_useful() {
        let storage = InMemoryStorage::new();
        write_partition(
            &storage,
            &dir(),
            &batch(&[(1, 10, 0)]),
            ParquetConfig::default(),
        )
        .unwrap();

        // The files are two levels down; looking one level down finds the year.
        let shallow = partition_dirs(&storage, "readings", PartitionInterval::Year).unwrap();
        assert_eq!(shallow, vec![PathBuf::from("data/readings/year=2026")]);

        // Pointed at that level the compactor finds no readable files and does
        // nothing, rather than failing on a directory it cannot read.
        assert!(
            compactor(&storage, eager())
                .compact_if_due(&shallow[0])
                .expect("must not fail on a directory")
                .is_none(),
            "a year directory holds directories, not files"
        );
    }

    #[test]
    fn a_table_with_no_partitions_yields_none() {
        let storage = InMemoryStorage::new();
        assert!(
            partition_dirs(&storage, "absent", PartitionInterval::Month)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn reading_a_directory_with_no_files_is_not_due() {
        let storage = InMemoryStorage::new();
        assert!(
            compactor(&storage, eager())
                .compact_if_due(Path::new("data/readings/year=2026/month=09"))
                .unwrap()
                .is_none()
        );
    }
}
