// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Moving frozen memtables to Parquet (§2, §4).
//!
//! # Trigger
//!
//! Per partition, on **size or age, whichever fires first** (§2). Size bounds
//! memory and yields well-sized row groups; age bounds the data-loss window and
//! the query staleness of a low-rate metric, which size alone never would — a
//! metric written once an hour would sit in memory indefinitely.
//!
//! Never globally: a global flush writes into many partitions at once and
//! scatters small files (§2).
//!
//! # Non-blocking
//!
//! [`Flusher::flush`] freezes the partition and writes the retired table. Writers
//! move on to the replacement immediately, so ingestion never waits for a flush
//! (§2). Only the freezing side waits, and only for the in-flight inserts to
//! leave.
//!
//! # Why the checkpoint is not the file's `seq`
//!
//! A checkpoint at `seq` asserts that *everything* at or below it is in Parquet,
//! and §4 reclaims segments on that basis. The file just written may hold a high
//! `seq` while another partition still holds a lower one in memory — a late
//! arrival routed to last month, flushed after this month's rows. Checkpointing
//! the file's own maximum would then reclaim a segment covering rows that exist
//! only in memory, and a crash would lose them silently.
//!
//! The safe watermark is therefore one below the lowest `seq` still held
//! anywhere. [`Flusher::checkpoint_watermark`] computes it, and it is the only
//! value this module ever checkpoints.
//!
//! # Where written files are tracked
//!
//! Nowhere: the files are the record. A partition's directory listing is the
//! truth, which is what the Hive-style layout is for (§11). A manifest would be a
//! second piece of durable state to keep consistent with the directory, and the
//! failure mode — a manifest disagreeing with what is on disk — is worse than a
//! listing.

use std::sync::Arc;
use std::time::Duration;

use crate::clock::Micros;
use crate::instance::DATA_DIR;
use crate::memtable::MemTableError;
use crate::parquet::{ParquetConfig, WriteError, WrittenFile, write_partition};
use crate::partition::{PartitionError, PartitionInterval};
use crate::seq::Seq;
use crate::storage::Storage;
use crate::table::PartitionedTable;

/// Why a flush failed.
#[derive(Debug, thiserror::Error)]
pub enum FlushError {
    /// The frozen table could not be materialised in flush order (§1).
    #[error(transparent)]
    MemTable(#[from] MemTableError),
    /// The file could not be written.
    #[error(transparent)]
    Write(#[from] WriteError),
    /// The partition's path could not be derived (§11).
    #[error(transparent)]
    Partition(#[from] PartitionError),
}

/// When a partition is flushed (§2).
#[derive(Debug, Clone, Copy)]
pub struct FlushConfig {
    /// Flush once a partition holds this many bytes.
    pub max_bytes: usize,
    /// Flush once a partition's oldest unflushed row is this old, in ingest time.
    ///
    /// Measured on `system_time` rather than a clock read, for the same reason the
    /// suppression window is (§1): it needs no clock on the write path and makes a
    /// replay reach the same decisions.
    pub max_age: Micros,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            // 64 MiB: large enough for row groups worth pruning, small enough
            // that a handful of partitions stay well inside a memory budget.
            max_bytes: 64 * 1024 * 1024,
            max_age: Duration::from_secs(300).as_micros() as Micros,
        }
    }
}

/// Why a partition is due for a flush, or why it is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// The partition reached the size cap.
    Size {
        /// Bytes it holds.
        held: usize,
    },
    /// The partition's oldest row reached the age cap.
    Age {
        /// How long ago it arrived, in ingest time.
        age: Micros,
    },
}

/// Whether a partition holding `bytes` since `oldest` is due at `now` (§2).
///
/// Size is checked first only because it is the cheaper answer; either alone is
/// sufficient, which is what "whichever fires first" means.
pub fn flush_reason(
    config: FlushConfig,
    bytes: usize,
    oldest: Option<Micros>,
    now: Micros,
) -> Option<FlushReason> {
    if bytes == 0 {
        // An empty partition has nothing to write, however old it looks.
        return None;
    }
    if bytes >= config.max_bytes {
        return Some(FlushReason::Size { held: bytes });
    }

    let age = now.saturating_sub(oldest?);
    (age >= config.max_age).then_some(FlushReason::Age { age })
}

/// Writes frozen memtables of one table to Parquet.
#[derive(Debug)]
pub struct Flusher {
    storage: Arc<dyn Storage>,
    table: String,
    interval: PartitionInterval,
    parquet: ParquetConfig,
}

impl Flusher {
    /// Flushes `table`'s partitions into `storage`.
    pub fn new(
        storage: Arc<dyn Storage>,
        table: impl Into<String>,
        interval: PartitionInterval,
        parquet: ParquetConfig,
    ) -> Self {
        Self {
            storage,
            table: table.into(),
            interval,
            parquet,
        }
    }

    /// The directory a partition's files live in (§11).
    pub fn partition_dir(&self, start: Micros) -> Result<std::path::PathBuf, FlushError> {
        Ok(std::path::Path::new(DATA_DIR)
            .join(&self.table)
            .join(self.interval.path(start)?))
    }

    /// Freezes one partition and writes it to Parquet (§2).
    ///
    /// Returns `None` if the partition held nothing. Ingestion does not wait: the
    /// replacement table is installed before a byte is written.
    pub fn flush(
        &self,
        state: &PartitionedTable,
        start: Micros,
    ) -> Result<Option<WrittenFile>, FlushError> {
        let Some(frozen) = state.freeze(start).map_err(|error| match error {
            crate::table::TableError::MemTable(error) => FlushError::MemTable(error),
            crate::table::TableError::Partition(error) => FlushError::Partition(error),
        })?
        else {
            return Ok(None);
        };

        // §1: the order is produced here, and the writer refuses it if wrong.
        let Some(batch) = frozen.sorted_batch()? else {
            return Ok(None);
        };

        Ok(Some(write_partition(
            self.storage.as_ref(),
            &self.partition_dir(start)?,
            &batch,
            self.parquet,
        )?))
    }

    /// The highest `seq` a checkpoint may safely claim is in Parquet (§4).
    ///
    /// One below the lowest `seq` still held in memory, or `highest_written` when
    /// memory is empty. Never the just-written file's maximum — see the module
    /// documentation.
    pub fn checkpoint_watermark(
        state: &PartitionedTable,
        highest_written: Option<Seq>,
    ) -> Option<Seq> {
        match state.min_seq() {
            // Something is still in memory: a segment may only be reclaimed
            // strictly below it.
            Some(lowest) => lowest.checked_sub(1),
            None => highest_written,
        }
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

    use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use crate::parquet::read_partition;
    use crate::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};
    use crate::storage::InMemoryStorage;
    use crate::suppression::SuppressionConfig;

    /// 2026-08-02T14:37:12Z and a month later.
    const AUGUST: Micros = 1_785_681_432_000_000;
    const SEPTEMBER: Micros = 1_788_220_800_000_000;
    const MINUTE: Micros = 60_000_000;

    fn storage_schema() -> SchemaRef {
        append_system_fields(&Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn stamped(rows: &[(u64, Micros, u64)], system_time: Micros) -> RecordBatch {
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
                Arc::new(TimestampMicrosecondArray::from(vec![
                    system_time;
                    rows.len()
                ])),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, _, seq)| *seq).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn table() -> PartitionedTable {
        PartitionedTable::new(
            storage_schema(),
            PartitionInterval::Month,
            SuppressionConfig::for_stream(),
        )
    }

    fn flusher(storage: &InMemoryStorage) -> Flusher {
        Flusher::new(
            Arc::new(storage.clone()),
            "readings",
            PartitionInterval::Month,
            ParquetConfig::default(),
        )
    }

    fn august() -> Micros {
        PartitionInterval::Month.start(AUGUST).unwrap()
    }

    // ---- the trigger (§2) ----

    #[test]
    fn an_empty_partition_is_never_due() {
        let config = FlushConfig {
            max_bytes: 1,
            max_age: 1,
        };
        assert_eq!(flush_reason(config, 0, Some(0), 1_000_000), None);
    }

    #[test]
    fn the_size_cap_fires() {
        let config = FlushConfig {
            max_bytes: 100,
            max_age: Micros::MAX,
        };
        assert_eq!(
            flush_reason(config, 100, Some(0), 0),
            Some(FlushReason::Size { held: 100 })
        );
        assert_eq!(flush_reason(config, 99, Some(0), 0), None);
    }

    /// §2: age bounds the staleness of a low-rate metric, which the size cap
    /// alone never would — it would sit in memory indefinitely.
    #[test]
    fn the_age_cap_fires_for_a_partition_that_never_fills() {
        let config = FlushConfig {
            max_bytes: usize::MAX,
            max_age: 5 * MINUTE,
        };
        assert_eq!(
            flush_reason(config, 1, Some(0), 5 * MINUTE),
            Some(FlushReason::Age { age: 5 * MINUTE })
        );
        assert_eq!(flush_reason(config, 1, Some(0), 4 * MINUTE), None);
    }

    /// §2: whichever fires first. With both caps reachable, size answers because
    /// it is the cheaper check, but either alone suffices.
    #[test]
    fn either_cap_alone_is_enough() {
        let config = FlushConfig {
            max_bytes: 100,
            max_age: 5 * MINUTE,
        };
        assert!(flush_reason(config, 100, None, 0).is_some(), "size alone");
        assert!(
            flush_reason(config, 1, Some(0), 5 * MINUTE).is_some(),
            "age alone"
        );
    }

    #[test]
    fn a_partition_with_no_rows_yet_is_not_aged_out() {
        let config = FlushConfig {
            max_bytes: usize::MAX,
            max_age: 1,
        };
        // Bytes without a known oldest row cannot be aged: nothing says when.
        assert_eq!(flush_reason(config, 10, None, Micros::MAX), None);
    }

    // ---- the flush (§1, §2) ----

    /// §1, §2: the frozen table becomes one sorted file, with every version.
    #[test]
    fn flushing_writes_a_sorted_file_with_every_version() {
        let storage = InMemoryStorage::new();
        let state = table();
        // Out of order, two versions of one logical row.
        state
            .insert(&stamped(
                &[(2, AUGUST, 0), (1, AUGUST, 3), (1, AUGUST, 1)],
                100,
            ))
            .unwrap();

        let file = flusher(&storage)
            .flush(&state, august())
            .unwrap()
            .expect("a file");

        assert_eq!(file.rows, 3, "every version must reach the file");
        assert_eq!(file.max_seq, 3);
        let read = read_partition(&storage, &file.path).unwrap();
        assert_eq!(read.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    /// §11: the file lands in the partition's directory, derived from the
    /// interval rather than assumed.
    #[test]
    fn the_file_lands_in_its_partition_directory() {
        let storage = InMemoryStorage::new();
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 0)], 100)).unwrap();

        let file = flusher(&storage).flush(&state, august()).unwrap().unwrap();

        assert!(
            file.path
                .to_string_lossy()
                .starts_with("data/readings/year=2026/month=08/"),
            "unexpected path: {}",
            file.path.display()
        );
    }

    /// §2: ingestion does not wait. The partition accepts writes immediately
    /// after a flush, and the retired rows are not in the live table.
    #[test]
    fn the_partition_accepts_writes_again_immediately() {
        let storage = InMemoryStorage::new();
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 0)], 100)).unwrap();

        flusher(&storage).flush(&state, august()).unwrap().unwrap();

        assert_eq!(state.num_rows(), 0, "the retired rows left memory");
        state.insert(&stamped(&[(1, AUGUST, 1)], 200)).unwrap();
        assert_eq!(state.num_rows(), 1, "and new writes land");
    }

    #[test]
    fn flushing_an_empty_partition_writes_nothing() {
        let storage = InMemoryStorage::new();
        assert!(
            flusher(&storage)
                .flush(&table(), august())
                .unwrap()
                .is_none()
        );
    }

    /// §2: a flush is per partition. Flushing one leaves the other's rows in
    /// memory, and writes only one file.
    #[test]
    fn flushing_one_partition_leaves_the_other() {
        let storage = InMemoryStorage::new();
        let state = table();
        state
            .insert(&stamped(&[(1, AUGUST, 0), (1, SEPTEMBER, 1)], 100))
            .unwrap();

        flusher(&storage).flush(&state, august()).unwrap().unwrap();

        assert_eq!(state.num_rows(), 1, "september is still in memory");
        assert_eq!(
            storage
                .list(std::path::Path::new("data/readings/year=2026/month=09"))
                .unwrap()
                .len(),
            0,
            "september must not have been written"
        );
    }

    // ---- the checkpoint (§4) ----

    /// §4: this is the trap. September flushed at seq 1 while August still holds
    /// seq 0 in memory. Checkpointing the file's own maximum would reclaim a
    /// segment covering August's rows, which exist only in memory — a crash would
    /// lose them silently.
    #[test]
    fn the_checkpoint_never_passes_a_seq_still_in_memory() {
        let storage = InMemoryStorage::new();
        let state = table();
        state
            .insert(&stamped(&[(1, AUGUST, 0), (1, SEPTEMBER, 1)], 100))
            .unwrap();

        let september = PartitionInterval::Month.start(SEPTEMBER).unwrap();
        let file = flusher(&storage)
            .flush(&state, september)
            .unwrap()
            .expect("september");
        assert_eq!(file.max_seq, 1, "the file itself holds seq 1");

        let watermark = Flusher::checkpoint_watermark(&state, Some(file.max_seq));
        assert_eq!(
            watermark, None,
            "seq 0 is still in memory, so nothing may be reclaimed yet"
        );
    }

    #[test]
    fn the_checkpoint_reaches_the_written_seq_once_memory_is_empty() {
        let storage = InMemoryStorage::new();
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 7)], 100)).unwrap();

        let file = flusher(&storage).flush(&state, august()).unwrap().unwrap();

        assert_eq!(
            Flusher::checkpoint_watermark(&state, Some(file.max_seq)),
            Some(7),
            "with nothing left in memory the whole file is checkpointable"
        );
    }

    #[test]
    fn the_checkpoint_stops_one_below_the_lowest_held_seq() {
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 5)], 100)).unwrap();

        assert_eq!(Flusher::checkpoint_watermark(&state, Some(99)), Some(4));
    }

    #[test]
    fn nothing_written_and_nothing_held_checkpoints_nothing() {
        assert_eq!(Flusher::checkpoint_watermark(&table(), None), None);
    }
}
