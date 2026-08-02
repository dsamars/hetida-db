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
    /// Flush once a partition's oldest unflushed row is this old.
    ///
    /// Measured against the **clock**, unlike the suppression window (§1), which
    /// advances on ingest time. The difference is deliberate: suppression is data
    /// semantics and must replay identically, where this is an operational bound
    /// on how long state may sit in memory. On ingest time a quiescent instance
    /// would never age anything out and its last rows would stay unqueryable for
    /// as long as nothing was written.
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
/// `now` and `oldest` are clock readings — see [`FlushConfig::max_age`].
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

        let written = write_partition(
            self.storage.as_ref(),
            &self.partition_dir(start)?,
            &batch,
            self.parquet,
        )?;

        // Only now, with the file renamed into place, may the rows leave memory.
        // Releasing earlier — or on the error path above — would lose them: a
        // frozen table is the only copy outside the log until its file exists.
        state.release(start, &frozen);

        Ok(Some(written))
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

/// Drives flushes for one table's partitions (§2).
///
/// Split from [`Flusher`] so the *decision* and the *mechanics* stay testable
/// without concurrency: [`Self::run_once`] does one full pass and is what the
/// tests exercise, while [`Self::run`] only adds a timer around it.
///
/// # A failed flush does not stop ingestion
///
/// A write error is counted and the partition keeps its rows, so the next pass
/// retries. Memory then grows, and the §7 cap eventually refuses ingestion with
/// `RESOURCE_EXHAUSTED` — a chain that ends in an explicit rejection the client
/// can see, rather than in silently dropped rows or a stalled stream. Failures
/// are counted precisely so an operator can tell that state apart from a healthy
/// instance that simply has nothing to flush.
#[derive(Debug)]
pub struct FlushScheduler {
    flusher: Flusher,
    config: FlushConfig,
    clock: Arc<dyn crate::clock::Clock>,
    flushed: std::sync::atomic::AtomicU64,
    failed: std::sync::atomic::AtomicU64,
}

/// What one scheduler pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FlushPass {
    /// Partitions written.
    pub flushed: usize,
    /// Partitions whose write failed and which keep their rows.
    pub failed: usize,
}

impl FlushScheduler {
    /// Drives `flusher` on `config`, reading the age bound from `clock`.
    pub fn new(flusher: Flusher, config: FlushConfig, clock: Arc<dyn crate::clock::Clock>) -> Self {
        Self {
            flusher,
            config,
            clock,
            flushed: std::sync::atomic::AtomicU64::new(0),
            failed: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Flushes every partition of `state` that is due, and returns what it did.
    ///
    /// One pass never gives up early: a partition whose write fails does not
    /// prevent the others from being written.
    pub fn run_once(&self, state: &PartitionedTable) -> FlushPass {
        let now = self.clock.now_micros();
        let mut pass = FlushPass::default();

        for start in state.partition_starts() {
            let due = flush_reason(
                self.config,
                state.partition_bytes(start),
                state.oldest_system_time(start),
                now,
            );
            if due.is_none() {
                continue;
            }

            match self.flusher.flush(state, start) {
                Ok(Some(_)) => {
                    pass.flushed += 1;
                    self.flushed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                // Nothing was held after all — a concurrent flush got there first.
                Ok(None) => {}
                Err(_) => {
                    pass.failed += 1;
                    self.failed
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        pass
    }

    /// Partitions successfully written since this scheduler started.
    pub fn flushed(&self) -> u64 {
        self.flushed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Flush failures since this scheduler started.
    ///
    /// Non-zero means partitions are retaining rows they should have shed, and
    /// the memory cap will eventually refuse ingestion (§7).
    pub fn failed(&self) -> u64 {
        self.failed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Runs [`Self::run_once`] every `tick` until `shutdown` resolves.
    ///
    /// The timer is the only thing this adds: everything it decides is already
    /// covered by `run_once`.
    pub async fn run(
        &self,
        state: Arc<PartitionedTable>,
        tick: Duration,
        shutdown: impl std::future::Future<Output = ()>,
    ) {
        let mut timer = tokio::time::interval(tick);
        // Skip missed ticks rather than firing back to back after a long pass:
        // catching up would flush a burst for no benefit.
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let shutdown = std::pin::pin!(shutdown);
        let mut shutdown = shutdown;

        loop {
            tokio::select! {
                _ = timer.tick() => { self.run_once(&state); }
                () = &mut shutdown => return,
            }
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

    use crate::clock::TestClock;
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

    // ---- the scheduler (§2, §7) ----

    fn scheduler(
        storage: &InMemoryStorage,
        config: FlushConfig,
        clock: Arc<TestClock>,
    ) -> FlushScheduler {
        FlushScheduler::new(flusher(storage), config, clock)
    }

    /// §2: a pass flushes what is due and leaves what is not.
    #[test]
    fn a_pass_flushes_only_due_partitions() {
        let storage = InMemoryStorage::new();
        let state = table();
        // August is old, September has just arrived.
        state.insert(&stamped(&[(1, AUGUST, 0)], 0)).unwrap();
        state
            .insert(&stamped(&[(1, SEPTEMBER, 1)], 10 * MINUTE))
            .unwrap();

        let clock = Arc::new(TestClock::new(10 * MINUTE));
        let scheduler = scheduler(
            &storage,
            FlushConfig {
                max_bytes: usize::MAX,
                max_age: 5 * MINUTE,
            },
            clock,
        );

        let pass = scheduler.run_once(&state);

        assert_eq!(
            pass,
            FlushPass {
                flushed: 1,
                failed: 0
            }
        );
        assert_eq!(state.num_rows(), 1, "september stays in memory");
    }

    /// §2: age is measured against the clock, so a quiescent instance still
    /// sheds its state — on ingest time nothing would ever age out.
    #[test]
    fn a_quiescent_partition_ages_out_on_the_clock() {
        let storage = InMemoryStorage::new();
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 0)], 0)).unwrap();

        let clock = Arc::new(TestClock::new(0));
        let scheduler = scheduler(
            &storage,
            FlushConfig {
                max_bytes: usize::MAX,
                max_age: 5 * MINUTE,
            },
            Arc::clone(&clock),
        );

        assert_eq!(scheduler.run_once(&state).flushed, 0, "not yet due");
        // No further writes: only the clock moves.
        clock.advance(5 * MINUTE);
        assert_eq!(scheduler.run_once(&state).flushed, 1, "now due");
        assert_eq!(state.num_rows(), 0);
    }

    #[test]
    fn a_pass_over_nothing_due_does_nothing() {
        let storage = InMemoryStorage::new();
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 0)], 0)).unwrap();

        let scheduler = scheduler(
            &storage,
            FlushConfig {
                max_bytes: usize::MAX,
                max_age: Micros::MAX,
            },
            Arc::new(TestClock::new(0)),
        );

        assert_eq!(scheduler.run_once(&state), FlushPass::default());
        assert_eq!(state.num_rows(), 1);
    }

    /// §7: a failed flush keeps its rows and is counted, so the memory cap can
    /// do its job rather than the rows being dropped.
    #[test]
    fn a_failed_flush_keeps_its_rows_and_is_counted() {
        let storage = InMemoryStorage::new();
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 0)], 0)).unwrap();

        let scheduler = scheduler(
            &storage,
            FlushConfig {
                max_bytes: 1,
                max_age: Micros::MAX,
            },
            Arc::new(TestClock::new(0)),
        );
        storage.inject(crate::storage::Fault::Fail(std::io::ErrorKind::StorageFull));

        let pass = scheduler.run_once(&state);

        assert_eq!(
            pass,
            FlushPass {
                flushed: 0,
                failed: 1
            }
        );
        assert_eq!(scheduler.failed(), 1, "the failure must be visible");
        assert_eq!(scheduler.flushed(), 0);
        // The freeze already happened, but the retired table is still held — so
        // the rows are neither lost nor invisible, and the §7 cap still sees the
        // memory they occupy. Handing the only reference to the writer used to
        // drop them on exactly this path.
        assert_eq!(state.num_rows(), 1, "a failed write must not lose the rows");
        assert!(state.memory_bytes() > 0, "and the cap must still see them");
    }

    /// §2, §5: the window a query must not fall into. Between the freeze and
    /// the file being renamed into place the rows are in neither the active
    /// table nor on disk, so the partition holds them — and holds them itself,
    /// not through the caller's reference.
    #[test]
    fn a_frozen_partition_stays_visible_before_its_file_exists() {
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 0)], 100)).unwrap();
        let start = august();

        let frozen = state.freeze(start).unwrap().expect("august");
        assert_eq!(state.num_rows(), 1, "the rows must stay readable");

        drop(frozen);
        assert_eq!(
            state.num_rows(),
            1,
            "holding them must not depend on the flusher's own reference"
        );
        assert_eq!(state.frozen(start).len(), 1);
    }

    /// And they leave exactly when their file is in place, not before.
    #[test]
    fn a_successful_flush_releases_the_rows_it_wrote() {
        let storage = InMemoryStorage::new();
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 0)], 100)).unwrap();

        flusher(&storage)
            .flush(&state, august())
            .expect("flush")
            .expect("a file");

        assert_eq!(state.num_rows(), 0, "written rows leave memory");
        assert!(state.frozen(august()).is_empty());
    }

    /// §4: a checkpoint may not pass a `seq` that only a frozen table holds —
    /// the segment covering it would be reclaimed while the rows are nowhere
    /// durable.
    #[test]
    fn the_checkpoint_stops_below_a_seq_still_being_flushed() {
        let state = table();
        state.insert(&stamped(&[(1, AUGUST, 5)], 100)).unwrap();

        let _frozen = state.freeze(august()).unwrap().expect("august");

        assert_eq!(
            Flusher::checkpoint_watermark(&state, Some(99)),
            Some(4),
            "seq 5 is in a frozen table, so nothing at or above it is durable"
        );
    }

    /// One partition's failure must not stop the others from being written.
    #[test]
    fn a_failure_does_not_abandon_the_rest_of_the_pass() {
        let storage = InMemoryStorage::new();
        let state = table();
        state
            .insert(&stamped(&[(1, AUGUST, 0), (1, SEPTEMBER, 1)], 0))
            .unwrap();

        let scheduler = scheduler(
            &storage,
            FlushConfig {
                max_bytes: 1,
                max_age: Micros::MAX,
            },
            Arc::new(TestClock::new(0)),
        );

        let pass = scheduler.run_once(&state);
        assert_eq!(
            pass.flushed + pass.failed,
            2,
            "both partitions must have been attempted"
        );
    }

    /// The timer only wraps `run_once`, and shutdown returns rather than
    /// aborting. Driven with paused time, so the test cannot flake.
    #[tokio::test(start_paused = true)]
    async fn the_timer_runs_passes_until_shutdown() {
        let storage = InMemoryStorage::new();
        let state = Arc::new(table());
        state.insert(&stamped(&[(1, AUGUST, 0)], 0)).unwrap();

        let scheduler = scheduler(
            &storage,
            FlushConfig {
                max_bytes: 1,
                max_age: Micros::MAX,
            },
            Arc::new(TestClock::new(0)),
        );

        // Paused time auto-advances while every task is idle, so three ticks
        // pass and the shutdown resolves without a single real sleep.
        scheduler
            .run(
                Arc::clone(&state),
                Duration::from_secs(1),
                tokio::time::sleep(Duration::from_secs(3)),
            )
            .await;

        assert!(
            scheduler.flushed() >= 1,
            "at least one tick must have flushed"
        );
    }
}
