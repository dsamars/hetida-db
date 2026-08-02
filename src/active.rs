// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Freeze-and-swap around the active memtable.
//!
//! §2 requires that a flush never blocks ingestion. The active table is
//! therefore retired rather than drained: [`ActiveMemTable::freeze`] installs a
//! fresh table, waits for the writers already inside the old one to leave, and
//! hands it over. Writers never wait on a flush; only the freezing side waits,
//! and only for the microseconds an in-flight insert takes.
//!
//! # Why the retry loop
//!
//! A writer that loaded the active table just before a swap would otherwise
//! write into a table already handed to the flusher, and the row would be lost.
//! Registering as a writer and *then* re-checking which table is current closes
//! that window: if the table moved on, the writer retries against the new one.
//!
//! # Suppression scope
//!
//! Unchanged-value suppression (§1) must reach across a freeze: the comparison
//! state lives in a [`SuppressionIndex`] this holder owns and lends to every
//! table it installs, so retiring a table does not reset it. Its reach is the
//! ingest-time window configured for *this table*, not the lifetime of a
//! memtable — which is what keeps the guarantee independent of flush timing, and
//! what lets a table recomputed hourly and a table fed by a stream sit in the
//! same instance with the reach each needs (§6).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::memtable::{InsertOutcome, MemTable, MemTableError};
use crate::suppression::{SuppressionConfig, SuppressionIndex};

/// Holds the table currently accepting writes and swaps it out on flush.
#[derive(Debug)]
pub struct ActiveMemTable {
    schema: SchemaRef,
    suppression: Arc<SuppressionIndex>,
    current: ArcSwap<MemTable>,
}

impl ActiveMemTable {
    /// Creates a holder with a fresh, empty table and the given reach.
    ///
    /// The reach is a per-table setting (§1, §6): a table a job recomputes on a
    /// schedule needs a window spanning its run interval, where a table fed by a
    /// stream needs minutes and would not afford hours. There is deliberately no
    /// instance-wide default to fall back on — see [`SuppressionConfig`].
    pub fn new(schema: SchemaRef, suppression: SuppressionConfig) -> Result<Self, MemTableError> {
        Self::with_suppression(schema, Arc::new(SuppressionIndex::new(suppression)))
    }

    /// Creates a holder comparing against `suppression`.
    ///
    /// The index outlives every table installed here, which is what lets a
    /// repeat arriving after a freeze still be recognised (§1).
    pub fn with_suppression(
        schema: SchemaRef,
        suppression: Arc<SuppressionIndex>,
    ) -> Result<Self, MemTableError> {
        let table = MemTable::new(Arc::clone(&schema), Arc::clone(&suppression))?;
        Ok(Self {
            schema,
            suppression,
            current: ArcSwap::from_pointee(table),
        })
    }

    /// The suppression state shared by every table this holder installs.
    pub fn suppression(&self) -> &Arc<SuppressionIndex> {
        &self.suppression
    }

    /// The table currently accepting writes.
    pub fn current(&self) -> Arc<MemTable> {
        self.current.load_full()
    }

    /// Writes a stamped batch into whichever table is current.
    ///
    /// Retries if a freeze intervenes between choosing the table and entering
    /// it, so a write is never applied to a table already handed over.
    pub fn insert(&self, batch: &RecordBatch) -> Result<InsertOutcome, MemTableError> {
        loop {
            let table = self.current.load_full();
            let guard = WriterGuard::enter(&table);

            if !Arc::ptr_eq(&self.current.load_full(), &table) {
                // Swapped out from under us; drop the guard and take the new one.
                drop(guard);
                continue;
            }

            return table.insert(batch);
        }
    }

    /// Retires the active table and returns it, installing a fresh one.
    ///
    /// Returns only once no writer remains inside the retired table, so the
    /// caller receives a table nothing can still be appending to.
    pub fn freeze(&self) -> Result<Arc<MemTable>, MemTableError> {
        // The replacement borrows the same suppression state, so the retired
        // table's comparison history is not lost with it (§1).
        let replacement = Arc::new(MemTable::new(
            Arc::clone(&self.schema),
            Arc::clone(&self.suppression),
        )?);
        let retired = self.current.swap(replacement);

        while retired.writers_in_flight() > 0 {
            std::hint::spin_loop();
            std::thread::yield_now();
        }

        Ok(retired)
    }
}

/// Marks a writer as being inside a table for as long as it lives.
#[derive(Debug)]
struct WriterGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> WriterGuard<'a> {
    fn enter(table: &'a MemTable) -> Self {
        let counter = table.writers();
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use crate::clock::Micros;
    use crate::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};
    use crate::suppression::SuppressionConfig;

    fn storage_schema() -> SchemaRef {
        append_system_fields(&Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn row(metric_id: u64, timestamp: i64, seq: u64) -> RecordBatch {
        RecordBatch::try_new(
            storage_schema(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
                Arc::new(UInt64Array::from(vec![metric_id])),
                Arc::new(Float64Array::from(vec![seq as f64])),
                Arc::new(TimestampMicrosecondArray::from(vec![100i64])),
                Arc::new(UInt64Array::from(vec![seq])),
            ],
        )
        .unwrap()
    }

    fn active() -> ActiveMemTable {
        ActiveMemTable::new(storage_schema(), SuppressionConfig::default()).unwrap()
    }

    #[test]
    fn a_new_holder_starts_with_an_empty_table() {
        assert_eq!(active().current().num_rows(), 0);
    }

    /// §2: the frozen table carries what was written, and the writer keeps it
    /// alive independently of the holder.
    #[test]
    fn freezing_hands_over_the_table_that_was_active() {
        let active = active();
        active.insert(&row(1, 10, 0)).unwrap();
        active.insert(&row(2, 20, 1)).unwrap();

        let frozen = active.freeze().unwrap();

        assert_eq!(frozen.num_rows(), 2);
        assert_eq!(active.current().num_rows(), 0, "a fresh table takes over");
    }

    #[test]
    fn freezing_installs_a_different_table() {
        let active = active();
        let before = active.current();
        let frozen = active.freeze().unwrap();

        assert!(Arc::ptr_eq(&before, &frozen));
        assert!(!Arc::ptr_eq(&active.current(), &frozen));
    }

    #[test]
    fn freezing_an_empty_table_is_allowed() {
        let active = active();
        assert_eq!(active.freeze().unwrap().num_rows(), 0);
    }

    #[test]
    fn writes_after_a_freeze_land_in_the_new_table() {
        let active = active();
        active.insert(&row(1, 10, 0)).unwrap();
        let frozen = active.freeze().unwrap();
        active.insert(&row(2, 20, 1)).unwrap();

        assert_eq!(frozen.num_rows(), 1);
        assert_eq!(active.current().num_rows(), 1);
    }

    #[test]
    fn a_frozen_table_stays_readable() {
        let active = active();
        active.insert(&row(1, 10, 0)).unwrap();
        let frozen = active.freeze().unwrap();

        for _ in 0..3 {
            active.freeze().unwrap();
        }
        assert_eq!(frozen.num_rows(), 1, "the writer's handle stays valid");
    }

    /// §2: a flush must not lose an in-flight write. A row is either in the
    /// frozen table or in the one that replaced it — never nowhere.
    #[test]
    fn concurrent_writes_across_repeated_freezes_lose_nothing() {
        const WRITERS: u64 = 8;
        const PER_WRITER: u64 = 250;
        const FREEZES: usize = 40;

        let active = Arc::new(active());
        let next_seq = Arc::new(AtomicU64::new(0));

        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let active = Arc::clone(&active);
                let next_seq = Arc::clone(&next_seq);
                thread::spawn(move || {
                    for step in 0..PER_WRITER {
                        let seq = next_seq.fetch_add(1, Ordering::SeqCst);
                        active.insert(&row(writer, step as i64, seq)).unwrap();
                    }
                })
            })
            .collect();

        let freezer = {
            let active = Arc::clone(&active);
            thread::spawn(move || {
                (0..FREEZES)
                    .map(|_| {
                        std::thread::yield_now();
                        active.freeze().expect("freeze must succeed")
                    })
                    .collect::<Vec<_>>()
            })
        };

        for writer in writers {
            writer.join().expect("writer panicked");
        }
        let mut tables = freezer.join().expect("freezer panicked");
        tables.push(active.freeze().unwrap());

        let total: usize = tables.iter().map(|table| table.num_rows()).sum();
        assert_eq!(total, (WRITERS * PER_WRITER) as usize);
        assert_eq!(active.current().num_rows(), 0);
    }

    /// Builds a stamped single row with an explicit payload value and
    /// `system_time` — the axis the suppression window advances on (§1).
    fn at(
        metric_id: u64,
        timestamp: i64,
        seq: u64,
        value: f64,
        system_time: Micros,
    ) -> RecordBatch {
        RecordBatch::try_new(
            storage_schema(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
                Arc::new(UInt64Array::from(vec![metric_id])),
                Arc::new(Float64Array::from(vec![value])),
                Arc::new(TimestampMicrosecondArray::from(vec![system_time])),
                Arc::new(UInt64Array::from(vec![seq])),
            ],
        )
        .unwrap()
    }

    /// Builds a stamped single row with an explicit payload value.
    fn valued(metric_id: u64, timestamp: i64, seq: u64, value: f64) -> RecordBatch {
        at(metric_id, timestamp, seq, value, 100)
    }

    /// Holder with an explicit reach, for the window and cap properties.
    fn active_with(window: Micros, max_entries: usize) -> ActiveMemTable {
        ActiveMemTable::new(
            storage_schema(),
            SuppressionConfig {
                window,
                max_entries,
            },
        )
        .unwrap()
    }

    /// One hour of ingest time.
    const HOUR: Micros = 3_600_000_000;

    /// Runs `runs` recomputations an hour apart, each rewriting the same value,
    /// and returns how many versions survived across all tables.
    fn versions_after_hourly_runs(active: &ActiveMemTable, runs: u64) -> usize {
        let mut tables = Vec::new();
        for run in 0..runs {
            let stamped = at(1, 10, run, 1.5, run as Micros * HOUR);
            active.insert(&stamped).unwrap();
            // A day of ingest crosses many flushes.
            tables.push(active.freeze().unwrap());
        }
        tables.push(active.current());
        tables.iter().map(|table| table.num_rows()).sum()
    }

    /// §1: the dominant case. A job recomputing its horizon every hour
    /// resubmits unchanged values; with the reach sized on the run interval, a
    /// full day of runs must leave a single version.
    #[test]
    fn a_day_of_hourly_recomputation_leaves_one_version() {
        let active =
            ActiveMemTable::new(storage_schema(), SuppressionConfig::for_recomputed(HOUR)).unwrap();

        assert_eq!(
            versions_after_hourly_runs(&active, 24),
            1,
            "24 identical hourly runs must not manufacture versions"
        );
    }

    /// §1: sizing the reach for network timeouts leaves the recomputation case
    /// unhandled — which is the error the per-table window corrects. This pins
    /// that a stream-sized window genuinely does not cover an hourly rhythm, so
    /// the distinction is not decoration.
    #[test]
    fn a_stream_sized_window_does_not_cover_an_hourly_rhythm() {
        let active =
            ActiveMemTable::new(storage_schema(), SuppressionConfig::for_stream()).unwrap();

        assert_eq!(
            versions_after_hourly_runs(&active, 24),
            24,
            "a stream window must not be expected to span hours"
        );
    }

    /// §1, §6: the reach belongs to the table. Two tables must be able to hold
    /// opposite settings without one leaking into the other.
    #[test]
    fn two_tables_keep_independent_reach() {
        let recomputed =
            ActiveMemTable::new(storage_schema(), SuppressionConfig::for_recomputed(HOUR)).unwrap();
        let streamed =
            ActiveMemTable::new(storage_schema(), SuppressionConfig::for_stream()).unwrap();

        assert_eq!(versions_after_hourly_runs(&recomputed, 6), 1);
        assert_eq!(versions_after_hourly_runs(&streamed, 6), 6);
    }

    /// §1: the window runs from the last touch, so a run that changes a value
    /// mid-series is still recognised as a change and everything after it is
    /// suppressed against the new value.
    #[test]
    fn a_change_inside_a_recomputation_series_is_the_only_new_version() {
        let active =
            ActiveMemTable::new(storage_schema(), SuppressionConfig::for_recomputed(HOUR)).unwrap();
        let mut tables = Vec::new();

        for run in 0..12u64 {
            // The forecast changes once, at run 6.
            let value = if run < 6 { 1.5 } else { 2.5 };
            active
                .insert(&at(1, 10, run, value, run as Micros * HOUR))
                .unwrap();
            tables.push(active.freeze().unwrap());
        }
        tables.push(active.current());

        let total: usize = tables.iter().map(|table| table.num_rows()).sum();
        assert_eq!(total, 2, "one version per distinct value, not per run");
    }

    /// §1: suppression reaches across a freeze. Tying it to the active table
    /// made the guarantee depend on flush timing, which no client can observe.
    #[test]
    fn a_retransmission_after_a_freeze_is_suppressed() {
        let active = active();
        active.insert(&valued(1, 10, 0, 1.5)).unwrap();
        let frozen = active.freeze().unwrap();

        let outcome = active.insert(&valued(1, 10, 1, 1.5)).unwrap();

        assert_eq!(outcome.suppressed, 1, "the retry duplicates a frozen row");
        assert_eq!(outcome.inserted, 0);
        assert_eq!(frozen.num_rows(), 1);
        assert_eq!(active.current().num_rows(), 0);
    }

    /// §1: a correction is never discarded, freeze or not. Suppression may only
    /// ever drop an identical payload.
    #[test]
    fn a_correction_after_a_freeze_is_kept() {
        let active = active();
        active.insert(&valued(1, 10, 0, 1.5)).unwrap();
        active.freeze().unwrap();

        let outcome = active.insert(&valued(1, 10, 1, 2.5)).unwrap();

        assert_eq!(outcome.inserted, 1);
        assert_eq!(outcome.suppressed, 0);
    }

    /// §1: the reach does not depend on flush timing, so no number of freezes
    /// inside the window may reset it.
    #[test]
    fn repeated_freezes_inside_the_window_do_not_reset_suppression() {
        let active = active();
        active.insert(&valued(1, 10, 0, 1.5)).unwrap();
        for _ in 0..20 {
            active.freeze().unwrap();
        }

        assert_eq!(active.insert(&valued(1, 10, 1, 1.5)).unwrap().suppressed, 1);
    }

    /// §1: past the window the same retry becomes a new version. The boundary is
    /// the configured window — a documented limit, not an accident of flushing.
    #[test]
    fn a_retransmission_past_the_window_is_not_suppressed() {
        const WINDOW: Micros = 1_000;

        let active = active_with(WINDOW, 1_000_000);
        active.insert(&at(1, 10, 0, 1.5, 5_000)).unwrap();
        active.freeze().unwrap();

        // Ingest time passes, carried by the data itself rather than a clock.
        active.insert(&at(2, 99, 1, 0.0, 5_000 + WINDOW)).unwrap();
        let outcome = active
            .insert(&at(1, 10, 2, 1.5, 5_000 + 2 * WINDOW))
            .unwrap();

        assert_eq!(
            outcome.inserted, 1,
            "beyond the reach a repeat is a new version"
        );
    }

    /// §7: an exhausted entry cap shortens the reach. What it must never do is
    /// discard a correction.
    #[test]
    fn an_exhausted_entry_cap_never_discards_a_correction() {
        let active = active_with(Micros::MAX, 2);

        for row in 0..50i64 {
            active.insert(&valued(1, row, row as u64, 1.5)).unwrap();
            active.freeze().unwrap();
        }

        assert!(
            active.suppression().forced_rotations() > 0,
            "the cap must report having bound the reach"
        );
        assert_eq!(active.insert(&valued(1, 49, 100, 9.5)).unwrap().inserted, 1);
    }

    /// §1, §4: the window advances on the data's own `system_time`, so feeding
    /// the same stamped batches to a fresh instance — which is what WAL replay
    /// does — reaches exactly the same decisions.
    #[test]
    fn replaying_the_same_batches_reaches_the_same_decisions() {
        let batches = [
            at(1, 10, 0, 1.5, 1_000),
            at(1, 10, 1, 1.5, 2_000), // retransmission
            at(1, 10, 2, 2.5, 3_000), // correction
        ];
        let run = || -> Vec<InsertOutcome> {
            let active = active();
            batches
                .iter()
                .map(|batch| active.insert(batch).unwrap())
                .collect()
        };

        let first = run();
        assert_eq!(first[1].suppressed, 1, "the retry must be recognised");
        assert_eq!(first[2].inserted, 1, "the correction must survive");
        assert_eq!(run(), first, "replay reached different decisions");
    }

    /// The guard must not outlive the swap: once `freeze` returns, no writer
    /// may still be inside the table it handed over.
    #[test]
    fn a_frozen_table_has_no_writers_left_in_it() {
        const WRITERS: u64 = 8;

        let active = Arc::new(active());
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let active = Arc::clone(&active);
                thread::spawn(move || {
                    for step in 0..200 {
                        active.insert(&row(writer, step, step as u64)).unwrap();
                    }
                })
            })
            .collect();

        for _ in 0..50 {
            let frozen = active.freeze().unwrap();
            assert_eq!(
                frozen.writers_in_flight(),
                0,
                "freeze returned while a writer was still inside"
            );
        }

        for writer in writers {
            writer.join().expect("writer panicked");
        }
    }
}
