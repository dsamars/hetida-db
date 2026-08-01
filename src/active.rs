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
//! Retransmission suppression (§1) compares against the newest version the
//! *active* table holds. A freeze resets that history, so a retransmission
//! arriving after the flush of the version it duplicates is not recognised.
//! §1 does not say how far back suppression must reach; this is the open edge
//! of that decision.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::memtable::{InsertOutcome, MemTable, MemTableError};

/// Holds the table currently accepting writes and swaps it out on flush.
#[derive(Debug)]
pub struct ActiveMemTable {
    schema: SchemaRef,
    current: ArcSwap<MemTable>,
}

impl ActiveMemTable {
    /// Creates a holder with a fresh, empty table.
    pub fn new(schema: SchemaRef) -> Result<Self, MemTableError> {
        let table = MemTable::new(Arc::clone(&schema))?;
        Ok(Self {
            schema,
            current: ArcSwap::from_pointee(table),
        })
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
        let replacement = Arc::new(MemTable::new(Arc::clone(&self.schema))?);
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

    use crate::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};

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
        ActiveMemTable::new(storage_schema()).unwrap()
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
