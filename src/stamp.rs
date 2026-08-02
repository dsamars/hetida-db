// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! System column stamping.
//!
//! Turns a client batch into a storage batch by appending `system_time` and
//! `seq` (§1). Both come from one [`Stamper`], which is what makes the §1
//! invariant hold: `system_time` never decreases as `seq` grows.

use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{ArrayRef, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::error::ArrowError;

use crate::clock::{Clock, Micros};
use crate::schema::append_system_fields;
use crate::seq::{Seq, SequenceSource};

/// Why stamping failed.
#[derive(Debug, thiserror::Error)]
pub enum StampError {
    /// The stamped columns did not fit the batch — an internal inconsistency.
    #[error("could not assemble the stamped batch: {0}")]
    Arrow(#[from] ArrowError),
}

/// The system values issued to one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    /// First sequence number of the reserved block; the batch's rows take
    /// `seq_start .. seq_start + rows`.
    pub seq_start: Seq,
    /// Observation-axis timestamp shared by every row of the batch.
    pub system_time: Micros,
}

/// Issues the system columns for incoming batches.
#[derive(Debug)]
pub struct Stamper {
    clock: Arc<dyn Clock>,
    sequence: Arc<dyn SequenceSource>,
    last_system_time: Mutex<Micros>,
}

impl Stamper {
    /// Creates a stamper drawing time from `clock` and order from `sequence`.
    pub fn new(clock: Arc<dyn Clock>, sequence: Arc<dyn SequenceSource>) -> Self {
        Self {
            clock,
            sequence,
            last_system_time: Mutex::new(Micros::MIN),
        }
    }

    /// Reserves `rows` sequence numbers and the batch's `system_time`.
    ///
    /// Both are issued inside one critical section on purpose. Drawing them
    /// from two independent atomics would let a writer with the higher `seq`
    /// read an earlier clock value, breaking the §1 invariant that
    /// `system_time` never decreases as `seq` grows.
    ///
    /// The lock is held for three operations and taken once per batch, not per
    /// row, so it is not the coarse-grained locking the concurrency rule bans.
    pub fn issue(&self, rows: u64) -> Stamp {
        let mut last = self.lock();
        // Clamp: a host clock that jumped backwards must not rewind the
        // observation axis, or a later version would be invisible at a later
        // observation point.
        let system_time = self.clock.now_micros().max(*last);
        *last = system_time;
        let seq_start = self.sequence.reserve(rows);

        Stamp {
            seq_start,
            system_time,
        }
    }

    /// Appends the system columns to `batch`, yielding a storage batch (§1).
    ///
    /// The client's columns are moved by `Arc`, never copied — the exception
    /// the hot-path rule grants this operation.
    pub fn stamp(&self, batch: &RecordBatch) -> Result<RecordBatch, StampError> {
        let rows = batch.num_rows();
        let stamp = self.issue(rows as u64);

        let system_time: ArrayRef = Arc::new(TimestampMicrosecondArray::from_value(
            stamp.system_time,
            rows,
        ));
        let seq: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            stamp.seq_start..stamp.seq_start.saturating_add(rows as u64),
        ));

        let mut columns = batch.columns().to_vec();
        columns.push(system_time);
        columns.push(seq);

        Ok(RecordBatch::try_new(
            append_system_fields(batch.schema().as_ref()),
            columns,
        )?)
    }

    /// Locks the clamp state, recovering from a poisoned mutex.
    fn lock(&self) -> MutexGuard<'_, Micros> {
        self.last_system_time
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    use std::sync::Arc;
    use std::thread;

    use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::clock::TestClock;
    use crate::schema::{
        METRIC_ID_COLUMN, SEQ_COLUMN, SYSTEM_TIME_COLUMN, TIMESTAMP_COLUMN, TableSchema,
        timestamp_type,
    };
    use crate::seq::AtomicSequence;

    fn stamper_at(now: Micros) -> (Arc<TestClock>, Stamper) {
        let clock = Arc::new(TestClock::new(now));
        let stamper = Stamper::new(clock.clone(), Arc::new(AtomicSequence::starting_at(0)));
        (clock, stamper)
    }

    fn batch(rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]);
        let timestamps: Vec<i64> = (0..rows as i64).collect();
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(TimestampMicrosecondArray::from(timestamps)),
                Arc::new(UInt64Array::from(vec![7u64; rows])),
                Arc::new(Float64Array::from(vec![1.5f64; rows])),
            ],
        )
        .unwrap()
    }

    fn seq_values(batch: &RecordBatch) -> Vec<u64> {
        batch
            .column_by_name(SEQ_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    /// §1: the server appends the system columns; the client never sends them.
    #[test]
    fn stamping_appends_the_system_columns() {
        let (_clock, stamper) = stamper_at(1_000);
        let stamped = stamper.stamp(&batch(3)).unwrap();

        let expected = TableSchema::new(batch(3).schema().as_ref())
            .unwrap()
            .storage();
        assert_eq!(stamped.schema(), expected);
        assert_eq!(stamped.num_rows(), 3);
    }

    #[test]
    fn every_row_of_a_batch_carries_the_same_system_time() {
        let (_clock, stamper) = stamper_at(4_242);
        let stamped = stamper.stamp(&batch(3)).unwrap();

        let system_time = stamped
            .column_by_name(SYSTEM_TIME_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(system_time.values(), &[4_242, 4_242, 4_242]);
    }

    /// §1: two versions sharing a `seq` would be indistinguishable, so every
    /// row gets its own — a contiguous block reserved in one step.
    #[test]
    fn every_row_gets_its_own_sequence_number() {
        let (_clock, stamper) = stamper_at(1_000);
        let stamped = stamper.stamp(&batch(4)).unwrap();

        assert_eq!(seq_values(&stamped), vec![0, 1, 2, 3]);
    }

    #[test]
    fn sequence_blocks_of_successive_batches_do_not_overlap() {
        let (_clock, stamper) = stamper_at(1_000);
        let first = stamper.stamp(&batch(2)).unwrap();
        let second = stamper.stamp(&batch(2)).unwrap();

        assert_eq!(seq_values(&first), vec![0, 1]);
        assert_eq!(seq_values(&second), vec![2, 3]);
    }

    /// The hot-path rule permits appending system columns precisely because the
    /// client's buffers are moved by `Arc`, not copied. If this ever regresses
    /// into a copy, the rule's exception no longer holds.
    #[test]
    fn the_clients_columns_are_shared_not_copied() {
        let (_clock, stamper) = stamper_at(1_000);
        let original = batch(3);
        let stamped = stamper.stamp(&original).unwrap();

        for column in 0..original.num_columns() {
            assert!(
                Arc::ptr_eq(original.column(column), stamped.column(column)),
                "column {column} was copied instead of shared"
            );
        }
    }

    /// §1: a host clock that moves backwards must not lower `system_time`, or a
    /// later version would be invisible at a later observation point.
    #[test]
    fn a_backwards_clock_does_not_lower_system_time() {
        let (clock, stamper) = stamper_at(5_000);
        let before = stamper.issue(1).system_time;

        clock.set(1_000);
        let after = stamper.issue(1).system_time;

        assert_eq!(before, 5_000);
        assert_eq!(after, 5_000, "system_time must be clamped, not rewound");
    }

    #[test]
    fn a_forward_clock_still_advances_system_time() {
        let (clock, stamper) = stamper_at(5_000);
        stamper.issue(1);

        clock.set(9_000);
        assert_eq!(stamper.issue(1).system_time, 9_000);
    }

    /// §1: the invariant is that `system_time` never decreases as `seq` grows.
    /// Issuing the two from independent atomics would break exactly this.
    #[test]
    fn higher_sequence_numbers_never_carry_earlier_system_times() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 200;

        let clock = Arc::new(TestClock::new(0));
        let stamper = Arc::new(Stamper::new(
            clock.clone(),
            Arc::new(AtomicSequence::starting_at(0)),
        ));

        let handles: Vec<_> = (0..THREADS)
            .map(|thread_index| {
                let stamper = Arc::clone(&stamper);
                let clock = Arc::clone(&clock);
                thread::spawn(move || {
                    (0..PER_THREAD)
                        .map(|_| {
                            // Half the threads drag the clock backwards while
                            // the others push it forward.
                            if thread_index % 2 == 0 {
                                clock.advance(10);
                            } else {
                                clock.advance(-7);
                            }
                            let stamp = stamper.issue(1);
                            (stamp.seq_start, stamp.system_time)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let mut stamps: Vec<(Seq, Micros)> = handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("worker thread panicked"))
            .collect();
        stamps.sort_by_key(|(seq, _)| *seq);

        for pair in stamps.windows(2) {
            if let [(earlier_seq, earlier_time), (later_seq, later_time)] = pair {
                assert!(
                    later_time >= earlier_time,
                    "seq {later_seq} has system_time {later_time}, \
                     earlier than seq {earlier_seq} at {earlier_time}"
                );
            }
        }
    }
}
