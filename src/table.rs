// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! One table's partitioned in-memory state (§2, §3, §11).
//!
//! §2 flushes per partition, never globally, so a table holds one
//! [`ActiveMemTable`] per partition rather than one overall. §3 requires the
//! split to happen at insert time, because a single batch may span interval
//! boundaries — a client sending an hour of data across midnight under a `day`
//! layout produces exactly that.
//!
//! # Suppression state is shared
//!
//! All partitions of a table compare against **one** [`SuppressionIndex`] (§1).
//! The entry cap is a memory bound (§7) and would otherwise multiply by the
//! partition count; the window advances on a single ingest-time axis, where a
//! rarely-written partition would keep its own rotation clock and never expire
//! anything. A logical row belongs to exactly one partition, so sharing costs no
//! correctness.
//!
//! # The common batch is not copied
//!
//! The hot-path rule bans re-batching outside the flush path, and §3 requires a
//! split. Both hold because the split is conditional: a batch whose rows all fall
//! in one partition — the normal case, since clients send time-contiguous data —
//! is passed through with its arrays shared by `Arc`. Only a batch that genuinely
//! straddles a boundary is filtered into per-partition batches, and then only
//! once per partition it touches.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;

use crate::active::ActiveMemTable;
use crate::clock::Micros;
use crate::memtable::{InsertOutcome, MemTable, MemTableError};
use crate::partition::{PartitionError, PartitionInterval};
use crate::schema::TIMESTAMP_COLUMN;
use crate::seq::Seq;
use crate::suppression::{SuppressionConfig, SuppressionIndex};

/// Why an insert into a partitioned table failed.
#[derive(Debug, thiserror::Error)]
pub enum TableError {
    /// A row's timestamp could not be placed in a partition (§11).
    #[error(transparent)]
    Partition(#[from] PartitionError),
    /// The batch could not be admitted by a partition's table.
    #[error(transparent)]
    MemTable(#[from] MemTableError),
}

/// One table's partitions, each with its own memtable (§2).
#[derive(Debug)]
pub struct PartitionedTable {
    schema: SchemaRef,
    interval: PartitionInterval,
    suppression: Arc<SuppressionIndex>,
    partitions: dashmap::DashMap<Micros, Arc<ActiveMemTable>>,
}

impl PartitionedTable {
    /// Creates an empty table laid out on `interval`.
    ///
    /// `suppression` configures the one index every partition shares.
    pub fn new(
        schema: SchemaRef,
        interval: PartitionInterval,
        suppression: SuppressionConfig,
    ) -> Self {
        Self {
            schema,
            interval,
            suppression: Arc::new(SuppressionIndex::new(suppression)),
            partitions: dashmap::DashMap::new(),
        }
    }

    /// The suppression state shared by every partition (§1).
    pub fn suppression(&self) -> &Arc<SuppressionIndex> {
        &self.suppression
    }

    /// The layout this table's partitions use (§11).
    pub fn interval(&self) -> PartitionInterval {
        self.interval
    }

    /// Routes a stamped batch to its partitions, splitting it if it spans a
    /// boundary (§3).
    ///
    /// The returned outcome sums what every partition admitted, so a caller sees
    /// one answer for the batch it sent.
    pub fn insert(&self, batch: &RecordBatch) -> Result<InsertOutcome, TableError> {
        if batch.num_rows() == 0 {
            return Ok(InsertOutcome::default());
        }

        let starts = self.partition_starts_of(batch)?;
        let mut total = InsertOutcome::default();

        // The overwhelmingly common case: every row belongs to one partition, so
        // the batch goes through untouched and its arrays stay shared.
        if let Some(single) = single_partition(&starts) {
            return Ok(self.partition(single)?.insert(batch)?);
        }

        for (start, mask) in masks_by_partition(&starts) {
            let slice = filter_record_batch(batch, &BooleanArray::from(mask))
                .map_err(MemTableError::from)?;
            let outcome = self.partition(start)?.insert(&slice)?;
            total.inserted += outcome.inserted;
            total.suppressed += outcome.suppressed;
        }

        Ok(total)
    }

    /// Retires a partition's active table and hands it to the caller (§2).
    ///
    /// `None` if the partition holds nothing. Freezing one partition leaves the
    /// others untouched, which is what makes a flush per partition possible.
    pub fn freeze(&self, start: Micros) -> Result<Option<Arc<MemTable>>, TableError> {
        match self.partitions.get(&start) {
            Some(partition) => Ok(Some(partition.freeze()?)),
            None => Ok(None),
        }
    }

    /// The partition starts this table currently holds, in time order.
    pub fn partition_starts(&self) -> Vec<Micros> {
        let mut starts: Vec<Micros> = self.partitions.iter().map(|entry| *entry.key()).collect();
        starts.sort_unstable();
        starts
    }

    /// A partition's active table, if it exists.
    pub fn partition_table(&self, start: Micros) -> Option<Arc<ActiveMemTable>> {
        self.partitions
            .get(&start)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Frozen tables of one partition, whose rows are not yet durable (§5).
    ///
    /// A reader must union these with the partition's active table, or a flush
    /// in flight makes rows disappear from answers.
    pub fn frozen(&self, start: Micros) -> Vec<Arc<MemTable>> {
        self.partitions
            .get(&start)
            .map_or_else(Vec::new, |entry| entry.value().frozen())
    }

    /// Drops a frozen table of `start`, on the caller's word that it is durable.
    ///
    /// Only the flusher calls this, and only once the Parquet file is in place.
    pub fn release(&self, start: Micros, table: &Arc<MemTable>) {
        if let Some(partition) = self.partitions.get(&start) {
            partition.value().release(table);
        }
    }

    /// Bytes the Arrow arrays of every partition hold, frozen ones included.
    ///
    /// Drives the hard cap that refuses ingestion (§7). Frozen tables count
    /// because they still occupy memory until their file is durable — a stalled
    /// flush is exactly when the cap must bite. The *flush* trigger uses
    /// [`Self::partition_bytes`] instead, which does not.
    pub fn memory_bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(|entry| entry.value().held_bytes())
            .sum()
    }

    /// When the oldest row a partition still holds arrived (§2).
    pub fn oldest_system_time(&self, start: Micros) -> Option<Micros> {
        self.partitions
            .get(&start)
            .and_then(|entry| entry.value().current().oldest_system_time())
    }

    /// Bytes one partition's *active* table holds (§2).
    ///
    /// Deliberately not frozen ones: this drives the flush trigger, and counting
    /// a flush already in flight would re-freeze the partition on every tick and
    /// produce a stream of empty tables.
    pub fn partition_bytes(&self, start: Micros) -> usize {
        self.partitions
            .get(&start)
            .map_or(0, |entry| entry.value().current().memory_bytes())
    }

    /// The lowest `seq` any partition still holds, frozen tables included (§4).
    ///
    /// A `seq` in a frozen table is not yet durable anywhere, so a checkpoint
    /// must stay below it.
    pub fn min_seq(&self) -> Option<Seq> {
        self.partitions
            .iter()
            .filter_map(|entry| entry.value().min_seq())
            .min()
    }

    /// Rows held across every partition, frozen tables included.
    pub fn num_rows(&self) -> usize {
        self.partitions
            .iter()
            .map(|entry| entry.value().held_rows())
            .sum()
    }

    /// The partition holding `start`, creating it on first use.
    ///
    /// Fallible rather than assuming construction cannot fail: the zero-panic
    /// policy covers this path, and a schema whose payload columns cannot be
    /// row-encoded would otherwise take the server down on a first insert.
    fn partition(&self, start: Micros) -> Result<Arc<ActiveMemTable>, TableError> {
        if let Some(held) = self.partitions.get(&start) {
            return Ok(Arc::clone(held.value()));
        }

        // Every partition borrows the table's one index (§1).
        let created = Arc::new(ActiveMemTable::with_suppression(
            Arc::clone(&self.schema),
            Arc::clone(&self.suppression),
        )?);

        // A racing writer may have created it meanwhile; whichever lands first
        // wins and the other's empty table is dropped.
        Ok(Arc::clone(
            self.partitions.entry(start).or_insert(created).value(),
        ))
    }

    /// Each row's partition start, in row order.
    fn partition_starts_of(&self, batch: &RecordBatch) -> Result<Vec<Micros>, TableError> {
        let timestamps = batch
            .column_by_name(TIMESTAMP_COLUMN)
            .and_then(|column| {
                column
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            })
            .ok_or_else(|| {
                TableError::MemTable(MemTableError::WrongColumnType(TIMESTAMP_COLUMN.to_owned()))
            })?;

        timestamps
            .values()
            .iter()
            .map(|timestamp| self.interval.start(*timestamp).map_err(TableError::from))
            .collect()
    }
}

/// The single partition every row belongs to, or `None` if they differ.
fn single_partition(starts: &[Micros]) -> Option<Micros> {
    let first = starts.first().copied()?;
    starts.iter().all(|start| *start == first).then_some(first)
}

/// A keep-mask per partition, in time order.
fn masks_by_partition(starts: &[Micros]) -> BTreeMap<Micros, Vec<bool>> {
    let mut masks: BTreeMap<Micros, Vec<bool>> = BTreeMap::new();
    for start in starts {
        masks
            .entry(*start)
            .or_insert_with(|| vec![false; starts.len()]);
    }
    for (row, start) in starts.iter().enumerate() {
        if let Some(mask) = masks.get_mut(start)
            && let Some(slot) = mask.get_mut(row)
        {
            *slot = true;
        }
    }
    masks
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

    use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::memtable::RowKey;
    use crate::schema::{METRIC_ID_COLUMN, append_system_fields, timestamp_type};

    /// 2026-08-02T14:37:12Z and 2026-09-01T00:00:00Z — different months.
    const AUGUST: Micros = 1_785_681_432_000_000;
    const SEPTEMBER: Micros = 1_788_220_800_000_000;
    const HOUR: Micros = 3_600_000_000;

    fn storage_schema() -> SchemaRef {
        append_system_fields(&Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    /// A stamped batch from `(metric_id, timestamp, seq, value)` rows.
    fn stamped(rows: &[(u64, Micros, u64, f64)], system_time: Micros) -> RecordBatch {
        RecordBatch::try_new(
            storage_schema(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, ts, _, _)| *ts).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|(_, _, _, v)| *v).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    system_time;
                    rows.len()
                ])),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, _, seq, _)| *seq).collect::<Vec<_>>(),
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

    #[test]
    fn a_new_table_holds_no_partitions() {
        let table = table();
        assert!(table.partition_starts().is_empty());
        assert_eq!(table.num_rows(), 0);
    }

    #[test]
    fn an_empty_batch_creates_no_partition() {
        let table = table();
        assert_eq!(table.insert(&stamped(&[], 100)).unwrap().inserted, 0);
        assert!(table.partition_starts().is_empty());
    }

    /// §3: a batch spanning a boundary is split, and every row lands in the
    /// partition its timestamp belongs to.
    #[test]
    fn a_batch_spanning_a_boundary_is_split() {
        let table = table();
        let outcome = table
            .insert(&stamped(
                &[
                    (1, AUGUST, 0, 1.0),
                    (1, SEPTEMBER, 1, 2.0),
                    (2, AUGUST + HOUR, 2, 3.0),
                ],
                100,
            ))
            .unwrap();

        assert_eq!(outcome.inserted, 3, "no row may be lost in the split");

        let starts = table.partition_starts();
        assert_eq!(starts.len(), 2);

        for start in starts {
            let held = table.partition_table(start).unwrap().current();
            for version in held.sorted() {
                assert_eq!(
                    PartitionInterval::Month
                        .start(version.key.timestamp)
                        .unwrap(),
                    start,
                    "a row landed in the wrong partition"
                );
            }
        }
    }

    /// The hot-path rule: a batch that touches one partition must pass through
    /// with its arrays shared, not rebuilt.
    #[test]
    fn a_single_partition_batch_is_not_copied() {
        let table = table();
        let batch = stamped(&[(1, AUGUST, 0, 1.0), (1, AUGUST + HOUR, 1, 2.0)], 100);
        table.insert(&batch).unwrap();

        let start = PartitionInterval::Month.start(AUGUST).unwrap();
        let held = table.partition_table(start).unwrap().current();
        let stored = held.batch(0).expect("one buffered batch");

        for column in 0..batch.num_columns() {
            assert!(
                Arc::ptr_eq(batch.column(column), stored.column(column)),
                "column {column} was copied instead of shared"
            );
        }
    }

    /// §1: one index per table, shared by every partition. A logical row lives in
    /// one partition, so this is observable as suppression still working after a
    /// batch for another partition has passed through.
    #[test]
    fn suppression_state_is_shared_across_partitions() {
        let table = table();
        table.insert(&stamped(&[(1, AUGUST, 0, 1.5)], 100)).unwrap();
        // Traffic into another partition must not disturb the first's state.
        table
            .insert(&stamped(&[(1, SEPTEMBER, 1, 9.9)], 100))
            .unwrap();

        let outcome = table.insert(&stamped(&[(1, AUGUST, 2, 1.5)], 100)).unwrap();

        assert_eq!(outcome.suppressed, 1, "the repeat must still be recognised");
        assert_eq!(outcome.inserted, 0);
    }

    /// §7: there is one index, so the entry cap bounds the table rather than
    /// every partition separately.
    #[test]
    fn every_partition_borrows_the_same_index() {
        let table = table();
        table
            .insert(&stamped(
                &[(1, AUGUST, 0, 1.0), (1, SEPTEMBER, 1, 2.0)],
                100,
            ))
            .unwrap();

        let starts = table.partition_starts();
        assert_eq!(starts.len(), 2);
        for start in starts {
            assert!(Arc::ptr_eq(
                table.partition_table(start).unwrap().suppression(),
                table.suppression()
            ));
        }
    }

    /// §2: a flush retires one partition. The others keep accepting writes.
    #[test]
    fn freezing_one_partition_leaves_the_others() {
        let table = table();
        table
            .insert(&stamped(
                &[(1, AUGUST, 0, 1.0), (1, SEPTEMBER, 1, 2.0)],
                100,
            ))
            .unwrap();

        let august = PartitionInterval::Month.start(AUGUST).unwrap();
        let september = PartitionInterval::Month.start(SEPTEMBER).unwrap();

        let frozen = table.freeze(august).unwrap().expect("august");
        assert_eq!(frozen.num_rows(), 1);
        assert_eq!(
            table
                .partition_table(september)
                .unwrap()
                .current()
                .num_rows(),
            1,
            "september must be untouched"
        );
        assert_eq!(
            table.partition_table(august).unwrap().current().num_rows(),
            0,
            "august must have a fresh table"
        );
    }

    #[test]
    fn freezing_an_absent_partition_yields_nothing() {
        assert!(table().freeze(AUGUST).unwrap().is_none());
    }

    /// §5, §7: a frozen partition's rows stay visible and stay counted until
    /// their file is durable. A reader consulting only the active tables would
    /// answer without them, and the memory cap would think they had gone.
    #[test]
    fn a_frozen_partition_is_still_held_until_released() {
        let table = table();
        table.insert(&stamped(&[(1, AUGUST, 0, 1.0)], 100)).unwrap();
        let august = PartitionInterval::Month.start(AUGUST).unwrap();
        let bytes = table.memory_bytes();

        let frozen = table.freeze(august).unwrap().expect("august");

        assert_eq!(table.num_rows(), 1, "the row must still be reachable");
        assert_eq!(table.memory_bytes(), bytes, "and still counted (§7)");
        assert_eq!(table.frozen(august).len(), 1);

        table.release(august, &frozen);
        assert_eq!(table.num_rows(), 0);
        assert_eq!(table.memory_bytes(), 0);
    }

    /// §2: the flush trigger measures the *active* table only. Counting a
    /// frozen one would re-freeze the partition on every tick while its write
    /// is in flight, producing a stream of empty tables.
    #[test]
    fn the_flush_trigger_ignores_a_flush_already_in_flight() {
        let table = table();
        table.insert(&stamped(&[(1, AUGUST, 0, 1.0)], 100)).unwrap();
        let august = PartitionInterval::Month.start(AUGUST).unwrap();

        table.freeze(august).unwrap().expect("august");

        assert_eq!(
            table.partition_bytes(august),
            0,
            "the partition has nothing new to flush"
        );
        assert!(table.memory_bytes() > 0, "but it is still holding memory");
    }

    /// §4: the checkpoint watermark reads this, and a `seq` in a frozen table
    /// is not yet durable anywhere.
    #[test]
    fn the_lowest_seq_includes_a_partition_being_flushed() {
        let table = table();
        table.insert(&stamped(&[(1, AUGUST, 5, 1.0)], 100)).unwrap();
        let august = PartitionInterval::Month.start(AUGUST).unwrap();
        let frozen = table.freeze(august).unwrap().unwrap();
        table.insert(&stamped(&[(1, AUGUST, 9, 2.0)], 100)).unwrap();

        assert_eq!(table.min_seq(), Some(5));
        table.release(august, &frozen);
        assert_eq!(table.min_seq(), Some(9));
    }

    /// §1: versions of one logical row stay in one partition and stay ordered.
    #[test]
    fn versions_of_a_row_share_its_partition() {
        let table = table();
        for seq in 0..3u64 {
            table
                .insert(&stamped(&[(1, AUGUST, seq, seq as f64)], 100))
                .unwrap();
        }

        let start = PartitionInterval::Month.start(AUGUST).unwrap();
        let held = table.partition_table(start).unwrap().current();
        let seqs: Vec<u64> = held
            .versions(RowKey {
                metric_id: 1,
                timestamp: AUGUST,
            })
            .iter()
            .map(|version| version.seq)
            .collect();

        assert_eq!(seqs, vec![0, 1, 2]);
        assert_eq!(table.partition_starts().len(), 1);
    }

    /// §11: a timestamp no calendar can place is reported, not routed somewhere.
    #[test]
    fn an_unplaceable_timestamp_is_rejected() {
        let table = table();
        let error = table
            .insert(&stamped(&[(1, Micros::MAX, 0, 1.0)], 100))
            .expect_err("must be rejected");

        assert!(matches!(error, TableError::Partition(_)));
        assert!(table.partition_starts().is_empty());
    }

    /// A finer layout puts the same batch in more partitions, from the same
    /// input — the interval is a parameter, not a constant (§11).
    #[test]
    fn the_interval_decides_how_many_partitions_a_batch_touches() {
        let rows = [
            (1u64, AUGUST, 0u64, 1.0),
            (1, AUGUST + HOUR, 1, 2.0),
            (1, AUGUST + 2 * HOUR, 2, 3.0),
        ];

        for (interval, expected) in [
            (PartitionInterval::Month, 1),
            (PartitionInterval::Day, 1),
            (PartitionInterval::Hour, 3),
        ] {
            let table =
                PartitionedTable::new(storage_schema(), interval, SuppressionConfig::for_stream());
            table.insert(&stamped(&rows, 100)).unwrap();

            assert_eq!(
                table.partition_starts().len(),
                expected,
                "{interval} should have produced {expected} partitions"
            );
        }
    }
}
