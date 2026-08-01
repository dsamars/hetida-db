// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Versioned in-memory table.
//!
//! Buffers stamped batches until a flush moves them to Parquet. A logical row
//! is identified by `(metric_id, timestamp)` and holds every version ever
//! written for it (§1) — the table never overwrites and never discards.
//!
//! Batches are stored whole and referenced by `(batch, row)`, so the client's
//! Arrow buffers are shared rather than copied. Building the index does walk
//! the rows once, reading the three key columns as primitive slices; that pass
//! is unavoidable, since versioning cannot know a row's identity without
//! looking at it.
//!
//! Version order is *not* maintained on the fly beyond each logical row's own
//! list. Global `(metric_id, timestamp, seq)` order is produced by
//! [`MemTable::sorted`] at flush time, which is exactly where §1 wants it.
//!
//! # Consistency
//!
//! The batch list and the index are updated in that order, without a shared
//! lock. While an insert is in flight a concurrent reader can therefore see a
//! batch's rows in [`MemTable::num_rows`] before they appear in the index. This
//! is deliberate — a lock spanning both would serialise the ingestion path for
//! no benefit, because the flush path does not read a live table: freeze-and-
//! swap (§2) retires the table first, and only then is it read.

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use arrow::array::{Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::SchemaRef;
use dashmap::DashMap;

use crate::clock::Micros;
use crate::registry::MetricId;
use crate::schema::{METRIC_ID_COLUMN, SEQ_COLUMN, TIMESTAMP_COLUMN};
use crate::seq::Seq;

/// Identity of a logical row (§1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowKey {
    /// The series (§10).
    pub metric_id: MetricId,
    /// Valid time — when the measurement was true.
    pub timestamp: Micros,
}

/// Where one version of a logical row lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionRef {
    /// The logical row this version belongs to.
    pub key: RowKey,
    /// Its position in the total order (§1).
    pub seq: Seq,
    /// Index of the buffered batch holding it.
    pub batch: usize,
    /// Its row within that batch.
    pub row: usize,
}

/// Why an insert was refused.
#[derive(Debug, thiserror::Error)]
pub enum MemTableError {
    /// The batch does not carry this table's storage schema.
    #[error("batch schema does not match the table's storage schema")]
    SchemaMismatch {
        /// Schema the table holds.
        expected: SchemaRef,
        /// Schema the batch carried.
        found: SchemaRef,
    },
    /// A key column had an unexpected Arrow type.
    #[error("column `{0}` has an unexpected type")]
    WrongColumnType(String),
}

/// Buffers stamped batches, retaining every version.
#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    batches: RwLock<Vec<RecordBatch>>,
    index: DashMap<RowKey, Vec<VersionRef>>,
}

impl MemTable {
    /// Creates an empty table accepting batches with this storage schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: RwLock::new(Vec::new()),
            index: DashMap::new(),
        }
    }

    /// The storage schema, system columns included.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Buffers a stamped batch and indexes its rows, returning the row count.
    ///
    /// Refuses a batch whose schema is not this table's — an unstamped batch
    /// has no version identity, and the table must not invent one.
    pub fn insert(&self, batch: &RecordBatch) -> Result<usize, MemTableError> {
        if batch.schema() != self.schema {
            return Err(MemTableError::SchemaMismatch {
                expected: Arc::clone(&self.schema),
                found: batch.schema(),
            });
        }

        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }

        let metric_ids = self.u64_column(batch, METRIC_ID_COLUMN)?;
        let seqs = self.u64_column(batch, SEQ_COLUMN)?;
        let timestamps = self.timestamp_column(batch, TIMESTAMP_COLUMN)?;

        // Publish the batch first so index entries can point at a stable slot.
        let batch_index = {
            let mut batches = self.write_batches();
            batches.push(batch.clone());
            batches.len() - 1
        };

        for (row, ((metric_id, timestamp), seq)) in metric_ids
            .values()
            .iter()
            .zip(timestamps.values().iter())
            .zip(seqs.values().iter())
            .enumerate()
        {
            let key = RowKey {
                metric_id: *metric_id,
                timestamp: *timestamp,
            };
            let version = VersionRef {
                key,
                seq: *seq,
                batch: batch_index,
                row,
            };

            let mut versions = self.index.entry(key).or_default();
            // Kept in `seq` order: arrival order is not version order, since a
            // batch reserved earlier may be inserted later.
            let at = versions.partition_point(|existing| existing.seq < version.seq);
            versions.insert(at, version);
        }

        Ok(rows)
    }

    /// Every version of a logical row, in `seq` order.
    pub fn versions(&self, key: RowKey) -> Vec<VersionRef> {
        self.index
            .get(&key)
            .map(|versions| versions.clone())
            .unwrap_or_default()
    }

    /// The newest version of a logical row, by `seq`.
    pub fn latest(&self, key: RowKey) -> Option<VersionRef> {
        self.index
            .get(&key)
            .and_then(|versions| versions.last().copied())
    }

    /// All versions in `(metric_id, timestamp, seq)` order — the flush order
    /// mandated by §1.
    pub fn sorted(&self) -> Vec<VersionRef> {
        let mut all: Vec<VersionRef> = self
            .index
            .iter()
            .flat_map(|entry| entry.value().clone())
            .collect();
        all.sort_unstable_by_key(|version| {
            (version.key.metric_id, version.key.timestamp, version.seq)
        });
        all
    }

    /// A buffered batch by index, as referenced by [`VersionRef::batch`].
    pub fn batch(&self, index: usize) -> Option<RecordBatch> {
        self.read_batches().get(index).cloned()
    }

    /// Total rows buffered, counting every version.
    pub fn num_rows(&self) -> usize {
        self.read_batches().iter().map(RecordBatch::num_rows).sum()
    }

    /// Distinct logical rows buffered, counting each once.
    pub fn num_logical_rows(&self) -> usize {
        self.index.len()
    }

    /// Bytes held by the buffered Arrow arrays.
    ///
    /// Drives the size half of the flush trigger (§2).
    pub fn memory_bytes(&self) -> usize {
        self.read_batches()
            .iter()
            .flat_map(RecordBatch::columns)
            .map(|column| column.get_array_memory_size())
            .sum()
    }

    fn u64_column<'a>(
        &self,
        batch: &'a RecordBatch,
        name: &str,
    ) -> Result<&'a UInt64Array, MemTableError> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| MemTableError::WrongColumnType(name.to_owned()))
    }

    fn timestamp_column<'a>(
        &self,
        batch: &'a RecordBatch,
        name: &str,
    ) -> Result<&'a TimestampMicrosecondArray, MemTableError> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .ok_or_else(|| MemTableError::WrongColumnType(name.to_owned()))
    }

    fn read_batches(&self) -> RwLockReadGuard<'_, Vec<RecordBatch>> {
        self.batches
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_batches(&self) -> RwLockWriteGuard<'_, Vec<RecordBatch>> {
        self.batches
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use crate::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};

    fn logical() -> Schema {
        Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ])
    }

    fn storage_schema() -> SchemaRef {
        append_system_fields(&logical())
    }

    /// Builds a stamped batch from `(metric_id, timestamp, seq)` triples.
    fn stamped(rows: &[(u64, i64, u64)]) -> RecordBatch {
        RecordBatch::try_new(
            storage_schema(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(vec![1.5f64; rows.len()])),
                Arc::new(TimestampMicrosecondArray::from(vec![100i64; rows.len()])),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, _, seq)| *seq).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn table() -> MemTable {
        MemTable::new(storage_schema())
    }

    #[test]
    fn a_new_table_is_empty() {
        let table = table();
        assert_eq!(table.num_rows(), 0);
        assert_eq!(table.num_logical_rows(), 0);
        assert!(table.sorted().is_empty());
    }

    #[test]
    fn inserting_indexes_every_row() {
        let table = table();
        assert_eq!(
            table.insert(&stamped(&[(1, 10, 0), (2, 20, 1)])).unwrap(),
            2
        );

        assert_eq!(table.num_rows(), 2);
        assert_eq!(table.num_logical_rows(), 2);
    }

    #[test]
    fn an_empty_batch_changes_nothing() {
        let table = table();
        assert_eq!(table.insert(&stamped(&[])).unwrap(), 0);
        assert_eq!(table.num_rows(), 0);
    }

    /// §1: a correction appends a version. Nothing is ever overwritten — that
    /// is the whole basis of time travel.
    #[test]
    fn a_second_version_of_a_logical_row_does_not_replace_the_first() {
        let table = table();
        table.insert(&stamped(&[(1, 10, 0)])).unwrap();
        table.insert(&stamped(&[(1, 10, 5)])).unwrap();

        let key = RowKey {
            metric_id: 1,
            timestamp: 10,
        };
        assert_eq!(table.num_rows(), 2, "both versions must be retained");
        assert_eq!(table.num_logical_rows(), 1, "but they are one logical row");
        assert_eq!(
            table
                .versions(key)
                .iter()
                .map(|v| v.seq)
                .collect::<Vec<_>>(),
            vec![0, 5]
        );
    }

    /// §1: `seq` is the order, not arrival. A batch reserved earlier may be
    /// inserted later, and version order must still follow `seq`.
    #[test]
    fn versions_are_ordered_by_seq_not_by_arrival() {
        let table = table();
        table.insert(&stamped(&[(1, 10, 9)])).unwrap();
        table.insert(&stamped(&[(1, 10, 2)])).unwrap();
        table.insert(&stamped(&[(1, 10, 5)])).unwrap();

        let key = RowKey {
            metric_id: 1,
            timestamp: 10,
        };
        assert_eq!(
            table
                .versions(key)
                .iter()
                .map(|v| v.seq)
                .collect::<Vec<_>>(),
            vec![2, 5, 9]
        );
        assert_eq!(table.latest(key).map(|v| v.seq), Some(9));
    }

    #[test]
    fn an_absent_logical_row_has_no_versions() {
        let table = table();
        let key = RowKey {
            metric_id: 7,
            timestamp: 7,
        };
        assert!(table.versions(key).is_empty());
        assert_eq!(table.latest(key), None);
    }

    #[test]
    fn rows_differing_only_in_metric_or_timestamp_are_distinct() {
        let table = table();
        table
            .insert(&stamped(&[(1, 10, 0), (1, 11, 1), (2, 10, 2)]))
            .unwrap();

        assert_eq!(table.num_logical_rows(), 3);
    }

    /// §1: flush writes `(metric_id, timestamp, seq)` order, so the table has
    /// to be able to produce it. Feeding it scrambled input proves the sort is
    /// real and not an accident of insertion order.
    #[test]
    fn sorted_yields_metric_then_timestamp_then_seq_order() {
        let table = table();
        table
            .insert(&stamped(&[
                (2, 10, 0),
                (1, 20, 1),
                (1, 10, 2),
                (1, 10, 9),
                (1, 10, 5),
                (2, 5, 6),
            ]))
            .unwrap();

        let order: Vec<(u64, i64, u64)> = table
            .sorted()
            .iter()
            .map(|v| (v.key.metric_id, v.key.timestamp, v.seq))
            .collect();

        assert_eq!(
            order,
            vec![
                (1, 10, 2),
                (1, 10, 5),
                (1, 10, 9),
                (1, 20, 1),
                (2, 5, 6),
                (2, 10, 0),
            ]
        );
    }

    /// A batch that never passed the stamper has no version identity, so the
    /// table must refuse it rather than invent one.
    #[test]
    fn an_unstamped_batch_is_rejected() {
        let table = table();
        let unstamped = RecordBatch::try_new(
            Arc::new(logical()),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![10i64])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(Float64Array::from(vec![1.5f64])),
            ],
        )
        .unwrap();

        let error = table.insert(&unstamped).expect_err("must be rejected");
        assert!(matches!(error, MemTableError::SchemaMismatch { .. }));
    }

    #[test]
    fn a_batch_of_another_table_is_rejected() {
        let table = table();
        let other = Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("different", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            append_system_fields(&other),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![10i64])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(TimestampMicrosecondArray::from(vec![100i64])),
                Arc::new(UInt64Array::from(vec![0u64])),
            ],
        )
        .unwrap();

        assert!(matches!(
            table.insert(&batch).expect_err("must be rejected"),
            MemTableError::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn memory_accounting_grows_with_inserts() {
        let table = table();
        assert_eq!(table.memory_bytes(), 0);

        table.insert(&stamped(&[(1, 10, 0)])).unwrap();
        let after_one = table.memory_bytes();
        assert!(after_one > 0);

        table.insert(&stamped(&[(1, 11, 1)])).unwrap();
        assert!(table.memory_bytes() > after_one);
    }

    /// The ingestion path is concurrent by design (§7). Losing rows under
    /// contention would silently drop acknowledged data.
    #[test]
    fn concurrent_inserts_lose_nothing() {
        const THREADS: u64 = 8;
        const PER_THREAD: u64 = 100;

        let table = Arc::new(table());
        let handles: Vec<_> = (0..THREADS)
            .map(|thread_index| {
                let table = Arc::clone(&table);
                thread::spawn(move || {
                    for step in 0..PER_THREAD {
                        let seq = thread_index * PER_THREAD + step;
                        table
                            .insert(&stamped(&[(thread_index, step as i64, seq)]))
                            .unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker thread panicked");
        }

        let total = (THREADS * PER_THREAD) as usize;
        assert_eq!(table.num_rows(), total);
        assert_eq!(table.num_logical_rows(), total);
        assert_eq!(table.sorted().len(), total);
    }

    /// Versions of one logical row written concurrently must all survive.
    #[test]
    fn concurrent_versions_of_one_row_all_survive() {
        const THREADS: u64 = 8;

        let table = Arc::new(table());
        let handles: Vec<_> = (0..THREADS)
            .map(|seq| {
                let table = Arc::clone(&table);
                thread::spawn(move || table.insert(&stamped(&[(1, 10, seq)])).unwrap())
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker thread panicked");
        }

        let key = RowKey {
            metric_id: 1,
            timestamp: 10,
        };
        let seqs: Vec<u64> = table.versions(key).iter().map(|v| v.seq).collect();
        assert_eq!(seqs, (0..THREADS).collect::<Vec<_>>());
    }
}
