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
//! Retransmission suppression compares against a [`SuppressionIndex`] the table
//! only borrows. That state deliberately does not belong to the table: a freeze
//! (§2) would reset it, and the §1 guarantee would then depend on flush timing.
//!
//! # Consistency
//!
//! The batch list and the index are updated in that order, without a shared
//! lock. While an insert is in flight a concurrent reader can therefore see a
//! batch's rows in [`MemTable::num_rows`] before they appear in the index. This
//! is deliberate — a lock spanning both would serialise the ingestion path for
//! no benefit, because the flush path does not read a live table: freeze-and-
//! swap (§2) retires the table first, and only then is it read.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray, UInt64Array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};
use dashmap::DashMap;

use crate::clock::Micros;
use crate::registry::MetricId;
use crate::schema::{
    METRIC_ID_COLUMN, SEQ_COLUMN, SYSTEM_TIME_COLUMN, TIMESTAMP_COLUMN, is_reserved,
};
use crate::seq::Seq;
use crate::suppression::{PayloadDigest, SuppressionIndex, digest};

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
    /// The payload could not be encoded or the batch could not be filtered.
    #[error("could not process the batch: {0}")]
    Arrow(#[from] ArrowError),
}

/// What an insert did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InsertOutcome {
    /// Rows that became new versions.
    pub inserted: usize,
    /// Rows discarded as retransmissions (§1).
    pub suppressed: usize,
}

/// Buffers stamped batches, retaining every version.
#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    payload_columns: Vec<usize>,
    encoder: RowConverter,
    batches: RwLock<Vec<RecordBatch>>,
    index: DashMap<RowKey, Vec<VersionRef>>,
    suppression: Arc<SuppressionIndex>,
    writers: AtomicUsize,
}

impl MemTable {
    /// Creates an empty table accepting batches with this storage schema.
    ///
    /// `suppression` is shared rather than owned, so it survives the freeze that
    /// retires this table (§1).
    ///
    /// Fails only if the payload columns cannot be row-encoded, which the
    /// suppression comparison depends on.
    pub fn new(
        schema: SchemaRef,
        suppression: Arc<SuppressionIndex>,
    ) -> Result<Self, MemTableError> {
        let payload_columns: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !is_reserved(field.name()))
            .map(|(index, _)| index)
            .collect();

        let sort_fields: Vec<SortField> = payload_columns
            .iter()
            .filter_map(|index| schema.fields().get(*index))
            .map(|field| SortField::new(field.data_type().clone()))
            .collect();

        Ok(Self {
            schema,
            payload_columns,
            encoder: RowConverter::new(sort_fields)?,
            batches: RwLock::new(Vec::new()),
            index: DashMap::new(),
            suppression,
            writers: AtomicUsize::new(0),
        })
    }

    /// The suppression state this table compares against (§1).
    pub fn suppression(&self) -> &Arc<SuppressionIndex> {
        &self.suppression
    }

    /// Counter of writers currently inside this table.
    ///
    /// Owned here rather than by the holder so that a table handed to the
    /// flusher can be asked directly whether anyone is still writing to it.
    /// See [`crate::active::ActiveMemTable::freeze`].
    pub fn writers(&self) -> &AtomicUsize {
        &self.writers
    }

    /// How many writers are currently inside this table.
    pub fn writers_in_flight(&self) -> usize {
        self.writers.load(Ordering::SeqCst)
    }

    /// The storage schema, system columns included.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Buffers a stamped batch and indexes its rows, returning the row count.
    ///
    /// Refuses a batch whose schema is not this table's — an unstamped batch
    /// has no version identity, and the table must not invent one.
    pub fn insert(&self, batch: &RecordBatch) -> Result<InsertOutcome, MemTableError> {
        if batch.schema() != self.schema {
            return Err(MemTableError::SchemaMismatch {
                expected: Arc::clone(&self.schema),
                found: batch.schema(),
            });
        }

        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(InsertOutcome::default());
        }

        let keys = self.row_keys(batch)?;
        let seqs = self.u64_column(batch, SEQ_COLUMN)?.values().to_vec();
        let digests = self.payload_digests(batch)?;

        // Advance the window before anything is compared, so state older than
        // the configured reach can no longer suppress a row (§1).
        self.suppression.observe(self.batch_system_time(batch)?);

        let keep = self.decide(&keys, &seqs, &digests);
        let kept = keep.iter().filter(|keep| **keep).count();
        let outcome = InsertOutcome {
            inserted: kept,
            suppressed: rows - kept,
        };
        if kept == 0 {
            return Ok(outcome);
        }

        // Only a batch that actually lost rows is rebuilt; the common path
        // stores the client's arrays untouched, as the hot-path rule requires.
        let stored = if kept == rows {
            batch.clone()
        } else {
            filter_record_batch(batch, &BooleanArray::from(keep.clone()))?
        };

        let batch_index = {
            let mut batches = self.write_batches();
            batches.push(stored);
            batches.len() - 1
        };

        let mut stored_row = 0;
        for (row, key) in keys.iter().enumerate() {
            if !keep.get(row).copied().unwrap_or(false) {
                continue;
            }
            let seq = seqs.get(row).copied().unwrap_or_default();
            let version = VersionRef {
                key: *key,
                seq,
                batch: batch_index,
                row: stored_row,
            };
            stored_row += 1;

            {
                let mut entry = self.index.entry(*key).or_default();
                // Kept in `seq` order: arrival order is not version order, since
                // a batch reserved earlier may be inserted later.
                let at = entry.value().partition_point(|held| held.seq < seq);
                entry.value_mut().insert(at, version);
            }
            // Recorded only after the row is actually stored, so a failure
            // earlier cannot leave a digest standing for a version that does
            // not exist — which would suppress a later, genuine write of it.
            if let Some(digest) = digests.get(row).copied() {
                self.suppression.record(*key, seq, digest);
            }
        }

        Ok(outcome)
    }

    /// Decides which rows survive §1 retransmission suppression.
    ///
    /// A row is discarded when its payload digest matches the newest version
    /// within reach for the same logical row — which spans freezes, because the
    /// state does not belong to this table. Rows earlier in the same batch count
    /// towards that, so a batch repeated inside itself collapses too.
    fn decide(&self, keys: &[RowKey], seqs: &[Seq], digests: &[PayloadDigest]) -> Vec<bool> {
        // Tracks what each key's newest kept version holds while walking the
        // batch, so no state is mutated before the outcome is known.
        let mut pending: HashMap<RowKey, (Seq, PayloadDigest)> = HashMap::new();

        keys.iter()
            .enumerate()
            .map(|(row, key)| {
                let seq = seqs.get(row).copied().unwrap_or_default();
                let Some(digest) = digests.get(row).copied() else {
                    return false;
                };

                let newest = pending
                    .get(key)
                    .copied()
                    .or_else(|| self.suppression.newest(*key));

                if newest.is_some_and(|(_, held)| held == digest) {
                    return false;
                }
                if newest.is_none_or(|(held, _)| seq > held) {
                    pending.insert(*key, (seq, digest));
                }
                true
            })
            .collect()
    }

    /// Digests each row's payload columns for the §1 comparison.
    ///
    /// The system columns are excluded: they differ on every write by
    /// construction, so including them would defeat suppression entirely. The
    /// row encoder supplies the canonical, type-aware byte form — nulls
    /// included — and only its digest is retained, never the bytes.
    fn payload_digests(&self, batch: &RecordBatch) -> Result<Vec<PayloadDigest>, MemTableError> {
        let columns: Vec<ArrayRef> = self
            .payload_columns
            .iter()
            .filter_map(|index| batch.columns().get(*index).cloned())
            .collect();
        let rows = self.encoder.convert_columns(&columns)?;
        Ok(rows.iter().map(|row| digest(row.as_ref())).collect())
    }

    /// The `system_time` a batch was stamped with.
    ///
    /// Every row of a batch carries the same one (§1), so the first is the
    /// batch's. This is the axis the suppression window advances on.
    fn batch_system_time(&self, batch: &RecordBatch) -> Result<Micros, MemTableError> {
        self.timestamp_column(batch, SYSTEM_TIME_COLUMN)?
            .values()
            .first()
            .copied()
            .ok_or_else(|| MemTableError::WrongColumnType(SYSTEM_TIME_COLUMN.to_owned()))
    }

    /// Reads the identity columns into logical row keys.
    fn row_keys(&self, batch: &RecordBatch) -> Result<Vec<RowKey>, MemTableError> {
        let metric_ids = self.u64_column(batch, METRIC_ID_COLUMN)?;
        let timestamps = self.timestamp_column(batch, TIMESTAMP_COLUMN)?;

        Ok(metric_ids
            .values()
            .iter()
            .zip(timestamps.values().iter())
            .map(|(metric_id, timestamp)| RowKey {
                metric_id: *metric_id,
                timestamp: *timestamp,
            })
            .collect())
    }

    /// Every version of a logical row, in `seq` order.
    pub fn versions(&self, key: RowKey) -> Vec<VersionRef> {
        self.index
            .get(&key)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// The newest version of a logical row, by `seq`.
    pub fn latest(&self, key: RowKey) -> Option<VersionRef> {
        self.index
            .get(&key)
            .and_then(|entry| entry.value().last().copied())
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
    ///
    /// The payload is derived from `seq` so that every version differs. Tests
    /// that need an identical payload — the retransmission case — use
    /// [`stamped_with_values`] and say so explicitly.
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
                Arc::new(Float64Array::from(
                    rows.iter()
                        .map(|(_, _, seq)| *seq as f64)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(vec![100i64; rows.len()])),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, _, seq)| *seq).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn table() -> MemTable {
        MemTable::new(storage_schema(), Arc::new(SuppressionIndex::default())).unwrap()
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
            table
                .insert(&stamped(&[(1, 10, 0), (2, 20, 1)]))
                .unwrap()
                .inserted,
            2
        );

        assert_eq!(table.num_rows(), 2);
        assert_eq!(table.num_logical_rows(), 2);
    }

    #[test]
    fn an_empty_batch_changes_nothing() {
        let table = table();
        assert_eq!(
            table.insert(&stamped(&[])).unwrap(),
            InsertOutcome::default()
        );
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

    /// Builds a stamped batch with an explicit payload value per row.
    fn stamped_with_values(rows: &[(u64, i64, u64, Option<f64>)]) -> RecordBatch {
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
                Arc::new(TimestampMicrosecondArray::from(vec![100i64; rows.len()])),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, _, seq, _)| *seq).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// §1: at-least-once delivery is the norm over Flight. A client re-sending
    /// a batch after a timeout must not manufacture a phantom version that time
    /// travel would then report as a change.
    #[test]
    fn an_identical_retransmission_is_suppressed() {
        let table = table();
        let batch = stamped_with_values(&[(1, 10, 0, Some(1.5))]);
        table.insert(&batch).unwrap();

        let outcome = table
            .insert(&stamped_with_values(&[(1, 10, 1, Some(1.5))]))
            .unwrap();

        assert_eq!(outcome.inserted, 0);
        assert_eq!(outcome.suppressed, 1);
        assert_eq!(table.num_rows(), 1);
        assert_eq!(
            table
                .versions(RowKey {
                    metric_id: 1,
                    timestamp: 10
                })
                .len(),
            1
        );
    }

    /// A genuine correction differs by definition, so it must survive.
    #[test]
    fn a_changed_payload_is_a_new_version() {
        let table = table();
        table
            .insert(&stamped_with_values(&[(1, 10, 0, Some(1.5))]))
            .unwrap();

        let outcome = table
            .insert(&stamped_with_values(&[(1, 10, 1, Some(2.5))]))
            .unwrap();

        assert_eq!(outcome.inserted, 1);
        assert_eq!(outcome.suppressed, 0);
        assert_eq!(table.num_rows(), 2);
    }

    /// §1 compares against the *current latest* version, not the whole history.
    /// Reverting to an earlier value is a real correction, not a duplicate.
    #[test]
    fn returning_to_an_earlier_value_is_not_suppressed() {
        let table = table();
        table
            .insert(&stamped_with_values(&[(1, 10, 0, Some(1.5))]))
            .unwrap();
        table
            .insert(&stamped_with_values(&[(1, 10, 1, Some(2.5))]))
            .unwrap();

        let outcome = table
            .insert(&stamped_with_values(&[(1, 10, 2, Some(1.5))]))
            .unwrap();

        assert_eq!(outcome.inserted, 1, "a revert is a correction, not a retry");
        assert_eq!(table.num_rows(), 3);
    }

    #[test]
    fn an_identical_payload_on_another_logical_row_is_kept() {
        let table = table();
        table
            .insert(&stamped_with_values(&[(1, 10, 0, Some(1.5))]))
            .unwrap();

        let outcome = table
            .insert(&stamped_with_values(&[(2, 10, 1, Some(1.5))]))
            .unwrap();

        assert_eq!(outcome.inserted, 1);
        assert_eq!(table.num_logical_rows(), 2);
    }

    #[test]
    fn a_partially_retransmitted_batch_keeps_only_the_new_rows() {
        let table = table();
        table
            .insert(&stamped_with_values(&[
                (1, 10, 0, Some(1.5)),
                (2, 20, 1, Some(2.5)),
            ]))
            .unwrap();

        let outcome = table
            .insert(&stamped_with_values(&[
                (1, 10, 2, Some(1.5)),  // retransmission
                (2, 20, 3, Some(9.0)),  // correction
                (3, 30, 4, Some(0.25)), // new row
            ]))
            .unwrap();

        assert_eq!(outcome.inserted, 2);
        assert_eq!(outcome.suppressed, 1);
        assert_eq!(table.num_rows(), 4);
        assert_eq!(
            table
                .versions(RowKey {
                    metric_id: 1,
                    timestamp: 10
                })
                .len(),
            1
        );
        assert_eq!(
            table
                .versions(RowKey {
                    metric_id: 2,
                    timestamp: 20
                })
                .len(),
            2
        );
    }

    /// Suppression must be idempotent — that is the property Phase 4 verifies.
    #[test]
    fn repeating_a_batch_many_times_leaves_one_version() {
        let table = table();
        for seq in 0..10 {
            table
                .insert(&stamped_with_values(&[(1, 10, seq, Some(1.5))]))
                .unwrap();
        }

        assert_eq!(table.num_rows(), 1);
    }

    /// Nulls take part in the comparison: null and a value are different
    /// payloads, two nulls are the same one.
    #[test]
    fn nulls_compare_like_any_other_payload() {
        let table = table();
        table
            .insert(&stamped_with_values(&[(1, 10, 0, None)]))
            .unwrap();

        let repeated = table
            .insert(&stamped_with_values(&[(1, 10, 1, None)]))
            .unwrap();
        assert_eq!(repeated.suppressed, 1);

        let changed = table
            .insert(&stamped_with_values(&[(1, 10, 2, Some(1.5))]))
            .unwrap();
        assert_eq!(changed.inserted, 1);
    }

    /// The system columns differ on every write by construction, so they must
    /// be excluded from the comparison or nothing would ever be suppressed.
    #[test]
    fn differing_system_columns_do_not_defeat_suppression() {
        let table = table();
        table
            .insert(&stamped_with_values(&[(1, 10, 0, Some(1.5))]))
            .unwrap();

        // Same payload, different seq *and* a different system_time.
        let later = RecordBatch::try_new(
            storage_schema(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![10i64])),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(Float64Array::from(vec![1.5f64])),
                Arc::new(TimestampMicrosecondArray::from(vec![999_999i64])),
                Arc::new(UInt64Array::from(vec![42u64])),
            ],
        )
        .unwrap();

        assert_eq!(table.insert(&later).unwrap().suppressed, 1);
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
