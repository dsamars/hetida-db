// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Batch records in the data log (§4).
//!
//! A data record is a table name and one stamped `RecordBatch` in Arrow IPC
//! form. The batch goes through Arrow's own encoder for the same reason a schema
//! does: it carries nested types, nullability and a schema of its own, and
//! re-implementing that is how a column quietly loses a property.
//!
//! Records hold **stamped** batches — `system_time` and `seq` already appended —
//! so a replay reproduces exactly the versions the original run created. Stamping
//! on replay instead would issue fresh numbers and turn recovery into a second
//! set of versions.

use std::path::Path;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

use crate::catalog::CatalogueError;
use crate::seq::Seq;
use crate::storage::Storage;
use crate::wal::{RecordKind, WalError, replay};

/// One logged batch: which table it belongs to, and its rows.
#[derive(Debug, Clone)]
pub struct DataRecord {
    /// Name of the table the batch was written to.
    pub table: String,
    /// The stamped batch, exactly as it was admitted.
    pub batch: RecordBatch,
    /// Position in the total order (§1).
    pub seq: Seq,
}

/// What a data-log replay recovered.
#[derive(Debug, Default)]
pub struct RecoveredData {
    /// Batches in the order they were logged.
    pub records: Vec<DataRecord>,
    /// Records that passed their CRC but could not be decoded, and were skipped.
    pub undecodable: usize,
    /// Whether a segment's tail was discarded as torn (§4).
    pub truncated: bool,
}

/// Encodes a batch record for the log.
pub fn encode_batch(table: &str, batch: &RecordBatch) -> Result<Vec<u8>, CatalogueError> {
    let mut writer = StreamWriter::try_new(Vec::new(), batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    let ipc = writer.into_inner()?;

    let mut bytes = Vec::with_capacity(4 + table.len() + ipc.len());
    bytes.extend_from_slice(&(table.len() as u32).to_le_bytes());
    bytes.extend_from_slice(table.as_bytes());
    bytes.extend_from_slice(&ipc);
    Ok(bytes)
}

/// Reads a batch record back, or `None` if the bytes are not one.
///
/// Checked throughout: a damaged payload yields `None` rather than a panic, which
/// is what lets recovery skip it and still start (§4).
pub fn decode_batch(bytes: &[u8]) -> Option<(String, RecordBatch)> {
    let length = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
    let table = String::from_utf8(bytes.get(4..4usize.checked_add(length)?)?.to_vec()).ok()?;
    let ipc = bytes.get(4 + length..)?;

    let mut reader = StreamReader::try_new(std::io::Cursor::new(ipc), None).ok()?;
    let batch = reader.next()?.ok()?;
    Some((table, batch))
}

/// Reads every batch the data log under `dir` still holds.
///
/// Never fails on damage: a torn tail is the expected state after a crash, and a
/// record that cannot be decoded is counted and skipped rather than fatal (§4).
pub fn replay_data(storage: &dyn Storage, dir: &Path) -> Result<RecoveredData, WalError> {
    let replayed = replay(storage, dir)?;
    let mut recovered = RecoveredData {
        truncated: !replayed.truncated.is_empty(),
        ..RecoveredData::default()
    };

    for record in replayed.records {
        match record.kind {
            RecordKind::Data => match decode_batch(&record.payload) {
                Some((table, batch)) => recovered.records.push(DataRecord {
                    table,
                    batch,
                    seq: record.seq,
                }),
                None => recovered.undecodable += 1,
            },
            // A checkpoint marks reclamation progress and carries no rows (§4).
            RecordKind::Checkpoint => {}
            _ => recovered.undecodable += 1,
        }
    }

    Ok(recovered)
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
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Float64Array, TimestampMicrosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};
    use crate::storage::InMemoryStorage;
    use crate::wal::{Wal, WalConfig};

    fn stamped(rows: usize) -> RecordBatch {
        let schema = append_system_fields(&Schema::new(vec![
            Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
            Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(
                    (0..rows as i64).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(vec![7u64; rows])),
                Arc::new(Float64Array::from(
                    (0..rows).map(|row| row as f64).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(vec![100i64; rows])),
                Arc::new(UInt64Array::from((0..rows as u64).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    fn dir() -> PathBuf {
        PathBuf::from("wal")
    }

    fn logged(records: &[(&str, RecordBatch)]) -> InMemoryStorage {
        let storage = InMemoryStorage::new();
        let wal = Wal::open(
            Arc::new(storage.clone()),
            dir(),
            WalConfig {
                segment_bytes: 1024 * 1024,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();
        for (seq, (table, batch)) in records.iter().enumerate() {
            wal.append(
                RecordKind::Data,
                seq as Seq,
                encode_batch(table, batch).unwrap(),
            )
            .unwrap();
        }
        storage
    }

    /// §4: what was logged comes back — the same rows, the same schema, and the
    /// system columns already stamped.
    #[test]
    fn a_batch_round_trips_through_the_log() {
        let batch = stamped(3);
        let storage = logged(&[("readings", batch.clone())]);

        let recovered = replay_data(&storage, &dir()).unwrap();

        assert_eq!(recovered.records.len(), 1);
        let record = &recovered.records[0];
        assert_eq!(record.table, "readings");
        assert_eq!(record.batch.schema(), batch.schema());
        assert_eq!(record.batch.num_rows(), 3);
        assert_eq!(record.batch.columns(), batch.columns());
    }

    #[test]
    fn records_of_several_tables_keep_their_names() {
        let storage = logged(&[("readings", stamped(1)), ("events", stamped(2))]);

        let recovered = replay_data(&storage, &dir()).unwrap();
        let names: Vec<&str> = recovered
            .records
            .iter()
            .map(|record| record.table.as_str())
            .collect();

        assert_eq!(names, vec!["readings", "events"]);
    }

    /// §1: replay must reproduce the versions the original run created, so the
    /// stamped columns come back unchanged rather than being reissued.
    #[test]
    fn the_stamped_columns_survive_a_replay() {
        let storage = logged(&[("readings", stamped(2))]);
        let recovered = replay_data(&storage, &dir()).unwrap();

        let seqs = recovered.records[0]
            .batch
            .column_by_name(crate::schema::SEQ_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .expect("seq column")
            .values()
            .to_vec();

        assert_eq!(seqs, vec![0, 1], "replay must not reissue seq");
    }

    #[test]
    fn an_empty_log_recovers_nothing() {
        let storage = InMemoryStorage::new();
        let recovered = replay_data(&storage, &dir()).unwrap();

        assert!(recovered.records.is_empty());
        assert_eq!(recovered.undecodable, 0);
        assert!(!recovered.truncated);
    }

    /// §4: a checkpoint carries no rows and is not damage.
    #[test]
    fn a_checkpoint_is_not_counted_as_undecodable() {
        let storage = InMemoryStorage::new();
        let wal = Wal::open(
            Arc::new(storage.clone()),
            dir(),
            WalConfig {
                segment_bytes: 1024 * 1024,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();
        wal.checkpoint(42).unwrap();

        let recovered = replay_data(&storage, &dir()).unwrap();
        assert_eq!(recovered.undecodable, 0);
        assert!(recovered.records.is_empty());
    }

    #[test]
    fn a_payload_that_is_not_a_batch_is_counted_and_skipped() {
        let storage = InMemoryStorage::new();
        let wal = Wal::open(
            Arc::new(storage.clone()),
            dir(),
            WalConfig {
                segment_bytes: 1024 * 1024,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();
        wal.append(RecordKind::Data, 0, b"not arrow ipc".to_vec())
            .unwrap();

        let recovered = replay_data(&storage, &dir()).unwrap();
        assert_eq!(recovered.undecodable, 1);
        assert!(recovered.records.is_empty());
    }

    #[test]
    fn a_truncated_name_length_decodes_to_nothing() {
        assert_eq!(decode_batch(&[]), None);
        assert_eq!(decode_batch(&[255, 255, 255, 255]), None);
    }
}
