// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The catalogue log: durable backing for everything that describes the
//! archive rather than being part of it (§4, §6, §10).
//!
//! Metric registrations (§10) and table schemas (§6) are the state that explains
//! the archive but is never part of it, so neither reaches Parquet. §4's
//! reclamation rule — drop a segment once its data is in Parquet — would
//! therefore destroy both. They live in this **append-only log that is never
//! reclaimed** ([`CATALOG_LOG_DIR`], separate from [`DATA_LOG_DIR`]), sharing the
//! record framing, group commit and recovery rule of the data log and differing
//! only in that nothing here ever calls
//! [`WalWriter::reclaim`](crate::wal::WalWriter::reclaim). The guarantee is
//! structural rather than conditional: no code path exists that could delete a
//! catalogue record.
//!
//! One log rather than two, because registrations and schemas share every
//! property that matters — never reclaimed, tiny, append-only, replayed before
//! the data log — and one log is one replay and one recovery rule.
//!
//! # What the log holds
//!
//! One record per series, and one per accepted schema change. Both are settled in
//! memory before the log is touched — registration by
//! [`MetricRegistry::register`](crate::registry::MetricRegistry::register)'s
//! idempotence, a schema by the catalogue finding the batch already conformant —
//! so a repeat is never written and there is nothing to compact. Size is bounded
//! by series cardinality plus the number of evolutions, not by ingest volume.
//!
//! Schema records accumulate rather than replace: the newest record for a table
//! is its current schema, and the earlier ones are history (§6).
//!
//! # Recovery
//!
//! [`replay_catalog`] reads the log and separates what it finds.
//! [`MetricRegistry::replay`](crate::registry::MetricRegistry::replay) takes the
//! registrations. It never fails on damage: a torn tail record is by construction
//! a change no caller ever saw, because an append returns only once the record is
//! durable and the result is published only after that. Discarding it is sound —
//! an id is handed to another series, an evolution is simply not yet applied.
//!
//! At startup this log is replayed **before** the data log, so every id and every
//! schema is known before the data referring to them is read.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::convert::try_schema_from_ipc_buffer;
use arrow::ipc::writer::StreamWriter;

use crate::catalog::{CatalogueError, SchemaLog};
use crate::registry::{MetricId, Registration, RegistrationLog, RegistryError, SeriesKey};
use crate::seq::SequenceSource;
use crate::storage::Storage;
use crate::wal::{RecordKind, Wal, WalError, replay};

/// Directory holding the never-reclaimed catalogue log (§4).
pub const CATALOG_LOG_DIR: &str = "catalog";

/// Directory holding the data log, the only one reclamation applies to (§4).
pub const DATA_LOG_DIR: &str = "wal";

/// One table's schema, as accepted or evolved (§6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRecord {
    /// Name of the table the schema belongs to.
    pub table: String,
    /// The schema as of this record.
    pub schema: SchemaRef,
}

/// What a catalogue replay recovered.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RecoveredCatalog {
    /// Registrations in the order they were logged.
    pub registrations: Vec<Registration>,
    /// Schema records in the order they were logged, so the last one for a
    /// table is its current schema (§6).
    pub schemas: Vec<SchemaRecord>,
    /// Segment whose tail was discarded, if any.
    pub truncated: Option<PathBuf>,
    /// Records that passed their CRC but could not be decoded, and were skipped.
    ///
    /// Non-zero means the log holds something this build does not understand.
    /// Skipping keeps the invariant that a damaged log never prevents a start
    /// (§4), but it is worth surfacing rather than swallowing.
    pub undecodable: usize,
}

/// Writes catalogue records through a never-reclaimed [`Wal`].
///
/// Implements [`RegistrationLog`] for the registry and offers
/// [`Self::append_schema`] for the catalogue, so both halves of the catalogue
/// reach one log.
///
/// Holds the log by `Arc` so the caller keeps it alive; dropping the last handle
/// joins the writer thread and so guarantees the final group reached the device.
pub struct WalCatalogLog {
    wal: Arc<Wal>,
    sequence: Arc<dyn SequenceSource>,
}

impl WalCatalogLog {
    /// Persists catalogue records through `wal`, numbering them from `sequence`.
    ///
    /// `sequence` must be the same source the data path draws from, so the order
    /// between a registration and a write stays defined (§4). The gaps this
    /// leaves in either stream are harmless — §1 requires monotonicity, not
    /// density.
    pub fn new(wal: Arc<Wal>, sequence: Arc<dyn SequenceSource>) -> Self {
        Self { wal, sequence }
    }

    /// Records `record` as the current schema of its table (§6).
    ///
    /// Returns only once it is durable, so the catalogue may publish the evolved
    /// schema afterwards and never before.
    pub fn append_schema(&self, record: &SchemaRecord) -> Result<(), CatalogueError> {
        let payload = encode_schema(record)?;
        self.wal
            .append(RecordKind::Schema, self.sequence.next(), payload)
            .map_err(|error| CatalogueError::Persistence(Box::new(error)))
    }
}

impl SchemaLog for WalCatalogLog {
    fn append(&self, record: &SchemaRecord) -> Result<(), CatalogueError> {
        self.append_schema(record)
    }
}

impl fmt::Debug for WalCatalogLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalCatalogLog")
            .field("wal", &self.wal)
            .finish_non_exhaustive()
    }
}

impl RegistrationLog for WalCatalogLog {
    fn append(&self, registration: &Registration) -> Result<(), RegistryError> {
        self.wal
            .append(
                RecordKind::Registration,
                self.sequence.next(),
                encode(registration),
            )
            .map_err(|error| RegistryError::Persistence(Box::new(error)))
    }
}

/// Reads the whole catalogue back from the log under `dir`.
///
/// Never fails on damage — see the module documentation.
pub fn replay_catalog(storage: &dyn Storage, dir: &Path) -> Result<RecoveredCatalog, WalError> {
    let replayed = replay(storage, dir)?;
    let mut recovered = RecoveredCatalog {
        truncated: replayed.truncated,
        ..RecoveredCatalog::default()
    };

    for record in replayed.records {
        match record.kind {
            RecordKind::Registration => match decode(&record.payload) {
                Some(registration) => recovered.registrations.push(registration),
                None => recovered.undecodable += 1,
            },
            RecordKind::Schema => match decode_schema(&record.payload) {
                Some(schema) => recovered.schemas.push(schema),
                None => recovered.undecodable += 1,
            },
            // The log holds nothing else, so a foreign kind is damage rather
            // than a reason to stop reading.
            _ => recovered.undecodable += 1,
        }
    }

    Ok(recovered)
}

/// Encodes a schema record: the table name, then the schema in Arrow IPC form.
///
/// The schema itself goes through Arrow's own encoder rather than a hand-rolled
/// one — a `Schema` carries nested types, metadata and nullability, and
/// re-implementing that is how a field quietly loses a property.
fn encode_schema(record: &SchemaRecord) -> Result<Vec<u8>, CatalogueError> {
    let mut writer = StreamWriter::try_new(Vec::new(), record.schema.as_ref())?;
    writer.finish()?;
    let ipc = writer.into_inner()?;

    let mut bytes = Vec::with_capacity(8 + record.table.len() + ipc.len());
    push_str(&mut bytes, &record.table);
    bytes.extend_from_slice(&ipc);
    Ok(bytes)
}

/// Reads a schema record back, or `None` if the bytes are not one.
fn decode_schema(bytes: &[u8]) -> Option<SchemaRecord> {
    let mut cursor = Cursor { bytes, at: 0 };
    let table = cursor.take_str()?;
    let ipc = bytes.get(cursor.at..)?;

    Some(SchemaRecord {
        table,
        schema: Arc::new(try_schema_from_ipc_buffer(ipc).ok()?),
    })
}

/// Encodes a registration into the log's payload form.
///
/// Length-prefixed throughout, matching the hand-rolled framing of the log
/// itself. The label order is the canonical one [`SeriesKey`] keeps, so the same
/// series always encodes to the same bytes.
fn encode(registration: &Registration) -> Vec<u8> {
    let labels = registration.key.labels();
    let mut bytes = Vec::with_capacity(16 + registration.key.name().len() + 24 * labels.len());

    bytes.extend_from_slice(&registration.id.to_le_bytes());
    push_str(&mut bytes, registration.key.name());
    bytes.extend_from_slice(&(labels.len() as u32).to_le_bytes());
    for (key, value) in labels {
        push_str(&mut bytes, key);
        push_str(&mut bytes, value);
    }
    bytes
}

/// Appends a length-prefixed string.
fn push_str(bytes: &mut Vec<u8>, text: &str) {
    bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
    bytes.extend_from_slice(text.as_bytes());
}

/// Reads a registration back, or `None` if the bytes are not one.
///
/// Every read is checked: a damaged payload yields `None` rather than a panic,
/// which is what lets recovery skip it and still start (§4).
fn decode(bytes: &[u8]) -> Option<Registration> {
    let mut cursor = Cursor { bytes, at: 0 };

    let id = MetricId::from_le_bytes(cursor.take(8)?.try_into().ok()?);
    let name = cursor.take_str()?;
    let count = u32::from_le_bytes(cursor.take(4)?.try_into().ok()?) as usize;

    let mut labels = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        labels.push((cursor.take_str()?, cursor.take_str()?));
    }

    // Trailing bytes mean the payload is not what this build writes.
    if cursor.at != bytes.len() {
        return None;
    }

    Some(Registration {
        id,
        key: SeriesKey::new(name, labels),
    })
}

/// Bounds-checked reader over a payload.
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    /// Takes `count` bytes, or `None` if fewer remain.
    fn take(&mut self, count: usize) -> Option<&[u8]> {
        let taken = self.bytes.get(self.at..self.at.checked_add(count)?)?;
        self.at += count;
        Some(taken)
    }

    /// Takes a length-prefixed UTF-8 string.
    fn take_str(&mut self) -> Option<String> {
        let length = u32::from_le_bytes(self.take(4)?.try_into().ok()?) as usize;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).ok()
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
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

    use crate::registry::{MetricRegistry, Registration, SeriesKey};
    use crate::seq::AtomicSequence;
    use crate::storage::{Fault, InMemoryStorage};
    use crate::wal::{RecordKind, Wal, WalConfig, WalWriter};

    fn config() -> WalConfig {
        WalConfig {
            segment_bytes: 1024,
            // Deterministic: group only what is already queued.
            max_commit_delay: Duration::ZERO,
        }
    }

    fn key(name: &str, labels: &[(&str, &str)]) -> SeriesKey {
        SeriesKey::new(
            name,
            labels
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        )
    }

    /// A catalogue log over `storage`.
    fn catalog_log(storage: &InMemoryStorage) -> Arc<WalCatalogLog> {
        let wal = Arc::new(
            Wal::open(
                Arc::new(storage.clone()),
                PathBuf::from(CATALOG_LOG_DIR),
                config(),
            )
            .unwrap(),
        );
        Arc::new(WalCatalogLog::new(
            wal,
            Arc::new(AtomicSequence::starting_at(0)),
        ))
    }

    /// A registry backed by a real catalogue log over `storage`.
    fn logged_registry(storage: &InMemoryStorage) -> MetricRegistry {
        MetricRegistry::new(catalog_log(storage))
    }

    fn recover(storage: &InMemoryStorage) -> RecoveredCatalog {
        replay_catalog(storage, &PathBuf::from(CATALOG_LOG_DIR)).unwrap()
    }

    #[test]
    fn a_registration_round_trips_through_the_log() {
        let storage = InMemoryStorage::new();
        let registry = logged_registry(&storage);
        let series = key("temp", &[("plant", "a")]);
        let id = registry.register(series.clone()).unwrap();

        let recovered = recover(&storage);

        assert_eq!(
            recovered.registrations,
            vec![Registration { id, key: series }]
        );
        assert_eq!(recovered.undecodable, 0);
        assert_eq!(recovered.truncated, None);
    }

    /// §10: the same labels in any order are the same series, so the encoding
    /// has to be canonical — otherwise a replayed key would not match the one a
    /// client sends.
    #[test]
    fn the_encoding_is_canonical_in_label_order() {
        let forwards = Registration {
            id: 7,
            key: key("temp", &[("plant", "a"), ("line", "1")]),
        };
        let backwards = Registration {
            id: 7,
            key: key("temp", &[("line", "1"), ("plant", "a")]),
        };

        assert_eq!(encode(&forwards), encode(&backwards));
        assert_eq!(decode(&encode(&forwards)), Some(forwards));
    }

    #[test]
    fn an_empty_encoding_decodes_to_nothing() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[0u8; 4]), None);
    }

    #[test]
    fn a_series_without_labels_round_trips() {
        let registration = Registration {
            id: 0,
            key: key("bare", &[]),
        };
        assert_eq!(decode(&encode(&registration)), Some(registration));
    }

    /// §4, §10: this is the decision itself. Reclaiming the data log — however
    /// aggressively — must not touch a registration, because registrations never
    /// reach Parquet and would otherwise be destroyed by the very rule that
    /// makes segments reclaimable.
    #[test]
    fn registrations_survive_reclaiming_the_whole_data_log() {
        let storage = InMemoryStorage::new();
        let registry = logged_registry(&storage);
        let temp = registry.register(key("temp", &[])).unwrap();
        let flow = registry.register(key("flow", &[("line", "1")])).unwrap();

        // A data log in its own directory, filled and then fully reclaimed.
        let mut data = WalWriter::open(
            Arc::new(storage.clone()),
            PathBuf::from(DATA_LOG_DIR),
            WalConfig {
                segment_bytes: 48,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();
        for seq in 0..20 {
            data.write_all(&[crate::wal::WalRecord {
                kind: RecordKind::Data,
                seq,
                payload: vec![b'x'; 24],
            }])
            .unwrap();
        }
        assert!(data.reclaim(u64::MAX).unwrap() > 0, "nothing was reclaimed");

        let recovered = recover(&storage);
        assert_eq!(recovered.registrations.len(), 2);

        let restored = logged_registry(&storage);
        restored.replay(recovered.registrations).unwrap();
        assert_eq!(restored.resolve(&key("temp", &[])), Some(temp));
        assert_eq!(restored.resolve(&key("flow", &[("line", "1")])), Some(flow));
    }

    /// The two logs must not share a directory, or reclamation would find the
    /// registrations after all.
    #[test]
    fn the_catalogue_log_is_not_the_data_log() {
        assert_ne!(CATALOG_LOG_DIR, DATA_LOG_DIR);
    }

    /// §10: an id is returned only once its append is durable, so a record a
    /// crash tore is by construction an id no client ever saw. Discarding it and
    /// handing the id to another series is sound.
    #[test]
    fn a_torn_tail_registration_is_discarded_and_its_id_freed() {
        let storage = InMemoryStorage::new();
        let registry = logged_registry(&storage);
        registry.register(key("first", &[])).unwrap();

        storage.inject(Fault::TornWrite { keep: 6 });
        registry.register(key("lost", &[])).unwrap();

        let recovered = recover(&storage);
        assert_eq!(recovered.registrations.len(), 1, "the torn record must go");
        assert!(recovered.truncated.is_some());

        // A fresh start must not fail, and the freed id goes to the next series.
        let restarted = logged_registry(&storage);
        restarted.replay(recovered.registrations).unwrap();
        assert_eq!(restarted.register(key("other", &[])).unwrap(), 1);
    }

    #[test]
    fn an_empty_log_recovers_nothing() {
        let storage = InMemoryStorage::new();
        let recovered = recover(&storage);

        assert!(recovered.registrations.is_empty());
        assert_eq!(recovered.undecodable, 0);
    }

    /// §10: nothing becomes visible before it is durable. A failing log must
    /// therefore assign no id at all.
    #[test]
    fn a_failing_log_assigns_no_id() {
        let storage = InMemoryStorage::new();
        let registry = logged_registry(&storage);
        storage.inject(Fault::Fail(std::io::ErrorKind::StorageFull));

        assert!(registry.register(key("doomed", &[])).is_err());
        assert_eq!(registry.resolve(&key("doomed", &[])), None);
        assert!(recover(&storage).registrations.is_empty());
    }

    /// §10: registration is idempotent, so a repeat must not reach the log —
    /// which is what keeps the log at exactly one record per series and free of
    /// anything to compact.
    #[test]
    fn a_repeated_registration_is_not_logged_twice() {
        let storage = InMemoryStorage::new();
        let registry = logged_registry(&storage);
        for _ in 0..5 {
            registry.register(key("temp", &[("plant", "a")])).unwrap();
        }

        assert_eq!(recover(&storage).registrations.len(), 1);
    }

    /// §4: registrations carry `seq` from the same source as data, so the order
    /// between a registration and a write is defined. Gaps in either stream are
    /// expected and harmless.
    #[test]
    fn registrations_carry_increasing_sequence_numbers() {
        let storage = InMemoryStorage::new();
        let registry = logged_registry(&storage);
        registry.register(key("a", &[])).unwrap();
        registry.register(key("b", &[])).unwrap();

        let records = crate::wal::replay(&storage, &PathBuf::from(CATALOG_LOG_DIR))
            .unwrap()
            .records;
        let seqs: Vec<u64> = records.iter().map(|record| record.seq).collect();

        assert!(records.iter().all(|r| r.kind == RecordKind::Registration));
        assert!(
            seqs.windows(2).all(|pair| pair[1] > pair[0]),
            "sequence numbers must increase: {seqs:?}"
        );
    }

    // ---- schema records (§6) ----

    fn table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("metric_id", DataType::UInt64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    /// §6: the schema is state and has to survive a restart, or the type-change
    /// rejection stops working — a client sending a different type would be
    /// admitted and the conflict would surface later as unreadable Parquet.
    #[test]
    fn a_schema_round_trips_through_the_log() {
        let storage = InMemoryStorage::new();
        let log = catalog_log(&storage);
        let recorded = SchemaRecord {
            table: "readings".to_owned(),
            schema: table_schema(),
        };
        log.append_schema(&recorded).unwrap();

        let recovered = recover(&storage);

        assert_eq!(recovered.schemas, vec![recorded]);
        assert_eq!(recovered.undecodable, 0);
    }

    /// The encoding has to be faithful for every type a payload column may hold,
    /// not just for the three in the sample table.
    #[test]
    fn a_schema_of_many_types_round_trips() {
        let wide = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Int8, false),
            Field::new("c", DataType::UInt64, true),
            Field::new("d", DataType::Float32, true),
            Field::new("e", DataType::Utf8, true),
            Field::new("f", DataType::Binary, true),
            Field::new("g", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("h", DataType::Decimal128(20, 4), true),
            Field::new(
                "i",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));

        let encoded = encode_schema(&SchemaRecord {
            table: "wide".to_owned(),
            schema: Arc::clone(&wide),
        })
        .unwrap();

        let decoded = decode_schema(&encoded).expect("decodes");
        assert_eq!(decoded.schema, wide, "a field lost its type or nullability");
        assert_eq!(decoded.table, "wide");
    }

    /// §4: registrations and schemas share one log, so a replay has to separate
    /// them again without losing either.
    #[test]
    fn registrations_and_schemas_share_one_log_and_separate_on_replay() {
        let storage = InMemoryStorage::new();
        let registry = logged_registry(&storage);
        let log = catalog_log(&storage);

        registry.register(key("temp", &[])).unwrap();
        log.append_schema(&SchemaRecord {
            table: "readings".to_owned(),
            schema: table_schema(),
        })
        .unwrap();
        registry.register(key("flow", &[])).unwrap();

        let recovered = recover(&storage);

        assert_eq!(recovered.registrations.len(), 2);
        assert_eq!(recovered.schemas.len(), 1);
        assert_eq!(recovered.undecodable, 0);
    }

    /// §6: evolution appends. The newest record for a table is the current
    /// schema, and the earlier ones are history rather than a conflict.
    #[test]
    fn an_evolved_schema_appends_rather_than_replacing() {
        let storage = InMemoryStorage::new();
        let log = catalog_log(&storage);

        let mut evolved = table_schema().fields().to_vec();
        evolved.push(Arc::new(Field::new("quality", DataType::Int32, true)));
        let evolved = Arc::new(Schema::new(evolved));

        for schema in [table_schema(), Arc::clone(&evolved)] {
            log.append_schema(&SchemaRecord {
                table: "readings".to_owned(),
                schema,
            })
            .unwrap();
        }

        let recovered = recover(&storage);
        assert_eq!(recovered.schemas.len(), 2, "both records must be kept");
        assert_eq!(
            recovered.schemas.last().map(|record| &record.schema),
            Some(&evolved),
            "the last record must be the current schema"
        );
    }

    /// §4, §6: the same guarantee the registrations have. Reclaiming the data log
    /// must not take a schema with it.
    #[test]
    fn schemas_survive_reclaiming_the_whole_data_log() {
        let storage = InMemoryStorage::new();
        catalog_log(&storage)
            .append_schema(&SchemaRecord {
                table: "readings".to_owned(),
                schema: table_schema(),
            })
            .unwrap();

        let mut data = WalWriter::open(
            Arc::new(storage.clone()),
            PathBuf::from(DATA_LOG_DIR),
            WalConfig {
                segment_bytes: 48,
                max_commit_delay: Duration::ZERO,
            },
        )
        .unwrap();
        for seq in 0..20 {
            data.write_all(&[crate::wal::WalRecord {
                kind: RecordKind::Data,
                seq,
                payload: vec![b'x'; 24],
            }])
            .unwrap();
        }
        assert!(data.reclaim(u64::MAX).unwrap() > 0);

        assert_eq!(recover(&storage).schemas.len(), 1);
    }

    #[test]
    fn a_schema_payload_that_is_not_one_is_counted_and_skipped() {
        let storage = InMemoryStorage::new();
        let wal = Wal::open(
            Arc::new(storage.clone()),
            PathBuf::from(CATALOG_LOG_DIR),
            config(),
        )
        .unwrap();
        wal.append(RecordKind::Schema, 0, b"not a schema".to_vec())
            .unwrap();

        let recovered = recover(&storage);
        assert_eq!(recovered.undecodable, 1);
        assert!(recovered.schemas.is_empty());
    }

    /// A record whose payload survived the CRC but cannot be decoded must not
    /// stop the start either — it is counted and skipped (§4).
    #[test]
    fn an_undecodable_record_is_counted_and_skipped() {
        let storage = InMemoryStorage::new();
        let wal = Wal::open(
            Arc::new(storage.clone()),
            PathBuf::from(CATALOG_LOG_DIR),
            config(),
        )
        .unwrap();
        wal.append(RecordKind::Registration, 0, b"not a registration".to_vec())
            .unwrap();
        wal.append(
            RecordKind::Registration,
            1,
            encode(&Registration {
                id: 3,
                key: key("intact", &[]),
            }),
        )
        .unwrap();

        let recovered = recover(&storage);
        assert_eq!(recovered.undecodable, 1);
        assert_eq!(recovered.registrations.len(), 1);
    }
}
