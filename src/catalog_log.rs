// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The catalogue log: durable backing for everything that describes the
//! archive rather than being part of it (§4, §6, §10).
//!
//! The registry is the one piece of state that never reaches Parquet, so §4's
//! reclamation rule — drop a segment once its data is in Parquet — would destroy
//! it. It therefore lives in its **own append-only log that is never
//! reclaimed** ([`CATALOG_LOG_DIR`], separate from [`DATA_LOG_DIR`]), sharing
//! the record framing, group commit and recovery rule of the data log and
//! differing only in that nothing here ever calls
//! [`WalWriter::reclaim`](crate::wal::WalWriter::reclaim). The guarantee is
//! structural rather than conditional: no code path exists that could delete a
//! registration.
//!
//! # What the log holds
//!
//! Exactly one record per series. Idempotence is settled in memory before the
//! log is touched (see [`MetricRegistry::register`](crate::registry::MetricRegistry::register)), so a duplicate is never
//! written and there is nothing to compact. Its size is bounded by series
//! cardinality, not ingest volume.
//!
//! # Recovery
//!
//! [`replay_registrations`] reads the log and hands the result to
//! [`MetricRegistry::replay`](crate::registry::MetricRegistry::replay). It never fails on damage: a torn tail record is
//! by construction an id no client ever saw, because [`RegistrationLog::append`]
//! returns only once the record is durable and the id is returned only after
//! that. Discarding it and handing the id to another series is sound, and
//! numbering resumes past the highest surviving id.
//!
//! At startup this log is replayed **before** the data log, so every id a data
//! record refers to is already resolvable.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::registry::{MetricId, Registration, RegistrationLog, RegistryError, SeriesKey};
use crate::seq::SequenceSource;
use crate::storage::Storage;
use crate::wal::{RecordKind, Wal, WalError, replay};

/// Directory holding the never-reclaimed catalogue log (§4).
pub const CATALOG_LOG_DIR: &str = "catalog";

/// Directory holding the data log, the only one reclamation applies to (§4).
pub const DATA_LOG_DIR: &str = "wal";

/// What a registration replay recovered.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RecoveredRegistrations {
    /// Registrations in the order they were logged.
    pub registrations: Vec<Registration>,
    /// Segment whose tail was discarded, if any.
    pub truncated: Option<PathBuf>,
    /// Records that passed their CRC but could not be decoded, and were skipped.
    ///
    /// Non-zero means the log holds something this build does not understand.
    /// Skipping keeps the invariant that a damaged log never prevents a start
    /// (§4), but it is worth surfacing rather than swallowing.
    pub undecodable: usize,
}

/// [`RegistrationLog`] writing through a never-reclaimed [`Wal`].
///
/// Holds the log by `Arc` so the caller keeps it alive; dropping the last handle
/// joins the writer thread and so guarantees the final group reached the device.
pub struct WalRegistrationLog {
    wal: Arc<Wal>,
    sequence: Arc<dyn SequenceSource>,
}

impl WalRegistrationLog {
    /// Persists registrations through `wal`, numbering them from `sequence`.
    ///
    /// `sequence` must be the same source the data path draws from, so the order
    /// between a registration and a write stays defined (§4). The gaps this
    /// leaves in either stream are harmless — §1 requires monotonicity, not
    /// density.
    pub fn new(wal: Arc<Wal>, sequence: Arc<dyn SequenceSource>) -> Self {
        Self { wal, sequence }
    }
}

impl fmt::Debug for WalRegistrationLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalRegistrationLog")
            .field("wal", &self.wal)
            .finish_non_exhaustive()
    }
}

impl RegistrationLog for WalRegistrationLog {
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

/// Reads every registration from the log under `dir`.
///
/// Never fails on damage — see the module documentation.
pub fn replay_registrations(
    storage: &dyn Storage,
    dir: &Path,
) -> Result<RecoveredRegistrations, WalError> {
    let replayed = replay(storage, dir)?;
    let mut recovered = RecoveredRegistrations {
        truncated: replayed.truncated,
        ..RecoveredRegistrations::default()
    };

    for record in replayed.records {
        // The log holds nothing else, but a foreign kind is damage rather than a
        // reason to stop reading.
        if record.kind != RecordKind::Registration {
            recovered.undecodable += 1;
            continue;
        }
        match decode(&record.payload) {
            Some(registration) => recovered.registrations.push(registration),
            None => recovered.undecodable += 1,
        }
    }

    Ok(recovered)
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

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

    /// A registry backed by a real log over `storage`.
    fn logged_registry(storage: &InMemoryStorage) -> MetricRegistry {
        let wal = Arc::new(
            Wal::open(
                Arc::new(storage.clone()),
                PathBuf::from(CATALOG_LOG_DIR),
                config(),
            )
            .unwrap(),
        );
        MetricRegistry::new(Arc::new(WalRegistrationLog::new(
            wal,
            Arc::new(AtomicSequence::starting_at(0)),
        )))
    }

    fn recover(storage: &InMemoryStorage) -> RecoveredRegistrations {
        replay_registrations(storage, &PathBuf::from(CATALOG_LOG_DIR)).unwrap()
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
