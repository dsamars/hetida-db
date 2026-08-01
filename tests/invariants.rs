// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Property-based invariant suite.
//!
//! Examples cannot cover the invariants this archive rests on, so they are
//! stated as properties over generated input. This file currently holds the
//! properties of the Phase 0 foundation; the three §1 invariants — version
//! resolution at arbitrary observation points, sort order surviving flush and
//! compaction, retransmission suppression being idempotent — join it in
//! Phase 1, once the components they describe exist.
//!
//! Being an integration test, this also proves the injection points are
//! reachable and object-safe from outside the crate. A test double that only
//! works inside the crate that defines it is no use to the modules under test.

use std::path::Path;
use std::sync::Arc;

use std::path::PathBuf;
use std::time::Duration;

use hetida_db::clock::{Clock, Micros, TestClock};
use hetida_db::seq::{AtomicSequence, Seq, SequenceSource};
use hetida_db::storage::{Fault, InMemoryStorage, Storage};
use hetida_db::wal::{RecordKind, WalConfig, WalRecord, WalWriter, replay};
use proptest::prelude::*;

/// Generates a record with an arbitrary kind, sequence number and payload.
fn any_record() -> impl Strategy<Value = WalRecord> {
    (
        prop_oneof![
            Just(RecordKind::Data),
            Just(RecordKind::Registration),
            Just(RecordKind::Checkpoint),
        ],
        any::<Seq>(),
        prop::collection::vec(any::<u8>(), 0..48),
    )
        .prop_map(|(kind, seq, payload)| WalRecord { kind, seq, payload })
}

/// Writes `records` to a fresh log and returns the store holding it.
fn logged(records: &[WalRecord]) -> InMemoryStorage {
    let storage = InMemoryStorage::new();
    let mut writer = WalWriter::open(
        Arc::new(storage.clone()),
        PathBuf::from("wal"),
        WalConfig {
            segment_bytes: u64::MAX,
            max_commit_delay: Duration::ZERO,
        },
    )
    .expect("open");
    writer.write_all(records).expect("write");
    storage
}

/// Every injection point must survive being stored as a trait object, which is
/// how the production components will hold them.
#[test]
fn injection_points_are_usable_as_trait_objects() {
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(0));
    let seq: Arc<dyn SequenceSource> = Arc::new(AtomicSequence::starting_at(0));
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

    clock.now_micros();
    seq.next();
    assert!(storage.list(Path::new("wal")).is_ok());
}

proptest! {
    /// The clock reports exactly what it was set to, for any instant — including
    /// before the epoch, which `Micros` represents as a negative value.
    #[test]
    fn test_clock_round_trips_any_instant(instant in any::<Micros>()) {
        let clock = TestClock::new(0);
        clock.set(instant);
        prop_assert_eq!(clock.now_micros(), instant);
    }

    /// §1: `seq` is a total order, so successive values must strictly increase
    /// no matter where the sequence resumes after recovery.
    #[test]
    fn sequence_is_strictly_increasing(
        start in 0..u64::from(u32::MAX),
        count in 1usize..64,
    ) {
        let seq = AtomicSequence::starting_at(start);
        let issued: Vec<Seq> = (0..count).map(|_| seq.next()).collect();

        prop_assert_eq!(issued.first().copied(), Some(start));
        for pair in issued.windows(2) {
            if let [previous, next] = pair {
                prop_assert!(next > previous, "{next} did not exceed {previous}");
            }
        }
    }

    /// Appends concatenate: what is read back is what was written, in order.
    #[test]
    fn appends_concatenate_in_order(
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..32), 0..16),
    ) {
        let storage = InMemoryStorage::new();
        let path = Path::new("wal/000001");
        let mut file = storage.open_append(path)?;
        for chunk in &chunks {
            file.append(chunk)?;
        }
        file.sync()?;

        let expected: Vec<u8> = chunks.concat();
        prop_assert_eq!(storage.read(path)?, expected);
    }

    /// §4: whatever was written comes back, byte for byte, for any record.
    #[test]
    fn wal_records_round_trip(records in prop::collection::vec(any_record(), 0..24)) {
        let storage = logged(&records);
        let recovered = replay(&storage, Path::new("wal"))?;

        prop_assert_eq!(recovered.records, records);
        prop_assert_eq!(recovered.truncated, None);
    }

    /// §4: this is the recovery guarantee itself. Cut the log at *any* byte and
    /// replay returns a prefix of what was written — never a phantom record,
    /// never a reordering, never a failure to start. Damage lands at the tail,
    /// so everything before it stays trustworthy.
    #[test]
    fn a_log_cut_at_any_byte_replays_a_prefix(
        records in prop::collection::vec(any_record(), 1..16),
        cut in 0.0f64..1.0,
    ) {
        let storage = logged(&records);
        let segment = storage.list(Path::new("wal"))?.pop().expect("one segment");
        let bytes = storage.read(&segment)?;

        let keep = (bytes.len() as f64 * cut) as usize;
        storage.remove(&segment)?;
        let mut handle = storage.open_append(&segment)?;
        handle.append(bytes.get(..keep).unwrap_or_default())?;

        let recovered = replay(&storage, Path::new("wal"))?.records;

        prop_assert!(
            recovered.len() <= records.len(),
            "replay invented {} records out of {}",
            recovered.len(),
            records.len()
        );
        prop_assert_eq!(
            &recovered[..],
            &records[..recovered.len()],
            "replay returned something other than a prefix"
        );
    }

    /// §4: a torn write always leaves a prefix of the intended bytes, never a
    /// suffix, a gap or corruption. WAL replay relies on exactly this shape —
    /// it can truncate at the first bad record because damage is only ever at
    /// the tail.
    #[test]
    fn torn_writes_always_leave_a_prefix(
        intended in prop::collection::vec(any::<u8>(), 0..128),
        keep in 0usize..128,
    ) {
        let storage = InMemoryStorage::new();
        let path = Path::new("wal/000001");
        let mut file = storage.open_append(path)?;

        storage.inject(Fault::TornWrite { keep });
        file.append(&intended)?;

        let stored = storage.read(path)?;
        prop_assert!(intended.starts_with(&stored));
        prop_assert_eq!(stored.len(), keep.min(intended.len()));
    }
}
