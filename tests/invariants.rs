// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Property-based invariant suite.
//!
//! Examples cannot cover the invariants this archive rests on, so they are
//! stated as properties over generated input. All three §1 invariants are here
//! — suppression being idempotent, version resolution answering correctly at an
//! arbitrary observation point, and sort order surviving flush and compaction —
//! together with the §8 retention rules: that a dropped partition never took a
//! row younger than the cutoff, and that rejection on ingest and reaping on disk
//! are the same rule seen from two sides.
//!
//! Being an integration test, this also proves the injection points are
//! reachable and object-safe from outside the crate. A test double that only
//! works inside the crate that defines it is no use to the modules under test.

use std::path::Path;
use std::sync::Arc;

use std::path::PathBuf;
use std::time::Duration;

use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use hetida_db::active::ActiveMemTable;
use hetida_db::archive::{Archive, ArchiveConfig};
use hetida_db::clock::{Clock, Micros, TestClock};
use hetida_db::compact::{CompactConfig, Compactor, keys_of, partition_dirs};
use hetida_db::flush::Flusher;
use hetida_db::parquet::ParquetConfig;
use hetida_db::partition::PartitionInterval;
use hetida_db::query::QueryEngine;
use hetida_db::registry::MetricId;
use hetida_db::retention::{Reaper, is_expired};
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};
use hetida_db::seq::Seq as SeqNumber;
use hetida_db::seq::{AtomicSequence, Seq, SequenceSource};
use hetida_db::stamp::Stamper;
use hetida_db::storage::{Fault, InMemoryStorage, Storage};
use hetida_db::suppression::SuppressionConfig;
use hetida_db::wal::{RecordKind, WalConfig, WalRecord, WalWriter, replay};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

/// Generates a record with an arbitrary kind, sequence number and payload.
fn any_record() -> impl Strategy<Value = WalRecord> {
    (
        prop_oneof![
            Just(RecordKind::Data),
            Just(RecordKind::Registration),
            Just(RecordKind::Checkpoint),
            Just(RecordKind::Schema),
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

/// The client-facing schema: identity columns plus one payload column.
fn client_schema() -> Schema {
    Schema::new(vec![
        Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("value", DataType::Float64, true),
    ])
}

/// One unstamped client row.
fn client_row(metric_id: u64, timestamp: Micros, value: f64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(client_schema()),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
            Arc::new(UInt64Array::from(vec![metric_id])),
            Arc::new(Float64Array::from(vec![value])),
        ],
    )
    .expect("client row")
}

/// Sane range for time properties: 1900-01-01 to 2200-01-01, comfortably inside
/// what a calendar represents and wider than any archive will hold.
fn any_timestamp() -> impl Strategy<Value = Micros> {
    -2_208_988_800_000_000i64..7_258_118_400_000_000i64
}

fn any_interval() -> impl Strategy<Value = PartitionInterval> {
    prop_oneof![
        Just(PartitionInterval::Hour),
        Just(PartitionInterval::Day),
        Just(PartitionInterval::Month),
        Just(PartitionInterval::Year),
    ]
}

/// Writes one Parquet-shaped file into the partition each timestamp belongs to,
/// and returns the store beside each timestamp's directory.
///
/// The reaper decides from the listing alone, so the file contents are
/// irrelevant — what matters is that the directories are the ones the real
/// layout would have produced (§11).
fn partitioned(
    timestamps: &[Micros],
    interval: PartitionInterval,
) -> Result<(InMemoryStorage, Vec<PathBuf>), TestCaseError> {
    let storage = InMemoryStorage::new();
    let mut dirs = Vec::with_capacity(timestamps.len());

    for (index, timestamp) in timestamps.iter().enumerate() {
        let directory = Path::new("data/readings").join(
            interval
                .path(*timestamp)
                .map_err(|error| TestCaseError::fail(error.to_string()))?,
        );
        let mut file = storage.open_append(&directory.join(format!("part-{index}.parquet")))?;
        file.append(b"rows")?;
        file.sync()?;
        dirs.push(directory);
    }

    Ok((storage, dirs))
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
        prop_assert!(recovered.truncated.is_empty());
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

    /// §1: retransmission suppression is idempotent, and its reach does not
    /// depend on flush timing. Re-sending every batch — with a freeze landing
    /// between the original and its retry at arbitrary points — must leave
    /// exactly as many versions as sending each batch once.
    ///
    /// `system_time` stays fixed, so this isolates the freeze dimension: the
    /// window's own boundary is an example test, not a property.
    #[test]
    fn suppression_is_idempotent_at_any_freeze_point(
        rows in prop::collection::vec((0u64..4, 0i64..8, 0i64..3), 1..24),
        freezes in prop::collection::vec(any::<bool>(), 24),
    ) {
        let versions = |retransmit: bool| -> usize {
            let active = ActiveMemTable::new(
                append_system_fields(&client_schema()),
                SuppressionConfig::default(),
            )
            .expect("holder");
            let stamper = Stamper::new(
                Arc::new(TestClock::new(1_000)),
                Arc::new(AtomicSequence::starting_at(0)),
            );
            let mut tables = Vec::new();

            for (index, (metric_id, timestamp, value)) in rows.iter().enumerate() {
                let batch = client_row(*metric_id, *timestamp, *value as f64);
                let send = || {
                    let stamped = stamper.stamp(&batch).expect("stamp");
                    active.insert(&stamped).expect("insert");
                };

                send();
                let freeze_here = freezes.get(index).copied().unwrap_or(false);
                if retransmit {
                    // Freezing before the retry puts the two in different
                    // tables, which is precisely the case that used to slip
                    // through.
                    if freeze_here {
                        tables.push(active.freeze().expect("freeze"));
                    }
                    send();
                }
                if freeze_here {
                    tables.push(active.freeze().expect("freeze"));
                }
            }

            tables.push(active.current());
            tables.iter().map(|table| table.num_rows()).sum()
        };

        prop_assert_eq!(
            versions(true),
            versions(false),
            "retransmitting every batch manufactured versions"
        );
    }

    /// §8: retention never deletes a row younger than the cutoff. A partition
    /// goes only when its *whole* interval is past it, so every timestamp the
    /// reaper takes with a directory lies strictly behind the cutoff — checked
    /// against the real reaper over a real listing, not against the predicate
    /// the reaper itself uses.
    #[test]
    fn a_reaped_partition_holds_nothing_younger_than_the_cutoff(
        timestamps in prop::collection::vec(any_timestamp(), 1..12),
        interval in any_interval(),
        cutoff in any_timestamp(),
    ) {
        let (storage, dirs) = partitioned(&timestamps, interval)?;
        let pass = Reaper::new(Arc::new(storage), interval)
            .reap("readings", Some(0), cutoff)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;

        for (timestamp, directory) in timestamps.iter().zip(&dirs) {
            if pass.dropped.contains(directory) {
                prop_assert!(
                    *timestamp < cutoff,
                    "a row at {} was dropped at cutoff {cutoff}",
                    timestamp
                );
            }
        }
    }

    /// §8: the rejection rule and the reaping rule are one rule. Anything
    /// `do_put` accepts lands in a partition the reaper is not entitled to drop
    /// at that same instant — otherwise the archive would acknowledge data it is
    /// about to delete. And anything it refuses is genuinely reapable, so the
    /// refusal never turns away data the archive would have kept.
    #[test]
    fn accepting_and_reaping_are_exactly_complementary(
        timestamps in prop::collection::vec(any_timestamp(), 1..12),
        interval in any_interval(),
        cutoff in any_timestamp(),
    ) {
        let (storage, dirs) = partitioned(&timestamps, interval)?;
        let pass = Reaper::new(Arc::new(storage), interval)
            .reap("readings", Some(0), cutoff)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;

        for (timestamp, directory) in timestamps.iter().zip(&dirs) {
            let refused = is_expired(interval, *timestamp, cutoff)?;
            prop_assert_eq!(
                refused,
                pass.dropped.contains(directory),
                "ingest and the reaper disagree about {}",
                timestamp
            );
        }
    }

    /// §8: a dropped partition never comes back, so the reaper cannot flap.
    /// Expiry is monotone in the cutoff, which is monotone in the clock.
    #[test]
    fn expiry_is_monotone_in_the_cutoff(
        timestamp in any_timestamp(),
        interval in any_interval(),
        earlier in any_timestamp(),
        delta in 0i64..(400 * 86_400_000_000),
    ) {
        if is_expired(interval, timestamp, earlier)? {
            prop_assert!(
                is_expired(interval, timestamp, earlier.saturating_add(delta))?,
                "a partition expired at {earlier} was live {delta} µs later"
            );
        }
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

// ---- the two §1 invariants that needed the query and archive paths ----

/// 2026-08-02T14:37:12Z — inside a month, so a partition holds a real interval.
const AUGUST: Micros = 1_785_681_432_000_000;
/// Ingest-time distance between two successive writes in a generated run.
const STEP: Micros = 1_000;

fn archive(storage: &InMemoryStorage, clock: Arc<TestClock>) -> Archive {
    Archive::open(
        Arc::new(storage.clone()),
        &ArchiveConfig {
            wal: WalConfig {
                segment_bytes: 64 * 1024,
                max_commit_delay: Duration::ZERO,
            },
            ..ArchiveConfig::default()
        },
        clock,
    )
    .expect("archive")
}

fn flusher(storage: &InMemoryStorage) -> Flusher {
    Flusher::new(
        Arc::new(storage.clone()),
        "readings",
        PartitionInterval::Month,
        ParquetConfig::default(),
    )
}

/// Every `(metric_id, timestamp, seq)` on disk for `readings`, in file order.
fn keys_on_disk(storage: &InMemoryStorage) -> Vec<Vec<(MetricId, Micros, SeqNumber)>> {
    let mut per_file = Vec::new();
    for directory in partition_dirs(storage, "readings", PartitionInterval::Month).expect("dirs") {
        for file in storage.list(&directory).expect("files") {
            per_file.push(keys_of(storage, &file).expect("keys"));
        }
    }
    per_file
}

proptest! {
    /// §1: the invariant time travel rests on. For an arbitrary run of writes
    /// and corrections, and an arbitrary observation point, the archive must
    /// answer with the version that was current at that point — no matter
    /// whether the version in question is still in memory, in a memtable a
    /// flush has in flight, or already in Parquet.
    ///
    /// The expected answer is computed here, independently: the last write to a
    /// logical row at or before the point. Suppression cannot change it, since a
    /// suppressed write carries the value it was compared against.
    #[test]
    fn resolution_answers_the_version_current_at_any_observation_point(
        writes in prop::collection::vec((0u64..3, 0i64..3, 0i64..4), 1..10),
        flushes in prop::collection::vec(any::<bool>(), 10),
        offset in -1i64..12_000,
    ) {
        let storage = InMemoryStorage::new();
        let clock = Arc::new(TestClock::new(AUGUST));
        let archive = Arc::new(archive(&storage, Arc::clone(&clock)));
        let flusher = flusher(&storage);

        // The model: what each logical row's value was, at each system time.
        let mut expected: std::collections::BTreeMap<(u64, Micros), f64> =
            std::collections::BTreeMap::new();
        let observe = AUGUST + offset;

        for (index, (metric, slot, value)) in writes.iter().enumerate() {
            let stamped_at = AUGUST + index as Micros * STEP;
            clock.set(stamped_at);

            let timestamp = AUGUST + slot * 86_400_000_000;
            archive
                .ingest("readings", &client_row(*metric, timestamp, *value as f64))
                .expect("ingest");

            // Only writes at or before the observation point are visible from it.
            if stamped_at <= observe {
                expected.insert((*metric, timestamp), *value as f64);
            }

            // A flush here moves everything written so far to Parquet, so the
            // point may fall on either side of the memory/disk boundary.
            if flushes.get(index).copied().unwrap_or(false)
                && let Some(state) = archive.table("readings")
            {
                for start in state.partition_starts() {
                    flusher.flush(&state, start).expect("flush");
                }
            }
        }

        let engine = QueryEngine::new(Arc::clone(&archive), Arc::new(storage.clone()));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let batches = runtime
            .block_on(engine.collect("SELECT * FROM readings", Some(observe)))
            .expect("query");

        let mut answered: std::collections::BTreeMap<(u64, Micros), f64> =
            std::collections::BTreeMap::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("metric_id")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .expect("metric_id");
            let stamps = batch
                .column_by_name("timestamp")
                .and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>())
                .expect("timestamp");
            let values = batch
                .column_by_name("value")
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                .expect("value");
            for row in 0..batch.num_rows() {
                answered.insert((ids.value(row), stamps.value(row)), values.value(row));
            }
        }

        prop_assert_eq!(
            answered,
            expected,
            "observation point {} answered with the wrong versions",
            observe
        );
    }

    /// §1: every flushed and compacted file is ordered by
    /// `(metric_id, timestamp, seq)`, and no version is lost on the way. Version
    /// resolution is a streaming merge over exactly that order and silently
    /// degrades to a full sort if it is broken — silently, which is why this is
    /// a property and not an example.
    #[test]
    fn sort_order_and_every_version_survive_flush_and_compaction(
        rows in prop::collection::vec((0u64..4, 0i64..3, 0i64..3), 1..24),
        flushes in prop::collection::vec(any::<bool>(), 24),
    ) {
        let storage = InMemoryStorage::new();
        let clock = Arc::new(TestClock::new(AUGUST));
        let archive = archive(&storage, Arc::clone(&clock));
        let flusher = flusher(&storage);

        let flush_all = |archive: &Archive| {
            if let Some(state) = archive.table("readings") {
                for start in state.partition_starts() {
                    flusher.flush(&state, start).expect("flush");
                }
            }
        };

        for (index, (metric, slot, value)) in rows.iter().enumerate() {
            clock.set(AUGUST + index as Micros * STEP);
            archive
                .ingest(
                    "readings",
                    &client_row(*metric, AUGUST + slot * 86_400_000_000, *value as f64),
                )
                .expect("ingest");
            if flushes.get(index).copied().unwrap_or(false) {
                flush_all(&archive);
            }
        }
        flush_all(&archive);

        // Sorted within every file, before anything is merged.
        let before = keys_on_disk(&storage);
        for file in &before {
            let mut sorted = file.clone();
            sorted.sort_unstable();
            prop_assert_eq!(file, &sorted, "a flushed file is out of order");
        }

        // Compaction re-sorts and merges, and must change neither.
        let compactor = Compactor::new(
            Arc::new(storage.clone()),
            CompactConfig {
                max_files: 2,
                max_bytes: usize::MAX,
                target_bytes: usize::MAX,
            },
            ParquetConfig::default(),
        );
        for directory in
            partition_dirs(&storage, "readings", PartitionInterval::Month).expect("dirs")
        {
            compactor.compact_if_due(&directory).expect("compact");
        }

        let after = keys_on_disk(&storage);
        for file in &after {
            let mut sorted = file.clone();
            sorted.sort_unstable();
            prop_assert_eq!(file, &sorted, "a compacted file is out of order");
        }

        let flatten = |files: &[Vec<(MetricId, Micros, SeqNumber)>]| {
            let mut all: Vec<_> = files.iter().flatten().copied().collect();
            all.sort_unstable();
            all
        };
        prop_assert_eq!(
            flatten(&after),
            flatten(&before),
            "compaction lost or invented a version"
        );
    }
}
