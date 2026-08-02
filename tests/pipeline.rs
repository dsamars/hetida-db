// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Cross-component tests over the ingestion path as far as it is built.
//!
//! Every component so far is unit-tested in isolation, which says nothing about
//! the assumptions that live *between* them. This file wires the real pieces
//! together — instance metadata, the registry and its log, the stamper, the
//! partition layout, and the versioned table with freeze-and-swap — and asserts
//! on what crosses the seams.
//!
//! What is deliberately absent, because the production code does not exist yet:
//! the data log carries no `RecordBatch` (its encoding arrives with `do_put`),
//! nothing routes a batch to a partition on its own, and no Parquet is written.
//! This file grows as those land rather than simulating them here — a test that
//! invents behaviour proves only that the test can invent it.

use std::sync::Arc;

use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use hetida_db::active::ActiveMemTable;
use hetida_db::catalog::{TableCatalogue, TableConfig};
use hetida_db::catalog_log::{CATALOG_LOG_DIR, WalCatalogLog, replay_catalog};
use hetida_db::clock::{Micros, TestClock};
use hetida_db::instance::InstanceMetadata;
use hetida_db::memtable::RowKey;
use hetida_db::partition::PartitionInterval;
use hetida_db::registry::{MetricId, MetricRegistry, SeriesKey};
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};
use hetida_db::seq::AtomicSequence;
use hetida_db::stamp::Stamper;
use hetida_db::storage::InMemoryStorage;
use hetida_db::suppression::SuppressionConfig;
use hetida_db::wal::{Wal, WalConfig};

/// One hour of ingest time.
const HOUR: Micros = 3_600_000_000;
/// 2026-08-02T14:00:00Z.
const AUGUST: Micros = 1_785_681_432_000_000;
/// 2026-09-01T00:00:00Z — the next month's partition.
const SEPTEMBER: Micros = 1_788_220_800_000_000;

fn wal_config() -> WalConfig {
    WalConfig {
        segment_bytes: 64 * 1024,
        // Deterministic: group only what is already queued.
        max_commit_delay: std::time::Duration::ZERO,
    }
}

/// The client-facing schema, before the server appends its system columns.
fn client_schema() -> Schema {
    Schema::new(vec![
        Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("value", DataType::Float64, true),
    ])
}

fn storage_schema() -> SchemaRef {
    append_system_fields(&client_schema())
}

/// A batch as a client would send it: ids resolved, no system columns.
fn client_batch(rows: &[(MetricId, Micros, f64)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(client_schema()),
        vec![
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("client batch")
}

/// Builds a registry writing through a real never-reclaimed log.
fn logged_registry(storage: &InMemoryStorage, sequence: Arc<AtomicSequence>) -> MetricRegistry {
    let log = Arc::new(
        Wal::open(
            Arc::new(storage.clone()),
            std::path::PathBuf::from(CATALOG_LOG_DIR),
            wal_config(),
        )
        .expect("registration log"),
    );
    MetricRegistry::new(Arc::new(WalCatalogLog::new(log, sequence)))
}

fn series(name: &str, labels: &[(&str, &str)]) -> SeriesKey {
    SeriesKey::new(
        name,
        labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
    )
}

/// §10, §11: a restart has to recover both the identity mapping and the layout,
/// or the archive on disk cannot be interpreted. The two come from different
/// places — the registration log and the instance metadata — and this is the
/// first thing a start does.
#[test]
fn a_restart_recovers_the_registry_and_the_layout() {
    let storage = InMemoryStorage::new();
    let assigned: Vec<MetricId>;

    // ---- first run ----
    {
        let metadata = InstanceMetadata::open(Arc::new(storage.clone()), PartitionInterval::Month)
            .expect("initialise");
        assert_eq!(metadata.partition_interval(), PartitionInterval::Month);

        let registry = logged_registry(&storage, Arc::new(AtomicSequence::starting_at(0)));
        assigned = vec![
            registry
                .register(series("temp", &[("plant", "a")]))
                .unwrap(),
            registry
                .register(series("temp", &[("plant", "b")]))
                .unwrap(),
            registry.register(series("flow", &[])).unwrap(),
        ];
    }

    // ---- restart ----
    let metadata = InstanceMetadata::open(Arc::new(storage.clone()), PartitionInterval::Month)
        .expect("reopen");
    assert_eq!(
        metadata.partition_interval(),
        PartitionInterval::Month,
        "the layout must survive the restart, not be re-chosen"
    );

    let recovered =
        replay_catalog(&storage, std::path::Path::new(CATALOG_LOG_DIR)).expect("replay catalogue");
    assert_eq!(recovered.registrations.len(), 3);
    assert_eq!(recovered.undecodable, 0);

    let registry = logged_registry(&storage, Arc::new(AtomicSequence::starting_at(0)));
    registry.replay(recovered.registrations).unwrap();

    // The ids a client cached before the restart must still resolve, and
    // re-registering must return them rather than mint new ones (§10).
    assert_eq!(
        registry.resolve(&series("temp", &[("plant", "a")])),
        Some(assigned[0])
    );
    assert_eq!(
        registry.register(series("flow", &[])).unwrap(),
        assigned[2],
        "re-registration after a restart must be idempotent"
    );
    assert_eq!(registry.len(), 3);
}

/// §11: a mismatch between the configured interval and the recorded one is a
/// startup error. Checked here as well as in the unit tests because this is the
/// path a real start takes — metadata first, then everything else.
#[test]
fn a_restart_with_a_different_interval_refuses_before_touching_anything() {
    let storage = InMemoryStorage::new();
    InstanceMetadata::open(Arc::new(storage.clone()), PartitionInterval::Month)
        .expect("initialise");

    let error = InstanceMetadata::open(Arc::new(storage.clone()), PartitionInterval::Hour)
        .expect_err("must refuse to start");

    let message = error.to_string();
    assert!(
        message.contains("month") && message.contains("hour"),
        "the error must name both intervals: {message}"
    );
}

/// §3, §1: a batch may span a partition boundary, and each partition's table
/// must end up sorted by `(metric_id, timestamp, seq)` — the order every flushed
/// file depends on. Routing itself is not built yet, so this test groups by
/// [`PartitionInterval::start`] to show that the layout and the table agree on
/// what belongs together.
#[test]
fn a_batch_spanning_a_boundary_splits_into_sorted_partitions() {
    let storage = InMemoryStorage::new();
    let interval = PartitionInterval::Month;
    InstanceMetadata::open(Arc::new(storage.clone()), interval).expect("initialise");

    let sequence = Arc::new(AtomicSequence::starting_at(0));
    let registry = logged_registry(&storage, Arc::clone(&sequence));
    let temp = registry.register(series("temp", &[])).unwrap();
    let flow = registry.register(series("flow", &[])).unwrap();

    let stamper = Stamper::new(
        Arc::new(TestClock::new(AUGUST)),
        Arc::clone(&sequence) as Arc<_>,
    );

    // Deliberately unsorted, and spanning two months.
    let rows = [
        (flow, SEPTEMBER + 60_000_000, 3.0),
        (temp, AUGUST, 1.0),
        (flow, AUGUST + 60_000_000, 2.0),
        (temp, SEPTEMBER, 4.0),
    ];

    let mut partitions: std::collections::BTreeMap<Micros, ActiveMemTable> =
        std::collections::BTreeMap::new();
    for row in rows {
        let start = interval.start(row.1).expect("partition start");
        let table = partitions.entry(start).or_insert_with(|| {
            ActiveMemTable::new(storage_schema(), SuppressionConfig::for_stream())
                .expect("memtable")
        });
        let stamped = stamper.stamp(&client_batch(&[row])).expect("stamp");
        table.insert(&stamped).expect("insert");
    }

    assert_eq!(partitions.len(), 2, "the batch spans two months");

    for (start, table) in &partitions {
        let frozen = table.freeze().expect("freeze");
        let order: Vec<(MetricId, Micros, u64)> = frozen
            .sorted()
            .iter()
            .map(|version| (version.key.metric_id, version.key.timestamp, version.seq))
            .collect();

        let mut expected = order.clone();
        expected.sort_unstable();
        assert_eq!(order, expected, "partition {start} is not in flush order");

        // Every row in a partition must belong to it, or the file would sit in a
        // directory whose name contradicts its contents.
        for (_, timestamp, _) in &order {
            assert_eq!(interval.start(*timestamp).unwrap(), *start);
        }
    }

    // The layout the writer will use, derived from the interval rather than
    // assumed (§11).
    let paths: Vec<String> = partitions
        .keys()
        .map(|start| {
            interval
                .path(*start)
                .expect("path")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert_eq!(paths, vec!["year=2026/month=08", "year=2026/month=09"]);
}

/// §1: the workload this archive is sized for — a job recomputing its horizon
/// every hour and resubmitting values that have not changed. Driven here through
/// the real stamper and clock rather than hand-written system columns, with a
/// freeze between every run, because that combination is what previously let
/// repeats through.
#[test]
fn a_day_of_hourly_recomputation_leaves_one_version_per_point() {
    const RUNS: usize = 24;
    const POINTS: usize = 6;

    let storage = InMemoryStorage::new();
    let sequence = Arc::new(AtomicSequence::starting_at(0));
    let registry = logged_registry(&storage, Arc::clone(&sequence));
    let forecast = registry
        .register(series("forecast", &[("horizon", "6h")]))
        .unwrap();

    let clock = Arc::new(TestClock::new(AUGUST));
    let stamper = Stamper::new(
        Arc::clone(&clock) as Arc<_>,
        Arc::clone(&sequence) as Arc<_>,
    );
    let active = ActiveMemTable::new(storage_schema(), SuppressionConfig::for_recomputed(HOUR))
        .expect("memtable");

    // The forecast covers the next six hours and never changes.
    let horizon: Vec<(MetricId, Micros, f64)> = (0..POINTS)
        .map(|point| (forecast, AUGUST + point as Micros * HOUR, 20.5))
        .collect();

    let mut tables = Vec::new();
    for _ in 0..RUNS {
        let stamped = stamper.stamp(&client_batch(&horizon)).expect("stamp");
        active.insert(&stamped).expect("insert");
        tables.push(active.freeze().expect("freeze"));
        clock.advance(HOUR);
    }
    tables.push(active.current());

    let versions: usize = tables.iter().map(|table| table.num_rows()).sum();
    assert_eq!(
        versions, POINTS,
        "{RUNS} identical runs over {POINTS} points must leave {POINTS} versions"
    );

    // And a genuine change still lands, at the same rhythm.
    let corrected: Vec<(MetricId, Micros, f64)> = horizon
        .iter()
        .map(|(id, timestamp, _)| (*id, *timestamp, 21.5))
        .collect();
    let stamped = stamper.stamp(&client_batch(&corrected)).expect("stamp");
    let outcome = active.insert(&stamped).expect("insert");

    assert_eq!(
        outcome.inserted, POINTS,
        "a changed forecast must be a new version for every point"
    );
    assert_eq!(outcome.suppressed, 0);
}

/// §1: the resolved version of a suppressed point keeps the `system_time` of the
/// last *change*, not of the last run that reasserted it. That is the reading
/// §1 settles on, and it is only observable across the whole stack.
#[test]
fn a_suppressed_run_does_not_move_the_resolved_system_time() {
    let storage = InMemoryStorage::new();
    let sequence = Arc::new(AtomicSequence::starting_at(0));
    let registry = logged_registry(&storage, Arc::clone(&sequence));
    let id = registry.register(series("forecast", &[])).unwrap();

    let clock = Arc::new(TestClock::new(AUGUST));
    let stamper = Stamper::new(
        Arc::clone(&clock) as Arc<_>,
        Arc::clone(&sequence) as Arc<_>,
    );
    let active = ActiveMemTable::new(storage_schema(), SuppressionConfig::for_recomputed(HOUR))
        .expect("memtable");

    active
        .insert(&stamper.stamp(&client_batch(&[(id, AUGUST, 20.5)])).unwrap())
        .unwrap();
    let first_seq = active
        .current()
        .latest(RowKey {
            metric_id: id,
            timestamp: AUGUST,
        })
        .expect("a version")
        .seq;

    // Three further runs reassert the same value an hour apart.
    for _ in 0..3 {
        clock.advance(HOUR);
        let stamped = stamper.stamp(&client_batch(&[(id, AUGUST, 20.5)])).unwrap();
        assert_eq!(active.insert(&stamped).unwrap().suppressed, 1);
    }

    let latest = active
        .current()
        .latest(RowKey {
            metric_id: id,
            timestamp: AUGUST,
        })
        .expect("a version");
    assert_eq!(
        latest.seq, first_seq,
        "the surviving version must still be the one that introduced the value"
    );
    assert_eq!(active.current().num_rows(), 1);
}

/// §6: the reason the schema is durable at all. A restart rebuilds the catalogue
/// from the same log the registrations come from, and the type-change rejection
/// still works afterwards — without it a client sending a conflicting type would
/// be admitted and the clash would surface later as unreadable Parquet.
#[test]
fn a_restart_keeps_rejecting_a_type_change() {
    let storage = InMemoryStorage::new();
    let sequence = Arc::new(AtomicSequence::starting_at(0));

    let log = Arc::new(
        Wal::open(
            Arc::new(storage.clone()),
            std::path::PathBuf::from(CATALOG_LOG_DIR),
            wal_config(),
        )
        .expect("catalogue log"),
    );
    let catalog_log = Arc::new(WalCatalogLog::new(log, Arc::clone(&sequence) as Arc<_>));

    // ---- first run: the table is created and evolved ----
    {
        let catalogue =
            TableCatalogue::new(Arc::clone(&catalog_log) as Arc<_>, TableConfig::default());
        catalogue
            .accept("readings", &client_schema())
            .expect("accept");

        let mut wider = client_schema().fields().to_vec();
        wider.push(Arc::new(Field::new("quality", DataType::Int32, true)));
        catalogue
            .accept("readings", &Schema::new(wider))
            .expect("evolve");
    }

    // ---- restart: rebuild the catalogue from the log ----
    let recovered =
        replay_catalog(&storage, std::path::Path::new(CATALOG_LOG_DIR)).expect("replay");
    assert_eq!(recovered.schemas.len(), 2, "creation and evolution");

    let catalogue = TableCatalogue::new(Arc::clone(&catalog_log) as Arc<_>, TableConfig::default());
    catalogue.replay(recovered.schemas).expect("replay schemas");

    let restored = catalogue.schema("readings").expect("readings");
    assert!(
        restored.logical().field_with_name("quality").is_ok(),
        "the evolution must survive the restart"
    );

    // The conflicting type must still be refused.
    let conflicting = Schema::new(vec![
        Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("value", DataType::Int64, true),
    ]);
    assert!(
        catalogue.accept("readings", &conflicting).is_err(),
        "a recorded schema must keep refusing a conflicting type after a restart"
    );
}
