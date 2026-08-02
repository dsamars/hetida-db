// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! System-level verification of the guarantees the archive advertises.
//!
//! These are not the primary tests — every component is unit-tested beside its
//! code, and the invariants are properties in `invariants.rs`. What is here are
//! the Phase 4 checks: each one takes a promise the archive makes to a *client*
//! and confirms it end to end, through the real archive, the real flusher, the
//! real compactor and the real query engine.
//!
//! Deterministic throughout. A crash is an injected torn write, not a killed
//! process; pruning is measured in bytes served by the store, not in wall time.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};

use hetida_db::archive::{Archive, ArchiveConfig};
use hetida_db::clock::{Clock, Micros, TestClock};
use hetida_db::compact::{CompactConfig, Compactor, keys_of, partition_dirs};
use hetida_db::flush::Flusher;
use hetida_db::parquet::ParquetConfig;
use hetida_db::query::QueryEngine;
use hetida_db::registry::{MetricId, SeriesKey};
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, timestamp_type};
use hetida_db::storage::{Fault, InMemoryStorage, Storage};
use hetida_db::wal::WalConfig;

/// 2026-08-02T14:37:12Z.
const AUGUST: Micros = 1_785_681_432_000_000;
/// 2026-06-15T00:00:00Z — two months earlier, a different partition.
const JUNE: Micros = 1_781_481_600_000_000;
const MINUTE: Micros = 60_000_000;
const HOUR: Micros = 3_600_000_000;
const DAY: Micros = 86_400_000_000;

/// The archive under test, with the pieces a verification needs to drive.
struct Archived {
    archive: Arc<Archive>,
    engine: QueryEngine,
    storage: InMemoryStorage,
    clock: Arc<TestClock>,
    parquet: ParquetConfig,
}

fn wal_config() -> WalConfig {
    WalConfig {
        segment_bytes: 64 * 1024,
        max_commit_delay: Duration::ZERO,
    }
}

fn open(storage: &InMemoryStorage, clock: Arc<TestClock>, parquet: ParquetConfig) -> Archived {
    let archive = Arc::new(
        Archive::open(
            Arc::new(storage.clone()),
            &ArchiveConfig {
                wal: wal_config(),
                ..ArchiveConfig::default()
            },
            Arc::clone(&clock) as Arc<_>,
        )
        .expect("archive"),
    );
    Archived {
        engine: QueryEngine::new(Arc::clone(&archive), Arc::new(storage.clone())),
        archive,
        storage: storage.clone(),
        clock,
        parquet,
    }
}

fn archived() -> Archived {
    open(
        &InMemoryStorage::new(),
        Arc::new(TestClock::new(AUGUST)),
        ParquetConfig::default(),
    )
}

impl Archived {
    fn ingest(&self, rows: &[(MetricId, Micros, f64)]) {
        self.archive
            .ingest("readings", &client_batch(rows))
            .expect("ingest");
    }

    /// Moves every partition of `readings` into Parquet.
    fn flush(&self) {
        let Some(state) = self.archive.table("readings") else {
            return;
        };
        let flusher = Flusher::new(
            Arc::new(self.storage.clone()),
            "readings",
            self.archive.interval(),
            self.parquet,
        );
        for start in state.partition_starts() {
            flusher.flush(&state, start).expect("flush");
        }
    }

    /// Merges every partition, however few files it holds.
    fn compact(&self) {
        let compactor = Compactor::new(
            Arc::new(self.storage.clone()),
            CompactConfig {
                max_files: 2,
                max_bytes: usize::MAX,
                target_bytes: usize::MAX,
            },
            self.parquet,
        );
        for directory in self.partitions() {
            compactor.compact_if_due(&directory).expect("compact");
        }
    }

    fn partitions(&self) -> Vec<std::path::PathBuf> {
        partition_dirs(&self.storage, "readings", self.archive.interval()).expect("partitions")
    }

    /// Every `(metric_id, timestamp, value)` a query answered with, sorted.
    async fn query(&self, sql: &str, as_of: Option<Micros>) -> Vec<(MetricId, Micros, f64)> {
        let batches = self.engine.collect(sql, as_of).await.expect("query");
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = column::<UInt64Array>(batch, METRIC_ID_COLUMN);
            let stamps = column::<TimestampMicrosecondArray>(batch, TIMESTAMP_COLUMN);
            let values = column::<Float64Array>(batch, "value");
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), stamps.value(row), values.value(row)));
            }
        }
        rows.sort_by(|a, b| a.partial_cmp(b).expect("total order"));
        rows
    }
}

fn column<'a, A: 'static>(batch: &'a RecordBatch, name: &str) -> &'a A {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<A>())
        .unwrap_or_else(|| panic!("column `{name}` is missing or the wrong type"))
}

fn client_schema() -> Schema {
    Schema::new(vec![
        Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("value", DataType::Float64, true),
    ])
}

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

// ---- zero copy, sort order and version preservation ----

/// The hot-path rule, verified where it actually matters: a client's payload
/// buffer must reach the memtable **shared, not copied**. Stamping builds a new
/// schema and two new arrays; every column the client sent is moved by `Arc`.
///
/// Checked by pointer identity, because nothing else distinguishes a share from
/// a copy that happens to hold equal values.
#[test]
fn a_clients_column_buffers_reach_the_memtable_uncopied() {
    let fixture = archived();
    let batch = client_batch(&[(1, AUGUST, 1.5), (1, AUGUST + HOUR, 2.5), (2, AUGUST, 3.5)]);

    fixture.archive.ingest("readings", &batch).expect("ingest");

    let state = fixture.archive.table("readings").expect("readings");
    let start = fixture.archive.interval().start(AUGUST).expect("start");
    let stored = state
        .partition_table(start)
        .expect("partition")
        .current()
        .batch(0)
        .expect("one buffered batch");

    for name in [TIMESTAMP_COLUMN, METRIC_ID_COLUMN, "value"] {
        let sent = batch.column_by_name(name).expect("sent");
        let held = stored.column_by_name(name).expect("held");
        assert!(
            Arc::ptr_eq(sent, held),
            "column `{name}` was copied on its way into the memtable"
        );
    }
}

/// §1, §2, §3: every file that reaches disk is ordered by
/// `(metric_id, timestamp, seq)`, and compaction preserves both the order and
/// every single version — including several versions of one logical row.
#[test]
fn flushed_and_compacted_files_stay_sorted_with_every_version() {
    let fixture = archived();

    // Three versions of one logical row, plus neighbours, written across two
    // flushes so compaction has something to merge.
    for value in [1.5, 2.5, 3.5] {
        fixture.ingest(&[(1, AUGUST, value)]);
        fixture.clock.advance(HOUR);
    }
    fixture.flush();
    fixture.ingest(&[(2, AUGUST, 9.9), (1, AUGUST + HOUR, 8.8)]);
    fixture.flush();

    let keys_before = every_key(&fixture);
    assert_eq!(keys_before.len(), 5, "every version must be on disk");

    fixture.compact();
    let keys_after = every_key(&fixture);

    assert_eq!(
        keys_after, keys_before,
        "compaction lost, invented or altered a version"
    );
    // Three versions of (1, AUGUST) must all still be there.
    assert_eq!(
        keys_after
            .iter()
            .filter(|(metric, timestamp, _)| *metric == 1 && *timestamp == AUGUST)
            .count(),
        3
    );
}

/// Every key on disk, sorted, after asserting each file is sorted on its own.
fn every_key(fixture: &Archived) -> Vec<(MetricId, Micros, u64)> {
    let mut all = Vec::new();
    for directory in fixture.partitions() {
        for file in fixture.storage.list(&directory).expect("files") {
            let keys = keys_of(&fixture.storage, &file).expect("keys");
            let mut sorted = keys.clone();
            sorted.sort_unstable();
            assert_eq!(keys, sorted, "{} is not in flush order", file.display());
            all.extend(keys);
        }
    }
    all.sort_unstable();
    all
}

// ---- time travel (§1) ----

/// §1: the promise the archive is for. A value, a correction, and each
/// observation point answering with the version that was current at it —
/// including the point before anything was known.
#[tokio::test]
async fn every_observation_point_answers_with_the_version_current_at_it() {
    let fixture = archived();

    fixture.ingest(&[(1, AUGUST, 1.5)]);
    let first_write = fixture.clock.now_micros();
    fixture.clock.advance(HOUR);
    fixture.ingest(&[(1, AUGUST, 9.9)]);
    let correction = fixture.clock.now_micros();

    let sql = "SELECT * FROM readings";
    assert!(
        fixture.query(sql, Some(first_write - 1)).await.is_empty(),
        "before the first write the archive knew nothing"
    );
    assert_eq!(
        fixture.query(sql, Some(first_write)).await,
        vec![(1, AUGUST, 1.5)],
        "between the two, the first value stood"
    );
    assert_eq!(
        fixture.query(sql, Some(correction - 1)).await,
        vec![(1, AUGUST, 1.5)],
        "and it still stood a microsecond before the correction"
    );
    assert_eq!(
        fixture.query(sql, Some(correction)).await,
        vec![(1, AUGUST, 9.9)]
    );
    assert_eq!(fixture.query(sql, None).await, vec![(1, AUGUST, 9.9)]);
}

/// §1, §3: the case the archive exists for — a correction that lands long after
/// its partition was flushed *and* compacted. Resolution then spans a version in
/// Parquet and a version in memory, and the earlier observation point must still
/// answer with what was on disk.
#[tokio::test]
async fn a_correction_after_flush_and_compaction_still_time_travels() {
    let fixture = archived();

    // Two versions on disk, in separate files, then merged into one.
    fixture.ingest(&[(1, AUGUST, 1.5)]);
    let original = fixture.clock.now_micros();
    fixture.flush();
    fixture.clock.advance(HOUR);
    fixture.ingest(&[(1, AUGUST, 2.5)]);
    let second = fixture.clock.now_micros();
    fixture.flush();
    fixture.compact();
    assert_eq!(
        fixture
            .storage
            .list(&fixture.partitions()[0])
            .expect("files")
            .len(),
        1,
        "both versions must now be in one compacted file"
    );

    // A third correction arrives much later and stays in memory.
    fixture.clock.advance(30 * DAY);
    fixture.ingest(&[(1, AUGUST, 3.5)]);
    let late = fixture.clock.now_micros();

    let sql = "SELECT * FROM readings";
    assert_eq!(
        fixture.query(sql, Some(original)).await,
        vec![(1, AUGUST, 1.5)],
        "compaction must not have moved what the first point saw"
    );
    assert_eq!(
        fixture.query(sql, Some(second)).await,
        vec![(1, AUGUST, 2.5)]
    );
    assert_eq!(fixture.query(sql, Some(late)).await, vec![(1, AUGUST, 3.5)]);

    // And the raw history holds all three, wherever they live.
    assert_eq!(
        fixture
            .query("SELECT * FROM \"readings$history\"", None)
            .await
            .len(),
        3
    );
}

// ---- retransmission suppression (§1) ----

/// §1: re-sending an identical batch must create no version and change no
/// answer — at *any* observation point, which is the part a version count alone
/// would not catch.
#[tokio::test]
async fn a_retransmission_creates_no_version_and_moves_no_answer() {
    let fixture = archived();
    let batch = client_batch(&[(1, AUGUST, 1.5), (2, AUGUST + HOUR, 2.5)]);

    fixture.archive.ingest("readings", &batch).expect("first");
    let first = fixture.clock.now_micros();

    let sql = "SELECT * FROM readings";
    let history = "SELECT * FROM \"readings$history\"";
    let before = fixture.query(sql, None).await;
    let before_at_first = fixture.query(sql, Some(first)).await;

    // Three retransmissions, spread across ingest time and a flush. A minute
    // apart, because the default reach is the *stream* preset — five minutes,
    // covering transport retries (§1). A job on an hourly rhythm needs
    // `SuppressionConfig::for_recomputed`, which is a per-table choice (§6).
    for round in 0..3 {
        fixture.clock.advance(MINUTE);
        let outcome = fixture
            .archive
            .ingest("readings", &batch)
            .expect("retransmission");
        assert_eq!(
            (outcome.inserted, outcome.suppressed),
            (0, 2),
            "round {round} manufactured a version"
        );
        if round == 1 {
            fixture.flush();
        }
    }

    assert_eq!(
        fixture.query(history, None).await.len(),
        2,
        "the history must still hold exactly the two original versions"
    );
    assert_eq!(fixture.query(sql, None).await, before);
    assert_eq!(
        fixture.query(sql, Some(first)).await,
        before_at_first,
        "an earlier observation point must be untouched too"
    );

    // A genuine change still lands, so suppression is not simply refusing writes.
    fixture.clock.advance(MINUTE);
    let changed = fixture
        .archive
        .ingest("readings", &client_batch(&[(1, AUGUST, 7.7)]))
        .expect("a change");
    assert_eq!(changed.inserted, 1);
    assert_eq!(fixture.query(sql, None).await[0], (1, AUGUST, 7.7));
}

// ---- crash recovery (§4) ----

/// §4: a crash mid-write. Reproduced as an injected torn record rather than a
/// killed process, because a crash test that cannot be reproduced is not a test.
///
/// Startup must succeed, everything acknowledged before the damage must be back,
/// and the registry — the one state the archive cannot be interpreted without —
/// must be intact.
#[test]
fn a_torn_write_does_not_stop_a_start_and_loses_only_the_torn_record() {
    let storage = InMemoryStorage::new();
    let registered;

    {
        let fixture = open(
            &storage,
            Arc::new(TestClock::new(AUGUST)),
            ParquetConfig::default(),
        );
        registered = fixture
            .archive
            .registry()
            .register(SeriesKey::new(
                "temp",
                [("plant".to_owned(), "a".to_owned())],
            ))
            .expect("register");

        fixture.ingest(&[(registered, AUGUST, 1.5)]);
        fixture.ingest(&[(registered, AUGUST + HOUR, 2.5)]);

        // The crash: the next record reaches the device only in part.
        storage.inject(Fault::TornWrite { keep: 8 });
        let _ = fixture.archive.ingest(
            "readings",
            &client_batch(&[(registered, AUGUST + 2 * HOUR, 3.5)]),
        );
    }

    let recovered = open(
        &storage,
        Arc::new(TestClock::new(AUGUST + DAY)),
        ParquetConfig::default(),
    );

    assert!(
        recovered.archive.recovered().truncated,
        "the damage must be reported, not silently absorbed"
    );
    assert_eq!(
        recovered.archive.recovered().data_records,
        2,
        "both acknowledged batches must be back"
    );
    assert_eq!(
        recovered.archive.registry().resolve(&SeriesKey::new(
            "temp",
            [("plant".to_owned(), "a".to_owned())]
        )),
        Some(registered),
        "the registry is what makes the data interpretable at all (§10)"
    );
}

/// §4: replay is deterministic. Opening the same store twice must recover the
/// same rows, the same sequence position and the same versions — otherwise a
/// restart could resolve differently from the run before it.
#[test]
fn replay_is_deterministic() {
    let storage = InMemoryStorage::new();
    {
        let fixture = open(
            &storage,
            Arc::new(TestClock::new(AUGUST)),
            ParquetConfig::default(),
        );
        for round in 0..5 {
            fixture.clock.advance(HOUR);
            fixture.ingest(&[(1, AUGUST, round as f64), (2, AUGUST + HOUR, 1.0)]);
        }
        storage.inject(Fault::TornWrite { keep: 12 });
        let _ = fixture
            .archive
            .ingest("readings", &client_batch(&[(3, AUGUST, 9.9)]));
    }

    let describe = || {
        let fixture = open(
            &storage,
            Arc::new(TestClock::new(AUGUST + DAY)),
            ParquetConfig::default(),
        );
        let state = fixture.archive.table("readings").expect("readings");
        (
            fixture.archive.recovered(),
            state.num_rows(),
            state.min_seq(),
            state.partition_starts(),
        )
    };

    let first = describe();
    let second = describe();
    assert_eq!(
        first, second,
        "two replays of one store disagreed, so a restart is not reproducible"
    );
}

/// §4: an acknowledgement means the rows survive a crash. What a client was told
/// was durable has to be queryable after a restart, not merely present in a log.
#[tokio::test]
async fn acknowledged_rows_are_queryable_after_a_restart() {
    let storage = InMemoryStorage::new();
    {
        let fixture = open(
            &storage,
            Arc::new(TestClock::new(AUGUST)),
            ParquetConfig::default(),
        );
        fixture.ingest(&[(1, AUGUST, 1.5)]);
        // One partition already in Parquet, one only in the log.
        fixture.flush();
        fixture.clock.advance(HOUR);
        fixture.ingest(&[(2, JUNE, 2.5)]);
    }

    let recovered = open(
        &storage,
        Arc::new(TestClock::new(AUGUST + DAY)),
        ParquetConfig::default(),
    );

    assert_eq!(
        recovered.query("SELECT * FROM readings", None).await,
        vec![(1, AUGUST, 1.5), (2, JUNE, 2.5)],
        "rows in Parquet and rows replayed from the log must both come back"
    );
}

// ---- metric pruning (§11) ----

/// §11: the sort is metric-major so that row-group statistics skip everything
/// outside the queried metrics. Measured in bytes served by the store, because
/// wall time would make this a benchmark and a flaky test.
#[tokio::test]
async fn a_query_on_one_metric_reads_far_less_than_the_whole_file() {
    const METRICS: u64 = 32;
    const POINTS: i64 = 512;

    // One row group per metric. Row groups any smaller and the footer, not the
    // data, is what a scan pays for — the metadata overhead §11 warns about,
    // which pruning cannot help with.
    let fixture = open(
        &InMemoryStorage::new(),
        Arc::new(TestClock::new(AUGUST)),
        ParquetConfig {
            row_group_rows: POINTS as usize,
            ..ParquetConfig::default()
        },
    );

    // Written metric-major, which is the order a flush produces anyway (§1).
    for metric in 1..=METRICS {
        let rows: Vec<(MetricId, Micros, f64)> = (0..POINTS)
            .map(|point| (metric, AUGUST + point * HOUR, point as f64))
            .collect();
        fixture.ingest(&rows);
    }
    fixture.flush();

    fixture.storage.reset_bytes_read();
    let everything = fixture.query("SELECT * FROM readings", None).await;
    let whole = fixture.storage.bytes_read();

    fixture.storage.reset_bytes_read();
    let one = fixture
        .query("SELECT * FROM readings WHERE metric_id = 7", None)
        .await;
    let pruned = fixture.storage.bytes_read();

    assert_eq!(everything.len() as u64, METRICS * POINTS as u64);
    assert_eq!(one.len() as i64, POINTS, "and the answer is still right");
    // Around 24 kB of 185 kB at the time of writing. The floor is the footer
    // and the page index, which every reader pays whatever it then skips — the
    // metadata overhead §11 warns about, and the reason the threshold here is a
    // factor rather than a small absolute number.
    assert!(
        pruned * 4 < whole,
        "one metric of {METRICS} read {pruned} bytes of {whole} — \
         row groups are not being skipped"
    );
}

/// §10, §11: the same pruning reached through a label rather than an id, which
/// is how a client actually asks. The predicate is resolved against the registry
/// before planning, so it arrives at the scan as a filter on `metric_id`.
#[tokio::test]
async fn a_label_predicate_prunes_as_hard_as_an_id_does() {
    const METRICS: u64 = 32;
    const POINTS: i64 = 512;

    let fixture = open(
        &InMemoryStorage::new(),
        Arc::new(TestClock::new(AUGUST)),
        ParquetConfig {
            row_group_rows: POINTS as usize,
            ..ParquetConfig::default()
        },
    );

    let mut wanted = 0;
    for index in 0..METRICS {
        let id = fixture
            .archive
            .registry()
            .register(SeriesKey::new(
                format!("temp-{index}"),
                [("plant".to_owned(), format!("p{index}"))],
            ))
            .expect("register");
        if index == 7 {
            wanted = id;
        }
        let rows: Vec<(MetricId, Micros, f64)> = (0..POINTS)
            .map(|point| (id, AUGUST + point * HOUR, point as f64))
            .collect();
        fixture.ingest(&rows);
    }
    fixture.flush();

    fixture.storage.reset_bytes_read();
    fixture.query("SELECT * FROM readings", None).await;
    let whole = fixture.storage.bytes_read();

    fixture.storage.reset_bytes_read();
    let answered = fixture
        .query(
            "SELECT * FROM readings WHERE hetida_label('plant', 'p7')",
            None,
        )
        .await;
    let pruned = fixture.storage.bytes_read();

    assert!(
        answered.iter().all(|(metric, _, _)| *metric == wanted),
        "the label selected the wrong series"
    );
    assert_eq!(answered.len() as i64, POINTS);
    // The same figures as the query by id: the rewrite has to reach the scan,
    // not merely answer correctly above it.
    assert!(
        pruned * 4 < whole,
        "a label predicate read {pruned} bytes of {whole} — it is not pruning"
    );
}
