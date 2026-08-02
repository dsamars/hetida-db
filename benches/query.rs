// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Query-side benchmarks: what resolution costs, and what the layout costs.
//!
//! Two questions Phase 4 asks, and neither is answerable by reasoning:
//!
//! - **Does as-of resolution stay a streaming merge as versions accumulate?**
//!   §1 says it must. A plan that degraded into a full sort would still answer
//!   correctly, so the only way to see it is to add versions and watch the
//!   latency. Linear in version depth is the merge; anything steeper is a sort.
//!   The unit tests read the physical plan for a `SortExec`; this measures the
//!   consequence, which is the thing that actually matters.
//!
//! - **Which partition interval should be the default?** §11 argues for `month`
//!   from metadata overhead, and says so explicitly as a claim to be settled
//!   empirically. This runs the workload §11 names — a few metrics over weeks to
//!   months — against all four, and reports files opened and bytes scanned
//!   beside the latency, because those are what the argument is actually about.
//!
//! Everything runs against [`InMemoryStorage`], so the figures are the archive's
//! own cost. A device-bound number belongs in the load harness.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use hetida_db::archive::{Archive, ArchiveConfig};
use hetida_db::clock::{Micros, TestClock};
use hetida_db::compact::partition_dirs;
use hetida_db::flush::Flusher;
use hetida_db::parquet::ParquetConfig;
use hetida_db::partition::PartitionInterval;
use hetida_db::query::QueryEngine;
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, timestamp_type};
use hetida_db::storage::{InMemoryStorage, Storage};
use hetida_db::wal::WalConfig;

/// 2026-08-01T00:00:00Z — a partition boundary, so the spans below are clean.
const AUGUST: Micros = 1_785_542_400_000_000;
const HOUR: Micros = 3_600_000_000;
const DAY: Micros = 86_400_000_000;

fn client_schema() -> Schema {
    Schema::new(vec![
        Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("value", DataType::Float64, true),
    ])
}

fn client_batch(rows: &[(u64, Micros, f64)]) -> RecordBatch {
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
                rows.iter().map(|(_, _, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("client batch")
}

/// An archive laid out on `interval`, with everything flushed to Parquet.
struct Prepared {
    engine: QueryEngine,
    storage: InMemoryStorage,
}

fn prepare(interval: PartitionInterval, write: impl FnOnce(&Archive, &TestClock)) -> Prepared {
    let storage = InMemoryStorage::new();
    let clock = Arc::new(TestClock::new(AUGUST));
    let archive = Arc::new(
        Archive::open(
            Arc::new(storage.clone()) as Arc<dyn Storage>,
            &ArchiveConfig {
                partition_interval: interval,
                wal: WalConfig {
                    segment_bytes: u64::MAX,
                    max_commit_delay: Duration::ZERO,
                },
                ..ArchiveConfig::default()
            },
            Arc::clone(&clock) as Arc<_>,
        )
        .expect("archive"),
    );

    write(&archive, &clock);

    // Everything on disk, so the measurement is of the read path and not of a
    // memtable that happens to still hold the answer.
    if let Some(state) = archive.table("readings") {
        let flusher = Flusher::new(
            Arc::new(storage.clone()),
            "readings",
            interval,
            ParquetConfig::default(),
        );
        for start in state.partition_starts() {
            flusher.flush(&state, start).expect("flush");
        }
    }

    Prepared {
        engine: QueryEngine::new(Arc::clone(&archive), Arc::new(storage.clone())),
        storage,
    }
}

/// §1: resolution against version depth. Linear is the streaming merge; steeper
/// is the full sort §1 exists to avoid.
///
/// The row *count* grows with the depth, so linear growth is expected and is
/// exactly the point: what would show a sort is growth faster than the number of
/// versions read.
fn resolution_against_version_depth(criterion: &mut Criterion) {
    const METRICS: u64 = 32;
    const POINTS: i64 = 64;

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = criterion.benchmark_group("as_of_resolution");

    for depth in [1usize, 10, 100] {
        let prepared = prepare(PartitionInterval::Month, |archive, clock| {
            for version in 0..depth {
                clock.set(AUGUST + version as Micros * HOUR);
                for metric in 0..METRICS {
                    let rows: Vec<(u64, Micros, f64)> = (0..POINTS)
                        .map(|point| {
                            // A different value every version, or suppression
                            // would discard all but the first (§1).
                            (
                                metric,
                                AUGUST + point * HOUR,
                                (version * 1000) as f64 + point as f64,
                            )
                        })
                        .collect();
                    archive
                        .ingest("readings", &client_batch(&rows))
                        .expect("ingest");
                }
            }
        });

        group.throughput(criterion::Throughput::Elements(
            METRICS * POINTS as u64 * depth as u64,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(depth),
            &prepared,
            |bencher, prepared| {
                bencher.iter(|| {
                    runtime
                        .block_on(prepared.engine.collect("SELECT * FROM readings", None))
                        .expect("query")
                });
            },
        );
    }
    group.finish();
}

/// §11: the four intervals against the workload §11 names — a few metrics over
/// weeks to months — reporting what the argument is actually about.
///
/// Latency alone would not settle it. The claim is that metadata overhead, not
/// scan volume, dominates this pattern, so files opened and bytes scanned are
/// printed beside it.
fn partition_interval_variants(criterion: &mut Criterion) {
    /// A few metrics, as §11 describes the workload.
    const METRICS: u64 = 8;
    /// Hourly points over a month — long enough that `day` and `hour` produce
    /// the many-directories case the default is chosen against.
    const SPAN_DAYS: i64 = 30;

    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let mut group = criterion.benchmark_group("partition_interval");

    // A fortnight of two metrics: the "weeks to months over a few metrics"
    // query §11 sizes the layout for.
    let query = format!(
        "SELECT * FROM readings \
         WHERE metric_id IN (1, 2) \
           AND \"timestamp\" >= arrow_cast({}, 'Timestamp(Microsecond, None)') \
           AND \"timestamp\" < arrow_cast({}, 'Timestamp(Microsecond, None)')",
        AUGUST + 7 * DAY,
        AUGUST + 21 * DAY,
    );

    println!("\n  partition interval | partitions | files | bytes scanned | rows");
    println!("  -------------------|------------|-------|---------------|-----");

    for interval in PartitionInterval::ALL {
        let prepared = prepare(interval, |archive, clock| {
            for day in 0..SPAN_DAYS {
                for hour in 0..24 {
                    clock.set(AUGUST + day * DAY + hour * HOUR);
                    let at = AUGUST + day * DAY + hour * HOUR;
                    let rows: Vec<(u64, Micros, f64)> = (0..METRICS)
                        .map(|metric| (metric, at, (day * 24 + hour) as f64))
                        .collect();
                    archive
                        .ingest("readings", &client_batch(&rows))
                        .expect("ingest");
                }
            }
        });

        // The three figures §11's argument turns on, measured once.
        let partitions =
            partition_dirs(&prepared.storage, "readings", interval).expect("partitions");
        let files: usize = partitions
            .iter()
            .map(|directory| {
                prepared
                    .storage
                    .list(directory)
                    .map(|entries| entries.len())
                    .unwrap_or(0)
            })
            .sum();
        prepared.storage.reset_bytes_read();
        let rows = runtime
            .block_on(prepared.engine.collect(&query, None))
            .expect("query")
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        let scanned = prepared.storage.bytes_read();

        println!(
            "  {:<18} | {:>10} | {:>5} | {:>13} | {:>4}",
            interval.as_str(),
            partitions.len(),
            files,
            scanned,
            rows,
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(interval.as_str()),
            &prepared,
            |bencher, prepared| {
                bencher.iter(|| {
                    runtime
                        .block_on(prepared.engine.collect(&query, None))
                        .expect("query")
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    resolution_against_version_depth,
    partition_interval_variants,
);
criterion_main!(benches);
