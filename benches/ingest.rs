// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Micro-benchmarks for the three components on the ingestion path.
//!
//! Benchmarks are not tests. Correctness was settled long before this file, and
//! nothing here asserts anything — these measure what the write path costs, so a
//! change that makes it cost more is visible rather than inferred.
//!
//! What is measured, and why each one:
//!
//! - **`MemTable::insert`** — the versioning index is built by walking the three
//!   key columns once, which is the one unavoidable per-row pass on the write
//!   path (§1). Everything else about a batch is moved by `Arc`.
//! - **WAL append** — an acknowledgement waits for the fsync its record was part
//!   of (§4), so this is what bounds `do_put` latency.
//! - **Parquet write** — the flush path, off the ingestion path (§2), but it is
//!   what bounds how fast an archive can shed memory under load.
//!
//! Every run uses [`InMemoryStorage`], so the numbers are the archive's own cost
//! and not the host filesystem's. A device-bound figure belongs in the load
//! harness, where it can be reported beside the hardware it was measured on.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use hetida_db::active::ActiveMemTable;
use hetida_db::clock::{Micros, TestClock};
use hetida_db::parquet::{ParquetConfig, write_partition};
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, append_system_fields, timestamp_type};
use hetida_db::seq::AtomicSequence;
use hetida_db::stamp::Stamper;
use hetida_db::storage::{InMemoryStorage, Storage};
use hetida_db::suppression::SuppressionConfig;
use hetida_db::wal::{RecordKind, Wal, WalConfig};

/// 2026-08-02T14:37:12Z.
const AUGUST: Micros = 1_785_681_432_000_000;
/// Batch sizes a client plausibly sends: one reading, a small window, a bulk load.
const BATCH_SIZES: [usize; 3] = [1, 1_000, 10_000];

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

/// A client batch of `rows` readings, spread over a handful of series.
///
/// Several metrics rather than one, because the versioning index hashes
/// `(metric_id, timestamp)` and a single series would measure an unrealistically
/// friendly key distribution.
fn client_batch(rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(client_schema()),
        vec![
            Arc::new(TimestampMicrosecondArray::from(
                (0..rows)
                    .map(|row| AUGUST + (row as Micros / 8) * 1_000_000)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                (0..rows).map(|row| row as u64 % 8).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..rows).map(|row| row as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("client batch")
}

/// A stamped batch, as the memtable and the writer see one.
fn stamped(rows: usize) -> RecordBatch {
    Stamper::new(
        Arc::new(TestClock::new(AUGUST)),
        Arc::new(AtomicSequence::starting_at(0)),
    )
    .stamp(&client_batch(rows))
    .expect("stamp")
}

/// The versioning index: one pass over the three key columns (§1).
fn memtable_insert(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("memtable_insert");

    for rows in BATCH_SIZES {
        let batch = stamped(rows);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &batch,
            |bencher, batch| {
                bencher.iter_batched(
                    // A fresh table per iteration: inserting into a table that has
                    // grown all run would measure the index's size, not the insert.
                    || {
                        ActiveMemTable::new(storage_schema(), SuppressionConfig::for_stream())
                            .expect("memtable")
                    },
                    |table| table.insert(batch).expect("insert"),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Suppression on the hot path: the digest is computed for every row, whether or
/// not it turns out to be a repeat, so the repeat case is the one that shows what
/// the comparison itself costs.
fn memtable_insert_suppressed(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("memtable_insert_suppressed");

    for rows in BATCH_SIZES {
        let batch = stamped(rows);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &batch,
            |bencher, batch| {
                bencher.iter_batched(
                    || {
                        let table =
                            ActiveMemTable::new(storage_schema(), SuppressionConfig::for_stream())
                                .expect("memtable");
                        table.insert(batch).expect("seed");
                        table
                    },
                    |table| table.insert(batch).expect("insert"),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// The fsync an acknowledgement waits for (§4).
///
/// Measured at two commit delays, because that setting is the whole trade: zero
/// groups only what is already queued, and a delay buys throughput with
/// acknowledgement latency and nothing else.
fn wal_append(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("wal_append");

    for payload_bytes in [128usize, 8 * 1024] {
        let payload = vec![0u8; payload_bytes];
        group.throughput(Throughput::Bytes(payload_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_bytes),
            &payload,
            |bencher, payload| {
                let storage = Arc::new(InMemoryStorage::new());
                let wal = Wal::open(
                    storage,
                    std::path::PathBuf::from("wal"),
                    WalConfig {
                        segment_bytes: u64::MAX,
                        max_commit_delay: Duration::ZERO,
                    },
                )
                .expect("wal");
                let mut seq = 0u64;
                bencher.iter(|| {
                    seq += 1;
                    wal.append(RecordKind::Data, seq, payload.clone())
                        .expect("append")
                });
            },
        );
    }
    group.finish();
}

/// The flush path: sorting is already done, so this is encoding and compression.
fn parquet_write(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("parquet_write");

    for rows in [1_000usize, 10_000, 100_000] {
        // Sorted, because the writer refuses anything else (§1) and a flush
        // never hands it anything else.
        let batch = sorted(rows);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &batch,
            |bencher, batch| {
                bencher.iter_batched(
                    InMemoryStorage::new,
                    |storage| {
                        write_partition(
                            &storage,
                            std::path::Path::new("data/readings/year=2026/month=08"),
                            batch,
                            ParquetConfig::default(),
                        )
                        .expect("write")
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// A batch already in `(metric_id, timestamp, seq)` order.
fn sorted(rows: usize) -> RecordBatch {
    let metric_ids: Vec<u64> = (0..rows).map(|row| (row / 128) as u64).collect();
    let timestamps: Vec<Micros> = (0..rows)
        .map(|row| AUGUST + (row % 128) as Micros * 1_000_000)
        .collect();
    let seqs: Vec<u64> = (0..rows).map(|row| row as u64).collect();

    RecordBatch::try_new(
        storage_schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(UInt64Array::from(metric_ids)),
            Arc::new(Float64Array::from(
                (0..rows).map(|row| row as f64).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(vec![AUGUST; rows])),
            Arc::new(UInt64Array::from(seqs)),
        ],
    )
    .expect("sorted batch")
}

/// The whole write path in one call: schema, stamp, log, route (§4).
///
/// The sum is what a `do_put` costs per batch minus the network, and comparing
/// it against the three components above is how a regression is attributed.
fn archive_ingest(criterion: &mut Criterion) {
    use hetida_db::archive::{Archive, ArchiveConfig};

    let mut group = criterion.benchmark_group("archive_ingest");

    for rows in BATCH_SIZES {
        let batch = client_batch(rows);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(rows),
            &batch,
            |bencher, batch| {
                bencher.iter_batched(
                    || {
                        let storage = InMemoryStorage::new();
                        Archive::open(
                            Arc::new(storage) as Arc<dyn Storage>,
                            &ArchiveConfig {
                                wal: WalConfig {
                                    segment_bytes: u64::MAX,
                                    max_commit_delay: Duration::ZERO,
                                },
                                ..ArchiveConfig::default()
                            },
                            Arc::new(TestClock::new(AUGUST)),
                        )
                        .expect("archive")
                    },
                    |archive| archive.ingest("readings", batch).expect("ingest"),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    memtable_insert,
    memtable_insert_suppressed,
    wal_append,
    parquet_write,
    archive_ingest,
);
criterion_main!(benches);
