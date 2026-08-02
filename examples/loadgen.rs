// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Load harness: `do_put` throughput and `do_get` latency under concurrency.
//!
//! Deliberately **not** `criterion`. Criterion's estimates assume a
//! distribution that network and scheduler jitter do not produce, and it reports
//! a mean with a confidence interval where what matters here is the tail — the
//! p99 a client actually waits for. This reports order statistics from the raw
//! samples instead, and says how many samples they came from.
//!
//! It is also not a test. Nothing here asserts; it measures, prints, and leaves
//! the judgement to whoever ran it.
//!
//! # What it does
//!
//! Starts a real server in-process on an ephemeral port, then drives it over the
//! loopback with real Arrow Flight clients — every batch goes through gRPC, the
//! IPC decoder, the schema check, the stamper, the WAL fsync and the memtable,
//! and every acknowledgement means the rows are durable (§4). The flusher runs
//! alongside, so the measurement includes an archive shedding memory rather than
//! one quietly filling up.
//!
//! With `HETIDA_LOAD_READERS` above zero it also runs queries against the same
//! archive while the writers are going, which is the number §5 is really about:
//! read latency under write load, not on an idle server.
//!
//! # Configuration
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `HETIDA_LOAD_CLIENTS` | 100 | Parallel Flight clients writing |
//! | `HETIDA_LOAD_BATCHES` | 200 | Batches each client sends |
//! | `HETIDA_LOAD_ROWS` | 500 | Rows per batch |
//! | `HETIDA_LOAD_READERS` | 4 | Parallel clients querying while writes run |
//! | `HETIDA_LOAD_DIR` | a temp dir | Where the archive is written |
//!
//! The defaults write ten million rows through a hundred clients. Point
//! `HETIDA_LOAD_DIR` at the device under test, or leave it unset to measure
//! against a temporary directory.
//!
//! ```sh
//! cargo run --release --example loadgen
//! HETIDA_LOAD_CLIENTS=8 HETIDA_LOAD_BATCHES=20 cargo run --release --example loadgen
//! ```
//!
//! An example rather than a binary: it needs `tempfile` and `tokio-stream`,
//! which are development dependencies, and a plain `cargo build` has no reason
//! to compile a measurement tool.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::array::{Float64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::{FlightData, FlightDescriptor, Ticket};
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message;
use tokio::net::TcpListener;
use tonic::transport::Server;

use hetida_db::archive::{Archive, ArchiveConfig};
use hetida_db::clock::{Micros, SystemClock};
use hetida_db::flight::HetidaFlightService;
use hetida_db::flush::{FlushConfig, FlushScheduler, Flusher};
use hetida_db::parquet::ParquetConfig;
use hetida_db::proto::Query;
use hetida_db::schema::{METRIC_ID_COLUMN, TIMESTAMP_COLUMN, timestamp_type};
use hetida_db::storage::FileStorage;

/// 2026-08-01T00:00:00Z — a partition boundary.
const BASE: Micros = 1_785_542_400_000_000;
/// How often the flusher looks for due partitions during a run.
const FLUSH_TICK: Duration = Duration::from_secs(1);

/// What a run was asked to do.
#[derive(Debug, Clone, Copy)]
struct Load {
    clients: usize,
    batches: usize,
    rows: usize,
    readers: usize,
}

impl Load {
    fn from_env() -> Self {
        Self {
            clients: number("HETIDA_LOAD_CLIENTS", 100),
            batches: number("HETIDA_LOAD_BATCHES", 200),
            rows: number("HETIDA_LOAD_ROWS", 500),
            readers: number("HETIDA_LOAD_READERS", 4),
        }
    }

    fn total_rows(&self) -> u64 {
        (self.clients * self.batches * self.rows) as u64
    }
}

fn number(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(fallback)
}

#[tokio::main]
async fn main() -> Result<()> {
    let load = Load::from_env();
    let (directory, _guard) = data_dir()?;

    println!(
        "hetida-loadgen: {} clients × {} batches × {} rows = {} rows, {} reader(s)",
        load.clients,
        load.batches,
        load.rows,
        load.total_rows(),
        load.readers,
    );
    println!("hetida-loadgen: archive in {}", directory.display());

    let storage = Arc::new(FileStorage::new(directory));
    let archive = Arc::new(
        Archive::open(
            Arc::clone(&storage) as Arc<_>,
            &ArchiveConfig::default(),
            Arc::new(SystemClock),
        )
        .context("could not open the archive")?,
    );

    let address = serve(Arc::clone(&archive), Arc::clone(&storage)).await?;
    let flushing = spawn_flusher(Arc::clone(&archive), Arc::clone(&storage));

    // Readers start first so they are already querying when the writes begin;
    // an idle-server read latency is not the number §5 is about.
    let reading = spawn_readers(address, load.readers);

    let started = Instant::now();
    let write_latencies = drive_writers(address, load).await?;
    let elapsed = started.elapsed();

    let read_latencies = reading.finish().await;
    flushing.abort();

    report_writes(load, &write_latencies, elapsed);
    report_reads(&read_latencies);
    report_archive(&archive);

    Ok(())
}

/// The directory to write into, and a guard keeping a temporary one alive.
fn data_dir() -> Result<(PathBuf, Option<tempfile::TempDir>)> {
    match std::env::var("HETIDA_LOAD_DIR") {
        Ok(configured) => Ok((PathBuf::from(configured), None)),
        Err(_) => {
            let temporary = tempfile::tempdir().context("could not create a temporary archive")?;
            Ok((temporary.path().to_path_buf(), Some(temporary)))
        }
    }
}

/// Serves `archive` on an ephemeral port and returns the address.
///
/// Without authorization: this measures the archive, and a token check once per
/// call would be measured as a constant that tells nobody anything (§9).
async fn serve(archive: Arc<Archive>, storage: Arc<FileStorage>) -> Result<std::net::SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("could not bind a loopback port")?;
    let address = listener.local_addr().context("no local address")?;
    let service = HetidaFlightService::new(archive, storage as Arc<_>);

    tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .ok();
    });

    Ok(address)
}

/// Runs the flusher for the duration of the run, so memory is actually shed.
fn spawn_flusher(archive: Arc<Archive>, storage: Arc<FileStorage>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(FLUSH_TICK);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let clock: Arc<dyn hetida_db::clock::Clock> = Arc::new(SystemClock);

        loop {
            timer.tick().await;
            for table in archive.catalogue().table_names() {
                let Some(state) = archive.table(&table) else {
                    continue;
                };
                FlushScheduler::new(
                    Flusher::new(
                        Arc::clone(&storage) as Arc<_>,
                        table,
                        archive.interval(),
                        ParquetConfig::default(),
                    ),
                    FlushConfig::default(),
                    Arc::clone(&clock),
                )
                .run_once(&state);
            }
        }
    })
}

/// Readers querying while the writers run, until told to stop.
struct Readers {
    stop: Arc<std::sync::atomic::AtomicBool>,
    handles: Vec<tokio::task::JoinHandle<Vec<Duration>>>,
}

impl Readers {
    async fn finish(self) -> Vec<Duration> {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut all = Vec::new();
        for handle in self.handles {
            if let Ok(latencies) = handle.await {
                all.extend(latencies);
            }
        }
        all
    }
}

fn spawn_readers(address: std::net::SocketAddr, readers: usize) -> Readers {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handles = (0..readers)
        .map(|_| {
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let mut latencies = Vec::new();
                let Ok(mut client) = connect(address).await else {
                    return latencies;
                };

                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let ticket = Ticket {
                        ticket: Query {
                            // A narrow, realistic read: a few series over a
                            // window, which is the shape §11 sizes the layout
                            // for. `LIMIT` keeps a reader from measuring the
                            // whole archive as it grows.
                            sql: "SELECT * FROM readings \
                                  WHERE metric_id IN (1, 2, 3) LIMIT 1000"
                                .to_owned(),
                            as_of_micros: None,
                        }
                        .encode_to_vec()
                        .into(),
                    };

                    let started = Instant::now();
                    let outcome = async {
                        let incoming = client.do_get(ticket).await.ok()?.into_inner();
                        let mut rows = 0usize;
                        let mut stream =
                            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                                incoming.map_err(arrow_flight::error::FlightError::from),
                            );
                        while let Some(Ok(batch)) = stream.next().await {
                            rows += batch.num_rows();
                        }
                        Some(rows)
                    }
                    .await;

                    // A query against a table no client has written to yet is
                    // not a measurement; skip it rather than record a failure
                    // as a fast read.
                    if outcome.is_some() {
                        latencies.push(started.elapsed());
                    } else {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                latencies
            })
        })
        .collect();

    Readers { stop, handles }
}

/// Drives every writer to completion and returns one latency per batch.
///
/// A latency is measured from sending a batch to its acknowledgement coming
/// back, which is what "durable" costs a client (§4).
async fn drive_writers(address: std::net::SocketAddr, load: Load) -> Result<Vec<Duration>> {
    let writers = (0..load.clients).map(|client| {
        tokio::spawn(async move {
            let mut connection = connect(address).await.ok()?;
            let mut latencies = Vec::with_capacity(load.batches);

            for batch_index in 0..load.batches {
                let batch = batch_for(client, batch_index, load.rows);
                let encoded: Vec<FlightData> =
                    arrow_flight::encode::FlightDataEncoderBuilder::new()
                        .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![
                            "readings".to_owned(),
                        ])))
                        .build(stream::iter(vec![Ok(batch)]))
                        .try_collect()
                        .await
                        .ok()?;

                let started = Instant::now();
                let mut acks = connection
                    .do_put(stream::iter(encoded))
                    .await
                    .ok()?
                    .into_inner();
                while acks.try_next().await.ok()?.is_some() {}
                latencies.push(started.elapsed());
            }
            Some(latencies)
        })
    });

    let mut all = Vec::new();
    for handle in writers.collect::<Vec<_>>() {
        if let Ok(Some(latencies)) = handle.await {
            all.extend(latencies);
        }
    }
    Ok(all)
}

async fn connect(
    address: std::net::SocketAddr,
) -> Result<FlightServiceClient<tonic::transport::Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .context("endpoint")?
        .connect()
        .await
        .context("could not connect to the server")?;
    Ok(FlightServiceClient::new(channel))
}

/// One client's batch. Each client owns a disjoint band of `metric_id`, so the
/// run measures concurrency rather than contention on one logical row.
fn batch_for(client: usize, batch_index: usize, rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP_COLUMN, timestamp_type(), false),
        Field::new(METRIC_ID_COLUMN, DataType::UInt64, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let first = (batch_index * rows) as Micros;

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(
                (0..rows)
                    .map(|row| BASE + (first + row as Micros) * 1_000_000)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(vec![client as u64; rows])),
            Arc::new(Float64Array::from(
                (0..rows).map(|row| row as f64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap_or_else(|error| panic!("a generated batch must be well formed: {error}"))
}

// ---- reporting ----

/// Order statistics from the raw samples.
///
/// Percentiles rather than a mean with a confidence interval: under
/// concurrency the distribution has a tail, and the tail is what a client waits
/// for. A mean hides exactly the thing worth reporting.
fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let last = sorted.len() - 1;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let index = ((last as f64) * fraction).round() as usize;
    sorted.get(index.min(last)).copied().unwrap_or_default()
}

fn report_writes(load: Load, latencies: &[Duration], elapsed: Duration) {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();

    let rows = load.total_rows();
    let seconds = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);

    println!("\n  do_put — {} batches acknowledged", sorted.len());
    println!("  wall clock          {elapsed:.2?}");
    println!("  throughput          {:.0} rows/s", rows as f64 / seconds);
    println!(
        "  batch rate          {:.0} batches/s",
        sorted.len() as f64 / seconds
    );
    println!("  latency p50         {:.2?}", percentile(&sorted, 0.50));
    println!("  latency p95         {:.2?}", percentile(&sorted, 0.95));
    println!("  latency p99         {:.2?}", percentile(&sorted, 0.99));
    println!("  latency max         {:.2?}", percentile(&sorted, 1.0));

    let missing = load.clients * load.batches - sorted.len();
    if missing > 0 {
        println!("  NOT ACKNOWLEDGED    {missing} batches — the run is not comparable");
    }
}

fn report_reads(latencies: &[Duration]) {
    if latencies.is_empty() {
        println!("\n  do_get — no readers ran");
        return;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();

    println!("\n  do_get under write load — {} queries", sorted.len());
    println!("  latency p50         {:.2?}", percentile(&sorted, 0.50));
    println!("  latency p95         {:.2?}", percentile(&sorted, 0.95));
    println!("  latency p99         {:.2?}", percentile(&sorted, 0.99));
    println!("  latency max         {:.2?}", percentile(&sorted, 1.0));
}

fn report_archive(archive: &Archive) {
    println!("\n  archive after the run");
    println!("  bytes held in memory {}", archive.memtable_bytes());
    if let Some(state) = archive.table("readings") {
        println!("  partitions held      {}", state.partition_starts().len());
    }
}
