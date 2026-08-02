// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The hetida-db server binary.
//!
//! Opens the archive, then serves it over Arrow Flight until interrupted. The
//! startup order is [`Archive::open`]'s (§4, §11); anything wrong with the layout
//! or the logs surfaces here as a refusal to start rather than as a running
//! server with a damaged view of its own data.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

use hetida_db::archive::{Archive, ArchiveConfig};
use hetida_db::clock::SystemClock;
use hetida_db::compact::{CompactConfig, Compactor, partition_dirs};
use hetida_db::flight::HetidaFlightService;
use hetida_db::flush::{FlushConfig, FlushScheduler, Flusher};
use hetida_db::parquet::ParquetConfig;
use hetida_db::storage::FileStorage;

/// Where the archive lives, unless `HETIDA_DATA_DIR` says otherwise.
const DEFAULT_DATA_DIR: &str = "hetida-data";
/// Address to serve on, unless `HETIDA_LISTEN` says otherwise.
const DEFAULT_LISTEN: &str = "127.0.0.1:50051";
/// How often the compactor looks for due partitions (§3).
///
/// Far less often than the flusher: compaction rewrites a whole partition, where
/// a flush appends one file. Sharing the flusher's tick would either compact far
/// more often than needed or flush far later than the age bound allows.
const COMPACT_TICK: std::time::Duration = std::time::Duration::from_secs(300);
/// How often the flusher looks for due partitions (§2).
///
/// Far below the age bound it enforces, so a partition is flushed close to when
/// it becomes due rather than up to a whole tick late.
const FLUSH_TICK: std::time::Duration = std::time::Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_dir = std::env::var("HETIDA_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_owned());
    let listen = std::env::var("HETIDA_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_owned());

    let storage = Arc::new(FileStorage::new(PathBuf::from(&data_dir)));
    let storage_for_archive = Arc::clone(&storage);
    let config = ArchiveConfig::default();

    let archive = Arc::new(
        Archive::open(storage_for_archive, &config, Arc::new(SystemClock))
            .with_context(|| format!("could not open the archive in `{data_dir}`"))?,
    );

    let recovered = archive.recovered();
    println!(
        "hetida-db: opened `{data_dir}` with a {} layout; \
         recovered {} series, {} schemas, resuming seq at {}",
        archive.interval(),
        recovered.registrations,
        recovered.schemas,
        recovered.resumed_at,
    );
    if recovered.truncated {
        // §4: a torn tail never stops a start, but it is never silent either.
        println!("hetida-db: a log tail was discarded as torn");
    }
    if recovered.undecodable > 0 {
        println!(
            "hetida-db: {} catalogue record(s) could not be decoded and were skipped",
            recovered.undecodable
        );
    }
    if recovered.data_records > 0 {
        println!(
            "hetida-db: {} data record(s) are in the log but not yet replayed \
             into memory — batch replay arrives with do_put",
            recovered.data_records
        );
    }

    // §2: one scheduler per table, driving flushes in the background so
    // ingestion never waits for one. Tables appear as clients write to them, so
    // the set is rebuilt on each tick rather than fixed at startup.
    let flush_config = FlushConfig::default();
    let flushing = tokio::spawn({
        let archive = Arc::clone(&archive);
        let storage_for_flush = Arc::clone(&storage);
        async move {
            let mut timer = tokio::time::interval(FLUSH_TICK);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let clock: Arc<dyn hetida_db::clock::Clock> = Arc::new(SystemClock);

            loop {
                timer.tick().await;
                for table in archive.catalogue().table_names() {
                    let Some(state) = archive.table(&table) else {
                        continue;
                    };
                    let scheduler = FlushScheduler::new(
                        Flusher::new(
                            Arc::clone(&storage_for_flush) as Arc<_>,
                            table.clone(),
                            archive.interval(),
                            ParquetConfig::default(),
                        ),
                        flush_config,
                        Arc::clone(&clock),
                    );
                    let pass = scheduler.run_once(&state);
                    if pass.failed > 0 {
                        // §7: the rows stay, memory grows, and ingestion is
                        // refused before anything is lost. Silence here would
                        // make that chain unexplainable.
                        eprintln!(
                            "hetida-db: {} partition(s) of `{table}` failed to flush; \
                             ingestion will be refused if memory fills",
                            pass.failed
                        );
                    }
                }
            }
        }
    });

    let compacting = tokio::spawn({
        let archive = Arc::clone(&archive);
        let storage = Arc::clone(&storage);
        async move {
            let mut timer = tokio::time::interval(COMPACT_TICK);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let compactor = Compactor::new(
                Arc::clone(&storage) as Arc<_>,
                CompactConfig::default(),
                ParquetConfig::default(),
            );

            loop {
                timer.tick().await;
                for table in archive.catalogue().table_names() {
                    let dirs = match partition_dirs(storage.as_ref(), &table, archive.interval()) {
                        Ok(dirs) => dirs,
                        Err(error) => {
                            eprintln!("hetida-db: could not list `{table}` partitions: {error}");
                            continue;
                        }
                    };
                    for dir in dirs {
                        match compactor.compact_if_due(&dir) {
                            Ok(Some(done)) => println!(
                                "hetida-db: compacted {} into {} file(s), {} rows",
                                dir.display(),
                                done.written.len(),
                                done.rows
                            ),
                            Ok(None) => {}
                            // A failed compaction changes nothing on disk: the
                            // inputs are removed only once the outputs are
                            // durable, so the partition is simply retried.
                            Err(error) => eprintln!(
                                "hetida-db: compacting {} failed, will retry: {error}",
                                dir.display()
                            ),
                        }
                    }
                }
            }
        }
    });

    let address = listen
        .parse()
        .with_context(|| format!("`{listen}` is not a socket address"))?;
    println!("hetida-db: serving Arrow Flight on {address}");

    Server::builder()
        .add_service(FlightServiceServer::new(HetidaFlightService::new(archive)))
        .serve_with_shutdown(address, shutdown())
        .await
        .context("the Flight server stopped with an error")?;

    flushing.abort();
    compacting.abort();
    println!("hetida-db: stopped");
    Ok(())
}

/// Resolves when the process is asked to stop.
///
/// Returning rather than aborting lets the server drain in flight calls, and
/// dropping the archive afterwards joins the log writers — so the last group
/// reaches the device before the process exits (§4).
async fn shutdown() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("hetida-db: could not listen for shutdown signals: {error}");
    }
}
