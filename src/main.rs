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

use hetida_db::archive::Archive;
use hetida_db::auth::{DEFAULT_TOKEN_TTL, SystemTokens, TokenIssuer};
use hetida_db::clock::{Clock, SystemClock};
use hetida_db::compact::{Compactor, partition_dirs};
use hetida_db::config::FileConfig;
use hetida_db::flight::BearerInterceptor;
use hetida_db::flight::HetidaFlightService;
use hetida_db::flush::{FlushScheduler, Flusher};
use hetida_db::retention::Reaper;
use hetida_db::serve::{AuthConfig, TransportConfig};
use hetida_db::storage::FileStorage;

/// Where the archive lives, unless `HETIDA_DATA_DIR` says otherwise.
const DEFAULT_DATA_DIR: &str = "hetida-data";
/// Address to serve on, unless `HETIDA_LISTEN` says otherwise.
const DEFAULT_LISTEN: &str = "127.0.0.1:50051";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_dir = std::env::var("HETIDA_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_owned());
    let listen = std::env::var("HETIDA_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_owned());

    // How the archive behaves comes from the file; where it is deployed and how
    // it is secured comes from the environment. Nothing is settable in both, so
    // there is never a question of which one won.
    let file = FileConfig::resolve(|name| std::env::var(name).ok())
        .context("the archive configuration could not be used")?;
    let config = file.archive();
    let parquet = file.parquet();
    let ticks = file.ticks();

    let storage = Arc::new(FileStorage::new(PathBuf::from(&data_dir)));
    let storage_for_archive = Arc::clone(&storage);

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
    let flush_config = file.flush();
    let flushing = tokio::spawn({
        let archive = Arc::clone(&archive);
        let storage_for_flush = Arc::clone(&storage);
        async move {
            let mut timer = tokio::time::interval(ticks.flush);
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
                            parquet,
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

    let compact_config = file.compaction();
    let compacting = tokio::spawn({
        let archive = Arc::clone(&archive);
        let storage = Arc::clone(&storage);
        async move {
            let mut timer = tokio::time::interval(ticks.compaction);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let compactor = Compactor::new(Arc::clone(&storage) as Arc<_>, compact_config, parquet);

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

    // §8: expired data is removed by dropping whole partition directories, by
    // event time. A table with no configured retention is never examined, so an
    // archive nobody gave a horizon can never lose anything to this task.
    let reaping = tokio::spawn({
        let archive = Arc::clone(&archive);
        let storage = Arc::clone(&storage);
        async move {
            let mut timer = tokio::time::interval(ticks.retention);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let reaper = Reaper::new(Arc::clone(&storage) as Arc<_>, archive.interval());
            let clock = SystemClock;

            loop {
                timer.tick().await;
                let now = clock.now_micros();
                for table in archive.catalogue().table_names() {
                    let Some(retention) = archive.catalogue().config(&table).retention else {
                        continue;
                    };
                    match reaper.reap(&table, Some(retention), now) {
                        Ok(pass) => {
                            for directory in &pass.dropped {
                                println!(
                                    "hetida-db: dropped expired partition {}",
                                    directory.display()
                                );
                            }
                            if pass.failed > 0 {
                                // Not fatal — the next pass retries — but an
                                // archive that cannot shed data grows without
                                // bound, so it is never silent.
                                eprintln!(
                                    "hetida-db: {} expired partition(s) of `{table}` \
                                     could not be dropped; will retry",
                                    pass.failed
                                );
                            }
                            if pass.unrecognised > 0 {
                                eprintln!(
                                    "hetida-db: {} directory/ies under `{table}` do not \
                                     match the {} layout and were left alone",
                                    pass.unrecognised,
                                    archive.interval(),
                                );
                            }
                        }
                        Err(error) => {
                            eprintln!("hetida-db: could not reap `{table}`: {error}");
                        }
                    }
                }
            }
        }
    });

    let address = listen
        .parse()
        .with_context(|| format!("`{listen}` is not a socket address"))?;

    // §9: neither of these guesses. Serving in the clear, or serving anyone at
    // all, is a decision an operator has to write down.
    let transport = TransportConfig::resolve(|name| std::env::var(name).ok())
        .context("the listener is not configured")?;
    let auth = AuthConfig::resolve(|name| std::env::var(name).ok())
        .context("authorization is not configured")?;

    let mut server = Server::builder();
    match &transport {
        TransportConfig::Tls { certificate, key } => {
            let identity = tonic::transport::Identity::from_pem(
                std::fs::read(certificate)
                    .with_context(|| format!("could not read {}", certificate.display()))?,
                std::fs::read(key).with_context(|| format!("could not read {}", key.display()))?,
            );
            server = server
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                .context("the TLS identity was refused")?;
            println!("hetida-db: serving Arrow Flight over TLS on {address}");
        }
        TransportConfig::Plaintext => {
            println!("hetida-db: serving Arrow Flight on {address}");
            eprintln!(
                "hetida-db: no TLS — a bearer token travels in a header, and \
                 anyone on the path can read and replay it (§9)"
            );
        }
    }

    let serving = match auth {
        AuthConfig::Users(credentials) => {
            let issuer = Arc::new(TokenIssuer::new(
                credentials,
                Arc::new(SystemTokens),
                Arc::new(SystemClock),
                DEFAULT_TOKEN_TTL,
            ));
            let service = HetidaFlightService::with_auth(
                Arc::clone(&archive),
                Arc::clone(&storage) as Arc<_>,
                Arc::clone(&issuer),
            );
            server
                .add_service(FlightServiceServer::with_interceptor(
                    service,
                    BearerInterceptor::new(issuer),
                ))
                .serve_with_shutdown(address, shutdown())
                .await
        }
        AuthConfig::Anonymous => {
            eprintln!(
                "hetida-db: no authorization — every caller may read and write \
                 every table (§9)"
            );
            server
                .add_service(FlightServiceServer::new(HetidaFlightService::new(
                    Arc::clone(&archive),
                    Arc::clone(&storage) as Arc<_>,
                )))
                .serve_with_shutdown(address, shutdown())
                .await
        }
    };
    serving.context("the Flight server stopped with an error")?;

    flushing.abort();
    compacting.abort();
    reaping.abort();
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
