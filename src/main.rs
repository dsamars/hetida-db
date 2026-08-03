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
use hetida_db::cli;
use hetida_db::clock::{Clock, SystemClock};
use hetida_db::compact::{Compactor, partition_dirs};
use hetida_db::config::{CONFIG_VAR, FileConfig};
use hetida_db::flight::BearerInterceptor;
use hetida_db::flight::HetidaFlightService;
use hetida_db::flush::{FlushScheduler, Flusher};
use hetida_db::retention::Reaper;
use hetida_db::serve::{AuthConfig, Warning, readable_beyond_owner, retired_in_use, tls_config};
use hetida_db::storage::FileStorage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Answered before anything else: neither needs a configuration, and asking
    // which build this is should work on a host where the archive will not open.
    // These print to stdout rather than through `tracing` — they are the
    // program's answer, not a diagnostic, and there is no subscriber yet (§13).
    match cli::parse(std::env::args().skip(1)) {
        cli::Request::Serve => {}
        cli::Request::Version => {
            println!("hetida-db {}", cli::version());
            return Ok(());
        }
        cli::Request::Help => {
            println!("{}", cli::usage());
            return Ok(());
        }
        cli::Request::Unknown(argument) => {
            anyhow::bail!(
                "`{argument}` is not an option this build knows; \
                 try `--help`. Everything except `--version` and `--help` is \
                 configured in the file {CONFIG_VAR} names"
            );
        }
    }

    // §9: a variable that used to work and is now ignored would deploy a server
    // listening somewhere else, or with its credentials silently dropped. Say so
    // and stop, rather than start something the operator did not ask for.
    let retired = retired_in_use(|name| std::env::var(name).ok());
    if !retired.is_empty() {
        for line in &retired {
            // §13's one exception: the subscriber is configured by the file this
            // check runs *before* reading, so there is nowhere else for this to
            // go. Everything past `install` speaks through `tracing`.
            eprintln!("hetida-db: {line}");
        }
        anyhow::bail!(
            "{} retired environment variable(s) are still set; \
             everything moved into the file {CONFIG_VAR} names",
            retired.len()
        );
    }

    // One place to configure, so there are no precedence rules.
    let file = FileConfig::resolve(|name| std::env::var(name).ok())
        .context("the configuration could not be used")?;

    // §13: everything after this point speaks through `tracing`. Before it there
    // is no subscriber, which is why the two failures above still print.
    let observability = file.observability();
    hetida_db::observability::install(&observability)
        .map_err(|error| anyhow::anyhow!("the log subscriber could not be installed: {error}"))?;

    let data_dir = file.data_dir().display().to_string();
    let listen = file.listen().to_owned();
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
    tracing::info!(
        data_dir = %data_dir,
        layout = %archive.interval(),
        series = recovered.registrations,
        schemas = recovered.schemas,
        resumed_at = recovered.resumed_at,
        "archive opened"
    );
    if recovered.truncated {
        // §4: a torn tail never stops a start, but it is never silent either.
        tracing::warn!("a log tail was discarded as torn");
    }
    if recovered.undecodable > 0 {
        tracing::warn!(
            records = recovered.undecodable,
            "catalogue records could not be decoded and were skipped"
        );
    }
    if recovered.data_records > 0 {
        tracing::info!(
            records = recovered.data_records,
            "data records replayed from the log"
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
                        tracing::warn!(
                            table = %table,
                            failed = pass.failed,
                            "partitions failed to flush; ingestion will be \
                             refused if memory fills (§7)"
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
                            tracing::warn!(
                                table = %table,
                                error = %error,
                                "could not list partitions"
                            );
                            continue;
                        }
                    };
                    for dir in dirs {
                        match compactor.compact_if_due(&dir) {
                            Ok(Some(done)) => tracing::info!(
                                partition = %dir.display(),
                                files = done.written.len(),
                                rows = done.rows,
                                "partition compacted"
                            ),
                            Ok(None) => {}
                            // A failed compaction changes nothing on disk: the
                            // inputs are removed only once the outputs are
                            // durable, so the partition is simply retried.
                            Err(error) => tracing::warn!(
                                partition = %dir.display(),
                                error = %error,
                                "compaction failed, will retry"
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
                                tracing::info!(
                                    table = %table,
                                    partition = %directory.display(),
                                    "expired partition dropped"
                                );
                            }
                            if pass.failed > 0 {
                                // Not fatal — the next pass retries — but an
                                // archive that cannot shed data grows without
                                // bound, so it is never silent.
                                tracing::warn!(
                                    table = %table,
                                    failed = pass.failed,
                                    "expired partitions could not be dropped; \
                                     will retry"
                                );
                            }
                            if pass.unrecognised > 0 {
                                tracing::warn!(
                                    table = %table,
                                    directories = pass.unrecognised,
                                    layout = %archive.interval(),
                                    "directories do not match the layout and \
                                     were left alone"
                                );
                            }
                        }
                        Err(error) => {
                            tracing::warn!(table = %table, error = %error, "could not reap");
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
    let transport = file.transport().context("the listener is not configured")?;
    let auth = file.auth().context("authorization is not configured")?;

    // A configuration holding literal passwords that anyone on the host can read
    // is worth saying out loud, but not worth refusing over — see `serve`.
    if AuthConfig::holds_literal_secrets(&file.users())
        && let Some(path) = std::env::var(CONFIG_VAR).ok().map(PathBuf::from)
        && let Some(mode) = readable_beyond_owner(&path)
    {
        tracing::warn!("{}", Warning::WorldReadableSecrets { path, mode });
    }

    let mut server = Server::builder();
    match tls_config(&transport).context("the TLS identity could not be loaded")? {
        Some(tls) => {
            server = server
                .tls_config(tls)
                .context("the TLS identity was refused")?;
            tracing::info!(%address, tls = true, "serving Arrow Flight");
        }
        None => {
            tracing::info!(%address, tls = false, "serving Arrow Flight");
            tracing::warn!(
                "no TLS — a bearer token travels in a header, and anyone on \
                 the path can read and replay it (§9)"
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
            tracing::warn!(
                "no authorization — every caller may read and write every \
                 table (§9)"
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
    tracing::info!("stopped");
    Ok(())
}

/// Resolves when the process is asked to stop.
///
/// Returning rather than aborting lets the server drain in flight calls, and
/// dropping the archive afterwards joins the log writers — so the last group
/// reaches the device before the process exits (§4).
async fn shutdown() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "could not listen for shutdown signals");
    }
}
