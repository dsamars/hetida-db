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
use hetida_db::flight::HetidaFlightService;
use hetida_db::storage::FileStorage;

/// Where the archive lives, unless `HETIDA_DATA_DIR` says otherwise.
const DEFAULT_DATA_DIR: &str = "hetida-data";
/// Address to serve on, unless `HETIDA_LISTEN` says otherwise.
const DEFAULT_LISTEN: &str = "127.0.0.1:50051";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_dir = std::env::var("HETIDA_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_owned());
    let listen = std::env::var("HETIDA_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_owned());

    let storage = Arc::new(FileStorage::new(PathBuf::from(&data_dir)));
    let config = ArchiveConfig::default();

    let archive = Arc::new(
        Archive::open(storage, &config, Arc::new(SystemClock))
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

    let address = listen
        .parse()
        .with_context(|| format!("`{listen}` is not a socket address"))?;
    println!("hetida-db: serving Arrow Flight on {address}");

    Server::builder()
        .add_service(FlightServiceServer::new(HetidaFlightService::new(archive)))
        .serve_with_shutdown(address, shutdown())
        .await
        .context("the Flight server stopped with an error")?;

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
