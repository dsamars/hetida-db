// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz
//
// hetida is a registered trademark of fuseki GmbH.

//! hetida-db — a high-performance bitemporal time-series archiver.
//!
//! Ingests Arrow `RecordBatch` streams over Arrow Flight, makes them durable in
//! a write-ahead log, buffers them in an in-memory table, and archives them as
//! partitioned Parquet files that DataFusion queries alongside the
//! not-yet-flushed memory state.
//!
//! Every version of a logical row is retained; a query resolves the newest one
//! at an optional observation point. See `ARCHITECTURE.md` for the design
//! decisions this implementation follows — code refers to them as §n.
//!
//! Implemented so far: the Phase 0 test foundation ([`clock`], [`seq`],
//! [`storage`]) and most of the Phase 1 data model — table schemas with their
//! evolution rules ([`schema`]) and the catalogue that applies them
//! ([`catalog`]), the metric registry ([`registry`]) backed by the
//! never-reclaimed catalogue log ([`catalog_log`]), the system-column stamper
//! ([`stamp`]), the versioned table with freeze-and-swap ([`memtable`],
//! [`active`]) and its suppression state ([`suppression`]), the write-ahead log
//! ([`wal`]), the partition layout ([`partition`]), the routing that places a
//! batch in it ([`table`]), the startup sequence that assembles all of it
//! ([`archive`]) and the Flight surface serving it ([`flight`]), with batches
//! made durable through the data log ([`data_log`]) and archived as Parquet
//! ([`parquet`]) by the flusher ([`flush`]), merged by the compactor
//! ([`compact`]) and expired by the reaper ([`retention`]), and with the whole
//! archive queryable through DataFusion ([`query`]) over a scan that spans
//! memory and Parquet ([`scan`], [`object_store`]).

#![warn(missing_docs)]
// The zero-panic policy, enforced mechanically. `panic`, `unreachable` and
// `todo` are on the list because an unreachable! in the ingest path is the same
// outage as an unwrap — a lesson from one slipping through review.
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable,
    clippy::todo
)]

pub mod active;
pub mod archive;
pub mod catalog;
pub mod catalog_log;
pub mod clock;
pub mod compact;
pub mod data_log;
pub mod flight;
pub mod flush;
pub mod instance;
pub mod memtable;
pub mod object_store;
pub mod parquet;
pub mod partition;

/// Control-plane message types, generated from `proto/hetida.proto` (§5).
///
/// These carry the payloads of Arrow Flight's opaque byte fields — an action
/// body, a `Ticket` — never time series data, which travels as Arrow
/// `RecordBatch` over the streams. The `.proto` file is the client contract, so
/// a client generates from it instead of reimplementing a byte layout.
#[allow(
    missing_docs,
    clippy::doc_markdown,
    clippy::derive_partial_eq_without_eq
)]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/hetida.v1.rs"));
}

pub mod query;
pub mod registry;
pub mod retention;
pub mod scan;
pub mod schema;
pub mod seq;
pub mod stamp;
pub mod storage;
pub mod suppression;
pub mod table;
pub mod wal;
