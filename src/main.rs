// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz
//
// hetida is a registered trademark of fuseki GmbH.

//! hetida-db — a high-performance time-series archiver.
//!
//! Ingests Arrow `RecordBatch` streams over Arrow Flight, makes them durable in
//! a write-ahead log, buffers them in an in-memory table, and archives them as
//! date-partitioned Parquet files that DataFusion queries alongside the
//! not-yet-flushed memory state.
//!
//! See `ARCHITECTURE.md` for the design decisions this implementation follows.

#![warn(missing_docs)]

/// Process entry point.
///
/// Will start the Arrow Flight server once the ingestion layer exists; for now
/// this is the cargo-generated placeholder (see Phase 1 in `tasks.md`).
fn main() {
    println!("Hello, world!");
}
