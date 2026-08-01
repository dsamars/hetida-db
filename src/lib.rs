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
//! [`storage`]) and the start of the Phase 1 data model — table schemas with
//! their evolution rules ([`schema`]) and the system-column stamper
//! ([`stamp`]).

#![warn(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

pub mod clock;
pub mod schema;
pub mod seq;
pub mod stamp;
pub mod storage;
