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
//! This module tree currently holds the Phase 0 test foundation: the injection
//! points that make time, ordering and storage faults controllable in tests.

#![warn(missing_docs)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

pub mod clock;
pub mod seq;
pub mod storage;
