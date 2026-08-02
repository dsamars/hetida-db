<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# High-Performance Parquet Time-Series Archiver

## Core Commands
- Build: `cargo build --release`
- Fast Check: `cargo check`
- Test: `cargo test`
- Benchmarks: `cargo bench`
- Lint: `cargo clippy -- -D warnings`
- Format: `cargo fmt`

## Critical Performance & Quality Rules
- **No Per-Row Copies In Hot Path:** Ingestion (`DoPut`) and Querying (`DoGet`) must pass `RecordBatch` instances through unchanged — from Arrow IPC decode to Parquet writer and back. No conversion into Rust structs, no row-by-row iteration, no re-batching outside the flush path. Two explicit exceptions: buffer allocation during IPC decode is unavoidable; and appending the `system_time`/`seq` system columns is permitted, because the client's column buffers are moved by `Arc`, not copied — only the schema and the two new arrays are built.
- **Pure Arrow Flight Protocol:** Do not introduce pure Protobuf messaging for time-series payload data. Use Arrow Flight `FlightService` (`DoPut` for writes, `DoGet` for reads).
- **Zero Panic Policy:** No `unwrap()` or `expect()` in ingestion pipelines, WAL parsing, or Flight handlers. Enforce it mechanically in those modules: `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]`.
- **Non-Blocking Flush:** Ingestion must never wait on a flush. Freeze the active `MemTable` and open a new one; the frozen one is written in the background.
- **No Wall-Clock Ordering:** Ordering and version resolution use the monotonic `seq`, never wall-clock time — it can move backwards and makes WAL replay non-deterministic. `system_time` is a user-visible *stamp* for time-travel predicates, not an ordering source, and must be clamped so it never decreases as `seq` increases.
- **Versions Are Immutable:** Never overwrite, replace or drop a row version — not in the `MemTable`, not on flush, not during compaction. Time travel depends on the full history being present; resolution belongs to the read path alone. The sole write-path exception is retransmission suppression (byte-identical payload), and the sole delete path is retention (`ARCHITECTURE.md` §8).
- **Preserve Sort Order:** Every flushed and compacted file is ordered by `(metric_id, timestamp, seq)`. Version resolution is a streaming merge and silently degrades to a full sort if this is broken.
- **No Name Resolution On Ingest:** `do_put` accepts `u64` metric ids only. Clients resolve names and labels to ids once via the registration action and cache them; resolving server-side would be a per-row registry lookup in the hot path.
- **WAL Must Not Block Startup:** A torn tail record after a crash truncates the log at that point. Never fail to start on a damaged last record.
- **Explicit Rejection Over Unbounded Buffering:** Under overload, apply gRPC flow control and — past the hard memory cap — return `RESOURCE_EXHAUSTED`. Never buffer without bound, never block indefinitely.
- **Auth Off The Hot Path:** Validate the bearer token once per call in the interceptor, never per streamed message. One `do_put` stream carries many `RecordBatch` messages behind a single authorization.
- **Retention Deletes Whole Partitions:** Expired data is removed by dropping partition directories. Never rewrite a Parquet file to delete expired rows.
- **Partition Interval Is Immutable:** Never derive a partition path from a hardcoded granularity. The interval comes from instance configuration and is validated against the on-disk layout at startup; a mismatch is a startup error, never a silent second layout.
- **Unsafe Code:** `unsafe` is strictly forbidden unless required for SIMD/Arrow operations and must be heavily documented with a `# Safety` block.
- **Quality Gate:** `cargo test` and `cargo clippy -- -D warnings` MUST pass before completing tasks. No production code is written without a failing test that demanded it — see *Test-Driven Development*.
- **Document Every Item:** Every module, struct, enum, trait, function and method carries a doc comment (`//!` / `///`) stating what it does and any invariant the caller must uphold — short and substantive, not a restatement of the signature. Enforced by `#![warn(missing_docs)]`.
- **File Headers:** Every source file starts with the SPDX identifier and copyright notice (see *Legal*).
- **Concurrency:** Ensure all thread synchronization uses lock-free datastructures (`crossbeam`, `dashmap`, `atomic`) or asynchronous channels (`tokio::sync::mpsc`). Avoid coarse-grained `Mutex` in the ingestion path.

## Test-Driven Development

Development is strictly test-driven. This is not a style preference — it constrains the design, and the constraints below are binding.

- **Red-green-refactor, in that order.** Write a failing test, make it pass with the simplest code that does, then refactor. Never write production code that no failing test asked for. Every task in `tasks.md` begins with its tests.
- **Every decision in `ARCHITECTURE.md` needs a test that fails when it is violated.** That, not line coverage, is the measure of whether this codebase is tested. A test guarding a decision names it in a comment (`// §1: …`).
- **Injectable clock.** Wall-clock time is never read directly. A `Clock` trait supplies `system_time`; production uses the system clock, tests drive it explicitly — including backwards jumps, to exercise the monotonicity clamp of §1. Time travel is untestable otherwise.
- **Injectable sequence source.** `seq` comes from an injected generator so version ordering is deterministic under test.
- **Storage abstraction with fault injection.** WAL recovery is tested by injecting torn writes and I/O errors, not by killing processes. A crash test that cannot be reproduced deterministically is not a test.
- **Deterministic tests only.** No `sleep`, no wall-clock assertions, no fixed ports. Use `tokio::time::pause()` for time-dependent async paths and port 0 for servers. A flaky test is treated as a failing test.
- **Property-based tests (`proptest`) for the invariants**, because examples cannot cover them: version resolution at arbitrary observation points, sort order surviving flush and compaction, retransmission suppression being idempotent.
- **Test layout:** unit tests in `#[cfg(test)] mod tests` beside the code, cross-component tests in `tests/`. Test modules carry `#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic, clippy::unreachable)]` — the zero-panic policy applies to production code, and tests may panic freely. Production code denies all five, so an `unreachable!` in the ingest path fails the build like an `unwrap` does.
- **Benchmarks are not tests.** Phase 4 measures; correctness is settled long before it.

## Tech Stack
- **Protocol & Network:** Pure Apache Arrow Flight (`arrow-flight` crate over `tonic` gRPC transport)
- **Engine & Storage:** Rust 2024, Tokio Runtime, Apache Arrow (`arrow`), Apache Parquet (`parquet`)
- **Query Processing:** Apache DataFusion (`datafusion`)
- **Error Handling:** `thiserror` for library/domain logic, `anyhow` for startup/CLI.
- **Version Alignment:** `arrow`, `parquet`, `arrow-flight` and `datafusion` must resolve to the same Arrow major version, otherwise Arrow types conflict across crate boundaries. Pick the DataFusion version first and derive the rest from its Arrow dependency.

## Legal

- **Author:** Heiko Dietz
- **License:** MIT — `Copyright (c) 2026 Heiko Dietz`. Full text in `LICENSE`.
- **Trademark:** *hetida* is a registered trademark of fuseki GmbH. The MIT License covers the source code only and grants no trademark rights; forks must be renamed.
- Every new `.rs` file begins with:
  ```rust
  // SPDX-License-Identifier: MIT
  // Copyright (c) 2026 Heiko Dietz
  ```
  Markdown and TOML files carry the same two lines as `<!-- -->` / `#` comments.

## Design Decisions

Settled architectural decisions — bitemporal versioning and time travel, flush, compaction, WAL, Flight surface, schema evolution, backpressure, retention, auth — are recorded in `ARCHITECTURE.md` under *Design Decisions*. Read them before implementing; do not re-decide them ad hoc.

The archive is **bitemporal**: `timestamp` is valid time, `system_time`/`seq` is system time. Many versions of `(metric_id, timestamp)` coexist and a query resolves the newest one at an optional observation point. Anything that discards a version is a bug — see §1.