<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Implementation Plan

Design decisions referenced below (§n) are recorded in `ARCHITECTURE.md`.

Every task is executed test-first: its tests are written and failing before any production code, per *Test-Driven Development* in `CLAUDE.md`. The verification tasks in Phase 4 are additional system-level checks, not the primary testing.

## Phase 0: Test Foundation
- [x] Establish the test layout: unit tests in `#[cfg(test)] mod tests` beside the code, cross-component tests in `tests/`, test modules allowing `unwrap`/`expect` so the zero-panic lints stay strict in production code.
- [x] Introduce an injectable `Clock` so `system_time` can be driven explicitly in tests, including backwards jumps that exercise the monotonicity clamp (§1).
- [x] Introduce an injectable sequence source so `seq` ordering is deterministic under test (§1).
- [x] Introduce a storage abstraction able to inject torn writes and I/O errors, so WAL recovery is tested deterministically instead of by killing processes (§4).
- [x] Add `proptest` and set up the invariant suite in `tests/invariants.rs`, with the properties the Phase 0 foundation supports.
- [ ] Carried to Phase 1 — suppression idempotence is now a property in `tests/invariants.rs`; the remaining two §1 invariants describe components that do not exist yet: version resolution at arbitrary observation points, and sort order surviving flush and compaction.

## Phase 1: Data Model & Ingestion Engine
- [x] Define the Arrow schema model (`timestamp`, `metric_id` as `u64`, attributes as real typed payload columns) with evolution rules: nullable column added = accept, column missing = null, type change = reject. Reject client schemas supplying the reserved `system_time`/`seq` columns (§6, §10).
- [ ] Build the multi-table catalogue mapping table name to its `TableSchema` and its per-table configuration — suppression window and entry cap (§1), retention duration (§8) — applying evolution on ingest and surfacing rejections as explicit Flight errors (§6).
- [x] Build the metric registry mapping name + label set to a `u64` id, idempotent on re-registration, durable before visible through a `RegistrationLog` seam (§10).
- [ ] Back the registry's `RegistrationLog` with its own never-reclaimed log, replayed before the data log, and expose the registry as a queryable catalog table (§4, §10).
- [ ] Implement the Flight registration action returning ids for a name and label set, so clients resolve once and cache (§10).
- [x] Implement the system-column stamper appending `system_time` and `seq` on ingest, with `system_time` clamped so it never decreases as `seq` grows (§1).
- [x] Implement lock-free concurrent versioned `MemTable`: `(metric_id, timestamp)` identifies a logical row, many versions coexist, nothing is overwritten (§1).
- [x] Implement unchanged-value suppression: discard an incoming version whose payload digest matches the current latest version of the same logical row, over a per-table ingest-time window with a hard entry cap that survives freezes and is measured from the last touch (§1, §6).
- [ ] Implement partition routing: split each incoming `RecordBatch` by the configured interval at insert time, since one batch may span interval boundaries (§3, §11).
- [x] Implement freeze-and-swap so a flush never blocks ingestion: freeze the active `MemTable`, open a new one, hand the frozen one to the background writer (§2).
- [x] Build append-only WAL with group commit (shared fsync, configurable max delay), per-record CRC, segment rotation, and checkpoint records for segment reclamation (§4).
- [x] Implement WAL replay: apply segments in sequence order, truncate at the first torn record, never fail startup on a damaged tail (§4).
- [ ] Implement Arrow Flight `do_put` streaming endpoint ingesting `RecordBatch` payload directly: assign sequence numbers, resolve the target table from `FlightDescriptor::path`, acknowledge only after fsync (§4, §5).
- [ ] Implement backpressure: bounded queues with gRPC flow control, plus `RESOURCE_EXHAUSTED` past the hard `MemTable` memory cap (§7).

## Phase 2: Parquet Storage & Archiver
- [ ] Implement Parquet writer with ZSTD compression and explicit Min/Max column index generation, including `system_time` so old observation points can skip whole row groups (§1).
- [ ] Implement the parameterised Hive-style partitioner for the `hour`/`day`/`month`/`year` intervals, taking the interval as an argument rather than a global (§11).
- [ ] Persist the partition interval in the instance metadata at initialisation and validate it against the on-disk layout at startup, refusing to start on a mismatch (§11).
- [ ] Build background flusher transferring frozen `MemTable`s to Parquet, triggered per partition by size cap or max age — whichever fires first — writing `part-<seq>.parquet` sorted by `(metric_id, timestamp, seq)` with all versions preserved (§1, §2, §3).
- [ ] Build background compactor merging and re-sorting a partition's files into `(metric_id, timestamp, seq)` order once file count or total size crosses a threshold, emitting files of a configurable target size with disjoint `(metric_id, timestamp)` ranges. It must never drop a version (§1, §3, §11).
- [ ] Implement per-table retention configuration and a background reaper dropping whole partition directories once their interval is entirely past the cutoff, by event time (§8).
- [ ] Reject ingestion of data already outside the retention window in `do_put` with an explicit Flight error (§8).

## Phase 3: High-Performance Query Engine & Arrow Flight Output
- [ ] Build DataFusion execution pipeline combining `MemTable` memory state and `Parquet` disk storage.
- [ ] Implement as-of version resolution: filter `system_time <= observation point`, then take the highest `seq` per `(metric_id, timestamp)` as a streaming merge over the sorted files — not a full-sort window function (§1).
- [ ] Expose each table as an as-of-resolved view plus `<table>$history` carrying the raw versions with `system_time` and `seq` as ordinary columns (§1, §5).
- [ ] Implement predicate pushdown and column projection for Parquet readers, including partition pruning via the Hive-style layout and row-group skipping on `system_time` and `metric_id` (§11).
- [ ] Resolve label predicates against the registry into a `metric_id` set and push that down as a filter on `metric_id` (§10).
- [ ] Implement `get_flight_info` and `get_schema` so generic Flight clients can follow the standard `get_flight_info` → `do_get` flow, accepting the same `{sql, as_of}` payload in the descriptor `cmd` (§5).
- [ ] Implement Arrow Flight `do_get` endpoint: parse `{sql, as_of}` from the `Ticket`, run it through DataFusion and stream results directly to clients via Arrow IPC (§5).
- [ ] Implement TLS termination and Flight `Handshake` issuing bearer tokens, validated by a `tonic` interceptor once per call — never per streamed message (§9).
- [ ] Implement per-table read/write authorization against the table from `FlightDescriptor::path` (`do_put`) and from the parsed SQL (`do_get`), behind a pluggable credential store with a static-config backend for v1 (§9).

## Phase 4: Benchmarks & High-Concurrency Stress Testing
- [ ] Micro-benchmark ingestion components with `criterion`: `MemTable` insert, WAL append, Parquet write.
- [ ] Build a dedicated load harness (not `criterion` — network jitter invalidates its statistics) and measure `do_put` throughput with 100 parallel Arrow Flight clients.
- [ ] Measure `do_get` query latency and network throughput under heavy write load via the same harness.
- [ ] Verify zero-copy behavior, and that flushed and compacted files stay sorted by `(metric_id, timestamp, seq)` with every version preserved.
- [ ] Verify time travel: write a value, correct it, and confirm each observation point returns the version current at that point — before the first write, between the two, and after. Include corrections that land after their partition was flushed and compacted.
- [ ] Verify retransmission suppression: re-sending an identical batch creates no new version and leaves every observation point's answer unchanged.
- [ ] Benchmark as-of query latency against version depth (1, 10, 100 versions per logical row) to confirm the streaming merge does not degrade into a full sort.
- [ ] Verify crash recovery: kill mid-write, confirm startup succeeds, acked data survives, the registry is intact, and replay is deterministic.
- [ ] Verify metric pruning: a query on a few metrics out of many reads only the matching row groups, measured by bytes scanned rather than wall time (§11).
- [ ] Benchmark the partition interval variants (`hour`/`day`/`month`/`year`) against the real workload — few metrics over weeks to months — reporting files opened, bytes scanned and latency, to settle the default empirically (§11).
- [ ] Verify retention: expired partitions are dropped, live partitions stay untouched, expired-range ingestion is rejected.
- [ ] Verify authorization: unauthenticated calls are refused, and per-table grants are enforced on both `do_put` and `do_get`.
