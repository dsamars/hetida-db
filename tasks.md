<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Implementation Plan

Design decisions referenced below (§n) are recorded in `ARCHITECTURE.md`.

## Phase 1: Data Model & Ingestion Engine
- [ ] Define dynamic Arrow Schema builder (`timestamp`, `metric_id`, attributes as real typed columns) with multi-table registry and evolution rules: nullable column added = accept, column missing = null, type change = reject with explicit Flight error (§6).
- [ ] Implement lock-free concurrent `MemTable` keyed on `(metric_id, timestamp)`, last-write-wins by monotonic ingestion sequence number (§1).
- [ ] Implement day-partition routing: split each incoming `RecordBatch` by day at insert time, since one batch may span day boundaries (§3).
- [ ] Implement freeze-and-swap so a flush never blocks ingestion: freeze the active `MemTable`, open a new one, hand the frozen one to the background writer (§2).
- [ ] Build append-only WAL with group commit (shared fsync, configurable max delay), per-record CRC, segment rotation, and checkpoint records for segment reclamation (§4).
- [ ] Implement WAL replay: apply segments in sequence order, truncate at the first torn record, never fail startup on a damaged tail (§4).
- [ ] Implement Arrow Flight `do_put` streaming endpoint ingesting `RecordBatch` payload directly: assign sequence numbers, resolve the target table from `FlightDescriptor::path`, acknowledge only after fsync (§4, §5).
- [ ] Implement backpressure: bounded queues with gRPC flow control, plus `RESOURCE_EXHAUSTED` past the hard `MemTable` memory cap (§7).

## Phase 2: Parquet Storage & Archiver
- [ ] Implement Parquet writer with ZSTD compression and explicit Min/Max column index generation.
- [ ] Implement date/time directory partitioner (`/year=YYYY/month=MM/day=DD/`).
- [ ] Build background flusher transferring frozen `MemTable`s to Parquet, triggered per partition by size cap or max age — whichever fires first — writing `part-<seq>.parquet` (§2, §3).
- [ ] Build background compactor merging, sorting and deduplicating a partition's files once file count or total size crosses a threshold. Required for deduplication to hold across flush windows (§1, §3).
- [ ] Implement per-table retention configuration and a background reaper dropping whole day-partition directories once their day is entirely past the cutoff, by event time (§8).
- [ ] Reject ingestion of data already outside the retention window in `do_put` with an explicit Flight error (§8).

## Phase 3: High-Performance Query Engine & Arrow Flight Output
- [ ] Build DataFusion execution pipeline combining `MemTable` memory state and `Parquet` disk storage.
- [ ] Implement predicate pushdown and column projection for Parquet readers, including partition pruning via the Hive-style layout.
- [ ] Implement `get_flight_info` and `get_schema` so generic Flight clients can follow the standard `get_flight_info` → `do_get` flow (§5).
- [ ] Implement Arrow Flight `do_get` endpoint: parse the SQL string from the `Ticket` via DataFusion and stream results directly to clients via Arrow IPC (§5).
- [ ] Implement TLS termination and Flight `Handshake` issuing bearer tokens, validated by a `tonic` interceptor once per call — never per streamed message (§9).
- [ ] Implement per-table read/write authorization against the table from `FlightDescriptor::path` (`do_put`) and from the parsed SQL (`do_get`), behind a pluggable credential store with a static-config backend for v1 (§9).

## Phase 4: Benchmarks & High-Concurrency Stress Testing
- [ ] Micro-benchmark ingestion components with `criterion`: `MemTable` insert, WAL append, Parquet write.
- [ ] Build a dedicated load harness (not `criterion` — network jitter invalidates its statistics) and measure `do_put` throughput with 100 parallel Arrow Flight clients.
- [ ] Measure `do_get` query latency and network throughput under heavy write load via the same harness.
- [ ] Verify zero-copy behavior and zero duplicate entries in target Parquet files, including duplicates arriving after their partition was flushed (must be resolved by compaction).
- [ ] Verify crash recovery: kill mid-write, confirm startup succeeds, acked data survives, and replay is deterministic.
- [ ] Verify retention: expired partitions are dropped, live partitions stay untouched, expired-range ingestion is rejected.
- [ ] Verify authorization: unauthenticated calls are refused, and per-table grants are enforced on both `do_put` and `do_get`.
