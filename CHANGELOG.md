<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Changelog

Notable changes are recorded here, in the format of
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) with the pre-1.0
reading: until 1.0 a minor bump may break anything, including the on-disk
layout. Design decisions referenced as §n live in [`ARCHITECTURE.md`](ARCHITECTURE.md).

## [Unreleased]

### Fixed

- **Records written after a crash recovery could be lost on the next start.**
  Replay stopped at the first segment with a torn tail. Recovery then opened a
  *new* segment, so everything acknowledged afterwards lived behind the damage
  and was skipped by every later replay — contradicting §4's *acked means
  durable*. Replay now reads every segment: a tear ends its own segment, not the
  log, which is sound because each segment is framed independently.

  In the data log the exposure was a window, closing whenever a checkpoint
  reclaimed the damaged segment. In the **catalogue log it was permanent and
  worse than loss**: that log is never reclaimed, so registrations and schema
  changes made after any crash stayed invisible, and the registry re-issued
  those `metric_id`s to other series while rows already in Parquet still carried
  them (§10).

  An archive that has never crashed mid-write is unaffected. One that has should
  be checked for series whose history changes meaning at a `seq` boundary.

## [0.1.0-alpha.1] — 2026-08-03

First alpha, and the first release of any kind. Every task in
[`tasks.md`](tasks.md) — Phases 0 through 4 — is complete, verified by 507 tests
with `cargo clippy -- -D warnings` clean. It is a working single-node bitemporal
archive; it is not yet something to put data in that you need back.

### Added

**Ingestion**

- Arrow Flight `do_put` streaming ingest of `RecordBatch` payloads, acknowledged
  only after fsync (§4, §5). Client column buffers reach the memtable uncopied —
  asserted by pointer identity, since nothing else distinguishes a share from a
  copy of equal values.
- Write-ahead log with group commit over a shared fsync, per-record CRC, segment
  rotation and checkpoint records for reclamation (§4). Replay applies segments
  in sequence order and truncates at the first torn record, so a damaged tail
  never stops a start.
- Lock-free versioned `MemTable`: `(metric_id, timestamp)` identifies a logical
  row, many versions coexist, nothing is overwritten (§1). Freeze-and-swap keeps
  a flush off the ingestion path (§2).
- System columns `system_time` and `seq` stamped on ingest, with `system_time`
  clamped so it never decreases as `seq` grows (§1).
- Retransmission suppression on payload digest, over a per-table ingest-time
  window with a hard entry cap, measured from the last touch and carried across
  freezes (§1, §6).
- Partition routing splitting a batch that spans an interval boundary at insert
  time; a single-partition batch passes through uncopied (§3, §11).
- Backpressure: acknowledgements produced lazily so gRPC flow control carries
  the pressure back, and `RESOURCE_EXHAUSTED` past the hard memory cap (§7).

**Storage**

- Parquet writer with ZSTD and page-level statistics on every column,
  `system_time` included, so an old observation point skips whole row groups
  (§1). It refuses an unsorted batch rather than sorting it, and declares its
  sort order in the footer.
- Hive-style partition layout at a configurable `hour`/`day`/`month`/`year`
  interval, recorded in `instance.meta` at initialisation and validated against
  the on-disk layout at startup — a mismatch is a startup error, never a second
  layout (§11).
- Background flusher per partition on size cap or max age, writing
  `part-<seq>.parquet` sorted by `(metric_id, timestamp, seq)` with every
  version preserved (§1, §2, §3). A failed flush keeps its rows.
- Background compactor merging a partition's files into files of a configurable
  target size with disjoint `(metric_id, timestamp)` ranges, dropping no version
  (§1, §3, §11).
- Retention by dropping whole partition directories once their interval is
  entirely past the cutoff, by event time; ingestion of an already-expired range
  is refused with `FAILED_PRECONDITION` (§8).

**Query**

- DataFusion pipeline over memory and Parquet through the archive's own
  `Storage`, so there is one view of the bytes. A frozen memtable is held until
  its file exists, or rows would vanish from answers during a flush.
- As-of version resolution as a streaming merge over the sorted files rather
  than a full-sort window function (§1). A test reads the physical plan and
  fails on a `SortExec`.
- Each table exposed twice: `<table>` resolved at the observation point, and
  `"<table>$history"` carrying every version with `system_time` and `seq` as
  ordinary columns. The observation point bounds `$history` too (§1, §5).
- Predicate pushdown, column projection, partition pruning off the directory
  name, and row-group skipping on `system_time` and `metric_id` (§11). Pruning
  is asserted in bytes scanned, not in rows returned.
- `hetida_label('plant', 'a')` as an analyzer marker rewritten into a
  `metric_id` filter before optimisation; a label nothing carries becomes
  `false` (§10).
- The registry as SQL: `hetida_series` and `hetida_labels` (§10).
- `get_flight_info`, `get_schema` and `do_get`, all taking the same
  `hetida.v1.Query { sql, as_of_micros }`; the ticket is the request, so nothing
  has to survive between two calls (§5).

**Operations**

- One TOML configuration file named by `HETIDA_CONFIG`, the only environment
  variable. Unknown keys are rejected, an unreadable named file stops the start,
  and a retired environment variable that is still set refuses the start and
  says where it moved.
- TLS termination, and Flight `Handshake` issuing bearer tokens validated by a
  `tonic` interceptor once per call, never per streamed message (§9). Covered
  end to end against a real handshake with a certificate generated per test run.
- Per-table read and write authorization behind a pluggable credential store,
  with a static-config backend. Read grants are checked against every table a
  planned query touches, so a join or a view cannot reach round them (§9).
- Serving without TLS or without credentials has to be written down
  (`allow_plaintext`, `allow_anonymous`); the server otherwise refuses to start.

**Measurements** — loopback, 100 parallel clients, from
`cargo run --release --example loadgen` and `cargo bench`:

| | |
| --- | --- |
| `do_put` throughput | ~306 000 rows/s, p50 151 ms, p99 321 ms |
| `do_get` under write load | p50 201 ms, p99 375 ms |
| as-of latency by version depth | 0.88 ms / 1.36 ms / 5.90 ms at 1 / 10 / 100 versions per logical row |
| metric pruning | 24 kB read of 185 kB, by id and by label alike |

The partition-interval comparison recorded in §11 rules out `hour` and `day`
decisively and does not separate `month` from `year` at a thirty-day span.
`month` is the default.

### Known limitations

Carried from *Open Questions* in [`ARCHITECTURE.md`](ARCHITECTURE.md):

- **No query cost limits** on client-supplied SQL. Do not expose this alpha to
  clients you would not also give a shell.
- **No downsampling or rollup** before retention drops a partition.
- **Credential backends**: static configuration only. No OIDC, no JWT, no mTLS —
  the trait is shaped not to preclude them.
- **One partition interval per instance**, immutable once data exists. No
  tiering of fine granularity for recent data and coarse for aged.
- **Single node.** No replication, no sharding, no failover.

### Compatibility

Nothing is stable in an alpha. The on-disk layout — WAL records, catalogue log,
`instance.meta`, the Parquet file and partition layout — the configuration file,
and the Flight and control-plane contract may each change without a migration
path before 0.1.0. Expect to re-create an archive rather than upgrade it.

[Unreleased]: https://github.com/dsamars/hetida-db/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/dsamars/hetida-db/releases/tag/v0.1.0-alpha.1
