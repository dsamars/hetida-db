<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Time-Series Parquet Archiver - Architecture

## System Design Overview

```text
               ┌──────────────────────────────────────────┐
               │  Concurrent Clients / Writers / Readers  │
               │  up to 1024 parallel producers/consumers │
               └────────────────────┬─────────────────────┘
                                    │
                                    │ Pure Arrow Flight Protocol
             ┌──────────────────────┴──────────────────────┐
             │                                             │
      DoPut Stream (Writes)                         DoGet Stream (Queries)
             │                                             │
             ▼                                             ▼
┌──────────────────────────┐                      ┌──────────────────────────┐
│ Ingestion Layer          │                      │ Query Engine             │
│ (FlightService::do_put)  │                      │ (FlightService::do_get)  │
└────────┬────────┬────────┘                      └────────┬────────┬────────┘
         │        │                                        │        │
         │        │ Append RecordBatches                   │        │ Scan
         ▼        ▼                                        ▼        ▼
 ┌──────────┐ ┌──────────┐                        ┌──────────┐ ┌──────────┐
 │ WAL Log  │ │ MemTable │                        │ MemTable │ │ Parquet  │
 │ (Disk)   │ │ (RAM)    │                        │ (RAM)    │ │ (Disk)   │
 └──────────┘ └────┬─────┘                        └──────────┘ └──────────┘
                   │                                       │
                   │ Flush (Sort by key + seq)             │ Merge & Resolve
                   ▼                                       ▼
        ┌────────────────────────────┐            ┌──────────────────────────┐
        │ Parquet Files              │            │ DataFusion Vectorized    │
        │ /year=YYYY/month=MM/       │            │ Query Plan               │
        └────────────────────────────┘            └──────────────────────────┘
```

The partition depth is configurable (§11); the diagram shows the `month` default.

## Design Decisions

These are settled. Reopen only with a reason recorded here.

### 1. Versioning & Time Travel

The archive is **bitemporal**. Two independent time axes:

- **Valid time** — the `timestamp` column: when the measurement was true. Also the partition key (§3).
- **System time** — when the row was written. This is the axis a time-travel query observes.

- **A logical row is identified by `(metric_id, timestamp)`;** attributes are payload, not identity. Many versions of the same logical row coexist. Nothing is ever overwritten or discarded — a correction appends a new version.
- **System columns**, appended by the server on ingest and never supplied by the client:
  - `system_time` (UTC timestamp) — the observation axis, user visible.
  - `seq` (monotonic `u64`) — total order and tiebreaker. `system_time` alone is insufficient: concurrent writers collide within the same millisecond.
- **Invariant: `system_time` is monotonically non-decreasing with `seq`.** If the wall clock moves backwards, clamp to the last issued value. Without this a later version could be invisible at a later observation point.
- **Read semantics:** a query with observation point *T* sees, for each `(metric_id, timestamp)`, the one version with the highest `seq` among those whose `system_time <= T`. With no observation point, *T* is now — the current state.
- **Retransmission suppression:** on insert, a version whose payload is byte-identical to the current latest version of the same logical row is discarded. At-least-once delivery is the norm over Flight, and a client re-sending a batch after a timeout must not manufacture a phantom version that time travel would then report as a change. A byte-identical payload is exactly the retransmission case; a genuine correction differs by definition.
- **Storage sort order is `(metric_id, timestamp, seq)`** in every flushed and compacted Parquet file. Version resolution is then a streaming merge — take the last row of each key group — instead of a window function requiring a full sort over the scanned range. This cannot be retrofitted without rewriting every file.
- **Nothing resolves versions by discarding them** — not the `MemTable`, not the flusher, not the compactor. Resolution happens at read time only. Version history is unbounded by default; see §8 for the optional horizon.

### 2. MemTable & Flush

- **Trigger:** whichever fires first — per-partition size cap, or maximum age. Size bounds memory and yields well-sized row groups; age bounds the data-loss window and query staleness for low-rate metrics.
- **Granularity:** flush per partition, never globally. A global flush writes into many partitions at once and scatters small files.
- **Non-blocking:** freeze the active `MemTable`, open a new one, write the frozen one in the background. Ingestion never waits for a flush.
- **Sorted output:** every flush writes rows ordered by `(metric_id, timestamp, seq)` and preserves all versions (§1).

### 3. Late-Arriving Data & Compaction

- **All data is accepted regardless of age.** Nothing is dropped by a lateness watermark — this is an archiver. The single exception is data already outside the configured retention window, which is rejected explicitly (§8).
- A `RecordBatch` is split by partition interval (§11) at insert time and routed to the matching partitions, since one batch may span interval boundaries.
- Each flush writes a new `part-<seq>.parquet` into its partition.
- A background **compactor** merges and re-sorts a partition's files into `(metric_id, timestamp, seq)` order once file count or total size crosses a threshold. It **never drops a version** — its purpose is file count and scan efficiency, not version resolution (§1).

### 4. Write-Ahead Log

- **Durability:** group commit. Concurrent writes share one fsync (max delay configurable, ~10ms). `do_put` acknowledges only after fsync, so *acked* means *durable*. Per-batch fsync would cost ~1ms per call and cap throughput far below the 1024-client target.
- **Rotation:** one segment per N MB. A segment may be deleted once all data it covers is in Parquet, tracked by a checkpoint record.
- **Recovery:** every record carries a CRC. Replay applies segments in sequence order. A torn tail record from a mid-write crash truncates the log at that point — it must never prevent startup.

### 5. Arrow Flight Surface

- **`do_put`:** `FlightDescriptor::path` carries the target table name.
- **`do_get`:** the `Ticket` carries `{ sql, as_of: Option<Timestamp> }` — a SQL string parsed by DataFusion, plus the optional observation point (§1). The observation point is a first-class field rather than something the client expresses in SQL, so correct version resolution is the default instead of every client re-implementing it. *Known risk:* arbitrary client SQL means arbitrary query cost — cost limits are required before exposing this to untrusted clients.
- **Each table is exposed as an as-of-resolved view.** The unresolved version history is reachable under `<table>$history` for audit and debugging, carrying `system_time` and `seq` as ordinary columns.
- **`get_flight_info` and `get_schema` are part of the surface.** The standard client flow is `get_flight_info` → `FlightInfo{schema, endpoints}` → `do_get`; without it generic Flight clients cannot talk to the server. `get_flight_info` takes the same `{sql, as_of}` payload in the descriptor `cmd`, so the schema it reports matches what `do_get` will stream.
- **Plain Arrow Flight for v1.** FlightSQL would add JDBC/ODBC driver compatibility but is significantly more surface; it is a later addition and is not excluded by the pure-Flight rule.

### 6. Tables & Schema Evolution

- **Multiple named tables**, each with its own schema.
- **Attributes are real typed columns**, not a `Map<Utf8, Utf8>` column. A map column would forfeit both compression and predicate pushdown on attributes — the two properties the columnar archive exists for.
- **Evolution rules:** adding a nullable column is accepted; a missing column reads as null; a type change on an existing column rejects the batch with an explicit Flight error. Parquet and DataFusion merge schemas across files as long as additions are nullable.
- **`system_time` and `seq` are reserved column names** (§1). A client batch that supplies either is rejected — the server owns those columns.
- **Labels are not attribute columns.** They belong to the series identity and live in the registry (§10); attribute columns carry payload only, such as values and quality flags.

### 7. Backpressure

- Bounded queues. When full, stop reading from the `do_put` stream and let gRPC flow control propagate the pressure back to the client — no protocol extension needed.
- **Hard limit:** if total `MemTable` memory exceeds the cap *and* flushing is falling behind, return `RESOURCE_EXHAUSTED`. Blocking indefinitely is worse than an explicit rejection, because the client then only sees a timeout without a cause.

### 8. Retention & TTL

- **Configured per table** as a duration. Unset means keep forever.
- **Time basis is event time** — the partition a row belongs to, not its ingestion time. The Hive-style layout already encodes this, so the reaper needs no additional index.
- **Granularity is the whole partition,** and therefore follows the configured partition interval (§11). A background reaper drops a partition directory once its entire interval is older than the cutoff. Never partial deletion: removing rows from inside a Parquet file means rewriting it, which is compaction's job, not retention's.
- **Coarse intervals over-retain.** With `month` partitions and a 90-day policy, data survives until its whole month clears the cutoff — up to roughly 120 days. Retention resolution can never be finer than the partition interval; choose the two together.
- **Ingesting already-expired data is rejected** with an explicit Flight error rather than written and reaped moments later. This is the one age-based rejection in the system, and it is an explicit configured policy — not the lateness watermark that §3 rules out.
- **Out of scope for v1:** downsampling or rollup before deletion. That is an aggregation feature, not a retention policy.

Retention has a **second, independent dimension** since §1:

- **Version retention is unbounded by default** — every correction is kept forever and time travel reaches back to the first write. For an archive with audit expectations, silently forgetting history is the worse surprise.
- An optional per-table horizon may let compaction collapse superseded versions older than it. The horizon *is* the time-travel reach, so shortening it destroys history — it must never be applied implicitly.
- The two dimensions are orthogonal: event-time retention drops whole partitions, version retention thins the history within surviving partitions.

### 9. Authentication & Authorization

- **Transport:** TLS, terminated by `tonic`.
- **Authentication:** Flight `Handshake` exchanges credentials for a bearer token. Every subsequent call carries it in the `authorization` call metadata, validated by a `tonic` interceptor.
- **Validation runs once per call, never per message.** A `do_put` stream carries many `RecordBatch` messages behind a single authorization; checking per message would put token validation in the hot path.
- **Authorization:** per-table read/write grants, checked against the table resolved from `FlightDescriptor::path` for `do_put` and from the parsed SQL for `do_get`. Multi-table is already given (§6), so table level is the natural granularity.
- **Credential store is pluggable** behind a trait; v1 ships a static configuration file. OIDC/JWT and mTLS client certificates are later backends.
- Authentication is a precondition for the query cost limits noted in §5, not a substitute for them: an authenticated client can still submit a ruinous query.

### 10. Metric Identity & Registry

- **`metric_id` is the full series identity** — the metric name *plus* its complete label set — resolved to a `u64` surrogate by a persistent registry.
  - *Rationale:* §1 identifies a logical row by `(metric_id, timestamp)`. If labels were payload, two parallel series sharing a name and a timestamp would collapse into one logical row and the second would be recorded as a **correction** of the first. Time travel would then report a change that never happened.
- **The type is `u64`.** The sort key `(metric_id, timestamp, seq)` is then 24 bytes of fixed width — cheap to compare in merge and radix sorts, cheap to hash in the `MemTable`, compact in Parquet min/max statistics. Rejected alternatives: `Utf8` puts variable-length string comparison in the hottest path; `Dictionary<UInt32, Utf8>` requires dictionary unification across every batch and file; a hash of the name carries a collision probability unacceptable for an audited archive.
- **Resolution happens on the client, never on ingest.** Clients obtain ids once through a registration action, cache them, and send `u64` in `do_put`. Server-side name resolution would be a per-row lookup in the hot path.
- **Registration is idempotent:** the same name and label set always yields the same id.
- **Labels live in the registry only** and are not repeated in the data files. A label predicate resolves against the registry to a set of `metric_id`s first, which then pushes down as a filter on `metric_id`. The registry holds one row per series and is small enough to scan; the fact table stays narrow.
- **The registry is critical state.** Without it the archive cannot be interpreted. It is written through the WAL like ingested data, belongs in every backup, and is exposed as a queryable catalog table.

### 11. Partition Layout

- **The partition interval is a parameter, fixed when an instance is initialized.** Valid values are `hour`, `day`, `month`, `year`; the default is `month`.
  - Typical queries span weeks to months over a few metrics. A month-granular layout answers those from one to three directories, where a day-granular one opens thirty to ninety and pays a footer read on each to retrieve a few kilobytes per metric — metadata overhead, not scan volume, dominates that pattern.
- **Calendar-aligned intervals only, never arbitrary durations.** The Hive path encodes calendar fields, so a `WHERE timestamp BETWEEN …` predicate maps straight onto partition columns DataFusion can prune. An arbitrary duration would require a synthetic bucket column and lose that mapping.

  | Interval | Path |
  |---|---|
  | `hour` | `/year=YYYY/month=MM/day=DD/hour=HH/` |
  | `day` | `/year=YYYY/month=MM/day=DD/` |
  | `month` | `/year=YYYY/month=MM/` |
  | `year` | `/year=YYYY/` |

- **The interval is immutable once data exists.** It is baked into the directory structure, so changing it means rewriting every file. It is persisted in the instance metadata and validated against the on-disk layout at startup; a mismatch refuses to start rather than silently building a second, incompatible layout beside the first.
- **The partitioner receives the interval as a parameter** rather than reading a global, so a later per-table override is a configuration change instead of a rewrite.
- **Compaction target file size is a separate parameter.** A coarse interval on a high-rate table must still compact into several files rather than one enormous one. Because compaction merges in sort order, each output file covers a disjoint `(metric_id, timestamp)` range, so whole files can be skipped before their footers are read.
- **No `metric_id` level, at any interval.** It would create intervals × metrics directories — at 100 000 metrics and day granularity roughly 36.5 million files per year, each with Parquet footer overhead, compression and read throughput destroyed, and every flush emitting one tiny file per metric.
- **Metric pruning comes from the sort order instead.** Files sorted by `(metric_id, timestamp, seq)` (§1) keep each metric's rows contiguous, so row-group min/max statistics and the page-level column index skip everything outside the queried metrics. This is precisely why the sort is metric-major: under `(timestamp, metric_id)` a metric's rows would scatter across every row group and pruning would fail.
- **No Bloom filters on `metric_id`.** They address unsorted columns; min/max is already optimal on a sorted one.
- **Escalation path** — should measurement ever show this insufficient, the answer is hash bucketing, a `bucket=H` level below the interval with `H = hash(metric_id) % N` for a small fixed N (16–64), sized so a bucket's per-interval file stays above ~100 MB. Not metric-level partitioning. Note that range predicates on `metric_id` stop pruning under bucketing, and changing N later requires a full rewrite.

## Open Questions

- **Query cost limits** for client-supplied SQL (§5) — required before untrusted clients.
- **Downsampling / rollup** before retention deletion (§8) — deferred.
- **OIDC/JWT and mTLS credential backends** (§9) — deferred; the trait must not preclude them.
- **Where registrations live** (§4 vs §10) — §10 puts the registry through the WAL, but §4 reclaims a segment once its data is in Parquet. Registrations are not flushed to Parquet, so reclaiming a segment holding them destroys the registry and with it the archive's interpretability. Resolutions: a separate never-reclaimed log, a registry snapshot written before reclamation, or reclamation skipping segments that hold registrations (which leaks segments). Undecided; the WAL wiring is blocked on it.
- **How far back retransmission suppression must reach** (§1) — it currently compares against the newest version the *active* memtable holds, so a retry arriving after that version was flushed is not recognised and becomes a phantom version. Closing the gap means either carrying a payload digest per logical row across freezes, or consulting flushed data on ingest. Undecided.
- **Tiered partition intervals** (§11) — fine granularity for recent data, coarse for aged. Not needed for the stated weeks-to-months query pattern, which a flat configurable interval already serves; deferred unless measurement contradicts it.
