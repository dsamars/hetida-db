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
- **Unchanged-value suppression:** on insert, a version whose payload is identical to the current latest version of the same logical row is discarded. A genuine correction differs by definition, so only a write that changes nothing is ever dropped.
  - **The dominant case is idempotent recomputation, not transport retries.** A forecast job rewriting its whole horizon every hour — up to 24 times a day — resubmits values that mostly have not changed. Those writes are deliberate and, from the server's side, indistinguishable from a first write except by their payload. At-least-once delivery over Flight produces the same shape on a much shorter timescale and is covered by the same mechanism, but it must not drive the design: sizing the reach for network timeouts leaves the recomputation case entirely unhandled, which is precisely the error this decision corrects.
  - **The reach is therefore sized on the write rhythm of the producing jobs** — hours where a scheduled job recomputes, minutes where a stream only needs to survive retries — as an ingest-time window with a hard entry cap, whichever binds first. It is not the lifetime of the active memtable: tying it there made the guarantee depend on flush timing, which no client can observe and which shortens precisely under load, where the memtable rotates in seconds. The window survives any number of freezes.
  - **The window is configured per table** (§6), because the two workloads want opposite settings and a single global value serves neither. A recomputed series needs hours; a measurement stream needs minutes and would blow the entry cap at that reach, because its keys never recur. One global window would have to take the maximum and would be unaffordable for the stream.
  - **Unbounded reach is impossible for measurement data and merely expensive for recomputed data.** A logical row's key contains `timestamp`, so for a measurement stream no key ever recurs and comparison state covering all history would be the archive's whole index held in memory. Recomputation is the opposite: the same `(metric_id, timestamp)` is rewritten every run, so the state is bounded by the horizon's cardinality — metrics × points in the horizon — and does not grow with ingest volume at all. This asymmetry is the reason the window belongs to the table and not to the instance.
  - **A miss costs audit accuracy and space, not correctness.** A duplicate version carries the same payload as the one it repeats, so every as-of query returns the same payload at every observation point. What degrades is `<table>$history`, which then shows a correction that never happened, the `system_time` of the resolved version, and the space it takes. Under recomputation that space is not a rounding error: a point rewritten hourly accumulates thousands of versions a year that say the same thing, and §1 leaves no later stage that may drop them — the insert path is the only place a repeat can disappear without violating this section, because there it is not yet a version.
  - **The surviving version's `system_time` means "unchanged since", not "last confirmed".** A suppressed write leaves no trace, so the axis answers when the value last *changed*. For a recomputed series that is the more useful reading, but proving that a particular run executed and reasserted a value is then a property of the job's own log, not of the archive.
  - **The comparison is a 128-bit digest of the payload columns,** held per logical row instead of the full payload, which is what makes a window spanning freezes affordable — it costs less memory than the full payloads of the active memtable alone do today. This is not the collision exposure §10 rejects for `metric_id`: there a hash establishes *identity* across all pairs, so the birthday bound applies. Here it decides one equality test between two known values per row, at 2⁻¹²⁸ each, which is orders of magnitude below the silent-corruption rate of the memory the full payload would otherwise sit in.
  - **The window advances on the `system_time` of incoming data,** not on a clock read. That axis is monotonic by the clamp above, so it needs no clock on the insert path and makes WAL replay reproduce the same expiry it saw the first time. A quiescent instance expires nothing, which is correct: the window measures ingest, not wall time.
  - **The comparison state belongs to the table, not to a partition.** §2 flushes per partition, so a table holds one memtable per partition, but one shared index serves them all. The entry cap is a memory bound (§7) and would otherwise multiply by the partition count — by 168 for an `hour` layout and a week-long forecast horizon. The window also advances on a single ingest-time axis: split per partition, a rarely-written one would keep its own rotation clock and never expire anything. Since a logical row belongs to exactly one partition, splitting the index would buy no correctness in exchange.
  - **The window runs from the last time a logical row was touched, not from the last time it was written.** A suppressed write stores nothing, so without this a producer resubmitting the same value forever would still lose its comparison state two windows after the *last change*, and every second run would slip through. Keeping the entry warm on a hit also makes the memory behaviour match the workloads: a recomputed horizon holds a constant set of entries, while a measurement stream's entries are never touched again and age out.
  - *Rejected:* **consulting flushed Parquet on insert**, which puts a footer read and a row-group scan on the per-row ingest path — ruled out by both the hot-path rule and the 1024-client target. **Protocol-level idempotence**, a producer id plus batch sequence number checked once per batch, is also rejected as a substitute: it recognises a repeated *batch*, whereas a recomputation run is a new batch carrying repeated *values*, so it does not see the dominant case at all. It remains a possible later addition for cheaply discarding true retries before their payload is even examined.
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
- **Two logs, one format.** The data log and the **catalogue log** share the record framing, the group commit and the recovery rule, and differ only in reclamation. The catalogue log holds everything that describes the archive rather than being part of it — metric registrations (§10) and schema changes (§6). They are one log rather than two because they share every property that matters: never reclaimed, tiny, append-only, and replayed before the data log. One log means one replay and one recovery rule. `seq` is issued from one source across both logs, so the order between a catalogue change and a write is always defined; the gaps this leaves in the data log's `seq` are harmless, because §1 requires monotonicity, not density.
- **Rotation:** one segment per N MB. A data segment may be deleted once all data it covers is in Parquet, tracked by a checkpoint record. **Reclamation applies to the data log alone** — it is the only log whose contents reach Parquet. The catalogue log is never reclaimed.
- **Recovery:** every record carries a CRC. Replay applies segments in sequence order. A torn tail record from a mid-write crash truncates the log at that point — it must never prevent startup. The catalogue log is replayed first, so every id and every schema is known before the data referring to them is read.

### 5. Arrow Flight Surface

- **`do_put`:** `FlightDescriptor::path` carries the target table name.
- **`do_get`:** the `Ticket` carries `{ sql, as_of: Option<Timestamp> }` — a SQL string parsed by DataFusion, plus the optional observation point (§1). The observation point is a first-class field rather than something the client expresses in SQL, so correct version resolution is the default instead of every client re-implementing it. *Known risk:* arbitrary client SQL means arbitrary query cost — cost limits are required before exposing this to untrusted clients.
- **Each table is exposed as an as-of-resolved view.** The unresolved version history is reachable under `<table>$history` for audit and debugging, carrying `system_time` and `seq` as ordinary columns.
- **`get_flight_info` and `get_schema` are part of the surface.** The standard client flow is `get_flight_info` → `FlightInfo{schema, endpoints}` → `do_get`; without it generic Flight clients cannot talk to the server. `get_flight_info` takes the same `{sql, as_of}` payload in the descriptor `cmd`, so the schema it reports matches what `do_get` will stream.
- **Control-plane payloads are Protobuf; time series data is Arrow.** Flight's `Action` body, its result, and the `Ticket` are opaque bytes, and something has to define them. Protobuf, in `proto/hetida.proto`, is the client contract: a client generates from a schema instead of reimplementing a byte layout described in prose. This does not weaken the pure-Arrow rule, which is about *payload* — no measurement ever travels as Protobuf, and no control message as Arrow. `prost` is already in the graph beneath `arrow-flight`, whose own messages are Protobuf, so this follows Flight's own convention rather than adding a second one.
  - The `.proto` is compiled by `protox` in `build.rs`, not by `prost-build`'s default path, which shells out to `protoc`. A pure-Rust compiler keeps `cargo build` reproducible without a system package every developer and CI runner must install first.
  - **Registration is batched:** one call resolves many series, because clients resolve once at startup and cache (§10).
- **Plain Arrow Flight for v1.** FlightSQL would add JDBC/ODBC driver compatibility but is significantly more surface; it is a later addition and is not excluded by the pure-Flight rule.

### 6. Tables & Schema Evolution

- **Multiple named tables**, each with its own schema.
- **Attributes are real typed columns**, not a `Map<Utf8, Utf8>` column. A map column would forfeit both compression and predicate pushdown on attributes — the two properties the columnar archive exists for.
- **Evolution rules:** adding a nullable column is accepted; a missing column reads as null; a type change on an existing column rejects the batch with an explicit Flight error. Parquet and DataFusion merge schemas across files as long as additions are nullable.
- **`system_time` and `seq` are reserved column names** (§1). A client batch that supplies either is rejected — the server owns those columns.
- **Labels are not attribute columns.** They belong to the series identity and live in the registry (§10); attribute columns carry payload only, such as values and quality flags.
- **Per-table configuration** covers what differs by workload rather than by deployment: the suppression window and its entry cap (§1), and the retention duration (§8). A table recomputed hourly and a table fed by a measurement stream want opposite values for the first, so the instance cannot hold one. This half is an operator's choice and comes from instance configuration, re-read at every start.
- **The schema half is state, and is recorded in the catalogue log** (§4). It is not configuration: it grows through evolution on ingest, so nothing outside the archive knows what it currently is. Without it recorded durably, the type-change rejection above stops working across a restart — with no schema to compare against, a client sending `Int64` where `Float64` was accepted before would be admitted, and the conflict would surface much later as Parquet that cannot be read across files. Every accepted evolution appends a record; the log holds one per change, not one per batch.
  - *Rejected:* **reconstructing the schema from Parquet footers.** It costs a footer read per file at startup, and a table whose rows are all still in memory has no file to read a schema from — so the check would be absent exactly for the newest table, which is where a client is most likely to get its types wrong.

### 7. Backpressure

- Bounded queues. When full, stop reading from the `do_put` stream and let gRPC flow control propagate the pressure back to the client — no protocol extension needed.
- **Hard limit:** if total `MemTable` memory exceeds the cap *and* flushing is falling behind, return `RESOURCE_EXHAUSTED`. Blocking indefinitely is worse than an explicit rejection, because the client then only sees a timeout without a cause.
- **The suppression index counts against the memory budget** (§1). Its entry cap bounds it independently of the memtables, and reaching that cap shortens the suppression window instead of growing memory — suppression degrades, but a correction is never discarded.

### 8. Retention & TTL

- **Configured per table** as a duration. Unset means keep forever.
- **Time basis is event time** — the partition a row belongs to, not its ingestion time. The Hive-style layout already encodes this, so the reaper needs no additional index.
- **Granularity is the whole partition,** and therefore follows the configured partition interval (§11). A background reaper drops a partition directory once its entire interval is older than the cutoff. Never partial deletion: removing rows from inside a Parquet file means rewriting it, which is compaction's job, not retention's.
- **Coarse intervals over-retain.** With `month` partitions and a 90-day policy, data survives until its whole month clears the cutoff — up to roughly 120 days. Retention resolution can never be finer than the partition interval; choose the two together.
- **Ingesting already-expired data is rejected** with an explicit Flight error rather than written and reaped moments later. This is the one age-based rejection in the system, and it is an explicit configured policy — not the lateness watermark that §3 rules out.
  - **Rejection and reaping are one rule seen from two sides.** A batch is refused exactly when its oldest row falls in a partition the reaper is already entitled to drop — never merely because a row lies behind the cutoff. The two must agree: a stricter rejection would turn away data the archive would in fact have kept and served, since a partition holding one live microsecond is retained whole and every row in it is queryable. A laxer one would acknowledge rows the next reaping pass deletes. The predicate is therefore written once and used by both, and the property that they never disagree is in the invariant suite.
  - The status is `FAILED_PRECONDITION`, not `RESOURCE_EXHAUSTED`: the batch is well formed and the server is healthy, so backing off and retrying is precisely wrong — the cutoff only moves further forward.
  - Nothing coordinates the reaper with the flusher. A memtable admitted before the cutoff moved may still flush into a directory just dropped, recreating it; the next pass drops it again. That race is left in deliberately, because the alternative is a lock across the ingestion path, and no query in between can be wrong — the rows were expired either way.
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
- **The registry is critical state.** Without it the archive cannot be interpreted, and it is the one piece of state that never reaches Parquet. It belongs in every backup — both logs do — and is exposed as a queryable catalog table.
- **It lives in the catalogue log, which is never reclaimed** (§4). Sharing the *data* log would place it under §4's reclamation rule, which drops a segment once its data is in Parquet — a condition registrations meet trivially and fatally. The point of a separate log is that the guarantee is *structural* rather than conditional: no code path exists that could delete a registration, so no misset flag and no write order interrupted by a crash can either. For the state the archive's readability depends on, that is the right kind of guarantee.
  - *Rejected alternatives:* a **registry snapshot written before reclamation** puts critical state behind a multi-step non-atomic protocol, and wins no space — the snapshot holds precisely what the log already holds. **Reclamation skipping registration-bearing segments** lets one 80-byte record pin a whole segment of flushed data, making disk growth a function of registration cadence, which nothing bounds. **Flushing the registry to Parquet like a data table** still needs the log first, because durability precedes visibility while flush is asynchronous (§2), and the table fits neither the partition layout (§11) nor the sort order (§1).
- **The log holds exactly one record per series and never needs compaction.** Idempotence is settled in memory before the log is touched, so a duplicate is never written and there is no garbage to collect. Its size is bounded by series cardinality rather than ingest volume, and a registry too large for the disk was already too large for the memory it is held in.
- **A torn tail record is correctly lost.** An id is returned only once its append is durable, so a record a crash damaged is by construction an id no client ever saw. Discarding it and handing the id to another series is sound; numbering resumes past the highest surviving id.
- **Retention never removes a registration.** Dropping a series' last partition (§8) leaves its identity in place, so re-registering it later still yields the same id.

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
- **Tiered partition intervals** (§11) — fine granularity for recent data, coarse for aged. Not needed for the stated weeks-to-months query pattern, which a flat configurable interval already serves; deferred unless measurement contradicts it.
