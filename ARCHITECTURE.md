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
                   │ Flush (Deduplicate & Sort)            │ Merge & Execute
                   ▼                                       ▼
        ┌────────────────────────────┐            ┌──────────────────────────┐
        │ Parquet Files              │            │ DataFusion Vectorized    │
        │ /year=YYYY/month=MM/day=DD/│            │ Query Plan               │
        └────────────────────────────┘            └──────────────────────────┘
```

## Design Decisions

These are settled. Reopen only with a reason recorded here.

### 1. Deduplication

- **Composite key:** `(metric_id, timestamp)`. Attributes are payload, not identity.
- **Conflict resolution:** last-write-wins, ordered by a monotonic ingestion sequence number assigned on `do_put`. Never wall-clock time — it can move backwards and would make WAL replay non-deterministic.
- **Scope:** the `MemTable` deduplicates within its flush window only. Duplicates that arrive after a partition has been flushed are resolved by compaction (§3), not at read time. The read path never deduplicates.
- **Rationale:** at-least-once delivery is the norm over Flight. A client that re-sends a batch after a timeout must not create duplicates.

### 2. MemTable & Flush

- **Trigger:** whichever fires first — per-partition size cap, or maximum age. Size bounds memory and yields well-sized row groups; age bounds the data-loss window and query staleness for low-rate metrics.
- **Granularity:** flush per day partition, never globally. A global flush writes into many partitions at once and scatters small files.
- **Non-blocking:** freeze the active `MemTable`, open a new one, write the frozen one in the background. Ingestion never waits for a flush.

### 3. Late-Arriving Data & Compaction

- **All data is accepted regardless of age.** Nothing is dropped by a watermark — this is an archiver.
- A `RecordBatch` is split by day at insert time and routed to the matching partition, since one batch may span day boundaries.
- Each flush writes a new `part-<seq>.parquet` into its partition.
- A background **compactor** merges, sorts and deduplicates a partition's files once file count or total size crosses a threshold. This is what makes deduplication complete across flush windows; without it §1 holds only per flush window.

### 4. Write-Ahead Log

- **Durability:** group commit. Concurrent writes share one fsync (max delay configurable, ~10ms). `do_put` acknowledges only after fsync, so *acked* means *durable*. Per-batch fsync would cost ~1ms per call and cap throughput far below the 1024-client target.
- **Rotation:** one segment per N MB. A segment may be deleted once all data it covers is in Parquet, tracked by a checkpoint record.
- **Recovery:** every record carries a CRC. Replay applies segments in sequence order. A torn tail record from a mid-write crash truncates the log at that point — it must never prevent startup.

### 5. Arrow Flight Surface

- **`do_put`:** `FlightDescriptor::path` carries the target table name.
- **`do_get`:** the `Ticket` carries a SQL string, parsed by DataFusion. Maximum expressiveness for minimal code. *Known risk:* arbitrary client SQL means arbitrary query cost — cost limits are required before exposing this to untrusted clients.
- **`get_flight_info` and `get_schema` are part of the surface.** The standard client flow is `get_flight_info` → `FlightInfo{schema, endpoints}` → `do_get`; without it generic Flight clients cannot talk to the server.
- **Plain Arrow Flight for v1.** FlightSQL would add JDBC/ODBC driver compatibility but is significantly more surface; it is a later addition and is not excluded by the pure-Flight rule.

### 6. Tables & Schema Evolution

- **Multiple named tables**, each with its own schema.
- **Attributes are real typed columns**, not a `Map<Utf8, Utf8>` column. A map column would forfeit both compression and predicate pushdown on attributes — the two properties the columnar archive exists for.
- **Evolution rules:** adding a nullable column is accepted; a missing column reads as null; a type change on an existing column rejects the batch with an explicit Flight error. Parquet and DataFusion merge schemas across files as long as additions are nullable.

### 7. Backpressure

- Bounded queues. When full, stop reading from the `do_put` stream and let gRPC flow control propagate the pressure back to the client — no protocol extension needed.
- **Hard limit:** if total `MemTable` memory exceeds the cap *and* flushing is falling behind, return `RESOURCE_EXHAUSTED`. Blocking indefinitely is worse than an explicit rejection, because the client then only sees a timeout without a cause.

## Open Questions

- **Retention / TTL** — undecided. Without it the archive grows without bound.
- **Authentication** — undecided. Flight provides `Handshake` for this.
