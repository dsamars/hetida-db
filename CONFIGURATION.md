<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Configuration

Every setting hetida-db has, where it is set, and what choosing it wrong costs.
Section references (§n) point at the decision in [`ARCHITECTURE.md`](ARCHITECTURE.md)
that the setting serves.

Settings fall into three groups, and the difference matters:

| Group | Set by | Changeable |
|---|---|---|
| [Environment variables](#environment-variables) | the operator, at launch | every start |
| [Instance metadata](#instance-metadata) | recorded at initialisation | never, once data exists |
| [Compiled defaults](#compiled-defaults) | the code | only by embedding the crate — see [the gap](#what-an-operator-cannot-set-yet) |

## Environment variables

The whole operator surface of the `hetida-db` binary today.

| Variable | Default | Meaning |
|---|---|---|
| `HETIDA_DATA_DIR` | `hetida-data` | Root of the archive: logs, Parquet and instance metadata |
| `HETIDA_LISTEN` | `127.0.0.1:50051` | Socket address to serve Arrow Flight on |
| `HETIDA_TLS_CERT` | — | Path to a PEM certificate chain (§9) |
| `HETIDA_TLS_KEY` | — | Path to the matching PEM private key (§9) |
| `HETIDA_ALLOW_PLAINTEXT` | — | `1` to serve without TLS, deliberately |
| `HETIDA_USERS` | — | Credentials and per-table grants (§9) |
| `HETIDA_ALLOW_ANONYMOUS` | — | `1` to serve without authentication, deliberately |

### `HETIDA_DATA_DIR`

Everything the archive owns lives beneath it:

```
hetida-data/
  instance.meta          the recorded partition interval (§11)
  catalog/               registrations and table schemas — never reclaimed (§4, §10)
  wal/                   the data log — reclaimed once its rows reach Parquet (§4)
  data/<table>/year=…/…  partitioned Parquet, one directory per partition (§11)
```

The two logs are not interchangeable and both belong in every backup. Without
`catalog/` the Parquet files cannot be interpreted: `metric_id` is a surrogate,
and the mapping back to a name and its labels exists nowhere else (§10).

### `HETIDA_LISTEN`

Any socket address. Bind to a loopback address unless the listener is protected
— see the next two settings.

### `HETIDA_TLS_CERT`, `HETIDA_TLS_KEY`, `HETIDA_ALLOW_PLAINTEXT`

**The server refuses to start unless one of these is decided.** A bearer token
travels in an HTTP header; without TLS anyone on the path can read it and replay
it for as long as it lives. That is not a risk a default can weigh, so it is not
one the binary guesses at.

Either set both halves of a TLS identity:

```sh
HETIDA_TLS_CERT=/etc/hetida/chain.pem HETIDA_TLS_KEY=/etc/hetida/key.pem
```

or decline TLS in writing:

```sh
HETIDA_ALLOW_PLAINTEXT=1
```

Setting only one half of the identity is a mistake rather than a request for
plaintext, and is refused. Affirmative values are `1`, `true` and `yes`, in any
case; anything else — including `0` and `false` — is not consent.

TLS is terminated by `tonic` using `rustls` with the `ring` backend, chosen over
`aws-lc` because it builds without `cmake` or a C toolchain.

### `HETIDA_USERS` and `HETIDA_ALLOW_ANONYMOUS`

**The server also refuses to start unless authorization is decided.** Grants are
per table and per direction (§9); table level is the natural granularity because
a table is what a `do_put` descriptor names and what a `do_get` query selects
from.

One entry per user, separated by `;`:

```
name:password:read=<tables>:write=<tables>
```

- `<tables>` is a comma-separated list, or `*` for every table — including tables
  that do not exist yet, since a table appears when a client first writes to it (§6).
- A missing `read` or `write` clause grants nothing. A grant nobody wrote down is
  not a grant.
- A malformed entry refuses the **whole** configuration rather than being skipped,
  so a typo cannot silently leave one user unauthenticated while the rest works.

```sh
HETIDA_USERS='ingest:$PW1:write=readings,events;analyst:$PW2:read=readings'
```

Or decline authentication in writing, which grants every caller everything:

```sh
HETIDA_ALLOW_ANONYMOUS=1
```

What a grant covers:

- **Write** is checked against the table in the `do_put` descriptor path, once
  per call — never per message. One stream carries thousands of `RecordBatch`es
  behind that single check, and repeating it would put credential validation in
  the ingestion hot path (§9).
- **Read** is checked against every table the *planned* query touches, so a view,
  a subquery or a join cannot reach round a grant. A query naming several tables
  needs every one of them.
- `<table>`, `"<table>$history"` and the internal `"<table>$raw"` are three views
  of one table's rows and share one grant.
- The registry catalog tables `hetida_series` and `hetida_labels` are ordinary
  table names and need granting by name. Series names and label values are
  metadata, but they are still the archive's data.
- `get_schema` and `get_flight_info` need the same read grant the rows do, or a
  schema would describe a table to a client that may not see inside it.

Credentials are checked through a `CredentialStore` trait; the static
configuration above is the v1 backend. OIDC/JWT and mTLS client certificates are
later backends, which is why authentication answers with a principal rather than
a verdict — a backend that learns grants from the credential itself fits without
changing anything above it (§9).

Tokens are 32 random bytes from the operating system, hex encoded, valid for
**12 hours**, and held in memory: a restart invalidates every one of them. That
is correct — the archive's durable state is its data, not its sessions, and a
client already has to handle re-handshaking.

## Instance metadata

`instance.meta` records what an instance was initialised with. It is written once,
on the first start against an empty directory, and validated at every later start.

| Key | Values | Default |
|---|---|---|
| `partition_interval` | `hour`, `day`, `month`, `year` | `month` |

**The partition interval is immutable once data exists** (§11). It is baked into
the directory structure, so changing it would mean rewriting every file. A
configured interval that disagrees with the recorded one is a startup error, never
a silent second layout beside the first. Partitions present with the metadata
missing is also a startup error: the layout cannot be inferred, and a wrong guess
is exactly the second layout the check exists to prevent.

Unknown keys are ignored, so a newer build may add fields without an older one
refusing to start. A missing or unparsable `partition_interval` is an error rather
than a fall back to the default.

Choosing the interval:

| Interval | Path | Suits |
|---|---|---|
| `hour` | `year=YYYY/month=MM/day=DD/hour=HH` | very high rate, or retention finer than a day |
| `day` | `year=YYYY/month=MM/day=DD` | high rate |
| `month` | `year=YYYY/month=MM` | the default — weeks-to-months queries over few metrics |
| `year` | `year=YYYY` | very low rate |

The default is `month` because the typical query spans weeks to months over a few
metrics, which a month-granular layout answers from one to three directories where
a day-granular one opens thirty to ninety and pays a footer read on each to
retrieve a few kilobytes per metric — metadata overhead, not scan volume, dominates
that pattern (§11).

The interval also bounds retention: whole partitions are dropped, so **retention
resolution can never be finer than the partition interval** (§8). Choose the two
together.

## Compiled defaults

These are settled in code and are not reachable from the binary today — see
[the gap](#what-an-operator-cannot-set-yet). They are documented because they
describe what the server actually does, and because embedding the crate is a
supported way to change them.

### Archive (`ArchiveConfig`, `src/archive.rs`)

| Setting | Default | What it costs |
|---|---|---|
| `partition_interval` | `month` | Validated against `instance.meta` — see above |
| `max_memtable_bytes` | 1 GiB | Past this, `do_put` answers `RESOURCE_EXHAUSTED` rather than buffering without bound (§7). Large enough that a healthy instance never meets it, small enough that an overloaded one refuses before the host does |
| `default_table` | see below | Applies to any table no entry names |
| `tables` | none | Per-table overrides (§6) |

The cap counts frozen memtables as well as active ones, because a frozen table
still occupies memory until its Parquet file exists. A stalled flush is exactly
when the cap must bite.

### Per table (`TableConfig`, `src/catalog.rs`)

| Setting | Default | What it costs |
|---|---|---|
| `suppression.window` | 5 min | Ingest-time span over which a byte-identical resend is recognised as a retransmission rather than recorded as a new version (§1) |
| `suppression.max_entries` | 2 000 000 | Entries one generation holds before the window is cut short — roughly 200 MB across both generations |
| `retention` | none — keep forever | Event-time horizon (§8) |

Two presets, and the choice is about the producer's rhythm rather than the
transport (§1, §6):

- `SuppressionConfig::for_stream()` — the default. Five minutes, covering transport
  retries only. A stream's keys never recur, so every entry is dead weight once its
  retry window has passed.
- `SuppressionConfig::for_recomputed(interval)` — for a table a scheduled job
  rewrites. A job recomputing its horizon hourly resubmits values that mostly have
  not changed, and the reach has to span the gap between two runs or **every run
  manufactures a version**. Pass the run interval generously: a run that slips past
  the window is simply not recognised. Memory does not grow with ingest here, since
  the same keys recur every run.

Retention is unset by default, and an unconfigured table is never even examined by
the reaper. An archive nobody gave a horizon cannot lose anything. When it is set:

- A partition is dropped only once its **entire interval** is past the cutoff, so
  coarse intervals over-retain — with `month` partitions and a 90-day policy, data
  survives up to roughly 120 days (§8).
- Ingesting into a partition the reaper could already drop is refused with
  `FAILED_PRECONDITION`. A row behind the horizon that shares a partition with live
  data is still accepted, because that partition is kept and served.
- Version retention is a separate, unbounded dimension: every correction is kept
  and time travel reaches back to the first write (§1, §8).

### Write-ahead log (`WalConfig`, `src/wal.rs`)

| Setting | Default | What it costs |
|---|---|---|
| `segment_bytes` | 64 MiB | Rotation point. Smaller segments reclaim sooner and cost more files |
| `max_commit_delay` | 10 ms | How long the writer waits for stragglers before syncing. Larger amortises the fsync across more writers at the cost of acknowledgement latency; zero groups only what is already queued (§4) |

An acknowledgement means the rows survive a crash, so this delay is added to every
`do_put` acknowledgement — it buys throughput with latency, and nothing else.

### Flush (`FlushConfig`, `src/flush.rs`)

| Setting | Default | What it costs |
|---|---|---|
| `max_bytes` | 64 MiB | Per-partition size trigger. Large enough for row groups worth pruning, small enough that several partitions stay inside a memory budget |
| `max_age` | 5 min | Per-partition age trigger, measured against the clock |

Whichever fires first (§2). Age is what bounds the data-loss window and the query
staleness of a low-rate metric — one written once an hour would otherwise sit in
memory indefinitely. It is measured against the clock rather than ingest time, so
a quiescent instance still sheds its state.

### Compaction (`CompactConfig`, `src/compact.rs`)

| Setting | Default | What it costs |
|---|---|---|
| `max_files` | 4 | Compact once a partition holds this many files. Beyond four, footer reads start to dominate (§11) |
| `max_bytes` | 256 MiB | Compact once the partition totals this much |
| `target_bytes` | 128 MiB | Soft target per output file — a file is cut at the next key boundary past it, so output ranges stay disjoint (§3) |

A single file is never compacted: merging one file into one file changes nothing
and would rewrite the partition on every pass.

### Parquet (`ParquetConfig`, `src/parquet.rs`)

| Setting | Default | What it costs |
|---|---|---|
| `zstd_level` | 3 | ZSTD's own default: most of the ratio for little of the cost |
| `row_group_rows` | 131 072 | The unit of skipping. Smaller groups prune more finely and cost more metadata |

Page-level statistics are written on every column, `system_time` included, so an
old observation point skips rows written after it (§1).

### Background timers (`src/main.rs`)

| Timer | Interval | Why |
|---|---|---|
| Flush | 10 s | Far below the age bound it enforces, so a partition is flushed close to when it becomes due |
| Compaction | 5 min | Compaction rewrites a whole partition where a flush appends one file; sharing the flush tick would compact far more often than needed |
| Retention | 15 min | Comfortably below the finest partition interval, which is an hour |

## What an operator cannot set yet

The binary reads the seven environment variables above and takes **compiled
defaults for everything else**. Retention in particular is implemented, tested and
documented, but `ArchiveConfig::tables` is empty in `main.rs`, so there is no way
to give a table a horizon without embedding the crate:

```rust
use hetida_db::archive::ArchiveConfig;
use hetida_db::catalog::TableConfig;

const DAY: i64 = 86_400_000_000;

let config = ArchiveConfig {
    tables: vec![(
        "readings".to_owned(),
        TableConfig {
            retention: Some(90 * DAY),
            ..TableConfig::default()
        },
    )],
    ..ArchiveConfig::default()
};
```

A configuration file covering the compiled defaults is not yet written. Until it
is, treat this section as the specification of what such a file would carry.

## Query surface

Not configuration, but a client cannot use a grant without knowing the names it
covers. Each table appears three times, and `$` needs quoting in SQL:

| Name | Holds |
|---|---|
| `readings` | one row per `(metric_id, timestamp)` — the version current at the observation point |
| `"readings$history"` | every version, with `system_time` and `seq` as ordinary columns |
| `hetida_series` | one row per registered series: `metric_id`, `name`, `labels` |
| `hetida_labels` | one row per label: `metric_id`, `label`, `value` |

The observation point travels in the `Ticket` beside the SQL, as
`hetida.v1.Query { sql, as_of_micros }`, and bounds `$history` too — seeing
versions written after the point one observes from would contradict the point.

Select series by label with `hetida_label(label, value)`, which is resolved
against the registry before planning and becomes a filter on `metric_id` that
row-group statistics can prune (§10, §11):

```sql
SELECT * FROM readings WHERE hetida_label('plant', 'a')
```

A join against `hetida_labels` answers the same question correctly, but reaches
the scan as a join and reads every row group.
