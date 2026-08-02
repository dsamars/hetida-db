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
| [The configuration file](#the-configuration-file) | `HETIDA_CONFIG` | every start |
| [Instance metadata](#instance-metadata) | recorded at initialisation | never, once data exists |

The split between the first two is deliberate: **the environment says where the
archive is deployed and how it is secured, the file says how it behaves.**
Nothing is settable in both, so there is never a question of which one won.

## Environment variables

Deployment and security. Everything about the archive's behaviour is in
[the file](#the-configuration-file).

| Variable | Default | Meaning |
|---|---|---|
| `HETIDA_DATA_DIR` | `hetida-data` | Root of the archive: logs, Parquet and instance metadata |
| `HETIDA_LISTEN` | `127.0.0.1:50051` | Socket address to serve Arrow Flight on |
| `HETIDA_CONFIG` | — | Path to the configuration file; unset means the defaults |
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

Credentials live in the environment rather than in the configuration file, which
is a deliberate split but not an ideal one: an environment variable is visible in
`ps` and in a process's `/proc` entry. Moving them into the file — or behind one
of the backends below — is the obvious next step.

Credentials are checked through a `CredentialStore` trait; the static
configuration above is the v1 backend. OIDC/JWT and mTLS client certificates are
later backends, which is why authentication answers with a principal rather than
a verdict — a backend that learns grants from the credential itself fits without
changing anything above it (§9).

Tokens are 32 random bytes from the operating system, hex encoded, valid for
**12 hours**, and held in memory: a restart invalidates every one of them. That
is correct — the archive's durable state is its data, not its sessions, and a
client already has to handle re-handshaking.

## The configuration file

`HETIDA_CONFIG` names a TOML file describing how the archive behaves. Unset means
the defaults below, which are safe. A file that *is* named but cannot be read,
parsed or understood **stops the start** — an operator who wrote a configuration
meant it to take effect, and starting with defaults instead would be the silent
kind of wrong.

**Unknown keys are rejected.** This file is written by hand, and a misspelled
`retention` that is quietly ignored is the costliest failure there is to find.
(Instance metadata takes the opposite rule, for the opposite reason — it is
machine-written, and tolerating unknown keys is what lets an older build read a
newer one's file.)

### Units

Durations carry a suffix; sizes may. A bare number is bytes for a size and an
**error** for a duration: there is no defensible default unit for time here, and
reading `5` as seconds where an operator meant milliseconds changes what the
setting means rather than how large it is.

| | Accepted |
|---|---|
| Duration | `ms`, `s`, `m`, `h`, `d` — `"10ms"`, `"5m"`, `"90d"` |
| Size | `B`, `KiB`, `MiB`, `GiB`, and the 1000-based `KB`, `MB`, `GB`; or a bare number of bytes |

Case does not matter and a space before the unit is fine, so `"512 MiB"` and
`"512mib"` both work.

### A complete file, at its defaults

Every key is optional. This is what the archive does if the file is absent.

```toml
# Layout granularity, validated against instance.meta (§11). Immutable once
# data exists — see below.
partition_interval = "month"

# Past this, do_put answers RESOURCE_EXHAUSTED rather than buffering without
# bound (§7). Counts frozen memtables too, so a stalled flush makes the cap
# bite instead of looking free.
max_memtable_bytes = "1GiB"

[wal]
# Rotation point. Smaller segments reclaim sooner and cost more files.
segment_bytes = "64MiB"
# How long the writer waits for stragglers before syncing (§4). An
# acknowledgement waits for its fsync, so this is added to every do_put: it
# buys throughput with latency and nothing else. "0ms" groups only what is
# already queued.
max_commit_delay = "10ms"

[flush]
# Per-partition size trigger. Large enough for row groups worth pruning, small
# enough that several partitions stay inside a memory budget.
max_bytes = "64MiB"
# Per-partition age trigger, measured against the clock. Whichever fires first
# (§2). Age is what bounds the staleness of a low-rate metric, which the size
# cap alone never would.
max_age = "5m"
# How often the flusher looks. Far below max_age, so a partition is flushed
# close to when it becomes due rather than a whole tick late.
tick = "10s"

[compaction]
# Compact once a partition holds this many files. Beyond four, footer reads
# start to dominate (§11).
max_files = 4
# Or once the partition totals this much.
max_bytes = "256MiB"
# Soft target per output file: a file is cut at the next key boundary past it,
# so output ranges stay disjoint (§3).
target_bytes = "128MiB"
# Compaction rewrites a whole partition where a flush appends one file, so it
# runs far less often.
tick = "5m"

[parquet]
# ZSTD's own default: most of the ratio for little of the cost.
zstd_level = 3
# The unit of skipping. Smaller groups prune more finely and cost more
# metadata (§11).
row_group_rows = 131072

[retention]
# How often the reaper looks. Comfortably below the finest partition interval,
# which is an hour.
tick = "15m"

# Applies to any table [tables.*] does not name.
[table_defaults]
# Ingest-time span over which a byte-identical resend is recognised as a
# retransmission rather than recorded as a new version (§1).
suppression_window = "5m"
# Entries one generation holds before the window is cut short — roughly 200 MB
# across both generations.
suppression_max_entries = 2000000
# retention is unset: keep forever. Retention deletes, so it is never inferred.
```

### Per-table settings

A `[tables.<name>]` section starts from `[table_defaults]` and overrides only
what it names, so setting one field never silently clears the rest.

```toml
[table_defaults]
suppression_window = "5m"

# A job recomputing its horizon every hour resubmits values that mostly have
# not changed. The reach has to span the gap between two runs, or every run
# manufactures a version (§1, §6).
[tables.forecast]
suppression_window = "1h"
retention = "400d"

# A measurement stream: keys never recur, so the default minutes are right and
# a longer window would only cost memory.
[tables.readings]
retention = "90d"

# Named nowhere, so: default suppression, and kept forever.
# [tables.audit]
```

| Key | Default | Meaning |
|---|---|---|
| `suppression_window` | `"5m"` | Reach of unchanged-value suppression (§1) |
| `suppression_max_entries` | 2 000 000 | Entries before the window is cut short |
| `retention` | unset — keep forever | Event-time horizon (§8) |

Two things about retention are easy to get wrong:

- **Resolution can never be finer than the partition interval,** because whole
  partitions are dropped. With `month` partitions and `"90d"`, data survives
  until its whole month clears the cutoff — up to roughly 120 days (§8). Choose
  the two together.
- **Version retention is a separate, unbounded dimension.** Every correction is
  kept and time travel reaches back to the first write (§1, §8). `retention`
  drops whole partitions by event time; it does not thin the history inside the
  ones that survive.

Ingesting into a partition the reaper could already drop is refused with
`FAILED_PRECONDITION`. A row behind the horizon that shares a partition with
live data is still accepted, because that partition is kept and served.

## Instance metadata

`instance.meta` records what an instance was initialised with. It is written once,
on the first start against an empty directory, and validated at every later start
against `partition_interval` in [the configuration file](#the-configuration-file).

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
