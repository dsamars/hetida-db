<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Configuration

Every setting hetida-db has, where it is set, and what choosing it wrong costs.
Section references (§n) point at the decision in [`ARCHITECTURE.md`](ARCHITECTURE.md)
that the setting serves.

Almost everything is in one file, and the exceptions are worth knowing:

| Group | Set by | Changeable |
|---|---|---|
| [The configuration file](#the-configuration-file) | the path in `HETIDA_CONFIG` | every start |
| [The environment](#the-environment) | `HETIDA_CONFIG`, and nothing else | every start |
| [Instance metadata](#instance-metadata) | recorded at initialisation | never, once data exists |

**One place to configure means no precedence rules.** Nothing is settable twice,
so there is never a question of which setting won — and a variable that used to
work but no longer does is a refusal to start, not a silent default.

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

### What `data_dir` holds

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
# Root of the archive: logs, Parquet and instance metadata.
data_dir = "hetida-data"

[server]
listen = "127.0.0.1:50051"
# A PEM certificate chain and private key. Both together, or neither with
# allow_plaintext — see "Deciding TLS and authorization" below.
# tls_certificate = "/etc/hetida/chain.pem"
# tls_key = "/etc/hetida/key.pem"
allow_plaintext = false

[auth]
allow_anonymous = false
# users = [] — see "Users and grants" below.

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

[observability]
# How much the server says: error, warn, info, debug or trace. Nothing else is
# accepted — a directive parser would read a typo as a *target* of that name and
# then log nothing at all (§13).
level = "info"
# How each event is rendered: text for a terminal, json for a collector.
format = "text"
# filter is unset. It takes a full tracing directive and overrides `level`, for
# what a level cannot express: filter = "info,hetida_db::wal=debug"

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

### Deciding TLS and authorization

**The server refuses to start until both are decided.** A bearer token travels
in an HTTP header; without TLS anyone on the path can read it and replay it for
as long as it lives. And an archive serving `do_put` to anyone who can reach the
port is a defensible choice on a private network and a disaster anywhere else.
Neither is a risk a default can weigh.

Either give the listener a TLS identity:

```toml
[server]
tls_certificate = "/etc/hetida/chain.pem"
tls_key = "/etc/hetida/key.pem"
```

or decline TLS in writing:

```toml
[server]
allow_plaintext = true
```

Setting only one half of the identity is a mistake rather than a request for
plaintext, and is refused. So is setting an identity *and* `allow_plaintext`:
both cannot be meant, and choosing one would be exactly the guess this refuses
to make.

TLS is terminated by `tonic` using `rustls` with the `ring` backend, chosen over
`aws-lc` because it builds without `cmake` or a C toolchain. Both PEM files are
read **at startup**, so a missing or unreadable identity stops the start rather
than failing the first connection that needs it.

`tests/tls.rs` drives the whole chain against a certificate generated for the
run: TOML text → `[server]` → PEM read → a real handshake → `do_put` and
`do_get` over the connection. It also confirms that a plaintext client cannot
reach the listener, and that a client which does not trust the certificate is
refused.

### Users and grants

```toml
[[auth.users]]
name = "ingest"
password_file = "/run/secrets/ingest"
write = ["readings", "events"]

[[auth.users]]
name = "analyst"
password = "hunter2"
read = ["readings"]

[[auth.users]]
name = "admin"
password_file = "/run/secrets/admin"
read = "*"
write = ["*"]
```

| Key | Meaning |
|---|---|
| `name` | The name the client authenticates as |
| `password` | The password, literally — see the warning below |
| `password_file` | A file holding it instead, so it is not in this file at all |
| `read` / `write` | Tables granted: a list, a bare name, or `"*"` for every table |

- Exactly one of `password` and `password_file` per user. Both, or neither, is
  refused rather than resolved.
- A `password_file` is read **at startup**, so an unreadable secret fails the
  start rather than the first handshake that needs it hours later. One trailing
  newline is stripped, because that is what `echo` and every secret-mounting
  tool leaves behind; nothing else is, since a password may legitimately begin
  or end with a space.
- `"*"` grants every table, **including tables that do not exist yet** — a table
  appears when a client first writes to it (§6).
- A missing `read` or `write` grants nothing. A grant nobody wrote down is not a
  grant.

Or decline authentication entirely, which grants every caller everything:

```toml
[auth]
allow_anonymous = true
```

Listing users *and* allowing anonymous access is refused: anonymous access
grants everything, so the grants would mean nothing.

> **Prefer `password_file`.** A literal password makes the configuration a
> secret, and a configuration is usually not treated as one. A start that finds
> literal passwords in a file readable beyond its owner says so — but it is a
> warning, not a refusal: the permission bit is a hygiene signal, not an
> undecided policy, and failing a deploy over an ownership arrangement this
> process cannot fully judge would be the wrong trade.

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
a verdict — a backend that learns the grants from the credential itself fits
without changing anything above it (§9).

Tokens are 32 random bytes from the operating system, hex encoded, valid for
**12 hours**, and held in memory: a restart invalidates every one of them. That
is correct — the archive's durable state is its data, not its sessions, and a
client already has to handle re-handshaking.

### What the server says about itself

`[observability]` is the whole of it (§13). There is no `RUST_LOG`:
`HETIDA_CONFIG` is the only environment variable this server reads, and a second
one governing diagnostics would put them somewhere you are not looking.

| Key | Default | What it does |
| --- | --- | --- |
| `level` | `info` | `error`, `warn`, `info`, `debug` or `trace`, and nothing else. |
| `format` | `text` | `text` for a terminal, `json` for one object per event. |
| `filter` | unset | A full directive, overriding `level`. Validated when the file is read. |

**`level` is a closed set on purpose.** Writing `level = "verbose"` is refused
rather than accepted, because the directive syntax underneath would read
`verbose` as the name of a *target* and enable it — leaving a server that starts
cleanly and says nothing, which looks exactly like a server with no traffic.

**`filter` is the escape hatch**, for the case a single level will not do:

```toml
[observability]
filter = "info,hetida_db::wal=debug,hetida_db::compact=trace"
```

It is the more dangerous of the two, deliberately. It *is* lenient about unknown
targets — a module you name may simply not have been reached yet — so a
misspelled module there is silently ineffective. Use `level` unless you have a
reason not to.

Events go to **stderr**, so piping the server's stdout somewhere never mixes
diagnostics into it. What each level carries:

- **`error`** — something stopped. Currently only a failure to listen for
  shutdown signals; the archive itself refuses to start rather than logging.
- **`warn`** — something failed and will be retried, or a client was refused: a
  torn log tail, a failed flush (with the note that ingestion will be refused if
  memory fills — §7), a failed compaction, partitions that could not be dropped,
  a refused write or a query that would not plan. Also the two configuration
  decisions worth saying out loud, serving without TLS and without authorization.
- **`info`** — what an operator wants to see happen: the archive opening, the
  listener, each compacted or dropped partition, and the stop.
- **`debug`** — per call and per batch: each ingested batch with its row count,
  each planned query. Never per row (§13).

A pass that did nothing says nothing. A reaper that dropped no partition and a
compactor with nothing due are both silent, because an archive that logs a line
per tick per table trains you to ignore the log.

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

## The environment

One variable.

| Variable | Default | Meaning |
|---|---|---|
| `HETIDA_CONFIG` | — | Path to the configuration file; unset means the defaults |

Everything else lives in the file it names. That is one place to configure, so
there are no precedence rules to get wrong — and it keeps passwords out of an
environment that `ps` and `/proc` expose to every user on the host.

### Variables that were retired

A start **refuses** while any of these is still set, and says where it moved.
Ignoring a variable that used to work would deploy a server listening somewhere
else, or with its credentials silently dropped.

| Retired | Now |
|---|---|
| `HETIDA_DATA_DIR` | `data_dir` |
| `HETIDA_LISTEN` | `[server] listen` |
| `HETIDA_TLS_CERT` | `[server] tls_certificate` |
| `HETIDA_TLS_KEY` | `[server] tls_key` |
| `HETIDA_ALLOW_PLAINTEXT` | `[server] allow_plaintext` |
| `HETIDA_USERS` | `[[auth.users]]` entries |
| `HETIDA_ALLOW_ANONYMOUS` | `[auth] allow_anonymous` |

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
