<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# hetida-db

High-performance time-series archiver built on Apache Arrow Flight, Apache Parquet and Apache DataFusion.

Clients write and read Arrow `RecordBatch` streams over pure Arrow Flight. Incoming data is made durable in a write-ahead log, buffered in an in-memory table, and archived into date-partitioned Parquet files, which DataFusion queries alongside the not-yet-flushed memory state.

The archive is **bitemporal**: `timestamp` is valid time, `system_time`/`seq` is system time. Many versions of a logical row coexist, nothing is ever overwritten, and a query resolves the newest version at an optional observation point.

## Running

```sh
cargo build --release
HETIDA_CONFIG=/etc/hetida/hetida.toml ./target/release/hetida-db
```

`HETIDA_CONFIG` is the only environment variable. Everything else lives in the
file it names — one place to configure, so there are no precedence rules, and
passwords stay out of an environment that `ps` exposes to every user on the host.
A start refuses while a retired variable is still set, and says where it moved.

```toml
data_dir = "/var/lib/hetida"

[server]
listen = "0.0.0.0:50051"
tls_certificate = "/etc/hetida/chain.pem"
tls_key = "/etc/hetida/key.pem"

[[auth.users]]
name = "ingest"
password_file = "/run/secrets/ingest"   # kept out of the configuration entirely
write = ["readings"]

[[auth.users]]
name = "analyst"
password_file = "/run/secrets/analyst"
read = ["readings"]

[tables.readings]
retention = "90d"        # dropped by whole partitions, by event time (§8)

[tables.forecast]
suppression_window = "1h"   # a job recomputing its horizon hourly (§1)
```

**The server refuses to start until TLS and authorization are decided.** A bearer
token travels in a header, and an archive serving `do_put` to anyone who can
reach the port is a defensible choice on a private network and a disaster
anywhere else — neither is something a default can weigh. To decline either one,
say so:

```toml
[server]
allow_plaintext = true

[auth]
allow_anonymous = true
```

Full reference: [`CONFIGURATION.md`](CONFIGURATION.md). Three things are worth
knowing before the first start:

- **The partition interval is immutable once data exists.** It defaults to `month`, is recorded in `instance.meta` at initialisation, and a later start that disagrees is a startup error rather than a silent second layout. It also bounds retention, since whole partitions are dropped — choose the two together.
- **A named file that cannot be read stops the start, and unknown keys are rejected.** The file is written by hand, and a misspelled `retention` that is silently ignored is the costliest failure there is to find.
- **Prefer `password_file` to `password`.** A literal password makes the configuration a secret, and a configuration is usually not treated as one.

## Using it

Clients resolve series names and labels to `u64` ids once through the `register` action and cache them; `do_put` accepts ids only, so no name resolution happens in the hot path.

Each table is queryable three ways:

```sql
SELECT * FROM readings                          -- resolved at the observation point
SELECT * FROM "readings$history"                -- every version, system columns exposed
SELECT * FROM readings WHERE hetida_label('plant', 'a')
```

The observation point travels in the `Ticket` beside the SQL as `hetida.v1.Query { sql, as_of_micros }`. The control-plane contract is [`proto/hetida.proto`](proto/hetida.proto) — generate from it rather than reimplementing a byte layout.

## Documentation

- [`CONFIGURATION.md`](CONFIGURATION.md) — every setting and what choosing it wrong costs
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — system design and the settled design decisions
- [`CLAUDE.md`](CLAUDE.md) — performance, quality and documentation rules for contributors
- [`tasks.md`](tasks.md) — phased implementation plan
- [`CHANGELOG.md`](CHANGELOG.md) — what each release contains, and what it does not
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — branch model, the test-first workflow, and what a pull request is judged against
- [`RELEASING.md`](RELEASING.md) — how a release is cut
- [`SECURITY.md`](SECURITY.md) — reporting a vulnerability, and what an alpha already tells you

Building it needs Rust 1.88 or newer, which is what `datafusion` and `tonic`
require. `protoc` is not needed: the control-plane contract is compiled by
`protox`, in pure Rust.

## Status

**Alpha — `0.1.0-alpha.1`.** All four phases in [`tasks.md`](tasks.md) are
complete: the write path, Parquet archival with compaction and retention, the
query engine with as-of resolution, TLS and per-table authorization, and the
Phase 4 benchmarks and verification suite. 507 tests pass.

Nothing is stable yet. The on-disk layout, the configuration file and the Flight
contract may each change without a migration path before 0.1.0, and there are no
query cost limits — so do not expose it to clients you would not also give a
shell. [`CHANGELOG.md`](CHANGELOG.md) lists what the release contains and what it
does not.

Development is strictly test-driven, and `cargo test` and `cargo clippy -- -D warnings` must pass before any task is complete. TLS is covered end to end against a certificate generated for the run, so nothing about the secure path rests on inspection alone.

## Measuring

```sh
cargo bench                              # ingestion components, resolution, partition intervals
cargo run --release --example loadgen    # do_put throughput and do_get latency under load
```

The load harness is not `criterion`: network jitter invalidates its statistics, so the harness reports order statistics from raw samples instead. On loopback with 100 parallel clients it writes around 300 000 rows/s with a p99 acknowledgement latency of ~320 ms, while readers querying concurrently see a p99 of ~375 ms. Numbers from your own hardware are the only ones worth acting on — `HETIDA_LOAD_CLIENTS`, `HETIDA_LOAD_BATCHES`, `HETIDA_LOAD_ROWS`, `HETIDA_LOAD_READERS` and `HETIDA_LOAD_DIR` configure a run.

## Author

Heiko Dietz

## License

Released under the MIT License. See [`LICENSE`](LICENSE).

## Trademark

*hetida* is a registered trademark of fuseki GmbH.

The MIT License covers the source code only and grants no rights to the *hetida* name, logo or any other fuseki GmbH trademark. Forks and derivative works must be distributed under a different name.
