<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# hetida-db

High-performance time-series archiver built on Apache Arrow Flight, Apache Parquet and Apache DataFusion.

Clients write and read Arrow `RecordBatch` streams over pure Arrow Flight. Incoming data is made durable in a write-ahead log, buffered in an in-memory table, and archived into date-partitioned Parquet files, which DataFusion queries alongside the not-yet-flushed memory state.

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — system design and the settled design decisions
- [`CLAUDE.md`](CLAUDE.md) — performance, quality and documentation rules for contributors
- [`tasks.md`](tasks.md) — phased implementation plan

## Status

Early development. No functionality is implemented yet — see `tasks.md`.

## Author

Heiko Dietz

## License

Released under the MIT License. See [`LICENSE`](LICENSE).

## Trademark

*hetida* is a registered trademark of fuseki GmbH.

The MIT License covers the source code only and grants no rights to the *hetida* name, logo or any other fuseki GmbH trademark. Forks and derivative works must be distributed under a different name.
