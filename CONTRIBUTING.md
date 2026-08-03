<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Contributing to hetida-db

Contributions are welcome. This document is what a pull request is judged
against, so it is worth reading before writing code rather than after.

The rules below are not style preferences — most of them constrain the design,
and a few of them are the reason the archive is correct. Where one seems to be
in the way, say so in an issue; do not work around it quietly.

## Before you start

**Read [`ARCHITECTURE.md`](ARCHITECTURE.md) first.** The settled design
decisions live there, numbered §1 through §11, each with its rationale and the
alternatives that were rejected. They are not re-decided in a pull request. If
you believe one is wrong, open an issue arguing against the recorded rationale —
that is a shorter conversation than a pull request that assumes the opposite.

Open an issue before a change of any size. A pull request that arrives without
one may be right and still be closed, because agreeing on the problem is cheaper
than reviewing the wrong solution.

## Branches and releases

`main` is protected, always releasable, and only reachable through a pull
request that passes CI. Work happens on short-lived branches off `main`:

| Prefix | For |
| --- | --- |
| `feature/…` | new behaviour |
| `fix/…` | a defect in released or unreleased code |
| `docs/…` | documentation only |
| `chore/…` | dependencies, CI, tooling |
| `release/x.y` | a maintenance line, cut by a maintainer at release time |

Keep a branch short. A branch that lives a week is a branch that will conflict.

Releases are tagged `vX.Y.Z` on `main`. At each minor release a `release/x.y`
branch is cut and kept for patch releases: a fix lands on `main` first and is
then cherry-picked back, never the other way round. This is what lets someone
pinned to `0.1` take a fix without also taking a new on-disk format. Before
1.0 a minor bump may break anything — see [`CHANGELOG.md`](CHANGELOG.md).

The release procedure itself is in [`RELEASING.md`](RELEASING.md).

## Test-driven, in that order

Development is strictly test-driven, and this is enforced in review.

- **Write the failing test first**, then the simplest code that passes it, then
  refactor. Production code that no failing test asked for is removed in review,
  not merged with a promise of tests later.
- **Every decision in `ARCHITECTURE.md` needs a test that fails when it is
  violated.** That, not line coverage, is what "tested" means here. A test
  guarding a decision names it in a comment: `// §1: …`.
- **For a subtle invariant, check the test can actually fail.** Break the
  implementation briefly, watch the test go red, put it back. A property test
  that passes against a broken implementation is worse than no test, because it
  is believed.
- **Deterministic only.** No `sleep`, no wall-clock assertions, no fixed ports.
  Wall-clock time comes from the injectable `Clock`, `seq` from the injectable
  sequence source, and time-dependent async paths use `tokio::time::pause()`.
  Servers bind port 0. A flaky test is treated as a failing test.
- **Invariants get property tests** (`proptest`), because examples cannot cover
  version resolution at an arbitrary observation point, sort order surviving
  flush and compaction, or suppression being idempotent.

**Test layout:** unit tests in `#[cfg(test)] mod tests` beside the code,
properties in `tests/invariants.rs`, cross-component tests in `tests/pipeline.rs`,
system-level checks in `tests/verification.rs` and `tests/authorization.rs`.
Extend those rather than adding a parallel harness.

Test modules carry
`#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic, clippy::unreachable)]`.
Tests may panic freely; production code may not.

## What the code must not do

These are the rules a reviewer will check first, because breaking one is not
visible in the diff's behaviour:

- **No per-row work in the hot path.** `do_put` and `do_get` pass `RecordBatch`
  instances through unchanged. No conversion into Rust structs, no row-by-row
  iteration, no re-batching outside the flush path.
- **No `unwrap()` or `expect()`** in ingestion, WAL parsing or Flight handlers.
  Those modules deny it mechanically; do not add an `allow`.
- **No wall-clock ordering.** Ordering and version resolution use the monotonic
  `seq`. `system_time` is a user-visible stamp, not an ordering source.
- **No version is ever discarded.** Not in the memtable, not on flush, not in
  compaction. The two exceptions are retransmission suppression and retention
  (§1, §8), and neither is extended without a decision recorded in
  `ARCHITECTURE.md`.
- **Sort order is preserved.** Every flushed and compacted file is ordered by
  `(metric_id, timestamp, seq)`; resolution silently degrades to a full sort if
  it is not.
- **No `unsafe`** unless SIMD or Arrow requires it, and then with a `# Safety`
  block that explains why it holds.
- **Every item is documented.** Modules, structs, enums, traits, functions and
  methods carry a doc comment saying what they do and what the caller must
  uphold — short and substantive, not a restatement of the signature.
- **Every `.rs` file starts with** the SPDX identifier and copyright notice;
  Markdown and TOML carry the same two lines as comments.

The full list, with the reasoning, is in [`CLAUDE.md`](CLAUDE.md).

## Dependencies

Most updates arrive as Dependabot pull requests and are reviewed like any other.
Two exceptions are worth knowing before you open one yourself:

- **The Arrow stack moves by hand, DataFusion first.** `arrow`, `arrow-flight`,
  `parquet` and `object_store` must resolve to the Arrow major that `datafusion`
  resolves, or their types conflict across crate boundaries. Dependabot ignores
  those four; it watches `datafusion`, because that is the crate whose version
  decides the others. When its pull request appears, pick the DataFusion version
  and *derive* the rest from what it depends on — never the reverse — and bump
  them in one commit. That pull request will not build until you do.
- **The MSRV is not a dependency.** `dtolnay/rust-toolchain@1.88` in
  `.github/workflows/ci.yml` is the minimum supported Rust version, not a pinned
  action to keep current. It changes only together with `rust-version` in
  `Cargo.toml`, and raising either is a breaking change for anyone pinned to the
  old toolchain.

## Commit messages

Commit subjects are prose in the imperative, and the body says *why* — including
when it says that an earlier decision turned out to be wrong. This project does
not use Conventional Commits: `feat(wal): add crc` records less than
`Give each WAL record its own CRC` does, and the changelog is written by hand so
nothing depends on parsing the subject line.

```
Hold a frozen memtable until its file exists

`freeze` used to hand away the only reference to the frozen table, so its rows
vanished from queries for the length of a flush and were lost outright when the
write failed. The active table now keeps it until the Parquet file is on disk.
```

Reference the decision you are working under (`§4`) where it is not obvious.
Do not add `Co-Authored-By` trailers for assistants.

## The quality gate

Before opening a pull request, and before every push to it:

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

All three must be clean. CI runs the same three on Linux and macOS, plus
`cargo doc` with warnings denied and a build against the minimum supported Rust
version (1.88, which is what `datafusion` and `tonic` require).

Benchmarks are not tests and are not run in CI. If a change could plausibly cost
throughput or query latency, run `cargo bench` and
`cargo run --release --example loadgen` before and after, and put both numbers in
the pull request. Numbers from your own machine compared against themselves are
worth more than absolute ones.

## Licensing and the name

Contributions are accepted under the MIT License, the same terms the project is
released under. There is no CLA; opening a pull request is your agreement that
your contribution may be distributed under those terms.

*hetida* is a registered trademark of fuseki GmbH. The MIT License covers the
source code only and grants no trademark rights. A fork distributed publicly
must be renamed.

## Reporting a vulnerability

Not here, and not in a public issue — see [`SECURITY.md`](SECURITY.md).
