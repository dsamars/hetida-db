<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

## What this changes, and why

<!-- The problem first, then the change. If it fixes an issue: "Closes #123". -->

## The test that demanded it

<!--
Name the test written before the code, and where it lives. If it guards a
decision in ARCHITECTURE.md, name the § — the test should carry the same
reference in a comment.

For a subtle invariant, say how you confirmed the test can fail: what you broke,
and that it went red.
-->

## Checklist

- [ ] The failing test came first, and the code is the simplest thing that passes it
- [ ] `cargo fmt --all` clean
- [ ] `cargo clippy --all-targets -- -D warnings` clean
- [ ] `cargo test` clean
- [ ] Every new item has a doc comment; every new `.rs` file has the SPDX header
- [ ] No `unwrap`/`expect` in ingestion, WAL parsing or Flight handlers
- [ ] No per-row work added to `do_put` or `do_get`
- [ ] `CHANGELOG.md` under `Unreleased` — unless this changes nothing a user can observe

## Decisions touched

<!--
Which §n in ARCHITECTURE.md this works under. If it contradicts one, stop and
open an issue instead: decisions are not re-decided in a pull request.

If it changes the on-disk layout, the configuration file or the Flight contract,
say so here — that is a breaking change and belongs in the changelog as one.
-->

## Performance

<!--
Delete if not applicable. Otherwise: `cargo bench` and/or the loadgen harness,
before and after, from the same machine. Absolute numbers are worth less than a
comparison against yourself.
-->
