<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Security Policy

hetida-db terminates TLS, issues and validates bearer tokens, and enforces
per-table read and write grants against client-supplied SQL. A defect in any of
those is a security defect, not a bug.

## Supported versions

| Version | Supported |
| --- | --- |
| `0.1.0-alpha.x` | yes — the current alpha, and the only line that receives fixes |

The project is pre-1.0. There is no long-term support line yet; once `0.1.0`
ships, its `release/0.1` branch receives security fixes for as long as it is the
newest released minor.

## Reporting a vulnerability

**Do not open a public issue.**

Report privately through GitHub:
[Security → Report a vulnerability](https://github.com/dsamars/hetida-db/security/advisories/new).
This opens a draft advisory only the maintainers and you can read. If that is
not available to you, email <hdietz@outlook.com> instead.

Useful in a report: the version or commit, the configuration involved
(especially whether `allow_plaintext` or `allow_anonymous` was set), and the
smallest sequence of Flight calls that reproduces it. A failing test against the
existing suite is the fastest possible report — see
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the test layout.

Expect an acknowledgement within a week. Fixes are developed in a private fork
and released together with an advisory crediting the reporter, unless the
reporter asks not to be named.

## What is in scope, and what an alpha already tells you

The following are **known and documented**, not vulnerabilities:

- **No query cost limits on client-supplied SQL** (§5, *Open Questions*). Any
  authenticated reader can plan a query that consumes the server. Do not grant
  read access to a client you would not also give a shell. Reports of this are
  welcome as issues, not as advisories.
- **`allow_plaintext = true` serves bearer tokens over an unencrypted
  connection**, and `allow_anonymous = true` disables authorization entirely.
  Both refuse to be defaults precisely because they are dangerous; choosing one
  is a deployment decision, not a defect.
- **Retransmission suppression uses a non-cryptographic digest** (xxh3), by the
  reasoning recorded in `Cargo.toml`: a client constructing a collision discards
  only its own correction.

In scope, and worth a private report: anything that lets a client read or write
a table it holds no grant for, escape the observation point in `$history`, reach
past the archive's `Storage` to the filesystem, bypass the interceptor on any
Flight method, or crash the server from an unauthenticated connection.
