<!-- SPDX-License-Identifier: MIT -->
<!-- Copyright (c) 2026 Heiko Dietz -->

# Releasing

For maintainers. The branch model this fits into is described in
[`CONTRIBUTING.md`](CONTRIBUTING.md#branches-and-releases).

## Versions

`vMAJOR.MINOR.PATCH`, tagged on the branch the release is cut from. Pre-1.0, a
minor bump may break anything, including the on-disk layout; a patch bump never
does. Pre-releases are `0.1.0-alpha.N` and `0.1.0-rc.N`, in that order.

Every release is tagged `v` + the exact `version` in `Cargo.toml`. The two never
disagree, because the tag is what someone bisects against.

## Cutting a release

1. **Confirm the gate on the branch being released.** Not from cache, not from a
   green run on an older commit:

   ```sh
   cargo fmt --all --check
   cargo clippy --all-targets -- -D warnings
   cargo test
   ```

2. **Write the changelog entry.** Move `Unreleased` into a dated version heading
   in [`CHANGELOG.md`](CHANGELOG.md) and add the new link reference at the
   bottom. Write *Known limitations* at the same length as the feature list —
   someone deciding whether to run this needs the second list more than the
   first. Carry anything still open from *Open Questions* in `ARCHITECTURE.md`.

3. **Bump `version` in `Cargo.toml`** and run `cargo check` so `Cargo.lock`
   follows in the same commit.

4. **Commit on a `release/…` pull request** — `main` is protected, so this goes
   through review like anything else. Subject: `Release X.Y.Z`.

5. **Tag the merge commit**, annotated, with a message that stands alone: what
   the release is, what is not stable in it.

   ```sh
   git tag -a vX.Y.Z -F -   # then the message on stdin
   git push origin main vX.Y.Z
   ```

6. **Publish the GitHub release** from the changelog section:

   ```sh
   gh release create vX.Y.Z --title "vX.Y.Z" --notes-file <(…) --prerelease
   ```

   `--prerelease` for anything before `1.0.0`, so it is not offered as *Latest*
   to someone who did not ask for an alpha.

7. **Cut the maintenance branch, at a minor release only:**

   ```sh
   git switch -c release/X.Y vX.Y.0 && git push -u origin release/X.Y
   ```

   Fixes land on `main` first and are cherry-picked here, never the reverse. A
   fix that cannot be cherry-picked is a sign the branch has diverged far enough
   that it should be retired instead.

## Not yet part of this

- **crates.io.** Nothing is published while the on-disk format can still change.
  Note that the package name and the *hetida* trademark are the same string —
  see the trademark note in [`README.md`](README.md) before claiming it.
- **Prebuilt binaries.** The release carries source only; a workflow that builds
  and attaches artefacts on tag is worth adding once there is a reason to trust
  the build more than a local `cargo build --release`.
