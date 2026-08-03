// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Generates the control-plane message types from `proto/hetida.proto` (§5),
//! and stamps the build with the commit it came from.
//!
//! Compiled with `protox` rather than `prost-build`'s default, which shells out
//! to `protoc`. A pure-Rust compiler keeps `cargo build` reproducible without a
//! system package every developer and CI runner would have to install first.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/hetida.proto";

    println!("cargo:rerun-if-changed={PROTO}");
    println!("cargo:rerun-if-changed=proto");

    stamp_commit();

    let descriptors = protox::compile([PROTO], ["."])?;
    prost_build::Config::new().compile_fds(descriptors)?;

    Ok(())
}

/// Records the commit this build came from, if it can be known.
///
/// A version alone does not identify a build between releases, which is exactly
/// where a bug report comes from. Absence is handled rather than guessed at: a
/// build from a source archive has no `.git`, and `--version` then reports the
/// package version alone instead of decorating it with a fiction.
fn stamp_commit() {
    // Only the HEAD ref, not the whole tree: a rebuild on every touched source
    // file would cost more than the stamp is worth.
    println!("cargo:rerun-if-changed=.git/HEAD");

    let described = std::process::Command::new("git")
        .args(["describe", "--always", "--dirty", "--tags"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty());

    if let Some(commit) = described {
        println!("cargo:rustc-env=HETIDA_BUILD_COMMIT={commit}");
    }
}
