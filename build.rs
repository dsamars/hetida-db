// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Generates the control-plane message types from `proto/hetida.proto` (§5).
//!
//! Compiled with `protox` rather than `prost-build`'s default, which shells out
//! to `protoc`. A pure-Rust compiler keeps `cargo build` reproducible without a
//! system package every developer and CI runner would have to install first.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/hetida.proto";

    println!("cargo:rerun-if-changed={PROTO}");
    println!("cargo:rerun-if-changed=proto");

    let descriptors = protox::compile([PROTO], ["."])?;
    prost_build::Config::new().compile_fds(descriptors)?;

    Ok(())
}
