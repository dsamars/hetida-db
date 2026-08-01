// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz
//
// hetida is a registered trademark of fuseki GmbH.

//! Server binary for hetida-db.
//!
//! The engine lives in the `hetida_db` library; this binary only starts it.

/// Process entry point.
///
/// Will start the Arrow Flight server once the ingestion layer exists (Phase 1
/// in `tasks.md`). Phase 0 delivered only the test foundation, so there is
/// nothing to serve yet.
fn main() {
    println!(
        "{} {} — no server implemented yet, see tasks.md",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
}
