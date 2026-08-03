// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Building the tracing subscriber the binary installs (§13).
//!
//! This module **builds** a subscriber and never installs one. A library that
//! calls `set_global_default` chooses the output format for every process that
//! embeds it, the test harness included; `main.rs` is the only place that
//! decides where events go.
//!
//! The filter comes from the configuration file, never from `RUST_LOG`:
//! `HETIDA_CONFIG` is the only environment variable this server reads (§13), and
//! a second one controlling diagnostics would put them somewhere the operator is
//! not looking.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::{LogFormat, Observability};

/// The filter directive `observability` describes.
///
/// `filter` wins over `level` when both are set — it is the escape hatch for
/// what a level cannot express, so a caller writing one means it.
pub fn directive(observability: &Observability) -> String {
    observability
        .filter
        .clone()
        .unwrap_or_else(|| observability.level.directive().to_owned())
}

/// Installs the subscriber `observability` describes, for this process.
///
/// Called once, from the binary. Fails only if a subscriber is already
/// installed, which in a server means it was called twice.
///
/// Events go to stderr rather than stdout, so that piping the server's output
/// somewhere never mixes diagnostics into whatever else a shell is reading.
pub fn install(observability: &Observability) -> Result<(), Box<dyn std::error::Error>> {
    // Already validated when the file was read, so this cannot fail on a
    // configured directive — but a fallback beats a panic in a startup path.
    let filter = EnvFilter::builder()
        .parse(directive(observability))
        .unwrap_or_else(|_| EnvFilter::new(observability.level.directive()));

    let registry = tracing_subscriber::registry().with(filter);
    match observability.format {
        LogFormat::Text => registry
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .try_init()?,
        LogFormat::Json => registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(std::io::stderr),
            )
            .try_init()?,
    }
    Ok(())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable
)]
pub(crate) mod capture {
    //! A subscriber that records what was emitted, so a test can assert on it
    //! rather than on the shape of a formatted line.

    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};

    /// One recorded event: its level, its target, and its fields as text.
    #[derive(Debug, Clone)]
    pub struct Recorded {
        /// How severe it was.
        pub level: Level,
        /// The module that emitted it.
        pub target: String,
        /// Every field, rendered, including the message.
        pub fields: Vec<(String, String)>,
    }

    impl Recorded {
        /// The `message` field, which is where a `tracing` macro's literal goes.
        pub fn message(&self) -> String {
            self.field("message").unwrap_or_default()
        }

        /// One field by name.
        pub fn field(&self, name: &str) -> Option<String> {
            self.fields
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        }
    }

    /// Everything a [`Capturing`] subscriber saw.
    pub type Log = Arc<Mutex<Vec<Recorded>>>;

    /// Records every event, filtering nothing — a test says what it wants.
    pub struct Capturing {
        log: Log,
    }

    impl Capturing {
        /// A subscriber writing into `log`.
        pub fn new(log: Log) -> Self {
            Self { log }
        }
    }

    struct Fields(Vec<(String, String)>);

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push((field.name().to_owned(), format!("{value:?}")));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.push((field.name().to_owned(), value.to_owned()));
        }
    }

    impl Subscriber for Capturing {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::Id {
            tracing::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::Id, _follows: &tracing::Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields(Vec::new());
            event.record(&mut fields);
            let metadata = event.metadata();
            if let Ok(mut log) = self.log.lock() {
                log.push(Recorded {
                    level: *metadata.level(),
                    target: metadata.target().to_owned(),
                    fields: fields.0,
                });
            }
        }

        fn enter(&self, _span: &tracing::Id) {}

        fn exit(&self, _span: &tracing::Id) {}
    }

    /// Runs `body` with a capturing subscriber and returns what it emitted.
    ///
    /// Scoped to this thread, so it neither needs nor touches a global
    /// subscriber and several tests may run at once.
    pub fn capturing<T>(body: impl FnOnce() -> T) -> (T, Vec<Recorded>) {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let value = tracing::subscriber::with_default(Capturing::new(Arc::clone(&log)), body);
        let recorded = log.lock().map(|log| log.clone()).unwrap_or_default();
        (value, recorded)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unreachable
)]
mod tests {
    use super::*;
    use crate::config::{FileConfig, Level};

    fn observability(text: &str) -> Observability {
        FileConfig::parse(text).unwrap().observability()
    }

    /// §13: the level is what an operator sets; the directive is derived.
    #[test]
    fn a_level_becomes_its_directive() {
        assert_eq!(directive(&observability("")), "info");
        assert_eq!(
            directive(&observability("[observability]\nlevel = \"debug\"")),
            "debug"
        );
    }

    /// §13: `filter` is the escape hatch, so it wins.
    #[test]
    fn a_filter_wins_over_the_level() {
        let observability = observability(
            "[observability]\nlevel = \"error\"\nfilter = \"info,hetida_db::wal=debug\"",
        );

        assert_eq!(directive(&observability), "info,hetida_db::wal=debug");
        // The level is still readable, so what was overridden stays visible.
        assert_eq!(observability.level, Level::Error);
    }

    /// The capture harness the rest of the suite asserts through has to record
    /// the level and the fields, or a test using it proves nothing.
    #[test]
    fn the_capture_harness_records_level_and_fields() {
        let ((), events) = capture::capturing(|| {
            tracing::warn!(table = "readings", rows = 7, "refused");
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].level, tracing::Level::WARN);
        assert_eq!(events[0].target, "hetida_db::observability::tests");
        assert_eq!(events[0].message(), "refused");
        assert_eq!(events[0].field("table").as_deref(), Some("readings"));
        assert_eq!(events[0].field("rows").as_deref(), Some("7"));
    }
}
