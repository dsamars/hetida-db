// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! The configuration file.
//!
//! Everything an operator configures: where the data lives, what to listen on,
//! TLS and credentials (§9), and how the archive behaves — the partition
//! interval, the memory cap, the write-ahead log, flush and compaction
//! thresholds, Parquet settings, and per-table suppression and retention.
//!
//! **`HETIDA_CONFIG` is the only environment variable left.** Everything else
//! moved here, and a start refuses when a retired variable is still set
//! ([`crate::serve::retired_in_use`]) rather than ignoring it. One place to
//! configure means no precedence rules, and it takes passwords out of an
//! environment that `ps` exposes to every user on the host.
//!
//! # A missing file is defaults; a broken one is a refusal
//!
//! No file means the compiled defaults, which are safe. A file that was named
//! but cannot be read, parsed or understood stops the start — an operator who
//! wrote a configuration meant it to take effect, and starting with defaults
//! instead would be the silent kind of wrong.
//!
//! Unknown keys are **rejected** for the same reason. This file is written by
//! hand, and a misspelled `retention` that is quietly ignored is the failure
//! mode that costs the most to find. (Instance metadata takes the opposite rule
//! — it is machine-written, and tolerating unknown keys is what lets an older
//! build read a newer one's file.)
//!
//! # Units are in the value, never assumed
//!
//! Durations carry a suffix (`ms`, `s`, `m`, `h`, `d`) and sizes may (`KiB`,
//! `MiB`, `GiB`, or the 1000-based `KB`, `MB`, `GB`). A bare number is bytes for
//! a size and an error for a duration: there is no defensible default unit for
//! time here, and guessing seconds where an operator meant milliseconds changes
//! the meaning of the setting rather than its magnitude.
//!
//! ```toml
//! partition_interval = "month"
//! max_memtable_bytes = "1GiB"
//!
//! [flush]
//! max_bytes = "64MiB"
//! max_age = "5m"
//!
//! [table_defaults]
//! suppression_window = "5m"
//!
//! [tables.forecast]
//! suppression_window = "1h"   # a job recomputing its horizon hourly (§1)
//! retention = "400d"
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;

use crate::archive::ArchiveConfig;
use crate::auth::TableSet;
use crate::catalog::TableConfig;
use crate::clock::Micros;
use crate::compact::CompactConfig;
use crate::flush::FlushConfig;
use crate::parquet::ParquetConfig;
use crate::partition::PartitionInterval;
use crate::serve::{AuthConfig, ConfigError, TransportConfig, UserSpec};
use crate::suppression::SuppressionConfig;
use crate::wal::WalConfig;

/// Path to the configuration file, if one is used.
///
/// The only environment variable the server reads. Everything else moved into
/// the file it names.
pub const CONFIG_VAR: &str = "HETIDA_CONFIG";

/// Where the archive lives, unless the file says otherwise.
pub const DEFAULT_DATA_DIR: &str = "hetida-data";
/// Address to serve on, unless the file says otherwise.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:50051";

/// Why a configuration file could not be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigFileError {
    /// The file could not be read.
    #[error("could not read `{path}`: {source}")]
    Unreadable {
        /// The path that was named.
        path: String,
        /// What the operating system said.
        source: std::io::Error,
    },
    /// The file is not valid configuration.
    ///
    /// Carries the message as `toml` rendered it, which names the line and the
    /// key — an operator needs to know *where*, not only that something is wrong.
    #[error("`{path}` is not valid configuration:\n{reason}")]
    Invalid {
        /// The path that was named.
        path: String,
        /// The parser's account of the problem.
        reason: String,
    },
}

/// How often the background tasks look for work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ticks {
    /// Flush interval (§2).
    pub flush: Duration,
    /// Compaction interval (§3).
    pub compaction: Duration,
    /// Retention interval (§8).
    pub retention: Duration,
}

impl Default for Ticks {
    fn default() -> Self {
        Self {
            // Far below the age bound the flusher enforces, so a partition is
            // flushed close to when it becomes due rather than a whole tick late.
            flush: Duration::from_secs(10),
            // Compaction rewrites a whole partition where a flush appends one
            // file; sharing the flush tick would compact far more than needed.
            compaction: Duration::from_secs(300),
            // Comfortably below the finest partition interval, which is an hour.
            retention: Duration::from_secs(900),
        }
    }
}

/// The configuration, as a file describes it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FileConfig {
    /// Root of the archive: logs, Parquet and instance metadata.
    data_dir: Option<PathBuf>,
    /// The listener and its TLS identity (§9).
    server: ServerSection,
    /// Who may do what (§9).
    auth: AuthSection,
    /// Layout granularity, validated against the instance metadata (§11).
    partition_interval: Option<Interval>,
    /// Hard cap on memtable bytes before ingestion is refused (§7).
    max_memtable_bytes: Option<Size>,
    #[serde(rename = "wal")]
    log: WalSection,
    flush: FlushSection,
    compaction: CompactionSection,
    parquet: ParquetSection,
    retention: RetentionSection,
    /// Applies to any table `tables` does not name.
    table_defaults: TableSection,
    /// Per-table overrides (§6).
    tables: BTreeMap<String, TableSection>,
    /// What the server says about itself, and how (§13).
    observability: ObservabilitySection,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct ObservabilitySection {
    level: Option<Level>,
    format: Option<LogFormat>,
    filter: Option<Filter>,
}

/// How much the server says (§13).
///
/// Deliberately a closed set rather than a directive: a directive parser reads
/// an unknown word as a *target* of that name and then logs nothing, which is
/// indistinguishable from a quiet server. Use [`Observability::filter`] where a
/// level genuinely will not do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// Only what stopped something.
    Error,
    /// Also what failed and will be retried.
    Warn,
    /// Also what an operator wants to see happen. The default.
    Info,
    /// Also per-call detail.
    Debug,
    /// Everything.
    Trace,
}

impl Level {
    /// The directive this level corresponds to.
    pub fn directive(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

/// How each event is rendered (§13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// For a terminal. The default: the first person to run this reads it.
    Text,
    /// One JSON object per event, for a collector that parses.
    Json,
}

/// What the server says about itself, and how (§13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observability {
    /// How much is said.
    pub level: Level,
    /// How it is rendered.
    pub format: LogFormat,
    /// A full directive, overriding `level` when set.
    ///
    /// Validated when the file is read, so a malformed one refuses the start
    /// rather than surfacing as a silent server.
    pub filter: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct ServerSection {
    listen: Option<String>,
    tls_certificate: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    allow_plaintext: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct AuthSection {
    allow_anonymous: bool,
    users: Vec<UserEntry>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct UserEntry {
    name: String,
    password: Option<String>,
    password_file: Option<PathBuf>,
    read: Tables,
    write: Tables,
}

/// A grant's table list, written as `["a", "b"]`, `"a"`, or `"*"` for all.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(untagged)]
enum Tables {
    /// A single name, or `"*"`.
    One(String),
    /// Several names; `"*"` anywhere means all.
    Many(Vec<String>),
    /// Absent — no grant. A grant nobody wrote down is not a grant.
    #[default]
    None,
}

impl Tables {
    fn resolve(&self) -> TableSet {
        let names: Vec<&str> = match self {
            Self::One(name) => vec![name.as_str()],
            Self::Many(names) => names.iter().map(String::as_str).collect(),
            Self::None => Vec::new(),
        };
        if names.contains(&"*") {
            return TableSet::All;
        }
        TableSet::named(names.into_iter().filter(|name| !name.is_empty()))
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct WalSection {
    segment_bytes: Option<Size>,
    max_commit_delay: Option<Span>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct FlushSection {
    max_bytes: Option<Size>,
    max_age: Option<Span>,
    tick: Option<Span>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct CompactionSection {
    max_files: Option<usize>,
    max_bytes: Option<Size>,
    target_bytes: Option<Size>,
    tick: Option<Span>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct ParquetSection {
    zstd_level: Option<i32>,
    row_group_rows: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RetentionSection {
    tick: Option<Span>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct TableSection {
    suppression_window: Option<Span>,
    suppression_max_entries: Option<usize>,
    /// Unset keeps forever (§8). Retention deletes, so it is never inferred.
    retention: Option<Span>,
}

impl FileConfig {
    /// Where the archive lives.
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR))
    }

    /// The socket address to serve Arrow Flight on.
    pub fn listen(&self) -> &str {
        self.server.listen.as_deref().unwrap_or(DEFAULT_LISTEN)
    }

    /// How the listener is protected, refusing to guess (§9).
    pub fn transport(&self) -> Result<TransportConfig, ConfigError> {
        TransportConfig::resolve(
            self.server.tls_certificate.as_deref(),
            self.server.tls_key.as_deref(),
            self.server.allow_plaintext,
        )
    }

    /// The users this file configures, before their passwords are read (§9).
    pub fn users(&self) -> Vec<UserSpec> {
        self.auth
            .users
            .iter()
            .map(|entry| UserSpec {
                name: entry.name.clone(),
                password: entry.password.clone(),
                password_file: entry.password_file.clone(),
                read: entry.read.resolve(),
                write: entry.write.resolve(),
            })
            .collect()
    }

    /// Who may do what, refusing to guess (§9).
    pub fn auth(&self) -> Result<AuthConfig, ConfigError> {
        AuthConfig::resolve(&self.users(), self.auth.allow_anonymous)
    }

    /// Reads the configuration at `path`.
    pub fn load(path: &Path) -> Result<Self, ConfigFileError> {
        let shown = path.display().to_string();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigFileError::Unreadable {
            path: shown.clone(),
            source,
        })?;
        Self::parse(&text).map_err(|reason| ConfigFileError::Invalid {
            path: shown,
            reason,
        })
    }

    /// Reads the configuration from TOML text.
    pub fn parse(text: &str) -> Result<Self, String> {
        toml::from_str(text).map_err(|error| error.to_string())
    }

    /// Reads the file `CONFIG_VAR` names, or the defaults if it names none.
    ///
    /// `lookup` is the environment, injected so this is testable without one.
    pub fn resolve(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigFileError> {
        match lookup(CONFIG_VAR).filter(|path| !path.trim().is_empty()) {
            Some(path) => Self::load(Path::new(&path)),
            None => Ok(Self::default()),
        }
    }

    /// The archive settings, with the per-table configuration folded in.
    pub fn archive(&self) -> ArchiveConfig {
        let defaults = ArchiveConfig::default();
        ArchiveConfig {
            partition_interval: self
                .partition_interval
                .map_or(defaults.partition_interval, |interval| interval.0),
            wal: WalConfig {
                segment_bytes: self
                    .log
                    .segment_bytes
                    .map_or(defaults.wal.segment_bytes, |size| size.0),
                max_commit_delay: self
                    .log
                    .max_commit_delay
                    .map_or(defaults.wal.max_commit_delay, Span::duration),
            },
            default_table: self.table_defaults.resolve(&TableConfig::default()),
            tables: self
                .tables
                .iter()
                .map(|(name, section)| {
                    // A named table starts from the instance defaults, so an
                    // override sets one field without silently clearing the rest.
                    let base = self.table_defaults.resolve(&TableConfig::default());
                    (name.clone(), section.resolve(&base))
                })
                .collect(),
            max_memtable_bytes: self
                .max_memtable_bytes
                .map_or(defaults.max_memtable_bytes, |size| {
                    usize::try_from(size.0).unwrap_or(usize::MAX)
                }),
        }
    }

    /// When a partition is flushed (§2).
    pub fn flush(&self) -> FlushConfig {
        let defaults = FlushConfig::default();
        FlushConfig {
            max_bytes: self.flush.max_bytes.map_or(defaults.max_bytes, |size| {
                usize::try_from(size.0).unwrap_or(usize::MAX)
            }),
            max_age: self.flush.max_age.map_or(defaults.max_age, |span| span.0),
        }
    }

    /// When a partition is compacted (§3).
    pub fn compaction(&self) -> CompactConfig {
        let defaults = CompactConfig::default();
        CompactConfig {
            max_files: self.compaction.max_files.unwrap_or(defaults.max_files),
            max_bytes: self
                .compaction
                .max_bytes
                .map_or(defaults.max_bytes, Size::usize),
            target_bytes: self
                .compaction
                .target_bytes
                .map_or(defaults.target_bytes, Size::usize),
        }
    }

    /// How a partition file is written (§1).
    pub fn parquet(&self) -> ParquetConfig {
        let defaults = ParquetConfig::default();
        ParquetConfig {
            zstd_level: self.parquet.zstd_level.unwrap_or(defaults.zstd_level),
            row_group_rows: self
                .parquet
                .row_group_rows
                .unwrap_or(defaults.row_group_rows),
        }
    }

    /// What the server says about itself, and how (§13).
    pub fn observability(&self) -> Observability {
        Observability {
            level: self.observability.level.unwrap_or(Level::Info),
            format: self.observability.format.unwrap_or(LogFormat::Text),
            filter: self
                .observability
                .filter
                .as_ref()
                .map(|filter| filter.0.clone()),
        }
    }

    /// How often each background task looks for work.
    pub fn ticks(&self) -> Ticks {
        let defaults = Ticks::default();
        Ticks {
            flush: self.flush.tick.map_or(defaults.flush, Span::duration),
            compaction: self
                .compaction
                .tick
                .map_or(defaults.compaction, Span::duration),
            retention: self
                .retention
                .tick
                .map_or(defaults.retention, Span::duration),
        }
    }
}

impl TableSection {
    /// This section applied over `base`, leaving unset fields as they were.
    fn resolve(&self, base: &TableConfig) -> TableConfig {
        TableConfig {
            suppression: SuppressionConfig {
                window: self
                    .suppression_window
                    .map_or(base.suppression.window, |span| span.0),
                max_entries: self
                    .suppression_max_entries
                    .unwrap_or(base.suppression.max_entries),
            },
            // Unset means "keep forever", and a table section that says nothing
            // about retention inherits rather than clears (§8).
            retention: self.retention.map(|span| span.0).or(base.retention),
        }
    }
}

// ---- units ----

/// A partition interval, spelled as in the instance metadata (§11).
#[derive(Debug, Clone, Copy)]
struct Interval(PartitionInterval);

impl<'de> Deserialize<'de> for Interval {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        PartitionInterval::from_str(&text)
            .map(Interval)
            .map_err(serde::de::Error::custom)
    }
}

/// A `tracing` filter directive, validated as it is read (§13).
///
/// Checked here rather than at the point of use so a directive that cannot be
/// parsed refuses the start, in the same breath as an unknown key — a server
/// that came up saying nothing because of a typo is the failure this avoids.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Filter(String);

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        tracing_subscriber::filter::EnvFilter::builder()
            .parse(&text)
            .map_err(serde::de::Error::custom)?;
        Ok(Filter(text))
    }
}

/// A span of time, in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Span(Micros);

impl Span {
    fn duration(self) -> Duration {
        Duration::from_micros(u64::try_from(self.0).unwrap_or(0))
    }
}

impl<'de> Deserialize<'de> for Span {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse_span(&text)
            .map(Span)
            .map_err(serde::de::Error::custom)
    }
}

/// A number of bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Size(u64);

impl Size {
    fn usize(self) -> usize {
        usize::try_from(self.0).unwrap_or(usize::MAX)
    }
}

impl<'de> Deserialize<'de> for Size {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        /// A size is written either as a plain number of bytes or with a unit.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Bytes(u64),
            Text(String),
        }

        match Written::deserialize(deserializer)? {
            Written::Bytes(bytes) => Ok(Size(bytes)),
            Written::Text(text) => parse_size(&text)
                .map(Size)
                .map_err(serde::de::Error::custom),
        }
    }
}

/// Reads `10ms`, `30s`, `5m`, `12h`, `90d` as microseconds.
///
/// A bare number is refused. There is no defensible default unit here: reading
/// `5` as seconds where an operator meant milliseconds changes what the setting
/// means, not merely how large it is.
fn parse_span(text: &str) -> Result<Micros, String> {
    const UNITS: [(&str, Micros); 5] = [
        ("ms", 1_000),
        ("s", 1_000_000),
        ("m", 60_000_000),
        ("h", 3_600_000_000),
        ("d", 86_400_000_000),
    ];
    parse_with_unit(text, &UNITS, "a duration needs a unit: ms, s, m, h or d")
}

/// Reads `4096`, `64KiB`, `256MiB`, `1GiB` — or the 1000-based `KB`, `MB`, `GB`
/// — as bytes.
fn parse_size(text: &str) -> Result<u64, String> {
    const UNITS: [(&str, i64); 7] = [
        ("kib", 1024),
        ("mib", 1024 * 1024),
        ("gib", 1024 * 1024 * 1024),
        ("kb", 1_000),
        ("mb", 1_000_000),
        ("gb", 1_000_000_000),
        ("b", 1),
    ];
    let bytes = parse_with_unit(
        text,
        &UNITS,
        "a size needs a unit: B, KiB, MiB, GiB, or the 1000-based KB, MB, GB",
    )?;
    u64::try_from(bytes).map_err(|_| format!("`{text}` is not a number of bytes"))
}

/// Splits `text` into a number and one of `units`, longest suffix first.
///
/// Case-insensitive, and tolerant of a space before the unit. Longest first
/// matters: `ms` must not be read as `m` followed by rubbish, and `mib` must not
/// be read as `mb`.
fn parse_with_unit(text: &str, units: &[(&str, i64)], missing: &str) -> Result<i64, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("an empty value".to_owned());
    }
    let lowered = trimmed.to_ascii_lowercase();

    let mut candidates: Vec<&(&str, i64)> = units.iter().collect();
    candidates.sort_by_key(|(suffix, _)| std::cmp::Reverse(suffix.len()));

    for (suffix, scale) in candidates {
        let Some(number) = lowered.strip_suffix(suffix) else {
            continue;
        };
        let number = number.trim();
        let amount: i64 = number
            .parse()
            .map_err(|_| format!("`{number}` in `{trimmed}` is not a whole number"))?;
        if amount < 0 {
            return Err(format!("`{trimmed}` is negative"));
        }
        return amount
            .checked_mul(*scale)
            .ok_or_else(|| format!("`{trimmed}` does not fit"));
    }

    // A bare number is a size in bytes, and never a duration.
    if let Ok(bare) = lowered.parse::<i64>()
        && units.iter().any(|(suffix, _)| *suffix == "b")
    {
        return Ok(bare);
    }
    Err(format!("`{trimmed}`: {missing}"))
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

    const MINUTE: Micros = 60_000_000;
    const HOUR: Micros = 3_600_000_000;
    const DAY: Micros = 86_400_000_000;

    fn parsed(text: &str) -> FileConfig {
        FileConfig::parse(text).unwrap_or_else(|error| panic!("{error}"))
    }

    // ---- units ----

    #[test]
    fn a_duration_reads_every_unit() {
        assert_eq!(parse_span("10ms"), Ok(10_000));
        assert_eq!(parse_span("30s"), Ok(30_000_000));
        assert_eq!(parse_span("5m"), Ok(5 * MINUTE));
        assert_eq!(parse_span("12h"), Ok(12 * HOUR));
        assert_eq!(parse_span("90d"), Ok(90 * DAY));
    }

    /// `ms` must not be read as `m` with rubbish after it — the two differ by a
    /// factor of sixty thousand.
    #[test]
    fn the_longest_unit_wins() {
        assert_eq!(parse_span("5ms"), Ok(5_000));
        assert_eq!(parse_span("5m"), Ok(5 * MINUTE));
        assert_eq!(parse_size("5mib"), Ok(5 * 1024 * 1024));
        assert_eq!(parse_size("5mb"), Ok(5_000_000));
    }

    /// There is no defensible default unit for time: reading `5` as seconds
    /// where an operator meant milliseconds changes what the setting means.
    #[test]
    fn a_duration_without_a_unit_is_refused() {
        assert!(parse_span("5").is_err());
        assert!(parse_span("").is_err());
        assert!(parse_span("soon").is_err());
        assert!(parse_span("-5m").is_err());
    }

    #[test]
    fn a_size_reads_binary_and_decimal_units() {
        assert_eq!(parse_size("4096"), Ok(4096), "a bare number is bytes");
        assert_eq!(parse_size("64KiB"), Ok(65_536));
        assert_eq!(parse_size("1GiB"), Ok(1_073_741_824));
        assert_eq!(parse_size("1GB"), Ok(1_000_000_000));
        assert_eq!(parse_size("512 MiB"), Ok(512 * 1024 * 1024));
    }

    #[test]
    fn a_size_that_makes_no_sense_is_refused() {
        assert!(parse_size("plenty").is_err());
        assert!(parse_size("-1MiB").is_err());
        assert!(parse_size("9999999999GiB").is_err(), "must not wrap");
    }

    // ---- the file ----

    /// No file at all is the compiled defaults, which are safe.
    #[test]
    fn an_empty_file_is_the_compiled_defaults() {
        let config = parsed("");
        let defaults = ArchiveConfig::default();

        assert_eq!(
            config.archive().max_memtable_bytes,
            defaults.max_memtable_bytes
        );
        assert_eq!(
            config.archive().partition_interval,
            PartitionInterval::Month
        );
        assert_eq!(config.flush().max_age, FlushConfig::default().max_age);
        assert_eq!(
            config.compaction().max_files,
            CompactConfig::default().max_files
        );
        assert_eq!(
            config.parquet().zstd_level,
            ParquetConfig::default().zstd_level
        );
        assert_eq!(config.ticks(), Ticks::default());
    }

    #[test]
    fn an_absent_config_var_is_the_defaults() {
        let config = FileConfig::resolve(|_| None).expect("defaults");
        assert_eq!(
            config.archive().partition_interval,
            PartitionInterval::Month
        );
    }

    /// A named file that cannot be read stops the start. An operator who wrote a
    /// configuration meant it to take effect.
    #[test]
    fn a_named_file_that_is_missing_stops_the_start() {
        let error = FileConfig::resolve(|name| {
            (name == CONFIG_VAR).then(|| "/nonexistent/hetida.toml".to_owned())
        })
        .expect_err("must refuse");

        assert!(
            matches!(error, ConfigFileError::Unreadable { .. }),
            "{error}"
        );
    }

    /// The reason this file rejects unknown keys and `instance.meta` tolerates
    /// them: this one is written by hand, and a misspelling that is silently
    /// ignored is the costliest failure there is to find.
    #[test]
    fn a_misspelled_key_is_refused_rather_than_ignored() {
        let error =
            FileConfig::parse("[tables.readings]\nretenshun = \"90d\"\n").expect_err("must refuse");
        assert!(error.contains("retenshun"), "{error}");

        assert!(FileConfig::parse("max_memtable_bites = 10").is_err());
        assert!(FileConfig::parse("[flsuh]\nmax_age = \"5m\"").is_err());
    }

    #[test]
    fn the_whole_surface_round_trips() {
        let config = parsed(
            r#"
            partition_interval = "day"
            max_memtable_bytes = "2GiB"

            [wal]
            segment_bytes = "128MiB"
            max_commit_delay = "25ms"

            [flush]
            max_bytes = "32MiB"
            max_age = "2m"
            tick = "5s"

            [compaction]
            max_files = 8
            max_bytes = "512MiB"
            target_bytes = "256MiB"
            tick = "10m"

            [parquet]
            zstd_level = 9
            row_group_rows = 65536

            [retention]
            tick = "30m"
            "#,
        );

        let archive = config.archive();
        assert_eq!(archive.partition_interval, PartitionInterval::Day);
        assert_eq!(archive.max_memtable_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(archive.wal.segment_bytes, 128 * 1024 * 1024);
        assert_eq!(archive.wal.max_commit_delay, Duration::from_millis(25));

        assert_eq!(config.flush().max_bytes, 32 * 1024 * 1024);
        assert_eq!(config.flush().max_age, 2 * MINUTE);
        assert_eq!(config.compaction().max_files, 8);
        assert_eq!(config.compaction().target_bytes, 256 * 1024 * 1024);
        assert_eq!(config.parquet().zstd_level, 9);
        assert_eq!(config.parquet().row_group_rows, 65_536);

        assert_eq!(
            config.ticks(),
            Ticks {
                flush: Duration::from_secs(5),
                compaction: Duration::from_secs(600),
                retention: Duration::from_secs(1800),
            }
        );
    }

    // ---- per-table configuration (§1, §6, §8) ----

    /// §8: this is the setting the file exists for. Retention was implemented
    /// and unreachable from the binary until now.
    #[test]
    fn a_table_can_be_given_a_retention_horizon() {
        let config = parsed(
            r#"
            [tables.readings]
            retention = "90d"
            "#,
        );
        let archive = config.archive();

        let (name, table) = archive.tables.first().expect("one table");
        assert_eq!(name, "readings");
        assert_eq!(table.retention, Some(90 * DAY));
    }

    /// §8: unset keeps forever. An archive nobody gave a horizon must not start
    /// deleting because a file existed.
    #[test]
    fn a_table_without_retention_keeps_everything() {
        let config = parsed(
            r#"
            [tables.readings]
            suppression_window = "1h"
            "#,
        );
        let (_, table) = config.archive().tables.first().expect("one table").clone();

        assert_eq!(table.retention, None);
        assert_eq!(table.suppression.window, HOUR);
    }

    /// §1, §6: the two suppression presets are what this setting is for — a job
    /// recomputing hourly needs a window spanning its run interval, where a
    /// stream needs minutes.
    #[test]
    fn suppression_is_configured_per_table_over_the_instance_default() {
        let config = parsed(
            r#"
            [table_defaults]
            suppression_window = "10m"
            suppression_max_entries = 500000

            [tables.forecast]
            suppression_window = "1h"
            "#,
        );
        let archive = config.archive();

        assert_eq!(archive.default_table.suppression.window, 10 * MINUTE);
        assert_eq!(archive.default_table.suppression.max_entries, 500_000);

        let (_, forecast) = archive.tables.first().expect("forecast").clone();
        assert_eq!(forecast.suppression.window, HOUR);
        assert_eq!(
            forecast.suppression.max_entries, 500_000,
            "an override must set one field, not clear the rest"
        );
    }

    /// A table section inherits the instance defaults rather than the compiled
    /// ones, or naming a table would silently reset everything else about it.
    #[test]
    fn a_named_table_inherits_the_instance_defaults() {
        let config = parsed(
            r#"
            [table_defaults]
            retention = "30d"

            [tables.readings]
            suppression_window = "1h"
            "#,
        );
        let (_, readings) = config.archive().tables.first().expect("readings").clone();

        assert_eq!(
            readings.retention,
            Some(30 * DAY),
            "the instance-wide horizon must still apply"
        );
        assert_eq!(readings.suppression.window, HOUR);
    }

    #[test]
    fn several_tables_are_configured_independently() {
        let config = parsed(
            r#"
            [tables.readings]
            retention = "90d"

            [tables.audit]
            suppression_window = "1m"
            "#,
        );
        let tables = config.archive().tables;

        assert_eq!(tables.len(), 2);
        let by_name: std::collections::BTreeMap<_, _> = tables.into_iter().collect();
        assert_eq!(by_name["readings"].retention, Some(90 * DAY));
        assert_eq!(by_name["audit"].retention, None);
        assert_eq!(by_name["audit"].suppression.window, MINUTE);
    }

    // ---- the server and its credentials (§9) ----

    #[test]
    fn the_listener_and_data_dir_default_when_the_file_is_silent() {
        let config = parsed("");
        assert_eq!(
            config.data_dir(),
            std::path::PathBuf::from(DEFAULT_DATA_DIR)
        );
        assert_eq!(config.listen(), DEFAULT_LISTEN);
    }

    #[test]
    fn the_listener_and_data_dir_come_from_the_file() {
        let config = parsed(
            r#"
            data_dir = "/var/lib/hetida"

            [server]
            listen = "0.0.0.0:50051"
            allow_plaintext = true
            "#,
        );

        assert_eq!(
            config.data_dir(),
            std::path::PathBuf::from("/var/lib/hetida")
        );
        assert_eq!(config.listen(), "0.0.0.0:50051");
        assert_eq!(
            config.transport(),
            Ok(crate::serve::TransportConfig::Plaintext)
        );
    }

    /// §9: the refusal survives the move. An unconfigured listener must not
    /// quietly serve in the clear because the setting now lives in a file.
    #[test]
    fn an_unconfigured_listener_still_refuses_to_start() {
        assert!(parsed("").transport().is_err());
    }

    #[test]
    fn a_tls_identity_is_read_from_the_file() {
        let config = parsed(
            r#"
            [server]
            tls_certificate = "/etc/hetida/chain.pem"
            tls_key = "/etc/hetida/key.pem"
            "#,
        );

        assert_eq!(
            config.transport(),
            Ok(crate::serve::TransportConfig::Tls {
                certificate: std::path::PathBuf::from("/etc/hetida/chain.pem"),
                key: std::path::PathBuf::from("/etc/hetida/key.pem"),
            })
        );
    }

    /// §9: per-table grants, written as a list, a bare name, or `"*"`.
    #[test]
    fn grants_are_read_in_every_form_they_may_be_written() {
        let config = parsed(
            r#"
            [[auth.users]]
            name = "admin"
            password = "s3cret"
            read = "*"
            write = ["*"]

            [[auth.users]]
            name = "analyst"
            password = "hunter2"
            read = ["readings", "events"]

            [[auth.users]]
            name = "one"
            password = "pw"
            read = "readings"
            "#,
        );
        let users = config.users();

        assert_eq!(users[0].read, TableSet::All, "a bare `*`");
        assert_eq!(users[0].write, TableSet::All, "or `*` in a list");
        assert_eq!(users[1].read, TableSet::named(["readings", "events"]));
        assert_eq!(
            users[1].write,
            TableSet::default(),
            "a clause nobody wrote is not a grant"
        );
        assert_eq!(users[2].read, TableSet::named(["readings"]));
    }

    /// §9: the whole point of the move — a password can stay out of the
    /// configuration entirely.
    #[test]
    fn a_user_may_name_a_password_file_instead_of_a_password() {
        let config = parsed(
            r#"
            [[auth.users]]
            name = "ingest"
            password_file = "/run/secrets/ingest"
            write = ["readings"]
            "#,
        );
        let users = config.users();

        assert_eq!(users[0].password, None);
        assert_eq!(
            users[0].password_file,
            Some(std::path::PathBuf::from("/run/secrets/ingest"))
        );
        assert!(
            !crate::serve::AuthConfig::holds_literal_secrets(&users),
            "nothing secret is in the file, so nothing needs protecting in it"
        );
    }

    #[test]
    fn anonymous_access_is_still_something_that_must_be_asked_for() {
        assert!(parsed("").auth().is_err());
        assert!(matches!(
            parsed("[auth]\nallow_anonymous = true").auth(),
            Ok(crate::serve::AuthConfig::Anonymous)
        ));
    }

    #[test]
    fn a_misspelled_user_key_is_refused() {
        assert!(
            FileConfig::parse("[[auth.users]]\nname = \"a\"\npassword = \"b\"\nreed = [\"x\"]")
                .is_err()
        );
    }

    #[test]
    fn an_unknown_partition_interval_is_refused() {
        let error = FileConfig::parse("partition_interval = \"fortnight\"").expect_err("refuse");
        assert!(error.contains("fortnight"), "{error}");
    }

    /// The error has to name where the problem is, or an operator is left
    /// bisecting a file.
    #[test]
    fn an_error_names_the_offending_value() {
        let error = FileConfig::parse("[flush]\nmax_age = \"soon\"").expect_err("refuse");
        assert!(error.contains("soon"), "{error}");
    }

    // ---- observability (§13) ----

    /// §13: the default has to be the human-readable one — the first person to
    /// run this is reading it themselves.
    #[test]
    fn observability_defaults_to_readable_info() {
        let observability = parsed("").observability();

        assert_eq!(observability.level, Level::Info);
        assert_eq!(observability.format, LogFormat::Text);
        assert_eq!(observability.filter, None);
    }

    #[test]
    fn every_level_and_format_is_selectable() {
        for (written, expected) in [
            ("error", Level::Error),
            ("warn", Level::Warn),
            ("info", Level::Info),
            ("debug", Level::Debug),
            ("trace", Level::Trace),
        ] {
            let text = format!("[observability]\nlevel = \"{written}\"");
            assert_eq!(parsed(&text).observability().level, expected);
        }

        assert_eq!(
            parsed("[observability]\nformat = \"json\"")
                .observability()
                .format,
            LogFormat::Json
        );
    }

    /// §13: this is the decision. A directive parser would read `verbose` as a
    /// *target* of that name and log nothing at all — the silent
    /// misconfiguration this file rejects unknown keys to avoid.
    #[test]
    fn an_unknown_level_is_refused_rather_than_read_as_a_target() {
        let error = FileConfig::parse("[observability]\nlevel = \"verbose\"").expect_err("refuse");
        assert!(error.contains("verbose"), "{error}");
    }

    #[test]
    fn an_unknown_format_is_refused() {
        let error =
            FileConfig::parse("[observability]\nformat = \"protobuf\"").expect_err("refuse");
        assert!(error.contains("protobuf"), "{error}");
    }

    /// §13: `filter` is the escape hatch for what a level cannot express, and it
    /// wins over `level` when both are written.
    #[test]
    fn a_filter_directive_overrides_the_level() {
        let observability =
            parsed("[observability]\nlevel = \"warn\"\nfilter = \"info,hetida_db::wal=debug\"")
                .observability();

        assert_eq!(
            observability.filter.as_deref(),
            Some("info,hetida_db::wal=debug")
        );
        // The level stays readable, so an operator can see what was overridden.
        assert_eq!(observability.level, Level::Warn);
    }

    /// The escape hatch is lenient about unknown *targets* — one may simply not
    /// have been reached yet — but not about a directive that is not one.
    #[test]
    fn a_malformed_filter_is_refused() {
        let error = FileConfig::parse("[observability]\nfilter = \"=@=\"").expect_err("refuse");
        assert!(!error.is_empty(), "the reason must not be empty");
    }

    #[test]
    fn unknown_observability_keys_are_refused() {
        assert!(FileConfig::parse("[observability]\ncolour = true").is_err());
    }
}
