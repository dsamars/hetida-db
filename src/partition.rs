// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Partition interval and Hive-style layout (§11).
//!
//! The interval is a parameter of the instance, not a constant: `hour`, `day`,
//! `month` or `year`, defaulting to `month`. Nothing may derive a partition path
//! from a hardcoded granularity — the interval is recorded when an instance is
//! initialised and validated at every start, so an instance can never grow a
//! silent second layout.
//!
//! Two operations carry the whole layout:
//!
//! - [`PartitionInterval::start`] truncates a timestamp to the beginning of its
//!   partition. This is what routing groups rows by (§3): one batch may span
//!   boundaries, and rows sharing a start belong to one partition.
//! - [`PartitionInterval::path`] renders the Hive-style directory. The `key=value`
//!   form is what lets DataFusion prune whole directories from a predicate on
//!   valid time (§11), and the depth is what a startup check compares against the
//!   recorded interval.
//!
//! The two agree by construction: two timestamps share a directory exactly when
//! they share a start, which the property suite pins. If they disagreed, rows
//! would be grouped into one partition and written into another.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

use arrow::temporal_conversions::timestamp_us_to_datetime;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

use crate::clock::Micros;

/// Why a timestamp could not be placed in a partition.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PartitionError {
    /// The timestamp lies outside the range a calendar can represent.
    ///
    /// Reported rather than clamped: silently mapping it into some other
    /// partition would put a row in a directory whose name contradicts it.
    #[error("timestamp {0} lies outside the representable calendar range")]
    Unrepresentable(Micros),
}

/// A partition interval was not one of the four valid values.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("unknown partition interval `{0}`; expected hour, day, month or year")]
pub struct UnknownInterval(String);

/// Granularity of the on-disk partition layout (§11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum PartitionInterval {
    /// `year=YYYY/month=MM/day=DD/hour=HH`.
    Hour,
    /// `year=YYYY/month=MM/day=DD`.
    Day,
    /// `year=YYYY/month=MM` — the default.
    ///
    /// Typical queries span weeks to months over a few metrics, which a
    /// month-granular layout answers from one to three directories where a
    /// day-granular one pays a footer read on thirty to ninety (§11).
    #[default]
    Month,
    /// `year=YYYY`.
    Year,
}

impl PartitionInterval {
    /// Every valid interval, coarsest last.
    pub const ALL: [Self; 4] = [Self::Hour, Self::Day, Self::Month, Self::Year];

    /// Start of the partition `timestamp` falls in.
    ///
    /// Truncation is idempotent and order-preserving, so a partition's rows are
    /// a contiguous range of valid time.
    pub fn start(&self, timestamp: Micros) -> Result<Micros, PartitionError> {
        let at = datetime(timestamp)?;
        let truncated = match self {
            Self::Hour => at.date().and_hms_opt(at.hour(), 0, 0),
            Self::Day => at.date().and_hms_opt(0, 0, 0),
            Self::Month => start_of_month(at.year(), at.month()),
            Self::Year => start_of_month(at.year(), 1),
        }
        .ok_or(PartitionError::Unrepresentable(timestamp))?;

        // Via whole seconds: a truncated instant has no sub-second part, and
        // `timestamp_micros` panics outside its range where this cannot.
        truncated
            .and_utc()
            .timestamp()
            .checked_mul(1_000_000)
            .ok_or(PartitionError::Unrepresentable(timestamp))
    }

    /// Hive-style directory of the partition `timestamp` falls in.
    pub fn path(&self, timestamp: Micros) -> Result<PathBuf, PartitionError> {
        let at = datetime(timestamp)?;
        let mut path = PathBuf::from(format!("year={:04}", at.year()));

        if !matches!(self, Self::Year) {
            path.push(format!("month={:02}", at.month()));
        }
        if matches!(self, Self::Day | Self::Hour) {
            path.push(format!("day={:02}", at.day()));
        }
        if matches!(self, Self::Hour) {
            path.push(format!("hour={:02}", at.hour()));
        }

        Ok(path)
    }

    /// How many directory levels this interval's layout has.
    ///
    /// The depth is what distinguishes the four layouts on disk, so a start-up
    /// check can compare it against the recorded interval.
    pub fn depth(&self) -> usize {
        match self {
            Self::Year => 1,
            Self::Month => 2,
            Self::Day => 3,
            Self::Hour => 4,
        }
    }

    /// The configuration and metadata spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
            Self::Year => "year",
        }
    }
}

impl FromStr for PartitionInterval {
    type Err = UnknownInterval;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.to_ascii_lowercase().as_str() {
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            "month" => Ok(Self::Month),
            "year" => Ok(Self::Year),
            _ => Err(UnknownInterval(text.to_owned())),
        }
    }
}

impl fmt::Display for PartitionInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Midnight on the first of a month, if that date exists.
fn start_of_month(year: i32, month: u32) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year, month, 1)?.and_hms_opt(0, 0, 0)
}

/// Reads a timestamp as a calendar instant.
fn datetime(timestamp: Micros) -> Result<NaiveDateTime, PartitionError> {
    timestamp_us_to_datetime(timestamp).ok_or(PartitionError::Unrepresentable(timestamp))
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
    use std::path::PathBuf;

    use proptest::prelude::*;

    /// 2026-08-02T14:37:12.000123Z — every field differs, so a truncation that
    /// zeroes the wrong one cannot pass unnoticed.
    const SAMPLE: Micros = 1_785_681_432_000_123;
    /// 2026-08-02T00:00:00Z.
    const SAMPLE_DAY: Micros = 1_785_628_800_000_000;
    /// 2026-01-01T00:00:00Z.
    const SAMPLE_YEAR: Micros = 1_767_225_600_000_000;

    fn path(interval: PartitionInterval) -> PathBuf {
        interval.path(SAMPLE).unwrap()
    }

    /// §11: the layout is Hive-style, and its depth follows the interval. Both
    /// halves matter: DataFusion prunes on the `key=value` form, and the depth is
    /// what a startup check can compare against the recorded interval.
    #[test]
    fn the_layout_is_hive_style_and_its_depth_follows_the_interval() {
        assert_eq!(path(PartitionInterval::Year), PathBuf::from("year=2026"));
        assert_eq!(
            path(PartitionInterval::Month),
            PathBuf::from("year=2026/month=08")
        );
        assert_eq!(
            path(PartitionInterval::Day),
            PathBuf::from("year=2026/month=08/day=02")
        );
        assert_eq!(
            path(PartitionInterval::Hour),
            PathBuf::from("year=2026/month=08/day=02/hour=14")
        );
    }

    #[test]
    fn every_component_is_zero_padded() {
        // 2026-01-02T03:04:05Z — single-digit month, day and hour.
        let early = 1_767_323_045_000_000;
        assert_eq!(
            PartitionInterval::Hour.path(early).unwrap(),
            PathBuf::from("year=2026/month=01/day=02/hour=03")
        );
    }

    /// The start of a timestamp's partition is what routing groups rows by
    /// (§3): one batch may span boundaries, and rows sharing a start belong in
    /// one partition.
    #[test]
    fn the_start_truncates_to_the_interval() {
        assert_eq!(PartitionInterval::Year.start(SAMPLE).unwrap(), SAMPLE_YEAR);
        assert_eq!(
            PartitionInterval::Month.start(SAMPLE).unwrap(),
            1_785_542_400_000_000, // 2026-08-01T00:00:00Z
        );
        assert_eq!(PartitionInterval::Day.start(SAMPLE).unwrap(), SAMPLE_DAY);
        assert_eq!(
            PartitionInterval::Hour.start(SAMPLE).unwrap(),
            SAMPLE_DAY + 14 * 3_600_000_000,
        );
    }

    #[test]
    fn a_timestamp_already_on_a_boundary_is_its_own_start() {
        assert_eq!(
            PartitionInterval::Day.start(SAMPLE_DAY).unwrap(),
            SAMPLE_DAY
        );
        assert_eq!(
            PartitionInterval::Year.start(SAMPLE_YEAR).unwrap(),
            SAMPLE_YEAR
        );
    }

    /// §3 accepts data of any age, so timestamps before the epoch have to work
    /// like any other — a negative `Micros` is not an error.
    #[test]
    fn timestamps_before_the_epoch_partition_normally() {
        // 1969-07-20T20:17:00Z.
        let moon = -14_182_980_000_000;
        assert_eq!(
            PartitionInterval::Day.path(moon).unwrap(),
            PathBuf::from("year=1969/month=07/day=20")
        );
        assert!(PartitionInterval::Day.start(moon).unwrap() <= moon);
    }

    /// §11: the interval comes from configuration, so it must survive the round
    /// trip through the text form the instance metadata stores.
    #[test]
    fn the_interval_round_trips_through_its_text_form() {
        for interval in PartitionInterval::ALL {
            assert_eq!(interval.as_str().parse::<PartitionInterval>(), Ok(interval));
        }
    }

    #[test]
    fn an_unknown_interval_is_rejected() {
        for text in ["minute", "week", "", "MONTH ", "months"] {
            assert!(
                text.parse::<PartitionInterval>().is_err(),
                "{text:?} was accepted"
            );
        }
    }

    /// Case is not significant in configuration.
    #[test]
    fn the_text_form_is_case_insensitive() {
        assert_eq!(
            "Month".parse::<PartitionInterval>(),
            Ok(PartitionInterval::Month)
        );
    }

    /// §11: the default is `month`.
    #[test]
    fn the_default_interval_is_month() {
        assert_eq!(PartitionInterval::default(), PartitionInterval::Month);
    }

    /// A timestamp beyond what a calendar can represent must be reported, not
    /// silently mapped into some other partition (zero-panic policy).
    #[test]
    fn an_unrepresentable_timestamp_is_an_error() {
        assert!(PartitionInterval::Day.path(Micros::MAX).is_err());
        assert!(PartitionInterval::Day.start(Micros::MIN).is_err());
    }

    /// Sane range for properties: 1900-01-01 to 2200-01-01, comfortably inside
    /// what a calendar represents and wider than any archive will hold.
    fn any_timestamp() -> impl Strategy<Value = Micros> {
        -2_208_988_800_000_000i64..7_258_118_400_000_000i64
    }

    fn any_interval() -> impl Strategy<Value = PartitionInterval> {
        prop_oneof![
            Just(PartitionInterval::Hour),
            Just(PartitionInterval::Day),
            Just(PartitionInterval::Month),
            Just(PartitionInterval::Year),
        ]
    }

    proptest! {
        /// §11: routing groups rows by their partition start, so truncating an
        /// already-truncated timestamp must not move it. Without this a row
        /// could be routed to one partition and its file written to another.
        #[test]
        fn truncation_is_idempotent(
            timestamp in any_timestamp(),
            interval in any_interval(),
        ) {
            let once = interval.start(timestamp)?;
            prop_assert_eq!(interval.start(once)?, once);
            prop_assert!(once <= timestamp);
        }

        /// The start and the path have to agree: two timestamps share a
        /// directory exactly when they share a start. Routing groups by the
        /// start and the writer picks the path from it — if these disagreed,
        /// rows would land in the wrong directory.
        #[test]
        fn sharing_a_start_is_sharing_a_path(
            first in any_timestamp(),
            second in any_timestamp(),
            interval in any_interval(),
        ) {
            let same_start = interval.start(first)? == interval.start(second)?;
            let same_path = interval.path(first)? == interval.path(second)?;
            prop_assert_eq!(same_start, same_path);
        }

        /// Truncation preserves order, which is what lets a partition's rows be
        /// a contiguous range of valid time.
        #[test]
        fn truncation_preserves_order(
            first in any_timestamp(),
            second in any_timestamp(),
            interval in any_interval(),
        ) {
            if first <= second {
                prop_assert!(interval.start(first)? <= interval.start(second)?);
            }
        }

        /// A coarser interval's partition always contains the finer one's, which
        /// is what makes the layout a strict hierarchy rather than four
        /// unrelated schemes.
        #[test]
        fn coarser_intervals_contain_finer_ones(timestamp in any_timestamp()) {
            let hour = PartitionInterval::Hour.start(timestamp)?;
            let day = PartitionInterval::Day.start(timestamp)?;
            let month = PartitionInterval::Month.start(timestamp)?;
            let year = PartitionInterval::Year.start(timestamp)?;

            prop_assert!(year <= month);
            prop_assert!(month <= day);
            prop_assert!(day <= hour);
            prop_assert!(hour <= timestamp);
        }

        /// The path is a prefix chain for the same reason: `hour` sits under the
        /// `day` directory, which sits under the `month` one.
        #[test]
        fn finer_paths_extend_coarser_ones(timestamp in any_timestamp()) {
            let year = PartitionInterval::Year.path(timestamp)?;
            let month = PartitionInterval::Month.path(timestamp)?;
            let day = PartitionInterval::Day.path(timestamp)?;
            let hour = PartitionInterval::Hour.path(timestamp)?;

            prop_assert!(month.starts_with(&year));
            prop_assert!(day.starts_with(&month));
            prop_assert!(hour.starts_with(&day));
        }
    }
}
