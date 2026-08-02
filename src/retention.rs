// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Event-time retention: dropping expired partitions (§8).
//!
//! A table may carry a retention duration. Data older than it is removed by
//! **dropping whole partition directories** — never by rewriting a Parquet file
//! to delete rows from inside it, which is compaction's job and would rewrite far
//! more than it deletes.
//!
//! # The unit is the partition, so the resolution is the interval
//!
//! A partition is dropped only once its **entire interval** lies past the cutoff.
//! Under a `month` layout with a 90-day policy, data survives until its whole
//! month clears the cutoff — up to roughly 120 days. That over-retention is
//! inherent: retention resolution can never be finer than the partition interval
//! (§8, §11), and the two are chosen together.
//!
//! # Event time, read off the directory
//!
//! The cutoff is compared against a partition's **valid time**, not against when
//! its rows arrived. The Hive-style layout already encodes exactly that, so the
//! reaper needs no index: it lists the directories and reads each one's start back
//! out of its name ([`PartitionInterval::start_of_path`]).
//!
//! A directory whose name this layout would not have written reads as nothing and
//! is left alone. Retention deletes, so an unrecognised name is never a guess.
//!
//! # Ingestion is refused for what would be reaped
//!
//! [`is_expired`] is the single predicate. The reaper drops a partition when it
//! holds, and `do_put` refuses a batch when it holds for that batch's oldest row
//! (§8) — so the two are exactly complementary: anything accepted lands in a
//! partition the reaper is not yet entitled to drop. Rejecting more than that
//! would refuse rows the archive would in fact have kept and served.
//!
//! # A reap racing a flush is self-healing
//!
//! Nothing coordinates the reaper with the flusher. A memtable admitted before the
//! cutoff moved may still flush into a directory just dropped, recreating it. The
//! next pass drops it again, and no query in between can be wrong: the rows were
//! expired either way. The alternative — a lock across the flush path — would put
//! retention on the ingestion path for a race that costs one extra pass.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::{Clock, Micros};
use crate::compact::partition_dirs;
use crate::partition::{PartitionError, PartitionInterval};
use crate::storage::{Storage, StorageError};

/// Why a reap could not be carried out.
#[derive(Debug, thiserror::Error)]
pub enum ReapError {
    /// The partitions could not be listed, so none were considered.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A partition's interval could not be placed on the calendar (§11).
    #[error(transparent)]
    Partition(#[from] PartitionError),
}

/// The instant at which data becomes expired (§8).
///
/// Saturating rather than wrapping: a retention longer than the age of the epoch
/// keeps everything, which is the safe direction for an operation that deletes.
pub fn cutoff(now: Micros, retention: Micros) -> Micros {
    now.saturating_sub(retention)
}

/// Whether the partition holding `timestamp` lies entirely past `cutoff` (§8).
///
/// The whole interval must be past it — a partition still holding one live
/// microsecond is kept. This is the predicate both the reaper and the ingest
/// rejection use, which is what makes them complementary.
pub fn is_expired(
    interval: PartitionInterval,
    timestamp: Micros,
    cutoff: Micros,
) -> Result<bool, PartitionError> {
    // The end is exclusive, so `<=` is "no microsecond of this partition is at
    // or after the cutoff".
    Ok(interval.end(timestamp)? <= cutoff)
}

/// What one reaping pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReapPass {
    /// Partition directories dropped.
    pub dropped: Vec<PathBuf>,
    /// Directories that were expired but could not be removed.
    ///
    /// Counted rather than fatal: one unwritable partition must not stop the
    /// others from being reclaimed, and the next pass retries it.
    pub failed: usize,
    /// Directories whose name this layout would not have written, and which were
    /// therefore left alone.
    pub unrecognised: usize,
}

/// Drops a table's expired partitions (§8).
///
/// Split from the timer the same way the flusher is: [`Self::reap`] is one full
/// pass and is what the tests drive, [`Self::run`] only adds a clock to it.
#[derive(Debug)]
pub struct Reaper {
    storage: Arc<dyn Storage>,
    interval: PartitionInterval,
}

impl Reaper {
    /// Reaps partitions laid out on `interval` in `storage`.
    pub fn new(storage: Arc<dyn Storage>, interval: PartitionInterval) -> Self {
        Self { storage, interval }
    }

    /// Drops every partition of `table` entirely past the cutoff at `now`.
    ///
    /// `retention` of `None` keeps everything and lists nothing — an
    /// unconfigured table is not merely spared, it is never even examined (§8).
    pub fn reap(
        &self,
        table: &str,
        retention: Option<Micros>,
        now: Micros,
    ) -> Result<ReapPass, ReapError> {
        let Some(retention) = retention else {
            return Ok(ReapPass::default());
        };
        let cutoff = cutoff(now, retention);
        let mut pass = ReapPass::default();

        for directory in partition_dirs(self.storage.as_ref(), table, self.interval)? {
            // Never a guess: a name this layout did not write is not dated, so
            // it is not dropped.
            let Some(start) = self.interval.start_of_path(&directory) else {
                pass.unrecognised += 1;
                continue;
            };
            if !is_expired(self.interval, start, cutoff)? {
                continue;
            }

            match self.storage.remove_dir_all(&directory) {
                Ok(()) => pass.dropped.push(directory),
                Err(_) => pass.failed += 1,
            }
        }

        Ok(pass)
    }

    /// Reaps `table` every `tick` until `shutdown` resolves.
    ///
    /// The timer is all this adds; everything it decides is in [`Self::reap`].
    pub async fn run(
        &self,
        table: &str,
        retention: Option<Micros>,
        clock: &dyn Clock,
        tick: Duration,
        shutdown: impl std::future::Future<Output = ()>,
    ) {
        let mut timer = tokio::time::interval(tick);
        // Skip missed ticks: catching up would reap several times over for a
        // cutoff that has moved by the same amount either way.
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    let _ = self.reap(table, retention, clock.now_micros());
                }
                () = &mut shutdown => return,
            }
        }
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
    use std::path::Path;

    use crate::clock::TestClock;
    use crate::storage::{Fault, InMemoryStorage};

    const DAY: Micros = 86_400_000_000;
    /// 2026-08-02T14:37:12Z.
    const AUGUST: Micros = 1_785_681_432_000_000;
    /// 2026-07-15T00:00:00Z — the month before.
    const JULY: Micros = 1_784_073_600_000_000;
    /// 2026-06-15T00:00:00Z — two months before.
    const JUNE: Micros = 1_781_481_600_000_000;

    /// Writes a file into the partition `timestamp` belongs to.
    fn with_partition(storage: &InMemoryStorage, timestamp: Micros) -> PathBuf {
        let directory =
            Path::new("data/readings").join(PartitionInterval::Month.path(timestamp).unwrap());
        let mut file = storage
            .open_append(&directory.join("part-0.parquet"))
            .unwrap();
        file.append(b"rows").unwrap();
        file.sync().unwrap();
        directory
    }

    fn reaper(storage: &InMemoryStorage) -> Reaper {
        Reaper::new(Arc::new(storage.clone()), PartitionInterval::Month)
    }

    fn remaining(storage: &InMemoryStorage) -> Vec<PathBuf> {
        partition_dirs(storage, "readings", PartitionInterval::Month).unwrap()
    }

    // ---- the cutoff (§8) ----

    #[test]
    fn the_cutoff_is_the_retention_behind_now() {
        assert_eq!(cutoff(AUGUST, 90 * DAY), AUGUST - 90 * DAY);
    }

    /// A retention longer than the age of the epoch must keep everything rather
    /// than wrap into a cutoff in the future — the safe direction for deletion.
    #[test]
    fn an_absurd_retention_saturates_rather_than_wrapping() {
        assert_eq!(cutoff(Micros::MIN, Micros::MAX), Micros::MIN);
    }

    /// §8: the whole interval must be past the cutoff. A partition still holding
    /// one live microsecond is kept.
    #[test]
    fn a_partition_is_expired_only_once_its_whole_interval_is_past() {
        let interval = PartitionInterval::Month;
        let end = interval.end(JULY).unwrap();

        assert!(!is_expired(interval, JULY, end - 1).unwrap(), "one live µs");
        assert!(is_expired(interval, JULY, end).unwrap(), "exactly cleared");
        assert!(is_expired(interval, JULY, end + 1).unwrap());
    }

    // ---- the reaper (§8) ----

    /// §8: unset retention keeps forever. An archive nobody gave a horizon must
    /// not start deleting because its data got old.
    #[test]
    fn an_unconfigured_table_is_never_reaped() {
        let storage = InMemoryStorage::new();
        with_partition(&storage, JUNE);

        let pass = reaper(&storage).reap("readings", None, AUGUST).unwrap();

        assert_eq!(pass, ReapPass::default());
        assert_eq!(remaining(&storage).len(), 1);
    }

    /// §8: a partition whose interval is entirely past the cutoff is dropped,
    /// directory and all.
    #[test]
    fn a_partition_entirely_past_the_cutoff_is_dropped() {
        let storage = InMemoryStorage::new();
        let june = with_partition(&storage, JUNE);
        with_partition(&storage, AUGUST);

        // 30 days back from August puts the cutoff in July, so June is clear.
        let pass = reaper(&storage)
            .reap("readings", Some(30 * DAY), AUGUST)
            .unwrap();

        assert_eq!(pass.dropped, vec![june.clone()]);
        assert!(
            storage.list(&june).unwrap().is_empty(),
            "the directory must be gone, not merely emptied of the files we knew"
        );
        assert_eq!(remaining(&storage).len(), 1, "august must survive");
    }

    /// §8: coarse intervals over-retain. July straddles a cutoff inside it, so
    /// it stays whole — retention never deletes part of a partition.
    #[test]
    fn a_partition_straddling_the_cutoff_survives_whole() {
        let storage = InMemoryStorage::new();
        let july = with_partition(&storage, JULY);

        // A cutoff of 2026-07-16 sits inside July's interval.
        let now = JULY + DAY;
        let pass = reaper(&storage).reap("readings", Some(0), now).unwrap();

        assert!(pass.dropped.is_empty(), "July was cut in half");
        assert_eq!(remaining(&storage), vec![july]);
    }

    /// Retention deletes, so a directory this layout would not have written is
    /// left alone rather than dated by guesswork.
    #[test]
    fn an_unrecognised_directory_is_left_alone() {
        let storage = InMemoryStorage::new();
        let mut file = storage
            .open_append(Path::new("data/readings/year=2026/week=27/part-0.parquet"))
            .unwrap();
        file.sync().unwrap();

        let pass = reaper(&storage)
            .reap("readings", Some(0), Micros::MAX / 2)
            .unwrap();

        assert!(pass.dropped.is_empty());
        assert_eq!(pass.unrecognised, 1, "it must be reported, not silent");
        assert_eq!(remaining(&storage).len(), 1);
    }

    /// One unremovable partition must not stop the others from being reclaimed.
    #[test]
    fn a_failed_removal_is_counted_and_the_pass_continues() {
        let storage = InMemoryStorage::new();
        with_partition(&storage, JUNE);
        with_partition(&storage, JULY);
        storage.inject(Fault::Fail(std::io::ErrorKind::PermissionDenied));

        let pass = reaper(&storage)
            .reap("readings", Some(0), Micros::MAX / 2)
            .unwrap();

        assert_eq!(pass.failed, 1);
        assert_eq!(pass.dropped.len(), 1, "the second partition still went");
    }

    /// A reaper runs on a timer, so a pass over nothing due — and a repeat of a
    /// pass that already dropped everything — must both be no-ops.
    #[test]
    fn reaping_is_idempotent() {
        let storage = InMemoryStorage::new();
        with_partition(&storage, JUNE);
        let reaper = reaper(&storage);

        let first = reaper
            .reap("readings", Some(30 * DAY), AUGUST)
            .unwrap()
            .dropped
            .len();
        let second = reaper
            .reap("readings", Some(30 * DAY), AUGUST)
            .unwrap()
            .dropped
            .len();

        assert_eq!((first, second), (1, 0));
    }

    /// §8: the cutoff moves with the clock, so a partition kept today expires
    /// later without anything else changing.
    #[test]
    fn a_partition_kept_today_expires_as_the_clock_moves() {
        let storage = InMemoryStorage::new();
        with_partition(&storage, JULY);
        let reaper = reaper(&storage);
        let retention = Some(60 * DAY);

        assert!(
            reaper
                .reap("readings", retention, AUGUST)
                .unwrap()
                .dropped
                .is_empty(),
            "60 days back from August is still inside July"
        );
        assert_eq!(
            reaper
                .reap("readings", retention, AUGUST + 60 * DAY)
                .unwrap()
                .dropped
                .len(),
            1,
            "two months on, July has cleared the cutoff"
        );
    }

    #[test]
    fn a_table_with_no_partitions_reaps_nothing() {
        let storage = InMemoryStorage::new();
        assert_eq!(
            reaper(&storage).reap("absent", Some(0), AUGUST).unwrap(),
            ReapPass::default()
        );
    }

    /// The timer only wraps `reap`, and shutdown returns rather than aborting.
    /// Driven with paused time so the test cannot flake.
    #[tokio::test(start_paused = true)]
    async fn the_timer_reaps_until_shutdown() {
        let storage = InMemoryStorage::new();
        with_partition(&storage, JUNE);
        let clock = TestClock::new(AUGUST);

        reaper(&storage)
            .run(
                "readings",
                Some(30 * DAY),
                &clock,
                Duration::from_secs(1),
                tokio::time::sleep(Duration::from_secs(3)),
            )
            .await;

        assert!(
            remaining(&storage).is_empty(),
            "at least one tick must have reaped"
        );
    }
}
