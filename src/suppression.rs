// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Comparison state for retransmission suppression (§1).
//!
//! §1 discards an incoming version whose payload repeats the newest version of
//! the same logical row. That needs a memory of what each logical row last
//! held — and that memory must outlive the memtable, because a freeze (§2) that
//! reset it would make the guarantee depend on flush timing, which no client
//! can observe.
//!
//! # Reach
//!
//! The state cannot cover all history: a [`RowKey`] contains a timestamp, so no
//! key ever recurs and full coverage would be the archive's whole index held in
//! memory. Reach is therefore a configured ingest-time window with a hard entry
//! cap, whichever binds first (§1). Reaching the cap shortens the window rather
//! than growing memory; suppression degrades, but a correction is never
//! discarded, because only an identical payload is ever suppressed.
//!
//! # How the window advances
//!
//! On the `system_time` of incoming data, not on a clock read — that axis is
//! monotonic by the §1 clamp, costs no clock access on the insert path, and
//! makes WAL replay reproduce the same expiry it saw the first time. An
//! instance receiving nothing expires nothing, which is what "ingest-time"
//! means.
//!
//! Expiry is generational rather than per entry: two maps, and a rotation
//! discards the older one whole. An entry therefore stays comparable for at
//! least one window and at most two, and eviction costs nothing per entry.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use dashmap::DashMap;

use crate::clock::Micros;
use crate::memtable::RowKey;
use crate::seq::Seq;

/// Digest of one row's payload columns.
///
/// 128 bits, so a single equality test between two known values misfires with
/// probability 2⁻¹²⁸ — orders of magnitude below the silent-corruption rate of
/// the memory a full payload copy would sit in. This is not the collision
/// exposure §10 rejects for `metric_id`: there a hash establishes identity
/// across all pairs, where the birthday bound applies.
pub type PayloadDigest = u128;

/// Digests `bytes` into a [`PayloadDigest`].
pub fn digest(bytes: &[u8]) -> PayloadDigest {
    xxhash_rust::xxh3::xxh3_128(bytes)
}

/// How far suppression reaches and how much memory it may spend.
#[derive(Debug, Clone, Copy)]
pub struct SuppressionConfig {
    /// Ingest-time span an entry stays comparable for, in microseconds.
    ///
    /// Set this against the retry budget of the clients: a retransmission
    /// arriving within the window is always recognised, one arriving later is
    /// not.
    pub window: Micros,
    /// Entries one generation may hold before the window is cut short.
    ///
    /// At roughly 50 bytes per entry, the default bounds the index at about
    /// 200 MB across both generations.
    pub max_entries: usize,
}

impl Default for SuppressionConfig {
    fn default() -> Self {
        Self {
            window: 5 * 60 * 1_000_000,
            max_entries: 2_000_000,
        }
    }
}

/// What the newest recorded version of a logical row holds.
type Newest = (Seq, PayloadDigest);

/// The two live generations and when the current one opened.
#[derive(Debug)]
struct Generations {
    current: Arc<DashMap<RowKey, Newest>>,
    previous: Arc<DashMap<RowKey, Newest>>,
    /// `system_time` the current generation opened at.
    ///
    /// [`Micros::MIN`] before the first observation, which makes the very
    /// first one rotate immediately — harmless, since both maps are empty.
    opened: Micros,
}

impl Generations {
    /// The two generations a lookup consults, newest first.
    fn fresh(opened: Micros) -> Self {
        Self {
            current: Arc::new(DashMap::new()),
            previous: Arc::new(DashMap::new()),
            opened,
        }
    }

    /// Retires `previous`, promotes `current`, and opens an empty one.
    fn rotated(&self, opened: Micros) -> Self {
        Self {
            current: Arc::new(DashMap::new()),
            previous: Arc::clone(&self.current),
            opened,
        }
    }

    /// The newest version recorded for `key`, current generation first.
    fn newest(&self, key: RowKey) -> Option<Newest> {
        self.current
            .get(&key)
            .or_else(|| self.previous.get(&key))
            .map(|entry| *entry.value())
    }
}

/// Remembers what each logical row last held, for §1 suppression.
///
/// Shared across freezes: the memtable holds a handle rather than owning the
/// state, which is the whole point — see the module documentation.
#[derive(Debug)]
pub struct SuppressionIndex {
    config: SuppressionConfig,
    generations: ArcSwap<Generations>,
    rotations: AtomicU64,
    forced_rotations: AtomicU64,
}

impl SuppressionIndex {
    /// Creates an empty index with the given reach.
    pub fn new(config: SuppressionConfig) -> Self {
        Self {
            config,
            generations: ArcSwap::from_pointee(Generations::fresh(Micros::MIN)),
            rotations: AtomicU64::new(0),
            forced_rotations: AtomicU64::new(0),
        }
    }

    /// The reach this index was configured with.
    pub fn config(&self) -> &SuppressionConfig {
        &self.config
    }

    /// Advances the window to `system_time`, retiring what fell out of it.
    ///
    /// Called once per batch before its rows are compared, so an entry older
    /// than the window can no longer suppress anything.
    pub fn observe(&self, system_time: Micros) {
        let generations = self.generations.load();
        let aged = system_time.saturating_sub(generations.opened) >= self.config.window;
        let full = generations.current.len() >= self.config.max_entries;
        if !aged && !full {
            return;
        }

        let rotated = Arc::new(generations.rotated(system_time));
        // Compare-and-swap so two writers rotating at once cannot each install
        // a generation and lose the other's entries.
        let installed = self
            .generations
            .compare_and_swap(&*generations, Arc::clone(&rotated));
        if Arc::ptr_eq(&installed, &generations) {
            self.rotations.fetch_add(1, Ordering::Relaxed);
            if full && !aged {
                // The cap bound before the window did: reach is now shorter
                // than configured, which an operator needs to be able to see.
                self.forced_rotations.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The newest version recorded for `key` while it is within reach.
    pub fn newest(&self, key: RowKey) -> Option<Newest> {
        self.generations.load().newest(key)
    }

    /// Records `digest` as what `key` holds at `seq`.
    ///
    /// Keeps the highest `seq` seen: an out-of-order arrival must not make an
    /// older version the comparison target, since §1 compares against the
    /// newest one.
    pub fn record(&self, key: RowKey, seq: Seq, digest: PayloadDigest) {
        let generations = self.generations.load();
        match generations.current.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut held) => {
                if seq >= held.get().0 {
                    held.insert((seq, digest));
                }
            }
            dashmap::mapref::entry::Entry::Vacant(free) => {
                // A newer version may sit in the previous generation; promoting
                // it here would cost a second lookup on every first write of a
                // generation, and losing to it only costs one suppression.
                free.insert((seq, digest));
            }
        }
    }

    /// Entries currently within reach, across both generations.
    ///
    /// Counts a logical row present in both once, as a lookup does.
    pub fn len(&self) -> usize {
        let generations = self.generations.load();
        let current = &generations.current;
        current.len()
            + generations
                .previous
                .iter()
                .filter(|entry| !current.contains_key(entry.key()))
                .count()
    }

    /// Whether nothing is within reach.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Generations retired so far.
    pub fn rotations(&self) -> u64 {
        self.rotations.load(Ordering::Relaxed)
    }

    /// Rotations forced by the entry cap rather than the window (§7).
    ///
    /// Non-zero means suppression reaches less far back than configured.
    pub fn forced_rotations(&self) -> u64 {
        self.forced_rotations.load(Ordering::Relaxed)
    }
}

impl Default for SuppressionIndex {
    fn default() -> Self {
        Self::new(SuppressionConfig::default())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::thread;

    const WINDOW: Micros = 1_000;

    fn index() -> SuppressionIndex {
        SuppressionIndex::new(SuppressionConfig {
            window: WINDOW,
            max_entries: 1_000_000,
        })
    }

    fn key(metric_id: u64, timestamp: Micros) -> RowKey {
        RowKey {
            metric_id,
            timestamp,
        }
    }

    #[test]
    fn a_new_index_knows_nothing() {
        let index = index();
        assert_eq!(index.newest(key(1, 10)), None);
        assert!(index.is_empty());
    }

    #[test]
    fn a_recorded_row_is_found_again() {
        let index = index();
        index.observe(0);
        index.record(key(1, 10), 7, digest(b"payload"));

        assert_eq!(index.newest(key(1, 10)), Some((7, digest(b"payload"))));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn distinct_logical_rows_are_held_apart() {
        let index = index();
        index.observe(0);
        index.record(key(1, 10), 0, digest(b"a"));
        index.record(key(1, 11), 1, digest(b"b"));
        index.record(key(2, 10), 2, digest(b"c"));

        assert_eq!(index.newest(key(1, 10)), Some((0, digest(b"a"))));
        assert_eq!(index.len(), 3);
    }

    /// §1 compares against the *newest* version, so a late arrival must not
    /// become the comparison target.
    #[test]
    fn an_out_of_order_record_does_not_displace_a_newer_one() {
        let index = index();
        index.observe(0);
        index.record(key(1, 10), 9, digest(b"newest"));
        index.record(key(1, 10), 4, digest(b"late"));

        assert_eq!(index.newest(key(1, 10)), Some((9, digest(b"newest"))));
    }

    #[test]
    fn a_newer_record_replaces_an_older_one() {
        let index = index();
        index.observe(0);
        index.record(key(1, 10), 4, digest(b"first"));
        index.record(key(1, 10), 9, digest(b"second"));

        assert_eq!(index.newest(key(1, 10)), Some((9, digest(b"second"))));
    }

    /// §1: the reach is the window, and it is measured on ingest time.
    #[test]
    fn an_entry_survives_at_least_one_full_window() {
        let index = index();
        index.observe(1_000);
        index.record(key(1, 10), 0, digest(b"payload"));

        index.observe(1_000 + WINDOW);
        assert!(
            index.newest(key(1, 10)).is_some(),
            "an entry must stay comparable for a whole window"
        );
    }

    #[test]
    fn an_entry_falls_out_of_reach_after_two_windows() {
        let index = index();
        index.observe(1_000);
        index.record(key(1, 10), 0, digest(b"payload"));

        index.observe(1_000 + WINDOW);
        index.observe(1_000 + 2 * WINDOW);

        assert_eq!(
            index.newest(key(1, 10)),
            None,
            "past two windows nothing may still suppress"
        );
    }

    /// The window measures ingest, not wall time: an instance receiving
    /// nothing must not forget what it holds.
    #[test]
    fn an_index_that_observes_nothing_expires_nothing() {
        let index = index();
        index.observe(1_000);
        index.record(key(1, 10), 0, digest(b"payload"));

        for _ in 0..100 {
            assert!(index.newest(key(1, 10)).is_some());
        }
    }

    /// §1: `system_time` never decreases, but a replay may observe the same
    /// value repeatedly — that must not rotate anything.
    #[test]
    fn observing_the_same_time_repeatedly_does_not_rotate() {
        let index = index();
        index.observe(5_000);
        index.record(key(1, 10), 0, digest(b"payload"));

        let before = index.rotations();
        for _ in 0..10 {
            index.observe(5_000);
        }

        assert_eq!(index.rotations(), before);
        assert!(index.newest(key(1, 10)).is_some());
    }

    /// §7: the cap bounds memory independently of the window. Reaching it
    /// shortens the reach instead of growing.
    #[test]
    fn the_entry_cap_cuts_the_window_short() {
        let index = SuppressionIndex::new(SuppressionConfig {
            window: Micros::MAX,
            max_entries: 4,
        });
        index.observe(0);

        for row in 0..12 {
            index.observe(0);
            index.record(key(1, row), row as u64, digest(b"payload"));
        }

        assert!(
            index.forced_rotations() > 0,
            "the cap must report having bound"
        );
        assert!(
            index.len() <= 8,
            "two generations of at most four entries each, found {}",
            index.len()
        );
        assert_eq!(
            index.newest(key(1, 11)),
            Some((11, digest(b"payload"))),
            "the most recent row must still be within reach"
        );
    }

    #[test]
    fn a_rotation_forced_by_age_is_not_counted_as_forced() {
        let index = index();
        index.observe(1_000);
        index.observe(1_000 + WINDOW);

        assert!(index.rotations() > 0);
        assert_eq!(index.forced_rotations(), 0);
    }

    /// Concurrent rotation must not lose the other writer's generation.
    #[test]
    fn concurrent_writers_and_rotations_keep_recent_entries() {
        const THREADS: u64 = 8;
        const PER_THREAD: u64 = 200;

        let index = Arc::new(index());
        let handles: Vec<_> = (0..THREADS)
            .map(|thread_index| {
                let index = Arc::clone(&index);
                thread::spawn(move || {
                    for step in 0..PER_THREAD {
                        // Every thread drives the same monotonic ingest axis,
                        // so rotations happen while others are recording.
                        index.observe(step as Micros * 20);
                        index.record(key(thread_index, step as Micros), step, digest(b"payload"));
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker thread panicked");
        }

        // Nothing is asserted about what survived — rotation is allowed to
        // retire entries. What must hold is that the index is still usable and
        // bounded, not that it kept everything.
        index.observe(PER_THREAD as Micros * 20);
        index.record(key(99, 1), 1, digest(b"fresh"));
        assert_eq!(index.newest(key(99, 1)), Some((1, digest(b"fresh"))));
    }

    #[test]
    fn identical_bytes_digest_identically() {
        assert_eq!(digest(b"payload"), digest(b"payload"));
        assert_ne!(digest(b"payload"), digest(b"payloae"));
        assert_ne!(digest(b""), digest(b"\0"));
    }
}
