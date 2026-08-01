// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Injectable source of ingestion sequence numbers.
//!
//! `seq` is the total order behind version resolution (§1): among the versions
//! of one logical row visible at an observation point, the one with the highest
//! `seq` wins. Wall-clock time cannot serve that role, because concurrent
//! writers collide within a microsecond and the host clock can move backwards.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonic ingestion sequence number (§1).
pub type Seq = u64;

/// Source of ingestion sequence numbers.
///
/// Implementations must hand every call a distinct, strictly increasing value,
/// including across threads — two versions sharing a `seq` would be
/// indistinguishable to version resolution.
pub trait SequenceSource: fmt::Debug + Send + Sync {
    /// Reserves `count` consecutive sequence numbers and returns the first.
    ///
    /// Reserving a block is the primitive rather than a convenience: a batch
    /// needs one number per row, and taking them one at a time would let
    /// another writer interleave, scattering a batch's versions across the
    /// order. Reserving zero returns the number the next call would issue,
    /// without consuming it.
    fn reserve(&self, count: u64) -> Seq;

    /// Returns the next sequence number.
    fn next(&self) -> Seq {
        self.reserve(1)
    }
}

/// Lock-free [`SequenceSource`] backed by an atomic counter.
#[derive(Debug)]
pub struct AtomicSequence {
    next: AtomicU64,
}

impl AtomicSequence {
    /// Creates a source whose first [`SequenceSource::next`] returns `start`.
    ///
    /// Recovery resumes from the highest `seq` found in the WAL, so the start
    /// is a parameter rather than always zero (§4).
    pub fn starting_at(start: Seq) -> Self {
        Self {
            next: AtomicU64::new(start),
        }
    }
}

impl SequenceSource for AtomicSequence {
    fn reserve(&self, count: u64) -> Seq {
        self.next.fetch_add(count, Ordering::Relaxed)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::thread;

    /// §1: version ordering must be reproducible, so tests pick the start.
    #[test]
    fn sequence_starts_where_it_was_told_to() {
        let seq = AtomicSequence::starting_at(42);
        assert_eq!(seq.next(), 42);
        assert_eq!(seq.next(), 43);
    }

    #[test]
    fn sequence_is_object_safe() {
        let seq: Arc<dyn SequenceSource> = Arc::new(AtomicSequence::starting_at(1));
        assert_eq!(seq.next(), 1);
    }

    /// §1: `seq` is the total order behind version resolution. Handing the same
    /// number to two concurrent writers would make two versions indistinguishable.
    #[test]
    fn concurrent_callers_never_share_a_sequence_number() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 1_000;

        let seq = Arc::new(AtomicSequence::starting_at(0));
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let seq = Arc::clone(&seq);
                thread::spawn(move || (0..PER_THREAD).map(|_| seq.next()).collect::<Vec<_>>())
            })
            .collect();

        let mut seen = BTreeSet::new();
        for handle in handles {
            for value in handle.join().expect("worker thread panicked") {
                assert!(seen.insert(value), "sequence number {value} issued twice");
            }
        }
        assert_eq!(seen.len(), THREADS * PER_THREAD);
    }
}
