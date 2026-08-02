// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Heiko Dietz

//! Injectable wall-clock time.
//!
//! Wall-clock time is never read directly anywhere in the crate. It arrives
//! through [`Clock`], so tests can drive `system_time` explicitly — including
//! backwards, which is what makes the §1 monotonicity clamp testable at all.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A point in time as microseconds since the Unix epoch, possibly negative.
///
/// Microseconds match the resolution of the `system_time` column (§1) and stay
/// within `i64` well past the year 200 000.
pub type Micros = i64;

/// Source of wall-clock time.
///
/// Implementations must be cheap to call: this sits on the ingestion path.
/// Nothing here promises monotonicity — a host clock can jump backwards, and
/// clamping that into a monotonic `system_time` is the stamper's job (§1).
pub trait Clock: fmt::Debug + Send + Sync {
    /// Returns the current time in microseconds since the Unix epoch.
    fn now_micros(&self) -> Micros;
}

/// [`Clock`] reading the host's system clock. The production implementation.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_micros(&self) -> Micros {
        // Saturating rather than panicking: a host clock set absurdly far from
        // the epoch must not take the server down (zero-panic policy).
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(since) => Micros::try_from(since.as_micros()).unwrap_or(Micros::MAX),
            Err(before) => Micros::try_from(before.duration().as_micros())
                .map_or(Micros::MIN, |micros| -micros),
        }
    }
}

/// [`Clock`] whose time is set explicitly by the test that owns it.
///
/// Shared by `&self` so a handle can stay behind an `Arc` while the test still
/// moves time — in either direction.
#[derive(Debug)]
pub struct TestClock {
    now: AtomicI64,
}

impl TestClock {
    /// Creates a clock reading `now`.
    pub fn new(now: Micros) -> Self {
        Self {
            now: AtomicI64::new(now),
        }
    }

    /// Sets the clock to `now`, which may be earlier than the current reading.
    pub fn set(&self, now: Micros) {
        self.now.store(now, Ordering::SeqCst);
    }

    /// Moves the clock by `delta` microseconds; a negative delta moves it back.
    pub fn advance(&self, delta: Micros) {
        self.now.fetch_add(delta, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_micros(&self) -> Micros {
        self.now.load(Ordering::SeqCst)
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
    use std::sync::Arc;

    /// §1: `system_time` must be explicit in tests, never read from the host.
    #[test]
    fn test_clock_reports_the_time_it_was_given() {
        let clock = TestClock::new(1_000);
        assert_eq!(clock.now_micros(), 1_000);
    }

    /// §1: the clamp keeping `system_time` monotonic with `seq` is only
    /// testable if the clock can be driven backwards on demand.
    #[test]
    fn test_clock_can_move_backwards() {
        let clock = TestClock::new(1_000);
        clock.set(400);
        assert_eq!(clock.now_micros(), 400);
    }

    #[test]
    fn test_clock_advances_by_a_delta() {
        let clock = TestClock::new(1_000);
        clock.advance(250);
        assert_eq!(clock.now_micros(), 1_250);
    }

    /// The injection point is useless if it cannot be stored as a trait object.
    #[test]
    fn clock_is_object_safe() {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new(7));
        assert_eq!(clock.now_micros(), 7);
    }

    /// Not a wall-clock assertion: any plausible host clock is past 2020, and
    /// the check cannot flake as time advances.
    #[test]
    fn system_clock_reports_a_plausible_epoch() {
        const Y2020_MICROS: i64 = 1_577_836_800_000_000;
        assert!(SystemClock.now_micros() > Y2020_MICROS);
    }
}
