use core::sync::atomic::{AtomicI64, Ordering};
use core::time::Duration;

use crate::Clock;

/// Deterministic clock for tests: reads return whatever was last set or
/// advanced to, never the wall clock.
///
/// Shared by reference; mutation goes through `&self`, so the handle kept by
/// a test can advance time under a component holding `&ManualClock`.
#[derive(Debug, Default)]
pub struct ManualClock {
    now_ns: AtomicI64,
}

impl ManualClock {
    /// Clock reading `now_ns` nanoseconds since the unix epoch.
    #[must_use]
    pub const fn new(now_ns: i64) -> Self {
        Self {
            now_ns: AtomicI64::new(now_ns),
        }
    }

    /// Set the reading.
    pub fn set_ns(&self, now_ns: i64) {
        self.now_ns.store(now_ns, Ordering::Relaxed);
    }

    /// Advance the reading by `dur`, saturating at `i64::MAX`.
    pub fn advance(&self, dur: Duration) {
        self.advance_ns(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX));
    }

    /// Advance the reading by `ns` (negative moves backward), saturating.
    pub fn advance_ns(&self, ns: i64) {
        // The closure always returns Some, so the update cannot fail.
        let _ = self
            .now_ns
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |now| {
                Some(now.saturating_add(ns))
            });
    }
}

impl Clock for ManualClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_advance() {
        let clock = ManualClock::new(0);
        assert_eq!(clock.now_ns(), 0);

        clock.set_ns(1_000);
        assert_eq!(clock.now_ns(), 1_000);

        clock.advance(Duration::from_secs(2));
        assert_eq!(clock.now_ns(), 2_000_001_000);

        clock.advance_ns(-1_000);
        assert_eq!(clock.now_ns(), 2_000_000_000);
    }

    #[test]
    fn advance_saturates() {
        let clock = ManualClock::new(i64::MAX - 1);
        clock.advance(Duration::from_secs(u64::MAX));
        assert_eq!(clock.now_ns(), i64::MAX);

        clock.set_ns(i64::MIN + 1);
        clock.advance_ns(i64::MIN);
        assert_eq!(clock.now_ns(), i64::MIN);
    }

    #[test]
    fn default_starts_at_zero() {
        assert_eq!(ManualClock::default().now_ns(), 0);
    }
}
