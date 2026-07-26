//! Closed-loop read-ahead: an EWMA of realized per-fetch latency folded
//! into a Little's-law window.

use alloc::boxed::Box;
use core::num::NonZeroUsize;
use core::time::Duration;

use nectar_clock::Clock;

use crate::config::Window;
use crate::num::u64_from_usize;
use crate::walk::{Observations, WindowPolicyFn};

/// Throughput headroom: the cap targets five quarters of the requested
/// rate, so realized throughput holds the target with margin.
const MARGIN_NUM: u64 = 5;
/// Denominator of the headroom ratio.
const MARGIN_DEN: u64 = 4;
/// EWMA smoothing denominator: each sample moves the estimate by an eighth
/// of the gap, at least one nanosecond.
const ALPHA: u64 = 8;
/// One sample never exceeds this multiple of the estimate: a stall reading
/// cannot blow up the cap in one step.
const SPIKE: u64 = 8;

/// Closed-loop window controller: tracks realized per-fetch latency and
/// returns the Little's-law cap for the target rate, never past `max` and
/// never zero. A few integer operations per call, cheap enough for the
/// poll path.
///
/// Growth needs a saturated window (the cap was the constraint); shrink is
/// always taken. A consumer stall therefore cannot grow the cap, and `max`
/// bounds memory outright.
#[derive(Debug)]
pub struct AdaptiveWindow<C> {
    clock: C,
    /// Margined target rate in bytes per second.
    target: u64,
    body: NonZeroUsize,
    max: Window,
    /// Latency estimate in nanoseconds, never zero.
    latency_ns: u64,
    /// Reading consumed by the previous sample; `None` before the first
    /// call.
    last_ns: Option<i64>,
    window: Window,
}

impl<C: Clock> AdaptiveWindow<C> {
    /// Controller targeting `bytes_per_second` over `body`-sized fetches,
    /// seeded with `mean_latency` and capped at `max`.
    pub fn new(
        bytes_per_second: u64,
        mean_latency: Duration,
        body: NonZeroUsize,
        max: Window,
        clock: C,
    ) -> Self {
        let target = bytes_per_second
            .saturating_mul(MARGIN_NUM)
            .wrapping_div(MARGIN_DEN);
        let latency_ns = u64::try_from(mean_latency.as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let window = cap(target, latency_ns, body, max);
        Self {
            clock,
            target,
            body,
            max,
            latency_ns,
            last_ns: None,
            window,
        }
    }

    /// The cap the controller currently holds.
    pub const fn window(&self) -> Window {
        self.window
    }

    /// One controller step: fold the interval into the latency estimate and
    /// return the cap. Completion-free calls extend the pending interval; a
    /// non-monotonic reading yields no sample.
    pub fn observe(&mut self, observations: &Observations) -> Window {
        let now = self.clock.now_ns();
        let Some(last) = self.last_ns else {
            self.last_ns = Some(now);
            return self.window;
        };
        if observations.completions == 0 {
            return self.window;
        }
        self.last_ns = Some(now);
        let Some(sample) = latency_sample(now.saturating_sub(last), observations) else {
            return self.window;
        };
        let sample = sample.min(self.latency_ns.saturating_mul(SPIKE)).max(1);
        self.latency_ns = ewma_step(self.latency_ns, sample);
        let desired = cap(self.target, self.latency_ns, self.body, self.max);
        let filled = observations
            .occupancy
            .saturating_add(observations.completions)
            >= usize::from(self.window.get());
        if desired < self.window || filled {
            self.window = desired;
        }
        self.window
    }

    /// Box into the walk's policy seam.
    pub fn into_policy(mut self) -> WindowPolicyFn
    where
        C: 'static,
    {
        Box::new(move |observations: &Observations| self.observe(observations))
    }
}

/// Margined Little's-law cap at the current latency estimate.
fn cap(target: u64, latency_ns: u64, body: NonZeroUsize, max: Window) -> Window {
    Window::for_throughput(target, Duration::from_nanos(latency_ns), body).min(max)
}

/// Per-fetch latency over one interval: elapsed time spread across the
/// slots that were in flight, per completion. `None` without forward time
/// or completions.
fn latency_sample(elapsed_ns: i64, observations: &Observations) -> Option<u64> {
    let elapsed = u64::try_from(elapsed_ns)
        .ok()
        .filter(|&elapsed| elapsed > 0)?;
    let flights = u64_from_usize(
        observations
            .in_flight
            .saturating_add(observations.completions),
    )
    .max(1);
    let completions = u64_from_usize(observations.completions).max(1);
    elapsed.saturating_mul(flights).checked_div(completions)
}

/// One EWMA step; a differing sample always moves the estimate, floored at
/// one nanosecond.
fn ewma_step(estimate: u64, sample: u64) -> u64 {
    if sample >= estimate {
        estimate.saturating_add(sample.saturating_sub(estimate).div_ceil(ALPHA))
    } else {
        estimate
            .saturating_sub(estimate.saturating_sub(sample).div_ceil(ALPHA))
            .max(1)
    }
}

#[cfg(test)]
mod tests {
    use nectar_clock::ManualClock;

    use super::*;

    const BODY: usize = 4096;

    fn body() -> NonZeroUsize {
        NonZeroUsize::new(BODY).unwrap()
    }

    fn controller(
        bytes_per_second: u64,
        mean_latency: Duration,
        max: u16,
        clock: &ManualClock,
    ) -> AdaptiveWindow<&ManualClock> {
        AdaptiveWindow::new(
            bytes_per_second,
            mean_latency,
            body(),
            Window::new(max).unwrap(),
            clock,
        )
    }

    fn full(controller: &AdaptiveWindow<&ManualClock>, completions: usize) -> Observations {
        let slots = usize::from(controller.window().get());
        Observations {
            completions,
            occupancy: slots.saturating_sub(completions),
            in_flight: slots.saturating_sub(completions),
        }
    }

    #[test]
    fn seed_is_the_margined_set_point() {
        let clock = ManualClock::new(0);
        // 1 MB/s at 120 ms per fetch is 30 slots unmargined, 37 with the
        // five-quarters headroom.
        let controller = controller(1_000_000, Duration::from_millis(120), 100, &clock);
        assert_eq!(controller.window().get(), 37);
    }

    #[test]
    fn seed_respects_max() {
        let clock = ManualClock::new(0);
        let controller = controller(1_000_000, Duration::from_millis(120), 8, &clock);
        assert_eq!(controller.window().get(), 8);
    }

    #[test]
    fn converges_toward_realized_latency() {
        let clock = ManualClock::new(0);
        // Seeded for 120 ms fetches; the store realizes 15 ms.
        let mut controller = controller(1_000_000, Duration::from_millis(120), 100, &clock);
        let seed = controller.window().get();
        assert_eq!(controller.observe(&full(&controller, 0)).get(), seed);
        for _ in 0..200 {
            clock.advance(Duration::from_millis(15));
            let observations = Observations {
                completions: usize::from(controller.window().get()),
                occupancy: 0,
                in_flight: 0,
            };
            controller.observe(&observations);
        }
        // 1 MB/s at 15 ms is 4 slots unmargined, 5 margined.
        assert_eq!(controller.window().get(), 5);
        assert!(controller.window().get() < seed);
    }

    #[test]
    fn grows_only_when_the_window_was_the_constraint() {
        let clock = ManualClock::new(0);
        // Seeded for 10 ms fetches; the store realizes 100 ms, but the
        // window never fills: the consumer, not the cap, is the bound.
        let mut controller = controller(1_000_000, Duration::from_millis(10), 100, &clock);
        let seed = controller.window().get();
        controller.observe(&full(&controller, 0));
        for _ in 0..50 {
            clock.advance(Duration::from_millis(100));
            let observations = Observations {
                completions: 1,
                occupancy: 0,
                in_flight: 0,
            };
            controller.observe(&observations);
        }
        assert_eq!(controller.window().get(), seed);
        // A saturated window releases the growth.
        for _ in 0..200 {
            clock.advance(Duration::from_millis(100));
            let observations = full(&controller, 1);
            controller.observe(&observations);
        }
        assert!(controller.window().get() > seed);
    }

    #[test]
    fn growth_is_capped_at_max() {
        let clock = ManualClock::new(0);
        let mut controller = controller(50_000_000, Duration::from_millis(10), 24, &clock);
        controller.observe(&full(&controller, 0));
        for _ in 0..200 {
            clock.advance(Duration::from_millis(500));
            let observations = full(&controller, 2);
            controller.observe(&observations);
        }
        assert_eq!(controller.window().get(), 24);
    }

    #[test]
    fn rewound_clock_yields_no_sample() {
        let clock = ManualClock::new(1_000_000_000);
        let mut controller = controller(1_000_000, Duration::from_millis(120), 100, &clock);
        let seed = controller.window().get();
        controller.observe(&full(&controller, 0));
        clock.set_ns(0);
        assert_eq!(controller.observe(&full(&controller, 4)).get(), seed);
        // The rewound reading re-anchors the interval.
        clock.advance(Duration::from_millis(15));
        let observations = Observations {
            completions: 4,
            occupancy: 0,
            in_flight: 0,
        };
        controller.observe(&observations);
        assert!(controller.window().get() < seed);
    }

    #[test]
    fn spike_cannot_collapse_or_blow_up_in_one_step() {
        let clock = ManualClock::new(0);
        let mut controller = controller(1_000_000, Duration::from_millis(10), 200, &clock);
        let before = controller.latency_ns;
        controller.observe(&full(&controller, 0));
        clock.advance(Duration::from_secs(3600));
        controller.observe(&full(&controller, 1));
        // One interval moves the estimate by at most spike/alpha of itself.
        assert!(controller.latency_ns <= before.saturating_mul(2));
    }

    #[test]
    fn zero_rate_floors_at_one_slot() {
        let clock = ManualClock::new(0);
        let mut controller = controller(0, Duration::from_millis(10), 8, &clock);
        assert_eq!(controller.window().get(), 1);
        controller.observe(&full(&controller, 0));
        clock.advance(Duration::from_millis(10));
        assert_eq!(controller.observe(&full(&controller, 1)).get(), 1);
    }
}
