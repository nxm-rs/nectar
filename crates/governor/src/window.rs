//! The read-ahead window the drains admit against.

use core::num::{NonZeroU16, NonZeroUsize};
use core::time::Duration;

/// Sixteen slots: the default window depth.
const DEFAULT_SLOTS: NonZeroU16 = match NonZeroU16::new(16) {
    Some(slots) => slots,
    None => NonZeroU16::MIN,
};
const _: () = assert!(DEFAULT_SLOTS.get() == 16);

/// Denominator scale of the throughput hint's byte-nanosecond arithmetic.
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Read-ahead window: fetches a drain may hold unconsumed, in flight or
/// buffered awaiting delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Window(NonZeroU16);

impl Window {
    /// Default window of sixteen fetch slots.
    pub const DEFAULT: Self = Self(DEFAULT_SLOTS);

    /// `None` when `slots` is zero; const twin of the `NonZeroU16`
    /// conversion.
    pub const fn new(slots: u16) -> Option<Self> {
        match NonZeroU16::new(slots) {
            Some(slots) => Some(Self(slots)),
            None => None,
        }
    }

    /// Window depth in slots.
    pub const fn get(self) -> u16 {
        self.0.get()
    }

    /// Little's law sizing: the slots that sustain `bytes_per_second` when
    /// a fetch takes `mean_latency`, `ceil(rate * latency / body_size)`
    /// saturated into `1..=u16::MAX`.
    ///
    /// ```
    /// use core::num::NonZeroUsize;
    /// use core::time::Duration;
    /// use nectar_governor::Window;
    ///
    /// let body = NonZeroUsize::new(4096).unwrap();
    /// // A 1 MB/s stream at 120 ms per fetch needs thirty slots.
    /// let window = Window::for_throughput(1_000_000, Duration::from_millis(120), body);
    /// assert_eq!(window.get(), 30);
    /// ```
    pub const fn for_throughput(
        bytes_per_second: u64,
        mean_latency: Duration,
        body_size: NonZeroUsize,
    ) -> Self {
        // Bytes resident under Little's law, in byte-nanoseconds to keep
        // the arithmetic integral.
        let resident = u128_from_u64(bytes_per_second).checked_mul(mean_latency.as_nanos());
        // Never zero: the body size is nonzero and the factor saturates.
        let slot = u128_from_usize(body_size.get()).saturating_mul(NANOS_PER_SEC);
        let slots = match resident {
            Some(bytes) => bytes.div_ceil(slot),
            None => u128::MAX,
        };
        match NonZeroU16::new(saturating_u16(slots)) {
            Some(slots) => Self(slots),
            None => Self(NonZeroU16::MIN),
        }
    }
}

impl Default for Window {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<NonZeroU16> for Window {
    fn from(slots: NonZeroU16) -> Self {
        Self(slots)
    }
}

impl From<Window> for NonZeroU16 {
    fn from(window: Window) -> Self {
        window.0
    }
}

/// Lossless const widening; `From` is not const-callable.
const fn u128_from_u64(value: u64) -> u128 {
    let [a, b, c, d, e, f, g, h] = value.to_le_bytes();
    u128::from_le_bytes([a, b, c, d, e, f, g, h, 0, 0, 0, 0, 0, 0, 0, 0])
}

/// Lossless const widening; `From` is not const-callable.
#[cfg(target_pointer_width = "64")]
const fn u128_from_usize(value: usize) -> u128 {
    let [a, b, c, d, e, f, g, h] = value.to_le_bytes();
    u128::from_le_bytes([a, b, c, d, e, f, g, h, 0, 0, 0, 0, 0, 0, 0, 0])
}

/// Lossless const widening; `From` is not const-callable.
#[cfg(target_pointer_width = "32")]
const fn u128_from_usize(value: usize) -> u128 {
    let [a, b, c, d] = value.to_le_bytes();
    u128::from_le_bytes([a, b, c, d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
}

/// Saturating const narrowing: values past `u16::MAX` clamp to it.
const fn saturating_u16(value: u128) -> u16 {
    if value > 0xFFFF {
        u16::MAX
    } else {
        let [low, high, ..] = value.to_le_bytes();
        u16::from_le_bytes([low, high])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_rejects_zero() {
        assert!(Window::new(0).is_none());
    }

    #[test]
    fn window_default_is_sixteen() {
        assert_eq!(Window::DEFAULT.get(), 16);
        assert_eq!(Window::default(), Window::DEFAULT);
    }

    #[test]
    fn window_round_trips_through_nonzero() {
        let slots = NonZeroU16::new(5).unwrap();
        assert_eq!(NonZeroU16::from(Window::from(slots)), slots);
    }

    #[test]
    fn throughput_window_applies_littles_law() {
        let body = NonZeroUsize::new(4096).unwrap();
        // Sixteen bodies per second at one second per fetch.
        let window = Window::for_throughput(16 * 4096, Duration::from_secs(1), body);
        assert_eq!(window.get(), 16);
        // A partial slot rounds up.
        let window = Window::for_throughput(16 * 4096 + 1, Duration::from_secs(1), body);
        assert_eq!(window.get(), 17);
        // A quarter of the latency needs a quarter of the slots.
        let window = Window::for_throughput(16 * 4096, Duration::from_millis(250), body);
        assert_eq!(window.get(), 4);
    }

    #[test]
    fn throughput_window_floors_at_one() {
        let body = NonZeroUsize::new(4096).unwrap();
        assert_eq!(
            Window::for_throughput(0, Duration::from_secs(1), body).get(),
            1
        );
        assert_eq!(
            Window::for_throughput(u64::MAX, Duration::ZERO, body).get(),
            1
        );
        assert_eq!(
            Window::for_throughput(1, Duration::from_nanos(1), body).get(),
            1
        );
    }

    #[test]
    fn throughput_window_saturates_at_slot_width() {
        let body = NonZeroUsize::new(1).unwrap();
        let window = Window::for_throughput(u64::MAX, Duration::from_secs(1), body);
        assert_eq!(window.get(), u16::MAX);
        // An overflowing byte-nanosecond product saturates, never wraps.
        let window = Window::for_throughput(u64::MAX, Duration::MAX, body);
        assert_eq!(window.get(), u16::MAX);
    }
}
