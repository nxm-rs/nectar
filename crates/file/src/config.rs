//! Engine admission budgets: the bounded windows the drains admit work
//! against.

use core::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use core::time::Duration;

/// Sixteen slots: the default depth of both bounded windows.
const DEFAULT_SLOTS: NonZeroU16 = match NonZeroU16::new(16) {
    Some(slots) => slots,
    None => NonZeroU16::MIN,
};
const _: () = assert!(DEFAULT_SLOTS.get() == 16);

/// Denominator scale of the throughput hint's byte-nanosecond arithmetic.
const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Fetch window: leaf bodies a read drain may hold resident, in flight or
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
    /// a leaf fetch takes `mean_latency`, `ceil(rate * latency /
    /// body_size)` saturated into `1..=u16::MAX`.
    ///
    /// ```
    /// use core::num::NonZeroUsize;
    /// use core::time::Duration;
    /// use nectar_file::Window;
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

/// Put window: sealed chunks a write drain may hold awaiting store puts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PutWindow(NonZeroU16);

impl PutWindow {
    /// Default window of sixteen put slots.
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
}

impl Default for PutWindow {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<NonZeroU16> for PutWindow {
    fn from(slots: NonZeroU16) -> Self {
        Self(slots)
    }
}

impl From<PutWindow> for NonZeroU16 {
    fn from(window: PutWindow) -> Self {
        window.0
    }
}

/// Hash window: leaf seals a split may hold in flight on the thread pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HashWindow(NonZeroU16);

impl HashWindow {
    /// Default window of sixteen seal slots.
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
}

impl Default for HashWindow {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<NonZeroU16> for HashWindow {
    fn from(slots: NonZeroU16) -> Self {
        Self(slots)
    }
}

impl From<HashWindow> for NonZeroU16 {
    fn from(window: HashWindow) -> Self {
        window.0
    }
}

/// Branch budget: intermediate fetches a walk may hold in flight. Derived
/// from the fetch window, never configured directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BranchBudget(NonZeroU32);

impl BranchBudget {
    /// `max(1, 1 + window / branches)`: a branch is admitted only when the
    /// pending-reference queue can absorb its full expansion, and the floor
    /// of one slot keeps descent live at any window.
    pub const fn derive(window: Window, branches: u32) -> Self {
        let per_expansion = match u32_from_u16(window.get()).checked_div(branches) {
            Some(quotient) => quotient,
            // A zero fan-out has no expansion to absorb; the floor stands.
            None => 0,
        };
        Self(at_least_one(per_expansion.saturating_add(1)))
    }

    /// Budget depth in slots.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// `max(1, value)`: budgets never fall below one slot.
const fn at_least_one(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(slots) => slots,
        None => NonZeroU32::MIN,
    }
}

/// Lossless const widening; `From` is not const-callable.
const fn u32_from_u16(value: u16) -> u32 {
    let [low, high] = value.to_le_bytes();
    u32::from_le_bytes([low, high, 0, 0])
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
    fn windows_reject_zero() {
        assert!(Window::new(0).is_none());
        assert!(PutWindow::new(0).is_none());
        assert!(HashWindow::new(0).is_none());
    }

    #[test]
    fn window_defaults_are_sixteen() {
        assert_eq!(Window::DEFAULT.get(), 16);
        assert_eq!(Window::default(), Window::DEFAULT);
        assert_eq!(PutWindow::DEFAULT.get(), 16);
        assert_eq!(PutWindow::default(), PutWindow::DEFAULT);
        assert_eq!(HashWindow::DEFAULT.get(), 16);
        assert_eq!(HashWindow::default(), HashWindow::DEFAULT);
    }

    #[test]
    fn windows_round_trip_through_nonzero() {
        let slots = NonZeroU16::new(5).unwrap();
        assert_eq!(NonZeroU16::from(Window::from(slots)), slots);
        assert_eq!(NonZeroU16::from(PutWindow::from(slots)), slots);
        assert_eq!(NonZeroU16::from(HashWindow::from(slots)), slots);
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

    #[test]
    fn branch_budget_floors_at_one() {
        assert_eq!(BranchBudget::derive(Window::DEFAULT, 128).get(), 1);
        assert_eq!(BranchBudget::derive(Window::DEFAULT, 0).get(), 1);
        let one = Window::new(1).unwrap();
        assert_eq!(BranchBudget::derive(one, 128).get(), 1);
    }

    #[test]
    fn branch_budget_tracks_expansion() {
        let window = Window::new(256).unwrap();
        assert_eq!(BranchBudget::derive(window, 64).get(), 5);
        assert_eq!(BranchBudget::derive(window, 128).get(), 3);
    }
}
