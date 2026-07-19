//! Engine admission budgets: the bounded windows the drains admit work
//! against.

use core::num::{NonZeroU16, NonZeroU32};

/// Sixteen slots: the default depth of both bounded windows.
const DEFAULT_SLOTS: NonZeroU16 = match NonZeroU16::new(16) {
    Some(slots) => slots,
    None => NonZeroU16::MIN,
};
const _: () = assert!(DEFAULT_SLOTS.get() == 16);

/// Fetch window: leaf fetches a read drain may hold in flight.
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
