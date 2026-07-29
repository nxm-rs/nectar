//! The head-slot admission liveness predicate.

use crate::window::Window;

/// Head-slot admission over a window: any candidate may fill the window once
/// the head is served; until then the last free slot stays reserved for the
/// head, so the serial drain always progresses.
///
/// The reserve is the walk's liveness contract, not a fairness rule. A
/// walker that admits out of order but delivers in order holds no fixed
/// head: it recomputes `head_served` against its current head of line on
/// every call, so the slot the reserve protects follows the head as the
/// walk advances. Read-ahead can therefore never fill the window and strand
/// the one fetch the serial drain is waiting on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Admission(Window);

impl Admission {
    /// Admission over `window` slots.
    pub const fn new(window: Window) -> Self {
        Self(window)
    }

    /// The window admitted against.
    pub const fn window(self) -> Window {
        self.0
    }

    /// Whether one more admission is granted at `occupancy`. `head_served`
    /// holds when the candidate is the head, or the head already holds a
    /// slot, in flight or buffered.
    ///
    /// `head_served` is a per-call input, not walker state: pass the value
    /// for the head of line at this moment. A `false` grant stops one short
    /// of the window, keeping a slot free for that head.
    pub fn admits(self, occupancy: usize, head_served: bool) -> bool {
        let window = usize::from(self.0.get());
        let cap = if head_served {
            window
        } else {
            window.saturating_sub(1)
        };
        occupancy < cap
    }
}

impl From<Window> for Admission {
    fn from(window: Window) -> Self {
        Self::new(window)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn window_of_one_is_serial() {
        let admission = Admission::new(Window::new(1).unwrap());
        assert!(admission.admits(0, true));
        assert!(!admission.admits(1, true));
        assert!(!admission.admits(0, false));
    }

    #[test]
    fn read_ahead_leaves_the_head_a_slot_until_it_is_served() {
        let admission = Admission::new(Window::new(4).unwrap());
        // The head is not launchable yet, so every candidate is non-head.
        let mut occupancy = 0;
        while admission.admits(occupancy, false) {
            occupancy += 1;
        }
        assert_eq!(occupancy, 3, "read-ahead saturates one slot short");
        // The head becomes launchable and takes the slot held for it.
        assert!(admission.admits(occupancy, true));
        occupancy += 1;
        // The window is full; it is closed to the head as well.
        assert!(!admission.admits(occupancy, true));
    }

    #[test]
    fn non_head_stops_one_short_of_the_window() {
        let admission = Admission::new(Window::new(16).unwrap());
        assert!(admission.admits(14, false));
        assert!(!admission.admits(15, false));
        assert!(admission.admits(15, true));
        assert!(!admission.admits(16, true));
    }

    proptest! {
        #[test]
        fn admission_is_capped_reserved_and_monotone(
            slots in 1..=u16::MAX,
            occupancy in 0usize..=140_000,
            head_served: bool,
        ) {
            let window = Window::new(slots).unwrap();
            let admission = Admission::new(window);
            let admits = admission.admits(occupancy, head_served);
            // Cap: a grant never exceeds the window.
            prop_assert!(!admits || occupancy < usize::from(slots));
            // Liveness: the head is granted exactly while a slot is free.
            if head_served {
                prop_assert_eq!(admits, occupancy < usize::from(slots));
            }
            // Reserve: a non-head grant leaves a free slot for the head.
            if !head_served && admits {
                prop_assert!(occupancy + 1 < usize::from(slots));
            }
            // Monotone: lower occupancy or a served head never loses a grant.
            if admits && occupancy > 0 {
                prop_assert!(admission.admits(occupancy - 1, head_served));
            }
            if admits {
                prop_assert!(admission.admits(occupancy, true));
            }
        }
    }
}
