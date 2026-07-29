//! The head-slot admission liveness predicate.

use crate::window::Window;

/// Head-slot admission over a window: any candidate may fill the window once
/// the head is served; until then the last free slot stays reserved for the
/// head, so the serial drain always progresses.
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
