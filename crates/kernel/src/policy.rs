//! The adaptive-window policy seam.

use core::fmt;

use crate::window::Window;

/// Occupancy snapshot an engine hands its policy between admission rounds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Observations {
    /// Completions since the previous policy call.
    pub completions: usize,
    /// Slots held: in flight plus buffered.
    pub occupancy: usize,
    /// Fetches outstanding.
    pub in_flight: usize,
}

mod sealed {
    pub trait Sealed {}
}

/// Recomputes the admission window between rounds.
///
/// Sealed: use [`Fixed`], or wrap a closure with [`from_fn`]. A policy runs
/// in the poll path, so it must be cheap and non-blocking; any timing lives
/// inside the closure.
pub trait AdmitPolicy: sealed::Sealed {
    /// The window for the next admission round.
    fn window(&mut self, observations: &Observations) -> Window;
}

/// Constant window; the no-feedback policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fixed(Window);

impl Fixed {
    /// Policy pinned to `window`.
    pub const fn new(window: Window) -> Self {
        Self(window)
    }
}

impl sealed::Sealed for Fixed {}

impl AdmitPolicy for Fixed {
    fn window(&mut self, _observations: &Observations) -> Window {
        self.0
    }
}

/// Policy wrapping a closure; built by [`from_fn`].
pub struct FromFn<F>(F);

impl<F> FromFn<F> {
    /// Unwrap the closure with its accumulated state.
    pub fn into_inner(self) -> F {
        self.0
    }
}

/// Policy from `f`, called once per admission round.
pub const fn from_fn<F>(f: F) -> FromFn<F>
where
    F: FnMut(&Observations) -> Window,
{
    FromFn(f)
}

impl<F> sealed::Sealed for FromFn<F> where F: FnMut(&Observations) -> Window {}

impl<F> AdmitPolicy for FromFn<F>
where
    F: FnMut(&Observations) -> Window,
{
    fn window(&mut self, observations: &Observations) -> Window {
        (self.0)(observations)
    }
}

impl<F> fmt::Debug for FromFn<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FromFn").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_ignores_observations() {
        let window = Window::new(4).unwrap();
        let mut policy = Fixed::new(window);
        let observations = Observations {
            completions: 7,
            occupancy: 3,
            in_flight: 2,
        };
        assert_eq!(policy.window(&observations), window);
        assert_eq!(policy.window(&Observations::default()), window);
    }

    #[test]
    fn into_inner_keeps_closure_state() {
        let mut policy = from_fn(|observations: &Observations| {
            Window::new(u16::try_from(observations.completions.max(1)).unwrap()).unwrap()
        });
        assert_eq!(policy.window(&Observations::default()).get(), 1);
        let mut closure = policy.into_inner();
        let observations = Observations {
            completions: 3,
            occupancy: 0,
            in_flight: 0,
        };
        assert_eq!(closure(&observations).get(), 3);
    }

    #[test]
    fn from_fn_carries_closure_state() {
        let mut calls = 0u16;
        {
            let mut policy = from_fn(|observations: &Observations| {
                calls += 1;
                Window::new(u16::try_from(observations.occupancy.max(1)).unwrap()).unwrap()
            });
            let observations = Observations {
                completions: 0,
                occupancy: 9,
                in_flight: 9,
            };
            assert_eq!(policy.window(&observations).get(), 9);
            assert_eq!(policy.window(&Observations::default()).get(), 1);
        }
        assert_eq!(calls, 2);
    }
}
