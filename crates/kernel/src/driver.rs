//! The shared bounded-admission walk loop.

use core::fmt;
use core::task::{Context, Poll};

use crate::inflight::InFlight;

/// Per-walker divergence beneath the shared driver: the frontier and its
/// admission, the ready ordering, the completion fold with its error timing,
/// and the drain outcome. The driver owns only the loop and the in-flight
/// set; every semantic decision stays here, so a monomorphised [`Driver`] is
/// the hand-rolled walk.
pub trait WalkPolicy {
    /// One completed fetch, carrying its own routing back.
    type Fetched;
    /// One delivered unit of the walk.
    type Frame;
    /// Terminal fault the walk surfaces.
    type Error;
    /// Ready-ordering selector passed per poll.
    type Drain: Copy;

    /// Dispatch queued work into `in_flight` until neither lane may proceed.
    fn admit(&mut self, in_flight: &mut InFlight<Self::Fetched>);

    /// Take the next deliverable frame, if the drain permits one.
    fn take_ready(&mut self, drain: Self::Drain) -> Option<Self::Frame>;

    /// Fold one completion; an error terminates the walk at its own timing.
    fn absorb(&mut self, fetched: Self::Fetched) -> Result<(), Self::Error>;

    /// Outcome once the in-flight set empties: `Ok` is clean completion,
    /// `Err` is a stall with work still owed.
    fn drained(&self) -> Result<(), Self::Error>;
}

/// The one bounded-admission walk loop: `admit`, then drain a ready frame,
/// else poll one completion into the policy. All state lives in the policy
/// and the in-flight set, so every poll is cancel-safe.
///
/// `F` is `P::Fetched`, kept a separate parameter ON PURPOSE so a holder names
/// the in-flight set at the policy's own bounds: borrow-based walkers must not
/// inherit the file walk's `Clone + 'static`. Do NOT collapse to `Driver<P>`
/// (projecting `P::Fetched` in the field virally widens every holder). Holders
/// embed `Driver<Policy, Policy's Fetched alias>` as a private field and land
/// the poll delegation with it.
pub struct Driver<P, F> {
    policy: P,
    in_flight: InFlight<F>,
    done: bool,
}

impl<P, F> Driver<P, F> {
    /// The driven policy.
    pub const fn policy(&self) -> &P {
        &self.policy
    }

    /// The driven policy, mutably.
    pub const fn policy_mut(&mut self) -> &mut P {
        &mut self.policy
    }

    /// Consume the driver, recovering the policy (and any store it owns).
    pub fn into_policy(self) -> P {
        self.policy
    }

    /// Whether the last frame has been delivered or the walk has failed.
    pub const fn is_finished(&self) -> bool {
        self.done
    }
}

impl<P: WalkPolicy> Driver<P, P::Fetched> {
    /// Drive `policy` from an empty in-flight set. Bounded so `F` can only be
    /// `P::Fetched`: a mismatched `Driver` has no constructor.
    pub const fn new(policy: P) -> Self {
        Self {
            policy,
            in_flight: InFlight::new(),
            done: false,
        }
    }

    /// Deliver the next frame under `drain`. `Ready(None)` after the last
    /// frame or a terminal error; a later poll after either stays `None`.
    #[inline]
    pub fn poll(
        &mut self,
        cx: &mut Context<'_>,
        drain: P::Drain,
    ) -> Poll<Option<Result<P::Frame, P::Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            self.policy.admit(&mut self.in_flight);
            if let Some(frame) = self.policy.take_ready(drain) {
                return Poll::Ready(Some(Ok(frame)));
            }
            match self.in_flight.poll(cx) {
                Poll::Ready(Some(fetched)) => {
                    if let Err(error) = self.policy.absorb(fetched) {
                        self.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(None) => {
                    self.done = true;
                    return match self.policy.drained() {
                        Ok(()) => Poll::Ready(None),
                        Err(error) => Poll::Ready(Some(Err(error))),
                    };
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<P, F> fmt::Debug for Driver<P, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Driver")
            .field("in_flight", &self.in_flight.len())
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}
