//! Poll-native split engine: the one bounded ascent building a chunk tree.
//!
//! Every write mode feeds this engine, and only its ascent seals
//! intermediates; the batch ingest pre-seals leaves but threads them through
//! this same ascent. The engine is push-driven and io-free (no spawns, channels or
//! timers), all state lives in the [`Split`], and sealed chunks flow to the
//! store through a bounded put window.
//!
//! Normative invariants, each pinned by a test:
//!
//! 1. Root identity: the sealed chunk set and root over any byte stream
//!    equal a whole-buffer split of the same bytes, including the lone
//!    trailing reference that carries up unwrapped.
//! 2. Bounded put window: puts in flight never exceed the
//!    [`PutWindow`](crate::PutWindow); sealed chunks awaiting a slot are
//!    bounded by the spine height, and no further bytes are consumed while
//!    any remain.
//! 3. Cancel-safe write: a put slot is secured before any byte is consumed,
//!    so an abandoned `poll_write` consumes nothing.
//! 4. Poisoned fuse: every error is terminal; after one, every poll returns
//!    [`Poisoned`](SplitError::Poisoned). Retry policy composes beneath the
//!    store seam.
//! 5. Fused finish: `poll_finish` is cancel-safe and re-callable; after the
//!    root is delivered every later call returns the same root.

mod engine;
mod error;
mod mode;
#[cfg(test)]
mod tests;

pub use engine::Split;
pub use error::SplitError;
pub use mode::{Sealed, SplitMode};

/// Occupancy witnesses of one split.
///
/// The peaks pin the engine's memory bounds in tests: puts in flight never
/// exceed the window, and sealed chunks awaiting a slot stay within the
/// spine height.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SplitStats {
    /// File bytes consumed.
    pub bytes: u64,
    /// Leaf chunks sealed.
    pub leaves: u64,
    /// Intermediate chunks sealed.
    pub intermediates: u64,
    /// Store puts dispatched.
    pub puts: u64,
    /// Peak puts in flight.
    pub peak_put_in_flight: usize,
    /// Peak sealed chunks awaiting a put slot.
    pub peak_pending: usize,
    /// Spine levels touched.
    pub peak_spine: usize,
}
