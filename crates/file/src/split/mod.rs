//! Poll-native split engine: the one bounded ascent building a chunk tree.
//!
//! Every write mode feeds this engine, and only its ascent seals
//! intermediates; the optional hash window fans leaf seals onto the rayon
//! pool and admits them in leaf order through this same ascent. The engine
//! is push-driven and io-free (no spawns, channels or timers, beyond the
//! opt-in pool handoff), all state lives in the [`Split`], and sealed chunks
//! flow to the store through a bounded put window.
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
//! 6. Bounded hash window: pool leaf seals in flight never exceed the
//!    [`HashWindow`](crate::HashWindow); sealed leaves are admitted in leaf
//!    order and every draw lands at submission, so a deterministic mode's
//!    chunk stream is byte-identical to the serial engine's.

#[cfg(feature = "encryption")]
mod encrypted;
mod engine;
mod error;
#[cfg(feature = "rayon")]
mod handoff;
mod mode;
mod save;
#[cfg(test)]
mod tests;

#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use encrypted::{KeyError, KeySource, RandomKeys};
#[cfg(test)]
pub use engine::Split;
pub use error::{SaveError, SealError, SplitError};
#[cfg(all(test, feature = "rayon"))]
pub use mode::Sealed;
pub use mode::SplitMode;
#[cfg(test)]
pub(crate) use save::collect_into;
pub(crate) use save::save_source;

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
    /// Peak leaf seals in flight on the hash pool; zero on the serial
    /// engine.
    pub peak_hash_in_flight: usize,
    /// Peak sealed chunks awaiting a put slot.
    pub peak_pending: usize,
    /// Spine levels touched.
    pub peak_spine: usize,
}
