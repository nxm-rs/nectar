//! Poll-native walk engine: the one bounded descent over a chunk tree.
//!
//! Every read mode drains this engine; nothing else in the crate fetches
//! tree nodes. The engine is pull-driven and io-free (no spawns, channels or
//! timers), all state lives in the [`Walk`], and completions are routed back
//! by the offset sequence each fetch carries.
//!
//! Normative invariants, each pinned by a test:
//!
//! 1. Fetch identity: the fetch set over the requested range equals the
//!    serial walk's, every node is fetched at most once, and every
//!    completion must return the requested address.
//! 2. Two-budget admission: leaf fetches draw on the
//!    [`Window`](crate::Window); branch fetches draw on the derived
//!    [`BranchBudget`](crate::BranchBudget), and a non-head branch is
//!    admitted only when the leaf frontier can absorb every outstanding
//!    expansion, capping buffered leaf references at `window + 2 *
//!    branches`.
//! 3. Head liveness: in-flight plus buffered leaf bodies never exceed the
//!    window, one slot stays reserved for the head (the node covering the
//!    lowest unconsumed offset) until the head holds one, and the head is
//!    exempt from the absorption rule, so the ordered drain progresses at
//!    any window depth.
//! 4. No retries: every store error is terminal and typed; retry policy
//!    composes beneath the store seam.

mod engine;
mod error;
mod mode;
#[cfg(test)]
mod tests;

use bytes::Bytes;

pub use engine::Walk;
pub use error::{DecodeError, ShapeError, WalkError};
pub use mode::{Encrypted, Plain, WalkMode};

/// One delivered run of file bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Absolute byte offset of the run.
    pub offset: u64,
    /// In-range body bytes at that offset.
    pub data: Bytes,
}

/// Occupancy witnesses of one walk.
///
/// The peaks pin the engine's memory bounds in tests: leaf bodies never
/// exceed the window, branch fetches never exceed the derived budget, and
/// buffered leaf references stay within `window + 2 * branches`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WalkStats {
    /// Store fetches dispatched.
    pub fetches: u64,
    /// Peak leaf bodies held at once, in flight plus buffered.
    pub peak_occupancy: usize,
    /// Peak branch fetches in flight.
    pub peak_branch_in_flight: usize,
    /// Peak buffered leaf references awaiting dispatch.
    pub peak_leaf_frontier: usize,
}
