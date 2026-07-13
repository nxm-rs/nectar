//! The proof data model: an ordered authenticated descent path.

use nectar_manifest::{Entry, Format, V1};
use nectar_primitives::Proof as SegmentProof;

/// Which authentication granularity a proof step ships.
///
/// Both anchor every byte to the node's address; they trade proof size against
/// verifier work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Granularity {
    /// Whole node bytes; the verifier re-BMTs each node (about 4 KB per node).
    Chunk,
    /// Only the leading BMT segments the descent reads, each behind its sibling
    /// path; the on-chain-cheap form (about 32 bytes plus seven hashes per
    /// covered segment).
    Segment,
}

/// One authenticated node on the descent path.
///
/// Each step authenticates its node against the address handed down from the
/// step before it (the trusted root for the first step), so a whole proof is a
/// hash chain anchored at the root.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PathStep {
    /// The node's whole payload; its address is the BMT of these bytes.
    Chunk {
        /// The node payload the verifier re-BMTs.
        payload: Vec<u8>,
    },
    /// A contiguous run of the node's leading BMT segments, from segment zero,
    /// each with the sibling path authenticating it against the node address.
    Segment {
        /// The covering segments in ascending index from zero.
        segments: Vec<SegmentProof>,
    },
}

/// The authenticated descent path for a key under a root: the ordered nodes the
/// descent visits, each authenticated against the previous node's reference.
///
/// A proof carries no verdict of its own; [`verify`](crate::verify) replays the
/// descent over the authenticated bytes and reports what it finds, so a proof
/// cannot assert a value its bytes do not.
#[derive(Clone, Debug)]
pub struct ForkPathProof {
    steps: Vec<PathStep>,
}

impl ForkPathProof {
    /// Assemble a proof from its ordered steps.
    #[must_use]
    pub const fn new(steps: Vec<PathStep>) -> Self {
        Self { steps }
    }

    /// The ordered path steps, root first.
    #[must_use]
    pub fn steps(&self) -> &[PathStep] {
        &self.steps
    }

    /// The number of authenticated nodes on the path.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.steps.len()
    }

    /// Whether the path has no steps; never true for a proof `verify` accepts.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

/// The outcome a verified proof establishes for a key under a root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict<F: Format = V1> {
    /// The key is present with this value.
    Present(Entry<F>),
    /// The key is provably absent.
    Absent,
}
