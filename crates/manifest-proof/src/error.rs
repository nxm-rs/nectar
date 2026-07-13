//! Prove- and verify-side failures.

use nectar_manifest::EncodeError;
use nectar_primitives::{ChunkAddress, PrimitivesError};

use crate::descent::DescentError;

/// A proof-generation failure.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ProveError {
    /// A node the descent needed was not in the source.
    #[error("node {0} not found")]
    NodeMissing(ChunkAddress),
    /// A node on the path could not be re-encoded to a single chunk; a spilled
    /// node has no one-chunk form and is out of scope here.
    #[error(transparent)]
    Encode(#[from] EncodeError),
    /// Reading a node's own bytes failed; a source node re-encoded to a
    /// malformed image, which is a source bug.
    #[error(transparent)]
    Descent(#[from] DescentError),
    /// Generating a BMT segment proof failed.
    #[error("bmt segment proof")]
    Bmt(#[source] PrimitivesError),
    /// The descent funnelled into an encrypted subtree the plain prover cannot
    /// open.
    #[error("descent reached an encrypted subtree")]
    Encrypted,
    /// An inclusion proof was asked for an absent key.
    #[error("key is absent")]
    NotPresent,
    /// An exclusion proof was asked for a present key.
    #[error("key is present")]
    NotAbsent,
}

/// A proof-verification failure. A rejected proof always fails here rather than
/// returning a wrong verdict.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The proof carried no steps.
    #[error("empty proof")]
    Empty,
    /// A node's authenticated bytes did not hash to the address handed down to
    /// it: a tampered node, wrong value, or wrong root.
    #[error("node bytes do not match the trusted address at step {0}")]
    Unauthenticated(usize),
    /// Sealing a chunk step's payload for re-hashing failed.
    #[error("seal chunk step")]
    Seal(#[source] PrimitivesError),
    /// Verifying a BMT segment proof failed.
    #[error("bmt segment proof")]
    Bmt(#[source] PrimitivesError),
    /// A segment step's segments were not a contiguous run from index zero.
    #[error("segment proof indices are not contiguous from zero")]
    SegmentGap,
    /// Reading a node's authenticated bytes failed.
    #[error(transparent)]
    Descent(#[from] DescentError),
    /// The path branched or terminated where the descent did not: a middle step
    /// that terminates, a last step that continues, or a descent into an
    /// encrypted subtree.
    #[error("proof path does not match the descent")]
    Malformed,
}
