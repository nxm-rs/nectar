//! State-transition proofs: what happened to one key between two roots.
//!
//! Each is two single-key proofs read against different roots: the key's state
//! under `root_before` and under `root_after`. An insertion is exclusion then
//! inclusion (absent, then present); its duals swap the halves - deletion is
//! inclusion then exclusion, an update is inclusion under both roots with a
//! changed value. The verifier confirms both halves and the shape, so a claim
//! that no change happened, or the wrong one, fails to authenticate.

use nectar_manifest::{Entry, Format, Key, V1};
use nectar_primitives::ChunkAddress;

use crate::error::{ProveError, VerifyError};
use crate::proof::{ForkPathProof, Granularity, Verdict};
use crate::prove::{NodeSource, prove_exclusion, prove_inclusion};
use crate::verify::verify;

/// Which state change a [`TransitionProof`] attests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Absent under the first root, present under the second.
    Insertion,
    /// Present under the first root, absent under the second.
    Deletion,
    /// Present under both roots with a changed value.
    Update,
}

/// The change a verified [`TransitionProof`] establishes for a key between two
/// roots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Transition<F: Format = V1> {
    /// The key was inserted with this value.
    Insertion(Entry<F>),
    /// The key holding this value was deleted.
    Deletion(Entry<F>),
    /// The key's value changed.
    Update {
        /// The value under the first root.
        before: Entry<F>,
        /// The value under the second root.
        after: Entry<F>,
    },
}

/// A proof that one key changed state between two roots: its authenticated state
/// under each, plus the shape they must form.
///
/// Carries no outcome of its own; [`verify_transition`] reads both halves and
/// rejects any pair that does not form the attested shape.
#[derive(Clone, Debug)]
pub struct TransitionProof {
    before: ForkPathProof,
    after: ForkPathProof,
    kind: Kind,
}

/// Prove that `key` was inserted between `root_before` and `root_after`: absent
/// under the first root, present under the second.
///
/// Errors when the shape does not hold - the key is present under the first
/// root, or absent under the second - so a false insertion is unrepresentable.
pub fn prove_transition<F, B, A>(
    before_source: &B,
    root_before: &ChunkAddress,
    after_source: &A,
    root_after: &ChunkAddress,
    key: &Key,
    granularity: Granularity,
) -> Result<TransitionProof, ProveError>
where
    F: Format,
    B: NodeSource<F>,
    A: NodeSource<F>,
{
    let before = prove_exclusion::<F, B>(before_source, root_before, key, granularity)?;
    let after = prove_inclusion::<F, A>(after_source, root_after, key, granularity)?;
    Ok(TransitionProof {
        before,
        after,
        kind: Kind::Insertion,
    })
}

/// Prove that `key` was deleted between `root_before` and `root_after`: present
/// under the first root, absent under the second. The dual of an insertion.
pub fn prove_deletion<F, B, A>(
    before_source: &B,
    root_before: &ChunkAddress,
    after_source: &A,
    root_after: &ChunkAddress,
    key: &Key,
    granularity: Granularity,
) -> Result<TransitionProof, ProveError>
where
    F: Format,
    B: NodeSource<F>,
    A: NodeSource<F>,
{
    let before = prove_inclusion::<F, B>(before_source, root_before, key, granularity)?;
    let after = prove_exclusion::<F, A>(after_source, root_after, key, granularity)?;
    Ok(TransitionProof {
        before,
        after,
        kind: Kind::Deletion,
    })
}

/// Prove that `key`'s value changed between `root_before` and `root_after`:
/// present under both roots. The verifier rejects a proof whose two values are
/// equal, so a no-op is not a transition.
pub fn prove_update<F, B, A>(
    before_source: &B,
    root_before: &ChunkAddress,
    after_source: &A,
    root_after: &ChunkAddress,
    key: &Key,
    granularity: Granularity,
) -> Result<TransitionProof, ProveError>
where
    F: Format,
    B: NodeSource<F>,
    A: NodeSource<F>,
{
    let before = prove_inclusion::<F, B>(before_source, root_before, key, granularity)?;
    let after = prove_inclusion::<F, A>(after_source, root_after, key, granularity)?;
    Ok(TransitionProof {
        before,
        after,
        kind: Kind::Update,
    })
}

/// Verify `proof` for `key` across `root_before` and `root_after`, returning the
/// authenticated change.
///
/// Both halves are re-verified against their own root and the pair must form the
/// attested shape, so a wrong root, a tampered half, or a mislabelled change
/// fails rather than mis-verifying.
pub fn verify_transition<F: Format>(
    root_before: &ChunkAddress,
    root_after: &ChunkAddress,
    key: &Key,
    proof: &TransitionProof,
) -> Result<Transition<F>, VerifyError> {
    let before = verify::<F>(root_before, key, &proof.before)?;
    let after = verify::<F>(root_after, key, &proof.after)?;
    match proof.kind {
        Kind::Insertion => match (before, after) {
            (Verdict::Absent, Verdict::Present(value)) => Ok(Transition::Insertion(value)),
            _ => Err(VerifyError::Malformed),
        },
        Kind::Deletion => match (before, after) {
            (Verdict::Present(value), Verdict::Absent) => Ok(Transition::Deletion(value)),
            _ => Err(VerifyError::Malformed),
        },
        Kind::Update => match (before, after) {
            (Verdict::Present(before), Verdict::Present(after)) if before != after => {
                Ok(Transition::Update { before, after })
            }
            _ => Err(VerifyError::Malformed),
        },
    }
}
