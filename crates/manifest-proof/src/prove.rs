//! Proof generation: descend the manifest and record the authenticated path.

use nectar_manifest::{Format, Key, Node, V1};
use nectar_primitives::bmt::HASH_SIZE;
use nectar_primitives::{ChunkAddress, Hasher, Prover};

use crate::descent::{self, Step};
use crate::error::ProveError;
use crate::proof::{ForkPathProof, Granularity, PathStep};

/// A read-only supply of manifest nodes by address, the tree a prover descends.
///
/// Implemented for any `Fn(&ChunkAddress) -> Option<Node<F>>`, so an in-memory
/// map or a store adapter serves without a wrapper.
pub trait NodeSource<F: Format = V1> {
    /// The node stored at `address`, or `None` when the source lacks it.
    fn node(&self, address: &ChunkAddress) -> Option<Node<F>>;
}

impl<F: Format, T: Fn(&ChunkAddress) -> Option<Node<F>>> NodeSource<F> for T {
    fn node(&self, address: &ChunkAddress) -> Option<Node<F>> {
        self(address)
    }
}

/// Prove that `key` resolves to a value under `root`, at the given granularity.
///
/// Errors with [`ProveError::NotPresent`] when the key is absent, so the caller
/// never mints an inclusion proof for a key that has none.
pub fn prove_inclusion<F, S>(
    source: &S,
    root: &ChunkAddress,
    key: &Key,
    granularity: Granularity,
) -> Result<ForkPathProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let (steps, present) = build_path::<F, S>(source, root, key, granularity)?;
    if present {
        Ok(ForkPathProof::new(steps))
    } else {
        Err(ProveError::NotPresent)
    }
}

/// Prove that `key` is absent under `root`, at the given granularity.
///
/// Errors with [`ProveError::NotAbsent`] when the key is present: an exclusion
/// proof for a present key is unrepresentable, so absence is sound.
pub fn prove_exclusion<F, S>(
    source: &S,
    root: &ChunkAddress,
    key: &Key,
    granularity: Granularity,
) -> Result<ForkPathProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let (steps, present) = build_path::<F, S>(source, root, key, granularity)?;
    if present {
        Err(ProveError::NotAbsent)
    } else {
        Ok(ForkPathProof::new(steps))
    }
}

/// Descend from `root`, emitting one authenticated step per referenced hop and
/// reporting whether the key terminated in a value.
fn build_path<F, S>(
    source: &S,
    root: &ChunkAddress,
    key: &Key,
    granularity: Granularity,
) -> Result<(Vec<PathStep>, bool), ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let key = key.as_bytes();
    let mut steps = Vec::new();
    let mut address = *root;
    let mut pos = 0usize;
    loop {
        let node = source
            .node(&address)
            .ok_or(ProveError::NodeMissing(address))?;
        let payload = node.encode()?;
        let mut reached = 0usize;
        let outcome = descent::descend::<F>(&payload, key, pos, &mut reached)?;
        let step = match granularity {
            Granularity::Chunk => PathStep::Chunk { payload },
            Granularity::Segment => segment_step(&payload, reached)?,
        };
        steps.push(step);
        match outcome {
            Step::Found(_) => return Ok((steps, true)),
            Step::Absent => return Ok((steps, false)),
            Step::Encrypted => return Err(ProveError::Encrypted),
            Step::Follow(next, next_pos) => {
                address = next;
                pos = next_pos;
            }
        }
    }
}

/// Build a segment step covering the leading segments the descent read.
fn segment_step(payload: &[u8], reached: usize) -> Result<PathStep, ProveError> {
    let span = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    let count = last_segment(reached).saturating_add(1);
    let mut hasher = Hasher::new();
    hasher.set_span(span);
    let mut segments = Vec::with_capacity(count);
    for index in 0..count {
        let proof = hasher
            .generate_proof(payload, index)
            .map_err(ProveError::Bmt)?;
        segments.push(proof);
    }
    Ok(PathStep::Segment { segments })
}

/// The index of the segment holding the last byte the descent read.
fn last_segment(reached: usize) -> usize {
    reached
        .checked_sub(1)
        .map_or(0, |last| last.checked_div(HASH_SIZE).unwrap_or(0))
}
