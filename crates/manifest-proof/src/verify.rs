//! Proof verification: replay the descent over authenticated bytes.
//!
//! Each step's bytes are authenticated against the address the step before it
//! yielded, so the whole path is a hash chain from the trusted root. The
//! verifier trusts no claim in the proof: it re-derives every hop and the
//! terminal verdict from the authenticated bytes, so a tampered node, a wrong
//! value, or a wrong root fails to authenticate rather than mis-verifying.

use alloy_primitives::B256;
use nectar_manifest::{Format, Key};
use nectar_primitives::bmt::HASH_SIZE;
use nectar_primitives::{
    ChunkAddress, ChunkOps, ContentChunk, DEFAULT_BODY_SIZE, Proof as SegmentProof,
};

use crate::descent::{self, Step};
use crate::error::VerifyError;
use crate::proof::{ForkPathProof, PathStep, Verdict};

/// Verify `proof` for `key` under `root`, returning the authenticated verdict.
///
/// Replaying the descent is the whole check: the returned verdict is whatever
/// the authenticated nodes say, so a present key cannot yield `Absent` and a
/// proof cannot assert a value its bytes do not carry.
pub fn verify<F: Format>(
    root: &ChunkAddress,
    key: &Key,
    proof: &ForkPathProof,
) -> Result<Verdict<F>, VerifyError> {
    let steps = proof.steps();
    let Some(last) = steps.len().checked_sub(1) else {
        return Err(VerifyError::Empty);
    };
    let key = key.as_bytes();
    let mut trusted = *root;
    let mut pos = 0usize;
    for (index, step) in steps.iter().enumerate() {
        match authenticated_step::<F>(step, &trusted, key, pos, index)? {
            Step::Found(entry) => {
                return if index == last {
                    Ok(Verdict::Present(entry))
                } else {
                    Err(VerifyError::Malformed)
                };
            }
            Step::Absent => {
                return if index == last {
                    Ok(Verdict::Absent)
                } else {
                    Err(VerifyError::Malformed)
                };
            }
            Step::Encrypted => return Err(VerifyError::Malformed),
            Step::Follow(next, next_pos) => {
                if index == last {
                    return Err(VerifyError::Malformed);
                }
                trusted = next;
                pos = next_pos;
            }
        }
    }
    Err(VerifyError::Malformed)
}

/// Authenticate one node's bytes against `trusted`, then descend it for the key.
fn authenticated_step<F: Format>(
    step: &PathStep,
    trusted: &ChunkAddress,
    key: &[u8],
    pos: usize,
    index: usize,
) -> Result<Step<F>, VerifyError> {
    let mut scratch = 0usize;
    match step {
        PathStep::Chunk { payload } => {
            let chunk = ContentChunk::<DEFAULT_BODY_SIZE>::new(payload.clone())
                .map_err(VerifyError::Seal)?;
            if chunk.address() != trusted {
                return Err(VerifyError::Unauthenticated(index));
            }
            Ok(descent::descend::<F>(payload, key, pos, &mut scratch)?)
        }
        PathStep::Segment { segments } => {
            let bytes = reassemble(segments, trusted, index)?;
            Ok(descent::descend::<F>(&bytes, key, pos, &mut scratch)?)
        }
    }
}

/// Authenticate a contiguous leading run of segments against `trusted` and
/// reassemble their bytes.
fn reassemble(
    segments: &[SegmentProof],
    trusted: &ChunkAddress,
    index: usize,
) -> Result<Vec<u8>, VerifyError> {
    let root = B256::from(*trusted);
    let mut bytes = Vec::with_capacity(segments.len().saturating_mul(HASH_SIZE));
    for (expected, proof) in segments.iter().enumerate() {
        // The run must start at segment zero and leave no gap, so the
        // reassembled prefix is anchored and contiguous.
        if proof.segment_index != expected {
            return Err(VerifyError::SegmentGap);
        }
        if !proof.verify(&root).map_err(VerifyError::Bmt)? {
            return Err(VerifyError::Unauthenticated(index));
        }
        bytes.extend_from_slice(proof.segment.as_slice());
    }
    if bytes.is_empty() {
        return Err(VerifyError::SegmentGap);
    }
    Ok(bytes)
}
