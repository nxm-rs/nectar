//! Inclusion proofs for segments of a Binary Merkle Tree.

use alloc::{vec, vec::Vec};
use alloy_primitives::B256;

use super::error::BmtError;
use super::hasher::{hash_pairs, node_hasher};
use crate::bmt::{Hasher, constants::*};
use crate::error::Result;

/// Represents a proof for a specific segment in a Binary Merkle Tree
#[derive(Clone, Debug)]
pub struct Proof {
    /// The segment index this proof is for
    pub segment_index: usize,
    /// The segment data being proven
    pub segment: B256,
    /// The sibling hashes on the path to the root, one per tree level.
    ///
    /// The length is fixed by the tree geometry, so an ill-sized path is
    /// unrepresentable rather than checked at verification time.
    pub proof_segments: [B256; PROOF_LENGTH],
    /// The span of the data
    pub span: u64,
    /// Optional prefix (used during verification)
    pub prefix: Option<Vec<u8>>,
}

impl Proof {
    /// Create a new BMT proof
    pub const fn new(
        segment_index: usize,
        segment: B256,
        proof_segments: [B256; PROOF_LENGTH],
        span: u64,
        prefix: Option<Vec<u8>>,
    ) -> Self {
        Self {
            segment_index,
            segment,
            proof_segments,
            span,
            prefix,
        }
    }

    /// Verify this proof against a root hash.
    ///
    /// The root is a typed 32-byte hash, so a mis-sized root cannot silently
    /// verify as a mismatch.
    pub fn verify(&self, root_hash: &B256) -> Result<bool> {
        // Start with the segment being proven
        let mut current_hash = self.segment;
        let mut current_index = self.segment_index;

        let prefix = self.prefix.as_deref();

        // Apply each proof segment to compute the root
        for proof_segment in &self.proof_segments {
            // Every intermediate node is keccak(prefix || left || right);
            // verifying without the prefix at each level would reject a valid
            // anchor-keyed proof.
            let mut hasher = node_hasher(prefix);

            // Order matters - left then right
            if current_index.is_multiple_of(2) {
                hasher.update(current_hash.as_slice());
                hasher.update(proof_segment.as_slice());
            } else {
                hasher.update(proof_segment.as_slice());
                hasher.update(current_hash.as_slice());
            }

            // Get hash for next level
            current_hash = B256::from_slice(hasher.finalize().as_slice());
            current_index /= 2;
        }

        // Final step: add prefix (if any) and span to compute the root hash
        let mut hasher = node_hasher(prefix);

        // Add span as little-endian bytes
        hasher.update(self.span.to_le_bytes());

        // Add the intermediate hash
        hasher.update(current_hash.as_slice());

        let computed_root = B256::from_slice(hasher.finalize().as_slice());

        // Compare with provided root hash
        Ok(computed_root == *root_hash)
    }
}

/// Extension trait to add proof-related functionality to BMTHasher
pub trait Prover {
    /// Generate a proof for a specific segment
    fn generate_proof(&self, data: &[u8], segment_index: usize) -> Result<Proof>;

    /// Verify a proof against a root hash
    fn verify_proof(proof: &Proof, root_hash: &B256) -> Result<bool>;
}

impl Prover for Hasher {
    fn generate_proof(&self, data: &[u8], segment_index: usize) -> Result<Proof> {
        // Materialise the BRANCHES zero-padded 32-byte leaf segments; data
        // past the tree width is ignored, matching the hashing geometry.
        let mut leaves = [[0u8; SEGMENT_SIZE]; BRANCHES];
        for (leaf, chunk) in leaves.iter_mut().zip(data.chunks(SEGMENT_SIZE)) {
            for (dst, src) in leaf.iter_mut().zip(chunk) {
                *dst = *src;
            }
        }

        // Get the segment being proven
        let Some(&segment_bytes) = leaves.get(segment_index) else {
            return Err(BmtError::SegmentOutOfBounds {
                index: segment_index,
                branches: BRANCHES,
            }
            .into());
        };
        let segment = B256::from(segment_bytes);

        // Include the prefix in the proof if there is one
        let prefix = if self.prefix().is_empty() {
            None
        } else {
            Some(self.prefix().to_vec())
        };
        let prefix_ref = prefix.as_deref();

        // Walk the tree bottom-up, batching each level's sibling pairs across
        // SIMD lanes. Zero padding is hashed literally, so under a prefix
        // every zero subtree comes out as keccak(prefix || ...). At each
        // level the sibling of the proven node is recorded before the level
        // is folded into the next.
        let mut current: Vec<[u8; 32]> = leaves.to_vec();
        let mut index = segment_index;
        let mut proof_segments = [B256::ZERO; PROOF_LENGTH];
        for slot in &mut proof_segments {
            let (level_pairs, _) = current.as_chunks::<2>();
            for (pair, pair_index) in level_pairs.iter().zip(0usize..) {
                if pair_index == index / 2 {
                    let [left, right] = pair;
                    *slot = B256::from(if index.is_multiple_of(2) {
                        *right
                    } else {
                        *left
                    });
                }
            }

            let mut next = vec![[0u8; 32]; current.len() / 2];
            hash_pairs(
                prefix_ref,
                current.as_flattened().chunks_exact(SEGMENT_PAIR_LENGTH),
                &mut next,
            );
            current = next;
            index /= 2;
        }

        Ok(Proof::new(
            segment_index,
            segment,
            proof_segments,
            self.span(),
            prefix,
        ))
    }

    fn verify_proof(proof: &Proof, root_hash: &B256) -> Result<bool> {
        proof.verify(root_hash)
    }
}
