//! Proof-related traits and structures for the Binary Merkle Tree
//!
//! This module provides functionality for generating and verifying inclusion proofs
//! for specific segments within a binary merkle tree.

use alloy_primitives::{B256, Keccak256};

use super::hasher::hash_pairs;
use crate::bmt::{Hasher, constants::*, error::BmtError};
use crate::error::Result;

/// Construct a Keccak256 seeded with the prefix when one is present.
///
/// Mirrors the hasher's per-node prefixing so that every node in a proof path is
/// `keccak(prefix || data)`, byte-identical to bee's prefix BMT.
#[inline(always)]
fn new_node_hasher(prefix: Option<&[u8]>) -> Keccak256 {
    let mut hasher = Keccak256::new();
    if let Some(p) = prefix {
        hasher.update(p);
    }
    hasher
}

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
            // Every intermediate node is keccak(prefix || left || right) to match
            // bee's per-node prefix hasher; verifying without the prefix at each
            // level would reject a valid anchor-keyed proof on-chain.
            let mut hasher = new_node_hasher(prefix);

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
        let mut hasher = Keccak256::new();

        // Add prefix if present
        if let Some(prefix) = &self.prefix {
            hasher.update(prefix);
        }

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
    #[allow(clippy::indexing_slicing)] // n = min(data.len(), ..) bounds data[..n], chunk.len() <= SEGMENT_SIZE bounds leaf[..], segment_index < BRANCHES is checked above, from_fn yields exactly PROOF_LENGTH levels so levels[level] is in range, and index halves in lockstep with each level's width so levels[level][index ^ 1] is in range
    fn generate_proof(&self, data: &[u8], segment_index: usize) -> Result<Proof> {
        if segment_index >= BRANCHES {
            return Err(self::BmtError::invalid_input_size(format!(
                "Segment index {segment_index} out of bounds for BRANCHES"
            ))
            .into());
        }

        // Materialise the BRANCHES zero-padded 32-byte leaf segments.
        let mut leaves = [[0u8; SEGMENT_SIZE]; BRANCHES];
        let n = data.len().min(BRANCHES * SEGMENT_SIZE);
        for (leaf, chunk) in leaves.iter_mut().zip(data[..n].chunks(SEGMENT_SIZE)) {
            leaf[..chunk.len()].copy_from_slice(chunk);
        }

        // Get the segment being proven
        let segment = B256::from(leaves[segment_index]);

        // Include the prefix in the proof if there is one
        let prefix = if self.prefix().is_empty() {
            None
        } else {
            Some(self.prefix().to_vec())
        };
        let prefix_ref = prefix.as_deref();

        // Build every tree level below the root, batching each level's sibling
        // pairs across SIMD lanes. Zero padding is hashed literally, so under a
        // prefix every zero subtree comes out as keccak(prefix || ...).
        let mut levels: Vec<Vec<[u8; 32]>> = Vec::with_capacity(PROOF_LENGTH);
        let mut current = leaves.to_vec();
        while current.len() > 1 {
            let mut next = vec![[0u8; 32]; current.len() / 2];
            hash_pairs(prefix_ref, current.as_flattened(), &mut next);
            levels.push(current);
            current = next;
        }

        // The proof is the sibling of the proven node at every level. One
        // sibling per level, so the level count fixes the array length.
        let mut index = segment_index;
        let proof_segments = core::array::from_fn(|level| {
            let sibling = B256::from(levels[level][index ^ 1]);
            index /= 2;
            sibling
        });

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
