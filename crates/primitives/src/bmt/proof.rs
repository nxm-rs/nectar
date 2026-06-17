//! Proof-related traits and structures for the Binary Merkle Tree
//!
//! This module provides functionality for generating and verifying inclusion proofs
//! for specific segments within a binary merkle tree.

use alloy_primitives::{B256, Keccak256};

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

/// Compute the prefix-aware zero subtree hash for `level`.
///
/// `level` 0 is `keccak(prefix || 64 zero bytes)`; each higher level doubles the
/// previous. With no prefix this equals the plain zero tree used by the hasher.
fn prefixed_zero_hash(prefix: Option<&[u8]>, level: usize) -> B256 {
    let mut hash = {
        let mut hasher = new_node_hasher(prefix);
        hasher.update([0u8; 2 * SEGMENT_SIZE]);
        B256::from_slice(hasher.finalize().as_slice())
    };
    for _ in 0..level {
        let mut hasher = new_node_hasher(prefix);
        hasher.update(hash.as_slice());
        hasher.update(hash.as_slice());
        hash = B256::from_slice(hasher.finalize().as_slice());
    }
    hash
}

/// Represents a proof for a specific segment in a Binary Merkle Tree
#[derive(Clone, Debug)]
pub struct Proof {
    /// The segment index this proof is for
    pub segment_index: usize,
    /// The segment data being proven
    pub segment: B256,
    /// The proof segments (sibling hashes in the path to the root)
    pub proof_segments: Vec<B256>,
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
        proof_segments: Vec<B256>,
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

    /// Verify this proof against a root hash
    pub fn verify(&self, root_hash: &[u8]) -> Result<bool> {
        if self.proof_segments.len() != PROOF_LENGTH {
            return Err(
                BmtError::invalid_proof_length(PROOF_LENGTH, self.proof_segments.len()).into(),
            );
        }

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
        Ok(computed_root.as_slice() == root_hash)
    }
}

/// Extension trait to add proof-related functionality to BMTHasher
pub trait Prover {
    /// Generate a proof for a specific segment
    fn generate_proof(&self, data: &[u8], segment_index: usize) -> Result<Proof>;

    /// Verify a proof against a root hash
    fn verify_proof(proof: &Proof, root_hash: &[u8]) -> Result<bool>;
}

impl Prover for Hasher {
    fn generate_proof(&self, data: &[u8], segment_index: usize) -> Result<Proof> {
        if segment_index >= BRANCHES {
            return Err(self::BmtError::invalid_input_size(format!(
                "Segment index {segment_index} out of bounds for BRANCHES"
            ))
            .into());
        }

        // Create segments from data, padding with zeros if needed
        let data_len = data.len();

        // Use platform-specific optimizations for segment generation
        #[cfg(not(target_arch = "wasm32"))]
        let segments = {
            use rayon::prelude::*;
            (0..BRANCHES)
                .into_par_iter()
                .map(|i| {
                    let start = i * SEGMENT_SIZE;
                    let mut segment = [0u8; SEGMENT_SIZE];

                    if start < data_len {
                        let end = (start + SEGMENT_SIZE).min(data_len);
                        let copy_len = end - start;
                        segment[..copy_len].copy_from_slice(&data[start..end]);
                    }

                    B256::from_slice(&segment)
                })
                .collect::<Vec<_>>()
        };

        #[cfg(target_arch = "wasm32")]
        let segments = {
            let mut segs = Vec::with_capacity(BRANCHES);
            for i in 0..BRANCHES {
                let start = i * SEGMENT_SIZE;
                let mut segment = [0u8; SEGMENT_SIZE];

                if start < data_len {
                    let end = (start + SEGMENT_SIZE).min(data_len);
                    let copy_len = end - start;
                    segment[..copy_len].copy_from_slice(&data[start..end]);
                }

                segs.push(B256::from_slice(&segment));
            }

            segs
        };

        // Get the segment being proven
        let segment = segments[segment_index];

        // Include the prefix in the proof if there is one
        let prefix = if self.prefix().is_empty() {
            None
        } else {
            Some(self.prefix().to_vec())
        };
        let prefix_ref = prefix.as_deref();

        // Generate proof segments
        let mut proof_segments = Vec::with_capacity(PROOF_LENGTH);

        // Build the Merkle tree level by level
        let mut current_level = segments;
        let mut current_index = segment_index;
        // Tree level of the nodes in `current_level`: 0 = raw leaf segments,
        // 1 = leaf-pair hashes, etc. Used to pick the right prefix-aware zero
        // hash when padding an odd/short level.
        let mut level: usize = 0;

        // Continue until we reach the root (or until we have BMT_PROOF_LENGTH segments)
        while proof_segments.len() < PROOF_LENGTH {
            // Get sibling's index
            let sibling_index = if current_index.is_multiple_of(2) {
                current_index + 1
            } else {
                current_index - 1
            };

            // Add sibling to proof. A missing sibling is an all-zero subtree at
            // this level, which under a prefix hashes differently from B256::ZERO.
            if sibling_index < current_level.len() {
                proof_segments.push(current_level[sibling_index]);
            } else if level == 0 {
                // Missing raw leaf segment is 32 zero bytes (not a hash).
                proof_segments.push(B256::ZERO);
            } else {
                proof_segments.push(prefixed_zero_hash(prefix_ref, level - 1));
            }

            // Compute the next level up in the tree
            let mut next_level = Vec::with_capacity(current_level.len().div_ceil(2));

            for i in (0..current_level.len()).step_by(2) {
                let left = &current_level[i];
                let right = if i + 1 < current_level.len() {
                    current_level[i + 1]
                } else if level == 0 {
                    B256::ZERO
                } else {
                    prefixed_zero_hash(prefix_ref, level - 1)
                };

                // Hash the pair to create the parent node (prefix-aware)
                let mut hasher = new_node_hasher(prefix_ref);
                hasher.update(left.as_slice());
                hasher.update(right.as_slice());

                let parent = B256::from_slice(hasher.finalize().as_slice());
                next_level.push(parent);
            }

            // Move up to the next level
            current_level = next_level;
            current_index /= 2;
            level += 1;

            // If we've reached the root or have only one node, break
            if current_level.len() <= 1 {
                break;
            }
        }

        // Ensure we have exactly BMT_PROOF_LENGTH segments in our proof
        while proof_segments.len() < PROOF_LENGTH {
            proof_segments.push(B256::ZERO);
        }

        Ok(Proof::new(
            segment_index,
            segment,
            proof_segments,
            self.span(),
            prefix,
        ))
    }

    fn verify_proof(proof: &Proof, root_hash: &[u8]) -> Result<bool> {
        proof.verify(root_hash)
    }
}
