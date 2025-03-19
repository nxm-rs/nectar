//! Proof related traits and structures for the Binary Merkle Tree.

use alloy_primitives::{B256, Keccak256};

use crate::bmt::{BMTHasher, constants::*, error::DigestError, error::Result};

/// Represents a proof for a specific segment in a Binary Merkle Tree
#[derive(Clone, Debug)]
pub struct BMTProof {
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

impl BMTProof {
    /// Create a new BMT proof
    pub fn new(
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
        if self.proof_segments.len() != BMT_PROOF_LENGTH {
            return Err(DigestError::invalid_proof("Invalid proof length"));
        }

        // Start with the segment being proven
        let mut current_hash = self.segment;
        let mut current_index = self.segment_index;

        // Apply each proof segment to compute the root
        for proof_segment in &self.proof_segments {
            let mut hasher = Keccak256::new();

            // Order matters - left then right
            if current_index % 2 == 0 {
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
pub trait BmtProver {
    /// Generate a proof for a specific segment
    fn generate_proof(&self, data: &[u8], segment_index: usize) -> Result<BMTProof>;

    /// Verify a proof against a root hash
    fn verify_proof(proof: &BMTProof, root_hash: &[u8]) -> Result<bool>;
}

impl BmtProver for BMTHasher {
    fn generate_proof(&self, data: &[u8], segment_index: usize) -> Result<BMTProof> {
        if segment_index >= BMT_BRANCHES {
            return Err(DigestError::invalid_proof(format!(
                "Segment index {segment_index} out of bounds for BMT_BRANCHES"
            )));
        }

        // Create segments from data, padding with zeros if needed
        let mut segments = Vec::with_capacity(BMT_BRANCHES);
        let data_len = data.len();

        // Use platform-specific optimizations for segment generation
        #[cfg(not(target_arch = "wasm32"))]
        {
            use rayon::prelude::*;
            segments = (0..BMT_BRANCHES)
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
                .collect::<Vec<_>>();
        }

        #[cfg(target_arch = "wasm32")]
        {
            for i in 0..BMT_BRANCHES {
                let start = i * SEGMENT_SIZE;
                let mut segment = [0u8; SEGMENT_SIZE];

                if start < data_len {
                    let end = (start + SEGMENT_SIZE).min(data_len);
                    let copy_len = end - start;
                    segment[..copy_len].copy_from_slice(&data[start..end]);
                }

                segments.push(B256::from_slice(&segment));
            }
        }

        // Get the segment being proven
        let segment = segments[segment_index];

        // Generate proof segments
        let mut proof_segments = Vec::with_capacity(BMT_PROOF_LENGTH);

        // Build the Merkle tree level by level
        let mut current_level = segments;
        let mut current_index = segment_index;

        // Continue until we reach the root (or until we have BMT_PROOF_LENGTH segments)
        while proof_segments.len() < BMT_PROOF_LENGTH {
            // Get sibling's index
            let sibling_index = if current_index % 2 == 0 {
                current_index + 1
            } else {
                current_index - 1
            };

            // Add sibling to proof
            if sibling_index < current_level.len() {
                proof_segments.push(current_level[sibling_index]);
            } else {
                proof_segments.push(B256::ZERO);
            }

            // Compute the next level up in the tree
            let mut next_level = Vec::with_capacity((current_level.len() + 1) / 2);

            for i in (0..current_level.len()).step_by(2) {
                let left = &current_level[i];
                let right = if i + 1 < current_level.len() {
                    &current_level[i + 1]
                } else {
                    &B256::ZERO
                };

                // Hash the pair to create the parent node
                let mut hasher = Keccak256::new();
                hasher.update(left.as_slice());
                hasher.update(right.as_slice());

                let parent = B256::from_slice(hasher.finalize().as_slice());
                next_level.push(parent);
            }

            // Move up to the next level
            current_level = next_level;
            current_index /= 2;

            // If we've reached the root or have only one node, break
            if current_level.len() <= 1 {
                break;
            }
        }

        // Ensure we have exactly BMT_PROOF_LENGTH segments in our proof
        while proof_segments.len() < BMT_PROOF_LENGTH {
            proof_segments.push(B256::ZERO);
        }

        // Include the prefix in the proof if there is one
        let prefix = if !self.prefix().is_empty() {
            Some(self.prefix().to_vec())
        } else {
            None
        };

        Ok(BMTProof::new(
            segment_index,
            segment,
            proof_segments,
            self.span(),
            prefix,
        ))
    }

    fn verify_proof(proof: &BMTProof, root_hash: &[u8]) -> Result<bool> {
        proof.verify(root_hash)
    }
}

/// Depth of the Binary Merkle Tree for proofs
pub const BMT_PROOF_LENGTH: usize = BMT_DEPTH - 1;
