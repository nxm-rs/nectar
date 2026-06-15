//! Proof-related traits and structures for the Binary Merkle Tree
//!
//! This module provides functionality for generating and verifying inclusion proofs
//! for specific segments within a binary merkle tree.

use alloy_primitives::{B256, Keccak256};

use crate::bmt::{Hasher, constants::*, error::BmtError};
use crate::error::Result;

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

        // Apply each proof segment to compute the root
        for proof_segment in &self.proof_segments {
            let mut hasher = Keccak256::new();

            // Order matters - left then right
            if current_index.is_multiple_of(2) {
                hasher.update(current_hash.as_slice());
                hasher.update(proof_segment.as_slice());
            } else {
                hasher.update(proof_segment.as_slice());
                hasher.update(current_hash.as_slice());
            }

            // Get hash for next level
            current_hash = hasher.finalize();
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

        let computed_root = hasher.finalize();

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

        let data_len = data.len().min(BRANCHES * SEGMENT_SIZE);

        // Build the 128 raw leaf segments directly into a fixed-size stack array.
        // Leaves are the *raw* zero-padded 32-byte segments (not pre-hashed); the
        // first keccak combines two raw segments. No heap allocation, no rayon —
        // for a single 4096-byte tree the fan-out overhead dwarfs the work.
        let mut level = [B256::ZERO; BRANCHES];
        for (i, slot) in level.iter_mut().enumerate() {
            let start = i * SEGMENT_SIZE;
            if start < data_len {
                let end = (start + SEGMENT_SIZE).min(data_len);
                let mut seg = [0u8; SEGMENT_SIZE];
                seg[..end - start].copy_from_slice(&data[start..end]);
                *slot = B256::from(seg);
            }
            // else: leaf stays B256::ZERO (zero-padded segment)
        }

        // The segment being proven (a raw leaf).
        let segment = level[segment_index];

        // Walk up the tree once, recording the sibling at each of the 7 levels
        // and collapsing pairs in place. `width` halves each round.
        let mut proof_segments = Vec::with_capacity(PROOF_LENGTH);
        let mut current_index = segment_index;
        let mut width = BRANCHES;

        while width > 1 {
            let sibling_index = current_index ^ 1;
            proof_segments.push(level[sibling_index]);

            let parents = width / 2;
            for j in 0..parents {
                let mut hasher = Keccak256::new();
                hasher.update(level[2 * j].as_slice());
                hasher.update(level[2 * j + 1].as_slice());
                level[j] = hasher.finalize();
            }

            current_index >>= 1;
            width = parents;
        }

        debug_assert_eq!(proof_segments.len(), PROOF_LENGTH);

        // Include the prefix in the proof if there is one
        let prefix = if !self.prefix().is_empty() {
            Some(self.prefix().to_vec())
        } else {
            None
        };

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
