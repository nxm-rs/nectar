//! Reference implementation of a Binary Merkle Tree hasher.

use alloy_primitives::{B256, Keccak256, keccak256};
use digest::{FixedOutput, FixedOutputReset, OutputSizeUser, Reset, Update};
use generic_array::{GenericArray, typenum::U32};
use std::marker::PhantomData;

use super::constants::*;
use crate::chunk::ChunkAddress;
use crate::error::Result;

/// Reference implementation of a BMT hasher that uses Keccak256
///
/// This implementation uses a fixed number of BMT branches (128) as defined by `BMT_BRANCHES`.
/// The Binary Merkle Tree is structured to efficiently hash data in parallel.
#[derive(Debug, Clone, Default)]
pub struct BMTHasher {
    span: u64,
    prefix: Vec<u8>,
    pending_data: Vec<u8>,
    _marker: PhantomData<Keccak256>,
}

impl BMTHasher {
    /// Create a new BMT hasher with `BMT_BRANCHES` (128) branches
    ///
    /// The hasher is optimized for data sized in multiples of SEGMENT_SIZE,
    /// with a maximum of BMT_BRANCHES * SEGMENT_SIZE bytes.
    pub fn new() -> Self {
        Self {
            span: 0,
            prefix: Vec::new(),
            pending_data: Vec::with_capacity(BMT_MAX_DATA_LENGTH),
            _marker: PhantomData,
        }
    }

    /// Set the span of data to be hashed
    pub fn set_span(&mut self, span: u64) {
        self.span = span;
    }

    /// Get the current span
    pub fn span(&self) -> u64 {
        self.span
    }

    /// Add a prefix to the hash calculation
    pub fn prefix_with(&mut self, prefix: &[u8]) {
        self.prefix = prefix.to_vec();
    }

    /// Get the current prefix
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Update the hasher with more data (non-destructive)
    pub fn update_data(&mut self, data: &[u8]) {
        self.pending_data.extend_from_slice(data);
    }

    /// Compute the BMT hash and return the chunk address (non-destructive)
    pub fn chunk_address(&self, data: &[u8]) -> Result<ChunkAddress> {
        // Create a clone to avoid modifying the original state
        let mut cloned = self.clone();
        cloned.update_data(data);

        // Get hash as B256
        let hash = cloned.sum();

        // Create address from hash
        ChunkAddress::from_slice(hash.as_slice()).map_err(|e| e.into())
    }

    /// Hash data using a binary merkle tree (internal implementation)
    #[inline(always)]
    fn hash_internal(&self, data: &[u8]) -> B256 {
        // Create a buffer for hashing
        let mut buffer = vec![0u8; BMT_MAX_DATA_LENGTH];

        // Copy data into buffer
        let len = data.len().min(BMT_MAX_DATA_LENGTH);
        buffer[..len].copy_from_slice(&data[..len]);

        // Process in parallel
        self.hash_helper_parallel(&buffer, BMT_MAX_DATA_LENGTH)
    }

    /// Recursively hash segments in parallel using rayon
    #[inline(always)]
    fn hash_helper_parallel(&self, data: &[u8], length: usize) -> B256 {
        if length == SEGMENT_PAIR_LENGTH {
            return B256::from_slice(keccak256(data).as_slice());
        }

        let half = length / 2;

        // Split data and hash both halves in parallel
        let (left, right) = data.split_at(half);
        let (left_hash, right_hash) = rayon::join(
            || self.hash_helper_parallel(left, half),
            || self.hash_helper_parallel(right, half),
        );

        // Combine the hashes
        let mut pair = Vec::with_capacity(2 * SEGMENT_SIZE);
        pair.extend_from_slice(left_hash.as_slice());
        pair.extend_from_slice(right_hash.as_slice());

        B256::from_slice(keccak256(&pair).as_slice())
    }

    /// Finalize with span and optional prefix
    #[inline(always)]
    fn finalize_with_prefix(&self, intermediate_hash: B256) -> B256 {
        let mut hasher = Keccak256::new();

        // Add prefix if present
        if !self.prefix.is_empty() {
            hasher.update(&self.prefix);
        }

        // Add span as little-endian bytes
        hasher.update(self.span.to_le_bytes());

        // Add the intermediate hash
        hasher.update(intermediate_hash.as_slice());

        // Convert to B256
        B256::from_slice(hasher.finalize().as_slice())
    }

    /// Compute the current hash value as B256 (non-destructive)
    /// This is similar to the Go sum() pattern
    pub fn sum(&self) -> B256 {
        let hash = self.hash_internal(&self.pending_data);
        self.finalize_with_prefix(hash)
    }

    /// Finalize the hash computation and reset the hasher (destructive)
    /// Resets all data except the prefix; span is set to zero.
    /// Returns the hash as B256
    pub fn finalize(&mut self) -> B256 {
        let result = self.sum();
        self.reset_internal();
        result
    }

    /// Reset the hasher's internal state
    fn reset_internal(&mut self) {
        self.pending_data.clear();
        self.span = 0;
        // Don't reset prefix, as it's considered a configuration parameter
    }

    /// Get segments for the current level of data
    pub fn get_level_segments(&self, data: &[u8]) -> Vec<B256> {
        let mut segments = Vec::with_capacity(BMT_BRANCHES);
        let data_len = data.len();

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

        segments
    }
}

// Implement the Digest trait methods to match the standard patterns
impl OutputSizeUser for BMTHasher {
    type OutputSize = U32; // 32-byte output size
}

impl Update for BMTHasher {
    fn update(&mut self, data: &[u8]) {
        self.update_data(data);
    }
}

impl Reset for BMTHasher {
    fn reset(&mut self) {
        // Reset only clears the data and span, not prefix
        self.reset_internal();
    }
}

impl FixedOutput for BMTHasher {
    fn finalize_into(self, out: &mut GenericArray<u8, Self::OutputSize>) {
        let b256 = self.sum();
        out.copy_from_slice(b256.as_slice());
    }
}

impl FixedOutputReset for BMTHasher {
    fn finalize_into_reset(&mut self, out: &mut GenericArray<u8, Self::OutputSize>) {
        let b256 = self.finalize();
        out.copy_from_slice(b256.as_slice());
    }
}

// Make BMTHasher a valid hash function
impl digest::HashMarker for BMTHasher {}

/// A factory that creates BMTHasher instances
#[derive(Debug, Default, Clone)]
pub struct BMTHasherFactory;

impl BMTHasherFactory {
    /// Create a new factory for BMTHasher instances
    pub fn new() -> Self {
        Self
    }

    /// Create a new BMT hasher
    pub fn create_hasher(&self) -> BMTHasher {
        BMTHasher::new()
    }
}
