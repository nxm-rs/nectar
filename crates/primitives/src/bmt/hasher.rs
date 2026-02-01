//! Binary Merkle Tree hasher implementation
//!
//! This module provides an implementation of a BMT hasher that uses Keccak256
//! for computing content-addressed hashes of arbitrary data.

use alloy_primitives::{B256, Keccak256};
use bytes::Bytes;
use digest::{FixedOutput, FixedOutputReset, OutputSizeUser, Reset, Update};
use generic_array::{GenericArray, typenum::U32};
use std::io::{self, Write};
use std::sync::LazyLock;

// Use rayon for parallel processing on non-WASM platforms
#[cfg(not(target_arch = "wasm32"))]
use rayon;

use super::constants::*;

/// Number of zero tree levels for the default body size.
const ZERO_TREE_LEVELS: usize = zero_tree_levels(DEFAULT_BODY_SIZE);

/// Pre-computed zero hashes for the default body size tree.
static ZERO_HASHES: LazyLock<[B256; ZERO_TREE_LEVELS]> = LazyLock::new(|| {
    let mut hashes = [B256::ZERO; ZERO_TREE_LEVELS];

    // Level 0: hash of 64 zero bytes (one segment pair)
    let mut hasher = Keccak256::new();
    hasher.update(&[0u8; SEGMENT_PAIR_LENGTH]);
    hashes[0] = B256::from_slice(hasher.finalize().as_slice());

    // Each subsequent level: hash of two copies of previous level's hash
    for i in 1..ZERO_TREE_LEVELS {
        let mut hasher = Keccak256::new();
        hasher.update(hashes[i - 1].as_slice());
        hasher.update(hashes[i - 1].as_slice());
        hashes[i] = B256::from_slice(hasher.finalize().as_slice());
    }

    hashes
});

/// BMT hasher with configurable body size.
#[derive(Debug, Clone)]
pub struct Hasher<const BODY_SIZE: usize = DEFAULT_BODY_SIZE> {
    span: u64,
    prefix: Option<Vec<u8>>,
    buffer: [u8; BODY_SIZE],
    cursor: usize,
}

impl<const BODY_SIZE: usize> Default for Hasher<BODY_SIZE> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<const BODY_SIZE: usize> Hasher<BODY_SIZE> {
    /// Create a new BMT hasher.
    #[inline]
    pub fn new() -> Self {
        Self {
            span: 0,
            prefix: None,
            buffer: [0u8; BODY_SIZE],
            cursor: 0,
        }
    }

    /// Set the span of data to be hashed
    #[inline]
    pub fn set_span(&mut self, span: u64) {
        self.span = span;
    }

    /// Get the current span
    #[inline(always)]
    pub fn span(&self) -> u64 {
        self.span
    }

    /// Add a prefix to the hash calculation
    #[inline]
    pub fn prefix_with(&mut self, prefix: &[u8]) {
        self.prefix = Some(prefix.to_vec());
    }

    /// Get the current prefix
    #[inline(always)]
    pub fn prefix(&self) -> &[u8] {
        self.prefix.as_deref().unwrap_or(&[])
    }

    /// Get the current cursor position
    #[inline(always)]
    pub fn position(&self) -> usize {
        self.cursor
    }

    /// Get the amount of data currently in the buffer
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.cursor
    }

    /// Check if the buffer is empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.cursor == 0
    }

    /// Update the hasher with more data (non-destructive)
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        // Calculate how much data we can actually copy
        let available_space = BODY_SIZE - self.cursor;
        let bytes_to_copy = data.len().min(available_space);

        if bytes_to_copy > 0 {
            // Copy data at cursor position
            self.buffer[self.cursor..self.cursor + bytes_to_copy]
                .copy_from_slice(&data[..bytes_to_copy]);

            // Update cursor position
            self.cursor += bytes_to_copy;
        }
    }

    /// Compute the BMT hash and return as SwarmAddress (non-destructive)
    #[inline]
    pub fn hash(&self, out: &mut [u8]) {
        let hash = self.sum();
        out.copy_from_slice(hash.as_slice());
    }

    /// Compute the BMT hash and return the result (non-destructive)
    #[inline]
    #[must_use]
    pub fn sum(&self) -> B256 {
        self.finalize_with_prefix(self.hash_internal())
    }

    /// Check if a byte slice is all zeros.
    /// Uses chunk-based iteration which LLVM optimizes to SIMD on supported platforms.
    #[inline(always)]
    fn is_all_zeros(data: &[u8]) -> bool {
        // Fold with bitwise OR - any non-zero byte makes the result non-zero
        // LLVM vectorizes this pattern into efficient SIMD code
        data.iter().fold(0u8, |acc, &b| acc | b) == 0
    }

    /// Hash data using a binary merkle tree (internal implementation)
    ///
    /// This uses an optimized algorithm that:
    /// 1. Finds the smallest power-of-2 subtree containing all data
    /// 2. Hashes only that subtree
    /// 3. Iteratively combines with pre-computed zero hashes to reach the root
    #[inline(always)]
    fn hash_internal(&self) -> B256 {
        // Special case: no data means entire tree is zeros
        if self.cursor == 0 {
            return ZERO_HASHES[ZERO_TREE_LEVELS - 1];
        }

        // Fast path: if all data is zeros, return pre-computed zero tree root
        // This avoids hashing entirely when the input is all zeros
        if Self::is_all_zeros(&self.buffer[..self.cursor]) {
            return ZERO_HASHES[ZERO_TREE_LEVELS - 1];
        }

        // Find the smallest power-of-2 subtree that contains all data
        let effective_size = self
            .cursor
            .next_power_of_two()
            .max(SEGMENT_PAIR_LENGTH)
            .min(BODY_SIZE);

        // Hash only the effective subtree (which contains all actual data)
        #[cfg(not(target_arch = "wasm32"))]
        let mut result = self.hash_subtree_parallel(&self.buffer[..effective_size], effective_size);

        #[cfg(target_arch = "wasm32")]
        let mut result =
            self.hash_subtree_sequential(&self.buffer[..effective_size], effective_size);

        // Roll up with zero hashes until we reach the full tree size
        let mut current_size = effective_size;
        while current_size < BODY_SIZE {
            // The current result is a left child, combine with zero hash for right sibling
            let sibling_level = Self::zero_tree_level(current_size);
            let mut hasher = Keccak256::new();
            hasher.update(result.as_slice());
            hasher.update(ZERO_HASHES[sibling_level].as_slice());
            result = B256::from_slice(hasher.finalize().as_slice());
            current_size *= 2;
        }

        result
    }

    /// Hash a subtree of exactly `length` bytes (must be power of 2, >= 64)
    ///
    /// For sizes < BODY_SIZE: uses sequential hashing (no rayon overhead).
    /// For BODY_SIZE (4096): uses recursive parallel hashing for maximum throughput.
    #[cfg(not(target_arch = "wasm32"))]
    #[inline(always)]
    fn hash_subtree_parallel(&self, data: &[u8], length: usize) -> B256 {
        debug_assert!(length.is_power_of_two());
        debug_assert!(length >= SEGMENT_PAIR_LENGTH);

        // For sizes < BODY_SIZE, use sequential (avoids rayon overhead for small/medium sizes)
        if length < BODY_SIZE {
            return self.hash_subtree_sequential(data, length);
        }

        // For BODY_SIZE (4096): use recursive parallel hashing
        // Pass cursor as parameter to avoid self indirection in hot loop
        Self::hash_subtree_recursive_parallel_inner(data, length, self.cursor)
    }

    /// Recursively hash a subtree using rayon for parallelism.
    /// Only called for full BODY_SIZE chunks where parallelism pays off.
    /// Takes cursor as parameter to avoid self indirection in recursive calls.
    #[cfg(not(target_arch = "wasm32"))]
    #[inline(always)]
    fn hash_subtree_recursive_parallel_inner(data: &[u8], length: usize, cursor: usize) -> B256 {
        debug_assert!(length.is_power_of_two());
        debug_assert!(length >= SEGMENT_PAIR_LENGTH);

        // Base case: 64 bytes (one segment pair)
        if length == SEGMENT_PAIR_LENGTH {
            let mut hasher = Keccak256::new();
            hasher.update(data);
            return B256::from_slice(hasher.finalize().as_slice());
        }

        let half = length / 2;
        let (left, right) = data.split_at(half);

        // Check if right half is entirely beyond cursor (all zeros in buffer)
        // cursor is relative to the start of this subtree
        let (left_hash, right_hash) = if half >= cursor {
            // Right side is all zeros - compute left only, use precomputed right
            let left_hash = Self::hash_subtree_recursive_parallel_inner(left, half, cursor);
            let right_hash = ZERO_HASHES[Self::zero_tree_level(half)];
            (left_hash, right_hash)
        } else {
            // Both sides have data, use parallel execution
            // Left cursor is capped at half (can't exceed subtree size)
            // Right cursor is adjusted by half (relative to right subtree start)
            rayon::join(
                || Self::hash_subtree_recursive_parallel_inner(left, half, half),
                || Self::hash_subtree_recursive_parallel_inner(right, half, cursor - half),
            )
        };

        let mut hasher = Keccak256::new();
        hasher.update(left_hash.as_slice());
        hasher.update(right_hash.as_slice());
        B256::from_slice(hasher.finalize().as_slice())
    }

    /// Hash a subtree of exactly `length` bytes (must be power of 2, >= 64) - sequential version
    #[inline(always)]
    fn hash_subtree_sequential(&self, data: &[u8], length: usize) -> B256 {
        debug_assert!(length.is_power_of_two());
        debug_assert!(length >= SEGMENT_PAIR_LENGTH);

        if length == SEGMENT_PAIR_LENGTH {
            let mut hasher = Keccak256::new();
            hasher.update(data);
            return B256::from_slice(hasher.finalize().as_slice());
        }

        let half = length / 2;
        let (left, right) = data.split_at(half);

        // Check if right half is entirely beyond cursor (all zeros in buffer)
        let (left_hash, right_hash) = if half >= self.cursor {
            // Right side is all zeros
            let left_hash = self.hash_subtree_sequential(left, half);
            let right_hash = ZERO_HASHES[Self::zero_tree_level(half)];
            (left_hash, right_hash)
        } else {
            let left_hash = self.hash_subtree_sequential(left, half);
            let right_hash = self.hash_subtree_sequential(right, half);
            (left_hash, right_hash)
        };

        let mut hasher = Keccak256::new();
        hasher.update(left_hash.as_slice());
        hasher.update(right_hash.as_slice());
        B256::from_slice(hasher.finalize().as_slice())
    }

    /// Calculate the zero-tree level for a given subtree length.
    /// Length must be a power of 2 between 64 and 4096.
    #[inline(always)]
    fn zero_tree_level(length: usize) -> usize {
        // length = 64 * 2^level, so level = log2(length) - log2(64) = log2(length) - 6
        length.trailing_zeros() as usize - 6
    }

    /// Finalize with span and optional prefix
    #[inline(always)]
    fn finalize_with_prefix(&self, intermediate_hash: B256) -> B256 {
        let mut hasher = Keccak256::new();

        // Add prefix if present
        if let Some(prefix) = &self.prefix {
            hasher.update(prefix);
        }

        // Add span as little-endian bytes
        hasher.update(self.span.to_le_bytes());

        // Add the intermediate hash
        hasher.update(intermediate_hash.as_slice());

        // Finalize to get the result
        B256::from_slice(hasher.finalize().as_slice())
    }

    /// Reset the hasher's internal state
    #[inline(always)]
    fn reset_internal(&mut self) {
        // Simply reset cursor - no need to clear the buffer as it will be overwritten
        self.cursor = 0;
        self.span = 0;
        // Don't reset prefix, as it's considered a configuration parameter
    }

    /// Get the current data as Bytes (immutable reference)
    #[inline]
    #[must_use]
    pub fn data(&self) -> Bytes {
        if self.cursor == 0 {
            return Bytes::new();
        }

        // Create Bytes from slice
        Bytes::copy_from_slice(&self.buffer[..self.cursor])
    }

    /// Get segments for the current level of data
    #[inline]
    pub fn get_level_segments(&self, data: &[u8]) -> Vec<B256> {
        let branches = branches_for_body_size(BODY_SIZE);

        #[cfg(not(target_arch = "wasm32"))]
        {
            use rayon::prelude::*;
            (0..branches)
                .into_par_iter()
                .map(|i| self.compute_segment_hash(data, i))
                .collect()
        }

        #[cfg(target_arch = "wasm32")]
        {
            (0..branches)
                .map(|i| self.compute_segment_hash(data, i))
                .collect()
        }
    }

    /// Compute the hash for a single segment at given index
    #[inline(always)]
    fn compute_segment_hash(&self, data: &[u8], i: usize) -> B256 {
        let start = i << SEGMENT_SIZE_LOG2; // Equivalent to i * SEGMENT_SIZE
        let mut hasher = Keccak256::new();

        if start < data.len() {
            let end = (start + SEGMENT_SIZE).min(data.len());
            let segment_data = &data[start..end];

            // Update with segment data
            hasher.update(segment_data);

            // If segment is shorter than SEGMENT_SIZE, the remaining bytes are zeros
            if segment_data.len() < SEGMENT_SIZE {
                hasher.update(&[0u8; SEGMENT_SIZE][..(SEGMENT_SIZE - segment_data.len())]);
            }
        } else {
            // Empty segment (all zeros)
            hasher.update(&[0u8; SEGMENT_SIZE]);
        }

        B256::from_slice(hasher.finalize().as_slice())
    }
}

impl<const BODY_SIZE: usize> Write for Hasher<BODY_SIZE> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let available = BODY_SIZE - self.cursor;
        let to_write = buf.len().min(available);
        if to_write > 0 {
            self.buffer[self.cursor..self.cursor + to_write].copy_from_slice(&buf[..to_write]);
            self.cursor += to_write;
        }
        Ok(to_write)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<const BODY_SIZE: usize> OutputSizeUser for Hasher<BODY_SIZE> {
    type OutputSize = U32;
}

impl<const BODY_SIZE: usize> Update for Hasher<BODY_SIZE> {
    #[inline]
    fn update(&mut self, data: &[u8]) {
        self.update(data);
    }
}

impl<const BODY_SIZE: usize> Reset for Hasher<BODY_SIZE> {
    #[inline]
    fn reset(&mut self) {
        self.reset_internal();
    }
}

impl<const BODY_SIZE: usize> FixedOutput for Hasher<BODY_SIZE> {
    #[inline]
    fn finalize_into(self, out: &mut GenericArray<u8, Self::OutputSize>) {
        let b256 = self.sum();
        out.copy_from_slice(b256.as_slice());
    }
}

impl<const BODY_SIZE: usize> FixedOutputReset for Hasher<BODY_SIZE> {
    #[inline]
    fn finalize_into_reset(&mut self, out: &mut GenericArray<u8, Self::OutputSize>) {
        let b256 = self.sum();
        out.copy_from_slice(b256.as_slice());
        self.reset_internal();
    }
}

impl<const BODY_SIZE: usize> digest::HashMarker for Hasher<BODY_SIZE> {}

/// Factory for creating BMT hashers.
#[derive(Debug, Default, Clone)]
pub struct HasherFactory<const BODY_SIZE: usize = DEFAULT_BODY_SIZE>;

impl<const BODY_SIZE: usize> HasherFactory<BODY_SIZE> {
    /// Create a new factory.
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Create a new BMT hasher.
    #[inline]
    pub fn create_hasher(&self) -> Hasher<BODY_SIZE> {
        Hasher::new()
    }
}
