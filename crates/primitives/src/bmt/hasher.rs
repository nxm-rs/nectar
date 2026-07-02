//! Binary Merkle Tree hasher implementation
//!
//! This module provides an implementation of a BMT hasher that uses Keccak256
//! for computing content-addressed hashes of arbitrary data.

use alloy_primitives::{B256, Keccak256};
use bytes::Bytes;
use digest::{FixedOutput, FixedOutputReset, OutputSizeUser, Reset, Update};
use hybrid_array::{Array, sizes::U32};
use std::io::{self, Write};
use std::sync::LazyLock;

use super::constants::*;

/// Number of zero tree levels for the default body size.
const ZERO_TREE_LEVELS: usize = zero_tree_levels(DEFAULT_BODY_SIZE);

/// Pre-computed zero hashes for the default body size tree.
static ZERO_HASHES: LazyLock<[B256; ZERO_TREE_LEVELS]> = LazyLock::new(|| {
    let mut hashes = [B256::ZERO; ZERO_TREE_LEVELS];

    // Level 0: hash of 64 zero bytes (one segment pair)
    let mut hasher = Keccak256::new();
    hasher.update([0u8; SEGMENT_PAIR_LENGTH]);
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

/// Hash consecutive 64-byte sibling pairs of a tree level, batched across SIMD
/// lanes.
///
/// `pairs` is the flat byte view of the level (`out.len()` pairs of two
/// 32-byte nodes); each output is `keccak(prefix || left || right)`.
pub(super) fn hash_pairs(prefix: Option<&[u8]>, pairs: &[u8], out: &mut [[u8; 32]]) {
    debug_assert_eq!(pairs.len(), out.len() * SEGMENT_PAIR_LENGTH);

    let Some(p) = prefix else {
        let inputs: Vec<&[u8]> = pairs.chunks_exact(SEGMENT_PAIR_LENGTH).collect();
        return keccak_batch::keccak256_many_into(&inputs, out);
    };

    let entry = p.len() + SEGMENT_PAIR_LENGTH;
    let mut scratch = vec![0u8; entry * out.len()];
    for (slot, pair) in scratch
        .chunks_exact_mut(entry)
        .zip(pairs.chunks_exact(SEGMENT_PAIR_LENGTH))
    {
        slot[..p.len()].copy_from_slice(p);
        slot[p.len()..].copy_from_slice(pair);
    }
    let inputs: Vec<&[u8]> = scratch.chunks_exact(entry).collect();
    keccak_batch::keccak256_many_into(&inputs, out);
}

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
    pub const fn new() -> Self {
        Self {
            span: 0,
            prefix: None,
            buffer: [0u8; BODY_SIZE],
            cursor: 0,
        }
    }

    /// Set the span of data to be hashed
    #[inline]
    pub const fn set_span(&mut self, span: u64) {
        self.span = span;
    }

    /// Get the current span
    #[inline(always)]
    pub const fn span(&self) -> u64 {
        self.span
    }

    /// Add a prefix to the hash calculation.
    ///
    /// The prefix is applied to *every* Keccak256 invocation in the tree (leaf
    /// sections, internal nodes and the final span wrap), matching bee's
    /// `swarm.NewPrefixHasher` semantics where `Reset()` re-writes the prefix as
    /// the first bytes before each node hash. This makes the resulting root
    /// byte-identical to bee's `transformedAddress`.
    #[inline]
    pub fn prefix_with(&mut self, prefix: &[u8]) {
        self.prefix = Some(prefix.to_vec());
    }

    /// Create a new BMT hasher pre-configured with an anchor `prefix`.
    ///
    /// Equivalent to [`Hasher::new`] followed by [`Hasher::prefix_with`]. The
    /// prefix is mixed into every node hash (see [`Hasher::prefix_with`]), so
    /// the produced root matches bee's anchor-keyed `transformedAddress`.
    #[inline]
    pub fn with_prefix(prefix: &[u8]) -> Self {
        let mut hasher = Self::new();
        hasher.prefix_with(prefix);
        hasher
    }

    /// Construct a fresh Keccak256, seeded with the prefix when one is set.
    ///
    /// Every node in the tree is hashed as `keccak(prefix || data)`; this helper
    /// centralises that so the prefix can never be forgotten at an individual
    /// hash site.
    #[inline(always)]
    fn node_hasher(prefix: Option<&[u8]>) -> Keccak256 {
        let mut hasher = Keccak256::new();
        if let Some(p) = prefix {
            hasher.update(p);
        }
        hasher
    }

    /// Get the current prefix
    #[inline(always)]
    pub fn prefix(&self) -> &[u8] {
        self.prefix.as_deref().unwrap_or(&[])
    }

    /// Get the current cursor position
    #[inline(always)]
    pub const fn position(&self) -> usize {
        self.cursor
    }

    /// Get the amount of data currently in the buffer
    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.cursor
    }

    /// Check if the buffer is empty
    #[inline(always)]
    pub const fn is_empty(&self) -> bool {
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

    /// Compute the BMT hash and write to output buffer.
    #[allow(clippy::should_implement_trait)] // BMT hash, not std::hash::Hash
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
        let prefix = self.prefix.as_deref();

        // Zero fast paths rely on the precomputed prefix-independent ZERO_HASHES
        // table, which is only valid for plain (unprefixed) hashing. Under a
        // non-empty prefix every zero section hashes as keccak(prefix||zeros),
        // so we must compute the zero subtrees with the prefix instead.
        let zero_hashes = self.zero_hashes(prefix);

        // Special case: no data means entire tree is zeros
        if self.cursor == 0 {
            return zero_hashes[ZERO_TREE_LEVELS - 1];
        }

        // Fast path: if all data is zeros, return the zero tree root.
        // Valid for both plain and prefixed hashing because `zero_hashes`
        // already accounts for the prefix.
        if Self::is_all_zeros(&self.buffer[..self.cursor]) {
            return zero_hashes[ZERO_TREE_LEVELS - 1];
        }

        // Find the smallest power-of-2 subtree that contains all data
        let effective_size = self
            .cursor
            .next_power_of_two()
            .max(SEGMENT_PAIR_LENGTH)
            .min(BODY_SIZE);

        // Hash only the effective subtree (which contains all actual data).
        let mut result = self.hash_subtree(&self.buffer[..effective_size], &zero_hashes);

        // Roll up with zero hashes until we reach the full tree size
        let mut current_size = effective_size;
        while current_size < BODY_SIZE {
            // The current result is a left child, combine with zero hash for right sibling
            let sibling_level = Self::zero_tree_level(current_size);
            let mut hasher = Self::node_hasher(prefix);
            hasher.update(result.as_slice());
            hasher.update(zero_hashes[sibling_level].as_slice());
            result = B256::from_slice(hasher.finalize().as_slice());
            current_size *= 2;
        }

        result
    }

    /// Return the per-level zero subtree hashes for the current prefix.
    ///
    /// With no prefix this returns the shared precomputed [`ZERO_HASHES`]. With
    /// a prefix set it computes the table on demand so that each level is
    /// `keccak(prefix || left || right)` (the level-0 entry being
    /// `keccak(prefix || 64 zero bytes)`), matching bee's per-prefix
    /// `zerohashes`.
    #[inline(always)]
    fn zero_hashes(&self, prefix: Option<&[u8]>) -> [B256; ZERO_TREE_LEVELS] {
        let Some(p) = prefix else {
            return *ZERO_HASHES;
        };

        let mut hashes = [B256::ZERO; ZERO_TREE_LEVELS];

        let mut hasher = Self::node_hasher(Some(p));
        hasher.update([0u8; SEGMENT_PAIR_LENGTH]);
        hashes[0] = B256::from_slice(hasher.finalize().as_slice());

        for i in 1..ZERO_TREE_LEVELS {
            let mut hasher = Self::node_hasher(Some(p));
            hasher.update(hashes[i - 1].as_slice());
            hasher.update(hashes[i - 1].as_slice());
            hashes[i] = B256::from_slice(hasher.finalize().as_slice());
        }

        hashes
    }

    /// Hash a power-of-two subtree (>= 64 bytes) level by level, batching each
    /// level's sibling pairs across SIMD lanes.
    ///
    /// Only pairs that overlap live data (the cursor) cost a Keccak; everything
    /// past them is an all-zero subtree taken from `zero_hashes`, which the
    /// caller has already made prefix-aware.
    fn hash_subtree(&self, data: &[u8], zero_hashes: &[B256; ZERO_TREE_LEVELS]) -> B256 {
        debug_assert!(data.len().is_power_of_two());
        debug_assert!(data.len() >= SEGMENT_PAIR_LENGTH);

        let prefix = self.prefix.as_deref();

        if data.len() == SEGMENT_PAIR_LENGTH {
            let mut hasher = Self::node_hasher(prefix);
            hasher.update(data);
            return B256::from_slice(hasher.finalize().as_slice());
        }

        // Level 0: pairs that overlap live data (the caller guarantees
        // cursor > 0); the rest of the level is zero pairs.
        let pairs = data.len() / SEGMENT_PAIR_LENGTH;
        let mut live = self.cursor.div_ceil(SEGMENT_PAIR_LENGTH).min(pairs);
        let mut level = vec![[0u8; 32]; pairs];
        hash_pairs(
            prefix,
            &data[..live * SEGMENT_PAIR_LENGTH],
            &mut level[..live],
        );
        for slot in &mut level[live..] {
            slot.copy_from_slice(zero_hashes[0].as_slice());
        }

        // Combine sibling digests level by level until one root remains.
        let mut next = vec![[0u8; 32]; pairs / 2];
        let mut count = pairs;
        let mut depth = 1;
        while count > 1 {
            count /= 2;
            live = live.div_ceil(2);
            hash_pairs(prefix, level[..live * 2].as_flattened(), &mut next[..live]);
            for slot in &mut next[live..count] {
                slot.copy_from_slice(zero_hashes[depth].as_slice());
            }
            std::mem::swap(&mut level, &mut next);
            depth += 1;
        }

        B256::from(level[0])
    }

    /// Calculate the zero-tree level for a given subtree length.
    /// Length must be a power of 2 between 64 and 4096.
    #[inline(always)]
    const fn zero_tree_level(length: usize) -> usize {
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
    const fn reset_internal(&mut self) {
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
        let mut hasher = Self::node_hasher(self.prefix.as_deref());

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
            hasher.update([0u8; SEGMENT_SIZE]);
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
    fn finalize_into(self, out: &mut Array<u8, Self::OutputSize>) {
        let b256 = self.sum();
        out.copy_from_slice(b256.as_slice());
    }
}

impl<const BODY_SIZE: usize> FixedOutputReset for Hasher<BODY_SIZE> {
    #[inline]
    fn finalize_into_reset(&mut self, out: &mut Array<u8, Self::OutputSize>) {
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
    pub const fn new() -> Self {
        Self
    }

    /// Create a new BMT hasher.
    #[inline]
    pub const fn create_hasher(&self) -> Hasher<BODY_SIZE> {
        Hasher::new()
    }
}
