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
    hasher.update([0u8; SEGMENT_PAIR_LENGTH]);
    hashes[0] = hasher.finalize();

    // Each subsequent level: hash of two copies of previous level's hash
    for i in 1..ZERO_TREE_LEVELS {
        let mut hasher = Keccak256::new();
        hasher.update(hashes[i - 1].as_slice());
        hasher.update(hashes[i - 1].as_slice());
        hashes[i] = hasher.finalize();
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
    /// Reads in `usize`-wide words (the dominant cost at 4096 bytes) and ORs them
    /// together, leaving only a short scalar tail. LLVM auto-vectorises the word
    /// loop into SIMD on supported platforms.
    #[inline(always)]
    fn is_all_zeros(data: &[u8]) -> bool {
        const W: usize = core::mem::size_of::<usize>();
        let (head, words, tail) = unsafe {
            // SAFETY: `align_to` only reinterprets bytes; it returns the maximal
            // middle slice that is correctly aligned for `usize` and leaves the
            // unaligned prefix/suffix as `u8` slices. No out-of-bounds access and
            // no mutation occur, and `u8`/`usize` have no invalid bit patterns.
            data.align_to::<usize>()
        };
        let mut acc_b = 0u8;
        for &b in head {
            acc_b |= b;
        }
        for &b in tail {
            acc_b |= b;
        }
        if acc_b != 0 {
            return false;
        }
        let mut acc_w = 0usize;
        for &w in words {
            acc_w |= w;
        }
        let _ = W;
        acc_w == 0
    }

    /// `keccak256(left_32 || right_32)` into a `B256`.
    ///
    /// `Keccak256::finalize` squeezes directly into an uninitialised `B256`, so
    /// this avoids the `finalize().as_slice() -> B256::from_slice` copy the old
    /// node code paid at every one of the 127 internal nodes.
    #[inline(always)]
    fn hash_pair(left: &B256, right: &B256) -> B256 {
        let mut hasher = Keccak256::new();
        hasher.update(left.as_slice());
        hasher.update(right.as_slice());
        hasher.finalize()
    }

    /// `keccak256(data_64)` of one contiguous 64-byte segment pair, into a `B256`.
    #[inline(always)]
    fn hash_segment_pair(data: &[u8]) -> B256 {
        debug_assert_eq!(data.len(), SEGMENT_PAIR_LENGTH);
        let mut hasher = Keccak256::new();
        hasher.update(data);
        hasher.finalize()
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

        // Hash only the effective subtree (which contains all actual data).
        // The full-size case can still farm out to rayon; everything smaller goes
        // through the allocation-free iterative bottom-up sweep.
        #[cfg(not(target_arch = "wasm32"))]
        let mut result = if effective_size == BODY_SIZE {
            Self::hash_subtree_recursive_parallel_inner(
                &self.buffer[..effective_size],
                effective_size,
                self.cursor,
            )
        } else {
            self.hash_subtree_iterative(effective_size)
        };

        #[cfg(target_arch = "wasm32")]
        let mut result = self.hash_subtree_iterative(effective_size);

        // Roll up with zero hashes until we reach the full tree size.
        // `result` is always a left child; the right sibling is the zero subtree
        // of the matching height, so this is a straight 7-deep (max) keccak chain.
        let mut current_size = effective_size;
        while current_size < BODY_SIZE {
            let sibling_level = Self::zero_tree_level(current_size);
            result = Self::hash_pair(&result, &ZERO_HASHES[sibling_level]);
            current_size *= 2;
        }

        result
    }

    /// Hash a power-of-two subtree of `length` bytes (>= 64, <= BODY_SIZE) with a
    /// fully iterative bottom-up sweep — no recursion, no heap allocation.
    ///
    /// The `length`-byte prefix of `buffer` is exactly `length / 64` contiguous
    /// 64-byte segment pairs (the BMT leaves). We keccak each pair straight from
    /// the buffer into a fixed `[B256; BODY_SIZE / 64]` level array, then collapse
    /// the level in place, halving its width each round until one node remains.
    ///
    /// Leaf pairs that lie entirely beyond `cursor` are all-zero, so we splice in
    /// the precomputed `ZERO_HASHES[0]` instead of hashing 64 zero bytes; the
    /// same shortcut applies to whole zero subtrees as the level collapses.
    #[inline]
    fn hash_subtree_iterative(&self, length: usize) -> B256 {
        Self::sweep_subtree(&self.buffer[..length], length, self.cursor.min(length))
    }

    /// Iterative bottom-up sweep of a power-of-two subtree over a borrowed slice.
    ///
    /// `data[..length]` is exactly `length / 64` contiguous 64-byte segment pairs
    /// (the leaves); the first `cursor` bytes hold real data and the rest are
    /// zero. We keccak each live leaf pair straight from the buffer into a fixed
    /// `[B256; BODY_SIZE/64]` level array, splice the precomputed zero leaf hash
    /// for any all-zero tail, then collapse the level in place — halving its
    /// width each round and substituting the zero subtree hash of the matching
    /// height for the dead tail — until one node remains.
    #[inline(always)]
    fn sweep_subtree(data: &[u8], length: usize, cursor: usize) -> B256 {
        debug_assert!(length.is_power_of_two());
        debug_assert!((SEGMENT_PAIR_LENGTH..=BODY_SIZE).contains(&length));
        debug_assert!(cursor <= length && data.len() >= length);

        // Max leaf count is BODY_SIZE/64 (= 64 for the default 4096 body).
        const MAX_LEAVES: usize = DEFAULT_BODY_SIZE / SEGMENT_PAIR_LENGTH;
        let mut level: [B256; MAX_LEAVES] = [B256::ZERO; MAX_LEAVES];

        let pairs = length / SEGMENT_PAIR_LENGTH;
        // Leaf pairs starting at or beyond the cursor are entirely zero.
        // `cursor` indexes raw bytes; a pair p covers [p*64, p*64+64).
        let live_pairs = cursor.div_ceil(SEGMENT_PAIR_LENGTH).min(pairs);

        for (i, slot) in level.iter_mut().take(live_pairs).enumerate() {
            let base = i * SEGMENT_PAIR_LENGTH;
            *slot = Self::hash_segment_pair(&data[base..base + SEGMENT_PAIR_LENGTH]);
        }
        // Remaining leaf pairs are zero subtrees of height 0.
        for slot in level[live_pairs..pairs].iter_mut() {
            *slot = ZERO_HASHES[0];
        }

        // Collapse the level in place. After processing `width` nodes we have
        // `width / 2` parents; `live` tracks how many of them carry real data so
        // the all-zero tail can reuse the precomputed zero hash for its height.
        let mut width = pairs;
        let mut live = live_pairs;
        let mut zero_level = 1usize; // ZERO_HASHES index for a fully-zero parent
        while width > 1 {
            let parents = width / 2;
            let live_parents = live.div_ceil(2);
            for j in 0..live_parents {
                level[j] = Self::hash_pair(&level[2 * j], &level[2 * j + 1]);
            }
            let zero = ZERO_HASHES[zero_level];
            for slot in level[live_parents..parents].iter_mut() {
                *slot = zero;
            }
            width = parents;
            live = live_parents;
            zero_level += 1;
        }

        level[0]
    }

    /// Recursively hash a subtree using rayon for parallelism.
    /// Only called for full BODY_SIZE chunks where parallelism pays off.
    /// Takes cursor as parameter to avoid self indirection in recursive calls.
    ///
    /// Once a subtree shrinks below a threshold it hands off to the iterative
    /// leaf sweep — recursion plus `rayon::join` is pure overhead for the small
    /// fixed trees near the leaves.
    #[cfg(not(target_arch = "wasm32"))]
    fn hash_subtree_recursive_parallel_inner(data: &[u8], length: usize, cursor: usize) -> B256 {
        debug_assert!(length.is_power_of_two());
        debug_assert!(length >= SEGMENT_PAIR_LENGTH);

        // Recurse with rayon::join until a subtree reaches this width, then hand
        // off to the allocation-free iterative leaf sweep. Splitting down to
        // 512-byte subtrees gives the leaf-heavy work ~8-way parallelism (a full
        // 4096 body fans out into 8 independent sweeps), while keeping the sweep
        // big enough that rayon's per-join overhead stays amortised — a measured
        // sweet spot well below the 2-way split a BODY_SIZE/2 threshold gives.
        const PARALLEL_THRESHOLD: usize = DEFAULT_BODY_SIZE / 8;

        if length <= PARALLEL_THRESHOLD || length == SEGMENT_PAIR_LENGTH {
            return Self::sweep_subtree(data, length, cursor.min(length));
        }

        let half = length / 2;
        let (left, right) = data.split_at(half);

        let (left_hash, right_hash) = if half >= cursor {
            // Right side is all zeros - compute left only, use precomputed right.
            let left_hash = Self::hash_subtree_recursive_parallel_inner(left, half, cursor);
            (left_hash, ZERO_HASHES[Self::zero_tree_level(half)])
        } else {
            // Both sides have data, use parallel execution.
            rayon::join(
                || Self::hash_subtree_recursive_parallel_inner(left, half, half),
                || Self::hash_subtree_recursive_parallel_inner(right, half, cursor - half),
            )
        };

        Self::hash_pair(&left_hash, &right_hash)
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
        hasher.finalize()
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
            hasher.update([0u8; SEGMENT_SIZE]);
        }

        hasher.finalize()
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
