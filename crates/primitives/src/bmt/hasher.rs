//! Binary Merkle Tree hasher.
//!
//! Keccak256 over a fixed-geometry binary tree of 32-byte segments, with an
//! optional per-node prefix and a little-endian span wrap at the root.

use alloc::{boxed::Box, vec, vec::Vec};
use alloy_primitives::{B256, Keccak256};
use bytes::Bytes;
use digest::{FixedOutput, FixedOutputReset, OutputSizeUser, Reset, Update};
use hybrid_array::{Array, sizes::U32};
use once_cell::race::OnceBox;

use super::constants::*;
use super::derived::DerivedAddress;

/// Number of zero tree levels for the default body size.
const ZERO_TREE_LEVELS: usize = zero_tree_levels(DEFAULT_BODY_SIZE);

/// Per-level zero-subtree hashes for plain (unprefixed) hashing, computed once
/// on first use.
static ZERO_HASHES: OnceBox<[B256; ZERO_TREE_LEVELS]> = OnceBox::new();

/// The shared plain zero-hash table.
fn zero_hash_table() -> &'static [B256; ZERO_TREE_LEVELS] {
    ZERO_HASHES.get_or_init(|| Box::new(zero_hash_levels(None)))
}

/// Compute the per-level zero-subtree hashes: level 0 is the hash of one
/// all-zero segment pair, level n the hash of two level n-1 digests.
fn zero_hash_levels(prefix: Option<&[u8]>) -> [B256; ZERO_TREE_LEVELS] {
    let mut hasher = node_hasher(prefix);
    hasher.update([0u8; SEGMENT_PAIR_LENGTH]);
    let mut current = B256::from_slice(hasher.finalize().as_slice());

    let mut hashes = [B256::ZERO; ZERO_TREE_LEVELS];
    let [levels @ .., top] = &mut hashes;
    for slot in levels {
        *slot = current;
        let mut hasher = node_hasher(prefix);
        hasher.update(current.as_slice());
        hasher.update(current.as_slice());
        current = B256::from_slice(hasher.finalize().as_slice());
    }
    *top = current;
    hashes
}

/// Construct a fresh Keccak256, seeded with the prefix when one is set.
///
/// Every node in the tree is hashed as `keccak(prefix || data)`; this helper
/// centralises that so the prefix can never be forgotten at a hash site.
#[inline(always)]
pub(super) fn node_hasher(prefix: Option<&[u8]>) -> Keccak256 {
    let mut hasher = Keccak256::new();
    if let Some(p) = prefix {
        hasher.update(p);
    }
    hasher
}

/// Hash 64-byte sibling pairs, batched across SIMD lanes.
///
/// Consumes exactly `out.len()` pairs from `pairs` (the caller guarantees the
/// iterator yields that many); each output is `keccak(prefix || left || right)`.
pub(super) fn hash_pairs<'a>(
    prefix: Option<&[u8]>,
    pairs: impl Iterator<Item = &'a [u8]>,
    out: &mut [[u8; 32]],
) {
    let Some(p) = prefix else {
        let inputs: Vec<&[u8]> = pairs.collect();
        return keccak_batch::keccak256_many_into(&inputs, out);
    };

    let entry = p.len().saturating_add(SEGMENT_PAIR_LENGTH);
    let mut scratch = vec![0u8; entry.saturating_mul(out.len())];
    for (slot, pair) in scratch.chunks_exact_mut(entry).zip(pairs) {
        for (dst, src) in slot.iter_mut().zip(p.iter().chain(pair)) {
            *dst = *src;
        }
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
    /// The prefix is mixed into *every* Keccak256 invocation in the tree (leaf
    /// sections, internal nodes and the final span wrap), so the root is the
    /// anchor-keyed transformed address rather than the plain chunk address.
    #[inline]
    pub fn prefix_with(&mut self, prefix: &[u8]) {
        self.prefix = Some(prefix.to_vec());
    }

    /// Create a new BMT hasher pre-configured with an anchor `prefix`.
    ///
    /// Equivalent to [`Hasher::new`] followed by [`Hasher::prefix_with`].
    #[inline]
    pub fn with_prefix(prefix: &[u8]) -> Self {
        let mut hasher = Self::new();
        hasher.prefix_with(prefix);
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

    /// Copy as much of `data` as fits after the cursor; returns the copied
    /// count (zero once the buffer is full).
    #[inline]
    fn fill(&mut self, data: &[u8]) -> usize {
        let n = data.len().min(BODY_SIZE.saturating_sub(self.cursor));
        match (
            self.buffer
                .get_mut(self.cursor..)
                .and_then(|b| b.get_mut(..n)),
            data.get(..n),
        ) {
            (Some(dst), Some(src)) => {
                dst.copy_from_slice(src);
                self.cursor = self.cursor.saturating_add(n);
                n
            }
            _ => 0,
        }
    }

    /// Update the hasher with more data (non-destructive)
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        let _ = self.fill(data);
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

    /// Compute the BMT root as a [`DerivedAddress`]: the value of
    /// [`sum`](Self::sum) carried with hasher provenance. A configured
    /// prefix participates, making the result the transformed root.
    #[inline]
    #[must_use]
    pub fn sum_derived(&self) -> DerivedAddress {
        DerivedAddress::new(self.sum())
    }

    /// Check if a byte slice is all zeros.
    /// Uses a bitwise-OR fold, which LLVM vectorizes on supported platforms.
    #[inline(always)]
    fn is_all_zeros(data: &[u8]) -> bool {
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

        // Zero fast paths rely on the precomputed prefix-independent zero-hash
        // table, which is only valid for plain (unprefixed) hashing. Under a
        // non-empty prefix every zero section hashes as keccak(prefix||zeros),
        // so the zero subtrees are computed with the prefix instead.
        let zero_hashes = self.zero_hashes(prefix);
        let [.., zero_root] = zero_hashes;

        // Fast path: no data, or all data zero, means the whole tree is the
        // zero tree. `zero_hashes` already accounts for the prefix.
        let live = self.buffer.get(..self.cursor).unwrap_or_default();
        if Self::is_all_zeros(live) {
            return zero_root;
        }

        // Find the smallest power-of-2 subtree that contains all data
        let effective_size = self
            .cursor
            .next_power_of_two()
            .max(SEGMENT_PAIR_LENGTH)
            .min(BODY_SIZE);

        // Hash only the effective subtree (which contains all actual data).
        let mut result = self.hash_subtree(effective_size, &zero_hashes);

        // Roll up with zero hashes until we reach the full tree size: at each
        // level the result is a left child whose right sibling is that level's
        // zero-subtree hash.
        let mut current_size = effective_size;
        for sibling in zero_hashes
            .iter()
            .skip(Self::zero_tree_level(effective_size))
        {
            if current_size >= BODY_SIZE {
                break;
            }
            let mut hasher = node_hasher(prefix);
            hasher.update(result.as_slice());
            hasher.update(sibling.as_slice());
            result = B256::from_slice(hasher.finalize().as_slice());
            current_size = current_size.saturating_mul(2);
        }

        result
    }

    /// Return the per-level zero subtree hashes for the current prefix.
    ///
    /// With no prefix this returns the shared precomputed table; with a prefix
    /// set the table is computed on demand so each level is
    /// `keccak(prefix || left || right)`.
    #[inline(always)]
    fn zero_hashes(&self, prefix: Option<&[u8]>) -> [B256; ZERO_TREE_LEVELS] {
        prefix.map_or_else(|| *zero_hash_table(), |p| zero_hash_levels(Some(p)))
    }

    /// Hash a power-of-two subtree of `size` bytes (>= 64, taken from the
    /// front of the buffer) level by level, batching each level's sibling
    /// pairs across SIMD lanes.
    ///
    /// Only pairs that overlap live data (the cursor) cost a Keccak; a live
    /// row with an odd node count is padded with that level's zero-subtree
    /// hash, so everything past the live nodes stays un-hashed.
    fn hash_subtree(&self, size: usize, zero_hashes: &[B256; ZERO_TREE_LEVELS]) -> B256 {
        debug_assert!(size.is_power_of_two());
        debug_assert!(size >= SEGMENT_PAIR_LENGTH);

        let prefix = self.prefix.as_deref();

        if size == SEGMENT_PAIR_LENGTH
            && let Some(pair) = self.buffer.first_chunk::<SEGMENT_PAIR_LENGTH>()
        {
            let mut hasher = node_hasher(prefix);
            hasher.update(pair);
            return B256::from_slice(hasher.finalize().as_slice());
        }

        // Level 0: hash only the pairs that overlap live data (the caller
        // guarantees cursor > 0, so at least one pair is live).
        let pairs = size.checked_div(SEGMENT_PAIR_LENGTH).unwrap_or_default();
        let live = self.cursor.div_ceil(SEGMENT_PAIR_LENGTH).min(pairs);
        let mut level = vec![[0u8; 32]; live];
        hash_pairs(
            prefix,
            self.buffer.chunks_exact(SEGMENT_PAIR_LENGTH).take(live),
            &mut level,
        );

        // Combine sibling digests level by level until one pair remains,
        // walking the zero-hash table in lockstep for odd-row padding. The
        // table always covers the tree depth, so the iterator never runs dry.
        let mut depth_zeros = zero_hashes.iter();
        let mut count = pairs;
        while count > 2 {
            let zero = depth_zeros.next().copied().unwrap_or_default();
            if !level.len().is_multiple_of(2) {
                level.push(zero.0);
            }
            let mut next = vec![[0u8; 32]; level.len() / 2];
            hash_pairs(
                prefix,
                level.as_flattened().chunks_exact(SEGMENT_PAIR_LENGTH),
                &mut next,
            );
            level = next;
            count /= 2;
        }

        // Final combine: exactly one pair remains at full width two.
        let zero = depth_zeros.next().copied().unwrap_or_default();
        if level.len() < 2 {
            level.push(zero.0);
        }
        let mut root = [[0u8; 32]; 1];
        hash_pairs(
            prefix,
            level.as_flattened().chunks_exact(SEGMENT_PAIR_LENGTH),
            &mut root,
        );
        let [root] = root;
        B256::from(root)
    }

    /// Calculate the zero-tree level for a given subtree length.
    /// Length must be a power of 2 between 64 and 4096.
    #[allow(clippy::arithmetic_side_effects, clippy::as_conversions)]
    // length >= 64 (per contract above), so trailing_zeros() >= 6 and the subtraction cannot underflow; the u32 -> usize widening is infallible (usize::from is not const-callable)
    #[inline(always)]
    const fn zero_tree_level(length: usize) -> usize {
        // length = 64 * 2^level, so level = log2(length) - log2(64) = log2(length) - 6
        length.trailing_zeros() as usize - 6
    }

    /// Finalize with span and optional prefix
    #[inline(always)]
    fn finalize_with_prefix(&self, intermediate_hash: B256) -> B256 {
        let mut hasher = node_hasher(self.prefix.as_deref());

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
        let live = self.buffer.get(..self.cursor).unwrap_or_default();
        if live.is_empty() {
            return Bytes::new();
        }
        Bytes::copy_from_slice(live)
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
        // Zero-pad the segment: bytes past the end of `data` stay zero.
        let mut segment = [0u8; SEGMENT_SIZE];
        let start = i.saturating_mul(SEGMENT_SIZE);
        for (dst, src) in segment.iter_mut().zip(data.iter().skip(start)) {
            *dst = *src;
        }

        let mut hasher = node_hasher(self.prefix.as_deref());
        hasher.update(segment);
        B256::from_slice(hasher.finalize().as_slice())
    }
}

#[cfg(feature = "std")]
impl<const BODY_SIZE: usize> std::io::Write for Hasher<BODY_SIZE> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(self.fill(buf))
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
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
