//! Chunk-tree structural-regime body generators.
//!
//! Lengths straddle the tree's structural boundaries, so the multi-chunk
//! regime past one body is always reachable; bodies are tiled from a short
//! drawn seed, so a large body costs a few draws. Generators stay
//! deterministic in `u`, so shrinking and replay work.

use alloc::vec::Vec;

use arbitrary::{Arbitrary, Unstructured};
use nectar_primitives::DEFAULT_BODY_SIZE;

/// Upper bound on a generated body: one byte past four full leaves, so a
/// depth-two tree with a partial tail is in range.
pub const MAX_BODY_LEN: usize = DEFAULT_BODY_SIZE.saturating_mul(4).saturating_add(1);

/// A body length biased to the chunk boundaries: empty, one exact body, one
/// byte past it, a leaf-aligned pair, or anywhere up to [`MAX_BODY_LEN`].
pub fn body_len(u: &mut Unstructured<'_>) -> arbitrary::Result<usize> {
    Ok(match u.int_in_range(0..=4u8)? {
        0 => 0,
        1 => DEFAULT_BODY_SIZE,
        2 => DEFAULT_BODY_SIZE.saturating_add(1),
        3 => DEFAULT_BODY_SIZE.saturating_mul(2),
        _ => u.int_in_range(0..=MAX_BODY_LEN)?,
    })
}

/// A length past one chunk body, so a split must build an intermediate level.
pub fn multi_chunk_len(u: &mut Unstructured<'_>) -> arbitrary::Result<usize> {
    u.int_in_range(DEFAULT_BODY_SIZE.saturating_add(1)..=MAX_BODY_LEN)
}

/// `len` bytes tiled from a drawn eight-byte seed.
pub fn fill(u: &mut Unstructured<'_>, len: usize) -> arbitrary::Result<Vec<u8>> {
    let seed = <[u8; 8]>::arbitrary(u)?;
    Ok(seed.iter().copied().cycle().take(len).collect())
}

/// A body of [`body_len`] bytes.
pub fn body(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<u8>> {
    let len = body_len(u)?;
    fill(u, len)
}

/// A multi-chunk body: always longer than one chunk body.
pub fn multi_chunk_body(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<u8>> {
    let len = multi_chunk_len(u)?;
    fill(u, len)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn body_width_matches_the_tree_geometry() {
        assert_eq!(
            DEFAULT_BODY_SIZE,
            usize::try_from(crate::geometry::DEFAULT_BODY_SIZE).unwrap()
        );
    }

    #[test]
    fn exhausted_input_stays_in_regime() {
        let mut u = Unstructured::new(&[]);
        assert!(multi_chunk_body(&mut u).unwrap().len() > DEFAULT_BODY_SIZE);
        assert!(body(&mut u).unwrap().len() <= MAX_BODY_LEN);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn bodies_stay_in_regime(seed in proptest::collection::vec(any::<u8>(), 0..256)) {
            let mut u = Unstructured::new(&seed);
            prop_assert!(body(&mut u).unwrap().len() <= MAX_BODY_LEN);
        }

        #[test]
        fn multi_chunk_bodies_cross_one_body(seed in proptest::collection::vec(any::<u8>(), 0..256)) {
            let mut u = Unstructured::new(&seed);
            let bytes = multi_chunk_body(&mut u).unwrap();
            prop_assert!(bytes.len() > DEFAULT_BODY_SIZE);
            prop_assert!(bytes.len() <= MAX_BODY_LEN);
        }

        #[test]
        fn bodies_are_deterministic(seed in proptest::collection::vec(any::<u8>(), 0..256)) {
            let first = body(&mut Unstructured::new(&seed)).unwrap();
            let second = body(&mut Unstructured::new(&seed)).unwrap();
            prop_assert_eq!(first, second);
        }
    }
}
