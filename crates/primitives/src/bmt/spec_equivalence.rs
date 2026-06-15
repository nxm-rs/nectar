//! Differential equivalence tests: the optimized [`Hasher`] vs an independent,
//! deliberately naive BMT reference derived directly from the BMT specification
//! (see `verification/bmt/Bmt.fst`).
//!
//! The production [`Hasher`] uses a zero-tree rollup, an all-zeros fast path,
//! and rayon parallelism. None of those tricks appear in the reference below:
//! it builds the full 128-leaf binary tree by brute force, exactly as the
//! specification describes. If the two ever disagree, an optimisation has
//! changed an *observable* hash — i.e. a Swarm chunk address. Pinning the fast
//! path to the simple, auditable one is what lets the implementation be
//! optimised further (SIMD, unsafe, new parallelism) without silently forking
//! addresses away from the network.
//!
//! This is the executable shadow of the F* model. The F* proof establishes that
//! the *reference* satisfies BMT proof soundness/completeness; these tests
//! establish that the *production code* equals the reference.

use super::constants::{BRANCHES, SEGMENT_SIZE};
use super::{Hasher, Prover};
use alloy_primitives::{B256, Keccak256};
use proptest::prelude::*;

/// Full chunk body: 128 segments * 32 bytes = 4096.
const BODY: usize = BRANCHES * SEGMENT_SIZE;

/// `keccak256(a || b)` for two 32-byte nodes — one internal BMT node.
fn node(a: &B256, b: &B256) -> B256 {
    let mut h = Keccak256::new();
    h.update(a.as_slice());
    h.update(b.as_slice());
    B256::from_slice(h.finalize().as_slice())
}

/// The 128 leaves: raw 32-byte segments of the zero-padded, 4096-capped body.
/// Leaves are the *raw* segment bytes (not pre-hashed); the first keccak in the
/// tree combines two raw segments, matching the production base case.
fn leaves(data: &[u8]) -> Vec<B256> {
    let data = &data[..data.len().min(BODY)];
    (0..BRANCHES)
        .map(|i| {
            let mut seg = [0u8; SEGMENT_SIZE];
            let start = i * SEGMENT_SIZE;
            if start < data.len() {
                let end = (start + SEGMENT_SIZE).min(data.len());
                seg[..end - start].copy_from_slice(&data[start..end]);
            }
            B256::from_slice(&seg)
        })
        .collect()
}

/// Brute-force BMT intermediate root: pair-hash the 128 leaves up 7 levels.
/// No zero-tree shortcut, no all-zeros fast path — the whole tree, every time.
fn naive_intermediate(data: &[u8]) -> B256 {
    let mut level = leaves(data);
    while level.len() > 1 {
        level = level.chunks(2).map(|p| node(&p[0], &p[1])).collect();
    }
    level[0]
}

/// Full BMT root: `keccak(span_le_u64 || intermediate)`. No prefix here.
fn naive_root(data: &[u8], span: u64) -> B256 {
    let inter = naive_intermediate(data);
    let mut h = Keccak256::new();
    h.update(span.to_le_bytes());
    h.update(inter.as_slice());
    B256::from_slice(h.finalize().as_slice())
}

/// The production hasher's root for the same input.
fn hasher_root(data: &[u8], span: u64) -> B256 {
    let mut hasher = Hasher::<BODY>::new();
    hasher.set_span(span);
    hasher.update(data);
    hasher.sum()
}

/// Sizes that exercise the zero-tree rollup, segment padding, and the
/// power-of-two subtree boundary — exactly where the optimisations live.
const BOUNDARY_SIZES: &[usize] = &[
    0,
    1,
    31,
    32,
    33,
    63,
    64,
    65,
    127,
    128,
    129,
    2048,
    BODY - 1,
    BODY,
    BODY + 1,
    BODY + 1000,
];

#[test]
fn boundary_sizes_match_reference() {
    for &n in BOUNDARY_SIZES {
        // Deterministic, non-zero patterned data (the all-zeros fast path is
        // covered separately so a pattern here stresses the general path).
        let data: Vec<u8> = (0..n)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
            .collect();
        for &span in &[0u64, n as u64, u64::MAX] {
            assert_eq!(
                hasher_root(&data, span),
                naive_root(&data, span),
                "Hasher disagrees with reference at size={n} span={span}"
            );
        }
    }
}

#[test]
fn all_zeros_match_reference() {
    // The production code has a dedicated all-zeros short circuit; make sure it
    // agrees with brute force at several sizes.
    for &n in &[0usize, 1, 64, 4096] {
        let data = vec![0u8; n];
        assert_eq!(hasher_root(&data, n as u64), naive_root(&data, n as u64));
    }
}

#[test]
fn proofs_verify_and_tampering_fails() {
    let data: Vec<u8> = (0..BODY).map(|i| (i.wrapping_mul(7) + 3) as u8).collect();
    let span = data.len() as u64;
    let root = hasher_root(&data, span);

    let mut hasher = Hasher::<BODY>::new();
    hasher.set_span(span);
    hasher.update(&data);

    for i in 0..BRANCHES {
        let proof = hasher.generate_proof(&data, i).unwrap();
        assert!(
            Hasher::<BODY>::verify_proof(&proof, root.as_slice()).unwrap(),
            "honest proof for segment {i} must verify"
        );

        // Soundness, empirically: flip the proven segment -> verification fails.
        let mut tampered = proof.clone();
        let mut seg = tampered.segment.0;
        seg[0] ^= 0xff;
        tampered.segment = B256::from(seg);
        assert!(
            !Hasher::<BODY>::verify_proof(&tampered, root.as_slice()).unwrap(),
            "tampered proof for segment {i} must NOT verify"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The headline property: for arbitrary data and span, the optimised hasher
    /// equals the brute-force reference.
    #[test]
    fn random_data_matches_reference(
        data in proptest::collection::vec(any::<u8>(), 0..=BODY + 256),
        span in any::<u64>(),
    ) {
        prop_assert_eq!(hasher_root(&data, span), naive_root(&data, span));
    }

    /// Generated proofs verify against the genuine root for any segment index.
    #[test]
    fn generated_proofs_verify(
        data in proptest::collection::vec(any::<u8>(), 0..=BODY),
        idx in 0usize..BRANCHES,
    ) {
        let span = data.len() as u64;
        let root = hasher_root(&data, span);
        let mut hasher = Hasher::<BODY>::new();
        hasher.set_span(span);
        hasher.update(&data);
        let proof = hasher.generate_proof(&data, idx).unwrap();
        prop_assert!(Hasher::<BODY>::verify_proof(&proof, root.as_slice()).unwrap());
    }
}
