//! The chunk segment kind: one segment of a chunk body bound to the chunk root.

use alloy_primitives::B256;

use nectar_primitives::bmt::Proof;

use crate::auth::Verifier;

/// The chunk segment proof kind.
///
/// The proof is the BMT segment proof from `nectar-primitives`. The replay
/// folds the segment hash over its sibling levels under the anchor prefix
/// until it lands on the chunk root.
#[derive(Debug)]
pub struct Segment;

impl Verifier for Segment {
    type Proof = Proof;

    fn verify(trusted: &B256, proof: &Self::Proof) -> bool {
        // The fold has no fallible path, so an error answers as non-binding.
        proof.verify(trusted).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nectar_primitives::{DEFAULT_BODY_SIZE, DefaultHasher, bmt::Prover};

    const ANCHOR: &[u8; 32] = b"proof-crate-anchor-deterministic";

    fn full_body() -> Vec<u8> {
        (0..DEFAULT_BODY_SIZE).map(|i| (i % 251) as u8).collect()
    }

    fn prove(hasher: &DefaultHasher, body: &[u8], segment: usize) -> Proof {
        hasher.generate_proof(body, segment).expect("body in range")
    }

    #[test]
    fn segment_verifies_at_the_plain_root() {
        let body = full_body();
        let mut hasher = DefaultHasher::new();
        hasher.set_span(body.len() as u64);
        hasher.update(&body);
        let root = hasher.sum();

        for index in [0usize, 1, 127, DEFAULT_BODY_SIZE / 32 - 1] {
            let proof = prove(&hasher, &body, index);
            assert!(
                Segment::verify(&root, &proof),
                "segment {index} must bind at the plain root"
            );
            // The fold is pure, so the proof copies and re-runs unchanged.
            let copied = proof;
            assert!(Segment::verify(&root, &copied));
        }
    }

    #[test]
    fn segment_verifies_at_the_anchored_root_only() {
        let body = full_body();
        let mut anchored = DefaultHasher::with_prefix(ANCHOR);
        anchored.set_span(body.len() as u64);
        anchored.update(&body);
        let anchored_root = anchored.sum();

        let mut plain = DefaultHasher::new();
        plain.set_span(body.len() as u64);
        plain.update(&body);
        let plain_root = plain.sum();

        let proof = prove(&anchored, &body, 7);
        assert_eq!(proof.prefix, B256::from_slice(ANCHOR));
        assert!(Segment::verify(&anchored_root, &proof));
        assert!(!Segment::verify(&plain_root, &proof));
    }

    #[test]
    fn a_step_that_does_not_bind_reports_false() {
        let body = full_body();
        let mut hasher = DefaultHasher::new();
        hasher.set_span(body.len() as u64);
        hasher.update(&body);
        let root = hasher.sum();

        let proof = prove(&hasher, &body, 3);
        assert!(Segment::verify(&root, &proof));

        // A tampered segment hash breaks the first fold.
        let mut tampered = proof;
        tampered.segment = proof.segment ^ B256::from([1u8; 32]);
        assert!(!Segment::verify(&root, &tampered));

        // The binding proof does not land at a different trusted address.
        let mut other_body = full_body();
        other_body[0] ^= 0xFF;
        let mut other = DefaultHasher::new();
        other.set_span(other_body.len() as u64);
        other.update(&other_body);
        assert!(!Segment::verify(&other.sum(), &proof));
    }
}
