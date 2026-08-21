//! Node smoke tests for the proving-lane no_std shape: the BMT root and an
//! inclusion proof round-trip execute on wasm32 with the std feature off.
//! Native targets compile this file to nothing.
#![cfg(target_arch = "wasm32")]

use alloy_primitives::{B256, hex};
use nectar_primitives_core::bmt::{DEFAULT_BODY_SIZE, Hasher, Prover};
use wasm_bindgen_test::wasm_bindgen_test;

type DefaultHasher = Hasher<DEFAULT_BODY_SIZE>;

#[wasm_bindgen_test]
fn bmt_root_runs_the_core_shape() {
    let data: [u8; 3] = [1, 2, 3];

    let mut hasher = DefaultHasher::new();
    hasher.set_span(u64::try_from(data.len()).unwrap());
    hasher.update(&data);
    let root = hasher.sum();

    // Carried from this repository's original BMT tests; the reference
    // client publishes no vector for this input.
    let expected = B256::from_slice(
        &hex::decode("ca6357a08e317d15ec560fef34e4c45f8f19f01c372aa70f1da72bfa7f1a4338").unwrap(),
    );
    assert_eq!(root, expected);
}

#[wasm_bindgen_test]
fn inclusion_proof_round_trips_the_core_shape() {
    let data: Vec<u8> = (0..DEFAULT_BODY_SIZE)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();

    let mut hasher = DefaultHasher::new();
    hasher.set_span(u64::try_from(DEFAULT_BODY_SIZE).unwrap());
    hasher.update(&data);
    let root = hasher.sum();

    for seg in [0usize, 1, 63, 127] {
        let proof = hasher.generate_proof(&data, seg).unwrap();
        assert!(
            <DefaultHasher as Prover>::verify_proof(&proof, &root).unwrap(),
            "proof for segment {seg} must verify against the root"
        );
    }
}
