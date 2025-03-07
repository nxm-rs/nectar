//! Tests for the Binary Merkle Tree implementation.

use super::*;
use alloy_primitives::{
    B256,
    hex::{self, ToHexExt},
};
use digest::{Digest, FixedOutputReset};
use proof::BmtProver;
use rand::Rng;

// Original tests from mod.rs updated for new API
#[test]
fn test_concurrent_simple() {
    let data: [u8; 3] = [1, 2, 3];

    let mut hasher = BMTHasher::new();
    hasher.set_span(data.len() as u64);

    // Update with data
    hasher.update(&data);
    // Use sum to get the hash
    let result = hasher.sum();

    // Check against the expected hash from the original test
    let expected = B256::from_slice(
        &hex::decode("ca6357a08e317d15ec560fef34e4c45f8f19f01c372aa70f1da72bfa7f1a4338").unwrap(),
    );
    assert_eq!(result, expected);
}

#[test]
fn test_concurrent_fullsize() {
    // Use a random seed for consistent results
    let data: Vec<u8> = (0..BMT_MAX_DATA_LENGTH)
        .map(|_| rand::random::<u8>())
        .collect();

    // Hash with the new hasher
    let mut hasher = BMTHasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(&data);
    let result1 = hasher.sum();

    // Hash again - should get same result
    let mut hasher = BMTHasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(&data);
    let result2 = hasher.sum();

    assert_eq!(result1, result2, "Same data should produce same hash");
}

#[test]
fn test_hasher_empty_data() {
    let mut hasher = BMTHasher::new();
    hasher.set_span(0);
    let result = hasher.sum();

    // Create a second hasher to verify deterministic result for empty data
    let mut hasher2 = BMTHasher::new();
    hasher2.set_span(0);
    let result2 = hasher2.sum();

    assert_eq!(result, result2, "Empty data should have consistent hash");
}

#[test]
fn test_sync_hasher_correctness() {
    let mut rng = rand::rng();
    let data: Vec<u8> = (0..BMT_MAX_DATA_LENGTH)
        .map(|_| rand::random::<u8>())
        .collect();

    // Test multiple sub-slices of the data
    let mut start = 0;
    while start < data.len() {
        let slice_len = std::cmp::min(1 + rng.random_range(0..=5), data.len() - start);

        let mut hasher = BMTHasher::new();
        hasher.set_span(slice_len as u64);
        hasher.update(&data[..slice_len]);
        let result = hasher.sum();

        // Verify the hash is consistent
        let mut hasher2 = BMTHasher::new();
        hasher2.set_span(slice_len as u64);
        hasher2.update(&data[..slice_len]);
        let result2 = hasher2.sum();

        assert_eq!(result, result2, "Same slice should produce same hash");

        start += slice_len;
    }
}

#[test]
fn test_bmt_hasher_with_prefix() {
    let mut hasher1 = BMTHasher::new();
    hasher1.set_span(11);
    hasher1.prefix_with(b"prefix-");

    let data = b"hello world";
    hasher1.update(data);
    let result_with_prefix = hasher1.sum();

    // Create a new hasher without prefix
    let mut hasher2 = BMTHasher::new();
    hasher2.set_span(11);
    hasher2.update(data);
    let result_without_prefix = hasher2.sum();

    // Results should be different
    assert_ne!(result_with_prefix, result_without_prefix);
}

#[test]
fn test_bmt_hasher_large_data() {
    let mut hasher = BMTHasher::new();
    hasher.set_span(BMT_MAX_DATA_LENGTH as u64);

    // Create data exactly the size of BMT_MAX_DATA_LENGTH
    let data = vec![0x42; BMT_MAX_DATA_LENGTH];
    hasher.update(&data);
    let result = hasher.sum();

    assert_eq!(result.as_slice().len(), HASH_SIZE);
}

#[test]
fn test_proof_generation_and_verification() {
    let data = b"hello world, this is a test for proof generation and verification";
    let mut hasher = BMTHasher::new();

    // Set the span and update the data
    hasher.set_span(data.len() as u64);
    hasher.update(data);
    let root_hash = hasher.sum();

    // Generate proof for segment 0
    let proof = hasher
        .generate_proof(data, 0)
        .expect("Failed to generate proof");

    // Verify the proof
    let is_valid =
        BMTHasher::verify_proof(&proof, root_hash.as_slice()).expect("Failed to verify proof");

    assert!(is_valid, "Proof verification should succeed");
}

#[test]
fn test_proof_correctness() {
    let mut buf = vec![0u8; BMT_MAX_DATA_LENGTH];
    let data = b"hello world";
    buf[..data.len()].copy_from_slice(data);

    let mut hasher = BMTHasher::new();
    let span = buf.len() as u64;
    hasher.set_span(span);
    hasher.update(&buf);
    let root_hash = hasher.sum();

    // Generate proof for segment 0
    let proof = hasher
        .generate_proof(&buf, 0)
        .expect("Failed to generate proof");

    // Verify the proof segments contain expected data
    assert_eq!(
        proof.proof_segments.len(),
        BMT_PROOF_LENGTH,
        "Incorrect proof length"
    );

    // Expected segment values (these are known from the original tests)
    let expected_segments = [
        "0000000000000000000000000000000000000000000000000000000000000000",
        "ad3228b676f7d3cd4284a5443f17f1962b36e491b30a40b2405849e597ba5fb5",
        "b4c11951957c6f8f642c4af61cd6b24640fec6dc7fc607ee8206a99e92410d30",
        "21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85",
        "e58769b32a1beaf1ea27375a44095a0d1fb664ce2dd358e7fcbfb78c26a19344",
        "0eb01ebfc9ed27500cd4dfc979272d1f0913cc9f66540d7e8005811109e1cf2d",
        "887c22bd8750d34016ac3c66b5ff102dacdd73f6b014e710b51e8022af9a1968",
    ];

    let verify_segments = |expected: &[&str], proof_segments: &[B256]| {
        assert_eq!(
            expected.len(),
            proof_segments.len(),
            "Incorrect number of proof segments"
        );

        for (i, (exp, actual)) in expected.iter().zip(proof_segments.iter()).enumerate() {
            let decoded = B256::from_slice(&hex::decode(exp).expect("Invalid hex encoding"));
            assert_eq!(
                &decoded,
                actual,
                "Segment {} mismatch: expected {}, got {}",
                i,
                exp,
                actual.encode_hex()
            );
        }
    };

    // Verify segments against expected values
    verify_segments(&expected_segments, &proof.proof_segments);

    // Test proof verification
    let is_valid =
        BMTHasher::verify_proof(&proof, root_hash.as_slice()).expect("Failed to verify proof");

    assert!(is_valid, "Proof verification should succeed");

    // Test rightmost segment (127)
    let rightmost_proof = hasher
        .generate_proof(&buf, 127)
        .expect("Failed to generate proof for rightmost segment");

    let expected_rightmost_segments = [
        "0000000000000000000000000000000000000000000000000000000000000000",
        "ad3228b676f7d3cd4284a5443f17f1962b36e491b30a40b2405849e597ba5fb5",
        "b4c11951957c6f8f642c4af61cd6b24640fec6dc7fc607ee8206a99e92410d30",
        "21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85",
        "e58769b32a1beaf1ea27375a44095a0d1fb664ce2dd358e7fcbfb78c26a19344",
        "0eb01ebfc9ed27500cd4dfc979272d1f0913cc9f66540d7e8005811109e1cf2d",
        "745bae095b6ff5416b4a351a167f731db6d6f5924f30cd88d48e74261795d27b",
    ];

    verify_segments(
        &expected_rightmost_segments,
        &rightmost_proof.proof_segments,
    );

    let is_valid = BMTHasher::verify_proof(&rightmost_proof, root_hash.as_slice())
        .expect("Failed to verify rightmost proof");
    assert!(is_valid, "Rightmost proof verification should succeed");

    // Test middle segment (64)
    let middle_proof = hasher
        .generate_proof(&buf, 64)
        .expect("Failed to generate proof for middle segment");

    let expected_middle_segments = [
        "0000000000000000000000000000000000000000000000000000000000000000",
        "ad3228b676f7d3cd4284a5443f17f1962b36e491b30a40b2405849e597ba5fb5",
        "b4c11951957c6f8f642c4af61cd6b24640fec6dc7fc607ee8206a99e92410d30",
        "21ddb9a356815c3fac1026b6dec5df3124afbadb485c9ba5a3e3398a04b7ba85",
        "e58769b32a1beaf1ea27375a44095a0d1fb664ce2dd358e7fcbfb78c26a19344",
        "0eb01ebfc9ed27500cd4dfc979272d1f0913cc9f66540d7e8005811109e1cf2d",
        "745bae095b6ff5416b4a351a167f731db6d6f5924f30cd88d48e74261795d27b",
    ];

    verify_segments(&expected_middle_segments, &middle_proof.proof_segments);

    let is_valid = BMTHasher::verify_proof(&middle_proof, root_hash.as_slice())
        .expect("Failed to verify middle proof");
    assert!(is_valid, "Middle proof verification should succeed");
}

#[test]
fn test_digest_trait_methods() {
    // Test that the common Digest trait methods work
    let data = b"test data";

    // Using static method
    let hash1 = BMTHasher::digest(data);

    // Using instance methods
    let mut hasher = BMTHasher::new();
    hasher.update(data);
    let hash2 = hasher.finalize_fixed_reset();

    // Should be the same
    assert_eq!(hash1.as_slice(), hash2.as_slice());

    // The hasher should be reset with span=0
    assert_eq!(
        hasher.span(),
        0,
        "Span should be reset after finalize_fixed_reset()"
    );
}

#[test]
fn test_root_hash_calculation() {
    // This test is based on the proof.rs test_root_hash_calculation test
    let mut buf = vec![0u8; BMT_MAX_DATA_LENGTH];
    let data = b"hello world";
    buf[..data.len()].copy_from_slice(data);

    let mut hasher = BMTHasher::new();
    hasher.set_span(buf.len() as u64);
    hasher.update(&buf);
    let expected_root_hash = hasher.sum();

    // Create a proof for segment 64
    let proof = hasher
        .generate_proof(&buf, 64)
        .expect("Failed to generate proof");

    // Verify the proof against the root hash
    let is_valid = BMTHasher::verify_proof(&proof, expected_root_hash.as_slice())
        .expect("Failed to verify proof");
    assert!(is_valid, "Proof verification should succeed");
}

#[test]
fn test_proof() {
    // Initialize a buffer with random data
    let mut buf = vec![0u8; BMT_MAX_DATA_LENGTH];
    rand::rng().fill(&mut buf[..]);

    let mut hasher = BMTHasher::new();
    hasher.set_span(buf.len() as u64);
    hasher.update(&buf);
    let root_hash = hasher.sum();

    // Iterate over several segments and test proofs
    for i in [0, 1, 32, 64, 127] {
        let segment_index = i;

        let proof = hasher
            .generate_proof(&buf, segment_index)
            .expect("Failed to generate proof");

        // Verify the proof
        let is_valid =
            BMTHasher::verify_proof(&proof, root_hash.as_slice()).expect("Failed to verify proof");

        assert!(
            is_valid,
            "Proof verification failed for segment {}",
            segment_index
        );
    }
}
