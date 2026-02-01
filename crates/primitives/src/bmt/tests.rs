//! Tests for the Binary Merkle Tree implementation.

use crate::bmt::constants::{DEFAULT_BODY_SIZE, PROOF_LENGTH};

use super::*;
use alloy_primitives::{
    B256, FixedBytes,
    hex::{self, ToHexExt},
};
use digest::{Digest, FixedOutputReset};
use proof::Prover;
use rand::Rng;

type DefaultHasher = Hasher<DEFAULT_BODY_SIZE>;

// Original tests from mod.rs updated for new API
#[test]
fn test_concurrent_simple() {
    let data: [u8; 3] = [1, 2, 3];

    let mut hasher = DefaultHasher::new();
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
    let data: Vec<u8> = (0..DEFAULT_BODY_SIZE)
        .map(|_| rand::random::<u8>())
        .collect();

    // Hash with the new hasher
    let mut hasher = DefaultHasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(&data);
    let result1 = hasher.sum();

    // Hash again - should get same result
    let mut hasher = DefaultHasher::new();
    hasher.set_span(data.len() as u64);
    hasher.update(&data);
    let result2 = hasher.sum();

    assert_eq!(result1, result2, "Same data should produce same hash");
}

#[test]
fn test_hasher_empty_data() {
    let mut hasher = DefaultHasher::new();
    hasher.set_span(0);
    let result = hasher.sum();

    // Create a second hasher to verify deterministic result for empty data
    let mut hasher2 = DefaultHasher::new();
    hasher2.set_span(0);
    let result2 = hasher2.sum();

    assert_eq!(result, result2, "Empty data should have consistent hash");
}

#[test]
fn test_sync_hasher_correctness() {
    let mut rng = rand::rng();
    let data: Vec<u8> = (0..DEFAULT_BODY_SIZE)
        .map(|_| rand::random::<u8>())
        .collect();

    // Test multiple sub-slices of the data
    let mut start = 0;
    while start < data.len() {
        let slice_len = std::cmp::min(1 + rng.random_range(0..=5), data.len() - start);

        let mut hasher = DefaultHasher::new();
        hasher.set_span(slice_len as u64);
        hasher.update(&data[..slice_len]);
        let result = hasher.sum();

        // Verify the hash is consistent
        let mut hasher2 = DefaultHasher::new();
        hasher2.set_span(slice_len as u64);
        hasher2.update(&data[..slice_len]);
        let result2 = hasher2.sum();

        assert_eq!(result, result2, "Same slice should produce same hash");

        start += slice_len;
    }
}

#[test]
fn test_bmt_hasher_with_prefix() {
    let mut hasher1 = DefaultHasher::new();
    hasher1.set_span(11);
    hasher1.prefix_with(b"prefix-");

    let data = b"hello world";
    hasher1.update(data);
    let result_with_prefix = hasher1.sum();

    // Create a new hasher without prefix
    let mut hasher2 = DefaultHasher::new();
    hasher2.set_span(11);
    hasher2.update(data);
    let result_without_prefix = hasher2.sum();

    // Results should be different
    assert_ne!(result_with_prefix, result_without_prefix);
}

#[test]
fn test_bmt_hasher_large_data() {
    let mut hasher = DefaultHasher::new();
    hasher.set_span(DEFAULT_BODY_SIZE as u64);

    // Create data exactly the size of BMT_DEFAULT_BODY_SIZE
    let data = vec![0x42; DEFAULT_BODY_SIZE];
    hasher.update(&data);
    let result = hasher.sum();

    assert_eq!(
        result.as_slice().len(),
        std::mem::size_of::<FixedBytes<32>>()
    );
}

#[test]
fn test_proof_generation_and_verification() {
    let data = b"hello world, this is a test for proof generation and verification";
    let mut hasher = DefaultHasher::new();

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
        DefaultHasher::verify_proof(&proof, root_hash.as_slice()).expect("Failed to verify proof");

    assert!(is_valid, "Proof verification should succeed");
}

#[test]
fn test_proof_correctness() {
    let mut buf = vec![0u8; DEFAULT_BODY_SIZE];
    let data = b"hello world";
    buf[..data.len()].copy_from_slice(data);

    let mut hasher = DefaultHasher::new();
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
        PROOF_LENGTH,
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
        DefaultHasher::verify_proof(&proof, root_hash.as_slice()).expect("Failed to verify proof");

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

    let is_valid = DefaultHasher::verify_proof(&rightmost_proof, root_hash.as_slice())
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

    let is_valid = DefaultHasher::verify_proof(&middle_proof, root_hash.as_slice())
        .expect("Failed to verify middle proof");
    assert!(is_valid, "Middle proof verification should succeed");
}

#[test]
fn test_digest_trait_methods() {
    // Test that the common Digest trait methods work
    let data = b"test data";

    // Using static method
    let hash1 = DefaultHasher::digest(data);

    // Using instance methods
    let mut hasher = DefaultHasher::new();
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
    let mut buf = vec![0u8; DEFAULT_BODY_SIZE];
    let data = b"hello world";
    buf[..data.len()].copy_from_slice(data);

    let mut hasher = DefaultHasher::new();
    hasher.set_span(buf.len() as u64);
    hasher.update(&buf);
    let expected_root_hash = hasher.sum();

    // Create a proof for segment 64
    let proof = hasher
        .generate_proof(&buf, 64)
        .expect("Failed to generate proof");

    // Verify the proof against the root hash
    let is_valid = DefaultHasher::verify_proof(&proof, expected_root_hash.as_slice())
        .expect("Failed to verify proof");
    assert!(is_valid, "Proof verification should succeed");
}

#[test]
fn test_proof() {
    // Initialize a buffer with random data
    let mut buf = vec![0u8; DEFAULT_BODY_SIZE];
    rand::rng().fill(&mut buf[..]);

    let mut hasher = DefaultHasher::new();
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
        let is_valid = DefaultHasher::verify_proof(&proof, root_hash.as_slice())
            .expect("Failed to verify proof");

        assert!(
            is_valid,
            "Proof verification failed for segment {}",
            segment_index
        );
    }
}

#[test]
fn test_excess_data_ignored() {
    // Create data that is exactly DEFAULT_BODY_SIZE
    let exact_data: Vec<u8> = (0..DEFAULT_BODY_SIZE).map(|i| (i % 256) as u8).collect();

    // Create data that exceeds the maximum by 100 bytes
    let mut excess_data = exact_data.clone();
    excess_data.extend(vec![0xFF; 100]); // Add 100 bytes of 0xFF

    let excess_len = excess_data.len() as u64;

    // Hash with exact data (using excess length as span)
    let mut hasher1 = DefaultHasher::new();
    hasher1.set_span(excess_len);
    hasher1.update(&exact_data);
    let result1 = hasher1.sum();

    // Hash with excess data
    let mut hasher2 = DefaultHasher::new();
    hasher2.set_span(excess_len);
    hasher2.update(&excess_data);
    let result2 = hasher2.sum();

    // Assert that only the first DEFAULT_BODY_SIZE bytes were considered
    assert_eq!(
        result1, result2,
        "Excess data should be ignored in hash calculation"
    );

    // Test incremental updates
    let mut hasher3 = DefaultHasher::new();
    hasher3.set_span(exact_data.len() as u64);

    // Add exact data first
    hasher3.update(&exact_data);
    let result_before_excess = hasher3.sum();

    // Add some excess data
    hasher3.update(&[0xFF; 100]);
    let result_after_excess = hasher3.sum();

    // Hash should remain unchanged
    assert_eq!(
        result_before_excess, result_after_excess,
        "Adding excess data should not change the hash"
    );

    // Test with Write trait
    let mut hasher4 = DefaultHasher::new();
    hasher4.set_span(exact_data.len() as u64);

    // Write exact data
    std::io::Write::write(&mut hasher4, &exact_data).unwrap();
    let write_result_before = hasher4.sum();

    // Try to write more data
    std::io::Write::write(&mut hasher4, &[0xFF; 100]).unwrap();
    let write_result_after = hasher4.sum();

    // Hash should remain unchanged when using Write trait
    assert_eq!(
        write_result_before, write_result_after,
        "Adding excess data via Write trait should not change the hash"
    );
}

#[test]
fn test_write_returns_actual_bytes_written() {
    use std::io::Write;

    let mut hasher = DefaultHasher::new();

    // Fill buffer completely
    let data = vec![0x42; DEFAULT_BODY_SIZE];
    let written = hasher.write(&data).unwrap();
    assert_eq!(
        written, DEFAULT_BODY_SIZE,
        "Should report all bytes written when buffer has space"
    );

    // Try to write more - should return 0 since buffer is full
    let more = hasher.write(&[0xFF; 100]).unwrap();
    assert_eq!(more, 0, "Should return 0 when buffer is full");

    // Verify we can still compute the hash
    hasher.set_span(DEFAULT_BODY_SIZE as u64);
    let _hash = hasher.sum();

    // Test partial write
    let mut hasher2 = DefaultHasher::new();

    // Write less than full
    let partial_data = vec![0x42; DEFAULT_BODY_SIZE - 50];
    let written = hasher2.write(&partial_data).unwrap();
    assert_eq!(
        written,
        DEFAULT_BODY_SIZE - 50,
        "Should report all bytes written for partial fill"
    );

    // Write more than remaining space
    let excess = hasher2.write(&[0xFF; 100]).unwrap();
    assert_eq!(
        excess, 50,
        "Should only write bytes that fit in remaining space"
    );

    // Buffer should now be full
    let final_write = hasher2.write(&[0xAA; 10]).unwrap();
    assert_eq!(
        final_write, 0,
        "Should return 0 when buffer is already full"
    );
}
