//! Tests for the Binary Merkle Tree implementation.

use crate::bmt::constants::{DEFAULT_BODY_SIZE, PROOF_LENGTH};

use super::*;
use alloy_primitives::{
    B256, FixedBytes,
    hex::{self, ToHexExt},
};
use digest::{Digest, FixedOutputReset};
use proof::Prover;
use rand::RngExt;

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
fn test_hasher_correctness() {
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

    // with_prefix is equivalent to new + prefix_with
    let mut hasher3 = DefaultHasher::with_prefix(b"prefix-");
    hasher3.set_span(11);
    hasher3.update(data);
    assert_eq!(result_with_prefix, hasher3.sum());
}

/// Reference (non-optimized) prefix BMT root: hashes the full 4096-byte tree
/// bottom-up with no zero fast paths, applying `keccak(prefix || ...)` at every
/// node. Used to prove the optimized hasher matches a naive implementation,
/// independently of the zero-subtree shortcuts.
fn reference_prefix_root(prefix: Option<&[u8]>, span: u64, payload: &[u8]) -> B256 {
    use alloy_primitives::Keccak256;

    let node = |left: &[u8], right: &[u8]| {
        let mut h = Keccak256::new();
        if let Some(p) = prefix {
            h.update(p);
        }
        h.update(left);
        h.update(right);
        B256::from_slice(h.finalize().as_slice())
    };

    // Materialise 128 leaf segments of 32 bytes (zero-padded).
    let mut buf = [0u8; DEFAULT_BODY_SIZE];
    let n = payload.len().min(DEFAULT_BODY_SIZE);
    buf[..n].copy_from_slice(&payload[..n]);

    let mut level: Vec<B256> = (0..DEFAULT_BODY_SIZE / 32)
        .map(|i| B256::from_slice(&buf[i * 32..i * 32 + 32]))
        .collect();

    while level.len() > 1 {
        level = level
            .chunks(2)
            .map(|pair| node(pair[0].as_slice(), pair[1].as_slice()))
            .collect();
    }

    // Final span wrap.
    let mut h = Keccak256::new();
    if let Some(p) = prefix {
        h.update(p);
    }
    h.update(span.to_le_bytes());
    h.update(level[0].as_slice());
    B256::from_slice(h.finalize().as_slice())
}

/// Cross-implementation parity gate against bee's published deterministic
/// vector (pkg/storer/sample_test.go TestSampleVectorCAC).
///
/// Chunk content is 4096 bytes with byte[i] = i % 256; the CAC span is 4096
/// (little-endian). The plain BMT root is the chunk address and the
/// anchor-prefixed BMT root is bee's transformed address. Reproducing both
/// byte-for-byte proves nectar's per-node prefixing is bee-identical.
#[test]
fn test_bee_sample_vector_cac_parity() {
    const ANCHOR: &[u8] = b"swarm-test-anchor-deterministic!";
    const WANT_CHUNK_ADDR: &str =
        "902406053a7a2f3a17f16097e1d0b4b6a4abeae6b84968f5503ae621f9522e16";
    const WANT_TRANSFORMED_ADDR: &str =
        "9dee91d1ed794460474ffc942996bd713176731db4581a3c6470fe9862905a60";

    assert_eq!(ANCHOR.len(), 32, "anchor must be exactly 32 bytes");

    // 4096-byte payload with the repeating i % 256 pattern.
    let payload: Vec<u8> = (0..DEFAULT_BODY_SIZE).map(|i| (i % 256) as u8).collect();
    let span = DEFAULT_BODY_SIZE as u64;

    // Plain BMT => chunk address.
    let mut plain = DefaultHasher::new();
    plain.set_span(span);
    plain.update(&payload);
    let chunk_addr = plain.sum();
    assert_eq!(
        chunk_addr.encode_hex(),
        WANT_CHUNK_ADDR,
        "plain BMT chunk address must match bee"
    );

    // Anchor-prefixed BMT => transformed address.
    let mut prefixed = DefaultHasher::with_prefix(ANCHOR);
    prefixed.set_span(span);
    prefixed.update(&payload);
    let transformed = prefixed.sum();
    assert_eq!(
        transformed.encode_hex(),
        WANT_TRANSFORMED_ADDR,
        "anchor-prefixed BMT transformed address must match bee"
    );

    // The optimized hasher must agree with the naive full-recursion reference.
    assert_eq!(
        transformed,
        reference_prefix_root(Some(ANCHOR), span, &payload)
    );
}

/// Zero-subtree parity guard. The dense i%256 vector has no trailing zeros, so
/// it cannot catch a prefix-independent zero table. A sparse payload (data only
/// in the first segment, the rest zero) forces the prefixed zero subtrees to be
/// exercised; the optimized path must still match the naive reference.
#[test]
fn test_prefix_sparse_trailing_zeros_parity() {
    const ANCHOR: &[u8] = b"swarm-test-anchor-deterministic!";

    // Only the first 5 bytes are non-zero; the remaining 4091 bytes are zero.
    let mut payload = vec![0u8; DEFAULT_BODY_SIZE];
    payload[..5].copy_from_slice(b"hello");
    let span = DEFAULT_BODY_SIZE as u64;

    let mut prefixed = DefaultHasher::with_prefix(ANCHOR);
    prefixed.set_span(span);
    prefixed.update(&payload);
    let optimized = prefixed.sum();

    assert_eq!(
        optimized,
        reference_prefix_root(Some(ANCHOR), span, &payload),
        "prefixed hash of a sparse chunk must match the naive reference (zero \
         subtrees must be hashed under the prefix, not via the plain zero table)"
    );

    // An all-zero prefixed chunk must also match the reference.
    let zero_payload = vec![0u8; DEFAULT_BODY_SIZE];
    let mut zero_hasher = DefaultHasher::with_prefix(ANCHOR);
    zero_hasher.set_span(span);
    zero_hasher.update(&zero_payload);
    assert_eq!(
        zero_hasher.sum(),
        reference_prefix_root(Some(ANCHOR), span, &zero_payload)
    );
}

/// A prefixed proof must verify against the prefixed root and only against it.
#[test]
fn test_prefix_proof_roundtrip() {
    const ANCHOR: &[u8] = b"swarm-test-anchor-deterministic!";

    let payload: Vec<u8> = (0..DEFAULT_BODY_SIZE).map(|i| (i % 256) as u8).collect();
    let span = DEFAULT_BODY_SIZE as u64;

    let mut hasher = DefaultHasher::with_prefix(ANCHOR);
    hasher.set_span(span);
    hasher.update(&payload);
    let root = hasher.sum();

    for seg in [0usize, 1, 63, 127] {
        let proof = hasher.generate_proof(&payload, seg).unwrap();
        assert_eq!(proof.prefix.as_deref(), Some(ANCHOR));
        assert!(
            DefaultHasher::verify_proof(&proof, &root).unwrap(),
            "prefixed proof for segment {seg} must verify against the prefixed root"
        );

        // The same proof must NOT verify against the plain (unprefixed) root.
        let mut plain = DefaultHasher::new();
        plain.set_span(span);
        plain.update(&payload);
        let plain_root = plain.sum();
        assert!(
            !DefaultHasher::verify_proof(&proof, &plain_root).unwrap(),
            "prefixed proof must not verify against the plain root"
        );
    }
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
    let is_valid = DefaultHasher::verify_proof(&proof, &root_hash).expect("Failed to verify proof");

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
    let is_valid = DefaultHasher::verify_proof(&proof, &root_hash).expect("Failed to verify proof");

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

    let is_valid = DefaultHasher::verify_proof(&rightmost_proof, &root_hash)
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

    let is_valid = DefaultHasher::verify_proof(&middle_proof, &root_hash)
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
    let is_valid =
        DefaultHasher::verify_proof(&proof, &expected_root_hash).expect("Failed to verify proof");
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
        let is_valid =
            DefaultHasher::verify_proof(&proof, &root_hash).expect("Failed to verify proof");

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

/// Pinned root vectors: a byte-identical regression oracle for the hasher.
///
/// Payload byte i is i % 256 and the span equals the payload length, covering
/// every subtree size class (sub-pair, single pair, partial and full trees).
#[test]
fn test_pinned_root_vectors() {
    let vectors = [
        (
            0usize,
            "b34ca8c22b9e982354f9c7f50b470d66db428d880c8a904d5fe4ec9713171526",
        ),
        (
            1,
            "fe60ba40b87599ddfb9e8947c1c872a4a1a5b56f7d1b80f0a646005b38db52a5",
        ),
        (
            3,
            "abc95807648ca3fc753b6f8a557d5ad3fe88d8c898f9ebffc12e149a3f233e20",
        ),
        (
            31,
            "ece86edb20669cc60d142789d464d57bdf5e33cb789d443f608cbd81cfa5697d",
        ),
        (
            32,
            "0be77f0bb7abc9cd0abed640ee29849a3072ccfd1020019fe03658c38f087e02",
        ),
        (
            33,
            "3463b46d4f9d5bfcbf9a23224d635e51896c1daef7d225b86679db17c5fd868e",
        ),
        (
            63,
            "95510c2ff18276ed94be2160aed4e69c9116573b6f69faaeed1b426fea6a3db8",
        ),
        (
            64,
            "490072cc55b8ad381335ff882ac51303cc069cbcb8d8d3f7aa152d9c617829fe",
        ),
        (
            65,
            "541552bae05e9a63a6cb561f69edf36ffe073e441667dbf7a0e9a3864bb744ea",
        ),
        (
            127,
            "d80c3347053158ae2917fcde392d50b6f46c1b79d5aa4fff033aa209590ec423",
        ),
        (
            128,
            "cd80756bba344aa93f29c21fe0b09433036f82ca7f133131e55e61e9d5dcf3c3",
        ),
        (
            4095,
            "993b76a7701c48ba2f2b701628e1af266ec5ad2cc88a203c2814c403dc25a7d6",
        ),
        (
            4096,
            "902406053a7a2f3a17f16097e1d0b4b6a4abeae6b84968f5503ae621f9522e16",
        ),
    ];

    for (len, want) in vectors {
        let payload: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
        let mut hasher = DefaultHasher::new();
        hasher.set_span(len as u64);
        hasher.update(&payload);
        assert_eq!(
            hasher.sum().encode_hex(),
            want,
            "pinned root mismatch for payload length {len}"
        );
    }
}

/// Pinned zero-tree and prefixed root vectors: the zero fast paths and the
/// per-prefix zero tables must stay byte-identical.
#[test]
fn test_pinned_zero_and_prefix_vectors() {
    const ANCHOR: &[u8] = b"swarm-test-anchor-deterministic!";

    // Empty hasher (span 0) and an all-zero full body hash to the same tree,
    // wrapped with their respective spans.
    let empty = DefaultHasher::new();
    assert_eq!(
        empty.sum().encode_hex(),
        "b34ca8c22b9e982354f9c7f50b470d66db428d880c8a904d5fe4ec9713171526"
    );

    let mut all_zero = DefaultHasher::new();
    all_zero.set_span(DEFAULT_BODY_SIZE as u64);
    all_zero.update(&vec![0u8; DEFAULT_BODY_SIZE]);
    assert_eq!(
        all_zero.sum().encode_hex(),
        "09ae927d0f3aaa37324df178928d3826820f3dd3388ce4aaebfc3af410bde23a"
    );

    // Sparse prefixed chunk: forces the prefixed zero subtrees.
    let mut payload = vec![0u8; DEFAULT_BODY_SIZE];
    payload[..5].copy_from_slice(b"hello");
    let mut sparse = DefaultHasher::with_prefix(ANCHOR);
    sparse.set_span(DEFAULT_BODY_SIZE as u64);
    sparse.update(&payload);
    assert_eq!(
        sparse.sum().encode_hex(),
        "7e7cd97d913012790d752642363df11019b0db1fdd45052e8a5886f68fae524d"
    );

    // Short prefixed payload: prefixed subtree plus prefixed roll-up.
    let payload: Vec<u8> = (0..100u32).map(|i| (i % 256) as u8).collect();
    let mut short = DefaultHasher::with_prefix(ANCHOR);
    short.set_span(100);
    short.update(&payload);
    assert_eq!(
        short.sum().encode_hex(),
        "ea08da374226a0788462f70afa21e0137b99a9213b816797b55880428a8e5011"
    );
}

/// An out-of-tree segment index is a typed error carrying the offending
/// index and the tree width.
#[test]
fn test_proof_segment_out_of_bounds_error() {
    let hasher = DefaultHasher::new();
    let err = hasher
        .generate_proof(b"data", crate::bmt::BRANCHES)
        .unwrap_err();
    match err {
        PrimitivesError::Bmt(BmtError::SegmentOutOfBounds { index, branches }) => {
            assert_eq!(index, crate::bmt::BRANCHES);
            assert_eq!(branches, crate::bmt::BRANCHES);
        }
        other => panic!("expected SegmentOutOfBounds, got {other:?}"),
    }
}
