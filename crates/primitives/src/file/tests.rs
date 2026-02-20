//! Integration tests for file splitting and joining.
//!
//! Includes cross-validation with Go implementation test vectors from
//! bee/pkg/file/testing/vector.go

use std::io::Write;

use alloy_primitives::hex;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::file::Splitter;
use crate::store::VecSink;

const CHUNK_SIZE: usize = DEFAULT_BODY_SIZE;

/// Generate sequential bytes like go-mockbytes with modulus 255.
fn sequential_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 255) as u8).collect()
}

/// Test vectors from bee/pkg/file/testing/vector.go
const TEST_VECTORS: &[(usize, &str)] = &[
    (31, "ece86edb20669cc60d142789d464d57bdf5e33cb789d443f608cbd81cfa5697d"),                      // 0
    (32, "0be77f0bb7abc9cd0abed640ee29849a3072ccfd1020019fe03658c38f087e02"),                      // 1
    (33, "3463b46d4f9d5bfcbf9a23224d635e51896c1daef7d225b86679db17c5fd868e"),                      // 2
    (63, "95510c2ff18276ed94be2160aed4e69c9116573b6f69faaeed1b426fea6a3db8"),                      // 3
    (64, "490072cc55b8ad381335ff882ac51303cc069cbcb8d8d3f7aa152d9c617829fe"),                      // 4
    (65, "541552bae05e9a63a6cb561f69edf36ffe073e441667dbf7a0e9a3864bb744ea"),                      // 5
    (CHUNK_SIZE, "c10090961e7682a10890c334d759a28426647141213abda93b096b892824d2ef"),              // 6
    (CHUNK_SIZE + 31, "91699c83ed93a1f87e326a29ccd8cc775323f9e7260035a5f014c975c5f3cd28"),         // 7
    (CHUNK_SIZE + 32, "73759673a52c1f1707cbb61337645f4fcbd209cdc53d7e2cedaaa9f44df61285"),         // 8
    (CHUNK_SIZE + 63, "db1313a727ffc184ae52a70012fbbf7235f551b9f2d2da04bf476abe42a3cb42"),         // 9
    (CHUNK_SIZE + 64, "ade7af36ac0c7297dc1c11fd7b46981b629c6077bce75300f85b02a6153f161b"),         // 10
    (CHUNK_SIZE * 2, "29a5fb121ce96194ba8b7b823a1f9c6af87e1791f824940a53b5a7efe3f790d9"),          // 11
    (CHUNK_SIZE * 2 + 32, "61416726988f77b874435bdd89a419edc3861111884fd60e8adf54e2f299efd6"),     // 12
    (CHUNK_SIZE * 128, "3047d841077898c26bbe6be652a2ec590a5d9bd7cd45d290ea42511b48753c09"),        // 13
    (CHUNK_SIZE * 128 + 31, "e5c76afa931e33ac94bce2e754b1bb6407d07f738f67856783d93934ca8fc576"),   // 14
    (CHUNK_SIZE * 128 + 32, "485a526fc74c8a344c43a4545a5987d17af9ab401c0ef1ef63aefcc5c2c086df"),   // 15
    (CHUNK_SIZE * 128 + 64, "624b2abb7aefc0978f891b2a56b665513480e5dc195b4a66cd8def074a6d2e94"),   // 16
    (CHUNK_SIZE * 129, "b8e1804e37a064d28d161ab5f256cc482b1423d5cd0a6b30fde7b0f51ece9199"),        // 17
    (CHUNK_SIZE * 130, "59de730bf6c67a941f3b2ffa2f920acfaa1713695ad5deea12b4a121e5f23fa1"),        // 18
];

/// Large test vectors (64MB+), run with --ignored
const LARGE_TEST_VECTORS: &[(usize, &str)] = &[
    (CHUNK_SIZE * 128 * 128, "522194562123473dcfd7a457b18ee7dee8b7db70ed3cfa2b73f348a992fdfd3b"),      // 19: 64MB
    (CHUNK_SIZE * 128 * 128 + 32, "ed0cc44c93b14fef2d91ab3a3674eeb6352a42ac2f0bbe524711824aae1e7bcc"), // 20: 64MB + 32
];

fn run_vector_test(size: usize, expected_hex: &str) {
    let data = sequential_bytes(size);

    let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
    let mut splitter = Splitter::new(sink, data.len() as u64);
    splitter.write_all(&data).unwrap();
    let (root, _) = splitter.finish().unwrap();

    assert_eq!(
        hex::encode(root.as_slice()),
        expected_hex,
        "Root hash mismatch for size {} bytes",
        size
    );
}

#[test]
fn test_go_vector_0_31_bytes() {
    run_vector_test(TEST_VECTORS[0].0, TEST_VECTORS[0].1);
}

#[test]
fn test_go_vector_1_32_bytes() {
    run_vector_test(TEST_VECTORS[1].0, TEST_VECTORS[1].1);
}

#[test]
fn test_go_vector_2_33_bytes() {
    run_vector_test(TEST_VECTORS[2].0, TEST_VECTORS[2].1);
}

#[test]
fn test_go_vector_3_63_bytes() {
    run_vector_test(TEST_VECTORS[3].0, TEST_VECTORS[3].1);
}

#[test]
fn test_go_vector_4_64_bytes() {
    run_vector_test(TEST_VECTORS[4].0, TEST_VECTORS[4].1);
}

#[test]
fn test_go_vector_5_65_bytes() {
    run_vector_test(TEST_VECTORS[5].0, TEST_VECTORS[5].1);
}

#[test]
fn test_go_vector_6_one_chunk() {
    run_vector_test(TEST_VECTORS[6].0, TEST_VECTORS[6].1);
}

#[test]
fn test_go_vector_7_chunk_plus_31() {
    run_vector_test(TEST_VECTORS[7].0, TEST_VECTORS[7].1);
}

#[test]
fn test_go_vector_8_chunk_plus_32() {
    run_vector_test(TEST_VECTORS[8].0, TEST_VECTORS[8].1);
}

#[test]
fn test_go_vector_9_chunk_plus_63() {
    run_vector_test(TEST_VECTORS[9].0, TEST_VECTORS[9].1);
}

#[test]
fn test_go_vector_10_chunk_plus_64() {
    run_vector_test(TEST_VECTORS[10].0, TEST_VECTORS[10].1);
}

#[test]
fn test_go_vector_11_two_chunks() {
    run_vector_test(TEST_VECTORS[11].0, TEST_VECTORS[11].1);
}

#[test]
fn test_go_vector_12_two_chunks_plus_32() {
    run_vector_test(TEST_VECTORS[12].0, TEST_VECTORS[12].1);
}

#[test]
fn test_go_vector_13_128_chunks() {
    run_vector_test(TEST_VECTORS[13].0, TEST_VECTORS[13].1);
}

#[test]
fn test_go_vector_14_128_chunks_plus_31() {
    run_vector_test(TEST_VECTORS[14].0, TEST_VECTORS[14].1);
}

#[test]
fn test_go_vector_15_128_chunks_plus_32() {
    run_vector_test(TEST_VECTORS[15].0, TEST_VECTORS[15].1);
}

#[test]
fn test_go_vector_16_128_chunks_plus_64() {
    run_vector_test(TEST_VECTORS[16].0, TEST_VECTORS[16].1);
}

#[test]
fn test_go_vector_17_129_chunks() {
    run_vector_test(TEST_VECTORS[17].0, TEST_VECTORS[17].1);
}

#[test]
fn test_go_vector_18_130_chunks() {
    run_vector_test(TEST_VECTORS[18].0, TEST_VECTORS[18].1);
}

/// 64MB file - run with: cargo test --ignored
#[test]
#[ignore]
fn test_go_vector_19_64mb() {
    run_vector_test(LARGE_TEST_VECTORS[0].0, LARGE_TEST_VECTORS[0].1);
}

/// 64MB + 32 bytes - run with: cargo test --ignored
#[test]
#[ignore]
fn test_go_vector_20_64mb_plus_32() {
    run_vector_test(LARGE_TEST_VECTORS[1].0, LARGE_TEST_VECTORS[1].1);
}

// Encrypted round-trip tests (matching Bee's TestEncryptDecrypt sizes)
#[cfg(feature = "encryption")]
mod encrypted {
    use std::collections::HashMap;

    use crate::bmt::DEFAULT_BODY_SIZE;
    use crate::chunk::{Chunk, ContentChunk};
    use crate::file::{join_encrypted, split_encrypted};

    /// Sizes matching Bee's TestEncryptDecrypt test suite.
    const TEST_SIZES: &[usize] = &[
        0,
        10,
        100,
        1000,
        4095,
        4096,
        4097,
        4096 * 2,
        4096 * 64,
        4096 * 64 + 1,
        4096 * 65,
        1_000_000,
    ];

    fn run_encrypted_roundtrip(size: usize) {
        let data: Vec<u8> = (0..size).map(|i| (i % 255) as u8).collect();

        let (root_ref, chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();

        assert_eq!(root_ref.to_vec().len(), 64, "Root ref must be 64 bytes for size {size}");

        // Build HashMap store from chunks
        let store: HashMap<_, _> = chunks
            .into_iter()
            .map(|c| (*c.address(), c))
            .collect::<HashMap<_, ContentChunk<DEFAULT_BODY_SIZE>>>();

        let recovered = join_encrypted(&store, root_ref).unwrap();
        assert_eq!(
            recovered, data,
            "Round-trip failed for size {size}"
        );
    }

    #[test]
    fn encrypted_roundtrip_empty() {
        run_encrypted_roundtrip(0);
    }

    #[test]
    fn encrypted_roundtrip_10_bytes() {
        run_encrypted_roundtrip(10);
    }

    #[test]
    fn encrypted_roundtrip_100_bytes() {
        run_encrypted_roundtrip(100);
    }

    #[test]
    fn encrypted_roundtrip_1000_bytes() {
        run_encrypted_roundtrip(1000);
    }

    #[test]
    fn encrypted_roundtrip_4095_bytes() {
        run_encrypted_roundtrip(4095);
    }

    #[test]
    fn encrypted_roundtrip_4096_bytes() {
        run_encrypted_roundtrip(4096);
    }

    #[test]
    fn encrypted_roundtrip_4097_bytes() {
        run_encrypted_roundtrip(4097);
    }

    #[test]
    fn encrypted_roundtrip_two_chunks() {
        run_encrypted_roundtrip(4096 * 2);
    }

    #[test]
    fn encrypted_roundtrip_64_chunks() {
        run_encrypted_roundtrip(4096 * 64);
    }

    #[test]
    fn encrypted_roundtrip_64_chunks_plus_1() {
        run_encrypted_roundtrip(4096 * 64 + 1);
    }

    #[test]
    fn encrypted_roundtrip_65_chunks() {
        run_encrypted_roundtrip(4096 * 65);
    }

    #[test]
    fn encrypted_roundtrip_1mb() {
        run_encrypted_roundtrip(1_000_000);
    }

    #[test]
    fn encrypted_all_test_sizes() {
        for &size in TEST_SIZES {
            run_encrypted_roundtrip(size);
        }
    }

    #[test]
    fn encrypted_chunk_count_single() {
        // Single chunk file: 1 encrypted chunk stored
        let data = b"small data";
        let (_, chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn encrypted_chunk_count_two_data() {
        // 4097 bytes → 2 data chunks + 1 intermediate = 3
        let data = vec![0xAB; 4097];
        let (_, chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(chunks.len(), 3);
    }

    #[test]
    fn encrypted_nondeterministic() {
        // Two encryptions of the same data produce different ciphertexts
        // (different random keys each time)
        let data = b"test determinism";
        let (ref1, _) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
        let (ref2, _) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();

        // Root addresses differ because encryption keys are random
        assert_ne!(ref1.address, ref2.address);
    }
}
