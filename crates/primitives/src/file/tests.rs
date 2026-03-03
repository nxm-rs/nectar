//! Integration tests for file splitting and joining.
//!
//! Includes cross-validation with reference implementation test vectors.

use std::io::Write;

use alloy_primitives::hex;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::file::SyncSplitter;
use crate::store::MemoryStore;

const CHUNK_SIZE: usize = DEFAULT_BODY_SIZE;

/// Generate sequential bytes with modulus 255.
fn sequential_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 255) as u8).collect()
}

/// Reference test vectors for file splitting.
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

    let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
    let mut splitter = SyncSplitter::new(store, data.len() as u64);
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
fn test_go_vectors() {
    for (i, &(size, expected)) in TEST_VECTORS.iter().enumerate() {
        run_vector_test(size, expected);
        eprintln!("  vector {i}: {size} bytes OK");
    }
}

/// 64MB+ files - run with: cargo test --ignored
#[test]
#[ignore]
fn test_go_vectors_large() {
    for (i, &(size, expected)) in LARGE_TEST_VECTORS.iter().enumerate() {
        run_vector_test(size, expected);
        eprintln!("  large vector {i}: {size} bytes OK");
    }
}

// Encrypted round-trip tests
#[cfg(feature = "encryption")]
mod encrypted {
    use crate::bmt::DEFAULT_BODY_SIZE;
    use crate::file::{sync_join, sync_split_encrypted};

    /// Test sizes covering various boundary conditions.
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

        let (root_ref, store) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();

        assert_eq!(Vec::from(&root_ref).len(), 64, "Root ref must be 64 bytes for size {size}");

        let recovered = sync_join(&store, root_ref).unwrap();
        assert_eq!(
            recovered, data,
            "Round-trip failed for size {size}"
        );
    }

    #[test]
    fn encrypted_roundtrip_all_sizes() {
        for &size in TEST_SIZES {
            run_encrypted_roundtrip(size);
            eprintln!("  encrypted roundtrip: {size} bytes OK");
        }
    }

    #[test]
    fn encrypted_chunk_count_single() {
        // Single chunk file: 1 encrypted chunk stored
        let data = b"small data";
        let (_, store) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn encrypted_chunk_count_two_data() {
        // 4097 bytes → 2 data chunks + 1 intermediate = 3
        let data = vec![0xAB; 4097];
        let (_, store) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(&data).unwrap();
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn encrypted_nondeterministic() {
        // Two encryptions of the same data produce different ciphertexts
        // (different random keys each time)
        let data = b"test determinism";
        let (ref1, _) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
        let (ref2, _) = sync_split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();

        // Root addresses differ because encryption keys are random
        assert_ne!(ref1.address(), ref2.address());
    }
}

mod write_file_ext {
    use crate::file::{SyncChunkGetExt, SyncChunkPutExt};
    use crate::store::MemoryStore;
    use crate::bmt::DEFAULT_BODY_SIZE;

    #[test]
    fn write_file_roundtrip() {
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let addr = store.write_file(b"hello swarm").unwrap();
        let recovered = store.read_file(addr).unwrap();
        assert_eq!(recovered, b"hello swarm");
    }

    #[test]
    fn write_file_large() {
        let data = vec![0xAB; 8192];
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let addr = store.write_file(&data).unwrap();
        let recovered = store.read_file(addr).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn writer_roundtrip() {
        use std::io::Write;
        let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
        let data = b"streaming via writer";
        let mut writer = store.writer(data.len() as u64);
        writer.write_all(data).unwrap();
        let (root, _) = writer.finish().unwrap();
        let recovered = store.read_file(root).unwrap();
        assert_eq!(recovered, data);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use crate::file::{SyncChunkGetExt, SyncChunkPutExt};
        use crate::store::MemoryStore;
        use crate::bmt::DEFAULT_BODY_SIZE;

        #[test]
        fn write_encrypted_file_roundtrip() {
            let store = MemoryStore::<DEFAULT_BODY_SIZE>::new();
            let enc_ref = store.write_encrypted_file(b"secret data").unwrap();
            let recovered = store.read_file(enc_ref).unwrap();
            assert_eq!(recovered, b"secret data");
        }
    }
}
