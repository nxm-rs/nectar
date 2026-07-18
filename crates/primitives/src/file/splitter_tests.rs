/// Generate plain (unencrypted) splitter tests.
///
/// The calling module must pass an adapter function:
/// ```ignore
/// fn split_and_store(data: &[u8]) -> (ChunkAddress, MemoryStore<StandardChunkSet>);
/// ```
macro_rules! generate_plain_splitter_tests {
    ($split_fn:ident) => {
        use super::super::constants::REF_SIZE;
        const REFS_PER_CHUNK: usize = DEFAULT_BODY_SIZE / REF_SIZE;

        #[test]
        fn test_splitter_empty() {
            let (root, store) = $split_fn(b"");
            assert_eq!(store.len(), 1);
            assert!(!root.is_zero());
        }

        #[test]
        fn test_splitter_small() {
            let (root, store) = $split_fn(b"hello world");
            assert_eq!(store.len(), 1);
            assert!(!root.is_zero());
        }

        #[test]
        fn test_splitter_exact_chunk() {
            let data = vec![0xAB; DEFAULT_BODY_SIZE];
            let (root, store) = $split_fn(&data);
            assert_eq!(store.len(), 1);
            assert!(!root.is_zero());
        }

        #[test]
        fn test_splitter_two_chunks() {
            let data = vec![0xCD; DEFAULT_BODY_SIZE + 1];
            let (root, store) = $split_fn(&data);
            assert_eq!(store.len(), 3);
            assert!(!root.is_zero());
        }

        #[test]
        fn test_splitter_128_chunks() {
            let mut data = vec![0u8; DEFAULT_BODY_SIZE * REFS_PER_CHUNK];
            rand::RngExt::fill(&mut rand::rng(), &mut data);
            let (root, store) = $split_fn(&data);
            assert_eq!(store.len(), REFS_PER_CHUNK + 1);
            assert!(!root.is_zero());
        }

        #[test]
        fn test_splitter_129_chunks() {
            let mut data = vec![0u8; DEFAULT_BODY_SIZE * (REFS_PER_CHUNK + 1)];
            rand::RngExt::fill(&mut rand::rng(), &mut data);
            let (root, store) = $split_fn(&data);
            assert_eq!(store.len(), REFS_PER_CHUNK + 1 + 2);
            assert!(!root.is_zero());
        }

        #[test]
        fn test_splitter_roundtrip_varying() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 123)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root, store) = $split_fn(&data);
                let recovered = crate::file::join(&store, root).await.unwrap();
                assert_eq!(recovered, data);
            })
        }
    };
}

/// Generate encrypted splitter tests.
///
/// The calling module must pass an adapter function:
/// ```ignore
/// fn encrypted_split_and_store(data: &[u8])
///     -> (EncryptedChunkRef, MemoryStore<StandardChunkSet>);
/// ```
#[cfg(feature = "encryption")]
macro_rules! generate_encrypted_splitter_tests {
    ($split_fn:ident) => {
        #[test]
        fn test_encrypted_splitter_empty() {
            let (root_ref, store) = $split_fn(b"");
            assert_eq!(store.len(), 1);
            assert_eq!(Vec::from(&root_ref).len(), 64);
        }

        #[test]
        fn test_encrypted_splitter_small() {
            let data = b"hello world";
            let (root_ref, store) = $split_fn(data);
            assert_eq!(Vec::from(&root_ref).len(), 64);
            assert_eq!(store.len(), 1);
        }

        #[test]
        fn test_encrypted_splitter_two_chunks() {
            nectar_testing::run(async {
                let data = vec![0xCD; DEFAULT_BODY_SIZE + 1];
                let (root_ref, store) = $split_fn(&data);
                assert_eq!(store.len(), 3);

                let recovered = crate::file::join(&store, root_ref).await.unwrap();
                assert_eq!(recovered, data);
            })
        }

        #[test]
        fn test_encrypted_splitter_roundtrip() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 123)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root_ref, store) = $split_fn(&data);

                let recovered = crate::file::join(&store, root_ref).await.unwrap();
                assert_eq!(recovered, data);
            })
        }
    };
}
