/// Generate plain (unencrypted) joiner tests over `$Joiner`.
///
/// The calling module must define:
/// ```ignore
/// fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, Chunk>);
/// ```
macro_rules! generate_plain_joiner_tests {
    ($Joiner:ident) => {
        #[test]
        fn test_joiner_empty() {
            nectar_testing::run(async {
                let data = b"";
                let (root, store) = split_and_store(data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                assert_eq!(joiner.size(), 0);
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data.as_slice());
            })
        }

        #[test]
        fn test_joiner_small() {
            nectar_testing::run(async {
                let data = b"hello world";
                let (root, store) = split_and_store(data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                assert_eq!(joiner.size(), data.len() as u64);
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data.as_slice());
            })
        }

        #[test]
        fn test_joiner_range() {
            nectar_testing::run(async {
                let data = b"hello world";
                let (root, store) = split_and_store(data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                let result = joiner.read_range(6, 5).await.unwrap();
                assert_eq!(result, b"world");
            })
        }

        #[test]
        fn test_joiner_two_chunks() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE + 100)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root, store) = split_and_store(&data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_joiner_range_spanning_chunks() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root, store) = split_and_store(&data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                let start = DEFAULT_BODY_SIZE - 50;
                let len = 100;
                let result = joiner.read_range(start as u64, len).await.unwrap();
                assert_eq!(result, &data[start..start + len]);
            })
        }

        #[test]
        fn test_round_trip_exact_chunk() {
            nectar_testing::run(async {
                let data = vec![0xAB; DEFAULT_BODY_SIZE];
                let (root, store) = split_and_store(&data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_joiner_128_chunks() {
            nectar_testing::run(async {
                let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * refs_per_chunk)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root, store) = split_and_store(&data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_joiner_129_chunks() {
            nectar_testing::run(async {
                let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * (refs_per_chunk + 1))
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root, store) = split_and_store(&data);
                let joiner = $Joiner::new(store, root).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_joiner_seek_past_end() {
            nectar_testing::run(async {
                let data = b"test data";
                let (root, store) = split_and_store(data);
                let mut joiner = $Joiner::new(store, root).await.unwrap();
                joiner.seek(std::io::SeekFrom::Start(1000)).unwrap();
                assert_eq!(joiner.position(), 1000);
                let result = joiner.read_range(joiner.position(), 10).await.unwrap();
                assert!(result.is_empty());
            })
        }

        #[test]
        fn test_joiner_seek_back_and_forth() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root, store) = split_and_store(&data);
                let mut joiner = $Joiner::new(store, root).await.unwrap();

                // Read from middle
                joiner
                    .seek(std::io::SeekFrom::Start(DEFAULT_BODY_SIZE as u64))
                    .unwrap();
                let buf1 = joiner.read_range(joiner.position(), 100).await.unwrap();
                assert_eq!(&buf1, &data[DEFAULT_BODY_SIZE..DEFAULT_BODY_SIZE + 100]);

                // Seek back to start
                joiner.seek(std::io::SeekFrom::Start(0)).unwrap();
                let buf2 = joiner.read_range(joiner.position(), 100).await.unwrap();
                assert_eq!(&buf2, &data[..100]);

                // Seek to near-end
                joiner.seek(std::io::SeekFrom::End(-50)).unwrap();
                let buf3 = joiner.read_range(joiner.position(), 50).await.unwrap();
                assert_eq!(&buf3, &data[data.len() - 50..]);
            })
        }

        #[test]
        fn test_joiner_seek_end() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root, store) = split_and_store(&data);
                let mut joiner = $Joiner::new(store, root).await.unwrap();

                joiner.seek(std::io::SeekFrom::End(-100)).unwrap();
                let buf = joiner.read_range(joiner.position(), 100).await.unwrap();
                assert_eq!(&buf, &data[data.len() - 100..]);
            })
        }
    };
}

/// Generate encrypted joiner tests over `$Joiner`.
///
/// The calling module must define:
/// ```ignore
/// fn encrypted_split_and_store(data: &[u8])
///     -> (EncryptedChunkRef, HashMap<ChunkAddress, Chunk>);
/// ```
#[cfg(feature = "encryption")]
macro_rules! generate_encrypted_joiner_tests {
    ($Joiner:ident) => {
        #[test]
        fn test_encrypted_joiner_empty() {
            nectar_testing::run(async {
                let (root_ref, store) = encrypted_split_and_store(b"");
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                assert_eq!(joiner.size(), 0);
                let result = joiner.read_all().await.unwrap();
                assert!(result.is_empty());
            })
        }

        #[test]
        fn test_encrypted_joiner_small() {
            nectar_testing::run(async {
                let data = b"hello world";
                let (root_ref, store) = encrypted_split_and_store(data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                assert_eq!(joiner.size(), data.len() as u64);
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data.as_slice());
            })
        }

        #[test]
        fn test_encrypted_joiner_range() {
            nectar_testing::run(async {
                let data = b"hello encrypted world";
                let (root_ref, store) = encrypted_split_and_store(data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                let result = joiner.read_range(6, 9).await.unwrap();
                assert_eq!(result, b"encrypted");
            })
        }

        #[test]
        fn test_encrypted_joiner_exact_chunk() {
            nectar_testing::run(async {
                let data = vec![0xAB; DEFAULT_BODY_SIZE];
                let (root_ref, store) = encrypted_split_and_store(&data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_encrypted_joiner_two_chunks() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE + 100)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root_ref, store) = encrypted_split_and_store(&data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_encrypted_joiner_128_chunks() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 128)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root_ref, store) = encrypted_split_and_store(&data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_encrypted_joiner_65_chunks() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 65)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root_ref, store) = encrypted_split_and_store(&data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_encrypted_joiner_256_chunks() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 256)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root_ref, store) = encrypted_split_and_store(&data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                let result = joiner.read_all().await.unwrap();
                assert_eq!(result, data);
            })
        }

        #[test]
        fn test_encrypted_joiner_range_from_middle() {
            nectar_testing::run(async {
                let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 128)
                    .map(|i| (i % 256) as u8)
                    .collect();
                let (root_ref, store) = encrypted_split_and_store(&data);
                let joiner = $Joiner::new(store, root_ref).await.unwrap();
                let start = DEFAULT_BODY_SIZE * 50;
                let len = DEFAULT_BODY_SIZE * 10;
                let result = joiner.read_range(start as u64, len).await.unwrap();
                assert_eq!(result, &data[start..start + len]);
            })
        }

        #[test]
        fn test_encrypted_joiner_seek() {
            nectar_testing::run(async {
                let data = b"hello encrypted world";
                let (root_ref, store) = encrypted_split_and_store(data);
                let mut joiner = $Joiner::new(store, root_ref).await.unwrap();
                joiner.seek(std::io::SeekFrom::Start(6)).unwrap();
                let result = joiner.read_range(joiner.position(), 9).await.unwrap();
                assert_eq!(result, b"encrypted");
            })
        }

        #[test]
        fn test_encrypted_joiner_seek_past_end() {
            nectar_testing::run(async {
                let data = b"test data";
                let (root_ref, store) = encrypted_split_and_store(data);
                let mut joiner = $Joiner::new(store, root_ref).await.unwrap();
                joiner.seek(std::io::SeekFrom::Start(1000)).unwrap();
                let result = joiner.read_range(joiner.position(), 10).await.unwrap();
                assert!(result.is_empty());
            })
        }
    };
}
