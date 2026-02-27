/// Generate plain (unencrypted) joiner tests for both sync and async variants.
///
/// # Parameters
/// - `$test_attr`: test attribute (`test` or `tokio::test`)
/// - `$Joiner`: joiner type name (`Joiner` or `AsyncJoiner`)
/// - `[$($async_fn:tt)*]`: `[async]` for async, `[]` for sync
/// - `[$($aw:tt)*]`: `[await]` for async, `[]` for sync
///
/// The calling module must define:
/// ```ignore
/// fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, AnyChunk>);
/// ```
macro_rules! generate_plain_joiner_tests {
    ($test_attr:meta, $Joiner:ident, [$($async_fn:tt)*], [$($aw:tt)*]) => {
        #[$test_attr]
        $($async_fn)* fn test_joiner_empty() {
            let data = b"";
            let (root, store) = split_and_store(data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            assert_eq!(joiner.size(), 0);
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data.as_slice());
        }

        #[$test_attr]
        $($async_fn)* fn test_joiner_small() {
            let data = b"hello world";
            let (root, store) = split_and_store(data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            assert_eq!(joiner.size(), data.len() as u64);
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data.as_slice());
        }

        #[$test_attr]
        $($async_fn)* fn test_joiner_range() {
            let data = b"hello world";
            let (root, store) = split_and_store(data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            let result = joiner.read_range(6, 5) $(.$aw)* .unwrap();
            assert_eq!(result, b"world");
        }

        #[$test_attr]
        $($async_fn)* fn test_joiner_two_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE + 100).map(|i| (i % 256) as u8).collect();
            let (root, store) = split_and_store(&data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_joiner_range_spanning_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
            let (root, store) = split_and_store(&data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            let start = DEFAULT_BODY_SIZE - 50;
            let len = 100;
            let result = joiner.read_range(start as u64, len) $(.$aw)* .unwrap();
            assert_eq!(result, &data[start..start + len]);
        }

        #[$test_attr]
        $($async_fn)* fn test_round_trip_exact_chunk() {
            let data = vec![0xAB; DEFAULT_BODY_SIZE];
            let (root, store) = split_and_store(&data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_joiner_128_chunks() {
            let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * refs_per_chunk).map(|i| (i % 256) as u8).collect();
            let (root, store) = split_and_store(&data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_joiner_129_chunks() {
            let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * (refs_per_chunk + 1)).map(|i| (i % 256) as u8).collect();
            let (root, store) = split_and_store(&data);
            let joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_joiner_seek_past_end() {
            let data = b"test data";
            let (root, store) = split_and_store(data);
            let mut joiner = $Joiner::new(store, root) $(.$aw)* .unwrap();
            joiner.seek(std::io::SeekFrom::Start(1000)).unwrap();
            assert_eq!(joiner.position(), 1000);
            let result = joiner.read_range(joiner.position(), 10) $(.$aw)* .unwrap();
            assert!(result.is_empty());
        }
    };
}

/// Generate encrypted joiner tests for both sync and async variants.
///
/// The calling module must define:
/// ```ignore
/// fn encrypted_split_and_store(data: &[u8])
///     -> (EncryptedChunkRef, HashMap<ChunkAddress, AnyChunk>);
/// ```
macro_rules! generate_encrypted_joiner_tests {
    ($test_attr:meta, $Joiner:ident, [$($async_fn:tt)*], [$($aw:tt)*]) => {
        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_empty() {
            let (root_ref, store) = encrypted_split_and_store(b"");
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            assert_eq!(joiner.size(), 0);
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert!(result.is_empty());
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_small() {
            let data = b"hello world";
            let (root_ref, store) = encrypted_split_and_store(data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            assert_eq!(joiner.size(), data.len() as u64);
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data.as_slice());
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_range() {
            let data = b"hello encrypted world";
            let (root_ref, store) = encrypted_split_and_store(data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            let result = joiner.read_range(6, 9) $(.$aw)* .unwrap();
            assert_eq!(result, b"encrypted");
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_exact_chunk() {
            let data = vec![0xAB; DEFAULT_BODY_SIZE];
            let (root_ref, store) = encrypted_split_and_store(&data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_two_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE + 100).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_128_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_65_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 65).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_256_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 256).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            let result = joiner.read_all() $(.$aw)* .unwrap();
            assert_eq!(result, data);
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_range_from_middle() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);
            let joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            let start = DEFAULT_BODY_SIZE * 50;
            let len = DEFAULT_BODY_SIZE * 10;
            let result = joiner.read_range(start as u64, len) $(.$aw)* .unwrap();
            assert_eq!(result, &data[start..start + len]);
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_seek() {
            let data = b"hello encrypted world";
            let (root_ref, store) = encrypted_split_and_store(data);
            let mut joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            joiner.seek(std::io::SeekFrom::Start(6)).unwrap();
            let result = joiner.read_range(joiner.position(), 9) $(.$aw)* .unwrap();
            assert_eq!(result, b"encrypted");
        }

        #[$test_attr]
        $($async_fn)* fn test_encrypted_joiner_seek_past_end() {
            let data = b"test data";
            let (root_ref, store) = encrypted_split_and_store(data);
            let mut joiner = $Joiner::new(store, root_ref) $(.$aw)* .unwrap();
            joiner.seek(std::io::SeekFrom::Start(1000)).unwrap();
            let result = joiner.read_range(joiner.position(), 10) $(.$aw)* .unwrap();
            assert!(result.is_empty());
        }
    };
}
