//! Parallel file joiner with BFS fan-out, streaming Read, and Seek.
//!
//! Uses a pre-expanded subtree frontier to avoid redundant intermediate node
//! decryption. Subtrees are processed in parallel via rayon, and `impl Read`
//! provides bounded-memory streaming.

use std::io::{self, SeekFrom};
use std::marker::PhantomData;

use bytes::Bytes;
use rayon::prelude::*;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::Result;
use super::frontier::{SubtreeNode, expand_frontier, read_subtree_bodies};
use super::mode::{JoinMode, PlainMode};
use super::tree::{ChunkRange, TreeParams};
use crate::store::ChunkGet;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic joiner parameterized by chunk mode.
///
/// Uses BFS fan-out to pre-expand intermediate nodes into a frontier of
/// roughly equal-sized subtrees. Implements `std::io::Read` and `std::io::Seek`
/// for bounded-memory streaming.
pub struct GenericJoiner<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
{
    getter: G,
    root: ChunkAddress,
    context: M::JoinerContext,
    span: u64,
    tree: TreeParams<BODY_SIZE>,

    /// Pre-expanded frontier for streaming (computed at construction).
    subtrees: Vec<SubtreeNode<M>>,

    /// Streaming state for Read impl.
    read_pos: u64,
    buffer: Vec<u8>,
    buffer_pos: usize,
    subtree_idx: usize,

    _mode: PhantomData<M>,
}

/// Plain (unencrypted) file joiner.
pub type Joiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericJoiner<G, PlainMode, BODY_SIZE>;

/// Encrypted file joiner.
#[cfg(feature = "encryption")]
pub type EncryptedJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericJoiner<G, EncryptedMode, BODY_SIZE>;

impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for GenericJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericJoiner")
            .field("root", &self.root)
            .field("span", &self.span)
            .field("read_pos", &self.read_pos)
            .finish_non_exhaustive()
    }
}

impl<G, M, const BODY_SIZE: usize> GenericJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
    M: JoinMode + Send + Sync,
{
    /// Create a joiner from a root reference.
    pub fn new(getter: G, input: M::RootRef) -> Result<Self> {
        const { super::constants::assert_valid_body_size::<BODY_SIZE>() };

        let (root, span, context) = super::mode::joiner_init::<M, G, BODY_SIZE>(&getter, input)?;
        let tree = TreeParams::<BODY_SIZE>::new(span);

        // 2x thread count gives each thread >=2 subtrees for balanced work distribution.
        let target = rayon::current_num_threads().max(1) * 2;
        let full_range = tree.chunks_for_range(0, span);
        let subtrees = expand_frontier::<G, M, BODY_SIZE>(
            &getter, &root, &context, span, &full_range, target,
        )?;

        Ok(Self {
            getter,
            root,
            context,
            span,
            tree,
            subtrees,
            read_pos: 0,
            buffer: Vec::new(),
            buffer_pos: 0,
            subtree_idx: 0,
            _mode: PhantomData,
        })
    }

    /// Total file size.
    #[inline]
    pub const fn size(&self) -> u64 {
        self.span
    }

    /// Current read position.
    #[inline]
    pub const fn position(&self) -> u64 {
        self.read_pos
    }

    /// Root address.
    #[inline]
    pub const fn root(&self) -> &ChunkAddress {
        &self.root
    }

    /// Read a range of bytes, fetching required chunks in parallel.
    pub fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        use super::helpers::{ReadRangeCheck, validate_read_range};

        match validate_read_range::<BODY_SIZE>(offset, len, self.span) {
            ReadRangeCheck::Empty => Ok(Vec::new()),
            ReadRangeCheck::SingleChunk { offset, actual_len } => {
                self.read_single_chunk(offset, actual_len)
            }
            ReadRangeCheck::MultiChunk { offset, actual_len } => {
                let chunk_range = self.tree.chunks_for_range(offset, actual_len as u64);
                let range_start_byte = chunk_range.start * BODY_SIZE as u64;
                let range_end_byte = chunk_range.end * BODY_SIZE as u64;

                let bodies =
                    self.collect_bodies(&chunk_range, range_start_byte, range_end_byte)?;

                Ok(super::tree::assemble_range(
                    &self.tree,
                    offset,
                    actual_len,
                    &chunk_range,
                    &bodies,
                ))
            }
        }
    }

    /// Read entire file into memory.
    pub fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.span as usize)
    }

    /// Filter pre-computed subtrees and collect leaf bodies in parallel.
    fn collect_bodies(
        &self,
        chunk_range: &ChunkRange,
        range_start_byte: u64,
        range_end_byte: u64,
    ) -> Result<Vec<Bytes>> {
        let getter = &self.getter;
        let nested: Vec<Vec<Bytes>> = self
            .subtrees
            .par_iter()
            .filter(|st| {
                st.byte_offset < range_end_byte
                    && st.byte_offset + st.span > range_start_byte
            })
            .map(|st| {
                let mut bodies = Vec::with_capacity((st.span as usize / BODY_SIZE).max(1));
                read_subtree_bodies::<G, M, BODY_SIZE>(getter, st, chunk_range, &mut bodies)?;
                Ok(bodies)
            })
            .collect::<Result<Vec<Vec<Bytes>>>>()?;

        Ok(nested.into_iter().flat_map(|v| v.into_iter()).collect())
    }

    fn read_single_chunk(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let body =
            super::mode::read_chunk_body::<M, G, BODY_SIZE>(&self.getter, &self.root, &self.context, self.span)?;
        let start = offset as usize;
        let end = start + len;
        Ok(body[start..end].to_vec())
    }

    /// Process the next batch of subtrees into the internal buffer.
    fn fill_buffer(&mut self) -> Result<()> {
        let batch_size = rayon::current_num_threads().max(1);
        let start_idx = self.subtree_idx;
        let end_idx = (start_idx + batch_size).min(self.subtrees.len());

        let batch = &self.subtrees[start_idx..end_idx];
        if batch.is_empty() {
            return Ok(());
        }

        let batch_start_byte = batch[0].byte_offset;
        let last = &batch[batch.len() - 1];
        let batch_end_byte = (last.byte_offset + last.span).min(self.span);
        let chunk_range = ChunkRange {
            start: batch_start_byte / BODY_SIZE as u64,
            end: batch_end_byte.div_ceil(BODY_SIZE as u64),
        };

        let getter = &self.getter;
        let all_bodies = batch
            .par_iter()
            .map(|st| {
                let mut bodies = Vec::with_capacity((st.span as usize / BODY_SIZE).max(1));
                read_subtree_bodies::<G, M, BODY_SIZE>(
                    getter, st, &chunk_range, &mut bodies,
                )?;
                Ok(bodies)
            })
            .collect::<Result<Vec<Vec<Bytes>>>>()?;

        let estimated = (batch_end_byte - batch_start_byte) as usize;
        self.buffer.clear();
        self.buffer.reserve(estimated);
        for bodies in all_bodies {
            for body in bodies {
                self.buffer.extend_from_slice(&body);
            }
        }
        self.buffer_pos = 0;
        self.subtree_idx = end_idx;

        // After a seek, read_pos may be past the batch start — skip ahead.
        if self.read_pos > batch_start_byte {
            self.buffer_pos = (self.read_pos - batch_start_byte) as usize;
        }

        Ok(())
    }

    /// Copy bytes from the internal buffer to the caller's buffer.
    fn drain_buffer(&mut self, buf: &mut [u8]) -> usize {
        let available = self.buffer.len() - self.buffer_pos;
        let to_copy = buf.len().min(available);
        buf[..to_copy].copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);
        self.buffer_pos += to_copy;
        self.read_pos += to_copy as u64;
        to_copy
    }
}

impl<G, M, const BODY_SIZE: usize> io::Read for GenericJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
    M: JoinMode + Send + Sync,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.read_pos >= self.span {
            return Ok(0);
        }

        if self.buffer_pos < self.buffer.len() {
            return Ok(self.drain_buffer(buf));
        }

        if self.subtree_idx >= self.subtrees.len() {
            return Ok(0);
        }

        self.fill_buffer().map_err(io::Error::other)?;

        if self.buffer.is_empty() {
            return Ok(0);
        }

        Ok(self.drain_buffer(buf))
    }
}

impl<G, M, const BODY_SIZE: usize> io::Seek for GenericJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + Clone + Send + Sync,
    M: JoinMode + Send + Sync,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.read_pos = super::resolve_seek_position(pos, self.read_pos, self.span)?;
        self.buffer.clear();
        self.buffer_pos = 0;
        self.subtree_idx = self
            .subtrees
            .iter()
            .position(|st| st.byte_offset + st.span > self.read_pos)
            .unwrap_or(self.subtrees.len());
        Ok(self.read_pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{Chunk, ContentChunk};
    use crate::file::split;
    use std::collections::HashMap;
    use std::io::{Read, Seek};

    fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, ContentChunk>) {
        let (root, chunks) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        let store: HashMap<ChunkAddress, ContentChunk> =
            chunks.into_iter().map(|c| (*c.address(), c)).collect();
        (root, store)
    }

    // --- Generated shared tests (sync variants) ---
    generate_plain_joiner_tests!(test, Joiner, [], []);

    // --- Sync-only tests: std::io::Read + Seek ---

    #[test]
    fn test_joiner_streaming() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 500).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);

        let mut joiner = Joiner::new(store, root).unwrap();
        let mut result = vec![0u8; data.len()];
        joiner.read_exact(&mut result).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_joiner_small_buffer_streaming() {
        let refs_per_chunk = DEFAULT_BODY_SIZE / super::super::constants::REF_SIZE;
        let data: Vec<u8> =
            (0..DEFAULT_BODY_SIZE * refs_per_chunk).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);

        let mut joiner = Joiner::new(store, root).unwrap();
        let mut result = Vec::new();
        let mut buf = [0u8; 100];
        loop {
            let n = joiner.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            result.extend_from_slice(&buf[..n]);
        }
        assert_eq!(result, data);
    }

    #[test]
    fn test_joiner_seek_start() {
        let data = b"hello world";
        let (root, store) = split_and_store(data);
        let mut joiner = Joiner::new(store, root).unwrap();

        joiner.seek(SeekFrom::Start(6)).unwrap();
        let result = joiner.read_all().unwrap();
        // read_all always reads from offset 0
        assert_eq!(result, data);

        // But seek + Read trait respects position
        joiner.seek(SeekFrom::Start(6)).unwrap();
        let mut buf = vec![0u8; 5];
        joiner.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_joiner_seek_current() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);
        let mut joiner = Joiner::new(store, root).unwrap();

        let offset = DEFAULT_BODY_SIZE + 100;
        joiner.seek(SeekFrom::Start(offset as u64)).unwrap();
        assert_eq!(joiner.position(), offset as u64);

        let mut buf = vec![0u8; 50];
        joiner.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &data[offset..offset + 50]);

        joiner.seek(SeekFrom::Current(-50)).unwrap();
        let mut buf2 = vec![0u8; 50];
        joiner.read_exact(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_joiner_seek_end() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);
        let mut joiner = Joiner::new(store, root).unwrap();

        joiner.seek(SeekFrom::End(-100)).unwrap();
        let mut buf = vec![0u8; 100];
        joiner.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &data[data.len() - 100..]);
    }

    #[test]
    fn test_joiner_seek_negative() {
        let data = b"test data";
        let (root, store) = split_and_store(data);
        let mut joiner = Joiner::new(store, root).unwrap();

        let result = joiner.seek(SeekFrom::Current(-100));
        assert!(result.is_err());
    }

    #[test]
    fn test_joiner_seek_back_and_forth() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);
        let mut joiner = Joiner::new(store, root).unwrap();

        // Read from middle
        joiner.seek(SeekFrom::Start(DEFAULT_BODY_SIZE as u64)).unwrap();
        let mut buf1 = vec![0u8; 100];
        joiner.read_exact(&mut buf1).unwrap();
        assert_eq!(&buf1, &data[DEFAULT_BODY_SIZE..DEFAULT_BODY_SIZE + 100]);

        // Seek back to start
        joiner.seek(SeekFrom::Start(0)).unwrap();
        let mut buf2 = vec![0u8; 100];
        joiner.read_exact(&mut buf2).unwrap();
        assert_eq!(&buf2, &data[..100]);

        // Seek to near-end
        joiner.seek(SeekFrom::End(-50)).unwrap();
        let mut buf3 = vec![0u8; 50];
        joiner.read_exact(&mut buf3).unwrap();
        assert_eq!(&buf3, &data[data.len() - 50..]);
    }

    #[test]
    fn test_joiner_partial_reads() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 2 + 500)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, store) = split_and_store(&data);
        let mut joiner = Joiner::new(store, root).unwrap();

        let mut recovered = Vec::new();
        let mut buf = [0u8; 100];
        loop {
            let n = joiner.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            recovered.extend_from_slice(&buf[..n]);
        }
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_joiner_read_at_eof() {
        let data = b"test data";
        let (root, store) = split_and_store(data);
        let mut joiner = Joiner::new(store, root).unwrap();

        let mut buf = vec![0u8; data.len()];
        joiner.read_exact(&mut buf).unwrap();

        let mut buf2 = [0u8; 10];
        let n = joiner.read(&mut buf2).unwrap();
        assert_eq!(n, 0);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::chunk::encryption::EncryptedChunkRef;
        use crate::file::split_encrypted;

        fn encrypted_split_and_store(
            data: &[u8],
        ) -> (EncryptedChunkRef, HashMap<ChunkAddress, ContentChunk>) {
            let (root_ref, chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
            let store: HashMap<ChunkAddress, ContentChunk> =
                chunks.into_iter().map(|c| (*c.address(), c)).collect();
            (root_ref, store)
        }

        // --- Generated shared tests (sync variants) ---
        generate_encrypted_joiner_tests!(test, EncryptedJoiner, [], []);

        // --- Sync-only tests: std::io::Read + Seek ---

        #[test]
        fn test_encrypted_joiner_streaming() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 65).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let mut joiner = EncryptedJoiner::new(store, root_ref).unwrap();
            let mut result = vec![0u8; data.len()];
            joiner.read_exact(&mut result).unwrap();
            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_joiner_small_buffer_streaming() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let mut joiner = EncryptedJoiner::new(store, root_ref).unwrap();
            let mut result = Vec::new();
            let mut buf = [0u8; 100];
            loop {
                let n = joiner.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                result.extend_from_slice(&buf[..n]);
            }
            assert_eq!(result, data);
        }

        #[test]
        fn test_encrypted_joiner_seek_back_and_forth() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);
            let mut joiner = EncryptedJoiner::new(store, root_ref).unwrap();

            // Read from middle
            joiner.seek(SeekFrom::Start(DEFAULT_BODY_SIZE as u64)).unwrap();
            let mut buf1 = vec![0u8; 100];
            joiner.read_exact(&mut buf1).unwrap();
            assert_eq!(&buf1, &data[DEFAULT_BODY_SIZE..DEFAULT_BODY_SIZE + 100]);

            // Seek back to start
            joiner.seek(SeekFrom::Start(0)).unwrap();
            let mut buf2 = vec![0u8; 100];
            joiner.read_exact(&mut buf2).unwrap();
            assert_eq!(&buf2, &data[..100]);
        }
    }
}
