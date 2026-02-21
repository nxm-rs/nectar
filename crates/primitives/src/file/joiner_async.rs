//! Async joiner with concurrent chunk prefetching.

use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, Stream, StreamExt};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::{FileError, Result};
use super::mode::{JoinMode, PlainMode};
use super::tree::TreeParams;
use crate::store::AsyncChunkGet;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Core state and tree-traversal logic shared by the joiner and its stream.
struct JoinerCore<G, M: JoinMode, const BODY_SIZE: usize>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    getter: Arc<G>,
    root: ChunkAddress,
    context: M::JoinerContext,
    span: u64,
    tree: TreeParams<BODY_SIZE>,
    _mode: PhantomData<M>,
}

impl<G, M, const BODY_SIZE: usize> JoinerCore<G, M, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
    M: JoinMode + Send + Sync,
{
    async fn read_single_chunk(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let chunk = self.getter.get(&self.root).await.map_err(FileError::getter)?;
        let body = M::decode_body::<BODY_SIZE>(chunk, &self.context, self.span)?;
        let start = offset as usize;
        let end = start + len;
        Ok(body[start..end].to_vec())
    }

    async fn collect_data_chunk_refs(
        &self,
        chunk_range: &super::tree::ChunkRange,
    ) -> Result<Vec<(ChunkAddress, M::JoinerContext)>> {
        let mut refs = Vec::with_capacity(chunk_range.len() as usize);
        for chunk_idx in chunk_range.iter() {
            let r = self.find_data_chunk_ref(chunk_idx).await?;
            refs.push(r);
        }
        Ok(refs)
    }

    async fn find_data_chunk_ref(
        &self,
        data_chunk_idx: u64,
    ) -> Result<(ChunkAddress, M::JoinerContext)> {
        let offset = data_chunk_idx * BODY_SIZE as u64;
        self.traverse_to_data_chunk(&self.root, &self.context, self.span, offset)
            .await
    }

    fn traverse_to_data_chunk<'a>(
        &'a self,
        addr: &'a ChunkAddress,
        context: &'a M::JoinerContext,
        span: u64,
        offset: u64,
    ) -> BoxFuture<'a, Result<(ChunkAddress, M::JoinerContext)>> {
        Box::pin(async move {
            if span <= BODY_SIZE as u64 {
                return Ok((*addr, context.clone()));
            }

            let chunk = self.getter.get(addr).await.map_err(FileError::getter)?;
            let body = M::decode_body::<BODY_SIZE>(chunk, context, span)?;

            let subspan = M::subspan_size::<BODY_SIZE>(span);
            let child_index = (offset / subspan) as usize;
            let child_offset = offset % subspan;

            let ref_start = child_index * M::REF_SIZE;
            let ref_end = ref_start + M::REF_SIZE;

            if ref_end > body.len() {
                return Err(FileError::InvalidReference { level: 0 });
            }

            let (child_addr, child_context) = M::parse_child_ref(&body, ref_start)?;
            let child_span = M::child_span::<BODY_SIZE>(span, subspan, child_index);

            self.traverse_to_data_chunk(&child_addr, &child_context, child_span, child_offset)
                .await
        })
    }

    /// Sequential range read used by the stream.
    async fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if offset >= self.span {
            return Ok(Vec::new());
        }

        let actual_len = len.min((self.span - offset) as usize);
        if actual_len == 0 {
            return Ok(Vec::new());
        }

        if self.span <= BODY_SIZE as u64 {
            return self.read_single_chunk(offset, actual_len).await;
        }

        let chunk_range = self.tree.chunks_for_range(offset, actual_len as u64);
        let mut bodies = Vec::with_capacity(chunk_range.len() as usize);
        for chunk_idx in chunk_range.iter() {
            let (addr, ctx) = self.find_data_chunk_ref(chunk_idx).await?;
            let (chunk_start, chunk_end) = self.tree.chunk_range(chunk_idx);
            let data_span = chunk_end - chunk_start;

            let chunk = self.getter.get(&addr).await.map_err(FileError::getter)?;
            let body = M::decode_body::<BODY_SIZE>(chunk, &ctx, data_span)?;
            bodies.push(body);
        }

        Ok(super::tree::assemble_range(
            &self.tree,
            offset,
            actual_len,
            &chunk_range,
            &bodies,
        ))
    }
}

/// Generic async joiner parameterized by chunk mode.
pub struct GenericAsyncJoiner<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    core: Arc<JoinerCore<G, M, BODY_SIZE>>,
    position: u64,
    concurrency: usize,
}

/// Plain (unencrypted) async joiner.
pub type AsyncJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericAsyncJoiner<G, PlainMode, BODY_SIZE>;

/// Encrypted async joiner.
#[cfg(feature = "encryption")]
pub type EncryptedAsyncJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE> =
    GenericAsyncJoiner<G, EncryptedMode, BODY_SIZE>;

impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for GenericAsyncJoiner<G, M, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericAsyncJoiner")
            .field("root", &self.core.root)
            .field("span", &self.core.span)
            .field("position", &self.position)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

impl<G, M, const BODY_SIZE: usize> GenericAsyncJoiner<G, M, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
    M: JoinMode + Send + Sync,
{
    /// Create an async joiner from a root reference.
    pub async fn new(getter: G, input: M::RootRef) -> Result<Self> {
        let addr = M::root_address(&input);
        let root_chunk = getter.get(&addr).await.map_err(FileError::getter)?;
        let (root, span, context) = M::init_from_chunk::<BODY_SIZE>(input, root_chunk)?;
        let tree = TreeParams::<BODY_SIZE>::new(span);

        Ok(Self {
            core: Arc::new(JoinerCore {
                getter: Arc::new(getter),
                root,
                context,
                span,
                tree,
                _mode: PhantomData,
            }),
            position: 0,
            concurrency: 8,
        })
    }

    /// Set concurrency level for prefetching.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Total file size.
    pub fn size(&self) -> u64 {
        self.core.span
    }

    /// Current read position.
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Root address.
    pub fn root(&self) -> &ChunkAddress {
        &self.core.root
    }

    /// Read a range of bytes with concurrent fetching.
    pub async fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if offset >= self.core.span {
            return Ok(Vec::new());
        }

        let actual_len = len.min((self.core.span - offset) as usize);
        if actual_len == 0 {
            return Ok(Vec::new());
        }

        // For small files, just use simple fetch
        if self.core.span <= BODY_SIZE as u64 {
            return self.core.read_single_chunk(offset, actual_len).await;
        }

        // Calculate required data chunks
        let chunk_range = self.core.tree.chunks_for_range(offset, actual_len as u64);

        // Collect addresses/contexts and fetch concurrently
        let data_refs = self.core.collect_data_chunk_refs(&chunk_range).await?;

        // Compute spans for each data chunk
        let data_spans: Vec<u64> = chunk_range
            .iter()
            .map(|idx| {
                let (s, e) = self.core.tree.chunk_range(idx);
                e - s
            })
            .collect();

        // Fetch and decode all data chunks concurrently
        let getter = Arc::clone(&self.core.getter);
        let bodies: Vec<Bytes> = stream::iter(data_refs.iter().zip(data_spans.iter()))
            .map(|((addr, ctx), span)| {
                let getter = Arc::clone(&getter);
                let addr = *addr;
                let ctx = ctx.clone();
                let span = *span;
                async move {
                    let chunk = getter.get(&addr).await.map_err(FileError::getter)?;
                    M::decode_body::<BODY_SIZE>(chunk, &ctx, span)
                }
            })
            .buffer_unordered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(super::tree::assemble_range(
            &self.core.tree,
            offset,
            actual_len,
            &chunk_range,
            &bodies,
        ))
    }

    /// Read entire file into memory.
    pub async fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.core.span as usize).await
    }

    /// Convert into a stream of chunks.
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        let chunk_size = BODY_SIZE;
        let span = self.core.span;
        let core = self.core;

        stream::unfold(0u64, move |offset| {
            let core = Arc::clone(&core);
            async move {
                if offset >= span {
                    return None;
                }

                let len = (span - offset).min(chunk_size as u64) as usize;
                match core.read_range(offset, len).await {
                    Ok(data) => Some((Ok(Bytes::from(data)), offset + len as u64)),
                    Err(e) => Some((Err(e), span)),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::{Chunk, ContentChunk};
    use crate::file::split;
    use std::collections::HashMap as StdHashMap;

    struct TestStore {
        chunks: StdHashMap<ChunkAddress, ContentChunk>,
    }

    impl AsyncChunkGet for TestStore {
        type Error = FileError;

        async fn get(&self, address: &ChunkAddress) -> Result<ContentChunk> {
            self.chunks
                .get(address)
                .cloned()
                .ok_or_else(|| FileError::ChunkNotFound(*address))
        }
    }

    fn split_and_store(data: &[u8]) -> (ChunkAddress, TestStore) {
        let (root, chunks) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        let store: StdHashMap<ChunkAddress, ContentChunk> =
            chunks.into_iter().map(|c| (*c.address(), c)).collect();
        (root, TestStore { chunks: store })
    }

    #[tokio::test]
    async fn test_async_joiner_small() {
        let data = b"hello world";
        let (root, store) = split_and_store(data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let result = joiner.read_all().await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_async_joiner_range() {
        let data = b"hello world";
        let (root, store) = split_and_store(data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let result = joiner.read_range(6, 5).await.unwrap();

        assert_eq!(result, b"world");
    }

    #[tokio::test]
    async fn test_async_joiner_two_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE + 100).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let result = joiner.read_all().await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_async_joiner_stream() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let chunks: Vec<Result<Bytes>> = joiner.into_stream().collect().await;

        let mut recovered = Vec::new();
        for chunk in chunks {
            recovered.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(recovered, data);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::split_encrypted;

        fn encrypted_split_and_store(
            data: &[u8],
        ) -> (
            crate::chunk::encryption::EncryptedChunkRef,
            TestStore,
        ) {
            let (root_ref, chunks) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
            let store: StdHashMap<ChunkAddress, ContentChunk> =
                chunks.into_iter().map(|c| (*c.address(), c)).collect();
            (root_ref, TestStore { chunks: store })
        }

        #[tokio::test]
        async fn test_encrypted_async_joiner_small() {
            let data = b"hello world";
            let (root_ref, store) = encrypted_split_and_store(data);

            let joiner = EncryptedAsyncJoiner::new(store, root_ref).await.unwrap();
            let result = joiner.read_all().await.unwrap();

            assert_eq!(result, data);
        }

        #[tokio::test]
        async fn test_encrypted_async_joiner_two_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE + 100).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let joiner = EncryptedAsyncJoiner::new(store, root_ref).await.unwrap();
            let result = joiner.read_all().await.unwrap();

            assert_eq!(result, data);
        }

        #[tokio::test]
        async fn test_encrypted_async_joiner_stream() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
            let (root_ref, store) = encrypted_split_and_store(&data);

            let joiner = EncryptedAsyncJoiner::new(store, root_ref).await.unwrap();
            let chunks: Vec<Result<Bytes>> = joiner.into_stream().collect().await;

            let mut recovered = Vec::new();
            for chunk in chunks {
                recovered.extend_from_slice(&chunk.unwrap());
            }

            assert_eq!(recovered, data);
        }
    }
}
