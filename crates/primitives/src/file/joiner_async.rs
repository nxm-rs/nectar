//! Async joiner with concurrent chunk prefetching.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{self, Stream, StreamExt};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{BmtChunk, Chunk, ChunkAddress, ContentChunk};

use super::constants::REF_SIZE;
use super::error::{FileError, Result};
use super::subspan_size;
use super::traits_async::AsyncChunkGet;
use super::tree::TreeParams;

/// Async joiner with concurrent chunk prefetching.
pub struct AsyncJoiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    getter: Arc<G>,
    root: ChunkAddress,
    span: u64,
    position: u64,
    tree: TreeParams<BODY_SIZE>,
    concurrency: usize,
}

impl<G, const BODY_SIZE: usize> std::fmt::Debug for AsyncJoiner<G, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncJoiner")
            .field("root", &self.root)
            .field("span", &self.span)
            .field("position", &self.position)
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

impl<G, const BODY_SIZE: usize> AsyncJoiner<G, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    /// Create an async joiner from a root address.
    pub async fn new(getter: G, root: ChunkAddress) -> Result<Self> {
        let root_chunk = getter.get(&root).await.map_err(FileError::getter)?;
        let span = root_chunk.span();
        let tree = TreeParams::<BODY_SIZE>::new(span);

        Ok(Self {
            getter: Arc::new(getter),
            root,
            span,
            position: 0,
            tree,
            concurrency: 8,
        })
    }

    /// Set concurrency level for prefetching.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Total file size.
    pub const fn size(&self) -> u64 {
        self.span
    }

    /// Current read position.
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Root address.
    pub const fn root(&self) -> &ChunkAddress {
        &self.root
    }

    /// Read a range of bytes with concurrent fetching.
    pub async fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if offset >= self.span {
            return Ok(Vec::new());
        }

        let actual_len = len.min((self.span - offset) as usize);
        if actual_len == 0 {
            return Ok(Vec::new());
        }

        // For small files, just use simple fetch
        if self.span <= BODY_SIZE as u64 {
            return self.read_single_chunk(offset, actual_len).await;
        }

        // Calculate required data chunks
        let chunk_range = self.tree.chunks_for_range(offset, actual_len as u64);

        // Collect addresses and fetch concurrently
        let data_addrs = self.collect_data_chunk_addrs(&chunk_range).await?;

        // Fetch all data chunks concurrently
        let getter = Arc::clone(&self.getter);
        let chunks: HashMap<ChunkAddress, ContentChunk<BODY_SIZE>> = stream::iter(data_addrs.iter())
            .map(|addr| {
                let getter = Arc::clone(&getter);
                let addr = *addr;
                async move {
                    let chunk = getter.get(&addr).await.map_err(FileError::getter)?;
                    Ok::<_, FileError>((addr, chunk))
                }
            })
            .buffer_unordered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<HashMap<_, _>>>()?;

        // Assemble result
        let mut result = vec![0u8; actual_len];
        let mut result_offset = 0;

        for chunk_idx in chunk_range.iter() {
            let (chunk_start, chunk_end) = self.tree.chunk_range(chunk_idx);
            let chunk_data_len = (chunk_end - chunk_start) as usize;

            let read_start = if chunk_start < offset {
                (offset - chunk_start) as usize
            } else {
                0
            };

            let read_end = if chunk_end > offset + actual_len as u64 {
                chunk_data_len - ((chunk_end - offset - actual_len as u64) as usize)
            } else {
                chunk_data_len
            };

            let bytes_to_copy = read_end - read_start;

            let addr = data_addrs[(chunk_idx - chunk_range.start) as usize];
            let chunk = chunks.get(&addr).ok_or(FileError::ChunkNotFound(addr))?;
            let data = chunk.data();

            result[result_offset..result_offset + bytes_to_copy]
                .copy_from_slice(&data[read_start..read_end]);
            result_offset += bytes_to_copy;
        }

        Ok(result)
    }

    /// Read entire file into memory.
    pub async fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.span as usize).await
    }

    /// Convert into a stream of chunks.
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        let chunk_size = BODY_SIZE;
        let span = self.span;
        let getter = self.getter;
        let root = self.root;
        let tree = self.tree;

        stream::unfold(0u64, move |offset| {
            let getter = Arc::clone(&getter);
            let tree = tree;
            async move {
                if offset >= span {
                    return None;
                }

                let len = (span - offset).min(chunk_size as u64) as usize;
                let joiner = AsyncJoinerInternal {
                    getter,
                    root,
                    span,
                    tree,
                };

                match joiner.read_range_internal(offset, len).await {
                    Ok(data) => Some((Ok(Bytes::from(data)), offset + len as u64)),
                    Err(e) => Some((Err(e), span)), // End stream on error
                }
            }
        })
    }

    async fn read_single_chunk(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let chunk = self.getter.get(&self.root).await.map_err(FileError::getter)?;
        let data = chunk.data();
        let start = offset as usize;
        let end = start + len;
        Ok(data[start..end].to_vec())
    }

    async fn collect_data_chunk_addrs(
        &self,
        chunk_range: &super::tree::ChunkRange,
    ) -> Result<Vec<ChunkAddress>> {
        let mut addrs = Vec::with_capacity(chunk_range.len() as usize);

        for chunk_idx in chunk_range.iter() {
            let addr = self.find_data_chunk_addr(chunk_idx).await?;
            addrs.push(addr);
        }

        Ok(addrs)
    }

    async fn find_data_chunk_addr(&self, data_chunk_idx: u64) -> Result<ChunkAddress> {
        let offset = data_chunk_idx * BODY_SIZE as u64;
        self.traverse_to_data_chunk(&self.root, self.span, offset)
            .await
    }

    fn traverse_to_data_chunk<'a>(
        &'a self,
        addr: &'a ChunkAddress,
        span: u64,
        offset: u64,
    ) -> BoxFuture<'a, Result<ChunkAddress>> {
        Box::pin(async move {
            if span <= BODY_SIZE as u64 {
                return Ok(*addr);
            }

            let chunk = self.getter.get(addr).await.map_err(FileError::getter)?;
            let chunk_data = chunk.data();

            let subspan = subspan_size::<BODY_SIZE>(span);
            let child_index = (offset / subspan) as usize;
            let child_offset = offset % subspan;

            let ref_start = child_index * REF_SIZE;
            let ref_end = ref_start + REF_SIZE;

            if ref_end > chunk_data.len() {
                return Err(FileError::InvalidReference { level: 0 });
            }

            let child_addr_bytes: [u8; 32] = chunk_data[ref_start..ref_end]
                .try_into()
                .map_err(|_| FileError::InvalidReference { level: 0 })?;
            let child_addr = ChunkAddress::from(child_addr_bytes);

            let refs_per_chunk = BODY_SIZE / REF_SIZE;
            let child_span = if child_index == refs_per_chunk - 1 {
                let preceding = child_index as u64 * subspan;
                span.saturating_sub(preceding)
            } else {
                subspan.min(span - child_index as u64 * subspan)
            };

            self.traverse_to_data_chunk(&child_addr, child_span, child_offset)
                .await
        })
    }
}

/// Internal helper for stream.
struct AsyncJoinerInternal<G, const BODY_SIZE: usize>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    getter: Arc<G>,
    root: ChunkAddress,
    span: u64,
    tree: TreeParams<BODY_SIZE>,
}

impl<G, const BODY_SIZE: usize> AsyncJoinerInternal<G, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    async fn read_range_internal(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if self.span <= BODY_SIZE as u64 {
            let chunk = self.getter.get(&self.root).await.map_err(FileError::getter)?;
            let data = chunk.data();
            let start = offset as usize;
            let end = start + len;
            return Ok(data[start..end].to_vec());
        }

        let chunk_range = self.tree.chunks_for_range(offset, len as u64);
        let mut result = vec![0u8; len];
        let mut result_offset = 0;

        for chunk_idx in chunk_range.iter() {
            let addr = self.find_data_chunk_addr(chunk_idx).await?;
            let chunk = self.getter.get(&addr).await.map_err(FileError::getter)?;
            let data = chunk.data();

            let (chunk_start, chunk_end) = self.tree.chunk_range(chunk_idx);
            let chunk_data_len = (chunk_end - chunk_start) as usize;

            let read_start = if chunk_start < offset {
                (offset - chunk_start) as usize
            } else {
                0
            };

            let read_end = if chunk_end > offset + len as u64 {
                chunk_data_len - ((chunk_end - offset - len as u64) as usize)
            } else {
                chunk_data_len
            };

            let bytes_to_copy = read_end - read_start;
            result[result_offset..result_offset + bytes_to_copy]
                .copy_from_slice(&data[read_start..read_end]);
            result_offset += bytes_to_copy;
        }

        Ok(result)
    }

    async fn find_data_chunk_addr(&self, data_chunk_idx: u64) -> Result<ChunkAddress> {
        let offset = data_chunk_idx * BODY_SIZE as u64;
        self.traverse_to_data_chunk(&self.root, self.span, offset)
            .await
    }

    fn traverse_to_data_chunk<'a>(
        &'a self,
        addr: &'a ChunkAddress,
        span: u64,
        offset: u64,
    ) -> BoxFuture<'a, Result<ChunkAddress>> {
        Box::pin(async move {
            if span <= BODY_SIZE as u64 {
                return Ok(*addr);
            }

            let chunk = self.getter.get(addr).await.map_err(FileError::getter)?;
            let chunk_data = chunk.data();

            let subspan = subspan_size::<BODY_SIZE>(span);
            let child_index = (offset / subspan) as usize;
            let child_offset = offset % subspan;

            let ref_start = child_index * REF_SIZE;
            let ref_end = ref_start + REF_SIZE;

            if ref_end > chunk_data.len() {
                return Err(FileError::InvalidReference { level: 0 });
            }

            let child_addr_bytes: [u8; 32] = chunk_data[ref_start..ref_end]
                .try_into()
                .map_err(|_| FileError::InvalidReference { level: 0 })?;
            let child_addr = ChunkAddress::from(child_addr_bytes);

            let refs_per_chunk = BODY_SIZE / REF_SIZE;
            let child_span = if child_index == refs_per_chunk - 1 {
                let preceding = child_index as u64 * subspan;
                span.saturating_sub(preceding)
            } else {
                subspan.min(span - child_index as u64 * subspan)
            };

            self.traverse_to_data_chunk(&child_addr, child_span, child_offset)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
