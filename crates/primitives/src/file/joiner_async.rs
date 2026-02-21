//! Async joiner with BFS expansion and concurrent chunk fetching.

use std::io::SeekFrom;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures::stream::{self, Stream, StreamExt};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::ChunkAddress;

use super::error::{FileError, Result};
use super::frontier::{expand_frontier_async, read_subtree_bodies_async};
use super::mode::{JoinMode, PlainMode};
use super::tree::TreeParams;
use crate::store::AsyncChunkGet;

#[cfg(feature = "encryption")]
use super::mode::EncryptedMode;

/// Generic async joiner parameterized by chunk mode.
pub struct GenericAsyncJoiner<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    getter: Arc<G>,
    root: ChunkAddress,
    context: M::JoinerContext,
    span: u64,
    tree: TreeParams<BODY_SIZE>,
    position: u64,
    concurrency: usize,
    _mode: PhantomData<M>,
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
            .field("root", &self.root)
            .field("span", &self.span)
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
            getter: Arc::new(getter),
            root,
            context,
            span,
            tree,
            position: 0,
            concurrency: 8,
            _mode: PhantomData,
        })
    }

    /// Set concurrency level for prefetching.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Total file size.
    #[inline]
    pub fn size(&self) -> u64 {
        self.span
    }

    /// Current read position.
    #[inline]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Root address.
    #[inline]
    pub fn root(&self) -> &ChunkAddress {
        &self.root
    }

    /// Read a range of bytes with concurrent fetching via BFS expansion.
    pub async fn read_range(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
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

        let subtrees = expand_frontier_async::<G, M, BODY_SIZE>(
            &*self.getter,
            &self.root,
            &self.context,
            self.span,
            &chunk_range,
            self.concurrency * 2,
        )
        .await?;

        let getter = Arc::clone(&self.getter);
        let bodies: Vec<Bytes> = stream::iter(subtrees.into_iter())
            .map(|st| {
                let getter = Arc::clone(&getter);
                let chunk_range = chunk_range;
                async move {
                    read_subtree_bodies_async::<G, M, BODY_SIZE>(&*getter, &st, &chunk_range)
                        .await
                }
            })
            .buffered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Vec<Bytes>>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(super::tree::assemble_range(
            &self.tree,
            offset,
            actual_len,
            &chunk_range,
            &bodies,
        ))
    }

    /// Read entire file into memory.
    pub async fn read_all(&self) -> Result<Vec<u8>> {
        self.read_range(0, self.span as usize).await
    }

    /// Update read position (synchronous — just updates internal state).
    pub fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.span as i64 + offset,
            SeekFrom::Current(offset) => self.position as i64 + offset,
        };

        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }

        self.position = new_pos as u64;
        Ok(self.position)
    }

    /// Convert into a stream of chunks.
    pub fn into_stream(self) -> impl Stream<Item = Result<Bytes>> {
        let chunk_size = BODY_SIZE;
        let span = self.span;
        let getter = self.getter;
        let root = self.root;
        let context = self.context;
        let tree = self.tree;
        let concurrency = self.concurrency;

        stream::unfold(0u64, move |offset| {
            let getter = Arc::clone(&getter);
            let root = root;
            let context = context.clone();
            let tree = tree;
            async move {
                if offset >= span {
                    return None;
                }

                let len = (span - offset).min(chunk_size as u64) as usize;
                let actual_len = len.min((span - offset) as usize);

                if span <= chunk_size as u64 {
                    // Single-chunk file
                    let chunk = match getter.get(&root).await {
                        Ok(c) => c,
                        Err(e) => return Some((Err(FileError::getter(e)), span)),
                    };
                    let body = match M::decode_body::<BODY_SIZE>(chunk, &context, span) {
                        Ok(b) => b,
                        Err(e) => return Some((Err(e), span)),
                    };
                    let start = offset as usize;
                    let end = start + actual_len;
                    return Some((Ok(Bytes::from(body[start..end].to_vec())), offset + len as u64));
                }

                let chunk_range = tree.chunks_for_range(offset, actual_len as u64);

                let subtrees = match expand_frontier_async::<G, M, BODY_SIZE>(
                    &*getter, &root, &context, span, &chunk_range, concurrency * 2,
                )
                .await
                {
                    Ok(st) => st,
                    Err(e) => return Some((Err(e), span)),
                };

                let bodies: Vec<Result<Vec<Bytes>>> = {
                    let getter2 = Arc::clone(&getter);
                    stream::iter(subtrees.into_iter())
                        .map(|st| {
                            let getter = Arc::clone(&getter2);
                            let chunk_range = chunk_range;
                            async move {
                                read_subtree_bodies_async::<G, M, BODY_SIZE>(
                                    &*getter, &st, &chunk_range,
                                )
                                .await
                            }
                        })
                        .buffered(concurrency)
                        .collect()
                        .await
                };

                let flat_bodies: Vec<Bytes> = match bodies
                    .into_iter()
                    .collect::<Result<Vec<Vec<Bytes>>>>()
                {
                    Ok(vv) => vv.into_iter().flatten().collect(),
                    Err(e) => return Some((Err(e), span)),
                };

                let data = super::tree::assemble_range(&tree, offset, actual_len, &chunk_range, &flat_bodies);
                Some((Ok(Bytes::from(data)), offset + len as u64))
            }
        })
    }

    /// Convert into an `AsyncRead` reader.
    pub fn into_reader(self) -> AsyncJoinerReader<G, M, BODY_SIZE> {
        AsyncJoinerReader {
            joiner: self,
            buffer: Bytes::new(),
            future: None,
        }
    }

    async fn read_single_chunk(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let chunk = self.getter.get(&self.root).await.map_err(FileError::getter)?;
        let body = M::decode_body::<BODY_SIZE>(chunk, &self.context, self.span)?;
        let start = offset as usize;
        let end = start + len;
        Ok(body[start..end].to_vec())
    }
}

/// Wrapper providing `tokio::io::AsyncRead` over an `AsyncJoiner`.
///
/// Created via [`GenericAsyncJoiner::into_reader`].
pub struct AsyncJoinerReader<G, M: JoinMode, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
{
    joiner: GenericAsyncJoiner<G, M, BODY_SIZE>,
    buffer: Bytes,
    #[allow(clippy::type_complexity)]
    future: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>>,
}

impl<G, M, const BODY_SIZE: usize> std::fmt::Debug for AsyncJoinerReader<G, M, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE>,
    M: JoinMode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncJoinerReader")
            .field("joiner", &self.joiner)
            .field("buffer_len", &self.buffer.len())
            .field("has_pending_future", &self.future.is_some())
            .finish()
    }
}

// Safety: AsyncJoinerReader contains no self-referential data.
// The boxed future is heap-allocated and all other fields are plain data.
impl<G: AsyncChunkGet<BODY_SIZE>, M: JoinMode, const BODY_SIZE: usize> Unpin
    for AsyncJoinerReader<G, M, BODY_SIZE>
{
}

impl<G, M, const BODY_SIZE: usize> tokio::io::AsyncRead for AsyncJoinerReader<G, M, BODY_SIZE>
where
    G: AsyncChunkGet<BODY_SIZE> + 'static,
    M: JoinMode + Send + Sync + 'static,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;

        let this = self.get_mut();

        // Drain any leftover buffer first
        if !this.buffer.is_empty() {
            let to_copy = this.buffer.len().min(buf.remaining());
            buf.put_slice(&this.buffer[..to_copy]);
            this.buffer.advance(to_copy);
            return Poll::Ready(Ok(()));
        }

        // EOF check
        if this.joiner.position >= this.joiner.span {
            return Poll::Ready(Ok(()));
        }

        // Create a future for the next read if we don't have one
        if this.future.is_none() {
            let position = this.joiner.position;
            let remaining = (this.joiner.span - position) as usize;
            let read_len = remaining.min(BODY_SIZE);
            let getter = Arc::clone(&this.joiner.getter);
            let root = this.joiner.root;
            let context = this.joiner.context.clone();
            let span = this.joiner.span;
            let tree = this.joiner.tree;
            let concurrency = this.joiner.concurrency;

            let fut = async move {
                let joiner = GenericAsyncJoiner::<G, M, BODY_SIZE> {
                    getter,
                    root,
                    context,
                    span,
                    tree,
                    position: 0,
                    concurrency,
                    _mode: PhantomData,
                };
                joiner.read_range(position, read_len).await
            };
            this.future = Some(Box::pin(fut));
        }

        // Poll the future
        let fut = this.future.as_mut().unwrap();
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(data)) => {
                this.future = None;
                let bytes = Bytes::from(data);
                this.joiner.position += bytes.len() as u64;
                let to_copy = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..to_copy]);
                if to_copy < bytes.len() {
                    this.buffer = bytes.slice(to_copy..);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                this.future = None;
                Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
            }
            Poll::Pending => Poll::Pending,
        }
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
    async fn test_async_joiner_128_chunks() {
        let data: Vec<u8> =
            (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let result = joiner.read_all().await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_async_joiner_range_spanning_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let start = DEFAULT_BODY_SIZE - 50;
        let len = 100;
        let result = joiner.read_range(start as u64, len).await.unwrap();
        assert_eq!(result, &data[start..start + len]);
    }

    #[tokio::test]
    async fn test_async_joiner_seek() {
        let data = b"hello world";
        let (root, store) = split_and_store(data);

        let mut joiner = AsyncJoiner::new(store, root).await.unwrap();
        joiner.seek(SeekFrom::Start(6)).unwrap();
        assert_eq!(joiner.position(), 6);

        let result = joiner.read_range(joiner.position(), 5).await.unwrap();
        assert_eq!(result, b"world");
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

    #[tokio::test]
    async fn test_async_reader_small() {
        use tokio::io::AsyncReadExt;

        let data = b"hello world";
        let (root, store) = split_and_store(data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_reader();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_async_reader_multi_chunk() {
        use tokio::io::AsyncReadExt;

        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123).map(|i| (i % 256) as u8).collect();
        let (root, store) = split_and_store(&data);

        let joiner = AsyncJoiner::new(store, root).await.unwrap();
        let mut reader = joiner.into_reader();
        let mut result = Vec::new();
        reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, data);
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
        async fn test_encrypted_async_joiner_128_chunks() {
            let data: Vec<u8> =
                (0..DEFAULT_BODY_SIZE * 128).map(|i| (i % 256) as u8).collect();
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

        #[tokio::test]
        async fn test_encrypted_async_joiner_seek() {
            let data = b"hello encrypted world";
            let (root_ref, store) = encrypted_split_and_store(data);

            let mut joiner = EncryptedAsyncJoiner::new(store, root_ref).await.unwrap();
            joiner.seek(SeekFrom::Start(6)).unwrap();
            let result = joiner.read_range(joiner.position(), 9).await.unwrap();
            assert_eq!(result, b"encrypted");
        }
    }
}
