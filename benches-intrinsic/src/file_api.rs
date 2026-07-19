//! Unification layer over the two file pipelines.

use core::future::poll_fn;

use futures::executor::block_on;
use nectar_file::{
    Encrypted, File, MemSink, Plain, PutWindow, RandomKeys, Split, SplitMode, Window,
};
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::chunk::{ChunkAddress, StandardChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkPut, ChunkStoreError};

/// Store surface every driver runs against; the counting store and its
/// latency-shaped wrapper both qualify.
pub trait BenchStore:
    ChunkGet<StandardChunkSet, Trust = Verified, Error = ChunkStoreError>
    + ChunkPut<StandardChunkSet, Error = core::convert::Infallible>
    + ChunkHas
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<T> BenchStore for T where
    T: ChunkGet<StandardChunkSet, Trust = Verified, Error = ChunkStoreError>
        + ChunkPut<StandardChunkSet, Error = core::convert::Infallible>
        + ChunkHas
        + Clone
        + Send
        + Sync
        + 'static
{
}

/// The common file surface both pipelines answer. Every driver is generic
/// over the store, so both sides monomorphize against identical state.
pub trait FilePipeline {
    /// Stable implementation label.
    const NAME: &'static str;

    /// Split `data` into chunks in `store`; the root address.
    fn split<S: BenchStore>(store: &S, data: &[u8]) -> ChunkAddress;

    /// Join the tree at `root` back into bytes.
    fn join<S: BenchStore>(store: &S, root: &ChunkAddress) -> Vec<u8>;

    /// Join with an explicit fetch depth, so both pipelines can run at a
    /// matched in-flight budget rather than their as-shipped defaults.
    fn join_depth<S: BenchStore>(store: &S, root: &ChunkAddress, depth: u16) -> Vec<u8>;

    /// Split `data` into encrypted chunks; the root reference.
    fn split_encrypted<S: BenchStore>(store: &S, data: &[u8]) -> EncryptedChunkRef;

    /// Join the encrypted tree at `root` back into bytes.
    fn join_encrypted<S: BenchStore>(store: &S, root: &EncryptedChunkRef) -> Vec<u8>;

    /// Open once, read every `(offset, len)` range in order; the
    /// concatenated bytes.
    fn read_ranges<S: BenchStore>(
        store: &S,
        root: &ChunkAddress,
        ranges: &[(u64, usize)],
    ) -> Vec<u8>;
}

/// Drive the streaming split engine to its root, either mode.
fn stream_split<S: BenchStore, M: SplitMode + Default>(store: &S, data: &[u8]) -> M::Root {
    block_on(async {
        let mut split: Split<S, M> = Split::new(store.clone(), PutWindow::DEFAULT);
        let mut rest = data;
        while !rest.is_empty() {
            let taken = poll_fn(|cx| split.poll_write(cx, rest)).await.unwrap();
            rest = &rest[taken..];
        }
        poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
    })
}

/// nectar-file streaming pipeline: poll-native split engine, read facade.
#[derive(Clone, Copy, Debug)]
pub struct FileStreaming;

impl FilePipeline for FileStreaming {
    const NAME: &'static str = "file-streaming";

    fn split<S: BenchStore>(store: &S, data: &[u8]) -> ChunkAddress {
        stream_split::<S, Plain>(store, data)
    }

    fn join<S: BenchStore>(store: &S, root: &ChunkAddress) -> Vec<u8> {
        block_on(async {
            let file = File::<_, Plain>::open(store.clone(), *root).await.unwrap();
            file.collect(u64::MAX).await.unwrap()
        })
    }

    fn join_depth<S: BenchStore>(store: &S, root: &ChunkAddress, depth: u16) -> Vec<u8> {
        block_on(async {
            let file = File::<_, Plain>::open(store.clone(), *root).await.unwrap();
            file.read()
                .window(Window::new(depth).unwrap())
                .collect(u64::MAX)
                .await
                .unwrap()
        })
    }

    fn split_encrypted<S: BenchStore>(store: &S, data: &[u8]) -> EncryptedChunkRef {
        stream_split::<S, Encrypted<RandomKeys>>(store, data)
    }

    fn join_encrypted<S: BenchStore>(store: &S, root: &EncryptedChunkRef) -> Vec<u8> {
        block_on(async {
            let file = File::<_, Encrypted>::open_encrypted(store.clone(), root.clone())
                .await
                .unwrap();
            file.collect(u64::MAX).await.unwrap()
        })
    }

    fn read_ranges<S: BenchStore>(
        store: &S,
        root: &ChunkAddress,
        ranges: &[(u64, usize)],
    ) -> Vec<u8> {
        block_on(async {
            let file = File::<_, Plain>::open(store.clone(), *root).await.unwrap();
            let mut out = Vec::new();
            for &(offset, len) in ranges {
                let end = offset + u64::try_from(len).unwrap();
                let bytes = file.read().range(offset..end).collect(u64::MAX).await.unwrap();
                out.extend_from_slice(&bytes);
            }
            out
        })
    }
}

/// Whole-file read through the streaming completion-order download path:
/// frames land in a positional sink as they finish, no ordered drain.
pub fn streaming_download_unordered<S: BenchStore>(store: &S, root: &ChunkAddress) -> Vec<u8> {
    block_on(async {
        let file = File::<_, Plain>::open(store.clone(), *root).await.unwrap();
        let mut sink = MemSink::new();
        file.download().run(&mut sink).await.unwrap();
        // Moves the backing buffer out of the sink; no copy is charged.
        Vec::from(sink)
    })
}

/// Deprecated primitives::file splitter and joiner.
#[derive(Clone, Copy, Debug)]
pub struct FileLegacy;

impl FilePipeline for FileLegacy {
    const NAME: &'static str = "file-legacy";

    #[allow(deprecated)]
    fn split<S: BenchStore>(store: &S, data: &[u8]) -> ChunkAddress {
        use std::io::Write;

        use nectar_primitives::file::Splitter;

        block_on(async {
            let mut splitter: Splitter = Splitter::new(u64::try_from(data.len()).unwrap());
            splitter.write_all(data).unwrap();
            let (root, chunks) = splitter.finish().unwrap();
            for content in chunks {
                // Seal once, matching the streaming engine's trust posture.
                let chunk = content.seal::<StandardChunkSet>();
                ChunkPut::put(store, chunk).await.unwrap();
            }
            root
        })
    }

    #[allow(deprecated)]
    fn join<S: BenchStore>(store: &S, root: &ChunkAddress) -> Vec<u8> {
        block_on(async { nectar_primitives::file::join(store.clone(), *root).await.unwrap() })
    }

    #[allow(deprecated)]
    fn join_depth<S: BenchStore>(store: &S, root: &ChunkAddress, depth: u16) -> Vec<u8> {
        use nectar_primitives::file::Joiner;

        block_on(async {
            let joiner = Joiner::new(store.clone(), *root)
                .await
                .unwrap()
                .with_concurrency(usize::from(depth));
            joiner.read_all().await.unwrap()
        })
    }

    #[allow(deprecated)]
    fn split_encrypted<S: BenchStore>(store: &S, data: &[u8]) -> EncryptedChunkRef {
        use std::io::Write;

        use nectar_primitives::file::EncryptedSplitter;

        block_on(async {
            let mut splitter: EncryptedSplitter =
                EncryptedSplitter::new(u64::try_from(data.len()).unwrap());
            splitter.write_all(data).unwrap();
            let (root, chunks) = splitter.finish().unwrap();
            for content in chunks {
                let chunk = content.seal::<StandardChunkSet>();
                ChunkPut::put(store, chunk).await.unwrap();
            }
            root
        })
    }

    #[allow(deprecated)]
    fn join_encrypted<S: BenchStore>(store: &S, root: &EncryptedChunkRef) -> Vec<u8> {
        use nectar_primitives::file::EncryptedJoiner;

        block_on(async {
            let joiner = EncryptedJoiner::new(store.clone(), root.clone()).await.unwrap();
            joiner.read_all().await.unwrap()
        })
    }

    #[allow(deprecated)]
    fn read_ranges<S: BenchStore>(
        store: &S,
        root: &ChunkAddress,
        ranges: &[(u64, usize)],
    ) -> Vec<u8> {
        use nectar_primitives::file::Joiner;

        block_on(async {
            let joiner = Joiner::new(store.clone(), *root).await.unwrap();
            let mut out = Vec::new();
            for &(offset, len) in ranges {
                out.extend_from_slice(&joiner.read_range(offset, len).await.unwrap());
            }
            out
        })
    }
}
