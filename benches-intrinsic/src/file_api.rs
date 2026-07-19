//! Unification layer over the two file pipelines.

use core::future::poll_fn;

use futures::executor::block_on;
use nectar_file::{File, Plain, PutWindow, Split};
use nectar_primitives::chunk::{ChunkAddress, StandardChunkSet};
use nectar_primitives::store::ChunkPut;

use crate::store::CountingStore;

/// The common file surface both pipelines answer.
pub trait FilePipeline {
    /// Stable implementation label.
    const NAME: &'static str;

    /// Split `data` into chunks in `store`; the root address.
    fn split(store: &CountingStore, data: &[u8]) -> ChunkAddress;

    /// Join the tree at `root` back into bytes.
    fn join(store: &CountingStore, root: &ChunkAddress) -> Vec<u8>;
}

/// nectar-file streaming pipeline: poll-native split engine, read facade.
#[derive(Clone, Copy, Debug)]
pub struct FileStreaming;

impl FilePipeline for FileStreaming {
    const NAME: &'static str = "file-streaming";

    fn split(store: &CountingStore, data: &[u8]) -> ChunkAddress {
        block_on(async {
            let mut split: Split<CountingStore, Plain> =
                Split::new(store.clone(), PutWindow::DEFAULT);
            let mut rest = data;
            while !rest.is_empty() {
                let taken = poll_fn(|cx| split.poll_write(cx, rest)).await.unwrap();
                rest = &rest[taken..];
            }
            poll_fn(|cx| split.poll_finish(cx)).await.unwrap()
        })
    }

    fn join(store: &CountingStore, root: &ChunkAddress) -> Vec<u8> {
        block_on(async {
            let file = File::<_, Plain>::open(store.clone(), *root).await.unwrap();
            file.collect(u64::MAX).await.unwrap()
        })
    }
}

/// Deprecated primitives::file splitter and joiner.
#[derive(Clone, Copy, Debug)]
pub struct FileLegacy;

impl FilePipeline for FileLegacy {
    const NAME: &'static str = "file-legacy";

    #[allow(deprecated)]
    fn split(store: &CountingStore, data: &[u8]) -> ChunkAddress {
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
    fn join(store: &CountingStore, root: &ChunkAddress) -> Vec<u8> {
        block_on(async { nectar_primitives::file::join(store.clone(), *root).await.unwrap() })
    }
}
