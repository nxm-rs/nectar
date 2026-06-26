//! Closure-driven fold combinators over the async joiner.
//!
//! Thin adapters that let a consumer scan the whole file through a closure
//! without buffering it: [`for_each_chunk`](GenericJoiner::for_each_chunk)
//! visits each leaf out of order at full throughput, and
//! [`try_for_each_window`](GenericJoiner::try_for_each_window) folds contiguous
//! in-order byte runs with bounded memory. Both compose over the existing
//! streaming methods, so neither re-walks the tree.

use core::ops::ControlFlow;

use bytes::Bytes;
use futures::stream::StreamExt;

use super::error::Result;
use super::joiner::GenericJoiner;
use super::mode::JoinMode;
use crate::store::{ChunkGet, MaybeSend};

impl<G, M, const BODY_SIZE: usize> GenericJoiner<G, M, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE> + 'static,
    M: JoinMode + MaybeSend + Sync,
{
    /// Visit each leaf body as it lands, out of order, tagged with its absolute
    /// offset. Peak memory is the in-flight width. `f` returns `Break` to stop
    /// early, which drops the stream and cancels in-flight fetches.
    pub async fn for_each_chunk<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(u64, &Bytes) -> ControlFlow<()>,
    {
        let s = self.into_offset_stream_chunked();
        futures::pin_mut!(s);
        while let Some(p) = s.next().await {
            let (off, body) = p?;
            if f(off, &body).is_break() {
                break;
            }
        }
        Ok(())
    }

    /// Fold contiguous in-order byte runs through `f` without buffering the
    /// file. `window` bounds peak memory (see
    /// [`into_windowed_reader`](Self::into_windowed_reader)). `f` returns
    /// `Break(acc)` to stop early with the carried accumulator; otherwise the
    /// final accumulator is returned at stream end.
    pub async fn try_for_each_window<B, F>(self, window: usize, init: B, mut f: F) -> Result<B>
    where
        F: FnMut(B, &[u8]) -> ControlFlow<B, B>,
    {
        let mut reader = self.into_windowed_reader(window);
        let s = reader.stream();
        futures::pin_mut!(s);
        let mut acc = init;
        while let Some(run) = s.next().await {
            let bytes = run?;
            match f(acc, &bytes) {
                ControlFlow::Continue(b) => acc = b,
                ControlFlow::Break(b) => return Ok(b),
            }
        }
        Ok(acc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bmt::DEFAULT_BODY_SIZE;
    use crate::chunk::{AnyChunk, ChunkAddress};
    use crate::file::Joiner;
    use crate::file::split;
    use futures::executor::block_on;
    use std::collections::HashMap;

    fn split_and_store(data: &[u8]) -> (ChunkAddress, HashMap<ChunkAddress, AnyChunk>) {
        let (root, store) = split::<DEFAULT_BODY_SIZE>(data).unwrap();
        (root, store.into_chunks())
    }

    /// `for_each_chunk` visits every leaf exactly once; reassembling by offset
    /// equals `read_all`.
    fn assert_for_each_chunk_matches(data: &[u8], width: usize) {
        let (root, store) = split_and_store(data);
        block_on(async {
            let expected = Joiner::new(store.clone(), root)
                .await
                .unwrap()
                .read_all()
                .await
                .unwrap();

            let joiner = Joiner::new(store, root)
                .await
                .unwrap()
                .with_concurrency(width);
            let total = joiner.size();

            let mut reassembled = vec![0u8; total as usize];
            let mut covered = 0u64;
            let mut seen = std::collections::HashSet::new();
            joiner
                .for_each_chunk(|off, body| {
                    assert!(seen.insert(off), "offset {off} visited more than once");
                    let start = off as usize;
                    reassembled[start..start + body.len()].copy_from_slice(body);
                    covered += body.len() as u64;
                    ControlFlow::Continue(())
                })
                .await
                .unwrap();

            assert_eq!(covered, total, "every byte covered exactly once");
            assert_eq!(reassembled, expected, "reassembly equals read_all");
            assert_eq!(reassembled, data, "reassembly equals input");
        });
    }

    #[test]
    fn for_each_chunk_reassembles() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123)
            .map(|i| (i % 256) as u8)
            .collect();
        assert_for_each_chunk_matches(b"hello world", 8);
        assert_for_each_chunk_matches(&data, 8);
        // width 1 (degenerate concurrent path) still visits every leaf.
        assert_for_each_chunk_matches(&data, 1);
    }

    #[test]
    fn for_each_chunk_break_stops_early() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, store) = split_and_store(&data);
        block_on(async {
            let joiner = Joiner::new(store, root).await.unwrap();
            let mut visited = 0usize;
            let res = joiner
                .for_each_chunk(|_off, _body| {
                    visited += 1;
                    if visited == 2 {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .await;
            assert!(res.is_ok(), "early break returns Ok");
            assert_eq!(visited, 2, "stops on the breaking leaf");
        });
    }

    /// Concatenating the in-order runs equals the file.
    fn assert_window_concat_matches(data: &[u8], window: usize) {
        let (root, store) = split_and_store(data);
        block_on(async {
            let joiner = Joiner::new(store, root).await.unwrap();
            let out = joiner
                .try_for_each_window(window, Vec::new(), |mut acc: Vec<u8>, run| {
                    acc.extend_from_slice(run);
                    ControlFlow::Continue(acc)
                })
                .await
                .unwrap();
            assert_eq!(out, data, "window concat equals file (window {window})");
        });
    }

    #[test]
    fn try_for_each_window_concat_equals_file() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 321)
            .map(|i| (i % 256) as u8)
            .collect();
        assert_window_concat_matches(&data, 16);
        // window 1 is the tightest in-order path.
        assert_window_concat_matches(&data, 1);
    }

    #[test]
    fn try_for_each_window_byte_count_equals_size() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 4 + 7)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, store) = split_and_store(&data);
        block_on(async {
            let joiner = Joiner::new(store, root).await.unwrap();
            let size = joiner.size();
            let count = joiner
                .try_for_each_window(8, 0u64, |acc, run| {
                    ControlFlow::Continue(acc + run.len() as u64)
                })
                .await
                .unwrap();
            assert_eq!(count, size, "folded byte count equals size()");
        });
    }

    #[test]
    fn try_for_each_window_break_returns_accumulator() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, store) = split_and_store(&data);
        block_on(async {
            let joiner = Joiner::new(store, root).await.unwrap();
            // Stop once at least one body has been seen; the carried count comes
            // back through `Break`.
            let count = joiner
                .try_for_each_window(8, 0u64, |acc, _run| {
                    let acc = acc + 1;
                    ControlFlow::Break(acc)
                })
                .await
                .unwrap();
            assert_eq!(count, 1, "break carries the accumulator and stops");
        });
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::file::EncryptedJoiner;
        use crate::file::split_encrypted;

        fn enc_split_and_store(
            data: &[u8],
        ) -> (
            crate::chunk::encryption::EncryptedChunkRef,
            HashMap<ChunkAddress, AnyChunk>,
        ) {
            let (root_ref, store) = split_encrypted::<DEFAULT_BODY_SIZE>(data).unwrap();
            (root_ref, store.into_chunks())
        }

        #[test]
        fn for_each_chunk_reassembles_encrypted() {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3 + 123)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root_ref, store) = enc_split_and_store(&data);
            block_on(async {
                let expected = EncryptedJoiner::new(store.clone(), root_ref.clone())
                    .await
                    .unwrap()
                    .read_all()
                    .await
                    .unwrap();

                let joiner = EncryptedJoiner::new(store, root_ref).await.unwrap();
                let total = joiner.size();
                let mut reassembled = vec![0u8; total as usize];
                let mut seen = std::collections::HashSet::new();
                joiner
                    .for_each_chunk(|off, body| {
                        assert!(seen.insert(off), "offset {off} visited more than once");
                        let start = off as usize;
                        reassembled[start..start + body.len()].copy_from_slice(body);
                        ControlFlow::Continue(())
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    reassembled, expected,
                    "encrypted reassembly equals read_all"
                );
            });
        }

        #[test]
        fn try_for_each_window_concat_equals_file_encrypted() {
            let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 5 + 321)
                .map(|i| (i % 256) as u8)
                .collect();
            let (root_ref, store) = enc_split_and_store(&data);
            block_on(async {
                let joiner = EncryptedJoiner::new(store, root_ref).await.unwrap();
                let out = joiner
                    .try_for_each_window(16, Vec::new(), |mut acc: Vec<u8>, run| {
                        acc.extend_from_slice(run);
                        ControlFlow::Continue(acc)
                    })
                    .await
                    .unwrap();
                assert_eq!(out, data, "encrypted window concat equals file");
            });
        }
    }
}
