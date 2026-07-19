//! Fuzz arbitrary seek and read sequences on the ordered reader.
//!
//! A fuzzed clip range and op sequence drive one reader over a store built
//! by the streaming split. The model is a cursor over the clipped source
//! slice: every read must deliver the model's bytes at the model's position,
//! a seek past the effective length must fail typed and move nothing, and a
//! final drain from zero must reproduce the whole clipped slice.

#![no_main]

use core::future::poll_fn;
use std::sync::Arc;

use arbitrary::Arbitrary;
use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use nectar_file::{File, Plain, PutWindow, Split};
use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress};
use nectar_primitives::store::MemoryStore;

/// Tiny body size: fan-out 8, so a few KiB already builds a deep tree.
const BODY: usize = 256;
/// Source-length cap; four tree levels at the tiny body size.
const MAX_LEN: usize = 32 * 1024;
/// Op-sequence cap per exec.
const MAX_OPS: usize = 64;
/// Per-read buffer cap, past one body so reads cross leaf boundaries.
const MAX_READ: usize = 700;

/// One fuzzed reader operation.
#[derive(Arbitrary, Debug)]
enum Op {
    /// Seek to a position derived from the raw value; overshoot by design.
    Seek(u64),
    /// Copy up to the given count of bytes into a buffer.
    Read(u16),
    /// Take the next uncopied in-order run.
    Segment,
}

/// Tile `seed` to `copies` repetitions, capped at [`MAX_LEN`] bytes.
fn tile(seed: &[u8], copies: u16) -> Vec<u8> {
    if seed.is_empty() {
        return Vec::new();
    }
    let len = seed
        .len()
        .saturating_mul(usize::from(copies.max(1)))
        .min(MAX_LEN);
    seed.iter().copied().cycle().take(len).collect()
}

/// Whole-buffer streaming split into a shared store.
fn split(data: &[u8]) -> (ChunkAddress, Arc<MemoryStore<AnyChunkSet<BODY>>>) {
    let store = Arc::new(MemoryStore::new());
    let mut split: Split<Arc<MemoryStore<AnyChunkSet<BODY>>>, Plain, BODY> =
        Split::new(Arc::clone(&store), PutWindow::DEFAULT);
    let root = block_on(async {
        let mut buf = data;
        while !buf.is_empty() {
            let n = poll_fn(|cx| split.poll_write(cx, buf))
                .await
                .expect("memory puts never fail");
            buf = &buf[n..];
        }
        poll_fn(|cx| split.poll_finish(cx))
            .await
            .expect("finish over a memory store succeeds")
    });
    (root, store)
}

fn to_usize(value: u64) -> usize {
    usize::try_from(value).expect("model offsets stay within the source cap")
}

fuzz_target!(|input: (Vec<u8>, u16, (u64, u64), Vec<Op>)| {
    let (seed, copies, (range_start, range_end), ops) = input;
    let data = tile(&seed, copies);
    let span = data.len() as u64;

    // Mirror the walk's clip rule so both bounds may overshoot the file.
    let overshoot = span.saturating_add(2 * BODY as u64).saturating_add(1);
    let end_req = range_end % overshoot;
    let start_req = range_start % overshoot;
    let clip_end = end_req.min(span);
    let clip_start = start_req.min(clip_end);
    let window = &data[to_usize(clip_start)..to_usize(clip_end)];
    let eff = clip_end - clip_start;

    let (root, store) = split(&data);
    block_on(async move {
        let file = File::<_, Plain, BODY>::open(store, root)
            .await
            .expect("open must succeed over a complete store");
        let mut reader = file.read().range(start_req..end_req).build();
        assert_eq!(reader.effective_len(), eff, "clip diverged from the model");

        let mut pos: u64 = 0;
        for op in ops.iter().take(MAX_OPS) {
            match op {
                Op::Seek(raw) => {
                    let target = raw % (eff + 2);
                    match reader.seek(target) {
                        Ok(()) => {
                            assert!(target <= eff, "seek accepted a past-end position");
                            pos = target;
                        }
                        Err(error) => {
                            assert!(target > eff, "seek rejected an in-range position");
                            assert_eq!(error.requested, target);
                            assert_eq!(error.effective_len, eff);
                        }
                    }
                }
                Op::Read(len) => {
                    let mut buf = vec![0u8; usize::from(*len) % (MAX_READ + 1)];
                    let n = reader
                        .read(&mut buf)
                        .await
                        .expect("read must succeed over a complete store");
                    assert!(n <= buf.len(), "read overfilled the buffer");
                    if !buf.is_empty() {
                        assert_eq!(n == 0, pos == eff, "zero read must mean end of range");
                    }
                    let expected = &window[to_usize(pos)..to_usize(pos) + n];
                    assert_eq!(&buf[..n], expected, "read bytes diverged from the source");
                    pos += n as u64;
                }
                Op::Segment => match reader.next_segment().await {
                    None => assert_eq!(pos, eff, "segments ended before the range"),
                    Some(run) => {
                        let run = run.expect("segment must succeed over a complete store");
                        let expected = &window[to_usize(pos)..to_usize(pos) + run.len()];
                        assert_eq!(run.as_ref(), expected, "segment diverged from the source");
                        pos += run.len() as u64;
                    }
                },
            }
            assert_eq!(
                reader.position(),
                pos,
                "reader position diverged from the model"
            );
        }

        // Final drain from zero must reproduce the whole clipped slice.
        reader.seek(0).expect("seek to zero is always in range");
        let mut out = Vec::with_capacity(window.len());
        while let Some(run) = reader.next_segment().await {
            out.extend_from_slice(&run.expect("drain must succeed over a complete store"));
        }
        assert_eq!(out, window, "post-seek drain diverged from the source");
    });
});
