//! Facade oracles: differential equality against the legacy read paths over
//! both reference widths, clip-with-effective-length ranges, seek, and the
//! runtime width dispatch.
#![allow(deprecated)]

use std::boxed::Box;
use std::string::{String, ToString};
use std::vec;
use std::vec::Vec;

use bytes::Bytes;
use futures::executor::block_on;
use nectar_primitives::chunk::encryption::{EncryptedChunkRef, EncryptionKey};
use nectar_primitives::chunk::{
    AnyChunk, AnyChunkSet, Chunk, ChunkAddress, ChunkOps, ChunkRef, ContentChunk,
};
use nectar_primitives::file::{ChunkGetExt, join, split, split_encrypted};
use nectar_primitives::store::{ChunkStoreError, MemoryStore, TrustedGet};
use nectar_primitives::{EntryRef, transcrypt};

use super::{AnyFile, File, FileReader, OpenError, SeekPastEnd};
use crate::config::Window;
use crate::geometry::Mode;
use crate::walk::{DecodeError, Encrypted, Plain, WalkError, WalkMode};

/// Tiny body size shared with the legacy joiner tests: fan-out 8 plain and 4
/// encrypted, so small files already build deep trees.
const TINY: usize = 256;

type TinyStore = MemoryStore<AnyChunkSet<TINY>>;

/// Distinct byte per file position so slices are position-sensitive.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

/// Sizes crossing every geometry edge: empty, single leaf, both branch
/// boundaries (8 plain, 4 encrypted), and a multi-level interior.
fn edge_sizes() -> Vec<usize> {
    vec![
        0,
        1,
        TINY - 1,
        TINY,
        TINY + 1,
        4 * TINY,
        4 * TINY + 1,
        8 * TINY,
        8 * TINY + 3,
        33 * TINY + 17,
    ]
}

fn drain_reader<S, M>(reader: &mut FileReader<S, M, TINY>) -> Vec<u8>
where
    S: TrustedGet<AnyChunkSet<TINY>, Error = ChunkStoreError> + Clone + 'static,
    M: WalkMode,
{
    block_on(async {
        let mut out = Vec::new();
        let mut buf = [0u8; 97];
        loop {
            let n = reader.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        out
    })
}

#[test]
fn plain_reader_matches_legacy_join() {
    for len in edge_sizes() {
        let data = fill(len);
        let (root, store) = split::<TINY>(&data).unwrap();
        let legacy = block_on(join(&store, root)).unwrap();
        assert_eq!(legacy, data, "legacy oracle diverged at {len}");

        for window in [1u16, 4] {
            let file = block_on(File::<_, Plain, TINY>::open(store.clone(), root)).unwrap();
            assert_eq!(file.len(), len as u64);
            assert_eq!(file.is_empty(), len == 0);
            let mut reader = file.read().window(Window::new(window).unwrap()).build();
            assert_eq!(reader.effective_len(), len as u64);
            assert_eq!(drain_reader(&mut reader), legacy, "diverged at {len}");
            assert_eq!(reader.position(), len as u64);
        }
    }
}

#[test]
fn encrypted_reader_matches_legacy_join() {
    for len in edge_sizes() {
        let data = fill(len);
        let (root_ref, store) = split_encrypted::<TINY>(&data).unwrap();
        let legacy = block_on(join(&store, root_ref.clone())).unwrap();
        assert_eq!(legacy, data, "legacy oracle diverged at {len}");

        for window in [1u16, 4] {
            let file = block_on(File::<_, Encrypted, TINY>::open_encrypted(
                store.clone(),
                root_ref.clone(),
            ))
            .unwrap();
            assert_eq!(file.len(), len as u64);
            let mut reader = file.read().window(Window::new(window).unwrap()).build();
            assert_eq!(drain_reader(&mut reader), legacy, "diverged at {len}");
        }
    }
}

#[test]
fn range_reads_match_legacy_read_range_both_widths() {
    let len = 33 * TINY + 17;
    let data = fill(len);
    let span = len as u64;
    let ranges = [
        0..10u64,
        0..span,
        100..3 * TINY as u64,
        TINY as u64..TINY as u64,
        255..513,
        511..(TINY as u64) * 9 + 1,
        span - 1..span,
        span / 2..span,
    ];

    let (plain_root, plain_store) = split::<TINY>(&data).unwrap();
    let (enc_root, enc_store) = split_encrypted::<TINY>(&data).unwrap();
    let plain_joiner = block_on(plain_store.clone().joiner(plain_root)).unwrap();
    let enc_joiner = block_on(enc_store.clone().joiner(enc_root.clone())).unwrap();
    let plain_file = block_on(File::<_, Plain, TINY>::open(plain_store, plain_root)).unwrap();
    let enc_file = block_on(File::<_, Encrypted, TINY>::open_encrypted(
        enc_store, enc_root,
    ))
    .unwrap();

    for range in ranges {
        let want = usize::try_from(range.end - range.start).unwrap();
        let legacy_plain = block_on(plain_joiner.read_range(range.start, want)).unwrap();
        let legacy_enc = block_on(enc_joiner.read_range(range.start, want)).unwrap();
        assert_eq!(legacy_plain, legacy_enc, "oracles diverged for {range:?}");

        let mut reader = plain_file.read().range(range.clone()).build();
        assert_eq!(reader.effective_len(), range.end - range.start);
        assert_eq!(drain_reader(&mut reader), legacy_plain, "plain {range:?}");

        let mut reader = enc_file
            .read()
            .range(range.clone())
            .window(Window::new(3).unwrap())
            .build();
        assert_eq!(drain_reader(&mut reader), legacy_enc, "encrypted {range:?}");
    }
}

#[test]
fn out_of_file_ranges_clip_to_effective_length() {
    let len = 5 * TINY + 9;
    let data = fill(len);
    let span = len as u64;
    let (root, store) = split::<TINY>(&data).unwrap();
    let file = block_on(File::<_, Plain, TINY>::open(store, root)).unwrap();

    // End past the file clips to the span.
    let mut reader = file.read().range(300..u64::MAX).build();
    assert_eq!(reader.effective_len(), span - 300);
    assert_eq!(drain_reader(&mut reader), &data[300..]);

    // A range entirely past the file is empty, not an error.
    let mut reader = file.read().range(span + 5..span + 50).build();
    assert_eq!(reader.effective_len(), 0);
    assert!(drain_reader(&mut reader).is_empty());

    // An inverted clip (start past end) is empty.
    let mut reader = file.read().range(span..3).build();
    assert_eq!(reader.effective_len(), 0);
    assert!(drain_reader(&mut reader).is_empty());
}

fn run_seek_script<M: WalkMode>(
    mut reader: FileReader<TinyStore, M, TINY>,
    data: &[u8],
    label: &str,
) {
    let len = data.len();
    let script = [
        (0u64, 100usize),
        (5000, 300),
        (37, 64),
        (len as u64 - 1, 50),
        (0, 1),
        (len as u64, 10),
    ];
    for (pos, want) in script {
        reader.seek(pos).unwrap();
        assert_eq!(reader.position(), pos, "{label}: position after seek");
        let mut buf = vec![0u8; want];
        let mut got = 0;
        block_on(async {
            loop {
                let n = reader.read(&mut buf[got..]).await.unwrap();
                if n == 0 {
                    break;
                }
                got += n;
            }
        });
        let expect_end = (pos as usize + want).min(len);
        let expect = &data[(pos as usize).min(len)..expect_end];
        assert_eq!(&buf[..got], expect, "{label}: seek {pos} read {want}");
    }
    // Past the effective length: typed, never clamped, reader survives.
    let err = reader.seek(len as u64 + 1).unwrap_err();
    assert_eq!(
        err,
        SeekPastEnd {
            requested: len as u64 + 1,
            effective_len: len as u64,
        }
    );
    reader.seek(3).unwrap();
    let mut buf = [0u8; 4];
    block_on(async {
        reader.read(&mut buf).await.unwrap();
    });
    assert_eq!(&buf[..], &data[3..7], "{label}: read after failed seek");
}

#[test]
fn seek_reads_match_oracle_slices_both_widths() {
    let data = fill(21 * TINY + 100);
    let (plain_root, plain_store) = split::<TINY>(&data).unwrap();
    let plain_file = block_on(File::<_, Plain, TINY>::open(plain_store, plain_root)).unwrap();
    let (enc_root, enc_store) = split_encrypted::<TINY>(&data).unwrap();
    let enc_file = block_on(File::<_, Encrypted, TINY>::open_encrypted(
        enc_store, enc_root,
    ))
    .unwrap();

    run_seek_script(plain_file.read().build(), &data, "plain");
    run_seek_script(enc_file.read().build(), &data, "encrypted");
}

#[test]
fn any_file_dispatches_on_the_wire_reference_width() {
    let data = fill(9 * TINY + 5);
    let (plain_root, plain_store) = split::<TINY>(&data).unwrap();
    let (enc_root, enc_store) = split_encrypted::<TINY>(&data).unwrap();

    let plain_entry = EntryRef::Plain(ChunkRef::new(plain_root));
    let any = block_on(AnyFile::<_, TINY>::open(plain_store, plain_entry)).unwrap();
    assert_eq!(any.mode(), Mode::Plain);
    assert_eq!(any.len(), data.len() as u64);
    assert_eq!(any.root(), &plain_root);
    let AnyFile::Plain(file) = any else {
        panic!("32-byte reference must open plain");
    };
    assert_eq!(drain_reader(&mut file.read().build()), data);

    let enc_entry = EntryRef::from(enc_root);
    let any = block_on(AnyFile::<_, TINY>::open(enc_store, enc_entry)).unwrap();
    assert_eq!(any.mode(), Mode::Encrypted);
    assert_eq!(any.len(), data.len() as u64);
    let AnyFile::Encrypted(file) = any else {
        panic!("64-byte reference must open encrypted");
    };
    assert_eq!(drain_reader(&mut file.read().build()), data);
}

#[test]
fn stream_tiles_the_range_and_carries_reader_leftovers() {
    let data = fill(13 * TINY + 40);
    let (root, store) = split::<TINY>(&data).unwrap();
    let file = block_on(File::<_, Plain, TINY>::open(store, root)).unwrap();

    // Fresh stream over a mid-file range.
    let range = 100u64..(11 * TINY) as u64;
    let collected = block_on(async {
        use futures::StreamExt;
        let mut stream = file.read().range(range.clone()).stream();
        let mut out = Vec::new();
        while let Some(segment) = stream.next().await {
            let segment: Bytes = segment.unwrap();
            assert!(!segment.is_empty(), "no empty segments");
            out.extend_from_slice(&segment);
        }
        out
    });
    assert_eq!(collected, &data[100..11 * TINY]);

    // A partially consumed reader hands its leftover bytes to the stream.
    let mut reader = file.read().build();
    let mut buf = [0u8; 100];
    block_on(async {
        reader.read(&mut buf).await.unwrap();
    });
    let rest = block_on(async {
        use futures::StreamExt;
        let mut stream = reader.into_stream();
        let mut out = Vec::new();
        while let Some(segment) = stream.next().await {
            out.extend_from_slice(&segment.unwrap());
        }
        out
    });
    assert_eq!(&buf[..], &data[..100]);
    assert_eq!(rest, &data[100..]);
}

#[test]
fn next_segment_is_gapless_and_zero_copy_sized() {
    let data = fill(6 * TINY + 30);
    let (root_ref, store) = split_encrypted::<TINY>(&data).unwrap();
    let file = block_on(File::<_, Encrypted, TINY>::open_encrypted(store, root_ref)).unwrap();
    let mut reader = file.read().build();
    let collected = block_on(async {
        let mut out = Vec::new();
        while let Some(segment) = reader.next_segment().await {
            out.extend_from_slice(&segment.unwrap());
        }
        out
    });
    assert_eq!(collected, data);
    assert_eq!(reader.position(), data.len() as u64);
}

/// Seal one hand-built chunk for the tiny profile.
fn seal(
    content: ContentChunk<TINY>,
) -> (
    ChunkAddress,
    Chunk<nectar_primitives::chunk::Verified, AnyChunkSet<TINY>>,
) {
    let address = *content.address();
    let chunk = Chunk::from_envelope(AnyChunk::from(content)).unwrap();
    (address, chunk)
}

/// Encrypt `span || body` by hand: the span rides counter `TINY / 32`, the
/// body counter zero, padded with zeros to the full body.
fn encrypt_node(key: &EncryptionKey, span: u64, body: &[u8]) -> Vec<u8> {
    let mut wire = vec![0u8; 8 + TINY];
    transcrypt(key, (TINY / 32) as u32, &span.to_le_bytes(), &mut wire[..8]).unwrap();
    let mut padded = vec![0u8; TINY];
    padded[..body.len()].copy_from_slice(body);
    transcrypt(key, 0, &padded, &mut wire[8..]).unwrap();
    wire
}

#[test]
fn truncated_ciphertext_is_a_typed_decode_error() {
    // A 40-byte chunk cannot be an encrypted node: ciphertexts are full-size.
    let (short_addr, short_chunk) = seal(ContentChunk::<TINY>::new(fill(40)).unwrap());
    let child_key = EncryptionKey::from([0x21; 32]);
    let parent_key = EncryptionKey::from([0x42; 32]);

    // Opening a short chunk as an encrypted root fails at open.
    let store = TinyStore::from_chunks([short_chunk.clone()]);
    let err = block_on(File::<_, Encrypted, TINY>::open_encrypted(
        store,
        EncryptedChunkRef::new(short_addr, parent_key.clone()),
    ))
    .unwrap_err();
    assert!(matches!(
        err,
        OpenError::Decode(DecodeError::CiphertextLength {
            len: 40,
            expected: TINY
        })
    ));

    // A valid encrypted parent referencing the short chunk fails mid-walk.
    let span = 2 * TINY as u64;
    let mut refs = Vec::new();
    refs.extend_from_slice(&EncryptedChunkRef::new(short_addr, child_key.clone()).to_bytes());
    refs.extend_from_slice(&EncryptedChunkRef::new(short_addr, child_key).to_bytes());
    let parent_wire = encrypt_node(&parent_key, span, &refs);
    let (parent_addr, parent_chunk) =
        seal(ContentChunk::<TINY>::try_from(Bytes::from(parent_wire)).unwrap());
    let store = TinyStore::from_chunks([short_chunk, parent_chunk]);

    let file = block_on(File::<_, Encrypted, TINY>::open_encrypted(
        store,
        EncryptedChunkRef::new(parent_addr, parent_key),
    ))
    .unwrap();
    assert_eq!(file.len(), span);
    let mut reader = file.read().build();
    let err = block_on(async {
        let mut buf = [0u8; 16];
        reader.read(&mut buf).await.unwrap_err()
    });
    assert!(matches!(
        err,
        WalkError::Decode {
            offset: 0,
            source: DecodeError::CiphertextLength {
                len: 40,
                expected: TINY
            },
        }
    ));
}

#[test]
fn debug_never_leaks_the_decryption_key() {
    let data = fill(2 * TINY);
    let key_free = |rendered: String| {
        // The reader's Debug output must stay structural.
        assert!(!rendered.contains("key"), "{rendered}");
    };
    let (root_ref, store) = split_encrypted::<TINY>(&data).unwrap();
    let file = block_on(File::<_, Encrypted, TINY>::open_encrypted(store, root_ref)).unwrap();
    key_free(std::format!("{file:?}"));
    key_free(std::format!("{:?}", file.read()));
    key_free(std::format!("{:?}", file.read().build()));
    key_free(std::format!("{:?}", file.read().frames()));
    key_free(std::format!("{:?}", file.download()));
}

fn collect_frames<S, M>(mut frames: super::FileFrames<S, M, TINY>) -> Vec<crate::walk::Frame>
where
    S: TrustedGet<AnyChunkSet<TINY>, Error = ChunkStoreError> + Clone + 'static,
    M: WalkMode,
{
    block_on(async {
        use futures::StreamExt;
        let mut out = Vec::new();
        while let Some(frame) = frames.next().await {
            out.push(frame.unwrap());
        }
        out
    })
}

#[test]
fn frames_tile_the_clipped_range_exactly_once() {
    let data = fill(17 * TINY + 29);
    let (root, store) = split::<TINY>(&data).unwrap();
    let file = block_on(File::<_, Plain, TINY>::open(store, root)).unwrap();

    let range = 100u64..(13 * TINY + 7) as u64;
    let builder = file
        .read()
        .range(range.clone())
        .window(Window::new(3).unwrap());
    let mut frames = collect_frames(builder.frames());
    frames.sort_by_key(|frame| frame.offset);

    let mut expect = range.start;
    let mut assembled = Vec::new();
    for frame in &frames {
        assert_eq!(frame.offset, expect, "frames must tile without overlap");
        assert!(!frame.data.is_empty(), "no empty frames");
        assembled.extend_from_slice(&frame.data);
        expect += frame.data.len() as u64;
    }
    assert_eq!(expect, range.end);
    assert_eq!(assembled, &data[100..13 * TINY + 7]);
}

#[test]
fn download_fills_a_sink_and_reports_progress_both_widths() {
    use std::sync::{Arc, Mutex};

    use crate::sink::{DataSink as _, MemSink};

    let data = fill(11 * TINY + 63);
    let (plain_root, plain_store) = split::<TINY>(&data).unwrap();
    let (enc_root, enc_store) = split_encrypted::<TINY>(&data).unwrap();
    let plain_file = block_on(File::<_, Plain, TINY>::open(plain_store, plain_root)).unwrap();
    let enc_file = block_on(File::<_, Encrypted, TINY>::open_encrypted(
        enc_store, enc_root,
    ))
    .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    let mut sink = MemSink::new();
    let written = block_on(
        plain_file
            .download()
            .window(Window::new(4).unwrap())
            .progress(Box::new(move |progress| log.lock().unwrap().push(progress)))
            .run(&mut sink),
    )
    .unwrap();
    assert_eq!(written, data.len() as u64);
    assert_eq!(sink.as_ref(), data);

    let seen = seen.lock().unwrap();
    assert!(!seen.is_empty());
    for pair in seen.windows(2) {
        assert!(pair[0].written < pair[1].written, "monotone progress");
    }
    for progress in seen.iter() {
        assert_eq!(progress.total, data.len() as u64);
    }
    assert_eq!(seen.last().unwrap().written, data.len() as u64);

    // The encrypted width lands the same bytes, and a sink pre-filled with
    // garbage is fully overwritten (idempotent full re-run semantics).
    let mut sink = MemSink::new();
    sink.write_at(0, &vec![0xa5; data.len()]).unwrap();
    let written = block_on(enc_file.download().run(&mut sink)).unwrap();
    assert_eq!(written, data.len() as u64);
    assert_eq!(sink.as_ref(), data);
}

#[test]
fn range_download_writes_range_relative_offsets() {
    use crate::sink::MemSink;

    let data = fill(9 * TINY + 11);
    let (root, store) = split::<TINY>(&data).unwrap();
    let file = block_on(File::<_, Plain, TINY>::open(store, root)).unwrap();

    let range = 300u64..(5 * TINY) as u64;
    let mut sink = MemSink::new();
    let written = block_on(file.download().range(range.clone()).run(&mut sink)).unwrap();
    assert_eq!(written, range.end - range.start);
    assert_eq!(sink.as_ref(), &data[300..5 * TINY]);

    // Clip semantics: an out-of-file range shrinks instead of failing.
    let mut sink = MemSink::new();
    let written = block_on(file.download().range(500..u64::MAX).run(&mut sink)).unwrap();
    assert_eq!(written, data.len() as u64 - 500);
    assert_eq!(sink.as_ref(), &data[500..]);
}

/// Store failing exactly one fetch (the countdown-th), healthy afterwards.
#[derive(Clone)]
struct FailOnce {
    inner: std::sync::Arc<TinyStore>,
    countdown: std::sync::Arc<std::sync::Mutex<Option<usize>>>,
}

impl nectar_primitives::store::ChunkGet<AnyChunkSet<TINY>> for FailOnce {
    type Trust = nectar_primitives::chunk::Verified;
    type Error = ChunkStoreError;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<nectar_primitives::chunk::Verified, AnyChunkSet<TINY>>, ChunkStoreError> {
        let fail = {
            let mut slot = self.countdown.lock().unwrap();
            match slot.as_mut() {
                Some(0) => {
                    *slot = None;
                    true
                }
                Some(left) => {
                    *left -= 1;
                    false
                }
                None => false,
            }
        };
        if fail {
            return Err(ChunkStoreError::Other(
                "transient outage".to_string().into(),
            ));
        }
        nectar_primitives::store::ChunkGet::get(self.inner.as_ref(), address).await
    }
}

#[test]
fn download_restart_after_transient_failure_is_idempotent() {
    use super::DownloadError;
    use crate::sink::MemSink;

    let data = fill(19 * TINY + 41);
    let (root, store) = split::<TINY>(&data).unwrap();
    let store = FailOnce {
        inner: std::sync::Arc::new(store),
        // Let several leaves land before the outage so the failed run
        // leaves partial bytes behind.
        countdown: std::sync::Arc::new(std::sync::Mutex::new(Some(9))),
    };

    let mut sink = MemSink::new();
    let file = block_on(File::<_, Plain, TINY>::open(store, root)).unwrap();
    let err = block_on(
        file.download()
            .window(Window::new(2).unwrap())
            .run(&mut sink),
    )
    .unwrap_err();
    assert!(matches!(err, DownloadError::Walk(WalkError::Fetch { .. })));
    assert!(
        !sink.is_empty() && sink.len() < data.len(),
        "the failed run must stop partway ({} of {})",
        sink.len(),
        data.len(),
    );

    // Restart: the full re-run overwrites the partial bytes idempotently.
    let written = block_on(file.download().run(&mut sink)).unwrap();
    assert_eq!(written, data.len() as u64);
    assert_eq!(sink.as_ref(), data);
}
