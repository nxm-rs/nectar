//! Round trips through the one handle: `save` then `load`, `collect` and
//! `open`, over both reference widths.

use std::sync::Arc;
use std::vec::Vec;

use nectar_primitives::chunk::{AnyChunkSet, ChunkAddress, ContentOnlyChunkSet};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkRef, EntryRef};
use nectar_testing::run;

use super::{TINY, fill};
use crate::config::Window;
use crate::handle::{File, Policy};
use crate::read::{CollectError, LoadError};
use crate::sink::{DataSink, MemSink, MemSinkError};

type Store = MemoryStore<AnyChunkSet<TINY>>;

/// Sink recording every positional write, so a load's tiling is observable
/// and not just its final bytes.
#[derive(Debug, Default)]
struct RecordingSink {
    inner: MemSink,
    writes: Vec<(u64, usize)>,
}

impl DataSink for RecordingSink {
    type Error = MemSinkError;

    fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<(), Self::Error> {
        self.writes.push((offset, data.len()));
        self.inner.write_at(offset, data)
    }
}

/// Save `data` plain and hand back the wire reference plus a read handle
/// over the same chunks.
fn saved(data: &[u8]) -> (EntryRef, File<ContentGet<Arc<Store>>, TINY>) {
    let store = Arc::new(Store::new());
    let root = run(File::<_, TINY>::new(Arc::clone(&store), Policy::DEFAULT).save(data)).unwrap();
    (
        EntryRef::Plain(ChunkRef::new(root)),
        File::new(ContentGet::new(store), Policy::DEFAULT),
    )
}

#[test]
fn a_load_tiles_the_whole_file_exactly_once() {
    for size in [
        0usize,
        1,
        TINY - 1,
        TINY,
        TINY + 1,
        8 * TINY,
        33 * TINY + 17,
    ] {
        let data = fill(size);
        let (root, file) = saved(&data);
        let mut sink = RecordingSink::default();
        let written = run(file.load(root, &mut sink)).unwrap();
        assert_eq!(written, size as u64, "written diverged at {size}");
        assert_eq!(sink.inner.as_ref(), &data[..], "bytes diverged at {size}");

        // Every write lands once and the writes tile the file gaplessly.
        let mut spans = sink.writes.clone();
        spans.sort_by_key(|(offset, _)| *offset);
        let mut cursor = 0u64;
        for (offset, len) in spans {
            assert_eq!(offset, cursor, "writes must tile the file once at {size}");
            cursor += len as u64;
        }
        assert_eq!(cursor, size as u64);
    }
}

#[test]
fn a_ranged_load_writes_range_relative_offsets() {
    let data = fill(20 * TINY + 7);
    let (root, file) = saved(&data);
    let mut sink = RecordingSink::default();
    let written = run(file.load_range(root, 300..5_000, &mut sink)).unwrap();
    assert_eq!(written, 4_700);
    assert_eq!(sink.inner.as_ref(), &data[300..5_000]);
    assert!(
        sink.writes.iter().all(|(offset, _)| *offset < 4_700),
        "a ranged load must write range-relative offsets"
    );
}

#[test]
fn a_collect_bound_is_refused_before_any_body_fetch() {
    let data = fill(20 * TINY + 11);
    let (root, file) = saved(&data);
    let error = run(file.collect(root, data.len() as u64 - 1)).unwrap_err();
    assert!(
        matches!(error, CollectError::TooLarge { len, max }
            if len == data.len() as u64 && max == data.len() as u64 - 1),
        "got {error:?}"
    );
}

#[test]
fn a_reader_seeks_within_the_clipped_range() {
    let data = fill(40 * TINY + 21);
    let (root, file) = saved(&data);
    let mut reader = run(file.open_range(root, 100..4_000)).unwrap();
    assert_eq!(reader.len(), data.len() as u64);
    assert_eq!(reader.effective_len(), 3_900);

    let mut buf = [0u8; 64];
    assert_eq!(run(reader.read(&mut buf)).unwrap(), 64);
    assert_eq!(&buf[..], &data[100..164]);

    reader.seek(2_000).unwrap();
    assert_eq!(reader.position(), 2_000);
    assert_eq!(run(reader.read(&mut buf)).unwrap(), 64);
    assert_eq!(&buf[..], &data[2_100..2_164]);

    // Seeking to the effective length is legal; past it is typed.
    reader.seek(3_900).unwrap();
    assert_eq!(run(reader.read(&mut buf)).unwrap(), 0);
    assert!(reader.seek(3_901).is_err());
}

#[test]
fn a_policy_window_reaches_the_walk() {
    let data = fill(40 * TINY);
    let store = Arc::new(Store::new());
    let root =
        run(File::<_, TINY>::new(Arc::clone(&store), Policy::DEFAULT).save(&data[..])).unwrap();
    let file = File::<_, TINY>::new(
        ContentGet::new(store),
        Policy::DEFAULT.with_window(Window::new(2).unwrap()),
    );
    let mut reader = run(file.open(EntryRef::Plain(ChunkRef::new(root)))).unwrap();
    let bytes = run(async {
        let mut out = Vec::new();
        while let Some(segment) = reader.next_segment().await {
            out.extend_from_slice(&segment.unwrap());
        }
        out
    });
    assert_eq!(bytes, data);
    assert!(
        reader.stats().peak_occupancy <= 2,
        "occupancy {} burst the policy window",
        reader.stats().peak_occupancy
    );
}

/// A sink refusing every write: the failure is typed and carries the offset,
/// never a panic.
#[derive(Debug)]
struct DeadSink;

impl DataSink for DeadSink {
    type Error = std::io::Error;

    fn write_at(&mut self, _offset: u64, _data: &[u8]) -> Result<(), Self::Error> {
        Err(std::io::Error::other("sink gone"))
    }
}

#[test]
fn a_sink_failure_is_typed_with_its_offset() {
    let data = fill(4 * TINY);
    let (root, file) = saved(&data);
    let error = run(file.load(root, &mut DeadSink)).unwrap_err();
    assert!(matches!(error, LoadError::Sink { .. }), "got {error:?}");
}

#[test]
fn an_absent_root_fails_the_open_typed() {
    let store: Arc<MemoryStore<ContentOnlyChunkSet<TINY>>> = Arc::new(MemoryStore::new());
    let file = File::<_, TINY>::new(store, Policy::DEFAULT);
    let missing = EntryRef::Plain(ChunkRef::new(ChunkAddress::from([0x5a; 32])));
    let mut sink = MemSink::new();
    assert!(matches!(
        run(file.load(missing, &mut sink)).unwrap_err(),
        LoadError::Open(_)
    ));
}

#[cfg(feature = "encryption")]
#[test]
fn an_encrypted_save_round_trips_through_the_same_handle() {
    let data = fill(17 * TINY + 43);
    let store = Arc::new(Store::new());
    let root =
        run(File::<_, TINY>::new(Arc::clone(&store), Policy::DEFAULT).save_encrypted(&data[..]))
            .unwrap();
    let file = File::<_, TINY>::new(ContentGet::new(store), Policy::DEFAULT);
    let reference = EntryRef::Encrypted(root);
    let mut sink = MemSink::new();
    let written = run(file.load(reference.clone(), &mut sink)).unwrap();
    assert_eq!(written, data.len() as u64);
    assert_eq!(sink.as_ref(), &data[..]);
    assert_eq!(run(file.collect(reference, u64::MAX)).unwrap(), data);
}
