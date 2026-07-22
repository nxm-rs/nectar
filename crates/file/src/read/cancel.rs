//! Read-side cancellation battery: poll-N-then-drop over the reader and
//! stream, seek-during-in-flight accounting, and a fetch-effect store
//! proving no effect survives a drop.
//!
//! Local drive harness; a shared workspace testing crate can absorb it once
//! one lands.

use core::future::Future;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};
use std::sync::{Arc, Mutex};
use std::vec::Vec;

use futures::Stream;
use futures::task::noop_waker;
use nectar_primitives::chunk::{AnyChunkSet, Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkStoreError, MemoryStore, TrustedGet};
use nectar_testing::{run, yield_now};

#[cfg(feature = "encryption")]
use crate::testutil::split_encrypted_fixture;
use crate::testutil::split_fixture;

use super::{File, FileReader, FileStream};
use crate::config::Window;
#[cfg(feature = "encryption")]
use crate::walk::Encrypted;
use crate::walk::{Plain, WalkMode};

/// Tiny body size shared with the sibling oracles: deep trees from small
/// files.
const TINY: usize = 256;

/// Polls after which a test declares the pipeline stalled.
const POLL_CAP: usize = 100_000;

type TinyRegistry = AnyChunkSet<TINY>;
type TinyChunk = Chunk<Verified, TinyRegistry>;
type TinyStore = MemoryStore<TinyRegistry>;

/// Distinct byte per file position so slices are position-sensitive.
fn fill(len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect()
}

/// `Pending` forever without a wake; legal because the battery polls
/// manually.
fn park_forever() -> impl Future<Output = ()> {
    struct Park;
    impl Future for Park {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    Park
}

/// Ledger of every observable fetch effect.
#[derive(Clone, Copy, Default)]
struct Effects {
    /// Fetch bodies that began executing.
    started: usize,
    /// Fetches that resolved.
    finished: usize,
    /// Fetches dropped before resolving.
    cancelled: usize,
    /// Effects observed after the seal; must stay zero.
    violations: usize,
    sealed: bool,
}

impl Effects {
    fn in_flight(&self) -> usize {
        self.started - self.finished - self.cancelled
    }
}

/// Scope guard of one fetch body: begun on first poll, finished on
/// resolution, otherwise counted as cancelled on drop.
struct Flight {
    effects: Arc<Mutex<Effects>>,
    done: bool,
}

impl Flight {
    fn begin(effects: &Arc<Mutex<Effects>>) -> Self {
        let mut ledger = effects.lock().unwrap();
        ledger.started += 1;
        if ledger.sealed {
            ledger.violations += 1;
        }
        drop(ledger);
        Self {
            effects: Arc::clone(effects),
            done: false,
        }
    }

    fn finish(mut self) {
        let mut ledger = self.effects.lock().unwrap();
        ledger.finished += 1;
        if ledger.sealed {
            ledger.violations += 1;
        }
        drop(ledger);
        self.done = true;
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        if !self.done {
            self.effects.lock().unwrap().cancelled += 1;
        }
    }
}

/// Store whose every fetch effect is observable; after [`EffectStore::seal`]
/// only cancellation is legal.
#[derive(Clone)]
struct EffectStore {
    chunks: Arc<TinyStore>,
    effects: Arc<Mutex<Effects>>,
    /// Yields before a fetch resolves, keeping the window in flight across
    /// polls.
    delay: usize,
    /// When set, every other address parks forever: polled, never resolved.
    resolve_only: Option<ChunkAddress>,
}

impl EffectStore {
    fn new(chunks: Arc<TinyStore>, delay: usize) -> Self {
        Self {
            chunks,
            effects: Arc::new(Mutex::new(Effects::default())),
            delay,
            resolve_only: None,
        }
    }

    fn parked_all_but(root: ChunkAddress, chunks: Arc<TinyStore>) -> Self {
        Self {
            resolve_only: Some(root),
            ..Self::new(chunks, 0)
        }
    }

    fn snapshot(&self) -> Effects {
        *self.effects.lock().unwrap()
    }

    /// Declare the consumer gone: any later start, progress or completion is
    /// a violation.
    fn seal(&self) {
        self.effects.lock().unwrap().sealed = true;
    }

    fn touch(&self) {
        let mut ledger = self.effects.lock().unwrap();
        if ledger.sealed {
            ledger.violations += 1;
        }
    }
}

impl ChunkGet<TinyRegistry> for EffectStore {
    type Trust = Verified;
    type Error = ChunkStoreError;

    async fn get(&self, address: &ChunkAddress) -> Result<TinyChunk, ChunkStoreError> {
        let flight = Flight::begin(&self.effects);
        if self.resolve_only.is_some_and(|only| only != *address) {
            park_forever().await;
        }
        for _ in 0..self.delay {
            self.touch();
            yield_now().await;
        }
        let fetched = ChunkGet::get(self.chunks.as_ref(), address).await;
        flight.finish();
        fetched
    }
}

/// One observed step of a drained endpoint.
enum Step {
    Bytes(Vec<u8>),
    End,
    Wait,
}

/// Outcome of a bounded manual drive.
struct Driven {
    bytes: Vec<u8>,
    finished: bool,
    polls: usize,
}

/// Poll `step` at most `budget` times with a noop waker, concatenating
/// delivered bytes.
fn drive(budget: usize, mut step: impl FnMut(&mut Context<'_>) -> Step) -> Driven {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut bytes = Vec::new();
    let mut polls = 0;
    while polls < budget {
        polls += 1;
        match step(&mut cx) {
            Step::Bytes(run) => bytes.extend_from_slice(&run),
            Step::End => {
                return Driven {
                    bytes,
                    finished: true,
                    polls,
                };
            }
            Step::Wait => {}
        }
    }
    Driven {
        bytes,
        finished: false,
        polls,
    }
}

/// Drive to the end; a stall is a failure, not a hang.
fn drain(step: impl FnMut(&mut Context<'_>) -> Step) -> Driven {
    let driven = drive(POLL_CAP, step);
    assert!(driven.finished, "stalled: no end within {POLL_CAP} polls");
    driven
}

fn reader_step<'a, S, M>(
    reader: &'a mut FileReader<S, M, TINY>,
) -> impl FnMut(&mut Context<'_>) -> Step + 'a
where
    S: TrustedGet<TinyRegistry, Error = ChunkStoreError> + Clone + 'static,
    M: WalkMode,
{
    move |cx| {
        let mut buf = [0u8; 97];
        match reader.poll_read(cx, &mut buf) {
            Poll::Ready(Ok(0)) => Step::End,
            Poll::Ready(Ok(n)) => Step::Bytes(buf[..n].to_vec()),
            Poll::Ready(Err(error)) => panic!("walk error: {error:?}"),
            Poll::Pending => Step::Wait,
        }
    }
}

fn stream_step<'a, S, M>(
    stream: &'a mut FileStream<S, M, TINY>,
) -> impl FnMut(&mut Context<'_>) -> Step + 'a
where
    S: TrustedGet<TinyRegistry, Error = ChunkStoreError> + Clone + 'static,
    M: WalkMode,
{
    move |cx| match Stream::poll_next(Pin::new(&mut *stream), cx) {
        Poll::Ready(Some(Ok(segment))) => {
            assert!(!segment.is_empty(), "no empty segments");
            Step::Bytes(segment.to_vec())
        }
        Poll::Ready(Some(Err(error))) => panic!("walk error: {error:?}"),
        Poll::Ready(None) => Step::End,
        Poll::Pending => Step::Wait,
    }
}

/// The normative poll-N-then-drop contract over the reader: for every cut
/// point, delivered bytes are a position-tracked prefix, the drop cancels
/// exactly the in-flight set with no effect after it, and a fresh reader
/// resumes from the cut.
fn reader_poll_n_drop_battery<M, F>(data: &[u8], chunks: &Arc<TinyStore>, delay: usize, open: F)
where
    M: WalkMode,
    F: Fn(EffectStore) -> File<EffectStore, M, TINY>,
{
    let window = Window::new(4).unwrap();
    let total = {
        let store = EffectStore::new(Arc::clone(chunks), delay);
        let file = open(store);
        let mut reader = file.read().window(window).build();
        let full = drain(reader_step(&mut reader));
        assert_eq!(full.bytes, data, "the full drain must be byte-exact");
        full.polls
    };

    for n in 0..total {
        let store = EffectStore::new(Arc::clone(chunks), delay);
        let file = open(store.clone());
        let mut reader = file.read().window(window).build();
        let cut = drive(n, reader_step(&mut reader));
        assert!(!cut.finished, "{n} polls must stop before the end");
        let delivered = cut.bytes.len();
        assert_eq!(cut.bytes, data[..delivered], "prefix diverged at {n} polls");
        assert_eq!(reader.position(), delivered as u64);

        let before = store.snapshot();
        store.seal();
        drop(reader);
        let after = store.snapshot();
        assert_eq!(after.violations, 0, "an effect ran after the drop at {n}");
        assert_eq!(after.started, before.started);
        assert_eq!(after.finished, before.finished);
        assert_eq!(
            after.cancelled,
            before.in_flight(),
            "the drop must cancel exactly the in-flight set at {n}"
        );
        assert_eq!(after.in_flight(), 0);

        let resume = EffectStore::new(Arc::clone(chunks), delay);
        let file = open(resume.clone());
        let mut reader = file.read().range(delivered as u64..u64::MAX).build();
        let rest = drain(reader_step(&mut reader));
        assert_eq!(rest.bytes, data[delivered..], "resume diverged at {n}");
        assert_eq!(resume.snapshot().cancelled, 0);
        assert_eq!(resume.snapshot().in_flight(), 0);
    }
}

#[test]
fn reader_poll_n_then_drop_delivers_a_prefix_and_cancels_cleanly() {
    let data = fill(21 * TINY + 57);
    let (root, chunks) = split_fixture::<TINY>(&data);
    let chunks = Arc::new(chunks);
    reader_poll_n_drop_battery::<Plain, _>(&data, &chunks, 2, |store| {
        run(File::<_, Plain, TINY>::open(store, root)).unwrap()
    });
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_reader_poll_n_then_drop_matches_the_plain_contract() {
    let data = fill(9 * TINY + 33);
    let (root_ref, chunks) = split_encrypted_fixture::<TINY>(&data);
    let chunks = Arc::new(chunks);
    reader_poll_n_drop_battery::<Encrypted, _>(&data, &chunks, 2, |store| {
        run(File::<_, Encrypted, TINY>::open_encrypted(
            store,
            root_ref.clone(),
        ))
        .unwrap()
    });
}

#[test]
fn stream_poll_n_then_drop_delivers_a_prefix_and_cancels_cleanly() {
    let data = fill(17 * TINY + 21);
    let (root, chunks) = split_fixture::<TINY>(&data);
    let chunks = Arc::new(chunks);
    let window = Window::new(3).unwrap();

    let total = {
        let store = EffectStore::new(Arc::clone(&chunks), 2);
        let file = run(File::<_, Plain, TINY>::open(store, root)).unwrap();
        let mut stream = file.read().window(window).stream();
        let full = drain(stream_step(&mut stream));
        assert_eq!(full.bytes, data, "the full drain must be byte-exact");
        full.polls
    };

    for n in 0..total {
        let store = EffectStore::new(Arc::clone(&chunks), 2);
        let file = run(File::<_, Plain, TINY>::open(store.clone(), root)).unwrap();
        let mut stream = file.read().window(window).stream();
        let cut = drive(n, stream_step(&mut stream));
        assert!(!cut.finished, "{n} polls must stop before the end");
        assert_eq!(
            cut.bytes,
            data[..cut.bytes.len()],
            "segments must concatenate to a prefix at {n} polls"
        );

        let before = store.snapshot();
        store.seal();
        drop(stream);
        let after = store.snapshot();
        assert_eq!(after.violations, 0, "an effect ran after the drop at {n}");
        assert_eq!(after.started, before.started);
        assert_eq!(after.finished, before.finished);
        assert_eq!(
            after.cancelled,
            before.in_flight(),
            "the drop must cancel exactly the in-flight set at {n}"
        );
        assert_eq!(after.in_flight(), 0);
    }
}

#[test]
fn abandoned_read_futures_lose_no_bytes_and_cancel_nothing() {
    let data = fill(13 * TINY + 5);
    let (root, chunks) = split_fixture::<TINY>(&data);
    let store = EffectStore::new(Arc::new(chunks), 3);
    let file = run(File::<_, Plain, TINY>::open(store.clone(), root)).unwrap();
    let mut reader = file.read().window(Window::new(2).unwrap()).build();

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut out = Vec::new();
    let mut abandoned = 0usize;
    let mut finished = false;
    for _ in 0..POLL_CAP {
        let mut buf = [0u8; 97];
        let mut taken = None;
        {
            let mut future = pin!(reader.read(&mut buf));
            for _ in 0..2 {
                if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
                    taken = Some(result.unwrap());
                    break;
                }
            }
            // Dropping the future here abandons a pending read mid flight.
        }
        match taken {
            Some(0) => {
                finished = true;
                break;
            }
            Some(n) => out.extend_from_slice(&buf[..n]),
            None => abandoned += 1,
        }
    }

    assert!(finished, "the read must complete under abandonment");
    assert!(abandoned > 0, "the schedule must abandon reads mid flight");
    assert_eq!(out, data, "no byte may be lost or repeated");
    assert_eq!(reader.position(), data.len() as u64);
    let end = store.snapshot();
    assert_eq!(
        end.cancelled, 0,
        "abandoning a read must not cancel fetches"
    );
    assert_eq!(end.in_flight(), 0);
}

#[test]
fn seek_during_in_flight_drops_the_old_window_and_rewalks() {
    let data = fill(25 * TINY + 9);
    let (root, chunks) = split_fixture::<TINY>(&data);
    let store = EffectStore::new(Arc::new(chunks), 6);
    let file = run(File::<_, Plain, TINY>::open(store.clone(), root)).unwrap();
    let mut reader = file.read().window(Window::new(4).unwrap()).build();

    // Drive until the window is genuinely in flight.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut delivered = Vec::new();
    let mut buf = [0u8; 97];
    for _ in 0..POLL_CAP {
        if store.snapshot().in_flight() >= 3 {
            break;
        }
        match reader.poll_read(&mut cx, &mut buf) {
            Poll::Ready(Ok(n)) => delivered.extend_from_slice(&buf[..n]),
            Poll::Ready(Err(error)) => panic!("walk error: {error:?}"),
            Poll::Pending => {}
        }
    }
    let before = store.snapshot();
    assert!(before.in_flight() >= 3, "the window never got in flight");
    assert_eq!(delivered, data[..delivered.len()]);

    let target = 19 * TINY as u64 + 7;
    reader.seek(target).unwrap();
    let after = store.snapshot();
    assert_eq!(after.started, before.started, "a seek must not fetch");
    assert_eq!(
        after.cancelled,
        before.in_flight(),
        "a seek must drop every in-flight fetch of the old walk"
    );
    assert_eq!(reader.position(), target);
    assert_eq!(reader.stats().fetches, 0, "the re-walk starts cold");

    let rest = drain(reader_step(&mut reader));
    assert_eq!(rest.bytes, data[target as usize..]);
    let end = store.snapshot();
    assert_eq!(end.in_flight(), 0);
    assert_eq!(
        end.cancelled, after.cancelled,
        "nothing cancels after the seek"
    );
    assert_eq!(
        reader.stats().fetches as usize,
        end.started - after.started,
        "every post-seek fetch must belong to the new walk"
    );
}

#[test]
fn seek_to_the_current_position_keeps_the_walk_and_window() {
    let data = fill(15 * TINY + 31);
    let (root, chunks) = split_fixture::<TINY>(&data);
    let store = EffectStore::new(Arc::new(chunks), 2);
    let file = run(File::<_, Plain, TINY>::open(store.clone(), root)).unwrap();
    let mut reader = file.read().window(Window::new(3).unwrap()).build();

    // Deliver past the first frame so the position sits mid frame.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut head = Vec::new();
    let mut buf = [0u8; 97];
    for _ in 0..POLL_CAP {
        if head.len() > TINY + 10 {
            break;
        }
        match reader.poll_read(&mut cx, &mut buf) {
            Poll::Ready(Ok(n)) => head.extend_from_slice(&buf[..n]),
            Poll::Ready(Err(error)) => panic!("walk error: {error:?}"),
            Poll::Pending => {}
        }
    }
    assert!(head.len() > TINY + 10, "the drive never delivered");

    let before = store.snapshot();
    let fetches = reader.stats().fetches;
    reader.seek(reader.position()).unwrap();
    let after = store.snapshot();
    assert_eq!(
        after.cancelled, before.cancelled,
        "an in-place seek must keep the in-flight window"
    );
    assert_eq!(after.started, before.started);
    assert_eq!(
        reader.stats().fetches,
        fetches,
        "the walk survives an in-place seek"
    );

    let rest = drain(reader_step(&mut reader));
    assert_eq!([head, rest.bytes].concat(), data);
}

#[test]
fn drop_with_a_parked_window_cancels_every_fetch() {
    // One branch level: the root resolves, all four leaves park in flight.
    let data = fill(4 * TINY);
    let (root, chunks) = split_fixture::<TINY>(&data);
    let store = EffectStore::parked_all_but(root, Arc::new(chunks));
    let file = run(File::<_, Plain, TINY>::open(store.clone(), root)).unwrap();
    let mut reader = file.read().window(Window::new(4).unwrap()).build();

    let cut = drive(16, reader_step(&mut reader));
    assert!(cut.bytes.is_empty() && !cut.finished);
    let before = store.snapshot();
    assert_eq!(
        before.in_flight(),
        4,
        "the full leaf window must be parked in flight"
    );

    store.seal();
    drop(reader);
    let after = store.snapshot();
    assert_eq!(after.violations, 0, "an effect ran after the drop");
    assert_eq!(after.started, before.started);
    assert_eq!(after.finished, before.finished);
    assert_eq!(after.cancelled, 4);
    assert_eq!(after.in_flight(), 0);
}
