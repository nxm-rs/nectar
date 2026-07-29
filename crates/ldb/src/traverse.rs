//! Dependency traversal: the chunk closure a persisted manifest depends on.
//!
//! Pinning, garbage collection, integrity checks and whole-collection push
//! treat a database as a chunk set. Key iteration yields entry references
//! only; the trie's own node chunks, and the segment chunks a spilled node
//! reassembles from, never surface there. [`AddressStream`] yields that full
//! closure: every node chunk, every segment chunk and each entry's referenced
//! address. An encrypted database walks the same way: each reference carries
//! the key that opens the chunk it names.

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, Stream};
use nectar_governor::BoxFuture;
use nectar_primitives::store::MaybeSync;
use nectar_primitives::{ChunkAddress, ChunkRef};

use crate::format::{Format, V1};
use crate::frontier::{Completion, Frame, Plan, claim, fill};
use crate::node::NodeRef;
use crate::reader::{Reader, ReaderError};
use crate::scan::{Step, flatten};
use crate::store::{NodeGet, materialize_traced};

/// A visited node's completion payload: its steps and the segment chunk
/// addresses its reassembly visited.
type Visited<F, R> = (Vec<Step<F, R>>, Vec<ChunkAddress>);

/// One completed prefetch payload: the fetched child's visit.
type Fetched<F, R> = Completion<Visited<F, R>>;

/// One delivered turn of the walk: an address or a non-terminal fault.
type Turn = Result<ChunkAddress, ReaderError>;

/// What visiting the top frame's next step resolves to, computed under a
/// short borrow so the stack push never overlaps it.
enum Advance<R: NodeRef> {
    /// The frame is spent; drop it and resume its parent.
    Pop,
    /// The step is consumed; a reference entry yields its address.
    Step(Option<ChunkAddress>),
    /// Descend into the referenced child, claiming the prefetch landed under
    /// `seq` once tagged.
    Descend {
        /// The child's reference.
        reference: R,
        /// The prefetch sequence id, once the window launched one.
        seq: Option<usize>,
    },
}

/// Depth-first stream of every chunk address a persisted manifest depends
/// on: node chunks, spilled segment chunks and each entry's referenced
/// address.
///
/// Delivery order is fixed by the trie: a node's own chunk, its segment
/// chunks in directory order, then its steps in ascending key order, with a
/// referenced subtree streamed in full at its key position. Shared subtrees
/// repeat, matching the serial walk.
///
/// Referenced children ahead of the walk are prefetched with a sliding
/// window of at most [`Format::READ_AHEAD`] fetches in flight, so the walk
/// retains O(depth) frames at the serial fetch count. Cancel-safe: all
/// progress lives in `self`, and a step is consumed only once its fetch has
/// completed, so a dropped [`next`](Self::next) future loses no addresses.
///
/// Every fault is non-terminal (a failed descent replays), so a fault rides
/// the delivered turn rather than ending the walk. The walk advances inside
/// [`admit`](Self::admit), where the launch a fresh descent needs is
/// reachable.
#[derive(Debug)]
pub struct AddressStream<'a, S, F: Format = V1, R: NodeRef = ChunkRef> {
    store: &'a S,
    /// The root reference, pending its visit.
    root: Option<R>,
    done: bool,
    /// Addresses discovered ahead of delivery: a visited node's own chunk
    /// and its segment chunks.
    pending: VecDeque<ChunkAddress>,
    /// One frame per referenced hop on the current path.
    stack: Vec<Frame<F, R>>,
    /// Node fetches launched ahead of the walk.
    in_flight: FuturesUnordered<BoxFuture<'a, Fetched<F, R>>>,
    /// Completions that arrived before the descent awaiting them; drained by
    /// sequence id and bounded with the in-flight set by the window.
    ready: Vec<Fetched<F, R>>,
    /// The next fetch sequence id to hand out.
    next_seq: usize,
    /// The turn advanced to under `admit`, awaiting hand-over.
    staged: Option<Turn>,
}

impl<'a, S, F, R> AddressStream<'a, S, F, R>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    /// Stage the next turn, then launch the read-ahead the walk needs.
    ///
    /// Staging first is what makes a fresh descent reachable: the fill only
    /// launches what the staged walk position asks for.
    fn admit(&mut self) {
        if self.staged.is_none() {
            self.staged = self.advance();
        }
        let store = self.store;
        fill(
            F::READ_AHEAD,
            self.ready.len(),
            &mut self.next_seq,
            &mut self.stack,
            &mut self.in_flight,
            |_base, step| {
                let Step::Ref { reference, .. } = step else {
                    return Plan::Skip;
                };
                let reference = reference.clone();
                Plan::Fetch(async move {
                    materialize_traced::<S, F, R>(store, &reference)
                        .await
                        .map(|(node, segments)| (flatten(&node, false), segments))
                        .map_err(ReaderError::from)
                })
            },
        );
    }

    /// One poll of the bounded-admission walk: admit, hand over a staged
    /// turn, else fold one completion. `None` ends the walk.
    ///
    /// All state lives in `self`, so a dropped poll replays.
    fn poll_turn(&mut self, cx: &mut Context<'_>) -> Poll<Option<Turn>> {
        loop {
            self.admit();
            if let Some(turn) = self.staged.take() {
                return Poll::Ready(Some(turn));
            }
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some(completion)) => self.ready.push(completion),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Advance the walk to its next deliverable turn: deliver queued
    /// addresses, pop spent frames, and descend once a child's fetch has
    /// landed. `None` parks the walk on the head fetch the fill launches.
    fn advance(&mut self) -> Option<Turn> {
        loop {
            if let Some(address) = self.pending.pop_front() {
                return Some(Ok(address));
            }
            let advance = match self.stack.last_mut() {
                None => return None,
                Some(frame) => match frame.steps.get(frame.index) {
                    None => Advance::Pop,
                    Some(Step::Value { entry, .. }) => {
                        frame.index = frame.index.saturating_add(1);
                        Advance::Step(entry.address().copied())
                    }
                    Some(Step::Ref { reference, .. }) => Advance::Descend {
                        reference: reference.clone(),
                        seq: frame.tag(frame.index),
                    },
                },
            };
            match advance {
                Advance::Pop => {
                    self.stack.pop();
                }
                Advance::Step(address) => {
                    if let Some(address) = address {
                        return Some(Ok(address));
                    }
                }
                Advance::Descend { reference, seq } => {
                    let seq = seq?;
                    let result = claim(&mut self.ready, seq)?;
                    match result {
                        Ok((steps, segments)) => {
                            // The step is consumed only now, so a cancelled or
                            // failed fetch replays the same descent.
                            if let Some(frame) = self.stack.last_mut() {
                                frame.index = frame.index.saturating_add(1);
                            }
                            self.enter(*reference.address(), segments, steps);
                        }
                        Err(error) => {
                            // The launch is spent; untag so the replay
                            // relaunches the fetch.
                            if let Some(frame) = self.stack.last_mut() {
                                let index = frame.index;
                                frame.clear_tag(index);
                            }
                            return Some(Err(error));
                        }
                    }
                }
            }
        }
    }

    /// Record a visited node: queue its own chunk and its segment chunks for
    /// delivery, and push its steps for descent.
    fn enter(
        &mut self,
        address: ChunkAddress,
        segments: Vec<ChunkAddress>,
        steps: Vec<Step<F, R>>,
    ) {
        self.pending.push_back(address);
        self.pending.extend(segments);
        self.stack.push(Frame::new(Bytes::new(), steps, 0));
    }

    /// A stream positioned before its root visit.
    fn start(store: &'a S, root: R) -> Self {
        Self {
            store,
            root: Some(root),
            done: false,
            pending: VecDeque::new(),
            stack: Vec::new(),
            in_flight: FuturesUnordered::new(),
            ready: Vec::new(),
            next_seq: 0,
            staged: None,
        }
    }

    /// The next address in the closure, or `None` when the walk is done.
    pub async fn next(&mut self) -> Result<Option<ChunkAddress>, ReaderError> {
        if self.done {
            return Ok(None);
        }
        if let Some(root) = &self.root {
            let address = *root.address();
            let (node, segments) = materialize_traced::<S, F, R>(self.store, root).await?;
            self.root = None;
            self.enter(address, segments, flatten(&node, true));
        }
        let Some(turn) = poll_fn(|cx| self.poll_turn(cx)).await else {
            self.done = true;
            return Ok(None);
        };
        turn.map(Some)
    }
}

impl<S, F, R> Reader<S, F, R>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    /// Every chunk address the database rooted at `root` depends on, in
    /// depth-first key order.
    #[must_use]
    pub fn addresses(&self, root: &R) -> AddressStream<'_, S, F, R> {
        AddressStream::start(self.store(), root.clone())
    }
}

#[cfg(test)]
mod tests {
    use core::pin::pin;
    use core::task::Poll;
    use std::collections::HashSet;
    use std::vec;

    use bytes::Bytes;
    use nectar_primitives::store::{ChunkGet, ContentGet, MemoryStore};
    use nectar_primitives::{
        Chunk, ChunkAddress, ChunkOps, ChunkRef, ContentOnlyChunkSet, Verified,
    };
    use nectar_testing::run;

    use crate::bounded::Prefix;
    use crate::builder::Builder;
    use crate::fork::{Child, ForkTable};
    use crate::format::V1;
    use crate::node::{Node, RootExtension};
    use crate::store::{NodePut, Plaintext};
    use crate::value::{Entry, Key};

    use super::*;

    fn addr(byte: u8) -> ChunkAddress {
        ChunkAddress::new([byte; 32])
    }

    fn entry(byte: u8) -> Entry {
        ChunkRef::new(addr(byte)).into()
    }

    fn prefix(bytes: &[u8]) -> Prefix {
        Prefix::try_from(bytes).unwrap()
    }

    fn drain<S>(mut stream: AddressStream<'_, S>) -> Vec<ChunkAddress>
    where
        S: NodeGet + MaybeSync,
    {
        run(async {
            let mut out = Vec::new();
            while let Some(address) = stream.next().await.unwrap() {
                out.push(address);
            }
            out
        })
    }

    // A two-level manifest: a root fork "a" behind an embedded child holding
    // "aa"/"ab", and "b" behind a referenced leaf holding "ba".
    fn sample(store: &ContentGet<MemoryStore>) -> (ChunkRef, ChunkRef) {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"a"), entry(0xBA).into(), None).unwrap();
        let leaf_addr = run(store.put_node(&Node::new(None, leaf), &Plaintext)).unwrap();

        let mut embedded = ForkTable::new();
        embedded
            .insert(prefix(b"a"), entry(0xAA).into(), None)
            .unwrap();
        embedded
            .insert(prefix(b"b"), entry(0xAB).into(), None)
            .unwrap();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), Child::Embedded(embedded).into(), None)
            .unwrap();
        forks
            .insert(prefix(b"b"), Child::Ref(leaf_addr).into(), None)
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks), &Plaintext)).unwrap();
        (root, leaf_addr)
    }

    #[test]
    fn streams_nodes_and_entry_addresses_depth_first() {
        let store = ContentGet::new(MemoryStore::default());
        let (root, leaf) = sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        let got = drain(reader.addresses(&root));
        assert_eq!(
            got,
            vec![
                *root.address(),
                addr(0xAA),
                addr(0xAB),
                *leaf.address(),
                addr(0xBA)
            ]
        );
    }

    #[test]
    fn the_root_extension_entry_leads_the_closure() {
        let store = ContentGet::new(MemoryStore::default());
        let root_ext = RootExtension::new(Some(entry(9)), None);
        let mut forks = ForkTable::new();
        forks.insert(prefix(b"k"), entry(1).into(), None).unwrap();
        let root = run(store.put_node(&Node::new(root_ext, forks), &Plaintext)).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            drain(reader.addresses(&root)),
            vec![*root.address(), addr(9), addr(1)]
        );
    }

    #[test]
    fn inline_entries_contribute_no_address() {
        let store = ContentGet::new(MemoryStore::default());
        let mut forks = ForkTable::new();
        forks
            .insert(
                prefix(b"a"),
                Entry::inline(Bytes::from_static(b"v")).unwrap().into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks), &Plaintext)).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(drain(reader.addresses(&root)), vec![*root.address()]);
    }

    #[test]
    fn a_ref64_entry_yields_its_address() {
        let store = ContentGet::new(MemoryStore::default());
        let mut forks = ForkTable::new();
        forks
            .insert(
                prefix(b"a"),
                Entry::from(nectar_primitives::EncryptedChunkRef::new(
                    addr(0x77),
                    nectar_primitives::EncryptionKey::from([0x11; 32]),
                ))
                .into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks), &Plaintext)).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            drain(reader.addresses(&root)),
            vec![*root.address(), addr(0x77)]
        );
    }

    #[test]
    fn a_shared_subtree_repeats_at_each_reference() {
        let store = ContentGet::new(MemoryStore::default());
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"x"), entry(0x33).into(), None).unwrap();
        let leaf_addr = run(store.put_node(&Node::new(None, leaf), &Plaintext)).unwrap();
        let mut forks = ForkTable::new();
        for first in [b"a", b"b"] {
            forks
                .insert(prefix(first), Child::Ref(leaf_addr).into(), None)
                .unwrap();
        }
        let root = run(store.put_node(&Node::new(None, forks), &Plaintext)).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            drain(reader.addresses(&root)),
            vec![
                *root.address(),
                *leaf_addr.address(),
                addr(0x33),
                *leaf_addr.address(),
                addr(0x33)
            ]
        );
    }

    #[test]
    fn a_spilled_node_streams_its_segment_chunks() {
        let store = ContentGet::new(MemoryStore::default());
        let mut builder = Builder::<V1>::new();
        for byte in 0u8..=255 {
            builder.insert(Key::from(&[byte][..]), entry(byte), None);
        }
        let root = *run(builder.build(&store, &Plaintext)).unwrap().root();

        // The expected segment addresses, straight off the root chunk's wire.
        let chunk = store.inner().get(root.address()).unwrap();
        let decoded = Node::<V1>::decode_chunk(chunk.envelope().data()).unwrap();
        let crate::codec::DecodedChunk::Segmented(_, dir) = decoded else {
            panic!("a 256-fork root must spill");
        };
        let segments: Vec<ChunkAddress> = dir
            .descriptors
            .iter()
            .map(|d| *d.reference.address())
            .collect();
        assert!(!segments.is_empty());

        let reader: Reader<_> = Reader::new(&store);
        let got = drain(reader.addresses(&root));
        let mut expected = vec![*root.address()];
        expected.extend(segments);
        expected.extend((0u8..=255).map(addr));
        assert_eq!(got, expected);

        // Completeness: the streamed chunk set is exactly what the build
        // stored, plus the entry addresses that live outside the store.
        let stored: HashSet<ChunkAddress> = store.into_inner().into_chunks().into_keys().collect();
        let streamed: HashSet<ChunkAddress> = got.iter().copied().collect();
        let entries: HashSet<ChunkAddress> = (0u8..=255).map(addr).collect();
        assert_eq!(
            streamed
                .difference(&entries)
                .copied()
                .collect::<HashSet<_>>(),
            stored
        );
    }

    // A database has one structural width, witnessed in every chunk's flags
    // byte, so a plain-typed walk of an encrypted image fails loud rather than
    // reading 64-byte references as 32-byte ones.
    #[cfg(feature = "encryption")]
    #[test]
    fn a_mis_typed_arrival_is_rejected_by_the_width_witness() {
        use nectar_primitives::ChunkRef;

        use crate::encryption::Encrypted;

        let store = ContentGet::new(MemoryStore::default());
        let mut builder = Builder::<V1>::new();
        builder.insert(Key::from(&b"a"[..]), entry(0xA1), None);
        let encrypted = run(builder.build(&store, &Encrypted::<V1>::new(b"secret")))
            .unwrap()
            .root()
            .clone();
        // Same chunk, read as a plaintext database: the ciphertext is not even
        // a manifest preamble.
        let reader: Reader<_> = Reader::new(&store);
        let mut stream = reader.addresses(&ChunkRef::new(*encrypted.address()));
        assert!(run(stream.next()).is_err());
    }

    /// Store wrapper that records fetches and yields once per get, so a
    /// `next` future can be observed mid-fetch.
    #[derive(Clone)]
    struct SlowStore {
        inner: std::sync::Arc<SlowInner>,
    }

    struct SlowInner {
        store: ContentGet<MemoryStore>,
        fetched: std::sync::Mutex<Vec<ChunkAddress>>,
    }

    impl SlowStore {
        fn new(store: ContentGet<MemoryStore>) -> Self {
            Self {
                inner: std::sync::Arc::new(SlowInner {
                    store,
                    fetched: std::sync::Mutex::new(Vec::new()),
                }),
            }
        }

        fn fetched(&self) -> Vec<ChunkAddress> {
            self.inner.fetched.lock().unwrap().clone()
        }
    }

    /// Yield once, waking immediately, so the caller observes a pending poll.
    async fn yield_once() {
        let mut yielded = false;
        poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }

    impl ChunkGet<ContentOnlyChunkSet> for SlowStore {
        type Trust = Verified;
        type Error = <ContentGet<MemoryStore> as ChunkGet<ContentOnlyChunkSet>>::Error;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
            self.inner.fetched.lock().unwrap().push(*address);
            yield_once().await;
            ChunkGet::get(&self.inner.store, address).await
        }
    }

    #[test]
    fn a_dropped_next_future_loses_no_addresses() {
        let store = ContentGet::new(MemoryStore::default());
        let (root, leaf) = sample(&store);
        let slow = SlowStore::new(store);
        let reader: Reader<_> = Reader::new(&slow);
        run(async {
            let mut stream = reader.addresses(&root);
            {
                let mut fut = pin!(stream.next());
                let state = poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx))).await;
                assert!(state.is_pending());
            }
            let mut out = Vec::new();
            while let Some(address) = stream.next().await.unwrap() {
                out.push(address);
            }
            assert_eq!(
                out,
                vec![
                    *root.address(),
                    addr(0xAA),
                    addr(0xAB),
                    *leaf.address(),
                    addr(0xBA)
                ]
            );
        });
    }

    /// A trusted store recording the peak number of concurrent `get` calls.
    struct GatedStore {
        inner: ContentGet<MemoryStore>,
        inflight: core::sync::atomic::AtomicUsize,
        peak: core::sync::atomic::AtomicUsize,
    }

    impl ChunkGet<ContentOnlyChunkSet> for GatedStore {
        type Trust = Verified;
        type Error = <ContentGet<MemoryStore> as ChunkGet<ContentOnlyChunkSet>>::Error;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
            use core::sync::atomic::Ordering;
            let now = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak.fetch_max(now, Ordering::Relaxed);
            yield_once().await;
            let chunk = ChunkGet::get(&self.inner, address).await;
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            chunk
        }
    }

    #[test]
    fn read_ahead_bounds_the_in_flight_node_fetches() {
        use core::sync::atomic::{AtomicUsize, Ordering};

        let inner = ContentGet::new(MemoryStore::default());
        // More top-level subtrees than the read-ahead window has slots, each
        // wide enough to spill into a referenced child, so an unbounded
        // frontier would launch past the cap.
        let mut builder = Builder::<V1>::new();
        for p in 0u8..24 {
            for x in 0u8..44 {
                builder.insert(Key::from(&[p, x][..]), entry(x.wrapping_add(p)), None);
            }
        }
        let root = *run(builder.build(&inner, &Plaintext)).unwrap().root();
        let store = GatedStore {
            inner,
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        };

        let reader: Reader<_> = Reader::new(&store);
        drain(reader.addresses(&root));

        let peak = store.peak.load(Ordering::Relaxed);
        // The window overlapped fetches, and never past the read-ahead cap:
        // the frontier is bounded, not O(width).
        assert!(peak > 1, "read-ahead ran node fetches concurrently, {peak}");
        assert!(
            peak <= V1::READ_AHEAD,
            "peak in-flight {peak} exceeded the read-ahead cap {}",
            V1::READ_AHEAD
        );
        // The frontier is wider than the window, so the cap is what bounds the
        // walk: a lost window would show up here.
        assert_eq!(peak, V1::READ_AHEAD);
    }

    #[test]
    fn the_walk_fetches_exactly_the_chunks_it_names() {
        let store = ContentGet::new(MemoryStore::default());
        let (root, leaf) = sample(&store);
        let slow = SlowStore::new(store);
        let reader: Reader<_> = Reader::new(&slow);
        drain(reader.addresses(&root));
        // Entry addresses are named, never fetched; each node is fetched
        // exactly once, the descent taking the prefetched copy.
        let mut fetched = slow.fetched();
        let mut expected = vec![*root.address(), *leaf.address()];
        fetched.sort_unstable();
        expected.sort_unstable();
        assert_eq!(fetched, expected);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use nectar_primitives::EncryptedChunkRef;

        use crate::encryption::Encrypted;

        use super::*;

        const SECRET: &[u8] = b"correct horse battery staple";

        fn seal() -> Encrypted<'static, V1> {
            Encrypted::new(SECRET)
        }

        /// Drain an encrypted-database address stream.
        fn drain_encrypted<S>(
            mut stream: AddressStream<'_, S, V1, EncryptedChunkRef>,
        ) -> Vec<ChunkAddress>
        where
            S: NodeGet + MaybeSync,
        {
            run(async {
                let mut out = Vec::new();
                while let Some(address) = stream.next().await.unwrap() {
                    out.push(address);
                }
                out
            })
        }

        // The closure of an encrypted database has the plaintext shape: the
        // root chunk, then each entry and referenced subtree in key order,
        // every hop opened by the key its own reference carries.
        #[test]
        fn an_encrypted_database_streams_the_same_closure_shape() {
            let store = ContentGet::new(MemoryStore::default());
            let mut child = Node::<V1, EncryptedChunkRef>::empty();
            child
                .forks_mut()
                .insert(prefix(b"x"), entry(0x11).into(), None)
                .unwrap();
            let child_ref = run(store.put_node(&child, &seal())).unwrap();

            let root_ext = RootExtension::new(Some(entry(9)), None);
            let mut forks = ForkTable::new();
            forks
                .insert(prefix(b"a"), Child::Ref(child_ref.clone()).into(), None)
                .unwrap();
            forks
                .insert(prefix(b"b"), entry(0x22).into(), None)
                .unwrap();
            let root = Node::new(root_ext, forks);
            let root_ref = run(store.put_node(&root, &seal())).unwrap();

            let reader = Reader::<&ContentGet<MemoryStore>, V1, EncryptedChunkRef>::new(&store);
            assert_eq!(
                drain_encrypted(reader.addresses(&root_ref)),
                vec![
                    *root_ref.address(),
                    addr(9),
                    *child_ref.address(),
                    addr(0x11),
                    addr(0x22),
                ]
            );
        }

        // A spilled encrypted node: its segment chunks are ciphertext too, and
        // the walk names each one before the keys it covers.
        #[test]
        fn an_encrypted_spilled_node_streams_its_segment_chunks() {
            let store = ContentGet::new(MemoryStore::default());
            let mut builder = Builder::<V1>::new();
            for byte in 0u8..=255 {
                builder.insert(Key::from(&[byte][..]), entry(byte), None);
            }
            let root = run(builder.build(&store, &seal())).unwrap().root().clone();

            let chunk = store.inner().get(root.address()).unwrap();
            let payload = {
                let mut bytes = chunk.envelope().data().to_vec();
                nectar_primitives::transcrypt_in_place(root.key(), 0, &mut bytes);
                bytes
            };
            let decoded = Node::<V1, EncryptedChunkRef>::decode_chunk(&payload).unwrap();
            let crate::codec::DecodedChunk::Segmented(_, dir) = decoded else {
                panic!("a 256-fork root must spill");
            };
            assert!(dir.descriptors.len() > 1);

            let reader = Reader::<&ContentGet<MemoryStore>, V1, EncryptedChunkRef>::new(&store);
            let mut expected = vec![*root.address()];
            expected.extend(dir.descriptors.iter().map(|d| *d.reference.address()));
            expected.extend((0u8..=255).map(addr));
            assert_eq!(drain_encrypted(reader.addresses(&root)), expected);
        }
    }
}
