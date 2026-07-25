//! Dependency traversal: the chunk closure a persisted manifest depends on.
//!
//! Pinning, garbage collection, integrity checks and whole-collection push
//! treat a manifest as a chunk set. Key iteration yields entry references
//! only; the trie's own node chunks, and the segment chunks a spilled node
//! reassembles from, never surface there. [`AddressStream`] yields that full
//! closure: every node chunk, every segment chunk and each entry's referenced
//! address.

use core::future::Future;
use core::pin::Pin;
use std::collections::VecDeque;

use futures::stream::{FuturesUnordered, StreamExt};
#[cfg(feature = "encryption")]
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::store::MaybeSync;
use nectar_primitives::{ChunkAddress, EncryptionKey};

use crate::format::{Format, V1};
use crate::reader::{Reader, ReaderError};
use crate::scan::{Step, flatten};
use crate::store::{NodeGet, materialize_traced};

/// One visited node's steps and the walk position within them.
#[derive(Debug)]
struct Frame<F: Format> {
    /// The node's steps in ascending key order.
    steps: Vec<Step<F>>,
    /// The next step to visit.
    index: usize,
    /// Per-step prefetch tag, parallel to `steps`: the sequence id a
    /// referenced child was launched under, once the window scheduled it.
    sched: Vec<Option<usize>>,
}

impl<F: Format> Frame<F> {
    /// A fresh frame over `steps` with an empty prefetch tag.
    fn new(steps: Vec<Step<F>>) -> Self {
        let sched = vec![None; steps.len()];
        Self {
            steps,
            index: 0,
            sched,
        }
    }
}

/// A completed node fetch: the sequence id it was launched under, its steps
/// and the segment chunk addresses its reassembly visited.
type Fetched<F> = (
    usize,
    Result<(Vec<Step<F>>, Vec<ChunkAddress>), ReaderError>,
);

/// An in-flight node fetch. Boxed to hold heterogeneous fetch futures in one
/// queue; `Send` on native, unbounded on the single-threaded wasm executor.
#[cfg(not(target_arch = "wasm32"))]
type Fetch<'a, F> = Pin<Box<dyn Future<Output = Fetched<F>> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type Fetch<'a, F> = Pin<Box<dyn Future<Output = Fetched<F>> + 'a>>;

/// The stream's root reference, pending its first visit.
#[derive(Debug)]
enum Root {
    /// A plain root address.
    Plain(ChunkAddress),
    /// An encrypted root reference.
    #[cfg(feature = "encryption")]
    Encrypted(EncryptedChunkRef),
}

/// What visiting the top frame's next step resolves to, computed under a
/// short borrow so the awaited fetch never overlaps it.
enum Advance {
    /// The frame is spent; drop it and resume its parent.
    Pop,
    /// The step is consumed; a reference entry yields its address.
    Step(Option<ChunkAddress>),
    /// Descend into the referenced child, decrypting with `key` when the
    /// record carried one, awaiting the prefetch under `seq` when scheduled.
    Descend {
        /// The child chunk address.
        address: ChunkAddress,
        /// The child's decryption key, from a ref64 record.
        key: Option<EncryptionKey>,
        /// The prefetch sequence id, when the window launched one.
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
/// repeat, matching the serial walk. An encrypted child is opened with the
/// key its record carries; without the `encryption` feature it ends the walk
/// with [`ReaderError::EncryptedChild`].
///
/// Referenced children ahead of the walk are prefetched with a sliding
/// window of at most [`Format::READ_AHEAD`] fetches in flight, so the walk
/// retains O(depth) frames at the serial fetch count. Cancel-safe: all
/// progress lives in `self`, and a step is consumed only once its fetch has
/// completed, so a dropped [`next`](Self::next) future loses no addresses.
#[derive(Debug)]
pub struct AddressStream<'a, S, F: Format = V1> {
    store: &'a S,
    /// The root reference, pending its visit.
    root: Option<Root>,
    /// Addresses discovered ahead of delivery: a visited node's own chunk
    /// and its segment chunks.
    pending: VecDeque<ChunkAddress>,
    /// One frame per referenced hop on the current path.
    stack: Vec<Frame<F>>,
    done: bool,
    /// Node fetches launched by the read-ahead window, awaiting completion.
    inflight: FuturesUnordered<Fetch<'a, F>>,
    /// Completions that arrived before the descent awaiting them; drained by
    /// sequence id. Bounded with `inflight` by the window.
    ready: Vec<Fetched<F>>,
    /// The next fetch sequence id to hand out.
    next_seq: usize,
}

impl<'a, S, F: Format> AddressStream<'a, S, F> {
    /// A stream positioned before its root visit.
    fn start(store: &'a S, root: Root) -> Self {
        Self {
            store,
            root: Some(root),
            pending: VecDeque::new(),
            stack: Vec::new(),
            done: false,
            inflight: FuturesUnordered::new(),
            ready: Vec::new(),
            next_seq: 0,
        }
    }
}

impl<'a, S, F> AddressStream<'a, S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    /// The next address in the closure, or `None` when the walk is done.
    pub async fn next(&mut self) -> Result<Option<ChunkAddress>, ReaderError> {
        loop {
            if let Some(address) = self.pending.pop_front() {
                return Ok(Some(address));
            }
            if self.done {
                return Ok(None);
            }
            if let Some(root) = &self.root {
                let (address, key) = match root {
                    Root::Plain(address) => (*address, None),
                    #[cfg(feature = "encryption")]
                    Root::Encrypted(reference) => {
                        (*reference.address(), Some(reference.key().clone()))
                    }
                };
                let (node, segments) =
                    materialize_traced::<S, F>(self.store, &address, key.as_ref()).await?;
                self.root = None;
                self.enter(address, segments, flatten(&node, true));
                continue;
            }
            self.schedule();
            let advance = match self.stack.last_mut() {
                None => {
                    self.done = true;
                    return Ok(None);
                }
                Some(frame) => match frame.steps.get(frame.index) {
                    None => Advance::Pop,
                    Some(Step::Value { entry, .. }) => {
                        frame.index = frame.index.saturating_add(1);
                        Advance::Step(entry.address().copied())
                    }
                    Some(Step::Ref { addr, .. }) => Advance::Descend {
                        address: *addr,
                        key: None,
                        seq: frame.sched.get(frame.index).copied().flatten(),
                    },
                    #[cfg(feature = "encryption")]
                    Some(Step::Encrypted { reference, .. }) => Advance::Descend {
                        address: *reference.address(),
                        key: Some(reference.key().clone()),
                        seq: frame.sched.get(frame.index).copied().flatten(),
                    },
                    #[cfg(not(feature = "encryption"))]
                    Some(Step::Encrypted { .. }) => return Err(ReaderError::EncryptedChild),
                },
            };
            match advance {
                Advance::Pop => {
                    self.stack.pop();
                }
                Advance::Step(address) => {
                    if let Some(address) = address {
                        return Ok(Some(address));
                    }
                }
                Advance::Descend { address, key, seq } => {
                    let (steps, segments) = self.resolve(seq, address, key).await?;
                    // The step is consumed only now, so a cancelled resolve
                    // replays the same descent.
                    if let Some(frame) = self.stack.last_mut() {
                        frame.index = frame.index.saturating_add(1);
                    }
                    self.enter(address, segments, steps);
                }
            }
        }
    }

    /// Record a visited node: queue its own chunk and its segment chunks for
    /// delivery, and push its steps for descent.
    fn enter(&mut self, address: ChunkAddress, segments: Vec<ChunkAddress>, steps: Vec<Step<F>>) {
        self.pending.push_back(address);
        self.pending.extend(segments);
        self.stack.push(Frame::new(steps));
    }

    /// Fill the read-ahead window: launch node fetches for the referenced
    /// children the walk reaches next, in ascending-key order, until at most
    /// [`Format::READ_AHEAD`] fetches are in flight.
    fn schedule(&mut self) {
        let cap = F::READ_AHEAD;
        let store = self.store;
        // Deepest frame first: that is ascending-key order from the walk, so
        // the child needed soonest is always launched first.
        'outer: for frame in self.stack.iter_mut().rev() {
            let mut index = frame.index;
            while let Some(step) = frame.steps.get(index) {
                if self.inflight.len().saturating_add(self.ready.len()) >= cap {
                    break 'outer;
                }
                let target = match step {
                    Step::Value { .. } => None,
                    Step::Ref { addr, .. } => Some((*addr, None)),
                    #[cfg(feature = "encryption")]
                    Step::Encrypted { reference, .. } => {
                        Some((*reference.address(), Some(reference.key().clone())))
                    }
                    // The walk errors here; nothing beyond it is fetched.
                    #[cfg(not(feature = "encryption"))]
                    Step::Encrypted { .. } => break 'outer,
                };
                if let Some((address, key)) = target
                    && !matches!(frame.sched.get(index), Some(Some(_)))
                {
                    let seq = self.next_seq;
                    self.next_seq = self.next_seq.saturating_add(1);
                    if let Some(slot) = frame.sched.get_mut(index) {
                        *slot = Some(seq);
                    }
                    let fetch: Fetch<'a, F> = Box::pin(async move {
                        let result = materialize_traced::<S, F>(store, &address, key.as_ref())
                            .await
                            .map(|(node, segments)| (flatten(&node, false), segments))
                            .map_err(ReaderError::from);
                        (seq, result)
                    });
                    self.inflight.push(fetch);
                }
                index = index.saturating_add(1);
            }
        }
    }

    /// The steps and segment trace of the child at `address`: take the
    /// prefetch launched under `seq`, buffering earlier-arriving completions,
    /// or fetch directly when the descent was not prefetched.
    async fn resolve(
        &mut self,
        seq: Option<usize>,
        address: ChunkAddress,
        key: Option<EncryptionKey>,
    ) -> Result<(Vec<Step<F>>, Vec<ChunkAddress>), ReaderError> {
        if let Some(seq) = seq {
            loop {
                if let Some(pos) = self.ready.iter().position(|(other, _)| *other == seq) {
                    return self.ready.swap_remove(pos).1;
                }
                match self.inflight.next().await {
                    Some((other, result)) if other == seq => return result,
                    Some(pair) => self.ready.push(pair),
                    // The launched fetch is unaccounted for; fetch directly.
                    None => break,
                }
            }
        }
        let (node, segments) =
            materialize_traced::<S, F>(self.store, &address, key.as_ref()).await?;
        Ok((flatten(&node, false), segments))
    }
}

impl<S, F> Reader<S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    /// Every chunk address the manifest rooted at `root` depends on, in
    /// depth-first key order.
    #[must_use]
    pub fn addresses(&self, root: &ChunkAddress) -> AddressStream<'_, S, F> {
        AddressStream::start(self.store(), Root::Plain(*root))
    }

    /// The same closure over an encrypted manifest, opening each node with
    /// the key its reference carries.
    #[cfg(feature = "encryption")]
    #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
    #[must_use]
    pub fn addresses_encrypted(&self, reference: &EncryptedChunkRef) -> AddressStream<'_, S, F> {
        AddressStream::start(self.store(), Root::Encrypted(reference.clone()))
    }
}

#[cfg(test)]
mod tests {
    use core::task::Poll;
    use std::collections::HashSet;

    use bytes::Bytes;
    #[cfg(not(feature = "encryption"))]
    use nectar_primitives::EncryptedChunkRef;
    use nectar_primitives::store::{ChunkGet, ChunkPut, MemoryStore};
    use nectar_primitives::{Chunk, ChunkAddress, ChunkOps, ChunkRef, ContentChunk};

    use nectar_testing::run;

    use crate::bounded::Prefix;
    use crate::builder::Builder;
    use crate::codec::{SegDesc, SegmentDir, encode_segmented_node};
    use crate::count::SubtreeCount;
    use crate::fork::{Child, ForkTable};
    use crate::format::V1;
    use crate::node::{Node, RootExtension};
    use crate::store::{NodeChunk, NodePut, StoreError};
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

    /// Seal a raw payload as a content chunk and store it.
    fn put_raw(store: &MemoryStore, payload: Vec<u8>) -> ChunkAddress {
        let content = ContentChunk::new(payload).unwrap();
        let chunk: NodeChunk = Chunk::from_envelope(content.into()).unwrap();
        let address = *chunk.address();
        run(ChunkPut::put(store, chunk)).unwrap();
        address
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
    fn sample(store: &MemoryStore) -> (ChunkAddress, ChunkAddress) {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"a"), entry(0xBA).into(), None).unwrap();
        let leaf_addr = run(store.put_node(&Node::new(None, leaf))).unwrap();

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
            .insert(
                prefix(b"b"),
                Child::Ref32(ChunkRef::new(leaf_addr)).into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        (root, leaf_addr)
    }

    #[test]
    fn streams_nodes_and_entry_addresses_depth_first() {
        let store = MemoryStore::default();
        let (root, leaf) = sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        let got = drain(reader.addresses(&root));
        assert_eq!(got, vec![root, addr(0xAA), addr(0xAB), leaf, addr(0xBA)]);
    }

    #[test]
    fn the_root_extension_entry_leads_the_closure() {
        let store = MemoryStore::default();
        let root_ext = RootExtension::new(Some(entry(9)), None);
        let mut forks = ForkTable::new();
        forks.insert(prefix(b"k"), entry(1).into(), None).unwrap();
        let root = run(store.put_node(&Node::new(root_ext, forks))).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(drain(reader.addresses(&root)), vec![root, addr(9), addr(1)]);
    }

    #[test]
    fn inline_entries_contribute_no_address() {
        let store = MemoryStore::default();
        let mut forks = ForkTable::new();
        forks
            .insert(
                prefix(b"a"),
                Entry::inline(Bytes::from_static(b"v")).unwrap().into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(drain(reader.addresses(&root)), vec![root]);
    }

    #[test]
    fn a_ref64_entry_yields_its_address() {
        let store = MemoryStore::default();
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
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(drain(reader.addresses(&root)), vec![root, addr(0x77)]);
    }

    #[test]
    fn a_shared_subtree_repeats_at_each_reference() {
        let store = MemoryStore::default();
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"x"), entry(0x33).into(), None).unwrap();
        let leaf_addr = run(store.put_node(&Node::new(None, leaf))).unwrap();
        let mut forks = ForkTable::new();
        for first in [b"a", b"b"] {
            forks
                .insert(
                    prefix(first),
                    Child::Ref32(ChunkRef::new(leaf_addr)).into(),
                    None,
                )
                .unwrap();
        }
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        assert_eq!(
            drain(reader.addresses(&root)),
            vec![root, leaf_addr, addr(0x33), leaf_addr, addr(0x33)]
        );
    }

    #[test]
    fn a_spilled_node_streams_its_segment_chunks() {
        let store = MemoryStore::default();
        let mut builder = Builder::<V1>::new();
        for byte in 0u8..=255 {
            builder.insert(Key::from(&[byte][..]), entry(byte), None);
        }
        let root = *run(builder.build(&store)).unwrap().root();

        // The expected segment addresses, straight off the root chunk's wire.
        let chunk = store.get(&root).unwrap();
        let decoded = Node::<V1>::decode_chunk(chunk.envelope().data()).unwrap();
        let crate::codec::DecodedChunk::Segmented(_, dir) = decoded else {
            panic!("a 256-fork root must spill");
        };
        let segments: Vec<ChunkAddress> = dir.descriptors.iter().map(|d| d.address).collect();
        assert!(!segments.is_empty());

        let reader: Reader<_> = Reader::new(&store);
        let got = drain(reader.addresses(&root));
        let mut expected = vec![root];
        expected.extend(segments);
        expected.extend((0u8..=255).map(addr));
        assert_eq!(got, expected);

        // Completeness: the streamed chunk set is exactly what the build
        // stored, plus the entry addresses that live outside the store.
        let stored: HashSet<ChunkAddress> = store.into_chunks().into_keys().collect();
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

    #[test]
    fn keyed_descriptors_under_a_plain_arrival_are_rejected() {
        let store = MemoryStore::default();
        let dir = SegmentDir {
            wide: true,
            descriptors: vec![SegDesc {
                first_key: b'a',
                address: addr(2),
                key: Some(nectar_primitives::EncryptionKey::from([3; 32])),
                seg_count: SubtreeCount::new(1),
            }],
        };
        let root = put_raw(&store, encode_segmented_node::<V1>(None, &dir));
        let reader: Reader<_> = Reader::new(&store);
        let mut stream = reader.addresses(&root);
        let err = run(stream.next()).unwrap_err();
        assert!(matches!(
            err,
            ReaderError::Store(StoreError::Decode(
                crate::codec::DecodeError::SegmentContext
            ))
        ));
    }

    #[cfg(not(feature = "encryption"))]
    #[test]
    fn an_encrypted_child_ends_the_walk_and_stays_an_error() {
        let store = MemoryStore::default();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), entry(0xA1).into(), None)
            .unwrap();
        forks
            .insert(
                prefix(b"m"),
                Child::Ref64(EncryptedChunkRef::new(
                    addr(0x4D),
                    nectar_primitives::EncryptionKey::from([0x4D; 32]),
                ))
                .into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        run(async {
            let mut stream = reader.addresses(&root);
            assert_eq!(stream.next().await.unwrap(), Some(root));
            assert_eq!(stream.next().await.unwrap(), Some(addr(0xA1)));
            // The blocked step is never consumed, so the error is stable.
            assert!(matches!(
                stream.next().await.unwrap_err(),
                ReaderError::EncryptedChild
            ));
            assert!(matches!(
                stream.next().await.unwrap_err(),
                ReaderError::EncryptedChild
            ));
        });
    }

    /// Store wrapper that records fetches and yields once per get, so a
    /// `next` future can be observed mid-fetch.
    #[derive(Clone)]
    struct SlowStore {
        inner: std::sync::Arc<SlowInner>,
    }

    struct SlowInner {
        store: MemoryStore,
        fetched: std::sync::Mutex<Vec<ChunkAddress>>,
    }

    impl SlowStore {
        fn new(store: MemoryStore) -> Self {
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
        futures::future::poll_fn(|cx| {
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

    impl ChunkGet for SlowStore {
        type Trust = nectar_primitives::Verified;
        type Error = <MemoryStore as ChunkGet>::Error;

        async fn get(&self, address: &ChunkAddress) -> Result<NodeChunk, Self::Error> {
            self.inner.fetched.lock().unwrap().push(*address);
            yield_once().await;
            ChunkGet::get(&self.inner.store, address).await
        }
    }

    #[test]
    fn a_dropped_next_future_loses_no_addresses() {
        let store = MemoryStore::default();
        let (root, leaf) = sample(&store);
        let slow = SlowStore::new(store);
        let reader: Reader<_> = Reader::new(&slow);
        run(async {
            let mut stream = reader.addresses(&root);
            {
                let fut = stream.next();
                futures::pin_mut!(fut);
                let state = futures::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx))).await;
                assert!(state.is_pending());
            }
            let mut out = Vec::new();
            while let Some(address) = stream.next().await.unwrap() {
                out.push(address);
            }
            assert_eq!(out, vec![root, addr(0xAA), addr(0xAB), leaf, addr(0xBA)]);
        });
    }

    #[test]
    fn the_walk_fetches_exactly_the_chunks_it_names() {
        let store = MemoryStore::default();
        let (root, leaf) = sample(&store);
        let slow = SlowStore::new(store);
        let reader: Reader<_> = Reader::new(&slow);
        drain(reader.addresses(&root));
        // Entry addresses are named, never fetched; each node is fetched
        // exactly once, the descent taking the prefetched copy.
        let mut fetched = slow.fetched();
        let mut expected = vec![root, leaf];
        fetched.sort_unstable();
        expected.sort_unstable();
        assert_eq!(fetched, expected);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use nectar_primitives::{EncryptedChunkRef, EncryptionKey, transcrypt_in_place};

        use crate::codec::encode_leaf_segment;
        use crate::encryption::EncryptedNodePut;

        use super::*;

        const SECRET: &[u8] = b"correct horse battery staple";

        #[test]
        fn a_plain_parent_opens_its_encrypted_child() {
            let store = MemoryStore::default();
            let mut child = Node::empty();
            child
                .forks_mut()
                .insert(prefix(b"x"), entry(0x11).into(), None)
                .unwrap();
            run(async {
                let reference = store.put_node_encrypted(&child, SECRET).await.unwrap();
                let mut root = Node::empty();
                root.forks_mut()
                    .insert(prefix(b"m"), Child::Ref64(reference.clone()).into(), None)
                    .unwrap();
                let root_addr = store.put_node(&root).await.unwrap();
                let reader: Reader<_> = Reader::new(&store);
                let mut stream = reader.addresses(&root_addr);
                let mut out = Vec::new();
                while let Some(address) = stream.next().await.unwrap() {
                    out.push(address);
                }
                assert_eq!(out, vec![root_addr, *reference.address(), addr(0x11)]);
            });
        }

        #[test]
        fn an_encrypted_root_streams_the_same_closure_shape() {
            let store = MemoryStore::default();
            run(async {
                let mut child = Node::empty();
                child
                    .forks_mut()
                    .insert(prefix(b"x"), entry(0x11).into(), None)
                    .unwrap();
                let child_ref = store.put_node_encrypted(&child, SECRET).await.unwrap();

                let mut leaf = Node::empty();
                leaf.forks_mut()
                    .insert(prefix(b"y"), entry(0x22).into(), None)
                    .unwrap();
                let leaf_addr = store.put_node(&leaf).await.unwrap();

                let root_ext = RootExtension::new(Some(entry(9)), None);
                let mut forks = ForkTable::new();
                forks
                    .insert(prefix(b"a"), Child::Ref64(child_ref.clone()).into(), None)
                    .unwrap();
                forks
                    .insert(
                        prefix(b"b"),
                        Child::Ref32(ChunkRef::new(leaf_addr)).into(),
                        None,
                    )
                    .unwrap();
                let root = Node::new(root_ext, forks);
                let root_ref = store.put_node_encrypted(&root, SECRET).await.unwrap();

                let reader: Reader<_> = Reader::new(&store);
                let mut stream = reader.addresses_encrypted(&root_ref);
                let mut out = Vec::new();
                while let Some(address) = stream.next().await.unwrap() {
                    out.push(address);
                }
                assert_eq!(
                    out,
                    vec![
                        *root_ref.address(),
                        addr(9),
                        *child_ref.address(),
                        addr(0x11),
                        leaf_addr,
                        addr(0x22),
                    ]
                );
            });
        }

        #[test]
        fn an_encrypted_spilled_node_streams_its_keyed_segments() {
            let store = MemoryStore::default();
            let mut table = ForkTable::new();
            table.insert(prefix(b"a"), entry(1).into(), None).unwrap();
            table.insert(prefix(b"b"), entry(2).into(), None).unwrap();
            let mut leaf_payload = encode_leaf_segment(&table);
            let leaf_key = EncryptionKey::from([7; 32]);
            transcrypt_in_place(&leaf_key, 0, &mut leaf_payload);
            let leaf_addr = put_raw(&store, leaf_payload);

            let dir = SegmentDir {
                wide: true,
                descriptors: vec![SegDesc {
                    first_key: b'a',
                    address: leaf_addr,
                    key: Some(leaf_key),
                    seg_count: SubtreeCount::new(2),
                }],
            };
            let mut root_payload = encode_segmented_node::<V1>(None, &dir);
            let root_key = EncryptionKey::from([9; 32]);
            transcrypt_in_place(&root_key, 0, &mut root_payload);
            let root_addr = put_raw(&store, root_payload);
            let reference = EncryptedChunkRef::new(root_addr, root_key);

            let reader: Reader<_> = Reader::new(&store);
            let got = drain(reader.addresses_encrypted(&reference));
            assert_eq!(got, vec![root_addr, leaf_addr, addr(1), addr(2)]);
        }

        #[test]
        fn bare_descriptors_under_an_encrypted_arrival_are_rejected() {
            let store = MemoryStore::default();
            let dir = SegmentDir {
                wide: false,
                descriptors: vec![SegDesc {
                    first_key: b'a',
                    address: addr(2),
                    key: None,
                    seg_count: SubtreeCount::new(1),
                }],
            };
            let mut payload = encode_segmented_node::<V1>(None, &dir);
            let root_key = EncryptionKey::from([9; 32]);
            transcrypt_in_place(&root_key, 0, &mut payload);
            let root_addr = put_raw(&store, payload);
            let reference = EncryptedChunkRef::new(root_addr, root_key);

            let reader: Reader<_> = Reader::new(&store);
            let mut stream = reader.addresses_encrypted(&reference);
            let err = run(stream.next()).unwrap_err();
            assert!(matches!(
                err,
                ReaderError::Store(StoreError::Decode(
                    crate::codec::DecodeError::SegmentContext
                ))
            ));
        }
    }
}
