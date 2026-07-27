//! Store seam: manifest nodes over the trusted chunk store.
//!
//! A node is a content-addressed chunk whose address is the BMT of its encoded
//! payload. The store is the trust boundary: a node read back from a
//! [`TrustedGet`] is decoded straight from the certified bytes, so the read
//! path never re-hashes. The write path seals a freshly built payload into a
//! [`Verified`] content chunk, deriving the address rather than trusting one.
//!
//! [`NodeGet`] and [`NodePut`] reuse the primitives store traits; the read
//! seam binds the content-only registry, so a non-content chunk is rejected
//! at decode rather than decoded as a node.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::mem;

use nectar_kernel::{Admission, InFlight, Window};
use nectar_primitives::store::{BoxedError, ChunkPut, MaybeSend, MaybeSync, TrustedGet};
use nectar_primitives::{
    Chunk, ChunkAddress, ChunkOps, ContentChunk, ContentOnlyChunkSet, EncryptionKey, Verified,
    transcrypt_in_place,
};

use crate::codec::{DecodeError, DecodedChunk, EncodeError, SegmentDir};
use crate::fork::ForkTable;
use crate::format::Format;
use crate::node::Node;

/// A manifest node sealed as a verified content chunk over the standard
/// registry, whose content-chunk member carries the node payload.
pub type NodeChunk = Chunk<Verified>;

/// A node chunk read back through the content-only registry: the fetch
/// itself certifies `address == BMT(body)`.
type FetchedChunk = Chunk<Verified, ContentOnlyChunkSet>;

/// A node write or read failure across the store seam.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Encoding the node payload failed.
    #[error(transparent)]
    Encode(#[from] EncodeError),
    /// Decoding a node from stored bytes failed.
    #[error(transparent)]
    Decode(#[from] DecodeError),
    /// Sealing the payload into a content chunk failed.
    #[error("seal node chunk")]
    Seal(#[source] nectar_primitives::PrimitivesError),
    /// The backing store failed.
    #[error("store")]
    Store(#[source] BoxedError),
}

impl StoreError {
    /// Box a backend error behind the seam.
    pub(crate) fn store<E: core::error::Error + MaybeSend + MaybeSync + 'static>(err: E) -> Self {
        Self::Store(Box::new(err))
    }
}

impl<F: Format> Node<F> {
    /// Seal this node as its content chunk, deriving the content address by
    /// BMT over the encoded payload; the address is derived, never supplied.
    pub fn to_chunk(&self) -> Result<NodeChunk, StoreError> {
        let payload = self.encode()?;
        let content = ContentChunk::new(payload).map_err(StoreError::Seal)?;
        Chunk::from_envelope(content.into()).map_err(StoreError::Seal)
    }

    /// Decode a node from a chunk the store has already certified.
    ///
    /// The [`Verified`] type is the trust boundary: the payload is decoded
    /// from the certified bytes, never re-hashed.
    pub fn from_chunk(chunk: &NodeChunk) -> Result<Self, DecodeError> {
        Self::decode(chunk.envelope().data())
    }
}

/// Async node retrieval over a trusted store.
///
/// Blanket-implemented for every [`TrustedGet`]; the `Trust = Verified`
/// bound is what lets [`get_node`](Self::get_node) skip re-hashing.
pub trait NodeGet: TrustedGet<ContentOnlyChunkSet> {
    /// Load and decode the node at `address`, materializing a spilled node's
    /// forks from its segments so the caller always sees one logical node.
    ///
    /// Reassembling a segmented node fetches its segment chunks under a
    /// bounded window and holds only that one node's forks, bounded by the
    /// fork count, so peak retained state stays O(depth).
    fn get_node<F: Format>(
        &self,
        address: &ChunkAddress,
    ) -> impl Future<Output = Result<Node<F>, StoreError>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        materialize_node::<Self, F>(self, address)
    }
}

impl<T: TrustedGet<ContentOnlyChunkSet>> NodeGet for T {}

/// The greatest legal segment-directory depth (spec 5.4); a deeper nesting is a
/// malformed image, not a tree this format ever produces.
const MAX_DIR_DEPTH: usize = 2;

/// Load the node at `address`, reassembling a segmented node's forks in place.
async fn materialize_node<S, F>(store: &S, address: &ChunkAddress) -> Result<Node<F>, StoreError>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    F: Format,
{
    let (node, _) = materialize_traced::<S, F>(store, address, None).await?;
    Ok(node)
}

/// Load the node at `address`, decrypting with `key` when the reference
/// carried one, and record each segment chunk address the reassembly fetches.
///
/// The arrival fixes the segment widths: a plain node's descriptors must be
/// bare, an encrypted node's must carry keys.
pub(crate) async fn materialize_traced<S, F>(
    store: &S,
    address: &ChunkAddress,
    key: Option<&EncryptionKey>,
) -> Result<(Node<F>, Vec<ChunkAddress>), StoreError>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    F: Format,
{
    let chunk = store.get(address).await.map_err(StoreError::store)?;
    match decode_fetched::<F>(&chunk, key)? {
        DecodedChunk::Node(node) => Ok((node, Vec::new())),
        DecodedChunk::Segmented(root, dir) => {
            let mut trace = Vec::with_capacity(dir.descriptors.len());
            let forks =
                collect_segment_forks::<S, F>(store, &dir, key.is_some(), &mut trace).await?;
            Ok((Node::new(root, forks), trace))
        }
        // A fork child reference names a node, never a bare segment.
        DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => {
            Err(StoreError::Decode(DecodeError::SegmentContext))
        }
    }
}

/// Decode a certified chunk, decrypting first when a key travels with it.
fn decode_fetched<F: Format>(
    chunk: &FetchedChunk,
    key: Option<&EncryptionKey>,
) -> Result<DecodedChunk<F>, DecodeError> {
    key.map_or_else(
        || Node::decode_chunk(chunk.envelope().data()),
        |key| {
            let mut payload = chunk.envelope().data().to_vec();
            transcrypt_in_place(key, 0, &mut payload);
            Node::decode_chunk(&payload)
        },
    )
}

/// Bounds a spilled node's concurrent segment fetches.
const SEGMENT_WINDOW: Window = Window::DEFAULT;

/// A malformed segment structure.
const fn segment_context() -> StoreError {
    StoreError::Decode(DecodeError::SegmentContext)
}

/// A segment fetch routed back by slot index.
type SegmentFetch = (usize, Result<FetchedChunk, StoreError>);

/// Fetch progress of one segment slot.
enum SlotState {
    /// Not yet admitted.
    Queued,
    /// Admitted into the in-flight set.
    Launched,
    /// Fetch complete, buffered for the in-order drain; boxed to keep the
    /// slot vector slim.
    Landed(Box<Result<FetchedChunk, StoreError>>),
    /// Drained.
    Spent,
}

/// One segment descriptor in the bounded join: its fetch identity, nesting
/// depth and successor in directory order.
struct SegmentSlot {
    /// The segment chunk address.
    address: ChunkAddress,
    /// The segment's decryption key, when the directory carries keys.
    key: Option<EncryptionKey>,
    /// Nesting depth of the directory holding this descriptor.
    depth: usize,
    /// The slot that follows in directory order; `None` ends the join.
    next: Option<usize>,
    /// Fetch progress.
    state: SlotState,
}

impl SegmentSlot {
    /// The landed outcome, once; `None` while queued, in flight or spent.
    fn take_landed(&mut self) -> Option<Result<FetchedChunk, StoreError>> {
        match mem::replace(&mut self.state, SlotState::Spent) {
            SlotState::Landed(outcome) => Some(*outcome),
            state => {
                self.state = state;
                None
            }
        }
    }
}

/// Queue `dir`'s descriptors as slots in directory order ahead of `tail`;
/// returns the head of the spliced run, or `tail` when the directory is
/// empty.
fn splice_directory(
    slots: &mut Vec<SegmentSlot>,
    dir: &SegmentDir,
    depth: usize,
    tail: Option<usize>,
) -> Option<usize> {
    let base = slots.len();
    let last = base.saturating_add(dir.descriptors.len().saturating_sub(1));
    for (index, descriptor) in dir.descriptors.iter().enumerate() {
        let position = base.saturating_add(index);
        let next = if position < last {
            Some(position.saturating_add(1))
        } else {
            tail
        };
        slots.push(SegmentSlot {
            address: descriptor.address,
            key: descriptor.key.clone(),
            depth,
            next,
            state: SlotState::Queued,
        });
    }
    if dir.descriptors.is_empty() {
        tail
    } else {
        Some(base)
    }
}

/// Fill the window: admit queued fetches from `head` onward in directory
/// order, stopping at the first denial. The head always launches, so the
/// in-order drain never waits on an unlaunched slot.
fn launch_segments<'a, S>(
    store: &'a S,
    admission: Admission,
    slots: &mut [SegmentSlot],
    head: usize,
    in_flight: &mut InFlight<'a, SegmentFetch>,
    buffered: usize,
) where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
{
    let mut cursor = Some(head);
    let mut head_served = false;
    while let Some(index) = cursor {
        let Some(slot) = slots.get_mut(index) else {
            return;
        };
        if matches!(slot.state, SlotState::Queued) {
            let occupancy = in_flight.len().saturating_add(buffered);
            if !admission.admits(occupancy, head_served || index == head) {
                return;
            }
            let address = slot.address;
            slot.state = SlotState::Launched;
            in_flight.push(Box::pin(async move {
                (index, store.get(&address).await.map_err(StoreError::store))
            }));
        }
        head_served = true;
        cursor = slot.next;
    }
}

/// Gather every fork of a spilled node: fetch the segments its directory
/// routes to under [`SEGMENT_WINDOW`], folding completions strictly in
/// directory order and recording each segment chunk address into `trace`.
///
/// Only the fetches overlap; the width checks, the record fold and the trace
/// all run in directory order, so the reassembled node matches a serial
/// gather exactly.
async fn collect_segment_forks<S, F>(
    store: &S,
    dir: &SegmentDir,
    encrypted: bool,
    trace: &mut Vec<ChunkAddress>,
) -> Result<ForkTable<F>, StoreError>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    F: Format,
{
    let mut slots = Vec::with_capacity(dir.descriptors.len());
    let mut head = splice_directory(&mut slots, dir, 0, None);
    let admission = Admission::new(SEGMENT_WINDOW);
    let mut in_flight = InFlight::new();
    let mut buffered: usize = 0;
    let mut table = ForkTable::new();
    while let Some(current) = head {
        launch_segments(
            store,
            admission,
            &mut slots,
            current,
            &mut in_flight,
            buffered,
        );
        let landed = slots.get_mut(current).and_then(|slot| {
            slot.take_landed().map(|outcome| {
                (
                    outcome,
                    slot.address,
                    slot.key.clone(),
                    slot.depth,
                    slot.next,
                )
            })
        });
        let Some((outcome, address, key, depth, next)) = landed else {
            // The head is launched before every wait, so an empty set here is
            // a lost completion, not a legal drain.
            let Some((index, outcome)) = poll_fn(|cx| in_flight.poll(cx)).await else {
                return Err(segment_context());
            };
            if let Some(slot) = slots.get_mut(index) {
                slot.state = SlotState::Landed(Box::new(outcome));
                buffered = buffered.saturating_add(1);
            }
            continue;
        };
        buffered = buffered.saturating_sub(1);
        // Descriptor width must match the arrival on both sides.
        if key.is_some() != encrypted {
            return Err(segment_context());
        }
        trace.push(address);
        let chunk = outcome?;
        match decode_fetched::<F>(&chunk, key.as_ref())? {
            DecodedChunk::Leaf(sub) => {
                for (first, record) in sub.into_records() {
                    if table.insert_record(first, record).is_some() {
                        return Err(segment_context());
                    }
                }
                head = next;
            }
            DecodedChunk::Directory(inner) => {
                let child_depth = depth.saturating_add(1);
                if child_depth >= MAX_DIR_DEPTH {
                    return Err(segment_context());
                }
                head = splice_directory(&mut slots, &inner, child_depth, next);
            }
            // A segment chunk is a leaf or an inner directory, never a node.
            DecodedChunk::Node(_) | DecodedChunk::Segmented(_, _) => {
                return Err(segment_context());
            }
        }
    }
    Ok(table)
}

/// Async node storage over a chunk putter.
///
/// Blanket-implemented for every [`ChunkPut`]; sealing happens before the
/// first await, so the returned future never holds the source node.
pub trait NodePut: ChunkPut {
    /// Seal `node`, store its chunk, and return the derived address.
    fn put_node<F: Format>(
        &self,
        node: &Node<F>,
    ) -> impl Future<Output = Result<ChunkAddress, StoreError>> + MaybeSend
    where
        Self: Sized + MaybeSync,
    {
        let sealed = node.to_chunk();
        async move {
            let chunk = sealed?;
            let address = *chunk.address();
            self.put(chunk).await.map_err(StoreError::store)?;
            Ok(address)
        }
    }
}

impl<T: ChunkPut> NodePut for T {}

#[cfg(test)]
mod tests {
    use core::pin::Pin;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Poll};
    use std::vec;

    use bytes::Bytes;
    use nectar_primitives::store::{ChunkGet, ChunkPut, ContentGet, MemoryStore};
    use nectar_primitives::{ChunkAddress, ChunkRef, DefaultContentChunk};
    use nectar_testing::run;

    use crate::bounded::Prefix;
    use crate::builder::Builder;
    use crate::codec::{encode_dir_segment, encode_leaf_segment, encode_segmented_node};
    use crate::count::SubtreeCount;
    use crate::format::V1;
    use crate::meta::{KeyId, Metadata};
    use crate::node::RootExtension;
    use crate::value::{Entry, Key};

    use super::*;

    fn sample() -> Node {
        let root = RootExtension::new(
            Some(Entry::from(ChunkRef::new(ChunkAddress::new([1; 32])))),
            Some(
                Metadata::new(
                    KeyId::WebsiteIndexDocument,
                    Bytes::from_static(b"index.html"),
                )
                .unwrap(),
            ),
        );
        let mut node = Node::new(root, Default::default());
        node.forks_mut()
            .insert(
                Prefix::try_from(&b"index.html"[..]).unwrap(),
                Entry::from(ChunkRef::new(ChunkAddress::new([7; 32]))).into(),
                None,
            )
            .unwrap();
        node
    }

    #[test]
    fn round_trips_through_a_memory_store() {
        let store = ContentGet::new(MemoryStore::default());
        let node = sample();

        let address = run(store.put_node(&node)).unwrap();
        let loaded: Node = run(store.get_node(&address)).unwrap();

        assert_eq!(loaded, node);
    }

    #[test]
    fn address_is_the_content_address_of_the_payload() {
        let node = sample();
        let chunk = node.to_chunk().unwrap();
        let expected = *DefaultContentChunk::new(node.encode().unwrap())
            .unwrap()
            .address();
        assert_eq!(chunk.address(), &expected);
    }

    #[test]
    fn from_chunk_decodes_without_a_store() {
        let node = sample();
        let chunk = node.to_chunk().unwrap();
        assert_eq!(Node::from_chunk(&chunk).unwrap(), node);
    }

    #[test]
    fn missing_address_is_a_store_error() {
        let store = ContentGet::new(MemoryStore::default());
        let err = run(store.get_node::<crate::V1>(&ChunkAddress::new([0; 32]))).unwrap_err();
        assert!(matches!(err, StoreError::Store(_)));
    }

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
    fn put_raw(store: &ContentGet<MemoryStore>, payload: Vec<u8>) -> ChunkAddress {
        let content = ContentChunk::new(payload).unwrap();
        let chunk: NodeChunk = Chunk::from_envelope(content.into()).unwrap();
        let address = *chunk.address();
        run(ChunkPut::put(store, chunk)).unwrap();
        address
    }

    /// Build a spilled 256-fork manifest root, returning its address.
    fn spilled_root(store: &ContentGet<MemoryStore>) -> ChunkAddress {
        let mut builder = Builder::<V1>::new();
        for byte in 0u8..=255 {
            builder.insert(Key::from(&[byte][..]), entry(byte), None);
        }
        *run(builder.build(store)).unwrap().root()
    }

    /// A serial reference gather: fetch each segment in directory order,
    /// depth first, folding records and recording the trace.
    fn serial_gather(
        store: &ContentGet<MemoryStore>,
        dir: &SegmentDir,
        table: &mut ForkTable,
        trace: &mut Vec<ChunkAddress>,
    ) {
        for descriptor in &dir.descriptors {
            trace.push(descriptor.address);
            let chunk = store.inner().get(&descriptor.address).unwrap();
            match Node::<V1>::decode_chunk(chunk.envelope().data()).unwrap() {
                DecodedChunk::Leaf(sub) => {
                    for (first, record) in sub.into_records() {
                        assert!(table.insert_record(first, record).is_none());
                    }
                }
                DecodedChunk::Directory(inner) => serial_gather(store, &inner, table, trace),
                DecodedChunk::Node(_) | DecodedChunk::Segmented(_, _) => {
                    panic!("a segment decodes to a leaf or a directory")
                }
            }
        }
    }

    #[test]
    fn spilled_reassembly_matches_a_serial_gather() {
        let store = ContentGet::new(MemoryStore::default());
        let root = spilled_root(&store);

        let chunk = store.inner().get(&root).unwrap();
        let DecodedChunk::Segmented(root_ext, dir) =
            Node::<V1>::decode_chunk(chunk.envelope().data()).unwrap()
        else {
            panic!("a 256-fork root must spill");
        };
        assert!(dir.descriptors.len() > 1);
        let mut expected = ForkTable::new();
        let mut expected_trace = Vec::new();
        serial_gather(&store, &dir, &mut expected, &mut expected_trace);

        let (node, trace) = run(materialize_traced::<_, V1>(&store, &root, None)).unwrap();
        assert_eq!(node, Node::new(root_ext, expected));
        assert_eq!(trace, expected_trace);
    }

    /// Completes on its second poll, so counted fetches genuinely overlap
    /// under the single-threaded test executor.
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// A trusted store recording the peak number of concurrent `get` calls.
    struct GatedStore {
        inner: ContentGet<MemoryStore>,
        inflight: AtomicUsize,
        peak: AtomicUsize,
    }

    impl ChunkGet<ContentOnlyChunkSet> for GatedStore {
        type Trust = Verified;
        type Error = <ContentGet<MemoryStore> as ChunkGet<ContentOnlyChunkSet>>::Error;

        async fn get(&self, address: &ChunkAddress) -> Result<FetchedChunk, Self::Error> {
            let now = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak.fetch_max(now, Ordering::Relaxed);
            YieldOnce(false).await;
            let chunk = ChunkGet::get(&self.inner, address).await;
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            chunk
        }
    }

    #[test]
    fn segment_fetches_overlap_under_the_window() {
        let inner = ContentGet::new(MemoryStore::default());
        let root = spilled_root(&inner);
        let store = GatedStore {
            inner,
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        };

        let node: Node = run(store.get_node(&root)).unwrap();
        assert_eq!(node.forks().len(), 256);

        let peak = store.peak.load(Ordering::Relaxed);
        assert!(peak > 1, "segment fetches overlapped, peak {peak}");
        assert!(peak <= usize::from(SEGMENT_WINDOW.get()));
    }

    #[test]
    fn a_depth_two_directory_reassembles_in_directory_order() {
        let store = ContentGet::new(MemoryStore::default());
        let leaf = |bytes: &[u8]| {
            let mut table = ForkTable::new();
            for &byte in bytes {
                table
                    .insert(prefix(&[byte]), entry(byte).into(), None)
                    .unwrap();
            }
            put_raw(&store, encode_leaf_segment(&table))
        };
        let leaf_a = leaf(&[0x10, 0x11]);
        let leaf_b = leaf(&[0x20, 0x21]);
        let leaf_c = leaf(&[0x30]);
        let leaf_d = leaf(&[0x40, 0x41]);
        let inner = SegmentDir::plain(vec![
            (0x20, leaf_b, SubtreeCount::new(2)),
            (0x30, leaf_c, SubtreeCount::new(1)),
        ]);
        let inner_dir = put_raw(&store, encode_dir_segment::<V1>(&inner));
        let top = SegmentDir::plain(vec![
            (0x10, leaf_a, SubtreeCount::new(2)),
            (0x20, inner_dir, SubtreeCount::new(3)),
            (0x40, leaf_d, SubtreeCount::new(2)),
        ]);
        let root = put_raw(&store, encode_segmented_node::<V1>(None, &top));

        let (node, trace) = run(materialize_traced::<_, V1>(&store, &root, None)).unwrap();
        // Depth-first, directory order: the inner directory's leaves land
        // between its siblings.
        assert_eq!(trace, vec![leaf_a, inner_dir, leaf_b, leaf_c, leaf_d]);
        let mut expected = ForkTable::new();
        for byte in [0x10, 0x11, 0x20, 0x21, 0x30, 0x40, 0x41] {
            expected
                .insert(prefix(&[byte]), entry(byte).into(), None)
                .unwrap();
        }
        assert_eq!(node, Node::new(None, expected));
    }
}
