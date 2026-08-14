//! Store seam: manifest nodes over the trusted chunk store.
//!
//! A node is a content-addressed chunk whose address is the BMT of its encoded
//! payload. The store is the trust boundary: a node read back from a
//! [`TrustedGet`] is decoded straight from the certified bytes, so the read
//! path never re-hashes. The write path seals a freshly built payload into a
//! [`Verified`] content chunk, deriving the address rather than trusting one.
//!
//! [`load_node`] and [`save_node`] are free functions over the layer-1 store
//! traits, not verbs hung off a store; the read side binds the content-only
//! registry, so a non-content chunk is rejected at decode rather than decoded
//! as a node.
//!
//! Both seams are generic over the structural reference width `R`. Reading
//! needs nothing but the reference: an encrypted one carries its own key, so
//! the arrival decides whether the fetched bytes are decrypted. Writing needs
//! a [`Seal`], which turns one payload into the stored chunk and the reference
//! that reaches it, and so owns whatever secret an encrypted database derives
//! its keys from.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::mem;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_util::stream::{FuturesUnordered, Stream};
use nectar_governor::{Admission, Window};
use nectar_primitives::store::{BoxedError, ChunkPut, MaybeSend, MaybeSync, TrustedGet};
use nectar_primitives::{
    Chunk, ChunkAddress, ChunkOps, ChunkRef, ContentChunk, ContentOnlyChunkSet, EncryptionKey,
    EntryRef, Verified, WrongRefKind, transcrypt_in_place,
};
use nectar_tasks::BoxFuture;

use crate::codec::{DecodeError, DecodedChunk, EncodeError, SegmentDir};
use crate::fork::ForkTable;
use crate::format::Format;
use crate::node::{Node, NodeRef};

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
    /// A runtime reference was of the wrong width for the typed read.
    #[error(transparent)]
    Width(#[from] WrongRefKind),
}

impl StoreError {
    /// Box a backend error behind the seam.
    pub(crate) fn store<E: core::error::Error + MaybeSend + MaybeSync + 'static>(err: E) -> Self {
        Self::Store(Box::new(err))
    }
}

/// Write seam: seal one node or segment payload into its stored chunk and the
/// reference of width `R` that reaches it.
///
/// The address is derived from the sealed bytes, never supplied, so a sealer
/// cannot make a chunk lie about its own content. An encrypted sealer owns the
/// secret its per-reference keys derive from; the read path needs no such
/// state, because an encrypted reference carries its key in band.
pub trait Seal<R: NodeRef> {
    /// Seal `payload`, returning the chunk to store and its reference.
    fn seal(&self, payload: Vec<u8>) -> Result<(NodeChunk, R), StoreError>;
}

/// Plaintext sealing: the payload is the chunk body, and the reference is its
/// content address.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Plaintext;

impl Seal<ChunkRef> for Plaintext {
    fn seal(&self, payload: Vec<u8>) -> Result<(NodeChunk, ChunkRef), StoreError> {
        let content = ContentChunk::new(payload).map_err(StoreError::Seal)?;
        let chunk = Chunk::from_envelope(content.into()).map_err(StoreError::Seal)?;
        let reference = ChunkRef::new(*chunk.address());
        Ok((chunk, reference))
    }
}

/// The decryption key a structural reference carries; `None` for a plain one.
pub(crate) fn reference_key<R: NodeRef>(reference: &R) -> Option<EncryptionKey> {
    match reference.clone().into_entry_ref() {
        EntryRef::Plain(_) => None,
        EntryRef::Encrypted(encrypted) => Some(encrypted.key().clone()),
    }
}

impl<F: Format, R: NodeRef> Node<F, R> {
    /// Seal this node with `seal`, returning the chunk and its reference.
    pub fn to_sealed<K: Seal<R>>(&self, seal: &K) -> Result<(NodeChunk, R), StoreError> {
        seal.seal(self.encode()?)
    }

    /// Decode a node from a chunk the store has already certified, opening it
    /// with the key `reference` carries.
    ///
    /// The [`Verified`] type is the trust boundary: the payload is decoded
    /// from the certified bytes, never re-hashed.
    pub fn from_chunk(chunk: &NodeChunk, reference: &R) -> Result<Self, DecodeError> {
        opened(chunk.envelope().data(), reference, Self::decode)
    }
}

/// Run `decode` over a chunk body, decrypting it first with the key
/// `reference` carries.
///
/// The one place the arrival's key meets the stored bytes; a plain reference
/// carries none, so a plaintext database never copies its payload.
fn opened<R, T>(
    body: &[u8],
    reference: &R,
    decode: impl Fn(&[u8]) -> Result<T, DecodeError>,
) -> Result<T, DecodeError>
where
    R: NodeRef,
{
    reference_key(reference).map_or_else(
        || decode(body),
        |key| {
            let mut payload = body.to_vec();
            transcrypt_in_place(&key, 0, &mut payload);
            decode(&payload)
        },
    )
}

/// Decode any manifest chunk the store has certified, opening it with the key
/// `reference` carries.
pub(crate) fn open_chunk<F: Format, R: NodeRef>(
    body: &[u8],
    reference: &R,
) -> Result<DecodedChunk<F, R>, DecodeError> {
    opened(body, reference, Node::decode_chunk)
}

impl<F: Format> Node<F> {
    /// Seal this node as its content chunk, deriving the content address by
    /// BMT over the encoded payload; the address is derived, never supplied.
    pub fn to_chunk(&self) -> Result<NodeChunk, StoreError> {
        Ok(self.to_sealed(&Plaintext)?.0)
    }
}

/// The greatest legal segment-directory depth (spec 5.4); a deeper nesting is a
/// malformed image, not a tree this format ever produces.
const MAX_DIR_DEPTH: usize = 2;

/// Load and decode the node `reference` reaches, reassembling a spilled node's
/// forks from its segments so the caller always sees one logical node.
///
/// The store's `Trust = Verified` bound is what lets the decode skip
/// re-hashing. Reassembly fetches segment chunks under a bounded window, so
/// peak retained state stays bounded by the fork count and that window.
pub async fn load_node<S, F, R>(store: &S, reference: &R) -> Result<Node<F, R>, StoreError>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    F: Format,
    R: NodeRef,
{
    let (node, _) = materialize_traced::<S, F, R>(store, reference).await?;
    Ok(node)
}

/// Load the node `reference` reaches, decrypting with the key it carries, and
/// record each segment chunk address the reassembly fetches.
///
/// The arrival fixes every width below: a segment descriptor is `R::SIZE`
/// wide, so a plain database read as encrypted (or the reverse) fails at the
/// chunk's own width witness rather than mis-parsing.
pub(crate) async fn materialize_traced<S, F, R>(
    store: &S,
    reference: &R,
) -> Result<(Node<F, R>, Vec<ChunkAddress>), StoreError>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    F: Format,
    R: NodeRef,
{
    let chunk = store
        .get(reference.address())
        .await
        .map_err(StoreError::store)?;
    match open_chunk::<F, R>(chunk.envelope().data(), reference)? {
        DecodedChunk::Node(node) => Ok((node, Vec::new())),
        DecodedChunk::Segmented(root, dir) => {
            let mut trace = Vec::with_capacity(dir.descriptors.len());
            let forks = collect_segment_forks::<S, F, R>(store, &dir, &mut trace).await?;
            Ok((Node::new(root, forks), trace))
        }
        // A fork child reference names a node, never a bare segment.
        DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => {
            Err(StoreError::Decode(DecodeError::SegmentContext))
        }
    }
}

/// The concurrent segment-fetch window: the format's read-ahead saturated
/// into a nonzero window, matching the enclosing scan and traverse walks.
fn segment_window<F: Format>() -> Window {
    let slots = u16::try_from(F::READ_AHEAD).unwrap_or(u16::MAX);
    Window::new(slots).unwrap_or(Window::DEFAULT)
}

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
struct SegmentSlot<R: NodeRef> {
    /// The reference reaching the segment chunk; an encrypted one carries the
    /// key that opens it.
    reference: R,
    /// Nesting depth of the directory holding this descriptor.
    depth: usize,
    /// The slot that follows in directory order; `None` ends the join.
    next: Option<usize>,
    /// Fetch progress.
    state: SlotState,
}

impl<R: NodeRef> SegmentSlot<R> {
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
fn splice_directory<R: NodeRef>(
    slots: &mut Vec<SegmentSlot<R>>,
    dir: &SegmentDir<R>,
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
            reference: descriptor.reference.clone(),
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
fn launch_segments<'a, S, R>(
    store: &'a S,
    admission: Admission,
    slots: &mut [SegmentSlot<R>],
    head: usize,
    in_flight: &mut FuturesUnordered<BoxFuture<'a, SegmentFetch>>,
    buffered: usize,
) where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    R: NodeRef,
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
            let address = *slot.reference.address();
            slot.state = SlotState::Launched;
            in_flight.push(Box::pin(async move {
                (index, store.get(&address).await.map_err(StoreError::store))
            }));
        }
        head_served = true;
        cursor = slot.next;
    }
}

/// The segment join: a directory-order frontier of slots over its own
/// in-flight set.
///
/// `admit` grows the frontier by splicing a decoded inner directory, then
/// launches queued fetches from the head; `poll_turn` folds the head slot
/// strictly in directory order, parks an inner directory for the next admit,
/// and reports a lost completion once the set drains. The in-order fold is
/// the reassembly's byte-exact semantics, not head-of-line blocking: only the
/// fetches overlap.
struct SegmentJoin<'a, S, F: Format, R: NodeRef> {
    store: &'a S,
    /// Descriptor slots in directory order; grows as inner directories splice.
    slots: Vec<SegmentSlot<R>>,
    admission: Admission,
    /// Fetches launched ahead of the in-order fold.
    in_flight: FuturesUnordered<BoxFuture<'a, SegmentFetch>>,
    /// The slot to drain next; `None` once the join is complete.
    head: Option<usize>,
    /// Completions landed but not yet drained.
    buffered: usize,
    /// An inner directory decoded at the head, awaiting its admit-side splice:
    /// its descriptors, child depth, and the directory slot's successor.
    pending: Option<(SegmentDir<R>, usize, Option<usize>)>,
    /// The reassembled forks, folded in directory order.
    table: ForkTable<F, R>,
    /// Each drained segment chunk address, in directory order.
    trace: Vec<ChunkAddress>,
}

impl<'a, S, F, R> SegmentJoin<'a, S, F, R>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    F: Format,
    R: NodeRef,
{
    /// A join over `dir`'s descriptors, spliced as the initial frontier.
    fn new(store: &'a S, dir: &SegmentDir<R>) -> Self {
        let mut slots = Vec::with_capacity(dir.descriptors.len());
        let head = splice_directory(&mut slots, dir, 0, None);
        Self {
            store,
            slots,
            admission: Admission::new(segment_window::<F>()),
            in_flight: FuturesUnordered::new(),
            head,
            buffered: 0,
            pending: None,
            table: ForkTable::new(),
            trace: Vec::with_capacity(dir.descriptors.len()),
        }
    }

    /// The folded forks and the directory-order segment trace.
    fn into_parts(self) -> (ForkTable<F, R>, Vec<ChunkAddress>) {
        (self.table, self.trace)
    }

    /// Splice any parked inner directory onto the frontier, then launch the
    /// queued fetches the window admits.
    fn admit(&mut self) {
        if let Some((inner, depth, next)) = self.pending.take() {
            self.head = splice_directory(&mut self.slots, &inner, depth, next);
        }
        if let Some(head) = self.head {
            launch_segments(
                self.store,
                self.admission,
                &mut self.slots,
                head,
                &mut self.in_flight,
                self.buffered,
            );
        }
    }

    /// One poll of the bounded join: admit, fold the head slot if it has
    /// landed, else absorb one completion. `None` ends the join.
    ///
    /// All state lives in `self`, so a dropped poll replays.
    fn poll_turn(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), StoreError>>> {
        loop {
            self.admit();
            if let Some(outcome) = self.take_ready() {
                return Poll::Ready(Some(outcome));
            }
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some((index, outcome))) => {
                    if let Some(slot) = self.slots.get_mut(index) {
                        slot.state = SlotState::Landed(Box::new(outcome));
                        self.buffered = self.buffered.saturating_add(1);
                    }
                }
                // The head launches before every wait, so an empty set with
                // work still owed is a lost completion, not a legal drain.
                Poll::Ready(None) => {
                    return Poll::Ready(
                        (self.head.is_some() || self.pending.is_some())
                            .then(|| Err(segment_context())),
                    );
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Fold the head slot once its fetch has landed, in directory order.
    fn take_ready(&mut self) -> Option<Result<(), StoreError>> {
        let head = self.head?;
        let slot = self.slots.get_mut(head)?;
        let outcome = slot.take_landed()?;
        let reference = slot.reference.clone();
        let depth = slot.depth;
        let next = slot.next;
        self.buffered = self.buffered.saturating_sub(1);
        self.trace.push(*reference.address());
        let chunk = match outcome {
            Ok(chunk) => chunk,
            Err(error) => return Some(Err(error)),
        };
        match open_chunk::<F, R>(chunk.envelope().data(), &reference) {
            Ok(DecodedChunk::Leaf(sub)) => {
                for (first, record) in sub.into_records() {
                    if self.table.insert_record(first, record).is_some() {
                        return Some(Err(segment_context()));
                    }
                }
                self.head = next;
                Some(Ok(()))
            }
            Ok(DecodedChunk::Directory(inner)) => {
                let child_depth = depth.saturating_add(1);
                if child_depth >= MAX_DIR_DEPTH {
                    return Some(Err(segment_context()));
                }
                // The splice is admit-side frontier growth; the drained head
                // stays put until admit repositions it onto the spliced run.
                self.pending = Some((inner, child_depth, next));
                Some(Ok(()))
            }
            // A segment chunk is a leaf or an inner directory, never a node.
            Ok(DecodedChunk::Node(_) | DecodedChunk::Segmented(_, _)) => {
                Some(Err(segment_context()))
            }
            Err(error) => Some(Err(error.into())),
        }
    }
}

/// Gather every fork of a spilled node: fetch the segments its directory
/// routes to under the format's read-ahead window, folding completions
/// strictly in directory order and recording each segment chunk address into
/// `trace`.
///
/// Only the fetches overlap; the width checks, the record fold and the trace
/// all run in directory order, so the reassembled node matches a serial
/// gather exactly.
async fn collect_segment_forks<S, F, R>(
    store: &S,
    dir: &SegmentDir<R>,
    trace: &mut Vec<ChunkAddress>,
) -> Result<ForkTable<F, R>, StoreError>
where
    S: TrustedGet<ContentOnlyChunkSet> + MaybeSync,
    F: Format,
    R: NodeRef,
{
    let mut join = SegmentJoin::<S, F, R>::new(store, dir);
    while let Some(turn) = poll_fn(|cx| join.poll_turn(cx)).await {
        turn?;
    }
    let (table, segments) = join.into_parts();
    trace.extend(segments);
    Ok(table)
}

/// Seal `node` with `seal`, store its chunk, and return the reference that
/// reaches it.
///
/// Sealing happens before the first await, so the returned future never holds
/// the source node.
pub fn save_node<'a, S, F, R, K>(
    store: &'a S,
    node: &Node<F, R>,
    seal: &K,
) -> impl Future<Output = Result<R, StoreError>> + MaybeSend + 'a
where
    S: ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let sealed = node.to_sealed(seal);
    async move {
        let (chunk, reference) = sealed?;
        store.put(chunk).await.map_err(StoreError::store)?;
        Ok(reference)
    }
}

#[cfg(test)]
mod tests {
    use crate::store::Plaintext;
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

        let address = run(save_node(&store, &node, &Plaintext)).unwrap();
        let loaded: Node = run(load_node(&store, &address)).unwrap();

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
        let reference = ChunkRef::new(*chunk.address());
        assert_eq!(Node::from_chunk(&chunk, &reference).unwrap(), node);
    }

    #[test]
    fn missing_address_is_a_store_error() {
        let store = ContentGet::new(MemoryStore::default());
        let err = run(load_node::<_, crate::V1, ChunkRef>(
            &store,
            &ChunkRef::new(ChunkAddress::new([0; 32])),
        ))
        .unwrap_err();
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

    /// Seal a raw payload as a content chunk, store it, and return its
    /// reference.
    fn put_raw(store: &ContentGet<MemoryStore>, payload: Vec<u8>) -> ChunkRef {
        let content = ContentChunk::new(payload).unwrap();
        let chunk: NodeChunk = Chunk::from_envelope(content.into()).unwrap();
        let reference = ChunkRef::new(*chunk.address());
        run(ChunkPut::put(store, chunk)).unwrap();
        reference
    }

    /// Build a spilled 256-fork manifest root, returning its reference.
    fn spilled_root(store: &ContentGet<MemoryStore>) -> ChunkRef {
        let mut builder = Builder::<V1>::new();
        for byte in 0u8..=255 {
            builder.insert(Key::from(&[byte][..]), entry(byte), None);
        }
        *run(builder.build(store, &Plaintext)).unwrap().root()
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
            trace.push(*descriptor.reference.address());
            let chunk = store.inner().get(descriptor.reference.address()).unwrap();
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

        let chunk = store.inner().get(root.address()).unwrap();
        let DecodedChunk::Segmented(root_ext, dir) =
            Node::<V1>::decode_chunk(chunk.envelope().data()).unwrap()
        else {
            panic!("a 256-fork root must spill");
        };
        assert!(dir.descriptors.len() > 1);
        let mut expected = ForkTable::new();
        let mut expected_trace = Vec::new();
        serial_gather(&store, &dir, &mut expected, &mut expected_trace);

        let (node, trace) = run(materialize_traced::<_, V1, ChunkRef>(&store, &root)).unwrap();
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

        let node: Node = run(load_node(&store, &root)).unwrap();
        assert_eq!(node.forks().len(), 256);

        let peak = store.peak.load(Ordering::Relaxed);
        assert!(peak > 1, "segment fetches overlapped, peak {peak}");
        assert!(peak <= usize::from(segment_window::<V1>().get()));
    }

    #[test]
    fn a_depth_two_directory_reassembles_in_directory_order() {
        let store = ContentGet::new(MemoryStore::default());
        let leaf = |bytes: &[u8]| {
            let mut table: ForkTable = ForkTable::new();
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
        let inner = SegmentDir::new(vec![
            (0x20, leaf_b, SubtreeCount::new(2)),
            (0x30, leaf_c, SubtreeCount::new(1)),
        ]);
        let inner_dir = put_raw(&store, encode_dir_segment::<V1, ChunkRef>(&inner));
        let top = SegmentDir::new(vec![
            (0x10, leaf_a, SubtreeCount::new(2)),
            (0x20, inner_dir, SubtreeCount::new(3)),
            (0x40, leaf_d, SubtreeCount::new(2)),
        ]);
        let root = put_raw(&store, encode_segmented_node::<V1, ChunkRef>(None, &top));

        let (node, trace) = run(materialize_traced::<_, V1, ChunkRef>(&store, &root)).unwrap();
        // Depth-first, directory order: the inner directory's leaves land
        // between its siblings.
        assert_eq!(
            trace,
            vec![
                *leaf_a.address(),
                *inner_dir.address(),
                *leaf_b.address(),
                *leaf_c.address(),
                *leaf_d.address()
            ]
        );
        let mut expected = ForkTable::new();
        for byte in [0x10, 0x11, 0x20, 0x21, 0x30, 0x40, 0x41] {
            expected
                .insert(prefix(&[byte]), entry(byte).into(), None)
                .unwrap();
        }
        assert_eq!(node, Node::new(None, expected));
    }

    /// Yields once per round before completing, so a test picks the order
    /// concurrent fetches land in.
    struct Yields(usize);

    impl Future for Yields {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 == 0 {
                Poll::Ready(())
            } else {
                self.0 -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// A trusted store that delays each address by its own round count and
    /// records the order fetches resolve in.
    struct SkewedStore {
        inner: ContentGet<MemoryStore>,
        rounds: alloc::collections::BTreeMap<ChunkAddress, usize>,
        arrivals: std::sync::Mutex<Vec<ChunkAddress>>,
    }

    impl ChunkGet<ContentOnlyChunkSet> for SkewedStore {
        type Trust = Verified;
        type Error = <ContentGet<MemoryStore> as ChunkGet<ContentOnlyChunkSet>>::Error;

        async fn get(&self, address: &ChunkAddress) -> Result<FetchedChunk, Self::Error> {
            Yields(self.rounds.get(address).copied().unwrap_or(0)).await;
            self.arrivals.lock().unwrap().push(*address);
            ChunkGet::get(&self.inner, address).await
        }
    }

    /// A leaf segment over `bytes`, stored and addressed.
    fn leaf_segment(store: &ContentGet<MemoryStore>, bytes: &[u8]) -> ChunkRef {
        let mut table = ForkTable::<V1>::new();
        for &byte in bytes {
            table
                .insert(prefix(&[byte]), entry(byte).into(), None)
                .unwrap();
        }
        put_raw(store, encode_leaf_segment(&table))
    }

    #[test]
    fn out_of_order_completions_still_fold_in_directory_order() {
        let store = ContentGet::new(MemoryStore::default());
        let leaf_a = leaf_segment(&store, &[0x10, 0x11]);
        let leaf_b = leaf_segment(&store, &[0x20, 0x21]);
        let leaf_c = leaf_segment(&store, &[0x30, 0x31]);
        let dir = SegmentDir::new(vec![
            (0x10, leaf_a, SubtreeCount::new(2)),
            (0x20, leaf_b, SubtreeCount::new(2)),
            (0x30, leaf_c, SubtreeCount::new(2)),
        ]);
        let root = put_raw(&store, encode_segmented_node::<V1, ChunkRef>(None, &dir));

        // The three fetches overlap and land last-first, so a fold in
        // completion order would invert the trace.
        let skewed = SkewedStore {
            rounds: [
                (*leaf_a.address(), 6),
                (*leaf_b.address(), 4),
                (*leaf_c.address(), 2),
            ]
            .into_iter()
            .collect(),
            inner: store,
            arrivals: std::sync::Mutex::new(Vec::new()),
        };
        let (node, trace) = run(materialize_traced::<_, V1, ChunkRef>(&skewed, &root)).unwrap();
        // The skew held: the segments really did land back to front.
        assert_eq!(
            skewed.arrivals.lock().unwrap().as_slice(),
            [
                *root.address(),
                *leaf_c.address(),
                *leaf_b.address(),
                *leaf_a.address()
            ]
        );
        assert_eq!(
            trace,
            vec![*leaf_a.address(), *leaf_b.address(), *leaf_c.address()]
        );
        let mut expected = ForkTable::new();
        for byte in [0x10, 0x11, 0x20, 0x21, 0x30, 0x31] {
            expected
                .insert(prefix(&[byte]), entry(byte).into(), None)
                .unwrap();
        }
        assert_eq!(node, Node::new(None, expected));
    }
}
