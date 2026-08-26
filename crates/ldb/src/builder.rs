//! Memory-bounded streaming builder: key entries -> assemble -> publish.
//!
//! The trie is assembled bottom-up over an explicit stack of open nodes. Each
//! finished node is embedded into its parent when the packing predicate allows,
//! otherwise sealed and dispatched into a bounded put window the moment it is
//! complete, so the peak retained node buffer count is the stack depth plus the
//! window, never the key count. Puts are order-free (content-derived
//! addresses), so siblings store concurrently and every put settles before the
//! root returns. The key set enters through a sorted map borrowed in place, so
//! the published tree is a pure function of the keys, identical whatever order
//! the caller streamed them in.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::marker::PhantomData;

use bytes::Bytes;
use nectar_governor::Window;
use nectar_primitives::store::{BoxedError, ChunkPut, MaybeSend, MaybeSync};
use nectar_primitives::{Chunk, ChunkRef};
use nectar_tasks::BoxFuture;

use crate::bounded::{Prefix, SegmentWeight};
use crate::codec::{
    SegmentDir, body_len, encode_dir_segment, encode_leaf_segment, encode_segmented_node,
    fork_count, record_weight, table_count,
};
use crate::count::SubtreeCount;
use crate::error::{ForkPrefixEmpty, PrefixTooLong, WeightOverBudget};
use crate::fork::{Child, ForkPayload, ForkRecord, ForkTable};
use crate::format::{Format, V1};
use crate::meta::Metadata;
use crate::node::{Node, NodeRef, RootExtension};
use crate::packing::{cut_allowance, embed, spill};
use crate::store::{Seal, StoreError};
use crate::value::{Entry, Key};

/// A build or publish failure.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// Sealing or storing a manifest node failed; over-budget nodes surface
    /// here as an encode error.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The backing store rejected a content chunk.
    #[error("store content chunk")]
    Backend(#[source] BoxedError),
    /// A compacted edge exceeded the format's prefix bound.
    #[error(transparent)]
    Prefix(#[from] PrefixTooLong),
    /// A fork prefix consumed no byte to index under.
    #[error(transparent)]
    EmptyPrefix(#[from] ForkPrefixEmpty),
    /// A single fork record outweighed a whole segment. Unreachable under the
    /// frozen bounds (worst record weight 2952 <= CAP_FORK 4091); a parameter
    /// drift trips it rather than corrupting silently.
    #[error(transparent)]
    Weight(#[from] WeightOverBudget),
    /// A stack invariant did not hold; a builder bug rather than bad input.
    #[error("builder invariant violated")]
    Internal,
}

impl BuildError {
    /// Box a backend error behind the seam.
    fn backend<E: core::error::Error + MaybeSend + MaybeSync + 'static>(err: E) -> Self {
        Self::Backend(Box::new(err))
    }
}

/// Peak and total work of one build, enough to witness the memory bound and
/// the put window's concurrency.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildStats {
    peak_open_nodes: usize,
    nodes_written: usize,
    nodes_embedded: usize,
    peak_in_flight: usize,
}

impl BuildStats {
    /// Most nodes ever open at once: the stack depth, which is the trie's node
    /// depth and independent of the key count.
    #[must_use]
    pub const fn peak_open_nodes(&self) -> usize {
        self.peak_open_nodes
    }

    /// Node chunks spilled to the store, including the root.
    #[must_use]
    pub const fn nodes_written(&self) -> usize {
        self.nodes_written
    }

    /// Subtrees inlined into their parent instead of spilled.
    #[must_use]
    pub const fn nodes_embedded(&self) -> usize {
        self.nodes_embedded
    }

    /// Peak puts ever parked in the window at once: above one when siblings
    /// overlap, zero when every put stored inline without taking a slot.
    #[must_use]
    pub const fn peak_in_flight(&self) -> usize {
        self.peak_in_flight
    }
}

/// The published manifest: the root reference and the build's work profile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Built<R: NodeRef = ChunkRef> {
    root: R,
    stats: BuildStats,
}

impl<R: NodeRef> Built<R> {
    /// The root reference a reader descends from; for an encrypted database
    /// this is the whole tree's read capability.
    #[must_use]
    pub const fn root(&self) -> &R {
        &self.root
    }

    /// The build's work profile.
    #[must_use]
    pub const fn stats(&self) -> &BuildStats {
        &self.stats
    }
}

/// Streaming manifest builder over key-value entries of format `F`.
///
/// Keys accumulate in a sorted map, so [`build`](Self::build) is
/// history-independent. The empty key carries the manifest's own value, distinct
/// from a fork.
#[derive(Clone, Debug)]
pub struct Builder<F: Format = V1> {
    keys: BTreeMap<Bytes, (Entry<F>, Option<Metadata<F>>)>,
    root_entry: Option<Entry<F>>,
    root_metadata: Option<Metadata<F>>,
}

impl<F: Format> Default for Builder<F> {
    fn default() -> Self {
        Self {
            keys: BTreeMap::new(),
            root_entry: None,
            root_metadata: None,
        }
    }
}

impl<F: Format> Builder<F> {
    /// An empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind `key` to `entry`, replacing any prior binding. The empty key sets
    /// the manifest's own value; its metadata, if any, becomes the manifest
    /// metadata.
    pub fn insert(
        &mut self,
        key: Key,
        entry: Entry<F>,
        metadata: Option<Metadata<F>>,
    ) -> &mut Self {
        if key.is_empty() {
            self.root_entry = Some(entry);
            if metadata.is_some() {
                self.root_metadata = metadata;
            }
        } else {
            self.keys.insert(key.into_bytes(), (entry, metadata));
        }
        self
    }

    /// Set the manifest-level metadata carried in the root extension.
    pub fn manifest_metadata(&mut self, metadata: Metadata<F>) -> &mut Self {
        self.root_metadata = Some(metadata);
        self
    }

    /// Assemble and publish the manifest, returning its root reference.
    ///
    /// `seal` decides the structural width: [`Plaintext`](crate::Plaintext) publishes
    /// a plaintext database keyed by [`ChunkRef`], an encrypted sealer one
    /// keyed by encrypted references. Peak retained node buffers stay at the
    /// trie's node depth: a finished subtree is embedded, or sealed and
    /// dispatched into the put window, before the next sibling opens. The
    /// returned reference covers a fully stored tree.
    pub async fn build<S, R, K>(&self, store: &S, seal: &K) -> Result<Built<R>, BuildError>
    where
        S: ChunkPut<Chunk> + MaybeSync,
        R: NodeRef,
        K: Seal<R>,
    {
        // Items borrow the sorted map: the descent indexes them without a
        // second owned copy of the key set.
        let items: Vec<Item<'_, F>> = self
            .keys
            .iter()
            .map(|(key, (entry, meta))| Item { key, entry, meta })
            .collect();
        let root_ext = RootExtension::new(self.root_entry.clone(), self.root_metadata.clone());
        let mut stats = BuildStats::default();
        let mut sink = PutSink::new(store, seal, put_window::<F>());
        let table = build_table_in(&mut sink, &items, 0, &mut stats).await?;
        let node = Node::new(root_ext, table);
        let root = emit_node_in(&mut sink, &node, &mut stats).await?;
        sink.settle().await?;
        Ok(Built { root, stats })
    }
}

/// The builder's put window: the format's read-ahead saturated into a nonzero
/// window, matching the segment reassembly and walk windows.
pub(crate) fn put_window<F: Format>() -> Window {
    let slots = u16::try_from(F::READ_AHEAD).unwrap_or(u16::MAX);
    Window::new(slots).unwrap_or(Window::DEFAULT)
}

/// A bounded window of node and segment puts riding the borrowed store.
///
/// A chunk's address is content-derived at seal, so a parent references a
/// child the moment it seals while the child's put still rides the window.
/// Puts are order-free, so the whole window admits; every put is settled
/// before the root is returned. Wraps the shared governor put-sink, sealing
/// chunks and mapping faults to [`BuildError`].
pub(crate) struct PutSink<'s, S: ChunkPut<Chunk> + MaybeSync, R: NodeRef, K: Seal<R>> {
    store: &'s S,
    seal: &'s K,
    sink: nectar_governor::PutSink<BoxFuture<'s, Result<(), BuildError>>>,
    _reference: PhantomData<R>,
}

impl<'s, S: ChunkPut<Chunk> + MaybeSync, R: NodeRef, K: Seal<R>> PutSink<'s, S, R, K> {
    /// A window admitting `window` puts at once over `store`, sealing every
    /// chunk with `seal`.
    pub(crate) fn new(store: &'s S, seal: &'s K, window: Window) -> Self {
        Self {
            store,
            seal,
            sink: nectar_governor::PutSink::new(window),
            _reference: PhantomData,
        }
    }

    /// The store the window rides, for a caller that fetches on the same seam.
    pub(crate) const fn store(&self) -> &'s S {
        self.store
    }

    /// Seal `node`, dispatch its put into the window, and return its
    /// reference. A backend fault surfaces as a store error.
    async fn put_node<F: Format>(
        &mut self,
        node: &Node<F, R>,
        stats: &mut BuildStats,
    ) -> Result<R, BuildError> {
        let payload = node.encode().map_err(StoreError::from)?;
        self.put_payload(payload, stats).await
    }

    /// Seal `payload` into its chunk, dispatch the put into the window, and
    /// return the reference that reaches it.
    async fn put_payload(
        &mut self,
        payload: Vec<u8>,
        stats: &mut BuildStats,
    ) -> Result<R, BuildError> {
        let (chunk, reference) = self.seal.seal(payload)?;
        let store = self.store;
        self.dispatch(
            Box::pin(async move { store.put(chunk).await.map_err(BuildError::backend) }),
            stats,
        )
        .await?;
        stats.nodes_written = stats.nodes_written.saturating_add(1);
        Ok(reference)
    }

    /// Admit `put`, dispatch it into the window, then sweep the puts ready now.
    /// Records the window's high-water occupancy so a build reports its put
    /// concurrency.
    async fn dispatch(
        &mut self,
        put: BoxFuture<'s, Result<(), BuildError>>,
        stats: &mut BuildStats,
    ) -> Result<(), BuildError> {
        if let Some(completion) = self.sink.admit().await {
            completion?;
        }
        if let Some(completion) = self.sink.push(put) {
            completion?;
        }
        stats.peak_in_flight = stats.peak_in_flight.max(self.sink.len());
        self.sink.sweep(|completion| completion).await
    }

    /// Await every outstanding put, so the returned root covers a fully stored
    /// tree.
    pub(crate) async fn settle(&mut self) -> Result<(), BuildError> {
        self.sink.settle(|completion| completion).await
    }
}

/// Assemble the top fork table for `items` at depth `consumed`, spilling each
/// finished subtree into the shared put window as it closes so siblings store
/// concurrently.
///
/// The returned table is the caller's to wrap: a root wears its extension and
/// always spills, a subtree defers its own embed decision to [`resolve_in`].
pub(crate) async fn build_table_in<'a, S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    items: &'a [Item<'a, F>],
    consumed: usize,
    stats: &mut BuildStats,
) -> Result<ForkTable<F, R>, BuildError>
where
    S: ChunkPut<Chunk> + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let mut stack: Vec<Frame<'a, F, R>> = Vec::new();
    stack.push(Frame::new(consumed, items));
    let mut returned: Option<Resolved<F, R>> = None;

    loop {
        stats.peak_open_nodes = stats.peak_open_nodes.max(stack.len());
        let action = {
            let frame = stack.last_mut().ok_or(BuildError::Internal)?;
            if let Some(resolved) = returned.take() {
                frame.attach(resolved)?;
            }
            frame.step()?
        };
        match action {
            Action::Continue => {}
            Action::Descend(child_items, plen) => stack.push(Frame::new(plen, child_items)),
            Action::Finalize => {
                let frame = stack.pop().ok_or(BuildError::Internal)?;
                if stack.is_empty() {
                    return Ok(frame.table);
                }
                returned = Some(resolve_in(sink, frame.table, stats).await?);
            }
        }
    }
}

/// One key-value binding, borrowed from the sorted map for indexed descent.
pub(crate) struct Item<'a, F: Format> {
    pub(crate) key: &'a Bytes,
    pub(crate) entry: &'a Entry<F>,
    pub(crate) meta: &'a Option<Metadata<F>>,
}

/// A resolved subtree bubbling up to its parent fork.
pub(crate) enum Resolved<F: Format, R: NodeRef> {
    /// Small enough to inline into the parent's chunk.
    Embedded(ForkTable<F, R>),
    /// Spilled to a chunk of its own, carrying its subtree count so the
    /// parent fork stamps it on the reference.
    Reference(R, Option<SubtreeCount>),
}

impl<F: Format, R: NodeRef> Resolved<F, R> {
    /// The child a parent fork holds for this resolved subtree.
    pub(crate) fn into_child(self) -> Child<F, R> {
        match self {
            Self::Embedded(table) => Child::Embedded(table),
            Self::Reference(reference, _) => Child::Ref(reference),
        }
    }

    /// The subtree count to stamp on the parent fork: present only for a
    /// referenced child.
    pub(crate) const fn child_count(&self) -> Option<SubtreeCount> {
        match self {
            Self::Embedded(_) => None,
            Self::Reference(_, count) => *count,
        }
    }
}

/// A fork awaiting the subtree currently under construction.
struct OpenFork<F: Format> {
    prefix: Prefix<F>,
    entry: Option<Entry<F>>,
    meta: Option<Metadata<F>>,
}

/// One node under construction: the keys below it, the cursor into them, the
/// table built so far, and the fork whose child is open.
struct Frame<'a, F: Format, R: NodeRef> {
    consumed: usize,
    items: &'a [Item<'a, F>],
    cursor: usize,
    table: ForkTable<F, R>,
    open: Option<OpenFork<F>>,
}

/// What the driver does after one frame step.
enum Action<'a, F: Format> {
    /// A terminal fork was inserted; keep processing this frame.
    Continue,
    /// A fork opened onto the given child keys at the given consumed depth.
    Descend(&'a [Item<'a, F>], usize),
    /// The frame is complete.
    Finalize,
}

impl<'a, F: Format, R: NodeRef> Frame<'a, F, R> {
    const fn new(consumed: usize, items: &'a [Item<'a, F>]) -> Self {
        Self {
            consumed,
            items,
            cursor: 0,
            table: ForkTable::new(),
            open: None,
        }
    }

    /// Close the open fork with its resolved child, stamping the referenced
    /// child's subtree count onto the record.
    fn attach(&mut self, resolved: Resolved<F, R>) -> Result<(), BuildError> {
        let open = self.open.take().ok_or(BuildError::Internal)?;
        let count = resolved.child_count();
        let child = resolved.into_child();
        let payload = match open.entry {
            Some(entry) => ForkPayload::Both { entry, child },
            None => ForkPayload::Child(child),
        };
        let (first, mut record) = ForkRecord::new(open.prefix, payload, open.meta)?;
        record.set_child_count(count);
        self.table.insert_record(first, record);
        Ok(())
    }

    /// Insert the next terminal fork, or open the next child, or finalize.
    fn step(&mut self) -> Result<Action<'a, F>, BuildError> {
        if self.cursor >= self.items.len() {
            return Ok(Action::Finalize);
        }
        let group = next_group(self.items, self.cursor, self.consumed)?;
        self.cursor = group.end;
        match group.child {
            None => {
                let entry = group.entry.ok_or(BuildError::Internal)?;
                self.table
                    .insert(group.prefix, ForkPayload::Entry(entry), group.meta)?;
                Ok(Action::Continue)
            }
            Some(child_items) => {
                self.open = Some(OpenFork {
                    prefix: group.prefix,
                    entry: group.entry,
                    meta: group.meta,
                });
                Ok(Action::Descend(child_items, group.plen))
            }
        }
    }
}

/// The fork run sharing the byte at `consumed`, starting at `cursor`.
struct Group<'a, F: Format> {
    prefix: Prefix<F>,
    plen: usize,
    entry: Option<Entry<F>>,
    meta: Option<Metadata<F>>,
    child: Option<&'a [Item<'a, F>]>,
    end: usize,
}

/// Cut the next fork out of `items`: the run sharing the byte at `consumed`,
/// its compacted edge (capped at the prefix bound), the value of any key that
/// terminates on the edge, and the child keys that continue past it.
fn next_group<'a, F: Format>(
    items: &'a [Item<'a, F>],
    cursor: usize,
    consumed: usize,
) -> Result<Group<'a, F>, BuildError> {
    let first = items.get(cursor).ok_or(BuildError::Internal)?;
    let byte = first
        .key
        .get(consumed)
        .copied()
        .ok_or(BuildError::Internal)?;

    let mut end = cursor.saturating_add(1);
    while let Some(item) = items.get(end) {
        match item.key.get(consumed).copied() {
            Some(next) if next == byte => end = end.saturating_add(1),
            _ => break,
        }
    }

    let last = items
        .get(end.saturating_sub(1))
        .ok_or(BuildError::Internal)?;
    let lcp = common_prefix_len(first.key, last.key, consumed, cut_allowance::<F>(consumed));
    let plen = consumed.saturating_add(lcp);
    let edge = first.key.get(consumed..plen).ok_or(BuildError::Internal)?;
    let prefix = Prefix::try_from(edge)?;

    let terminates = first.key.len() == plen;
    let child_start = if terminates {
        cursor.saturating_add(1)
    } else {
        cursor
    };
    let child = items.get(child_start..end).filter(|run| !run.is_empty());
    let (entry, meta) = if terminates {
        (Some(first.entry.clone()), first.meta.clone())
    } else {
        (None, None)
    };

    Ok(Group {
        prefix,
        plen,
        entry,
        meta,
        child,
        end,
    })
}

/// The shared byte run of `a` and `b` from `consumed`, capped at `cap`. At
/// least one: both share the byte at `consumed` by construction.
fn common_prefix_len(a: &Bytes, b: &Bytes, consumed: usize, cap: usize) -> usize {
    let tail_a = a.get(consumed..).unwrap_or_default();
    let tail_b = b.get(consumed..).unwrap_or_default();
    let mut len = 0usize;
    for (x, y) in tail_a.iter().zip(tail_b.iter()) {
        if len >= cap || x != y {
            break;
        }
        len = len.saturating_add(1);
    }
    len
}

/// Embed a finished subtree into its parent, or spill it through the put window.
///
/// The embed decision is child-local: it reads the subtree's flat length alone,
/// so it is stable under re-rooting and history-independent.
pub(crate) async fn resolve_in<S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    table: ForkTable<F, R>,
    stats: &mut BuildStats,
) -> Result<Resolved<F, R>, BuildError>
where
    S: ChunkPut<Chunk> + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let flat = Node::new(None, table.clone())
        .encoded_len()
        .saturating_sub(F::PREAMBLE.len());
    if embed::<F>(flat) {
        stats.nodes_embedded = stats.nodes_embedded.saturating_add(1);
        return Ok(Resolved::Embedded(table));
    }
    // The spilled subtree's count is the in-buffer sum of its fork counts, read
    // before the table is consumed; the parent stamps it on the reference.
    let count = Some(SubtreeCount::new(table_count(&table)));
    let node = Node::new(None, table);
    let reference = emit_node_in(sink, &node, stats).await?;
    Ok(Resolved::Reference(reference, count))
}

/// Publish `node` as one chunk, or, when its flat body overruns the format
/// budget, spill it into a segment directory of sub-chunks that each fit, every
/// part riding the shared put window.
///
/// The single-chunk-node invariant holds here by construction: a body within
/// `F::BUDGET` seals as one node, and a wider body partitions at content-defined
/// boundaries into leaf and directory segments no larger than one chunk. The
/// `OverBudget` guard on [`Node::encode`] therefore stays unreachable on this
/// path.
pub(crate) async fn emit_node_in<S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    node: &Node<F, R>,
    stats: &mut BuildStats,
) -> Result<R, BuildError>
where
    S: ChunkPut<Chunk> + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    if body_len(node) <= F::BUDGET {
        return sink.put_node(node, stats).await;
    }
    spill_node_in(sink, node, stats).await
}

/// Spill an over-budget node into a `<=` depth-two segment directory, storing
/// each leaf and directory segment and returning the segmented node's address.
async fn spill_node_in<S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    node: &Node<F, R>,
    stats: &mut BuildStats,
) -> Result<R, BuildError>
where
    S: ChunkPut<Chunk> + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let forks: Vec<(u8, &ForkRecord<F, R>)> = node.forks().iter().collect();
    let mut items: Vec<(Prefix<F>, SegmentWeight<F>)> = Vec::with_capacity(forks.len());
    for (first, record) in &forks {
        let mut full = Vec::with_capacity(record.tail().len().saturating_add(1));
        full.push(*first);
        full.extend_from_slice(record.tail().as_bytes());
        items.push((
            Prefix::try_from(full.as_slice())?,
            SegmentWeight::new(record_weight(record))?,
        ));
    }

    let directory = spill::<F, R>(&items);
    let mut leaf_descs: Vec<(u8, R, SubtreeCount)> = Vec::with_capacity(directory.leaves().len());
    for range in directory.leaves() {
        let slice = forks.get(range.clone()).ok_or(BuildError::Internal)?;
        let &(first_key, _) = slice.first().ok_or(BuildError::Internal)?;
        let mut leaf = ForkTable::new();
        for (byte, record) in slice {
            leaf.insert_record(*byte, (*record).clone());
        }
        // The descriptor routes the segment's whole subtree count: the sum of
        // its covered forks' counts, so a reader descends by rank without a
        // fetch.
        let seg_count = descriptor_count(slice.iter().map(|(_, record)| fork_count(record)));
        let reference = sink.put_payload(encode_leaf_segment(&leaf), stats).await?;
        leaf_descs.push((first_key, reference, seg_count));
    }

    let top = if directory.dirs().len() <= 1 {
        SegmentDir::new(leaf_descs)
    } else {
        let mut dir_descs: Vec<(u8, R, SubtreeCount)> = Vec::with_capacity(directory.dirs().len());
        for range in directory.dirs() {
            let group = leaf_descs.get(range.clone()).ok_or(BuildError::Internal)?;
            let first_key = group.first().ok_or(BuildError::Internal)?.0;
            let seg_count = descriptor_count(group.iter().map(|(_, _, count)| count.get()));
            let reference = sink
                .put_payload(
                    encode_dir_segment::<F, R>(&SegmentDir::new(group.to_vec())),
                    stats,
                )
                .await?;
            dir_descs.push((first_key, reference, seg_count));
        }
        SegmentDir::new(dir_descs)
    };

    sink.put_payload(encode_segmented_node::<F, R>(node.root(), &top), stats)
        .await
}

/// The subtree count a segment descriptor routes: the sum of the covered
/// forks' (or nested descriptors') counts.
fn descriptor_count(counts: impl Iterator<Item = u64>) -> SubtreeCount {
    SubtreeCount::new(counts.fold(0, u64::saturating_add))
}
