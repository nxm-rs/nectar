//! History-independent batch update: fold a changeset into a manifest to a new
//! root that is byte-identical to building the merged key set from scratch.
//!
//! The update is a bottom-up path-copy union: only the nodes on the touched
//! paths are rewritten, and a shared ancestor is rewritten once per apply, not
//! once per changeset entry, so a wide batch amortizes over its overlap. An
//! unchanged fork is spliced in verbatim; an untouched referenced subtree is
//! reused by address without a fetch, bar the one read that runs a lone
//! continuation back into an edge that lost its terminal. Embedding is
//! child-local and a cut is keyed on the fork-relative prefix, so a reused
//! subtree keeps its shape;
//! the forced `PLEN_MAX` cap is anchored to the absolute key offset, so a
//! re-rooted or merged edge re-compacts into the same chain a build at the new
//! depth would. Hence `apply(root, delta)` and a from-scratch build of the
//! merged keys agree bit for bit (invariant I6 under updates).
//!
//! Node puts across the whole changeset ride one bounded window, and each level
//! prefetches the referenced children its groups descend into on a second
//! bounded window, so disjoint changed subtrees read and write concurrently.
//! Peak retained state is O(depth + changeset frontier + window): the descent
//! holds one node per level on the current path plus a level's prefetched
//! children, never a whole subtree.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::Poll;

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, Stream};
use nectar_governor::{Admission, BoxFuture, Window};
use nectar_primitives::store::{ChunkPut, MaybeSync};

use crate::bounded::Prefix;
use crate::builder::{
    BuildError, BuildStats, Item, PutSink, build_table_in, emit_node_in, put_window, resolve_in,
};
use crate::count::SubtreeCount;
use crate::error::{ForkPrefixEmpty, PrefixTooLong};
use crate::fork::{Child, ForkPayload, ForkRecord, ForkTable};
use crate::format::{Format, V1};
use crate::meta::Metadata;
use crate::node::{Node, NodeRef, RootExtension};
use crate::packing::cut_allowance;
use crate::store::{NodeGet, Seal, StoreError};
use crate::value::{Entry, Key};

/// One key's update within a changeset.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Op<F: Format> {
    /// Bind the key to a value, with optional metadata.
    Insert {
        /// The value to bind.
        entry: Entry<F>,
        /// The value's metadata, if any.
        meta: Option<Metadata<F>>,
    },
    /// Remove the key.
    Delete,
}

/// A batch of key updates to fold into a manifest in one pass.
///
/// Keys accumulate in a sorted map, so an [`apply`] is history-independent: the
/// order updates were staged in never reaches the produced root. The empty key
/// carries the manifest's own value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changeset<F: Format = V1> {
    ops: BTreeMap<Bytes, Op<F>>,
}

impl<F: Format> Default for Changeset<F> {
    fn default() -> Self {
        Self {
            ops: BTreeMap::new(),
        }
    }
}

impl<F: Format> Changeset<F> {
    /// An empty changeset.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage a binding of `key` to `entry`, replacing any staged update for it.
    /// The empty key sets the manifest's own value; its metadata, if any,
    /// becomes the manifest metadata.
    pub fn insert(
        &mut self,
        key: Key,
        entry: Entry<F>,
        metadata: Option<Metadata<F>>,
    ) -> &mut Self {
        self.ops.insert(
            key.into_bytes(),
            Op::Insert {
                entry,
                meta: metadata,
            },
        );
        self
    }

    /// Stage the removal of `key`, replacing any staged update for it.
    pub fn remove(&mut self, key: Key) -> &mut Self {
        self.ops.insert(key.into_bytes(), Op::Delete);
        self
    }

    /// Number of staged updates.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns `true` when nothing is staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// An apply failure.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    /// Loading or storing a node across the store seam failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Building or spilling a rewritten subtree failed.
    #[error(transparent)]
    Build(#[from] BuildError),
    /// A rewritten edge exceeded the format's prefix bound.
    #[error(transparent)]
    Prefix(#[from] PrefixTooLong),
    /// A fork prefix consumed no byte to index under.
    #[error(transparent)]
    EmptyPrefix(#[from] ForkPrefixEmpty),
    /// A merge invariant did not hold; an apply bug rather than bad input.
    #[error("apply invariant violated")]
    Internal,
}

/// Fold `changeset` into the database rooted at `root`, returning the new
/// root reference.
///
/// `seal` publishes the rewritten nodes at the same structural width `root`
/// arrived at, so an encrypted database stays encrypted across an update. The
/// result equals a from-scratch build of the merged key set, byte for byte: an
/// empty changeset returns `root` unchanged, and a single update is just a
/// one-entry changeset.
///
/// An untouched subtree is spliced in verbatim, key and all, so an encrypted
/// `seal` must carry the secret the base tree was sealed under; a different one
/// still reads back, but the result no longer matches a from-scratch build.
pub async fn apply<S, F, R, K>(
    store: &S,
    seal: &K,
    root: &R,
    changeset: &Changeset<F>,
) -> Result<R, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    if changeset.is_empty() {
        return Ok(root.clone());
    }
    let node = store.get_node::<F, R>(root).await?;

    // The empty key is the root's own value; every other key descends the trie.
    let mut root_entry = node.entry().cloned();
    let mut root_meta = node.metadata().cloned();
    match changeset.ops.get(&Bytes::new()) {
        Some(Op::Insert { entry, meta }) => {
            root_entry = Some(entry.clone());
            if meta.is_some() {
                root_meta = meta.clone();
            }
        }
        Some(Op::Delete) => root_entry = None,
        None => {}
    }
    let root_ext = RootExtension::new(root_entry, root_meta);

    let changes: Vec<Change<'_, F>> = changeset
        .ops
        .iter()
        .filter(|(key, _)| !key.is_empty())
        .map(|(key, op)| Change {
            key: key.clone(),
            op,
        })
        .collect();

    // One put window over the whole changeset. Puts are order-free
    // (content-derived addresses), so it admits freely and every put settles
    // before the root returns.
    let mut sink = PutSink::new(store, seal, put_window::<F>());
    let mut stats = BuildStats::default();
    let forks = Box::pin(apply_forks(
        &mut sink,
        node.forks().clone(),
        0,
        &changes,
        &mut stats,
    ))
    .await?;
    let new_node = Node::new(root_ext, forks);
    let root = emit_node_in(&mut sink, &new_node, &mut stats).await?;
    sink.settle().await?;
    Ok(root)
}

/// One staged update paired with its key, borrowed for the length of the apply.
struct Change<'c, F: Format> {
    key: Bytes,
    op: &'c Op<F>,
}

impl<F: Format> Change<'_, F> {
    /// A cheap re-borrow, so a subset can be routed into a child without cloning
    /// the operation.
    fn reborrow(&self) -> Change<'_, F> {
        Change {
            key: self.key.clone(),
            op: self.op,
        }
    }
}

/// Merge `changes` into the fork table at depth `consumed`, rewriting only the
/// forks a change touches and splicing the rest in verbatim.
///
/// Every change shares the `consumed`-byte prefix that reaches this table, so a
/// change group is the contiguous run sharing the byte at `consumed`.
async fn apply_forks<'c, S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    mut table: ForkTable<F, R>,
    consumed: usize,
    changes: &[Change<'c, F>],
    stats: &mut BuildStats,
) -> Result<ForkTable<F, R>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let groups = group_changes(consumed, changes);

    // Fetch every referenced child a group descends into up front on one bounded
    // window; the rewrite below consumes each in group order. A missed or failed
    // prefetch falls back to an inline descent fetch, so the result is
    // byte-identical.
    let mut fetched = prefetch_children::<S, F, R>(
        sink.store(),
        groups
            .iter()
            .enumerate()
            .filter_map(|(slot, &(i, j, byte))| {
                let group = changes.get(i..j)?;
                descent_child(consumed, byte, table.get(byte)?, group)
                    .map(|reference| (slot, reference))
            }),
        groups.len(),
    )
    .await;

    for (slot, &(i, j, byte)) in groups.iter().enumerate() {
        let group = changes.get(i..j).ok_or(ApplyError::Internal)?;
        let existing = table.remove(byte);
        let child = fetched.get_mut(slot).and_then(Option::take).transpose()?;
        if let Some(record) = Box::pin(reconcile(
            sink, consumed, byte, existing, group, child, stats,
        ))
        .await?
        {
            table.insert_record(byte, record);
        }
    }
    Ok(table)
}

/// The change groups at depth `consumed`: each `(start, end, byte)` is the
/// contiguous run of changes sharing the byte at `consumed`. Keys too short to
/// index here belong to the parent boundary and are skipped.
fn group_changes<F: Format>(consumed: usize, changes: &[Change<'_, F>]) -> Vec<(usize, usize, u8)> {
    let mut groups = Vec::new();
    let mut i = 0usize;
    while let Some(first) = changes.get(i) {
        let Some(&byte) = first.key.get(consumed) else {
            i = i.saturating_add(1);
            continue;
        };
        let mut j = i.saturating_add(1);
        while changes.get(j).and_then(|c| c.key.get(consumed)) == Some(&byte) {
            j = j.saturating_add(1);
        }
        groups.push((i, j, byte));
        i = j;
    }
    groups
}

/// The referenced child a group descends into, or `None` when its reconcile
/// splits within the edge, stays on the boundary, or has no referenced child to
/// fetch. Mirrors the head of [`reconcile`]/[`descend`], so a returned
/// reference is exactly the descent's inline fetch.
fn descent_child<F: Format, R: NodeRef>(
    consumed: usize,
    byte: u8,
    existing: &ForkRecord<F, R>,
    group: &[Change<'_, F>],
) -> Option<R> {
    let Some(Child::Ref(reference)) = existing.child() else {
        return None;
    };
    let mut edge = Vec::with_capacity(existing.tail().len().saturating_add(1));
    edge.push(byte);
    edge.extend_from_slice(existing.tail().as_bytes());
    let plen = consumed.saturating_add(edge.len());
    for change in group {
        if let Op::Insert { .. } = change.op {
            let suffix = change.key.get(consumed..).unwrap_or_default();
            if common_prefix(suffix, &edge) < edge.len() {
                // An insertion diverges within the edge: a split, not a descent.
                return None;
            }
        }
    }
    // A change past the edge is what folds into the child; a bare terminal
    // update or off-edge deletion never reads it.
    let deeper = group.iter().any(|change| {
        let suffix = change.key.get(consumed..).unwrap_or_default();
        suffix.starts_with(&edge) && change.key.len() > plen
    });
    deeper.then(|| reference.clone())
}

/// Reconcile the fork indexed under `byte` with its change group, returning the
/// rewritten fork or `None` when it collapses away.
async fn reconcile<'c, S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    consumed: usize,
    byte: u8,
    existing: Option<ForkRecord<F, R>>,
    group: &[Change<'c, F>],
    child: Option<Node<F, R>>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F, R>>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let existing = match existing {
        Some(record) => record,
        None => {
            // No fork here yet: build one from the group's insertions alone.
            let items = inserts_to_items(group);
            if items.is_empty() {
                return Ok(None);
            }
            let mut fresh = build_table_in(sink, &items, consumed, stats).await?;
            return Ok(fresh.remove(byte));
        }
    };

    // The fork's full edge: the index byte followed by its stored tail.
    let mut edge = Vec::with_capacity(existing.tail().len().saturating_add(1));
    edge.push(byte);
    edge.extend_from_slice(existing.tail().as_bytes());

    // The merged key set's compacted edge shortens to the least point any
    // insertion diverges from the existing edge; deletions off the edge target
    // no existing key and never move it.
    let mut cut = edge.len();
    for change in group {
        if let Op::Insert { .. } = change.op {
            let suffix = change.key.get(consumed..).unwrap_or_default();
            cut = cut.min(common_prefix(suffix, &edge));
        }
    }

    if cut < edge.len() {
        split(sink, consumed, &edge, cut, existing, group, stats).await
    } else {
        Box::pin(descend(
            sink, consumed, &edge, existing, group, child, stats,
        ))
        .await
    }
}

/// The existing edge stays intact: update the terminal value and fold the
/// deeper updates into the child.
async fn descend<'c, S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    consumed: usize,
    edge: &[u8],
    existing: ForkRecord<F, R>,
    group: &[Change<'c, F>],
    child: Option<Node<F, R>>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F, R>>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    // The absolute key offset past the edge: where a deeper change forks off.
    let plen = consumed.saturating_add(edge.len());
    let mut new_entry = existing.entry().cloned();
    let mut new_meta = existing.metadata().cloned();
    let mut deeper: Vec<Change<'_, F>> = Vec::new();
    for change in group {
        let suffix = change.key.get(consumed..).unwrap_or_default();
        if !suffix.starts_with(edge) {
            // Diverges off the intact edge: a deletion of an absent key.
            continue;
        }
        if change.key.len() == plen {
            match change.op {
                Op::Insert { entry, meta } => {
                    new_entry = Some(entry.clone());
                    new_meta = meta.clone();
                }
                Op::Delete => {
                    new_entry = None;
                    new_meta = None;
                }
            }
        } else {
            deeper.push(change.reborrow());
        }
    }

    if deeper.is_empty() {
        // Losing the terminal can leave a lone referenced continuation that a
        // build would have run on into the edge; nothing else here can.
        if new_entry.is_none()
            && let Some((merged, absorbed)) =
                absorb(sink.store(), consumed, edge, existing.child()).await?
        {
            let child = Counted {
                child: absorbed.child().cloned(),
                count: absorbed.child_count(),
            };
            // The merged edge lands on the forced cut, so the absorbed fork is
            // already the boundary a build places: no further compaction.
            return settle(
                &merged,
                absorbed.entry().cloned(),
                absorbed.metadata().cloned(),
                child,
            );
        }
        // The child is untouched: reuse it verbatim, carrying its stored count.
        let child = Counted {
            child: existing.child().cloned(),
            count: existing.child_count(),
        };
        return finish(sink, consumed, edge, new_entry, new_meta, child, stats).await;
    }

    let child_table = match existing.child() {
        None => {
            let items = inserts_to_items(&deeper);
            if items.is_empty() {
                // A deletion of an absent deeper key: the fork is unchanged bar
                // its terminal value.
                return finish(
                    sink,
                    consumed,
                    edge,
                    new_entry,
                    new_meta,
                    Counted::none(),
                    stats,
                )
                .await;
            }
            build_table_in(sink, &items, plen, stats).await?
        }
        Some(Child::Embedded(inner)) => {
            Box::pin(apply_forks(sink, inner.clone(), plen, &deeper, stats)).await?
        }
        Some(Child::Ref(reference)) => {
            // The prefetch supplies this exact node when it landed; otherwise
            // the read runs here.
            let node = match child {
                Some(node) => node,
                None => sink.store().get_node::<F, R>(reference).await?,
            };
            Box::pin(apply_forks(
                sink,
                node.forks().clone(),
                plen,
                &deeper,
                stats,
            ))
            .await?
        }
    };
    assemble(
        sink,
        consumed,
        edge,
        new_entry,
        new_meta,
        child_table,
        stats,
    )
    .await
}

/// A fork's child paired with the subtree count that rides it when it is a
/// reference.
struct Counted<F: Format, R: NodeRef> {
    /// The child, or `None` for a leaf fork.
    child: Option<Child<F, R>>,
    /// The count stamped on a referenced child.
    count: Option<SubtreeCount>,
}

impl<F: Format, R: NodeRef> Counted<F, R> {
    /// No child, and so no count.
    const fn none() -> Self {
        Self {
            child: None,
            count: None,
        }
    }
}

/// Fold a rewritten child table back into a fork record over `edge`, which
/// starts at absolute key offset `at`, collapsing an empty or single-fork child
/// so the result matches a from-scratch build.
///
/// The single-fork merge runs before the child is resolved, so a lone branch
/// re-inlines whatever its size would spill to.
async fn assemble<S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    at: usize,
    edge: &[u8],
    entry: Option<Entry<F>>,
    meta: Option<Metadata<F>>,
    table: ForkTable<F, R>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F, R>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    if table.is_empty() {
        return finish(sink, at, edge, entry, meta, Counted::none(), stats).await;
    }
    // Edge-compaction: a child-only fork over a single-fork child merges into
    // one edge, exactly as a from-scratch build would compact the shared run.
    if entry.is_none()
        && table.len() == 1
        && let Some((first, record)) = table.iter().next()
    {
        return compact(sink, at, edge, first, record, stats).await;
    }
    let resolved = resolve_in(sink, table, stats).await?;
    let child = Counted {
        count: resolved.child_count(),
        child: Some(resolved.into_child()),
    };
    finish(sink, at, edge, entry, meta, child, stats).await
}

/// An insertion diverges within the edge: branch at the divergence, re-rooting
/// the existing subtree verbatim under the edge remainder.
async fn split<'c, S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    consumed: usize,
    edge: &[u8],
    cut: usize,
    existing: ForkRecord<F, R>,
    group: &[Change<'c, F>],
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F, R>>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let boundary = consumed.saturating_add(cut);
    let new_edge = edge.get(..cut).ok_or(ApplyError::Internal)?;
    let mut split_entry: Option<Entry<F>> = None;
    let mut split_meta: Option<Metadata<F>> = None;
    let mut remaining: Vec<Change<'_, F>> = Vec::new();
    for change in group {
        let suffix = change.key.get(consumed..).unwrap_or_default();
        if !suffix.starts_with(new_edge) {
            // Diverges above the branch point: a deletion of an absent key.
            // Every insertion shares the branch edge by construction.
            continue;
        }
        if change.key.len() == boundary {
            if let Op::Insert { entry, meta } = change.op {
                split_entry = Some(entry.clone());
                split_meta = meta.clone();
            }
            // A deletion at the new boundary targets no existing key: drop it.
        } else {
            remaining.push(change.reborrow());
        }
    }

    // The existing subtree hangs under the remainder of its edge, spliced in
    // verbatim: anchoring keeps every cut below the split in place.
    let mut branch = ForkTable::new();
    let remainder = edge.get(cut..).ok_or(ApplyError::Internal)?;
    let first = *remainder.first().ok_or(ApplyError::Internal)?;
    if let Some(record) = reroot(remainder, existing)? {
        branch.insert_record(first, record);
    }
    let table = Box::pin(apply_forks(sink, branch, boundary, &remaining, stats)).await?;
    assemble(
        sink,
        consumed,
        new_edge,
        split_entry,
        split_meta,
        table,
        stats,
    )
    .await
}

/// Re-root an existing fork under a shortened `remainder` edge. Anchoring
/// leaves every cut below the split where it was, so the fork re-roots
/// verbatim.
fn reroot<F: Format, R: NodeRef>(
    remainder: &[u8],
    existing: ForkRecord<F, R>,
) -> Result<Option<ForkRecord<F, R>>, ApplyError> {
    make_fork(
        remainder,
        existing.payload().clone(),
        existing.metadata().cloned(),
        existing.child_count(),
    )
}

/// Assemble a fork record from an intact edge starting at absolute key offset
/// `at`, its terminal value and its child, or `None` when neither survives.
///
/// A child-only fork over a single-fork embedded child compacts into one edge,
/// so a deletion that strips a fork's terminal value re-inlines its lone
/// remaining branch exactly as a from-scratch build would.
async fn finish<S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    at: usize,
    edge: &[u8],
    entry: Option<Entry<F>>,
    meta: Option<Metadata<F>>,
    child: Counted<F, R>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F, R>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    if entry.is_none()
        && let Some(Child::Embedded(table)) = &child.child
        && table.len() == 1
        && let Some((first, record)) = table.iter().next()
    {
        return compact(sink, at, edge, first, record, stats).await;
    }
    settle(edge, entry, meta, child)
}

/// A fork record over `edge` from its parts, dropping metadata that no terminal
/// value carries, or `None` when neither a value nor a child survives.
fn settle<F: Format, R: NodeRef>(
    edge: &[u8],
    entry: Option<Entry<F>>,
    meta: Option<Metadata<F>>,
    child: Counted<F, R>,
) -> Result<Option<ForkRecord<F, R>>, ApplyError> {
    let Counted { child, count } = child;
    let has_entry = entry.is_some();
    ForkPayload::new(entry, child).map_or_else(
        || Ok(None),
        |payload| make_fork(edge, payload, if has_entry { meta } else { None }, count),
    )
}

/// Merge a child-only `edge`, which starts at absolute key offset `at`, into its
/// lone child fork (index byte `first` plus `record`), emitting the compacted
/// fork a from-scratch build would produce.
///
/// One hop suffices: anchoring pins every cut below the merge to the same
/// absolute offsets a build places, so the merged run re-segments into a
/// canonical chain and the record's own boundary stays where it was.
async fn compact<S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    at: usize,
    edge: &[u8],
    first: u8,
    record: &ForkRecord<F, R>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F, R>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let mut merged = edge.to_vec();
    merged.push(first);
    merged.extend_from_slice(record.tail().as_bytes());
    chain(
        sink,
        at,
        &merged,
        record.payload().clone(),
        record.metadata().cloned(),
        record.child_count(),
        stats,
    )
    .await
}

/// Absorb the lone referenced continuation of a child-only `edge`, which starts
/// at absolute key offset `at`, returning the merged edge and the continuation's
/// own fork record.
///
/// A build runs an edge on to its forced cut, so an edge that stops short of one
/// carrying no terminal value and a single continuation must swallow that
/// continuation's edge. Anchoring bounds this to one hop: the merged edge lands
/// exactly on the forced cut, so everything below it is already canonical and
/// keeps its references untouched.
///
/// `None` when no merge applies, and then nothing was fetched: an edge already
/// at its forced cut short-circuits before the child is read at all, so
/// splitting and re-rooting stay fetch-free.
async fn absorb<S, F, R>(
    store: &S,
    at: usize,
    edge: &[u8],
    child: Option<&Child<F, R>>,
) -> Result<Option<(Vec<u8>, ForkRecord<F, R>)>, ApplyError>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    // The edge already reaches its forced cut, so the boundary is the one a
    // build places: nothing to absorb, and no read.
    if edge.len() >= cut_allowance::<F>(at) {
        return Ok(None);
    }
    let reference = match child {
        Some(Child::Ref(reference)) => reference,
        Some(Child::Embedded(_)) | None => return Ok(None),
    };
    let node = store.get_node::<F, R>(reference).await?;
    // A branch below is a boundary a build keeps; only a lone continuation runs
    // on into the edge.
    if node.forks().len() != 1 {
        return Ok(None);
    }
    let (first, record) = node.forks().iter().next().ok_or(ApplyError::Internal)?;
    let mut merged = Vec::with_capacity(
        edge.len()
            .saturating_add(record.tail().len())
            .saturating_add(1),
    );
    merged.extend_from_slice(edge);
    merged.push(first);
    merged.extend_from_slice(record.tail().as_bytes());
    Ok(Some((merged, record.clone())))
}

/// A fork record over `prefix`, which starts at absolute key offset `at`, split
/// into a chain of child-only nodes when it overruns the forced cap, exactly as
/// the builder compacts an over-long shared run. The cap is anchored to `at`
/// through [`cut_allowance`], so a re-rooted run keeps a build's boundaries. The
/// innermost fork carries the payload and its metadata; every wrapping fork
/// carries only the continuation.
async fn chain<S, F, R, K>(
    sink: &mut PutSink<'_, S, R, K>,
    at: usize,
    prefix: &[u8],
    payload: ForkPayload<F, R>,
    meta: Option<Metadata<F>>,
    child_count: Option<SubtreeCount>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F, R>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
    R: NodeRef,
    K: Seal<R>,
{
    let allowed = cut_allowance::<F>(at);
    if prefix.len() <= allowed {
        return make_fork(prefix, payload, meta, child_count);
    }
    let head = prefix.get(..allowed).ok_or(ApplyError::Internal)?;
    let rest = prefix.get(allowed..).ok_or(ApplyError::Internal)?;
    let &first = rest.first().ok_or(ApplyError::Internal)?;
    let inner = Box::pin(chain(
        sink,
        at.saturating_add(allowed),
        rest,
        payload,
        meta,
        child_count,
        stats,
    ))
    .await?
    .ok_or(ApplyError::Internal)?;
    let mut table = ForkTable::new();
    table.insert_record(first, inner);
    // The wrapping child-only fork routes the same subtree; its reference count
    // is recomputed from the resolved table, not the terminal payload's.
    let resolved = resolve_in(sink, table, stats).await?;
    let count = resolved.child_count();
    let child = resolved.into_child();
    make_fork(head, ForkPayload::Child(child), None, count)
}

/// A fork record for `edge` (its index byte plus tail) carrying `payload`,
/// stamping the referenced-child subtree count so it survives the rewrite.
fn make_fork<F: Format, R: NodeRef>(
    edge: &[u8],
    payload: ForkPayload<F, R>,
    meta: Option<Metadata<F>>,
    child_count: Option<SubtreeCount>,
) -> Result<Option<ForkRecord<F, R>>, ApplyError> {
    let tail = Prefix::try_from(edge.get(1..).ok_or(ApplyError::Internal)?)?;
    let mut record = ForkRecord::from_tail_parts(tail, payload, meta);
    // The count rides only a referenced child; an embedded or leaf fork walks
    // it in place, so a stray count never reaches the record.
    if record.child().is_some_and(Child::is_reference) {
        record.set_child_count(child_count);
    }
    Ok(Some(record))
}

/// The insertions of a change group as builder items, dropping deletions.
///
/// The items borrow their key and value from `changes`, so the change group
/// outlives the table build it feeds.
fn inserts_to_items<'a, F: Format>(changes: &'a [Change<'_, F>]) -> Vec<Item<'a, F>> {
    changes
        .iter()
        .filter_map(|change| match change.op {
            Op::Insert { entry, meta } => Some(Item {
                key: &change.key,
                entry,
                meta,
            }),
            Op::Delete => None,
        })
        .collect()
}

/// The length of the shared byte prefix of `a` and `b`.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let mut len = 0usize;
    for (x, y) in a.iter().zip(b.iter()) {
        if x != y {
            break;
        }
        len = len.saturating_add(1);
    }
    len
}

/// The child-prefetch window: the format's read-ahead saturated into a nonzero
/// window, matching the scan and segment-join read windows.
fn child_window<F: Format>() -> Window {
    let slots = u16::try_from(F::READ_AHEAD).unwrap_or(u16::MAX);
    Window::new(slots).unwrap_or(Window::DEFAULT)
}

/// One landed child prefetch: its request slot and the fetched node.
type Prefetched<F, R> = (usize, Result<Node<F, R>, StoreError>);

/// Prefetch each requested child node concurrently, returning the landed nodes
/// indexed by slot; `slots` sizes the result and every slot without a request
/// stays `None`.
///
/// Nothing streams: the window admits freely because reads are order-free,
/// and every completion lands in its own slot, so the result is independent
/// of completion order.
async fn prefetch_children<S, F, R>(
    store: &S,
    requests: impl Iterator<Item = (usize, R)>,
    slots: usize,
) -> Vec<Option<Result<Node<F, R>, StoreError>>>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    let mut landed: Vec<Option<Result<Node<F, R>, StoreError>>> =
        (0..slots).map(|_| None).collect();
    let mut queue: Vec<(usize, R)> = requests.collect();
    if queue.is_empty() {
        return landed;
    }
    let admission = Admission::new(child_window::<F>());
    let mut in_flight: FuturesUnordered<BoxFuture<'_, Prefetched<F, R>>> = FuturesUnordered::new();
    poll_fn(|cx| {
        loop {
            while admission.admits(in_flight.len(), true) {
                let Some((slot, reference)) = queue.pop() else {
                    break;
                };
                in_flight.push(Box::pin(async move {
                    (slot, store.get_node::<F, R>(&reference).await)
                }));
            }
            match Pin::new(&mut in_flight).poll_next(cx) {
                Poll::Ready(Some((slot, outcome))) => {
                    if let Some(cell) = landed.get_mut(slot) {
                        *cell = Some(outcome);
                    }
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
    })
    .await;
    landed
}

#[cfg(test)]
mod tests {
    use crate::store::Plaintext;
    use core::future::Future;
    use core::pin::Pin;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use core::task::{Context, Poll};
    use std::vec;

    use nectar_primitives::store::{ChunkGet, ChunkPut, ContentGet, MemoryStore};
    use nectar_primitives::{Chunk, ChunkAddress, ChunkRef, ContentOnlyChunkSet, Verified};
    use nectar_testing::run;

    use crate::builder::Builder;
    use crate::format::V1;
    use crate::meta::{KeyId, Metadata};

    use super::*;

    fn entry(byte: u8) -> Entry {
        ChunkRef::new(ChunkAddress::new([byte; 32])).into()
    }

    // Walk the counted tree, asserting every stored referenced-child count
    // equals the walked subtree size, and return the subtree's key count.
    fn walk_counts<'a>(
        store: &'a ContentGet<MemoryStore>,
        table: &'a ForkTable<V1>,
    ) -> Pin<Box<dyn Future<Output = u64> + 'a>> {
        Box::pin(async move {
            let mut total = 0u64;
            for (_, record) in table.iter() {
                let child = match record.child() {
                    None => 0,
                    Some(Child::Embedded(inner)) => walk_counts(store, inner).await,
                    Some(Child::Ref(reference)) => {
                        let node = store.get_node::<V1, ChunkRef>(reference).await.unwrap();
                        let actual = walk_counts(store, node.forks()).await;
                        assert_eq!(
                            record.child_count(),
                            Some(SubtreeCount::new(actual)),
                            "stored count must equal the walked subtree size"
                        );
                        actual
                    }
                };
                total += u64::from(record.entry().is_some()) + child;
            }
            total
        })
    }

    #[test]
    fn counted_child_counts_match_a_full_walk_oracle() {
        let store = ContentGet::new(MemoryStore::default());
        let mut builder = Builder::<V1>::new();
        let mut expected = 0u64;
        // Many wide sub-trees, each referenced (over the embedding budget), under
        // enough root forks to spill the root into a segment directory: exercises
        // referenced-child counts and the segment path at once.
        for p in 0u8..128 {
            for x in 0u8..44 {
                builder.insert(Key::from(&[p, x][..]), entry(x), None);
                expected += 1;
            }
        }
        let root = *run(builder.build(&store, &Plaintext)).unwrap().root();
        let node = run(store.get_node::<V1, ChunkRef>(&root)).unwrap();
        let total = u64::from(node.entry().is_some()) + run(walk_counts(&store, node.forks()));
        assert_eq!(total, expected);
    }

    #[test]
    fn counted_apply_matches_a_rebuild_and_preserves_counts() {
        let store = ContentGet::new(MemoryStore::default());
        // A base that references a wide sub-tree under "a", then a changeset that
        // deepens it: apply must reproduce the from-scratch counted root.
        let mut base = Builder::<V1>::new();
        for x in 0u8..40 {
            base.insert(Key::from(&[b'a', x][..]), entry(x), None);
        }
        let base_root = *run(base.build(&store, &Plaintext)).unwrap().root();

        let mut cs = Changeset::<V1>::new();
        for x in 40u8..64 {
            cs.insert(Key::from(&[b'a', x][..]), entry(x), None);
        }
        let applied = run(apply(&store, &Plaintext, &base_root, &cs)).unwrap();

        let mut scratch = Builder::<V1>::new();
        for x in 0u8..64 {
            scratch.insert(Key::from(&[b'a', x][..]), entry(x), None);
        }
        let scratch_root =
            *run(scratch.build(&ContentGet::new(MemoryStore::default()), &Plaintext))
                .unwrap()
                .root();
        assert_eq!(applied, scratch_root, "apply must match a counted rebuild");

        // The applied tree's stored counts still equal the walked subtree sizes.
        let node = run(store.get_node::<V1, ChunkRef>(&applied)).unwrap();
        let total = u64::from(node.entry().is_some()) + run(walk_counts(&store, node.forks()));
        assert_eq!(total, 64);
    }

    // Build a manifest from `keys` and return its root.
    fn build(store: &ContentGet<MemoryStore>, keys: &[(&[u8], u8)]) -> ChunkRef {
        let mut builder = Builder::<V1>::new();
        for (key, fill) in keys {
            builder.insert(Key::from(*key), entry(*fill), None);
        }
        *run(builder.build(store, &Plaintext)).unwrap().root()
    }

    // The root a from-scratch build of `keys` produces, for the byte-identity
    // check: a fresh store makes the address depend on the bytes alone.
    fn rebuilt(keys: &[(&[u8], u8)]) -> ChunkRef {
        build(&ContentGet::new(MemoryStore::default()), keys)
    }

    #[test]
    fn an_empty_changeset_returns_the_root_unchanged() {
        let store = ContentGet::new(MemoryStore::default());
        let root = build(&store, &[(b"a", 1), (b"b", 2)]);
        let out = run(apply(&store, &Plaintext, &root, &Changeset::<V1>::new())).unwrap();
        assert_eq!(out, root);
    }

    #[test]
    fn a_single_insert_equals_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        let root = build(&store, &[(b"a", 1), (b"c", 3)]);
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&b"b"[..]), entry(2), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"a", 1), (b"b", 2), (b"c", 3)]));
    }

    #[test]
    fn a_batch_touching_one_ancestor_equals_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        let root = build(&store, &[(b"road", 1), (b"roam", 2)]);
        // Two inserts under the shared "ro" ancestor, rewritten in one pass.
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&b"rock"[..]), entry(3), None);
        cs.insert(Key::from(&b"rose"[..]), entry(4), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(
            out,
            rebuilt(&[(b"road", 1), (b"roam", 2), (b"rock", 3), (b"rose", 4)])
        );
    }

    #[test]
    fn an_update_overwrites_in_place() {
        let store = ContentGet::new(MemoryStore::default());
        let root = build(&store, &[(b"a", 1), (b"b", 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&b"a"[..]), entry(9), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"a", 9), (b"b", 2)]));
    }

    #[test]
    fn a_deletion_that_re_inlines_a_sibling_equals_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // "roam"/"road" share a "roa" branch; deleting one collapses the branch
        // back into a single compacted edge.
        let root = build(&store, &[(b"roam", 1), (b"road", 2), (b"x", 3)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&b"road"[..]));
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"roam", 1), (b"x", 3)]));
    }

    #[test]
    fn deleting_the_last_child_removes_the_fork() {
        let store = ContentGet::new(MemoryStore::default());
        let root = build(&store, &[(b"a", 1), (b"b", 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&b"a"[..]));
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"b", 2)]));
    }

    #[test]
    fn deleting_an_absent_key_is_a_no_op() {
        let store = ContentGet::new(MemoryStore::default());
        let root = build(&store, &[(b"a", 1), (b"ab", 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&b"absent"[..]));
        cs.remove(Key::from(&b"a"[..]));
        cs.insert(Key::from(&b"a"[..]), entry(1), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"a", 1), (b"ab", 2)]));
    }

    #[test]
    fn a_split_within_an_edge_equals_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // "abcdef" sits behind a long compacted edge; inserting "abz" branches
        // inside that edge.
        let root = build(&store, &[(b"abcdef", 1)]);
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&b"abz"[..]), entry(2), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"abcdef", 1), (b"abz", 2)]));
    }

    #[test]
    fn a_split_above_a_chain_boundary_recompacts_like_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // A 256-byte key sits behind a PLEN_MAX(255) chain: one 255-byte edge
        // over a child holding its last byte. Inserting a key that shares only
        // the first byte branches above that chain, shortening the existing edge
        // so its final byte re-merges into the edge, no longer a child hop.
        let mut base = vec![2u8];
        base.extend(std::iter::repeat_n(0u8, 255));
        let root = build(&store, &[(&base[..], 1)]);
        let mut cs = Changeset::<V1>::new();
        let branched = [2u8, 2, 1];
        cs.insert(Key::from(&branched[..]), entry(2), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&base[..], 1), (&branched[..], 2)]));
    }

    #[test]
    fn a_split_above_a_multi_fork_chain_recompacts_like_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // A 511-byte key sits behind a PLEN_MAX(255) chain of three forks
        // (255 + 255 + 1). Inserting its one-byte prefix branches at the head,
        // leaving a 510-byte continuation run whose canonical shape is a
        // 255 + 255 chain, not the 255 + 254 + 1 a single-level re-compaction
        // stops at.
        let long = vec![0xffu8; 511];
        let prefix = [0xffu8];
        let root = build(&store, &[(&long[..], 1)]);
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&prefix[..]), entry(2), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&long[..], 1), (&prefix[..], 2)]));
    }

    #[test]
    fn a_split_above_a_deep_chain_recompacts_like_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // A 766-byte key is a four-fork chain (255 + 255 + 255 + 1); branching
        // at its head must re-segment the whole 765-byte run, not just the top
        // link.
        let long = vec![0xffu8; 766];
        let prefix = [0xffu8];
        let root = build(&store, &[(&long[..], 1)]);
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&prefix[..]), entry(2), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&long[..], 1), (&prefix[..], 2)]));
    }

    #[test]
    fn a_remove_beside_a_recapped_chain_insert_equals_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // Removing the only key while inserting a 257-byte sibling splits the
        // shared first byte, and the re-capped PLEN_MAX(255) chain boundary
        // lands one byte earlier than the inserted subtree's own cap: the tail
        // beyond the new boundary must merge into one edge rather than keep
        // the stale one-byte chain hop.
        let root = build(&store, &[(&[0u8, 0][..], 1)]);
        let mut long = vec![0u8, 1];
        long.extend(std::iter::repeat_n(0u8, 255));
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&[0u8, 0][..]));
        cs.insert(Key::from(&long[..]), entry(2), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&long[..], 2)]));
    }

    #[test]
    fn the_empty_key_sets_and_clears_the_root_value() {
        let store = ContentGet::new(MemoryStore::default());
        let root = build(&store, &[(b"a", 1)]);
        let mut set = Changeset::<V1>::new();
        set.insert(Key::empty(), entry(7), None);
        let with_root = run(apply(&store, &Plaintext, &root, &set)).unwrap();

        let mut expect = Builder::<V1>::new();
        expect.insert(Key::empty(), entry(7), None);
        expect.insert(Key::from(&b"a"[..]), entry(1), None);
        let rebuilt_root = *run(expect.build(&ContentGet::new(MemoryStore::default()), &Plaintext))
            .unwrap()
            .root();
        assert_eq!(with_root, rebuilt_root);

        let mut clear = Changeset::<V1>::new();
        clear.remove(Key::empty());
        let cleared = run(apply(&store, &Plaintext, &with_root, &clear)).unwrap();
        assert_eq!(cleared, rebuilt(&[(b"a", 1)]));
    }

    #[test]
    fn a_collapse_past_the_prefix_bound_chains_like_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // Two keys sharing a 200-byte prefix, total length 260: the fork over
        // the shared run terminates one key and continues to the other. Deleting
        // the terminal leaves a single 260-byte key whose from-scratch shape is
        // a PLEN_MAX(255)-capped chain, not one over-long edge.
        let short = vec![b'a'; 200];
        let mut long = short.clone();
        long.extend(std::iter::repeat_n(b'b', 60));
        let root = build(&store, &[(&short[..], 1), (&long[..], 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&short[..]));
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&long[..], 2)]));
    }

    #[test]
    fn a_split_above_a_spilled_chain_link_equals_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // A 1708-byte key spills its top chain link (subtree body over
        // INLINE_MAX), so the shifted run continues behind a reference.
        let long = vec![0x07u8; 1708];
        let root = build(&store, &[(&long[..], 1)]);
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&[0x07u8][..]), entry(2), None);
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&long[..], 1), (&[0x07u8][..], 2)]));
    }

    #[test]
    fn a_delete_merging_into_a_spilled_chain_equals_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        // Stripping the short key's terminal merges its edge into a continuation
        // that has spilled to a reference.
        let short = vec![0x07u8; 100];
        let mut long = short.clone();
        long.extend(core::iter::repeat_n(0x08u8, 1453));
        let root = build(&store, &[(&short[..], 1), (&long[..], 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&short[..]));
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&long[..], 2)]));
    }

    #[test]
    fn an_edge_at_its_forced_cut_absorbs_nothing() {
        let store = ContentGet::new(MemoryStore::default());
        // The root edge fills PLEN_MAX exactly, so its boundary is the forced
        // cut a build places and its spilled continuation must stay put: a
        // stripped terminal here merges nothing.
        // A 305-byte shared run over a 40-way branch: the node past the cut is
        // too heavy to embed, so the root's continuation is a reference.
        let shared = vec![0x09u8; 305];
        let keys: Vec<(Vec<u8>, u8)> = (0u8..40)
            .map(|x| {
                let mut key = shared.clone();
                key.push(x);
                (key, x)
            })
            .collect();
        let borrowed: Vec<(&[u8], u8)> = keys.iter().map(|(k, x)| (&k[..], *x)).collect();
        let root = build(&store, &borrowed);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&shared[..V1::PLEN_MAX]));
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&borrowed));
        assert_eq!(out, root);
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

    /// A store recording the peak number of concurrent reads; puts pass
    /// straight through.
    struct GatedStore {
        inner: ContentGet<MemoryStore>,
        inflight: AtomicUsize,
        peak: AtomicUsize,
    }

    impl ChunkGet<ContentOnlyChunkSet> for GatedStore {
        type Trust = Verified;
        type Error = <ContentGet<MemoryStore> as ChunkGet<ContentOnlyChunkSet>>::Error;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
            let now = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
            self.peak.fetch_max(now, Ordering::Relaxed);
            YieldOnce(false).await;
            let chunk = ChunkGet::get(&self.inner, address).await;
            self.inflight.fetch_sub(1, Ordering::Relaxed);
            chunk
        }
    }

    impl ChunkPut for GatedStore {
        type Error = <ContentGet<MemoryStore> as ChunkPut>::Error;

        async fn put(&self, chunk: Chunk<Verified>) -> Result<(), Self::Error> {
            self.inner.put(chunk).await
        }
    }

    #[test]
    fn disjoint_subtree_reads_overlap_under_the_window() {
        let inner = ContentGet::new(MemoryStore::default());
        // Four top-level subtrees, each wide enough to spill to a reference; the
        // root keeps four forks, so it is one chunk, not a segment directory.
        let mut builder = Builder::<V1>::new();
        for p in 0u8..4 {
            for x in 0u8..44 {
                builder.insert(Key::from(&[p, x][..]), entry(x), None);
            }
        }
        let root = *run(builder.build(&inner, &Plaintext)).unwrap().root();
        let store = GatedStore {
            inner,
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        };

        // A deeper insert under each subtree forces a descent into all four
        // referenced children at the root at once.
        let mut cs = Changeset::<V1>::new();
        for p in 0u8..4 {
            cs.insert(Key::from(&[p, 0, 9][..]), entry(99), None);
        }
        let applied = run(apply(&store, &Plaintext, &root, &cs)).unwrap();

        let mut scratch = Builder::<V1>::new();
        for p in 0u8..4 {
            for x in 0u8..44 {
                scratch.insert(Key::from(&[p, x][..]), entry(x), None);
            }
            scratch.insert(Key::from(&[p, 0, 9][..]), entry(99), None);
        }
        let expected = *run(scratch.build(&ContentGet::new(MemoryStore::default()), &Plaintext))
            .unwrap()
            .root();
        assert_eq!(applied, expected, "apply must match a from-scratch build");

        let peak = store.peak.load(Ordering::Relaxed);
        assert!(peak > 1, "disjoint subtree reads overlapped, peak {peak}");
        // Four requests never reach the window; the fan-out test below is what
        // pins the cap.
        assert!(peak <= usize::from(child_window::<V1>().get()));
    }

    #[test]
    fn the_child_window_bounds_the_prefetch_fan_out() {
        let inner = ContentGet::new(MemoryStore::default());
        // More referenced children under the root than the window has slots,
        // each wide enough to spill to a reference, so an unbounded prefetch
        // would launch the whole group at once.
        let mut builder = Builder::<V1>::new();
        for p in 0u8..24 {
            for x in 0u8..44 {
                builder.insert(Key::from(&[p, x][..]), entry(x), None);
            }
        }
        let root = *run(builder.build(&inner, &Plaintext)).unwrap().root();
        let node: Node<V1> = run(inner.get_node(&root)).unwrap();
        let children = node
            .forks()
            .iter()
            .filter(|(_, record)| matches!(record.child(), Some(Child::Ref(_))))
            .count();
        let window = usize::from(child_window::<V1>().get());
        assert!(
            children > window,
            "the frontier must outgrow the window, {children} children"
        );

        let store = GatedStore {
            inner,
            inflight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        };
        // A deeper insert under every subtree descends into all of them at
        // once, so the whole group rides one prefetch.
        let mut cs = Changeset::<V1>::new();
        for p in 0u8..24 {
            cs.insert(Key::from(&[p, 0, 9][..]), entry(99), None);
        }
        run(apply(&store, &Plaintext, &root, &cs)).unwrap();

        let peak = store.peak.load(Ordering::Relaxed);
        // The cap is what bounds the fan-out: a lost window would show up here.
        assert_eq!(
            peak, window,
            "peak in-flight {peak} is not the child window {window}"
        );
    }

    /// Yields once per round before completing, so a test picks the order
    /// concurrent prefetches land in.
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

    /// A store that delays each address by its own round count and records the
    /// order reads resolve in; puts pass straight through.
    struct SkewedStore {
        inner: ContentGet<MemoryStore>,
        rounds: BTreeMap<ChunkAddress, usize>,
        arrivals: std::sync::Mutex<Vec<ChunkAddress>>,
    }

    impl ChunkGet<ContentOnlyChunkSet> for SkewedStore {
        type Trust = Verified;
        type Error = <ContentGet<MemoryStore> as ChunkGet<ContentOnlyChunkSet>>::Error;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
            Yields(self.rounds.get(address).copied().unwrap_or(0)).await;
            self.arrivals.lock().unwrap().push(*address);
            ChunkGet::get(&self.inner, address).await
        }
    }

    impl ChunkPut for SkewedStore {
        type Error = <ContentGet<MemoryStore> as ChunkPut>::Error;

        async fn put(&self, chunk: Chunk<Verified>) -> Result<(), Self::Error> {
            self.inner.put(chunk).await
        }
    }

    #[test]
    fn out_of_order_child_prefetches_land_in_their_request_slots() {
        let inner = ContentGet::new(MemoryStore::default());
        // Four top-level subtrees, each wide enough to spill to a reference and
        // each holding different values, so a misrouted child is visible in the
        // rewritten root.
        let mut builder = Builder::<V1>::new();
        for p in 0u8..4 {
            for x in 0u8..44 {
                builder.insert(Key::from(&[p, x][..]), entry(x.wrapping_add(p)), None);
            }
        }
        let root = *run(builder.build(&inner, &Plaintext)).unwrap().root();
        let node: Node<V1> = run(inner.get_node(&root)).unwrap();
        let children: Vec<ChunkAddress> = node
            .forks()
            .iter()
            .filter_map(|(_, record)| match record.child() {
                Some(Child::Ref(reference)) => Some(*reference.address()),
                _ => None,
            })
            .collect();
        assert_eq!(children.len(), 4, "each subtree spilled to a reference");
        assert_eq!(
            children
                .iter()
                .map(|address| (*address, ()))
                .collect::<BTreeMap<_, _>>()
                .len(),
            4,
            "the four children are distinct nodes"
        );

        // Round counts scrambling the completions into slot order 2, 0, 3, 1:
        // neither the request order nor its reverse, so only routing by slot
        // pairs each child with its own group.
        let rounds = children
            .iter()
            .copied()
            .zip([4usize, 8, 2, 6])
            .collect::<BTreeMap<_, _>>();
        let store = SkewedStore {
            inner,
            rounds,
            arrivals: std::sync::Mutex::new(Vec::new()),
        };

        // A deeper insert under each subtree descends into all four referenced
        // children at once, so all four ride one prefetch.
        let mut cs = Changeset::<V1>::new();
        for p in 0u8..4 {
            cs.insert(Key::from(&[p, 0, 9][..]), entry(99), None);
        }
        let applied = run(apply(&store, &Plaintext, &root, &cs)).unwrap();

        // The skew held: the children landed scrambled, so a fold that filled
        // slots in completion order would misroute three of the four.
        let landed: Vec<ChunkAddress> = store
            .arrivals
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|address| children.contains(address))
            .collect();
        assert_eq!(
            landed,
            vec![children[2], children[0], children[3], children[1]]
        );

        let mut scratch = Builder::<V1>::new();
        for p in 0u8..4 {
            for x in 0u8..44 {
                scratch.insert(Key::from(&[p, x][..]), entry(x.wrapping_add(p)), None);
            }
            scratch.insert(Key::from(&[p, 0, 9][..]), entry(99), None);
        }
        let expected = *run(scratch.build(&ContentGet::new(MemoryStore::default()), &Plaintext))
            .unwrap()
            .root();
        // Each prefetched child reconciled with its own group, whatever the
        // completion order.
        assert_eq!(applied, expected, "apply must match a from-scratch build");
    }

    #[test]
    fn carried_metadata_survives_a_rebuild() {
        let store = ContentGet::new(MemoryStore::default());
        let meta = Metadata::new(KeyId::ContentType, Bytes::from_static(b"text/html")).unwrap();
        let root = build(&store, &[(b"a", 1)]);
        let mut cs = Changeset::<V1>::new();
        cs.insert(Key::from(&b"index.html"[..]), entry(2), Some(meta.clone()));
        let out = run(apply(&store, &Plaintext, &root, &cs)).unwrap();

        let mut expect = Builder::<V1>::new();
        expect.insert(Key::from(&b"a"[..]), entry(1), None);
        expect.insert(Key::from(&b"index.html"[..]), entry(2), Some(meta));
        let rebuilt_root = *run(expect.build(&ContentGet::new(MemoryStore::default()), &Plaintext))
            .unwrap()
            .root();
        assert_eq!(out, rebuilt_root);
    }
}
