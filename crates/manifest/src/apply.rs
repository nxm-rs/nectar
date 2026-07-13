//! History-independent batch update: fold a changeset into a manifest to a new
//! root that is byte-identical to building the merged key set from scratch.
//!
//! The update is a bottom-up path-copy union: only the nodes on the touched
//! paths are rewritten, and a shared ancestor is rewritten once per apply, not
//! once per changeset entry, so a wide batch amortizes over its overlap. An
//! unchanged fork is spliced in verbatim; an untouched referenced subtree is
//! reused by address without a fetch. Embedding is child-local and a cut is
//! keyed on the fork-relative prefix, so a reused subtree keeps its shape;
//! re-rooting only shifts where the `PLEN_MAX` edge cap falls, so a re-rooted or
//! merged edge is re-compacted into the same chain a build at the new depth
//! would. Hence `apply(root, delta)` and a from-scratch build of the merged keys
//! agree bit for bit (invariant I6 under updates).
//!
//! Peak retained state is O(depth + changeset frontier): the descent holds one
//! node per level on the current path, never a whole subtree.

use std::collections::BTreeMap;

use bytes::Bytes;
use nectar_primitives::ChunkAddress;
use nectar_primitives::store::{ChunkPut, MaybeSync};

use crate::bounded::Prefix;
use crate::builder::{BuildError, BuildStats, Item, build_table, emit_node, resolve};
use crate::count::SubtreeCount;
use crate::error::{ForkPrefixEmpty, PrefixTooLong};
use crate::fork::{Child, ForkPayload, ForkRecord, ForkTable};
use crate::format::{Format, V1};
use crate::meta::Metadata;
use crate::node::{Node, RootExtension};
use crate::store::{NodeGet, StoreError};
use crate::value::{Entry, Key};

/// One key's update within a changeset.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Op<F: Format> {
    /// Bind the key to a value, with optional metadata.
    Put {
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
    pub fn put(&mut self, key: Key, entry: Entry<F>, metadata: Option<Metadata<F>>) -> &mut Self {
        self.ops.insert(
            key.into_bytes(),
            Op::Put {
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
    /// An update descended into an encrypted subtree the plain path cannot open.
    #[error("descent reached an encrypted subtree")]
    EncryptedChild,
    /// A merge invariant did not hold; an apply bug rather than bad input.
    #[error("apply invariant violated")]
    Internal,
}

/// Fold `changeset` into the manifest rooted at `root`, returning the new root.
///
/// The result equals a from-scratch build of the merged key set, byte for byte:
/// an empty changeset returns `root` unchanged, and a single update is just a
/// one-entry changeset.
pub async fn apply<S, F>(
    store: &S,
    root: &ChunkAddress,
    changeset: &Changeset<F>,
) -> Result<ChunkAddress, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
{
    if changeset.is_empty() {
        return Ok(*root);
    }
    let node = store.get_node::<F>(root).await?;

    // The empty key is the root's own value; every other key descends the trie.
    let mut root_entry = node.entry().cloned();
    let mut root_meta = node.metadata().cloned();
    match changeset.ops.get(&Bytes::new()) {
        Some(Op::Put { entry, meta }) => {
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

    let mut stats = BuildStats::default();
    let forks = Box::pin(apply_forks(
        store,
        node.forks().clone(),
        0,
        &changes,
        &mut stats,
    ))
    .await?;
    let new_node = Node::new(root_ext, forks);
    Ok(emit_node(store, &new_node, &mut stats).await?)
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
async fn apply_forks<'c, S, F>(
    store: &S,
    mut table: ForkTable<F>,
    consumed: usize,
    changes: &[Change<'c, F>],
    stats: &mut BuildStats,
) -> Result<ForkTable<F>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
{
    let mut i = 0usize;
    while let Some(first) = changes.get(i) {
        let Some(&byte) = first.key.get(consumed) else {
            // A key with no byte here belongs to the parent boundary already.
            i = i.saturating_add(1);
            continue;
        };
        let mut j = i.saturating_add(1);
        while changes.get(j).and_then(|c| c.key.get(consumed)) == Some(&byte) {
            j = j.saturating_add(1);
        }
        let group = changes.get(i..j).ok_or(ApplyError::Internal)?;
        let existing = table.remove(byte);
        if let Some(record) =
            Box::pin(reconcile(store, consumed, byte, existing, group, stats)).await?
        {
            table.insert_record(byte, record);
        }
        i = j;
    }
    Ok(table)
}

/// Reconcile the fork indexed under `byte` with its change group, returning the
/// rewritten fork or `None` when it collapses away.
async fn reconcile<'c, S, F>(
    store: &S,
    consumed: usize,
    byte: u8,
    existing: Option<ForkRecord<F>>,
    group: &[Change<'c, F>],
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
{
    let existing = match existing {
        Some(record) => record,
        None => {
            // No fork here yet: build one from the group's insertions alone.
            let items = inserts_to_items(group);
            if items.is_empty() {
                return Ok(None);
            }
            let mut fresh = build_table(store, &items, consumed, stats).await?;
            return Ok(fresh.remove(byte));
        }
    };

    // The fork's full edge: the index byte followed by its stored tail.
    let mut edge = Vec::with_capacity(existing.tail().len().saturating_add(1));
    edge.push(byte);
    edge.extend_from_slice(existing.tail().as_bytes());
    let plen = consumed.saturating_add(edge.len());

    // The merged key set's compacted edge shortens to the least point any
    // insertion diverges from the existing edge; deletions off the edge target
    // no existing key and never move it.
    let mut cut = edge.len();
    for change in group {
        if let Op::Put { .. } = change.op {
            let suffix = change.key.get(consumed..).unwrap_or_default();
            cut = cut.min(common_prefix(suffix, &edge));
        }
    }

    if cut < edge.len() {
        split(store, consumed, &edge, cut, existing, group, stats).await
    } else {
        Box::pin(descend(
            store, consumed, &edge, plen, existing, group, stats,
        ))
        .await
    }
}

/// The existing edge stays intact: update the terminal value and fold the
/// deeper updates into the child.
async fn descend<'c, S, F>(
    store: &S,
    consumed: usize,
    edge: &[u8],
    plen: usize,
    existing: ForkRecord<F>,
    group: &[Change<'c, F>],
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
{
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
                Op::Put { entry, meta } => {
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
        // The child is untouched: reuse it verbatim, carrying its stored count.
        return finish(
            store,
            edge,
            new_entry,
            new_meta,
            existing.child().cloned(),
            existing.child_count(),
            stats,
        )
        .await;
    }

    let child_table = match existing.child() {
        None => {
            let items = inserts_to_items(&deeper);
            if items.is_empty() {
                // A deletion of an absent deeper key: the fork is unchanged bar
                // its terminal value.
                return finish(store, edge, new_entry, new_meta, None, None, stats).await;
            }
            build_table(store, &items, plen, stats).await?
        }
        Some(Child::Embedded(inner)) => {
            Box::pin(apply_forks(store, inner.clone(), plen, &deeper, stats)).await?
        }
        Some(Child::Ref32(reference)) => {
            let node = store.get_node::<F>(reference.address()).await?;
            Box::pin(apply_forks(
                store,
                node.forks().clone(),
                plen,
                &deeper,
                stats,
            ))
            .await?
        }
        Some(Child::Ref64(_)) => return Err(ApplyError::EncryptedChild),
    };
    assemble(store, edge, new_entry, new_meta, child_table, stats).await
}

/// Fold a rewritten child table back into a fork record over `edge`, collapsing
/// an empty or single-fork child so the result matches a from-scratch build.
///
/// The single-fork merge runs before the child is resolved, so a lone branch
/// re-inlines whatever its size would spill to.
async fn assemble<S, F>(
    store: &S,
    edge: &[u8],
    entry: Option<Entry<F>>,
    meta: Option<Metadata<F>>,
    table: ForkTable<F>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    if table.is_empty() {
        return finish(store, edge, entry, meta, None, None, stats).await;
    }
    // Edge-compaction: a child-only fork over a single-fork child merges into
    // one edge, exactly as a from-scratch build would compact the shared run.
    if entry.is_none()
        && table.len() == 1
        && let Some((first, record)) = table.iter().next()
    {
        return compact(store, edge, first, record, stats).await;
    }
    let resolved = resolve(store, table, stats).await?;
    let count = resolved.child_count();
    let child = resolved.into_child();
    finish(store, edge, entry, meta, Some(child), count, stats).await
}

/// An insertion diverges within the edge: branch at the divergence, re-rooting
/// the existing subtree verbatim under the edge remainder.
async fn split<'c, S, F>(
    store: &S,
    consumed: usize,
    edge: &[u8],
    cut: usize,
    existing: ForkRecord<F>,
    group: &[Change<'c, F>],
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: NodeGet + ChunkPut + MaybeSync,
    F: Format,
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
            if let Op::Put { entry, meta } = change.op {
                split_entry = Some(entry.clone());
                split_meta = meta.clone();
            }
            // A deletion at the new boundary targets no existing key: drop it.
        } else {
            remaining.push(change.reborrow());
        }
    }

    // The existing subtree hangs under the remainder of its edge. Shortening
    // the edge can bring a chained child-only fork back within the prefix
    // bound, so the re-root re-compacts exactly as a build at the new depth
    // would rather than splicing the old shape in verbatim.
    let mut branch = ForkTable::new();
    let remainder = edge.get(cut..).ok_or(ApplyError::Internal)?;
    let first = *remainder.first().ok_or(ApplyError::Internal)?;
    if let Some(record) = reroot(store, remainder, existing, stats).await? {
        branch.insert_record(first, record);
    }
    let table = Box::pin(apply_forks(store, branch, boundary, &remaining, stats)).await?;
    assemble(store, new_edge, split_entry, split_meta, table, stats).await
}

/// Re-root an existing fork under a shortened `remainder` edge, re-compacting a
/// child-only chain fork that the shorter edge now brings within the prefix
/// bound. A fork with a terminal value, or one whose single continuation still
/// overruns the bound, is depth-independent and re-roots verbatim.
async fn reroot<S, F>(
    store: &S,
    remainder: &[u8],
    existing: ForkRecord<F>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    if existing.entry().is_none()
        && let Some(Child::Embedded(table)) = existing.child()
        && table.len() == 1
        && let Some((first, record)) = table.iter().next()
    {
        return compact(store, remainder, first, record, stats).await;
    }
    make_fork(
        remainder,
        existing.payload().clone(),
        existing.metadata().cloned(),
        existing.child_count(),
    )
}

/// Assemble a fork record from an intact edge, its terminal value and its child,
/// or `None` when neither survives.
///
/// A child-only fork over a single-fork embedded child compacts into one edge,
/// so a deletion that strips a fork's terminal value re-inlines its lone
/// remaining branch exactly as a from-scratch build would.
async fn finish<S, F>(
    store: &S,
    edge: &[u8],
    entry: Option<Entry<F>>,
    meta: Option<Metadata<F>>,
    child: Option<Child<F>>,
    child_count: Option<SubtreeCount>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    if entry.is_none()
        && let Some(Child::Embedded(table)) = &child
        && table.len() == 1
        && let Some((first, record)) = table.iter().next()
    {
        return compact(store, edge, first, record, stats).await;
    }
    let has_entry = entry.is_some();
    ForkPayload::new(entry, child).map_or_else(
        || Ok(None),
        |payload| {
            make_fork(
                edge,
                payload,
                if has_entry { meta } else { None },
                child_count,
            )
        },
    )
}

/// Merge a child-only `edge` into its lone child fork (index byte `first` plus
/// `record`), emitting the compacted fork a from-scratch build would produce.
async fn compact<S, F>(
    store: &S,
    edge: &[u8],
    first: u8,
    record: &ForkRecord<F>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    let mut merged = edge.to_vec();
    merged.push(first);
    merged.extend_from_slice(record.tail().as_bytes());
    chain(
        store,
        &merged,
        record.payload().clone(),
        record.metadata().cloned(),
        record.child_count(),
        stats,
    )
    .await
}

/// A fork record over `prefix`, split into a `PLEN_MAX`-capped chain of
/// child-only nodes when it overruns the bound, exactly as the builder compacts
/// an over-long shared run. The innermost fork carries the payload and its
/// metadata; every wrapping fork carries only the continuation.
async fn chain<S, F>(
    store: &S,
    prefix: &[u8],
    payload: ForkPayload<F>,
    meta: Option<Metadata<F>>,
    child_count: Option<SubtreeCount>,
    stats: &mut BuildStats,
) -> Result<Option<ForkRecord<F>>, ApplyError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    if prefix.len() <= F::PLEN_MAX {
        return make_fork(prefix, payload, meta, child_count);
    }
    let head = prefix.get(..F::PLEN_MAX).ok_or(ApplyError::Internal)?;
    let rest = prefix.get(F::PLEN_MAX..).ok_or(ApplyError::Internal)?;
    let &first = rest.first().ok_or(ApplyError::Internal)?;
    let inner = Box::pin(chain(store, rest, payload, meta, child_count, stats))
        .await?
        .ok_or(ApplyError::Internal)?;
    let mut table = ForkTable::new();
    table.insert_record(first, inner);
    // The wrapping child-only fork routes the same subtree; its reference count
    // is recomputed from the resolved table, not the terminal payload's.
    let resolved = resolve(store, table, stats).await?;
    let count = resolved.child_count();
    let child = resolved.into_child();
    make_fork(head, ForkPayload::Child(child), None, count)
}

/// A fork record for `edge` (its index byte plus tail) carrying `payload`,
/// stamping the referenced-child subtree count so it survives the rewrite.
fn make_fork<F: Format>(
    edge: &[u8],
    payload: ForkPayload<F>,
    meta: Option<Metadata<F>>,
    child_count: Option<SubtreeCount>,
) -> Result<Option<ForkRecord<F>>, ApplyError> {
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
fn inserts_to_items<F: Format>(changes: &[Change<'_, F>]) -> Vec<Item<F>> {
    changes
        .iter()
        .filter_map(|change| match change.op {
            Op::Put { entry, meta } => Some(Item {
                key: change.key.clone(),
                entry: entry.clone(),
                meta: meta.clone(),
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

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef};

    use crate::builder::Builder;
    use crate::format::V1;
    use crate::meta::{KeyId, Metadata};

    use super::*;

    fn entry(byte: u8) -> Entry {
        ChunkRef::new(ChunkAddress::new([byte; 32])).into()
    }

    // Walk the counted tree, asserting every stored referenced-child count
    // equals the walked subtree size, and return the subtree's key count.
    fn walk_counts(store: &MemoryStore, table: &ForkTable<V1>) -> u64 {
        let mut total = 0u64;
        for (_, record) in table.iter() {
            let child = match record.child() {
                None => 0,
                Some(Child::Embedded(inner)) => walk_counts(store, inner),
                Some(Child::Ref32(reference)) => {
                    let node = block_on(store.get_node::<V1>(reference.address())).unwrap();
                    let actual = walk_counts(store, node.forks());
                    assert_eq!(
                        record.child_count(),
                        Some(SubtreeCount::new(actual)),
                        "stored count must equal the walked subtree size"
                    );
                    actual
                }
                Some(Child::Ref64(_)) => unreachable!("a plaintext build has no encrypted child"),
            };
            total += u64::from(record.entry().is_some()) + child;
        }
        total
    }

    #[test]
    fn counted_child_counts_match_a_full_walk_oracle() {
        let store = MemoryStore::default();
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
        let root = *block_on(builder.build(&store)).unwrap().root();
        let node = block_on(store.get_node::<V1>(&root)).unwrap();
        let total = u64::from(node.entry().is_some()) + walk_counts(&store, node.forks());
        assert_eq!(total, expected);
    }

    #[test]
    fn counted_apply_matches_a_rebuild_and_preserves_counts() {
        let store = MemoryStore::default();
        // A base that references a wide sub-tree under "a", then a changeset that
        // deepens it: apply must reproduce the from-scratch counted root.
        let mut base = Builder::<V1>::new();
        for x in 0u8..40 {
            base.insert(Key::from(&[b'a', x][..]), entry(x), None);
        }
        let base_root = *block_on(base.build(&store)).unwrap().root();

        let mut cs = Changeset::<V1>::new();
        for x in 40u8..64 {
            cs.put(Key::from(&[b'a', x][..]), entry(x), None);
        }
        let applied = block_on(apply(&store, &base_root, &cs)).unwrap();

        let mut scratch = Builder::<V1>::new();
        for x in 0u8..64 {
            scratch.insert(Key::from(&[b'a', x][..]), entry(x), None);
        }
        let scratch_root = *block_on(scratch.build(&MemoryStore::default()))
            .unwrap()
            .root();
        assert_eq!(applied, scratch_root, "apply must match a counted rebuild");

        // The applied tree's stored counts still equal the walked subtree sizes.
        let node = block_on(store.get_node::<V1>(&applied)).unwrap();
        let total = u64::from(node.entry().is_some()) + walk_counts(&store, node.forks());
        assert_eq!(total, 64);
    }

    // Build a manifest from `keys` and return its root.
    fn build(store: &MemoryStore, keys: &[(&[u8], u8)]) -> ChunkAddress {
        let mut builder = Builder::<V1>::new();
        for (key, fill) in keys {
            builder.insert(Key::from(*key), entry(*fill), None);
        }
        *block_on(builder.build(store)).unwrap().root()
    }

    // The root a from-scratch build of `keys` produces, for the byte-identity
    // check: a fresh store makes the address depend on the bytes alone.
    fn rebuilt(keys: &[(&[u8], u8)]) -> ChunkAddress {
        build(&MemoryStore::default(), keys)
    }

    #[test]
    fn an_empty_changeset_returns_the_root_unchanged() {
        let store = MemoryStore::default();
        let root = build(&store, &[(b"a", 1), (b"b", 2)]);
        let out = block_on(apply(&store, &root, &Changeset::<V1>::new())).unwrap();
        assert_eq!(out, root);
    }

    #[test]
    fn a_single_insert_equals_a_rebuild() {
        let store = MemoryStore::default();
        let root = build(&store, &[(b"a", 1), (b"c", 3)]);
        let mut cs = Changeset::<V1>::new();
        cs.put(Key::from(&b"b"[..]), entry(2), None);
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"a", 1), (b"b", 2), (b"c", 3)]));
    }

    #[test]
    fn a_batch_touching_one_ancestor_equals_a_rebuild() {
        let store = MemoryStore::default();
        let root = build(&store, &[(b"road", 1), (b"roam", 2)]);
        // Two inserts under the shared "ro" ancestor, rewritten in one pass.
        let mut cs = Changeset::<V1>::new();
        cs.put(Key::from(&b"rock"[..]), entry(3), None);
        cs.put(Key::from(&b"rose"[..]), entry(4), None);
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(
            out,
            rebuilt(&[(b"road", 1), (b"roam", 2), (b"rock", 3), (b"rose", 4)])
        );
    }

    #[test]
    fn an_update_overwrites_in_place() {
        let store = MemoryStore::default();
        let root = build(&store, &[(b"a", 1), (b"b", 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.put(Key::from(&b"a"[..]), entry(9), None);
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"a", 9), (b"b", 2)]));
    }

    #[test]
    fn a_deletion_that_re_inlines_a_sibling_equals_a_rebuild() {
        let store = MemoryStore::default();
        // "roam"/"road" share a "roa" branch; deleting one collapses the branch
        // back into a single compacted edge.
        let root = build(&store, &[(b"roam", 1), (b"road", 2), (b"x", 3)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&b"road"[..]));
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"roam", 1), (b"x", 3)]));
    }

    #[test]
    fn deleting_the_last_child_removes_the_fork() {
        let store = MemoryStore::default();
        let root = build(&store, &[(b"a", 1), (b"b", 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&b"a"[..]));
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"b", 2)]));
    }

    #[test]
    fn deleting_an_absent_key_is_a_no_op() {
        let store = MemoryStore::default();
        let root = build(&store, &[(b"a", 1), (b"ab", 2)]);
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(&b"absent"[..]));
        cs.remove(Key::from(&b"a"[..]));
        cs.put(Key::from(&b"a"[..]), entry(1), None);
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"a", 1), (b"ab", 2)]));
    }

    #[test]
    fn a_split_within_an_edge_equals_a_rebuild() {
        let store = MemoryStore::default();
        // "abcdef" sits behind a long compacted edge; inserting "abz" branches
        // inside that edge.
        let root = build(&store, &[(b"abcdef", 1)]);
        let mut cs = Changeset::<V1>::new();
        cs.put(Key::from(&b"abz"[..]), entry(2), None);
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(b"abcdef", 1), (b"abz", 2)]));
    }

    #[test]
    fn a_split_above_a_chain_boundary_recompacts_like_a_rebuild() {
        let store = MemoryStore::default();
        // A 256-byte key sits behind a PLEN_MAX(255) chain: one 255-byte edge
        // over a child holding its last byte. Inserting a key that shares only
        // the first byte branches above that chain, shortening the existing edge
        // so its final byte re-merges into the edge, no longer a child hop.
        let mut base = vec![2u8];
        base.extend(std::iter::repeat_n(0u8, 255));
        let root = build(&store, &[(&base[..], 1)]);
        let mut cs = Changeset::<V1>::new();
        let branched = [2u8, 2, 1];
        cs.put(Key::from(&branched[..]), entry(2), None);
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&base[..], 1), (&branched[..], 2)]));
    }

    #[test]
    fn the_empty_key_sets_and_clears_the_root_value() {
        let store = MemoryStore::default();
        let root = build(&store, &[(b"a", 1)]);
        let mut set = Changeset::<V1>::new();
        set.put(Key::empty(), entry(7), None);
        let with_root = block_on(apply(&store, &root, &set)).unwrap();

        let mut expect = Builder::<V1>::new();
        expect.insert(Key::empty(), entry(7), None);
        expect.insert(Key::from(&b"a"[..]), entry(1), None);
        let rebuilt_root = *block_on(expect.build(&MemoryStore::default()))
            .unwrap()
            .root();
        assert_eq!(with_root, rebuilt_root);

        let mut clear = Changeset::<V1>::new();
        clear.remove(Key::empty());
        let cleared = block_on(apply(&store, &with_root, &clear)).unwrap();
        assert_eq!(cleared, rebuilt(&[(b"a", 1)]));
    }

    #[test]
    fn a_collapse_past_the_prefix_bound_chains_like_a_rebuild() {
        let store = MemoryStore::default();
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
        let out = block_on(apply(&store, &root, &cs)).unwrap();
        assert_eq!(out, rebuilt(&[(&long[..], 2)]));
    }

    #[test]
    fn carried_metadata_survives_a_rebuild() {
        let store = MemoryStore::default();
        let meta = Metadata::new(KeyId::ContentType, Bytes::from_static(b"text/html")).unwrap();
        let root = build(&store, &[(b"a", 1)]);
        let mut cs = Changeset::<V1>::new();
        cs.put(Key::from(&b"index.html"[..]), entry(2), Some(meta.clone()));
        let out = block_on(apply(&store, &root, &cs)).unwrap();

        let mut expect = Builder::<V1>::new();
        expect.insert(Key::from(&b"a"[..]), entry(1), None);
        expect.insert(Key::from(&b"index.html"[..]), entry(2), Some(meta));
        let rebuilt_root = *block_on(expect.build(&MemoryStore::default()))
            .unwrap()
            .root();
        assert_eq!(out, rebuilt_root);
    }
}
