//! Memory-bounded streaming builder: files -> BMT -> assemble -> publish.
//!
//! The trie is assembled bottom-up over an explicit stack of open nodes. Each
//! finished node is embedded into its parent when the packing predicate allows,
//! otherwise sealed and spilled to the store the moment it is complete, so the
//! peak retained node buffer count is the stack depth, never the key count. The
//! key set enters through a sorted map, so the published tree is a pure function
//! of the keys, identical whatever order the caller streamed them in.

use std::collections::BTreeMap;
use std::io::Write;

use bytes::Bytes;
use nectar_primitives::store::{ChunkPut, MaybeSync};
use nectar_primitives::{
    Chunk, ChunkAddress, ChunkRef, DefaultSplitter, FileError, PrimitivesError,
};

use crate::bounded::Prefix;
use crate::error::{ForkPrefixEmpty, PrefixTooLong};
use crate::fork::{Child, ForkPayload, ForkTable};
use crate::format::{Format, V1};
use crate::meta::Metadata;
use crate::node::{Node, RootExtension};
use crate::packing::{Domain, embed};
use crate::store::{NodePut, StoreError};
use crate::value::{Entry, Key};

/// A build or publish failure.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// Sealing or storing a manifest node failed; over-budget nodes surface
    /// here as an encode error.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Splitting a file into BMT chunks failed.
    #[error("split file")]
    Split(#[source] FileError),
    /// Buffering a file for splitting failed.
    #[error("buffer file")]
    Buffer(#[source] std::io::Error),
    /// Sealing a file chunk failed.
    #[error("seal file chunk")]
    Seal(#[source] PrimitivesError),
    /// The backing store rejected a file chunk.
    #[error("store file chunk")]
    Backend(#[source] Box<dyn core::error::Error + Send + Sync>),
    /// A compacted edge exceeded the format's prefix bound.
    #[error(transparent)]
    Prefix(#[from] PrefixTooLong),
    /// A fork prefix consumed no byte to index under.
    #[error(transparent)]
    EmptyPrefix(#[from] ForkPrefixEmpty),
    /// A stack invariant did not hold; a builder bug rather than bad input.
    #[error("builder invariant violated")]
    Internal,
}

impl BuildError {
    /// Box a backend error behind the seam.
    fn backend<E: core::error::Error + Send + Sync + 'static>(err: E) -> Self {
        Self::Backend(Box::new(err))
    }
}

/// Peak and total work of one build, enough to witness the memory bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildStats {
    peak_open_nodes: usize,
    nodes_written: usize,
    nodes_embedded: usize,
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
}

/// The published manifest: the root chunk address and the build's work profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Built {
    root: ChunkAddress,
    stats: BuildStats,
}

impl Built {
    /// The root chunk address; the reference a reader descends from.
    #[must_use]
    pub const fn root(&self) -> &ChunkAddress {
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

    /// Assemble and publish the manifest, returning the root address.
    ///
    /// Peak retained node buffers stay at the trie's node depth: a finished
    /// subtree is embedded or spilled to `store` before the next sibling opens.
    pub async fn build<S>(&self, store: &S) -> Result<Built, BuildError>
    where
        S: ChunkPut + MaybeSync,
    {
        let items: Vec<Item<F>> = self
            .keys
            .iter()
            .map(|(key, (entry, meta))| Item {
                key: key.clone(),
                entry: entry.clone(),
                meta: meta.clone(),
            })
            .collect();
        let root_ext = RootExtension::new(self.root_entry.clone(), self.root_metadata.clone());
        let mut stats = BuildStats::default();
        let table = build_table(store, &items, 0, &mut stats).await?;
        let node = Node::new(root_ext, table);
        let root = put_counted(store, &node, &mut stats).await?;
        Ok(Built { root, stats })
    }
}

/// Assemble the top fork table for `items` at depth `consumed`, resolving every
/// finished subtree to an embedded table or a spilled reference as it closes.
///
/// The returned table is the caller's to wrap: a root wears its extension and
/// always spills, a subtree defers its own embed decision to [`resolve`].
pub(crate) async fn build_table<S, F>(
    store: &S,
    items: &[Item<F>],
    consumed: usize,
    stats: &mut BuildStats,
) -> Result<ForkTable<F>, BuildError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    let mut stack: Vec<Frame<'_, F>> = Vec::new();
    stack.push(Frame::new(consumed, items));
    let mut returned: Option<Resolved<F>> = None;

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
                returned = Some(resolve(store, frame.table, stats).await?);
            }
        }
    }
}

/// One key-value binding, cloned into a linear array for indexed descent.
pub(crate) struct Item<F: Format> {
    pub(crate) key: Bytes,
    pub(crate) entry: Entry<F>,
    pub(crate) meta: Option<Metadata<F>>,
}

/// A resolved subtree bubbling up to its parent fork.
pub(crate) enum Resolved<F: Format> {
    /// Small enough to inline into the parent's chunk.
    Embedded(ForkTable<F>),
    /// Spilled to a chunk of its own.
    Reference(ChunkRef),
}

impl<F: Format> Resolved<F> {
    /// The child a parent fork holds for this resolved subtree.
    pub(crate) fn into_child(self) -> Child<F> {
        match self {
            Self::Embedded(table) => Child::Embedded(table),
            Self::Reference(reference) => Child::Ref32(reference),
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
struct Frame<'a, F: Format> {
    consumed: usize,
    items: &'a [Item<F>],
    cursor: usize,
    table: ForkTable<F>,
    open: Option<OpenFork<F>>,
}

/// What the driver does after one frame step.
enum Action<'a, F: Format> {
    /// A terminal fork was inserted; keep processing this frame.
    Continue,
    /// A fork opened onto the given child keys at the given consumed depth.
    Descend(&'a [Item<F>], usize),
    /// The frame is complete.
    Finalize,
}

impl<'a, F: Format> Frame<'a, F> {
    const fn new(consumed: usize, items: &'a [Item<F>]) -> Self {
        Self {
            consumed,
            items,
            cursor: 0,
            table: ForkTable::new(),
            open: None,
        }
    }

    /// Close the open fork with its resolved child.
    fn attach(&mut self, resolved: Resolved<F>) -> Result<(), BuildError> {
        let open = self.open.take().ok_or(BuildError::Internal)?;
        let child = match resolved {
            Resolved::Embedded(table) => Child::Embedded(table),
            Resolved::Reference(reference) => Child::Ref32(reference),
        };
        let payload = match open.entry {
            Some(entry) => ForkPayload::Both { entry, child },
            None => ForkPayload::Child(child),
        };
        self.table.insert(open.prefix, payload, open.meta)?;
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
    child: Option<&'a [Item<F>]>,
    end: usize,
}

/// Cut the next fork out of `items`: the run sharing the byte at `consumed`,
/// its compacted edge (capped at the prefix bound), the value of any key that
/// terminates on the edge, and the child keys that continue past it.
fn next_group<'a, F: Format>(
    items: &'a [Item<F>],
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
    let lcp = common_prefix_len(&first.key, &last.key, consumed, F::PLEN_MAX);
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

/// Embed a finished subtree into its parent, or spill it to the store.
///
/// The embed decision is child-local: it reads the subtree's flat length alone,
/// so it is stable under re-rooting and history-independent.
pub(crate) async fn resolve<S, F>(
    store: &S,
    table: ForkTable<F>,
    stats: &mut BuildStats,
) -> Result<Resolved<F>, BuildError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    let flat = Node::new(None, table.clone())
        .encoded_len()
        .saturating_sub(F::PREAMBLE.len());
    if embed::<F>(flat, Domain::Plain, Domain::Plain) {
        stats.nodes_embedded = stats.nodes_embedded.saturating_add(1);
        return Ok(Resolved::Embedded(table));
    }
    let node = Node::new(None, table);
    let address = put_counted(store, &node, stats).await?;
    Ok(Resolved::Reference(ChunkRef::new(address)))
}

/// Spill one node to the store, counting it.
async fn put_counted<S, F>(
    store: &S,
    node: &Node<F>,
    stats: &mut BuildStats,
) -> Result<ChunkAddress, BuildError>
where
    S: ChunkPut + MaybeSync,
    F: Format,
{
    let address = store.put_node(node).await?;
    stats.nodes_written = stats.nodes_written.saturating_add(1);
    Ok(address)
}

/// Split `data` through BMT, spill its chunks to `store`, and return its plain
/// root reference. Reuses the primitives splitter, so the BMT is shared.
async fn split_file<S>(store: &S, data: &[u8]) -> Result<ChunkRef, BuildError>
where
    S: ChunkPut + MaybeSync,
{
    let span = u64::try_from(data.len()).map_err(|_| BuildError::Internal)?;
    let mut splitter = DefaultSplitter::new(span);
    splitter.write_all(data).map_err(BuildError::Buffer)?;
    let (root, chunks) = splitter.finish().map_err(BuildError::Split)?;
    for chunk in chunks {
        let sealed = Chunk::from_envelope(chunk.into()).map_err(BuildError::Seal)?;
        store.put(sealed).await.map_err(BuildError::backend)?;
    }
    Ok(ChunkRef::new(root))
}

/// Stream files through BMT into one published manifest.
///
/// Each `(key, file)` pair is split into content chunks, stored, and bound to
/// the file's root reference; the manifest is then assembled and published. The
/// iteration order does not affect the published root.
///
/// ```
/// use nectar_manifest::{build_files, Key};
/// use nectar_primitives::MemoryStore;
///
/// # async fn demo() -> Result<(), nectar_manifest::BuildError> {
/// let store = MemoryStore::default();
/// let files = [(Key::from(&b"index.html"[..]), bytes::Bytes::from_static(b"<h1>hi</h1>"))];
/// let built = build_files(&store, files).await?;
/// let _root = built.root();
/// # Ok(()) }
/// ```
pub async fn build_files<S, I>(store: &S, files: I) -> Result<Built, BuildError>
where
    S: ChunkPut + MaybeSync,
    I: IntoIterator<Item = (Key, Bytes)>,
{
    let mut builder = Builder::<V1>::new();
    for (key, data) in files {
        let reference = split_file(store, &data).await?;
        builder.insert(key, Entry::from(reference), None);
    }
    builder.build(store).await
}
