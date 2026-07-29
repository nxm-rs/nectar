//! Ordered listing cursor and address stream over persisted mantaray tries.
//!
//! Both walk a trie depth-first in ascending fork order, loading nodes
//! through a bounded read-ahead window: unconsumed fetches never exceed the
//! window, so the fetched set is the serial walk's consumed set plus at most
//! one window of lookahead, and errors surface at the failing node's serial
//! position, never earlier. The walk owns its in-flight set directly: the
//! path-keyed frontier parks every completion, fault included, until its
//! serial turn at the head.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, VecDeque};
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_util::stream::{FuturesUnordered, Stream};
pub use nectar_governor::Window;
use nectar_governor::{Admission, BoxFuture};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::ChunkAddress;

use crate::entry::Entry;
use crate::error::CursorError;
use crate::node::NodeType;
use crate::persist::NodeLoader;
use crate::view::{ForkView, NodeView};

/// One queued subtree root: the child's full-width reference plus the
/// filter state its subtree inherits.
struct Pending {
    reference: EntryRef,
    /// Full path of the node from the trie root.
    path: Vec<u8>,
    /// Arriving-fork value payload; `Some` exactly when the walk delivers an
    /// entry at this node.
    value: Option<BTreeMap<String, String>>,
    /// Prefix bytes still to match; non-empty means the node's path is a
    /// proper prefix of the requested one.
    goal: Vec<u8>,
    /// Resume bound suffix still to exceed; `Some` means the node's path is
    /// at or before the bound.
    after: Option<Vec<u8>>,
}

/// One frontier position, in depth-first order.
enum Slot {
    /// Awaiting a window slot.
    Queued(Pending),
    /// Fetch dispatched; the completion is matched back by id.
    Fetching(u64),
}

/// Completion payload: the fetch id, its node, and the load outcome.
type Fetched = (
    u64,
    Pending,
    Result<(Vec<u8>, Vec<ChunkAddress>), CursorError>,
);

/// One resolved node awaiting its serial turn: the pending record, its
/// image addresses, and the decoded view.
type Resolved = Result<(Pending, Vec<ChunkAddress>, NodeView), CursorError>;

/// One consumed node in depth-first order.
struct Visit {
    path: Vec<u8>,
    /// Every chunk address the node's stored image occupies, root first.
    addresses: Vec<ChunkAddress>,
    value: Option<BTreeMap<String, String>>,
    view: NodeView,
}

/// The bounded-lookahead walk: a path-keyed depth-first frontier whose head
/// is consumed in serial order while up to a window of fetches runs ahead.
///
/// Every completion, fault included, parks in `resolved` until its serial
/// turn at the head, so a lookahead fetch never fails a listing that stops
/// before it.
struct TrieWalk<L> {
    store: L,
    admission: Admission,
    frontier: VecDeque<Slot>,
    /// Completed fetches awaiting their serial turn at the head, keyed by id.
    resolved: BTreeMap<u64, Resolved>,
    in_flight: FuturesUnordered<BoxFuture<'static, Fetched>>,
    next_id: u64,
    /// Latched when a fault surfaces at the head or the in-flight set
    /// empties; a later poll must not resume the frontier past a terminal
    /// error.
    done: bool,
}

impl<L> TrieWalk<L>
where
    L: NodeLoader + Clone + 'static,
{
    fn new(
        store: L,
        root: EntryRef,
        goal: Vec<u8>,
        after: Option<Vec<u8>>,
        window: Window,
    ) -> Self {
        let mut frontier = VecDeque::new();
        frontier.push_back(Slot::Queued(Pending {
            reference: root,
            path: Vec::new(),
            value: None,
            goal,
            after,
        }));
        Self {
            store,
            admission: Admission::new(window),
            frontier,
            resolved: BTreeMap::new(),
            in_flight: FuturesUnordered::new(),
            next_id: 0,
            done: false,
        }
    }

    /// Deliver the next node in depth-first order, expanding it into its
    /// children first.
    ///
    /// Cancel-safe: all progress lives in `self`. `Ready(None)` after the
    /// last node or a terminal error.
    fn poll_visit(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Visit, CursorError>>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            self.admit();
            if let Some(outcome) = self.take_ready() {
                if outcome.is_err() {
                    self.done = true;
                }
                return Poll::Ready(Some(outcome));
            }
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some(fetched)) => self.absorb(fetched),
                Poll::Ready(None) => {
                    // Nothing in flight and no ready head: either the walk is
                    // complete or the frontier is owed work nobody will do.
                    self.done = true;
                    return Poll::Ready(if self.frontier.is_empty() {
                        None
                    } else {
                        Some(Err(CursorError::Stalled {
                            pending: self.frontier.len(),
                        }))
                    });
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Admit queued nodes into the window, lowest frontier position first.
    ///
    /// The head-slot predicate keeps the serial drain live: unconsumed
    /// fetches never exceed the window. The scan is O(window): every slot
    /// passed over or filled counts toward occupancy, which the window caps.
    #[inline]
    fn admit(&mut self) {
        let admission = self.admission;
        let mut occupancy = self.in_flight.len().saturating_add(self.resolved.len());
        let mut head_holds_slot = matches!(self.frontier.front(), Some(Slot::Fetching(_)));
        for (index, slot) in self.frontier.iter_mut().enumerate() {
            if matches!(slot, Slot::Fetching(_)) {
                continue;
            }
            if !admission.admits(occupancy, index == 0 || head_holds_slot) {
                return;
            }
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            let Slot::Queued(pending) = core::mem::replace(slot, Slot::Fetching(id)) else {
                // The slot was matched as queued above.
                return;
            };
            let store = self.store.clone();
            let reference = pending.reference.clone();
            let fetch: BoxFuture<'static, Fetched> =
                Box::pin(async move {
                    let fetched = store.load_with_addresses(&reference).await.map_err(|e| {
                        CursorError::Store {
                            address: *reference.address(),
                            source: Arc::new(e),
                        }
                    });
                    (id, pending, fetched)
                });
            self.in_flight.push(fetch);
            occupancy = occupancy.saturating_add(1);
            if index == 0 {
                head_holds_slot = true;
            }
        }
    }

    /// Consume the head once resolved, expanding it into its children; a
    /// parked fault surfaces here, at its serial turn.
    #[inline]
    fn take_ready(&mut self) -> Option<Result<Visit, CursorError>> {
        let Some(Slot::Fetching(id)) = self.frontier.front() else {
            return None;
        };
        let outcome = self.resolved.remove(id)?;
        self.frontier.pop_front();
        match outcome {
            Ok((pending, addresses, view)) => {
                self.expand(&pending, &view);
                Some(Ok(Visit {
                    path: pending.path,
                    addresses,
                    value: pending.value,
                    view,
                }))
            }
            Err(error) => Some(Err(error)),
        }
    }

    /// Park one completion for its serial turn; never eagerly terminal.
    #[inline]
    fn absorb(&mut self, (id, pending, fetched): Fetched) {
        let outcome = match fetched {
            Err(error) => Err(error),
            Ok((bytes, addresses)) => match NodeView::try_from(bytes.as_slice()) {
                Ok(view) => Ok((pending, addresses, view)),
                Err(source) => Err(CursorError::Corrupt {
                    address: *pending.reference.address(),
                    source,
                }),
            },
        };
        self.resolved.insert(id, outcome);
    }

    /// Queue the node's children at the frontier head in ascending fork
    /// order, pruning subtrees the prefix and resume bounds exclude.
    fn expand(&mut self, parent: &Pending, view: &NodeView) {
        for fork in view.forks().iter().rev() {
            if let Some(child) = child_pending(parent, fork) {
                self.frontier.push_front(Slot::Queued(child));
            }
        }
    }
}

/// The child subtree a fork roots, with narrowed filters; `None` when the
/// prefix or resume bound excludes the whole subtree.
fn child_pending(parent: &Pending, fork: &ForkView) -> Option<Pending> {
    let edge = fork.prefix();
    let goal = narrow_goal(&parent.goal, edge)?;
    let after = narrow_after(parent.after.as_deref(), edge)?;
    let mut path = parent.path.clone();
    path.extend_from_slice(edge);
    let value = (fork.node_type().contains(NodeType::VALUE) && goal.is_empty() && after.is_none())
        .then(|| fork.metadata().cloned().unwrap_or_default());
    Some(Pending {
        reference: fork.reference().clone(),
        path,
        value,
        goal,
        after,
    })
}

/// Prefix bytes still to match below `edge`; `None` prunes the subtree.
fn narrow_goal(goal: &[u8], edge: &[u8]) -> Option<Vec<u8>> {
    if goal.is_empty() || edge.starts_with(goal) {
        return Some(Vec::new());
    }
    goal.strip_prefix(edge).map(<[u8]>::to_vec)
}

/// Resume bound below `edge`: `None` prunes a subtree wholly at or before
/// the bound, `Some(None)` lifts the bound, `Some(Some(rest))` keeps
/// filtering.
fn narrow_after(after: Option<&[u8]>, edge: &[u8]) -> Option<Option<Vec<u8>>> {
    let Some(bound) = after else {
        return Some(None);
    };
    if let Some(rest) = bound.strip_prefix(edge) {
        return Some(Some(rest.to_vec()));
    }
    if edge > bound { Some(None) } else { None }
}

/// Ordered listing cursor over a persisted trie.
///
/// Yields value entries in path order under an optional prefix, resuming
/// strictly after an optional bound, up to an optional limit; the resume
/// token for the next page is the last yielded path. Configure before the
/// first poll; configuration set later is ignored.
pub struct Cursor<L> {
    store: L,
    root: EntryRef,
    window: Window,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    remaining: Option<usize>,
    walk: Option<TrieWalk<L>>,
}

impl<L> Cursor<L> {
    /// Cursor over the whole trie rooted at `root`, with the default window.
    pub fn new(store: L, root: impl Into<EntryRef>) -> Self {
        Self {
            store,
            root: root.into(),
            window: Window::DEFAULT,
            prefix: Vec::new(),
            after: None,
            remaining: None,
            walk: None,
        }
    }

    /// Replace the read-ahead window.
    #[must_use]
    pub const fn with_window(mut self, window: Window) -> Self {
        self.window = window;
        self
    }

    /// List only paths that start with `prefix`.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl AsRef<[u8]>) -> Self {
        self.prefix = prefix.as_ref().to_vec();
        self
    }

    /// Resume strictly after `path`, the last path of the previous page.
    #[must_use]
    pub fn after(mut self, path: impl AsRef<[u8]>) -> Self {
        self.after = Some(path.as_ref().to_vec());
        self
    }

    /// End the listing after `limit` entries.
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.remaining = Some(limit);
        self
    }

    /// The backing loader.
    #[must_use]
    pub const fn store(&self) -> &L {
        &self.store
    }
}

impl<L> Cursor<L>
where
    L: NodeLoader + Clone + 'static,
{
    /// Deliver the next entry in path order.
    ///
    /// Cancel-safe: all progress lives in `self`. `Ready(None)` after the
    /// last entry, the limit, or a terminal error.
    pub fn poll_next_entry(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Entry, CursorError>>> {
        if self.remaining == Some(0) {
            return Poll::Ready(None);
        }
        let walk = self.walk.get_or_insert_with(|| {
            TrieWalk::new(
                self.store.clone(),
                self.root.clone(),
                core::mem::take(&mut self.prefix),
                self.after.take(),
                self.window,
            )
        });
        loop {
            match walk.poll_visit(cx) {
                Poll::Ready(Some(Ok(visit))) => {
                    let Some(metadata) = visit.value else {
                        continue;
                    };
                    if let Some(remaining) = &mut self.remaining {
                        *remaining = remaining.saturating_sub(1);
                    }
                    return Poll::Ready(Some(Ok(Entry {
                        path: visit.path,
                        reference: visit.view.entry().cloned(),
                        metadata,
                    })));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Await the next entry; `None` when the listing is exhausted.
    pub async fn next(&mut self) -> Option<Result<Entry, CursorError>> {
        core::future::poll_fn(|cx| self.poll_next_entry(cx)).await
    }
}

impl<L> Stream for Cursor<L>
where
    L: NodeLoader + Clone + Unpin + 'static,
{
    type Item = Result<Entry, CursorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_entry(cx)
    }
}

impl<L> core::fmt::Debug for Cursor<L> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Cursor")
            .field("root", &self.root)
            .field("window", &self.window)
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

/// Depth-first address stream over a persisted trie: every chunk address a
/// node's stored image occupies (root first), with a value node's entry
/// address right after its node's addresses.
///
/// Enumerates every chunk the trie depends on, for pinning and garbage
/// collection; a multi-chunk node contributes all of its tree's addresses,
/// and shared subtrees repeat, matching the serial walk. Delivery order is
/// fixed by the trie, not the window. Configure before the first poll;
/// configuration set later is ignored.
pub struct AddressStream<L> {
    store: L,
    root: EntryRef,
    window: Window,
    queued: VecDeque<ChunkAddress>,
    walk: Option<TrieWalk<L>>,
}

impl<L> AddressStream<L> {
    /// Stream over the whole trie rooted at `root`, with the default window.
    pub fn new(store: L, root: impl Into<EntryRef>) -> Self {
        Self {
            store,
            root: root.into(),
            window: Window::DEFAULT,
            queued: VecDeque::new(),
            walk: None,
        }
    }

    /// Replace the read-ahead window.
    #[must_use]
    pub const fn with_window(mut self, window: Window) -> Self {
        self.window = window;
        self
    }

    /// The backing loader.
    #[must_use]
    pub const fn store(&self) -> &L {
        &self.store
    }
}

impl<L> AddressStream<L>
where
    L: NodeLoader + Clone + 'static,
{
    /// Deliver the next address in depth-first order.
    ///
    /// Cancel-safe: all progress lives in `self`. `Ready(None)` after the
    /// last address or a terminal error.
    pub fn poll_next_address(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ChunkAddress, CursorError>>> {
        loop {
            if let Some(address) = self.queued.pop_front() {
                return Poll::Ready(Some(Ok(address)));
            }
            let walk = self.walk.get_or_insert_with(|| {
                TrieWalk::new(
                    self.store.clone(),
                    self.root.clone(),
                    Vec::new(),
                    None,
                    self.window,
                )
            });
            match walk.poll_visit(cx) {
                Poll::Ready(Some(Ok(visit))) => {
                    self.queued.extend(visit.addresses);
                    if visit.value.is_some()
                        && let Some(entry) = visit.view.entry()
                    {
                        self.queued.push_back(*entry.address());
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Await the next address; `None` when the trie is exhausted.
    pub async fn next(&mut self) -> Option<Result<ChunkAddress, CursorError>> {
        core::future::poll_fn(|cx| self.poll_next_address(cx)).await
    }
}

impl<L> Stream for AddressStream<L>
where
    L: NodeLoader + Clone + Unpin + 'static,
{
    type Item = Result<ChunkAddress, CursorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_address(cx)
    }
}

impl<L> core::fmt::Debug for AddressStream<L> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AddressStream")
            .field("root", &self.root)
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use bytes::Bytes;
    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::chunk::{ChunkOps, ChunkRef, ContentChunk};
    use nectar_primitives::store::{ChunkGet, ChunkPut, MemoryStore, VerifyError, VerifyingStore};
    use nectar_primitives::{
        Chunk, EncryptedChunkRef, EncryptionKey, StandardChunkSet, Unverified,
    };
    use nectar_testing::run;

    use crate::ManifestEditor;
    use crate::node::{Fork, Node, Prefix};
    use crate::persist::single_chunk::{SingleChunkError, SingleChunkLoadSaver};

    type Store = MemoryStore<StandardChunkSet>;
    type LoadSaver = SingleChunkLoadSaver<Store>;

    /// A ChunkAddress from a string, right-padded with zeroes.
    fn make_addr(s: &str) -> ChunkAddress {
        let bytes = s.as_bytes();
        let mut buf = [0u8; 32];
        let len = bytes.len().min(32);
        buf[..len].copy_from_slice(&bytes[..len]);
        ChunkAddress::from(buf)
    }

    fn window(slots: u16) -> Window {
        Window::new(slots).unwrap()
    }

    /// Trie shapes covering prefix splits, nested folders, one-byte edge
    /// chains, and edges longer than the 30-byte prefix limit.
    fn corpora() -> Vec<Vec<&'static str>> {
        vec![
            vec!["a"],
            vec![
                "aa", "b", "aaaaaa", "aaaaab", "abbbb", "abbba", "bbbbba", "bbbaaa", "bbbaab",
            ],
            vec!["index.html", "img/1.png", "img/2.png", "robots.txt"],
            vec![
                "a/b/c/d/e/f/g/h/file00.dat",
                "a/b/c/d/e/f/g/h/file01.dat",
                "a/b/c/x.txt",
            ],
            vec!["a", "ab", "abc", "abcd", "abcde"],
            vec!["oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure"],
        ]
    }

    /// Build a persisted plain manifest over the paths through the editor.
    fn build(paths: &[&str]) -> (ChunkAddress, LoadSaver) {
        let mut editor: ManifestEditor<LoadSaver> =
            ManifestEditor::new(LoadSaver::new(Store::new()));
        for &p in paths {
            editor.put(p, make_addr(p));
        }
        run(editor.commit()).unwrap()
    }

    /// Every chunk address held by the loadsaver's backing store.
    fn stored_addresses(loadsaver: &LoadSaver) -> Vec<ChunkAddress> {
        loadsaver
            .store()
            .clone()
            .into_chunks()
            .keys()
            .copied()
            .collect()
    }

    fn collect_entries<L>(mut cursor: Cursor<L>) -> Vec<Entry>
    where
        L: NodeLoader + Clone + 'static,
    {
        run(async {
            let mut out = Vec::new();
            while let Some(item) = cursor.next().await {
                out.push(item.unwrap());
            }
            out
        })
    }

    fn collect_until_err<L>(mut cursor: Cursor<L>) -> (Vec<Entry>, Option<CursorError>)
    where
        L: NodeLoader + Clone + 'static,
    {
        run(async {
            let mut out = Vec::new();
            while let Some(item) = cursor.next().await {
                match item {
                    Ok(entry) => out.push(entry),
                    Err(error) => return (out, Some(error)),
                }
            }
            (out, None)
        })
    }

    fn collect_addresses<L>(mut stream: AddressStream<L>) -> Vec<ChunkAddress>
    where
        L: NodeLoader + Clone + 'static,
    {
        run(async {
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item.unwrap());
            }
            out
        })
    }

    /// Loader wrapper recording node loads, concurrency peaks, and scripted
    /// faults; `Clone` shares one recording.
    #[derive(Clone)]
    struct RecordingStore {
        inner: std::sync::Arc<Recording>,
    }

    struct Recording {
        store: LoadSaver,
        fetched: Mutex<Vec<ChunkAddress>>,
        inflight: AtomicUsize,
        peak: AtomicUsize,
        delay: bool,
        fail: Option<ChunkAddress>,
    }

    impl RecordingStore {
        fn with(store: LoadSaver, delay: bool, fail: Option<ChunkAddress>) -> Self {
            Self {
                inner: std::sync::Arc::new(Recording {
                    store,
                    fetched: Mutex::new(Vec::new()),
                    inflight: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                    delay,
                    fail,
                }),
            }
        }

        fn new(store: LoadSaver) -> Self {
            Self::with(store, false, None)
        }

        fn delayed(store: LoadSaver) -> Self {
            Self::with(store, true, None)
        }

        fn failing(store: LoadSaver, fail: ChunkAddress) -> Self {
            Self::with(store, false, Some(fail))
        }

        fn fetched(&self) -> Vec<ChunkAddress> {
            self.inner.fetched.lock().unwrap().clone()
        }

        fn fetch_count(&self) -> usize {
            self.inner.fetched.lock().unwrap().len()
        }

        fn peak(&self) -> usize {
            self.inner.peak.load(Ordering::SeqCst)
        }
    }

    /// Yield once so queued sibling fetches can ramp their in-flight count
    /// before any single fetch resolves.
    async fn yield_once() {
        let mut yielded = false;
        futures_util::future::poll_fn(|cx| {
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

    impl NodeLoader for RecordingStore {
        type Error = SingleChunkError;

        async fn load(&self, reference: &EntryRef) -> Result<Vec<u8>, Self::Error> {
            let address = *reference.address();
            self.inner.fetched.lock().unwrap().push(address);
            let level = self.inner.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.inner.peak.fetch_max(level, Ordering::SeqCst);
            if self.inner.delay {
                yield_once().await;
            }
            let result = if self.inner.fail == Some(address) {
                self.inner
                    .store
                    .load(&EntryRef::from(make_addr("absent-sentinel")))
                    .await
            } else {
                self.inner.store.load(reference).await
            };
            self.inner.inflight.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    /// Serial truth from a window-one walk: the fetch sequence and the
    /// cumulative fetch count at each yielded entry.
    fn serial_profile(root: ChunkAddress, store: &LoadSaver) -> (Vec<ChunkAddress>, Vec<usize>) {
        let rec = RecordingStore::new(store.clone());
        let mut cursor: Cursor<RecordingStore> =
            Cursor::new(rec.clone(), root).with_window(window(1));
        let mut counts = Vec::new();
        run(async {
            while let Some(item) = cursor.next().await {
                item.unwrap();
                counts.push(rec.fetch_count());
            }
        });
        (rec.fetched(), counts)
    }

    #[test]
    fn listing_yields_every_path_in_path_order() {
        for paths in corpora() {
            let (root, loadsaver) = build(&paths);
            let got = collect_entries(Cursor::new(loadsaver, root));
            let mut want = paths.clone();
            want.sort_unstable();
            assert_eq!(got.len(), want.len(), "corpus {paths:?}");
            for (entry, path) in got.iter().zip(&want) {
                assert_eq!(entry.path(), path.as_bytes(), "corpus {paths:?}");
                assert_eq!(
                    entry.reference().map(|r| *r.address()),
                    Some(make_addr(path)),
                    "reference for {path:?}"
                );
                assert!(entry.metadata().is_empty(), "metadata for {path:?}");
            }
        }
    }

    #[test]
    fn metadata_and_root_document_survive_the_listing() {
        let mut editor: ManifestEditor<LoadSaver> =
            ManifestEditor::new(LoadSaver::new(Store::new()));
        editor.put("plain.txt", make_addr("plain"));
        let meta: BTreeMap<String, String> =
            [("Content-Type".to_string(), "image/png".to_string())].into();
        editor.put_with_metadata("logo.png", make_addr("logo"), meta.clone());
        editor.set_index_document("index.html");
        let (root, loadsaver) = run(editor.commit()).unwrap();

        let got = collect_entries(Cursor::new(loadsaver, root));
        assert_eq!(got.len(), 3);
        let plain = got.iter().find(|e| e.path() == b"plain.txt").unwrap();
        assert_eq!(
            plain.reference().map(|r| *r.address()),
            Some(make_addr("plain"))
        );
        let logo = got.iter().find(|e| e.path() == b"logo.png").unwrap();
        assert_eq!(logo.metadata(), &meta);
        let doc = got.iter().find(|e| e.path() == b"/").unwrap();
        assert!(doc.reference().is_none());
        assert_eq!(
            doc.metadata().get("website-index-document").cloned(),
            Some("index.html".to_string())
        );
    }

    #[test]
    fn encrypted_listing_yields_the_stored_references() {
        let paths = ["secret/a.txt", "secret/b.txt", "top.txt"];
        let key = EncryptionKey::from([0x5a; 32]);
        let mut editor: ManifestEditor<LoadSaver, EncryptedChunkRef> =
            ManifestEditor::new_encrypted(LoadSaver::new(Store::new()));
        for p in paths {
            editor.put(p, EncryptedChunkRef::new(make_addr(p), key.clone()));
        }
        let (root, loadsaver) = run(editor.commit()).unwrap();

        let got = collect_entries(Cursor::new(loadsaver, root));
        let mut want = paths.to_vec();
        want.sort_unstable();
        assert_eq!(got.len(), want.len());
        for (entry, path) in got.iter().zip(&want) {
            assert_eq!(entry.path(), path.as_bytes());
            match entry.reference() {
                Some(EntryRef::Encrypted(reference)) => {
                    assert_eq!(reference.address(), &make_addr(path));
                    assert_eq!(reference.key(), &key);
                }
                other => panic!("expected an encrypted reference, got {other:?}"),
            }
        }
    }

    #[test]
    fn prefix_narrows_the_listing() {
        for paths in corpora() {
            let (root, loadsaver) = build(&paths);
            let full = collect_entries(Cursor::new(loadsaver.clone(), root));
            let mut probes = vec![String::new(), "zzz-absent".to_string()];
            for p in &paths {
                probes.push((*p).to_string());
                if p.len() > 1 {
                    probes.push(p[..1].to_string());
                    probes.push(p[..p.len() - 1].to_string());
                }
                probes.push(format!("{p}x"));
            }
            for probe in probes {
                let want: Vec<Entry> = full
                    .iter()
                    .filter(|e| e.path().starts_with(probe.as_bytes()))
                    .cloned()
                    .collect();
                let got = collect_entries(Cursor::new(loadsaver.clone(), root).with_prefix(&probe));
                assert_eq!(got, want, "prefix {probe:?} over {paths:?}");
            }
        }
    }

    #[test]
    fn resume_after_continues_where_the_page_ended() {
        for paths in corpora() {
            let (root, loadsaver) = build(&paths);
            let full = collect_entries(Cursor::new(loadsaver.clone(), root));
            for k in 0..full.len() {
                let page = collect_entries(Cursor::new(loadsaver.clone(), root).with_limit(k));
                assert_eq!(page.as_slice(), &full[..k]);
                let mut resumed = Cursor::new(loadsaver.clone(), root);
                if let Some(last) = page.last() {
                    resumed = resumed.after(last.path());
                }
                let rest = collect_entries(resumed);
                let mut joined = page;
                joined.extend(rest);
                assert_eq!(joined, full, "page {k} over {paths:?}");
            }
        }
    }

    #[test]
    fn resume_tokens_need_not_be_stored_paths() {
        for paths in corpora() {
            let (root, loadsaver) = build(&paths);
            let full = collect_entries(Cursor::new(loadsaver.clone(), root));
            let mut tokens = vec![String::new(), "zzz-absent".to_string()];
            for p in &paths {
                tokens.push(format!("{p}0"));
                if p.len() > 1 {
                    tokens.push(p[..p.len() - 1].to_string());
                }
            }
            for token in tokens {
                let want: Vec<Entry> = full
                    .iter()
                    .filter(|e| e.path() > token.as_bytes())
                    .cloned()
                    .collect();
                let got = collect_entries(Cursor::new(loadsaver.clone(), root).after(&token));
                assert_eq!(got, want, "token {token:?} over {paths:?}");
            }
        }
    }

    #[test]
    fn prefix_and_resume_compose() {
        let paths = [
            "index.html",
            "img/1.png",
            "img/2.png",
            "img/3.png",
            "robots.txt",
        ];
        let (root, loadsaver) = build(&paths);
        let full = collect_entries(Cursor::new(loadsaver.clone(), root));
        let want: Vec<Entry> = full
            .iter()
            .filter(|e| e.path().starts_with(b"img/") && e.path() > b"img/1.png".as_slice())
            .cloned()
            .collect();
        let got = collect_entries(
            Cursor::new(loadsaver, root)
                .with_prefix("img/")
                .after("img/1.png"),
        );
        assert_eq!(got, want);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn zero_limit_lists_nothing_and_fetches_nothing() {
        let (root, loadsaver) = build(&["a", "b"]);
        let rec = RecordingStore::new(loadsaver);
        let got = collect_entries(Cursor::new(rec.clone(), root).with_limit(0));
        assert!(got.is_empty());
        assert_eq!(rec.fetch_count(), 0);
    }

    #[test]
    fn fetched_set_stays_within_the_serial_set_plus_window() {
        for paths in corpora() {
            let (root, loadsaver) = build(&paths);
            let (serial_seq, counts) = serial_profile(root, &loadsaver);
            let serial_set: std::collections::BTreeSet<ChunkAddress> =
                serial_seq.iter().copied().collect();
            for w in [1u16, 2, 4, 16] {
                for k in 1..=counts.len() {
                    let rec = RecordingStore::new(loadsaver.clone());
                    let page = collect_entries(
                        Cursor::new(rec.clone(), root)
                            .with_window(window(w))
                            .with_limit(k),
                    );
                    assert_eq!(page.len(), k);
                    let fetched = rec.fetched();
                    assert!(
                        fetched.iter().all(|a| serial_set.contains(a)),
                        "window {w} page {k}: fetched outside the serial set"
                    );
                    assert!(
                        fetched.len() <= counts[k - 1] + usize::from(w),
                        "window {w} page {k}: {} fetches exceed serial {} + window",
                        fetched.len(),
                        counts[k - 1]
                    );
                }
                let rec = RecordingStore::new(loadsaver.clone());
                let full = collect_entries(Cursor::new(rec.clone(), root).with_window(window(w)));
                assert_eq!(full.len(), counts.len());
                let mut got = rec.fetched();
                got.sort();
                let mut want = serial_seq.clone();
                want.sort();
                assert_eq!(got, want, "window {w}: full-walk fetch multiset");
            }
        }
    }

    #[test]
    fn in_flight_fetches_stay_within_the_window_and_overlap() {
        let paths: Vec<String> = (0..24).map(|i| format!("file{i:02}.dat")).collect();
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        let (root, loadsaver) = build(&refs);
        for w in [1u16, 4, 8] {
            let rec = RecordingStore::delayed(loadsaver.clone());
            let got = collect_entries(Cursor::new(rec.clone(), root).with_window(window(w)));
            assert_eq!(got.len(), paths.len());
            let peak = rec.peak();
            assert!(peak <= usize::from(w), "peak {peak} exceeds window {w}");
            if w == 1 {
                assert_eq!(peak, 1, "window one must be serial");
            } else {
                assert!(peak > 1, "window {w} fetches must overlap (peak {peak})");
            }
        }
    }

    #[test]
    fn store_failure_surfaces_at_its_serial_position() {
        let paths = [
            "a/b/c/d/e/f/g/h/file00.dat",
            "a/b/c/d/e/f/g/h/file01.dat",
            "a/b/c/x.txt",
            "zz.txt",
        ];
        let (root, loadsaver) = build(&paths);
        let (serial_seq, _) = serial_profile(root, &loadsaver);
        for victim_pos in [0, serial_seq.len() / 2, serial_seq.len() - 1] {
            let victim = serial_seq[victim_pos];
            let (want_entries, want_err) = collect_until_err(
                Cursor::new(RecordingStore::failing(loadsaver.clone(), victim), root)
                    .with_window(window(1)),
            );
            assert!(
                matches!(want_err, Some(CursorError::Store { address, .. }) if address == victim)
            );
            for w in [2u16, 16] {
                let (entries, err) = collect_until_err(
                    Cursor::new(RecordingStore::failing(loadsaver.clone(), victim), root)
                        .with_window(window(w)),
                );
                assert_eq!(entries, want_entries, "victim {victim_pos} window {w}");
                assert!(
                    matches!(err, Some(CursorError::Store { address, .. }) if address == victim)
                );
            }
            // A limit that stops before the failing node never sees the
            // error, even when the lookahead already fetched it.
            let (entries, err) = collect_until_err(
                Cursor::new(RecordingStore::failing(loadsaver.clone(), victim), root)
                    .with_window(window(16))
                    .with_limit(want_entries.len()),
            );
            assert_eq!(entries, want_entries);
            assert!(err.is_none(), "victim {victim_pos}: parked error surfaced");
        }
    }

    /// A terminal error ends the walk. The siblings beyond the failing node
    /// are still queued on the frontier, so a walk that resumed after the
    /// fault would keep delivering entries past it.
    #[test]
    fn a_terminal_error_ends_the_listing() {
        let (root, loadsaver) = build(&["a", "b", "c"]);
        let (serial_seq, _) = serial_profile(root, &loadsaver);
        // The first child: its two siblings are queued when it fails.
        let victim = serial_seq[1];
        assert_ne!(victim, root);
        let mut cursor =
            Cursor::new(RecordingStore::failing(loadsaver, victim), root).with_window(window(4));
        run(async {
            assert!(matches!(
                cursor.next().await,
                Some(Err(CursorError::Store { address, .. })) if address == victim
            ));
            assert!(
                cursor.next().await.is_none(),
                "the listing resumed past a terminal error"
            );
            assert!(cursor.next().await.is_none());
        });
    }

    /// The same latch on the address stream, which shares the walk.
    #[test]
    fn a_terminal_error_ends_the_address_stream() {
        let (root, loadsaver) = build(&["a", "b", "c"]);
        let (serial_seq, _) = serial_profile(root, &loadsaver);
        let victim = serial_seq[1];
        let mut stream = AddressStream::new(RecordingStore::failing(loadsaver, victim), root)
            .with_window(window(4));
        run(async {
            // The root's own addresses precede the failing child.
            let mut failed = false;
            while let Some(item) = stream.next().await {
                if let Err(error) = item {
                    assert!(
                        matches!(error, CursorError::Store { address, .. } if address == victim)
                    );
                    failed = true;
                    break;
                }
            }
            assert!(failed, "the failing child must fault the stream");
            assert!(
                stream.next().await.is_none(),
                "the stream resumed past a terminal error"
            );
        });
    }

    #[test]
    fn undecodable_child_is_a_corrupt_error() {
        let store = Store::new();
        let garbage =
            ContentChunk::<DEFAULT_BODY_SIZE>::new(Bytes::from_static(b"not a mantaray node"))
                .unwrap();
        let gaddr = *garbage.address();
        let sealed: Chunk = Chunk::from_envelope(garbage.into()).unwrap();
        run(store.put(sealed)).unwrap();

        let mut child = Node::<ChunkRef>::from_reference(ChunkRef::from(gaddr));
        child.node_type = NodeType::VALUE;
        let mut trie = Node::<ChunkRef>::new_unencrypted();
        trie.forks.insert(
            b'x',
            Fork {
                prefix: Prefix::from_slice(b"x"),
                node: child,
            },
        );
        let image = trie.encode().unwrap();
        let root_chunk = ContentChunk::<DEFAULT_BODY_SIZE>::new(Bytes::from(image)).unwrap();
        let root = *root_chunk.address();
        let sealed: Chunk = Chunk::from_envelope(root_chunk.into()).unwrap();
        run(store.put(sealed)).unwrap();

        let (entries, err) = collect_until_err(Cursor::new(LoadSaver::new(store), root));
        assert!(entries.is_empty());
        assert!(matches!(err, Some(CursorError::Corrupt { address, .. }) if address == gaddr));
    }

    /// Misrouting store: answers `at` with the chunk stored at `with`,
    /// declared untrusted so the verifying boundary is the guard.
    #[derive(Clone)]
    struct MisroutedStore {
        store: Store,
        at: ChunkAddress,
        with: ChunkAddress,
    }

    type MisrouteError = VerifyError<<Store as ChunkGet<StandardChunkSet>>::Error>;

    impl ChunkGet<StandardChunkSet> for MisroutedStore {
        type Trust = Unverified;
        type Error = <Store as ChunkGet<StandardChunkSet>>::Error;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Unverified, StandardChunkSet>, Self::Error> {
            let target = if *address == self.at {
                self.with
            } else {
                *address
            };
            let chunk = ChunkGet::get(&self.store, &target).await?;
            Ok(Chunk::parse(*chunk.address(), &chunk.typed_bytes()).unwrap())
        }
    }

    #[test]
    fn misrouted_store_is_caught_at_the_verifying_boundary() {
        let (root, loadsaver) = build(&["a", "b"]);
        let (serial_seq, _) = serial_profile(root, &loadsaver);
        let other = *serial_seq.last().unwrap();
        assert_ne!(other, root);
        let lifted = SingleChunkLoadSaver::<_, DEFAULT_BODY_SIZE>::new(VerifyingStore::new(
            MisroutedStore {
                store: loadsaver.into_store(),
                at: root,
                with: other,
            },
        ));
        let (entries, err) = collect_until_err(Cursor::new(lifted, root));
        assert!(entries.is_empty());
        let Some(CursorError::Store { address, source }) = err else {
            panic!("expected a store error, got {err:?}");
        };
        assert_eq!(address, root);
        let Some(SingleChunkError::Store(inner)) = source.downcast_ref::<SingleChunkError>() else {
            panic!("expected a wrapped store failure, got {source:?}");
        };
        assert!(matches!(
            inner.downcast_ref::<MisrouteError>(),
            Some(VerifyError::AddressMismatch { requested, returned })
                if *requested == root && *returned == other
        ));
    }

    #[test]
    fn address_stream_covers_nodes_and_entries() {
        for paths in corpora() {
            let (root, loadsaver) = build(&paths);
            let ordered = collect_addresses(AddressStream::new(loadsaver.clone(), root));
            let windowed = collect_addresses(
                AddressStream::new(loadsaver.clone(), root).with_window(window(8)),
            );
            assert_eq!(
                ordered, windowed,
                "delivery order must not depend on the window"
            );
            // The commit stored exactly the trie nodes, so the stream must
            // cover every stored node plus every value reference.
            let mut got = ordered;
            got.sort();
            let mut want = stored_addresses(&loadsaver);
            want.extend(paths.iter().map(|p| make_addr(p)));
            want.sort();
            assert_eq!(got, want, "corpus {paths:?}");
        }
    }

    #[test]
    fn encrypted_address_stream_covers_nodes_and_entries() {
        let paths = ["secret/a.txt", "secret/b.txt", "top.txt"];
        let mut editor: ManifestEditor<LoadSaver, EncryptedChunkRef> =
            ManifestEditor::new_encrypted(LoadSaver::new(Store::new()));
        for p in paths {
            editor.put(
                p,
                EncryptedChunkRef::new(make_addr(p), EncryptionKey::from([0x5a; 32])),
            );
        }
        let (root, loadsaver) = run(editor.commit()).unwrap();

        // Value entries ride the full encrypted width on the wire; the
        // stream carries their 32-byte addresses next to every node address.
        let mut got = collect_addresses(AddressStream::new(loadsaver.clone(), root));
        got.sort();
        let mut want = stored_addresses(&loadsaver);
        want.extend(paths.iter().map(|p| make_addr(p)));
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn empty_trie_lists_nothing_and_streams_only_the_root() {
        let editor: ManifestEditor<LoadSaver> = ManifestEditor::new(LoadSaver::new(Store::new()));
        let (root, loadsaver) = run(editor.commit()).unwrap();
        assert!(collect_entries(Cursor::new(loadsaver.clone(), root)).is_empty());
        assert_eq!(
            collect_addresses(AddressStream::new(loadsaver, root)),
            vec![root]
        );
    }

    #[test]
    fn missing_root_is_a_store_error() {
        let root = make_addr("nowhere");
        let (entries, err) = collect_until_err(Cursor::new(LoadSaver::new(Store::new()), root));
        assert!(entries.is_empty());
        assert!(matches!(err, Some(CursorError::Store { address, .. }) if address == root));
    }

    #[test]
    fn cursor_and_address_stream_drive_as_streams() {
        use futures_util::StreamExt;
        let (root, loadsaver) = build(&["a", "b", "c"]);
        let entries: Vec<_> = run(Cursor::new(loadsaver.clone(), root).collect::<Vec<_>>());
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(Result::is_ok));
        let addresses: Vec<_> = run(AddressStream::new(loadsaver, root).collect::<Vec<_>>());
        assert!(addresses.len() > 3);
        assert!(addresses.iter().all(Result::is_ok));
    }
}
