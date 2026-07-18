//! Ordered listing cursor and address stream over persisted mantaray tries.
//!
//! Both walk a trie depth-first in ascending fork order, fetching nodes
//! through a bounded read-ahead window: unconsumed fetches never exceed the
//! window, so the fetched set is the serial walk's consumed set plus at most
//! one window of lookahead, and errors surface at the failing node's serial
//! position, never earlier. The admission scheduler mirrors the file walk
//! engine's, whose byte-offset sequencing seam does not fit path-keyed trie
//! order, so the shape is copied rather than shared.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, VecDeque};
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::future::Future;
use core::num::NonZeroU16;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures::Stream;
use futures::stream::FuturesUnordered;
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::{ChunkAddress, ChunkOps};
use nectar_primitives::store::TrustedGet;
use nectar_primitives::{AnyChunkSet, Chunk, Verified};

use crate::entry::Entry;
use crate::error::CursorError;
use crate::node::NodeType;
use crate::view::{ForkView, NodeView};

/// Sixteen slots: the default read-ahead depth.
const DEFAULT_SLOTS: NonZeroU16 = match NonZeroU16::new(16) {
    Some(slots) => slots,
    None => NonZeroU16::MIN,
};
const _: () = assert!(DEFAULT_SLOTS.get() == 16);

/// Read-ahead window: node fetches a walk may hold unconsumed, in flight or
/// buffered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Window(NonZeroU16);

impl Window {
    /// Default window of sixteen fetch slots.
    pub const DEFAULT: Self = Self(DEFAULT_SLOTS);

    /// `None` when `slots` is zero; const twin of the `NonZeroU16`
    /// conversion.
    pub const fn new(slots: u16) -> Option<Self> {
        match NonZeroU16::new(slots) {
            Some(slots) => Some(Self(slots)),
            None => None,
        }
    }

    /// Window depth in slots.
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl Default for Window {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl From<NonZeroU16> for Window {
    fn from(slots: NonZeroU16) -> Self {
        Self(slots)
    }
}

impl From<Window> for NonZeroU16 {
    fn from(window: Window) -> Self {
        window.0
    }
}

/// One queued subtree root: the child's address plus the filter state its
/// subtree inherits.
struct Pending {
    address: ChunkAddress,
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

/// Completion payload: the fetch id, its node, and the store outcome.
type Fetched<const B: usize> = (
    u64,
    Pending,
    Result<Chunk<Verified, AnyChunkSet<B>>, CursorError>,
);

/// Boxed fetch future: `Send` on native, unbounded on wasm32 so `!Send`
/// browser stores stay usable.
#[cfg(not(target_arch = "wasm32"))]
type BoxFetch<const B: usize> = Pin<Box<dyn Future<Output = Fetched<B>> + Send>>;
/// Boxed fetch future: `Send` on native, unbounded on wasm32 so `!Send`
/// browser stores stay usable.
#[cfg(target_arch = "wasm32")]
type BoxFetch<const B: usize> = Pin<Box<dyn Future<Output = Fetched<B>>>>;

/// One consumed node in depth-first order.
struct Visit {
    path: Vec<u8>,
    address: ChunkAddress,
    value: Option<BTreeMap<String, String>>,
    view: NodeView,
}

/// The shared bounded-lookahead walk: a depth-first frontier whose head is
/// consumed in serial order while up to a window of fetches runs ahead.
struct TrieWalk<S, const BS: usize> {
    store: S,
    window: usize,
    frontier: VecDeque<Slot>,
    in_flight: FuturesUnordered<BoxFetch<BS>>,
    in_flight_count: usize,
    /// Completed fetches awaiting their serial turn at the head, keyed by id.
    resolved: BTreeMap<u64, Result<(Pending, NodeView), CursorError>>,
    next_id: u64,
    done: bool,
}

impl<S, const BS: usize> TrieWalk<S, BS>
where
    S: TrustedGet<AnyChunkSet<BS>> + Clone + 'static,
{
    fn new(
        store: S,
        root: ChunkAddress,
        goal: Vec<u8>,
        after: Option<Vec<u8>>,
        window: Window,
    ) -> Self {
        let mut frontier = VecDeque::new();
        frontier.push_back(Slot::Queued(Pending {
            address: root,
            path: Vec::new(),
            value: None,
            goal,
            after,
        }));
        Self {
            store,
            window: usize::from(window.get()),
            frontier,
            in_flight: FuturesUnordered::new(),
            in_flight_count: 0,
            resolved: BTreeMap::new(),
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
            let head = match self.frontier.front() {
                Some(Slot::Fetching(id)) => self.resolved.remove(id),
                Some(Slot::Queued(_)) => None,
                None => {
                    self.done = true;
                    return Poll::Ready(None);
                }
            };
            if let Some(outcome) = head {
                self.frontier.pop_front();
                match outcome {
                    Ok((pending, view)) => {
                        self.expand(&pending, &view);
                        return Poll::Ready(Some(Ok(Visit {
                            path: pending.path,
                            address: pending.address,
                            value: pending.value,
                            view,
                        })));
                    }
                    Err(error) => {
                        self.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some((id, pending, fetched))) => self.absorb(id, pending, fetched),
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(CursorError::Stalled {
                        pending: self.frontier.len(),
                    })));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Admit queued nodes into the window, lowest frontier position first.
    ///
    /// One slot stays reserved for the head until the head occupies one, so
    /// the serial drain always progresses; unconsumed fetches never exceed
    /// the window. The scan is O(window): every slot passed over or filled
    /// counts toward occupancy, which the window caps.
    fn admit(&mut self) {
        let mut occupancy = self.in_flight_count.saturating_add(self.resolved.len());
        let mut head_holds_slot = matches!(self.frontier.front(), Some(Slot::Fetching(_)));
        for (index, slot) in self.frontier.iter_mut().enumerate() {
            if occupancy >= self.window {
                return;
            }
            if matches!(slot, Slot::Fetching(_)) {
                continue;
            }
            if index > 0 && !head_holds_slot && occupancy >= self.window.saturating_sub(1) {
                return;
            }
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            let Slot::Queued(pending) = core::mem::replace(slot, Slot::Fetching(id)) else {
                // The slot was matched as queued above.
                return;
            };
            let store = self.store.clone();
            let address = pending.address;
            let fetch: BoxFetch<BS> = Box::pin(async move {
                let fetched = store.get(&address).await.map_err(|e| CursorError::Store {
                    address,
                    source: Arc::new(e),
                });
                (id, pending, fetched)
            });
            self.in_flight.push(fetch);
            self.in_flight_count = self.in_flight_count.saturating_add(1);
            occupancy = occupancy.saturating_add(1);
            if index == 0 {
                head_holds_slot = true;
            }
        }
    }

    /// Fold one completion into the resolved set; a failure waits for its
    /// serial turn, so a lookahead fetch never fails a listing that stops
    /// before it.
    fn absorb(
        &mut self,
        id: u64,
        pending: Pending,
        fetched: Result<Chunk<Verified, AnyChunkSet<BS>>, CursorError>,
    ) {
        self.in_flight_count = self.in_flight_count.saturating_sub(1);
        let outcome = match fetched {
            Err(error) => Err(error),
            Ok(chunk) => {
                let returned = *chunk.address();
                if returned == pending.address {
                    match NodeView::try_from(chunk.envelope().data().as_ref()) {
                        Ok(view) => Ok((pending, view)),
                        Err(source) => Err(CursorError::Corrupt {
                            address: pending.address,
                            source,
                        }),
                    }
                } else {
                    Err(CursorError::AddressMismatch {
                        requested: pending.address,
                        returned,
                    })
                }
            }
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
        address: *fork.reference().address(),
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
///
/// ```
/// # use nectar_mantaray::{Cursor, ManifestEditor, DefaultMemoryStore};
/// # use nectar_primitives::chunk::ChunkAddress;
/// # futures::executor::block_on(async {
/// let mut editor = ManifestEditor::new(DefaultMemoryStore::new());
/// editor.put("a.txt", ChunkAddress::from([1u8; 32]));
/// editor.put("b/c.txt", ChunkAddress::from([2u8; 32]));
/// let (root, store) = editor.commit().await.unwrap();
/// let mut cursor = Cursor::new(store, root).with_prefix("b/");
/// let entry = cursor.next().await.unwrap().unwrap();
/// assert_eq!(entry.path(), b"b/c.txt");
/// assert!(cursor.next().await.is_none());
/// # });
/// ```
pub struct Cursor<S, const BS: usize = DEFAULT_BODY_SIZE> {
    store: S,
    root: ChunkAddress,
    window: Window,
    prefix: Vec<u8>,
    after: Option<Vec<u8>>,
    remaining: Option<usize>,
    walk: Option<TrieWalk<S, BS>>,
}

impl<S, const BS: usize> Cursor<S, BS> {
    /// Cursor over the whole trie rooted at `root`, with the default window.
    pub const fn new(store: S, root: ChunkAddress) -> Self {
        Self {
            store,
            root,
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

    /// The backing store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<S, const BS: usize> Cursor<S, BS>
where
    S: TrustedGet<AnyChunkSet<BS>> + Clone + 'static,
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
                self.root,
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

impl<S, const BS: usize> Stream for Cursor<S, BS>
where
    S: TrustedGet<AnyChunkSet<BS>> + Clone + Unpin + 'static,
{
    type Item = Result<Entry, CursorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_entry(cx)
    }
}

impl<S, const BS: usize> core::fmt::Debug for Cursor<S, BS> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Cursor")
            .field("root", &self.root)
            .field("window", &self.window)
            .field("remaining", &self.remaining)
            .finish_non_exhaustive()
    }
}

/// Depth-first address stream over a persisted trie: every node chunk
/// address, with a value node's entry address right after its node.
///
/// Enumerates every chunk the trie depends on, for pinning and garbage
/// collection; shared subtrees repeat, matching the serial walk. Delivery
/// order is fixed by the trie, not the window. Configure before the first
/// poll; configuration set later is ignored.
pub struct AddressStream<S, const BS: usize = DEFAULT_BODY_SIZE> {
    store: S,
    root: ChunkAddress,
    window: Window,
    queued: Option<ChunkAddress>,
    walk: Option<TrieWalk<S, BS>>,
}

impl<S, const BS: usize> AddressStream<S, BS> {
    /// Stream over the whole trie rooted at `root`, with the default window.
    pub const fn new(store: S, root: ChunkAddress) -> Self {
        Self {
            store,
            root,
            window: Window::DEFAULT,
            queued: None,
            walk: None,
        }
    }

    /// Replace the read-ahead window.
    #[must_use]
    pub const fn with_window(mut self, window: Window) -> Self {
        self.window = window;
        self
    }

    /// The backing store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }
}

impl<S, const BS: usize> AddressStream<S, BS>
where
    S: TrustedGet<AnyChunkSet<BS>> + Clone + 'static,
{
    /// Deliver the next address in depth-first order.
    ///
    /// Cancel-safe: all progress lives in `self`. `Ready(None)` after the
    /// last address or a terminal error.
    pub fn poll_next_address(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ChunkAddress, CursorError>>> {
        if let Some(address) = self.queued.take() {
            return Poll::Ready(Some(Ok(address)));
        }
        let walk = self.walk.get_or_insert_with(|| {
            TrieWalk::new(self.store.clone(), self.root, Vec::new(), None, self.window)
        });
        match walk.poll_visit(cx) {
            Poll::Ready(Some(Ok(visit))) => {
                if visit.value.is_some()
                    && let Some(entry) = visit.view.entry()
                {
                    self.queued = Some(*entry.address());
                }
                Poll::Ready(Some(Ok(visit.address)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Await the next address; `None` when the trie is exhausted.
    pub async fn next(&mut self) -> Option<Result<ChunkAddress, CursorError>> {
        core::future::poll_fn(|cx| self.poll_next_address(cx)).await
    }
}

impl<S, const BS: usize> Stream for AddressStream<S, BS>
where
    S: TrustedGet<AnyChunkSet<BS>> + Clone + Unpin + 'static,
{
    type Item = Result<ChunkAddress, CursorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_address(cx)
    }
}

impl<S, const BS: usize> core::fmt::Debug for AddressStream<S, BS> {
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
    use futures::executor::block_on;
    use nectar_primitives::chunk::{ChunkRef, ContentChunk};
    use nectar_primitives::store::{ChunkGet, ChunkPut, MemoryStore};
    use nectar_primitives::{EncryptedChunkRef, EncryptionKey, EntryRef, StandardChunkSet};

    use crate::node::{Fork, Node, Prefix};
    use crate::{EncryptedManifest, PlainManifest};

    type Store = MemoryStore<StandardChunkSet>;
    type Manifest = PlainManifest<Store>;

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

    /// Build a persisted plain manifest over the paths.
    fn build(paths: &[&str]) -> (ChunkAddress, Store) {
        let mut m = Manifest::new(Store::new());
        for &p in paths {
            block_on(m.add(p, make_addr(p))).unwrap();
        }
        let root = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        (root, store)
    }

    /// The legacy shared-read listing over the same persisted trie.
    fn legacy_entries(root: ChunkAddress, store: &Store) -> Vec<Entry> {
        let m = Manifest::open(root, store.clone());
        block_on(m.entries()).unwrap()
    }

    fn collect_entries<S>(mut cursor: Cursor<S>) -> Vec<Entry>
    where
        S: TrustedGet<AnyChunkSet<DEFAULT_BODY_SIZE>> + Clone + 'static,
    {
        block_on(async {
            let mut out = Vec::new();
            while let Some(item) = cursor.next().await {
                out.push(item.unwrap());
            }
            out
        })
    }

    fn collect_until_err<S>(mut cursor: Cursor<S>) -> (Vec<Entry>, Option<CursorError>)
    where
        S: TrustedGet<AnyChunkSet<DEFAULT_BODY_SIZE>> + Clone + 'static,
    {
        block_on(async {
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

    fn collect_addresses<S>(mut stream: AddressStream<S>) -> Vec<ChunkAddress>
    where
        S: TrustedGet<AnyChunkSet<DEFAULT_BODY_SIZE>> + Clone + 'static,
    {
        block_on(async {
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item.unwrap());
            }
            out
        })
    }

    /// Store wrapper recording fetches, concurrency peaks, and scripted
    /// faults; `Clone` shares one recording.
    #[derive(Clone)]
    struct RecordingStore {
        inner: std::sync::Arc<Recording>,
    }

    struct Recording {
        store: Store,
        fetched: Mutex<Vec<ChunkAddress>>,
        inflight: AtomicUsize,
        peak: AtomicUsize,
        delay: bool,
        fail: Option<ChunkAddress>,
        lie: Option<(ChunkAddress, ChunkAddress)>,
    }

    impl RecordingStore {
        fn with(store: Store, delay: bool, fail: Option<ChunkAddress>) -> Self {
            Self {
                inner: std::sync::Arc::new(Recording {
                    store,
                    fetched: Mutex::new(Vec::new()),
                    inflight: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                    delay,
                    fail,
                    lie: None,
                }),
            }
        }

        fn new(store: Store) -> Self {
            Self::with(store, false, None)
        }

        fn delayed(store: Store) -> Self {
            Self::with(store, true, None)
        }

        fn failing(store: Store, fail: ChunkAddress) -> Self {
            Self::with(store, false, Some(fail))
        }

        fn lying(store: Store, at: ChunkAddress, with: ChunkAddress) -> Self {
            let mut this = Self::with(store, false, None);
            std::sync::Arc::get_mut(&mut this.inner).unwrap().lie = Some((at, with));
            this
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

    impl ChunkGet<StandardChunkSet> for RecordingStore {
        type Trust = Verified;
        type Error = <Store as ChunkGet<StandardChunkSet>>::Error;

        async fn get(&self, address: &ChunkAddress) -> Result<Chunk, Self::Error> {
            self.inner.fetched.lock().unwrap().push(*address);
            let level = self.inner.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.inner.peak.fetch_max(level, Ordering::SeqCst);
            if self.inner.delay {
                yield_once().await;
            }
            let result = if self.inner.fail == Some(*address) {
                ChunkGet::get(&self.inner.store, &make_addr("absent-sentinel")).await
            } else if let Some((at, with)) = self.inner.lie
                && at == *address
            {
                ChunkGet::get(&self.inner.store, &with).await
            } else {
                ChunkGet::get(&self.inner.store, address).await
            };
            self.inner.inflight.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    /// Serial truth from a window-one walk: the fetch sequence and the
    /// cumulative fetch count at each yielded entry.
    fn serial_profile(root: ChunkAddress, store: &Store) -> (Vec<ChunkAddress>, Vec<usize>) {
        let rec = RecordingStore::new(store.clone());
        let mut cursor: Cursor<RecordingStore> =
            Cursor::new(rec.clone(), root).with_window(window(1));
        let mut counts = Vec::new();
        block_on(async {
            while let Some(item) = cursor.next().await {
                item.unwrap();
                counts.push(rec.fetch_count());
            }
        });
        (rec.fetched(), counts)
    }

    #[test]
    fn listing_matches_legacy_entries_in_path_order() {
        for paths in corpora() {
            let (root, store) = build(&paths);
            let want = legacy_entries(root, &store);
            assert_eq!(want.len(), paths.len());
            let got = collect_entries(Cursor::new(store, root));
            assert_eq!(got, want, "corpus {paths:?}");
            assert!(
                got.windows(2).all(|pair| pair[0].path() < pair[1].path()),
                "listing must be strictly path-ordered"
            );
        }
    }

    #[test]
    fn metadata_and_root_document_survive_the_listing() {
        let mut m = Manifest::new(Store::new());
        block_on(m.add("plain.txt", make_addr("plain"))).unwrap();
        let meta: BTreeMap<String, String> =
            [("Content-Type".to_string(), "image/png".to_string())].into();
        block_on(m.add_with_metadata("logo.png", make_addr("logo"), meta.clone())).unwrap();
        block_on(m.set_index_document("index.html")).unwrap();
        let root = block_on(m.save()).unwrap();
        let want = block_on(m.entries()).unwrap();
        let (_, store) = m.into_parts();

        let got = collect_entries(Cursor::new(store, root));
        assert_eq!(got, want);
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
    fn encrypted_listing_matches_legacy_entries() {
        let mut m = EncryptedManifest::new_encrypted(Store::new());
        let paths = ["secret/a.txt", "secret/b.txt", "top.txt"];
        for p in paths {
            let r = EncryptedChunkRef::new(make_addr(p), EncryptionKey::from([0x5a; 32]));
            block_on(m.add(p, r)).unwrap();
        }
        let manifest_ref = block_on(m.save()).unwrap();
        let (root, _key) = manifest_ref.into_parts();
        let want = block_on(m.entries()).unwrap();
        let (_, store) = m.into_parts();

        let got = collect_entries(Cursor::new(store, root));
        assert_eq!(got, want);
        assert!(
            got.iter()
                .all(|e| matches!(e.reference(), Some(EntryRef::Encrypted(_))))
        );
    }

    #[test]
    fn prefix_narrows_the_listing() {
        for paths in corpora() {
            let (root, store) = build(&paths);
            let full = collect_entries(Cursor::new(store.clone(), root));
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
                let got = collect_entries(Cursor::new(store.clone(), root).with_prefix(&probe));
                assert_eq!(got, want, "prefix {probe:?} over {paths:?}");
            }
        }
    }

    #[test]
    fn resume_after_continues_where_the_page_ended() {
        for paths in corpora() {
            let (root, store) = build(&paths);
            let full = collect_entries(Cursor::new(store.clone(), root));
            for k in 0..full.len() {
                let page = collect_entries(Cursor::new(store.clone(), root).with_limit(k));
                assert_eq!(page.as_slice(), &full[..k]);
                let mut resumed = Cursor::new(store.clone(), root);
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
            let (root, store) = build(&paths);
            let full = collect_entries(Cursor::new(store.clone(), root));
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
                let got = collect_entries(Cursor::new(store.clone(), root).after(&token));
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
        let (root, store) = build(&paths);
        let full = collect_entries(Cursor::new(store.clone(), root));
        let want: Vec<Entry> = full
            .iter()
            .filter(|e| e.path().starts_with(b"img/") && e.path() > b"img/1.png".as_slice())
            .cloned()
            .collect();
        let got = collect_entries(
            Cursor::new(store, root)
                .with_prefix("img/")
                .after("img/1.png"),
        );
        assert_eq!(got, want);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn zero_limit_lists_nothing_and_fetches_nothing() {
        let (root, store) = build(&["a", "b"]);
        let rec = RecordingStore::new(store);
        let got = collect_entries(Cursor::new(rec.clone(), root).with_limit(0));
        assert!(got.is_empty());
        assert_eq!(rec.fetch_count(), 0);
    }

    #[test]
    fn fetched_set_stays_within_the_serial_set_plus_window() {
        for paths in corpora() {
            let (root, store) = build(&paths);
            let (serial_seq, counts) = serial_profile(root, &store);
            let serial_set: std::collections::BTreeSet<ChunkAddress> =
                serial_seq.iter().copied().collect();
            for w in [1u16, 2, 4, 16] {
                for k in 1..=counts.len() {
                    let rec = RecordingStore::new(store.clone());
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
                let rec = RecordingStore::new(store.clone());
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
        let (root, store) = build(&refs);
        for w in [1u16, 4, 8] {
            let rec = RecordingStore::delayed(store.clone());
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
        let (root, store) = build(&paths);
        let (serial_seq, _) = serial_profile(root, &store);
        for victim_pos in [0, serial_seq.len() / 2, serial_seq.len() - 1] {
            let victim = serial_seq[victim_pos];
            let (want_entries, want_err) = collect_until_err(
                Cursor::new(RecordingStore::failing(store.clone(), victim), root)
                    .with_window(window(1)),
            );
            assert!(
                matches!(want_err, Some(CursorError::Store { address, .. }) if address == victim)
            );
            for w in [2u16, 16] {
                let (entries, err) = collect_until_err(
                    Cursor::new(RecordingStore::failing(store.clone(), victim), root)
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
                Cursor::new(RecordingStore::failing(store.clone(), victim), root)
                    .with_window(window(16))
                    .with_limit(want_entries.len()),
            );
            assert_eq!(entries, want_entries);
            assert!(err.is_none(), "victim {victim_pos}: parked error surfaced");
        }
    }

    #[test]
    fn undecodable_child_is_a_corrupt_error() {
        let store = Store::new();
        let garbage =
            ContentChunk::<DEFAULT_BODY_SIZE>::new(Bytes::from_static(b"not a mantaray node"))
                .unwrap();
        let gaddr = *garbage.address();
        let sealed: Chunk = Chunk::from_envelope(garbage.into()).unwrap();
        block_on(store.put(sealed)).unwrap();

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
        block_on(store.put(sealed)).unwrap();

        let (entries, err) = collect_until_err(Cursor::new(store, root));
        assert!(entries.is_empty());
        assert!(matches!(err, Some(CursorError::Corrupt { address, .. }) if address == gaddr));
    }

    #[test]
    fn lying_store_is_an_address_mismatch() {
        let (root, store) = build(&["a", "b"]);
        let (serial_seq, _) = serial_profile(root, &store);
        let other = *serial_seq.last().unwrap();
        assert_ne!(other, root);
        let rec = RecordingStore::lying(store, root, other);
        let (entries, err) = collect_until_err(Cursor::new(rec, root));
        assert!(entries.is_empty());
        assert!(matches!(
            err,
            Some(CursorError::AddressMismatch { requested, returned })
                if requested == root && returned == other
        ));
    }

    #[test]
    fn address_stream_matches_the_legacy_address_walk() {
        for paths in corpora() {
            let (root, store) = build(&paths);
            let mut m = Manifest::open(root, store.clone());
            let mut want: Vec<Vec<u8>> = Vec::new();
            block_on(m.iterate_addresses(|bytes| {
                want.push(bytes.to_vec());
                Ok(())
            }))
            .unwrap();
            let ordered = collect_addresses(AddressStream::new(store.clone(), root));
            let windowed =
                collect_addresses(AddressStream::new(store.clone(), root).with_window(window(8)));
            assert_eq!(
                ordered, windowed,
                "delivery order must not depend on the window"
            );
            let mut got: Vec<Vec<u8>> = ordered.iter().map(|a| a.as_bytes().to_vec()).collect();
            got.sort();
            want.sort();
            assert_eq!(got, want, "corpus {paths:?}");
        }
    }

    #[test]
    fn encrypted_address_stream_covers_nodes_and_entries() {
        let mut m = EncryptedManifest::new_encrypted(Store::new());
        let paths = ["secret/a.txt", "secret/b.txt", "top.txt"];
        for p in paths {
            let r = EncryptedChunkRef::new(make_addr(p), EncryptionKey::from([0x5a; 32]));
            block_on(m.add(p, r)).unwrap();
        }
        let manifest_ref = block_on(m.save()).unwrap();
        let (root, _key) = manifest_ref.into_parts();
        let mut want: Vec<Vec<u8>> = Vec::new();
        block_on(m.iterate_addresses(|bytes| {
            // Node references arrive as 32 addresses, value entries at the
            // full encrypted width; the stream carries addresses only.
            want.push(bytes[..32].to_vec());
            Ok(())
        }))
        .unwrap();
        let (_, store) = m.into_parts();

        let mut got: Vec<Vec<u8>> = collect_addresses(AddressStream::new(store, root))
            .iter()
            .map(|a| a.as_bytes().to_vec())
            .collect();
        got.sort();
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn empty_trie_lists_nothing_and_streams_only_the_root() {
        let mut m = Manifest::new(Store::new());
        let root = block_on(m.save()).unwrap();
        let (_, store) = m.into_parts();
        assert!(collect_entries(Cursor::new(store.clone(), root)).is_empty());
        assert_eq!(
            collect_addresses(AddressStream::new(store, root)),
            vec![root]
        );
    }

    #[test]
    fn missing_root_is_a_store_error() {
        let root = make_addr("nowhere");
        let (entries, err) = collect_until_err(Cursor::new(Store::new(), root));
        assert!(entries.is_empty());
        assert!(matches!(err, Some(CursorError::Store { address, .. }) if address == root));
    }

    #[test]
    fn cursor_and_address_stream_drive_as_streams() {
        use futures::StreamExt;
        let (root, store) = build(&["a", "b", "c"]);
        let entries: Vec<_> = block_on(Cursor::new(store.clone(), root).collect::<Vec<_>>());
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(Result::is_ok));
        let addresses: Vec<_> = block_on(AddressStream::new(store, root).collect::<Vec<_>>());
        assert!(addresses.len() > 3);
        assert!(addresses.iter().all(Result::is_ok));
    }
}
