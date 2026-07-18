//! Ordered read operations over the trie: full iteration, bounded scans, and
//! floor lookup, all as O(depth) descent on top of the streaming reader.
//!
//! Iteration walks the fork tables along the frontier only: a value rides in
//! its fork record, so a key and its value surface without fetching the chunk
//! a reference points at. The only fetches are the trie nodes on the current
//! path, so peak retained state is O(depth) and the value chunks are never
//! pulled.
//!
//! The ordered cursor prefetches the covering frontier with bounded
//! concurrency: it keeps up to [`Format::READ_AHEAD`] node fetches in flight in
//! ascending-key order, so a scan pays O(depth) parallel rounds rather than one
//! serial round trip per node. Chunks are immutable and content-addressed, so
//! concurrent fetch needs no locking; the sliding window never materializes the
//! whole frontier, so peak retained state stays O(depth) at the same fetch
//! count a serial walk pays.

use core::cmp::Ordering;
use core::future::Future;
use core::pin::Pin;

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use nectar_primitives::ChunkAddress;
use nectar_primitives::store::MaybeSync;

use crate::fork::{Child, ForkTable};
use crate::format::{Format, V1};
use crate::node::Node;
use crate::reader::{Reader, ReaderError};
use crate::store::NodeGet;
use crate::value::{Entry, Key};

/// One resolved position in a chunk's ordered contents.
///
/// The suffix is the key bytes below the chunk's root, so a step's key is the
/// chunk base followed by the suffix. A referenced child is a descent point,
/// never a value: iteration fetches it only to keep walking, not to read it.
#[derive(Clone, Debug)]
enum Step<F: Format> {
    /// A key terminates here with this value.
    Value {
        /// Key bytes below the chunk root.
        suffix: Bytes,
        /// The bound value.
        entry: Entry<F>,
    },
    /// The trie continues into a referenced child chunk.
    Ref {
        /// Key bytes below the chunk root leading to the child.
        suffix: Bytes,
        /// The child chunk address.
        addr: ChunkAddress,
    },
    /// The trie continues into an encrypted child the plain reader cannot open.
    Encrypted {
        /// Key bytes below the chunk root leading to the child.
        suffix: Bytes,
    },
}

impl<F: Format> Step<F> {
    /// The key bytes below the chunk root.
    fn suffix(&self) -> &[u8] {
        match self {
            Self::Value { suffix, .. } | Self::Ref { suffix, .. } | Self::Encrypted { suffix } => {
                suffix
            }
        }
    }
}

/// One chunk's ordered contents plus the key prefix that reaches its root.
#[derive(Clone, Debug)]
struct Frame<F: Format> {
    /// Key bytes consumed to reach this chunk's root.
    base: Bytes,
    /// The chunk's steps in ascending key order.
    steps: Vec<Step<F>>,
    /// The next step to visit.
    index: usize,
    /// Per-step prefetch tag, parallel to `steps`: the sequence id a referenced
    /// child was launched under, once the read-ahead window scheduled it.
    sched: Vec<Option<usize>>,
}

impl<F: Format> Frame<F> {
    /// A frame over `steps`, resuming at `index`, with an empty prefetch tag.
    fn new(base: Bytes, steps: Vec<Step<F>>, index: usize) -> Self {
        let sched = vec![None; steps.len()];
        Self {
            base,
            steps,
            index,
            sched,
        }
    }
}

/// A launched node fetch tagged with the sequence id it was scheduled under, so
/// out-of-order completions route back to the descent that awaits them.
type Fetched<F> = (usize, Result<Vec<Step<F>>, ReaderError>);

/// An in-flight node fetch. Boxed to hold heterogeneous fetch futures in one
/// queue; `Send` on native, unbounded on the single-threaded wasm executor.
#[cfg(not(target_arch = "wasm32"))]
type Fetch<'a, F> = Pin<Box<dyn Future<Output = Fetched<F>> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
type Fetch<'a, F> = Pin<Box<dyn Future<Output = Fetched<F>> + 'a>>;

/// An ordered cursor over a manifest, yielding `(key, value)` in key order.
///
/// The cursor fetches trie nodes on demand and retains one frame per referenced
/// hop on the current path, so a full walk peaks at O(depth) whatever the key
/// count. An exclusive upper bound stops the walk without fetching subtrees
/// that lie past it.
///
/// Referenced children ahead of the current position are prefetched with a
/// sliding window of at most [`Format::READ_AHEAD`] fetches in flight, launched
/// in ascending-key order and never past the upper bound, so the concurrent
/// walk fetches exactly the nodes a serial walk would and returns them in the
/// same order.
#[derive(Debug)]
pub struct Cursor<'a, S, F: Format = V1> {
    store: &'a S,
    stack: Vec<Frame<F>>,
    end: Option<Bytes>,
    done: bool,
    /// Node fetches launched by the read-ahead window, awaiting completion.
    inflight: FuturesUnordered<Fetch<'a, F>>,
    /// Completions that arrived before the descent awaiting them; drained by
    /// sequence id. Bounded with `inflight` by the window, so O(depth) overall.
    ready: Vec<Fetched<F>>,
    /// The next fetch sequence id to hand out.
    next_seq: usize,
    /// Remaining yields a paginated cursor may return; `None` is unbounded.
    remaining: Option<usize>,
}

/// What visiting the top frame's next step resolves to, computed under a short
/// borrow so the awaited fetch and the stack push never overlap it.
enum Advance<F: Format> {
    /// The frame is spent; drop it and resume its parent.
    Pop,
    /// A key and its value at this position.
    Yield(Vec<u8>, Entry<F>),
    /// Descend into the referenced child rooted at this key prefix, awaiting the
    /// prefetch launched under this sequence id when one was scheduled.
    Descend(Vec<u8>, ChunkAddress, Option<usize>),
    /// An encrypted child blocks the walk at this key prefix.
    Encrypted(Vec<u8>),
}

impl<'a, S, F> Cursor<'a, S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    /// Position a cursor at the least key `>= start`, streaming forward until
    /// `end` (exclusive), descending only the referenced hops on the seek path.
    pub(crate) async fn seek(
        store: &'a S,
        root: &ChunkAddress,
        start: &[u8],
        end: Option<Bytes>,
    ) -> Result<Self, ReaderError> {
        let mut stack: Vec<Frame<F>> = Vec::new();
        let mut base: Vec<u8> = Vec::new();
        let mut addr = *root;
        let mut is_root = true;
        loop {
            let node = store.get_node::<F>(&addr).await?;
            let steps = flatten(&node, is_root);
            let remaining = start.get(base.len()..).unwrap_or(&[]);
            if remaining.is_empty() {
                stack.push(Frame::new(Bytes::from(base), steps, 0));
                break;
            }
            let mut chosen = steps.len();
            let mut deeper: Option<(usize, ChunkAddress, Bytes)> = None;
            for (i, step) in steps.iter().enumerate() {
                let v = step.suffix();
                if v >= remaining {
                    chosen = i;
                    break;
                }
                // `v < remaining`: the seek key descends only into a referenced
                // child whose whole edge is a prefix of what remains.
                match step {
                    Step::Ref {
                        suffix,
                        addr: child,
                    } if remaining.starts_with(v) => {
                        deeper = Some((i, *child, suffix.clone()));
                        break;
                    }
                    Step::Encrypted { .. } if remaining.starts_with(v) => {
                        return Err(ReaderError::EncryptedChild);
                    }
                    _ => {}
                }
            }
            match deeper {
                Some((i, child, suffix)) => {
                    stack.push(Frame::new(
                        Bytes::from(base.clone()),
                        steps,
                        i.saturating_add(1),
                    ));
                    base.extend_from_slice(&suffix);
                    addr = child;
                    is_root = false;
                }
                None => {
                    stack.push(Frame::new(Bytes::from(base), steps, chosen));
                    break;
                }
            }
        }
        Ok(Self {
            store,
            stack,
            end,
            done: false,
            inflight: FuturesUnordered::new(),
            ready: Vec::new(),
            next_seq: 0,
            remaining: None,
        })
    }

    /// An already-exhausted cursor: yields nothing. Used when a paginated seek
    /// starts past the last key.
    pub(crate) fn exhausted(store: &'a S) -> Self {
        Self {
            store,
            stack: Vec::new(),
            end: None,
            done: true,
            inflight: FuturesUnordered::new(),
            ready: Vec::new(),
            next_seq: 0,
            remaining: None,
        }
    }

    /// Cap this cursor at `limit` yields, for a paginated page of a listing.
    #[must_use]
    pub(crate) const fn with_limit(mut self, limit: usize) -> Self {
        self.remaining = Some(limit);
        self
    }

    /// The next `(key, value)` in key order, or `None` at the end of the walk.
    ///
    /// Fetches the trie nodes on the frontier only; the value chunk a reference
    /// names is never pulled, so listing a manifest costs node fetches, not one
    /// fetch per key.
    pub async fn next(&mut self) -> Result<Option<(Key, Entry<F>)>, ReaderError> {
        if self.done {
            return Ok(None);
        }
        if self.remaining == Some(0) {
            self.done = true;
            return Ok(None);
        }
        loop {
            self.schedule();
            let advance = match self.stack.last_mut() {
                None => {
                    self.done = true;
                    return Ok(None);
                }
                Some(frame) => {
                    let index = frame.index;
                    match frame.steps.get(index) {
                        None => Advance::Pop,
                        Some(step) => {
                            frame.index = index.saturating_add(1);
                            match step {
                                Step::Value { suffix, entry } => {
                                    Advance::Yield(join(&frame.base, suffix), entry.clone())
                                }
                                Step::Ref { suffix, addr } => {
                                    let seq = frame.sched.get(index).copied().flatten();
                                    Advance::Descend(join(&frame.base, suffix), *addr, seq)
                                }
                                Step::Encrypted { suffix } => {
                                    Advance::Encrypted(join(&frame.base, suffix))
                                }
                            }
                        }
                    }
                }
            };
            match advance {
                Advance::Pop => {
                    self.stack.pop();
                }
                Advance::Yield(key, entry) => {
                    if self.past_end(&key) {
                        self.done = true;
                        return Ok(None);
                    }
                    if let Some(left) = self.remaining {
                        self.remaining = Some(left.saturating_sub(1));
                    }
                    return Ok(Some((Key::new(Bytes::from(key)), entry)));
                }
                Advance::Descend(child_base, addr, seq) => {
                    if self.past_end(&child_base) {
                        self.done = true;
                        return Ok(None);
                    }
                    let steps = self.resolve(seq, &addr).await?;
                    self.stack
                        .push(Frame::new(Bytes::from(child_base), steps, 0));
                }
                Advance::Encrypted(child_base) => {
                    if self.past_end(&child_base) {
                        self.done = true;
                        return Ok(None);
                    }
                    return Err(ReaderError::EncryptedChild);
                }
            }
        }
    }

    /// Fill the read-ahead window: launch node fetches for the referenced
    /// children the walk will reach next, in ascending-key order, until at most
    /// [`Format::READ_AHEAD`] fetches are in flight.
    ///
    /// Scheduling mirrors the walk's own termination, so it launches exactly the
    /// nodes the walk fetches: it stops at the first step at or past the upper
    /// bound and at the first encrypted child, and never relaunches a child
    /// already tagged with a sequence id.
    fn schedule(&mut self) {
        let cap = F::READ_AHEAD;
        let store = self.store;
        // Deepest frame first: that is ascending-key order from the cursor, so
        // the child needed soonest is always launched first and never starved.
        'outer: for frame in self.stack.iter_mut().rev() {
            let mut index = frame.index;
            while let Some(step) = frame.steps.get(index) {
                if self.inflight.len().saturating_add(self.ready.len()) >= cap {
                    break 'outer;
                }
                let key = join(&frame.base, step.suffix());
                if self
                    .end
                    .as_ref()
                    .is_some_and(|end| key.as_slice() >= end.as_ref())
                {
                    // The walk stops at this bound; nothing beyond it is fetched.
                    break 'outer;
                }
                match step {
                    // The walk errors here; no deeper node is fetched.
                    Step::Encrypted { .. } => break 'outer,
                    Step::Ref { addr, .. } if !matches!(frame.sched.get(index), Some(Some(_))) => {
                        let seq = self.next_seq;
                        self.next_seq = self.next_seq.saturating_add(1);
                        if let Some(slot) = frame.sched.get_mut(index) {
                            *slot = Some(seq);
                        }
                        let addr = *addr;
                        let fetch: Fetch<'a, F> = Box::pin(async move {
                            let result = store
                                .get_node::<F>(&addr)
                                .await
                                .map(|node| flatten(&node, false))
                                .map_err(ReaderError::from);
                            (seq, result)
                        });
                        self.inflight.push(fetch);
                    }
                    Step::Ref { .. } | Step::Value { .. } => {}
                }
                index = index.saturating_add(1);
            }
        }
    }

    /// The steps of the child reached by a descent: take the prefetch launched
    /// under `seq`, driving in-flight fetches until it completes and buffering
    /// any earlier-arriving completions. Falls back to a direct fetch when the
    /// descent was not prefetched.
    async fn resolve(
        &mut self,
        seq: Option<usize>,
        addr: &ChunkAddress,
    ) -> Result<Vec<Step<F>>, ReaderError> {
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
        let node = self.store.get_node::<F>(addr).await?;
        Ok(flatten(&node, false))
    }

    /// Whether `key` has reached the exclusive upper bound. A referenced child
    /// whose least key is already at the bound holds nothing in range, so the
    /// same test prunes the descent.
    fn past_end(&self, key: &[u8]) -> bool {
        self.end.as_ref().is_some_and(|end| key >= end.as_ref())
    }
}

impl<S, F> Reader<S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    /// Every `(key, value)` in ascending key order.
    pub async fn iter(&self, root: &ChunkAddress) -> Result<Cursor<'_, S, F>, ReaderError> {
        Cursor::seek(self.store(), root, &[], None).await
    }

    /// Every `(key, value)` with `lo <= key < hi`, in ascending key order.
    pub async fn range(
        &self,
        root: &ChunkAddress,
        lo: &Key,
        hi: &Key,
    ) -> Result<Cursor<'_, S, F>, ReaderError> {
        let end = Bytes::copy_from_slice(hi.as_bytes());
        Cursor::seek(self.store(), root, lo.as_bytes(), Some(end)).await
    }

    /// Every `(key, value)` whose key starts with `prefix`, in ascending order.
    ///
    /// The prefix range is `[prefix, successor(prefix))`; an all-`0xFF` or empty
    /// prefix has no successor and the scan runs unbounded to the last key.
    pub async fn prefix(
        &self,
        root: &ChunkAddress,
        prefix: &Key,
    ) -> Result<Cursor<'_, S, F>, ReaderError> {
        let end = successor(prefix.as_bytes());
        Cursor::seek(self.store(), root, prefix.as_bytes(), end).await
    }

    /// The greatest key `<= key` and its value, or `None` when every key is
    /// larger.
    ///
    /// Follows the target down the trie and, where the path dead-ends, takes the
    /// rightmost key of the largest branch left of it, so the cost stays
    /// O(depth) rather than a scan of the level.
    pub async fn floor(
        &self,
        root: &ChunkAddress,
        key: &Key,
    ) -> Result<Option<(Key, Entry<F>)>, ReaderError> {
        let store = self.store();
        let target = key.as_bytes();
        let mut base: Vec<u8> = Vec::new();
        let mut addr = *root;
        let mut is_root = true;
        // The greatest branch strictly left of the target found at a shallower
        // level; a deeper left branch always outranks it, so one slot suffices.
        let mut fallback: Option<(Bytes, Step<F>)> = None;
        loop {
            let node = store.get_node::<F>(&addr).await?;
            let steps = flatten(&node, is_root);
            let remaining = target.get(base.len()..).unwrap_or(&[]);
            let mut left: Option<Step<F>> = None;
            let mut descend: Option<(ChunkAddress, Bytes)> = None;
            let mut exact: Option<Entry<F>> = None;
            for step in &steps {
                match step.suffix().cmp(remaining) {
                    Ordering::Equal => {
                        if let Step::Value { entry, .. } = step {
                            exact = Some(entry.clone());
                        }
                        break;
                    }
                    Ordering::Greater => break,
                    Ordering::Less => match step {
                        Step::Value { .. } => left = Some(step.clone()),
                        Step::Ref {
                            suffix,
                            addr: child,
                        } => {
                            if remaining.starts_with(step.suffix()) {
                                descend = Some((*child, suffix.clone()));
                                break;
                            }
                            left = Some(step.clone());
                        }
                        Step::Encrypted { .. } => {
                            if remaining.starts_with(step.suffix()) {
                                return Err(ReaderError::EncryptedChild);
                            }
                            left = Some(step.clone());
                        }
                    },
                }
            }
            if let Some(entry) = exact {
                return Ok(Some((Key::new(Bytes::copy_from_slice(target)), entry)));
            }
            if let Some((child, suffix)) = descend {
                if let Some(step) = left {
                    fallback = Some((Bytes::from(base.clone()), step));
                }
                base.extend_from_slice(&suffix);
                addr = child;
                is_root = false;
                continue;
            }
            let candidate = left.map_or(fallback, |step| Some((Bytes::from(base), step)));
            return match candidate {
                Some((base, step)) => max_key(store, base, step).await,
                None => Ok(None),
            };
        }
    }
}

/// The greatest key at or below a resolved step: a value is itself, a
/// referenced child is its rightmost key, an encrypted child cannot be opened.
async fn max_key<S, F>(
    store: &S,
    base: Bytes,
    step: Step<F>,
) -> Result<Option<(Key, Entry<F>)>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    let mut path = base.to_vec();
    match step {
        Step::Value { suffix, entry } => {
            path.extend_from_slice(&suffix);
            Ok(Some((Key::new(Bytes::from(path)), entry)))
        }
        Step::Encrypted { .. } => Err(ReaderError::EncryptedChild),
        Step::Ref { suffix, addr } => {
            path.extend_from_slice(&suffix);
            rightmost(store, path, addr).await
        }
    }
}

/// The rightmost key of the subtree rooted at `addr`: the greatest step of each
/// chunk on the descent is the last one, so one hop per level reaches it.
async fn rightmost<S, F>(
    store: &S,
    mut path: Vec<u8>,
    mut addr: ChunkAddress,
) -> Result<Option<(Key, Entry<F>)>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    loop {
        let node = store.get_node::<F>(&addr).await?;
        let steps = flatten(&node, false);
        match steps.last() {
            None => return Ok(None),
            Some(Step::Value { suffix, entry }) => {
                path.extend_from_slice(suffix);
                return Ok(Some((Key::new(Bytes::from(path)), entry.clone())));
            }
            Some(Step::Ref {
                suffix,
                addr: child,
            }) => {
                path.extend_from_slice(suffix);
                addr = *child;
            }
            Some(Step::Encrypted { .. }) => return Err(ReaderError::EncryptedChild),
        }
    }
}

/// A chunk's contents flattened into ascending-key steps. The root chunk's own
/// value is the empty key, the least of all, so it leads the list.
fn flatten<F: Format>(node: &Node<F>, is_root: bool) -> Vec<Step<F>> {
    let mut steps = Vec::new();
    if is_root && let Some(entry) = node.entry() {
        steps.push(Step::Value {
            suffix: Bytes::new(),
            entry: entry.clone(),
        });
    }
    let mut prefix = Vec::new();
    flatten_table(node.forks(), &mut prefix, &mut steps);
    steps
}

/// Walk a fork table in wire order, appending each terminal value and referenced
/// child as a step. Embedded children stay in the chunk and recurse in place, so
/// a whole chunk flattens without a fetch; the value of a fork precedes its
/// child, matching key order.
fn flatten_table<F: Format>(table: &ForkTable<F>, prefix: &mut Vec<u8>, steps: &mut Vec<Step<F>>) {
    for (first, record) in table.iter() {
        let mark = prefix.len();
        prefix.push(first);
        prefix.extend_from_slice(record.tail().as_bytes());
        if let Some(entry) = record.entry() {
            steps.push(Step::Value {
                suffix: Bytes::copy_from_slice(prefix.as_slice()),
                entry: entry.clone(),
            });
        }
        match record.child() {
            Some(Child::Embedded(inner)) => flatten_table(inner, prefix, steps),
            Some(Child::Ref32(reference)) => steps.push(Step::Ref {
                suffix: Bytes::copy_from_slice(prefix.as_slice()),
                addr: *reference.address(),
            }),
            Some(Child::Ref64(_)) => steps.push(Step::Encrypted {
                suffix: Bytes::copy_from_slice(prefix.as_slice()),
            }),
            None => {}
        }
        prefix.truncate(mark);
    }
}

/// Base bytes followed by suffix bytes.
fn join(base: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(base.len().saturating_add(suffix.len()));
    out.extend_from_slice(base);
    out.extend_from_slice(suffix);
    out
}

/// The least byte string strictly greater than every string starting with
/// `prefix`: increment the last byte below `0xFF` after dropping the trailing
/// `0xFF` run. `None` when the prefix is empty or all `0xFF`, i.e. unbounded.
pub(crate) fn successor(prefix: &[u8]) -> Option<Bytes> {
    let mut bytes = prefix.to_vec();
    while let Some(&last) = bytes.last() {
        if last == 0xFF {
            bytes.pop();
        } else {
            let tail = bytes.len().saturating_sub(1);
            if let Some(slot) = bytes.get_mut(tail) {
                *slot = last.saturating_add(1);
            }
            return Some(Bytes::from(bytes));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef, EncryptionKey};
    use nectar_testing::run;

    use crate::bounded::Prefix;
    use crate::fork::{Child, ForkTable};
    use crate::node::Node;
    use crate::store::NodePut;
    use crate::value::{Entry, Key};

    use super::*;

    fn entry(byte: u8) -> Entry {
        ChunkRef::new(ChunkAddress::new([byte; 32])).into()
    }

    fn prefix(bytes: &[u8]) -> Prefix {
        Prefix::try_from(bytes).unwrap()
    }

    async fn drain(mut cursor: Cursor<'_, &MemoryStore>) -> Vec<(Vec<u8>, Entry)> {
        let mut out = Vec::new();
        while let Some((key, value)) = cursor.next().await.unwrap() {
            out.push((key.as_bytes().to_vec(), value));
        }
        out
    }

    // A two-level manifest: a root fork "a" behind an embedded child holding
    // "aa"/"ab", and "b" behind a referenced leaf holding "ba".
    async fn sample(store: &MemoryStore) -> ChunkAddress {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"a"), entry(0xBA).into(), None).unwrap();
        let leaf_ref = store.put_node(&Node::new(None, leaf)).await.unwrap();

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
                Child::Ref32(ChunkRef::new(leaf_ref)).into(),
                None,
            )
            .unwrap();
        store.put_node(&Node::new(None, forks)).await.unwrap()
    }

    #[test]
    fn iteration_is_ascending_across_embedded_and_referenced_children() {
        run(async {
            let store = MemoryStore::default();
            let root = sample(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            let got = drain(reader.iter(&root).await.unwrap()).await;
            assert_eq!(
                got,
                vec![
                    (b"aa".to_vec(), entry(0xAA)),
                    (b"ab".to_vec(), entry(0xAB)),
                    (b"ba".to_vec(), entry(0xBA)),
                ]
            );
        })
    }

    #[test]
    fn the_root_value_is_the_empty_key_and_leads_iteration() {
        run(async {
            let store = MemoryStore::default();
            let root_ext = crate::node::RootExtension::new(Some(entry(9)), None);
            let mut forks = ForkTable::new();
            forks.insert(prefix(b"k"), entry(1).into(), None).unwrap();
            let root = store.put_node(&Node::new(root_ext, forks)).await.unwrap();
            let reader: Reader<_> = Reader::new(&store);
            let got = drain(reader.iter(&root).await.unwrap()).await;
            assert_eq!(got, vec![(Vec::new(), entry(9)), (b"k".to_vec(), entry(1))]);
        })
    }

    #[test]
    fn range_is_half_open() {
        run(async {
            let store = MemoryStore::default();
            let root = sample(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            let got = drain(
                reader
                    .range(&root, &Key::from(&b"aa"[..]), &Key::from(&b"ba"[..]))
                    .await
                    .unwrap(),
            )
            .await;
            // "aa" is included, "ba" is the excluded upper bound.
            assert_eq!(
                got,
                vec![(b"aa".to_vec(), entry(0xAA)), (b"ab".to_vec(), entry(0xAB))]
            );
        })
    }

    #[test]
    fn range_starting_between_keys_seeks_to_the_ceiling() {
        run(async {
            let store = MemoryStore::default();
            let root = sample(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            let got = drain(
                reader
                    .range(&root, &Key::from(&b"ac"[..]), &Key::from(&b"z"[..]))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(got, vec![(b"ba".to_vec(), entry(0xBA))]);
        })
    }

    #[test]
    fn prefix_selects_one_subtree() {
        run(async {
            let store = MemoryStore::default();
            let root = sample(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            let got = drain(reader.prefix(&root, &Key::from(&b"a"[..])).await.unwrap()).await;
            assert_eq!(
                got,
                vec![(b"aa".to_vec(), entry(0xAA)), (b"ab".to_vec(), entry(0xAB))]
            );
        })
    }

    #[test]
    fn floor_resolves_present_absent_and_below_all_keys() {
        run(async {
            let store = MemoryStore::default();
            let root = sample(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            // Exact hit.
            assert_eq!(
                reader.floor(&root, &Key::from(&b"ab"[..])).await.unwrap(),
                Some((Key::from(&b"ab"[..]), entry(0xAB)))
            );
            // Between "ab" and "ba": floor is "ab".
            assert_eq!(
                reader.floor(&root, &Key::from(&b"az"[..])).await.unwrap(),
                Some((Key::from(&b"ab"[..]), entry(0xAB)))
            );
            // Past the last key: floor is the greatest key, reached through the ref.
            assert_eq!(
                reader.floor(&root, &Key::from(&b"zz"[..])).await.unwrap(),
                Some((Key::from(&b"ba"[..]), entry(0xBA)))
            );
            // Below every key: nothing.
            assert_eq!(
                reader.floor(&root, &Key::from(&b"a"[..])).await.unwrap(),
                None
            );
        })
    }

    // An encrypted (ref64) child the plain reader cannot open.
    fn encrypted(byte: u8) -> Child {
        Child::Ref64(EncryptedChunkRef::new(
            ChunkAddress::new([byte; 32]),
            EncryptionKey::from([byte ^ 0xFF; 32]),
        ))
    }

    // A root holding "a" and "z" as plain values with an encrypted subtree
    // wedged between them under "m".
    async fn with_encrypted(store: &MemoryStore) -> ChunkAddress {
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), entry(0xA1).into(), None)
            .unwrap();
        forks
            .insert(prefix(b"m"), encrypted(0x4D).into(), None)
            .unwrap();
        forks
            .insert(prefix(b"z"), entry(0x2C).into(), None)
            .unwrap();
        store.put_node(&Node::new(None, forks)).await.unwrap()
    }

    #[test]
    fn iteration_surfaces_an_encrypted_subtree_as_an_error() {
        run(async {
            let store = MemoryStore::default();
            let root = with_encrypted(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            let mut cursor = reader.iter(&root).await.unwrap();
            // The plain value before the encrypted edge reads back.
            assert_eq!(
                cursor.next().await.unwrap(),
                Some((Key::from(&b"a"[..]), entry(0xA1)))
            );
            // Reaching the encrypted child stops the walk with an error.
            assert!(matches!(
                cursor.next().await.unwrap_err(),
                ReaderError::EncryptedChild
            ));
        })
    }

    #[test]
    fn a_bound_short_of_the_encrypted_edge_prunes_it() {
        run(async {
            let store = MemoryStore::default();
            let root = with_encrypted(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            // "m" is the exclusive upper bound, so the encrypted child at "m" is
            // pruned rather than fetched, and the scan completes without error.
            let got = drain(
                reader
                    .range(&root, &Key::from(&b"a"[..]), &Key::from(&b"m"[..]))
                    .await
                    .unwrap(),
            )
            .await;
            assert_eq!(got, vec![(b"a".to_vec(), entry(0xA1))]);
        })
    }

    #[test]
    fn floor_past_an_encrypted_edge_reads_the_plain_key() {
        run(async {
            let store = MemoryStore::default();
            let root = with_encrypted(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            // The floor of "z" is "z" itself; the encrypted subtree is left of the
            // path and never opened.
            assert_eq!(
                reader.floor(&root, &Key::from(&b"z"[..])).await.unwrap(),
                Some((Key::from(&b"z"[..]), entry(0x2C)))
            );
        })
    }

    #[test]
    fn floor_landing_in_an_encrypted_subtree_cannot_be_read() {
        run(async {
            let store = MemoryStore::default();
            let root = with_encrypted(&store).await;
            let reader: Reader<_> = Reader::new(&store);
            // Every key at or below "n" that could be the floor lives in the
            // encrypted subtree under "m", so the answer is unreadable.
            assert!(matches!(
                reader
                    .floor(&root, &Key::from(&b"n"[..]))
                    .await
                    .unwrap_err(),
                ReaderError::EncryptedChild
            ));
        })
    }

    #[test]
    fn successor_bounds_the_prefix_range() {
        assert_eq!(successor(b"ab").as_deref(), Some(&b"ac"[..]));
        assert_eq!(successor(b"a\xff").as_deref(), Some(&b"b"[..]));
        assert_eq!(successor(b"\xff\xff"), None);
        assert_eq!(successor(b""), None);
    }
}
