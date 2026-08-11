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

use alloc::vec::Vec;
use core::cmp::Ordering;
use core::future::poll_fn;
use core::ops::{Bound, RangeBounds};
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, Stream};
use nectar_governor::BoxFuture;
use nectar_primitives::ChunkRef;
use nectar_primitives::store::MaybeSync;

use crate::fork::{Child, ForkTable};
use crate::format::{Format, V1};
use crate::frontier::{Completion, Frame, Plan, claim, fill};
use crate::node::{Node, NodeRef};
use crate::reader::{Reader, ReaderError};
use crate::store::NodeGet;
use crate::value::{Entry, Key};

/// One resolved position in a chunk's ordered contents.
///
/// The suffix is the key bytes below the chunk's root, so a step's key is the
/// chunk base followed by the suffix. A referenced child is a descent point,
/// never a value: iteration fetches it only to keep walking, not to read it.
#[derive(Clone, Debug)]
pub(crate) enum Step<F: Format, R: NodeRef> {
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
        /// The reference reaching the child chunk; an encrypted one carries
        /// the key that opens it.
        reference: R,
    },
}

impl<F: Format, R: NodeRef> Step<F, R> {
    /// The key bytes below the chunk root.
    fn suffix(&self) -> &[u8] {
        match self {
            Self::Value { suffix, .. } | Self::Ref { suffix, .. } => suffix,
        }
    }
}

/// One completed prefetch payload: the fetched child's flattened steps.
type Fetched<F, R> = Completion<Vec<Step<F, R>>>;

/// One delivered turn of the walk: a key-value pair, the end of the walk, or
/// a non-terminal fault.
type Turn<F> = Result<Option<(Key, Entry<F>)>, ReaderError>;

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
///
/// Cancel-safe: a descent's step is consumed only after its fetch completes,
/// so a dropped [`next`](Self::next) future replays the same descent.
///
/// Every fault is non-terminal (a failed descent replays), so a fault rides
/// the delivered turn rather than ending the walk. The walk advances inside
/// [`admit`](Self::admit), where the launch a fresh descent needs is
/// reachable.
#[derive(Debug)]
pub struct Cursor<'a, S, F: Format = V1, R: NodeRef = ChunkRef> {
    store: &'a S,
    /// One frame per referenced hop on the current path.
    stack: Vec<Frame<F, R>>,
    /// Exclusive upper bound on yielded keys.
    end: Option<Bytes>,
    /// Node fetches launched ahead of the walk.
    in_flight: FuturesUnordered<BoxFuture<'a, Fetched<F, R>>>,
    /// Completions that arrived before the descent awaiting them; drained by
    /// sequence id and bounded with the in-flight set by the window.
    ready: Vec<Fetched<F, R>>,
    /// The next fetch sequence id to hand out.
    next_seq: usize,
    /// The turn advanced to under `admit`, awaiting hand-over.
    staged: Option<Turn<F>>,
    done: bool,
    /// Remaining yields a paginated cursor may return; `None` is unbounded.
    remaining: Option<usize>,
}

/// What visiting the top frame's next step resolves to, computed under a short
/// borrow so the bound check and the stack push never overlap it.
enum Advance<F: Format> {
    /// The frame is spent; drop it and resume its parent.
    Pop,
    /// A key and its value at this position.
    Yield(Vec<u8>, Entry<F>),
    /// Descend into the referenced child rooted at this key prefix, claiming
    /// the prefetch landed under this sequence id once tagged.
    Descend(Vec<u8>, Option<usize>),
}

impl<'a, S, F, R> Cursor<'a, S, F, R>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    /// Stage the next turn, then launch the read-ahead the walk needs.
    ///
    /// Staging first is what makes a fresh descent reachable: the fill only
    /// launches what the staged walk position asks for.
    fn admit(&mut self) {
        if self.staged.is_none() {
            self.staged = self.advance();
        }
        let store = self.store;
        let end = self.end.as_ref();
        fill(
            F::READ_AHEAD,
            self.ready.len(),
            &mut self.next_seq,
            &mut self.stack,
            &mut self.in_flight,
            |base, step| {
                let key = join(base, step.suffix());
                if end.is_some_and(|end| key.as_slice() >= end.as_ref()) {
                    // The walk stops at this bound; nothing beyond it is fetched.
                    return Plan::Stop;
                }
                match step {
                    Step::Value { .. } => Plan::Skip,
                    Step::Ref { reference, .. } => {
                        let reference = reference.clone();
                        Plan::Fetch(async move {
                            store
                                .get_node::<F, R>(&reference)
                                .await
                                .map(|node| flatten(&node, false))
                                .map_err(ReaderError::from)
                        })
                    }
                }
            },
        );
    }

    /// One poll of the bounded-admission walk: admit, hand over a staged
    /// turn, else fold one completion. `None` ends the walk.
    ///
    /// All state lives in `self`, so a dropped poll replays.
    fn poll_turn(&mut self, cx: &mut Context<'_>) -> Poll<Option<Turn<F>>> {
        loop {
            self.admit();
            if let Some(turn) = self.staged.take() {
                return Poll::Ready(Some(turn));
            }
            match Pin::new(&mut self.in_flight).poll_next(cx) {
                Poll::Ready(Some(completion)) => self.ready.push(completion),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Advance the walk to its next deliverable turn: pop spent frames, yield
    /// values, and descend once a child's fetch has landed. `None` parks the
    /// walk on the head fetch the fill launches.
    fn advance(&mut self) -> Option<Turn<F>> {
        loop {
            let advance = match self.stack.last_mut() {
                None => return None,
                Some(frame) => {
                    let index = frame.index;
                    match frame.steps.get(index) {
                        None => Advance::Pop,
                        Some(step) => match step {
                            Step::Value { suffix, entry } => {
                                frame.index = index.saturating_add(1);
                                Advance::Yield(join(&frame.base, suffix), entry.clone())
                            }
                            Step::Ref { suffix, .. } => {
                                Advance::Descend(join(&frame.base, suffix), frame.tag(index))
                            }
                        },
                    }
                }
            };
            match advance {
                Advance::Pop => {
                    self.stack.pop();
                }
                Advance::Yield(key, entry) => {
                    if self.past_end(&key) {
                        return Some(Ok(None));
                    }
                    return Some(Ok(Some((Key::new(Bytes::from(key)), entry))));
                }
                Advance::Descend(child_base, seq) => {
                    if self.past_end(&child_base) {
                        return Some(Ok(None));
                    }
                    let seq = seq?;
                    let result = claim(&mut self.ready, seq)?;
                    match result {
                        Ok(steps) => {
                            // The step is consumed only now, so a cancelled or
                            // failed fetch replays the same descent.
                            if let Some(frame) = self.stack.last_mut() {
                                frame.index = frame.index.saturating_add(1);
                            }
                            self.stack
                                .push(Frame::new(Bytes::from(child_base), steps, 0));
                        }
                        Err(error) => {
                            // The launch is spent; untag so the replay
                            // relaunches the fetch.
                            if let Some(frame) = self.stack.last_mut() {
                                let index = frame.index;
                                frame.clear_tag(index);
                            }
                            return Some(Err(error));
                        }
                    }
                }
            }
        }
    }

    /// Whether `key` has reached the exclusive upper bound. A referenced child
    /// whose least key is already at the bound holds nothing in range, so the
    /// same test prunes the descent.
    fn past_end(&self, key: &[u8]) -> bool {
        self.end.as_ref().is_some_and(|end| key >= end.as_ref())
    }

    /// Position a cursor at the least key `>= start`, streaming forward until
    /// `end` (exclusive), descending only the referenced hops on the seek path.
    pub(crate) async fn seek(
        store: &'a S,
        root: &R,
        start: &[u8],
        end: Option<Bytes>,
    ) -> Result<Self, ReaderError> {
        let mut stack: Vec<Frame<F, R>> = Vec::new();
        let mut base: Vec<u8> = Vec::new();
        let mut reference = root.clone();
        let mut is_root = true;
        loop {
            let node = store.get_node::<F, R>(&reference).await?;
            let steps = flatten(&node, is_root);
            let remaining = start.get(base.len()..).unwrap_or(&[]);
            if remaining.is_empty() {
                stack.push(Frame::new(Bytes::from(base), steps, 0));
                break;
            }
            let mut chosen = steps.len();
            let mut deeper: Option<(usize, R, Bytes)> = None;
            for (i, step) in steps.iter().enumerate() {
                let v = step.suffix();
                if v >= remaining {
                    chosen = i;
                    break;
                }
                // `v < remaining`: the seek key descends only into a referenced
                // child whose whole edge is a prefix of what remains.
                if let Step::Ref {
                    suffix,
                    reference: child,
                } = step
                    && remaining.starts_with(v)
                {
                    deeper = Some((i, child.clone(), suffix.clone()));
                    break;
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
                    reference = child;
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
            in_flight: FuturesUnordered::new(),
            ready: Vec::new(),
            next_seq: 0,
            staged: None,
            done: false,
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
            in_flight: FuturesUnordered::new(),
            ready: Vec::new(),
            next_seq: 0,
            staged: None,
            done: true,
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
        let Some(turn) = poll_fn(|cx| self.poll_turn(cx)).await else {
            self.done = true;
            return Ok(None);
        };
        match turn {
            Ok(Some((key, entry))) => {
                if let Some(left) = self.remaining {
                    self.remaining = Some(left.saturating_sub(1));
                }
                Ok(Some((key, entry)))
            }
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

impl<S, F, R> Reader<S, F, R>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    /// Every `(key, value)` in ascending key order.
    pub async fn iter(&self, root: &R) -> Result<Cursor<'_, S, F, R>, ReaderError> {
        Cursor::seek(self.store(), root, &[], None).await
    }

    /// Every `(key, value)` within `bounds`, in ascending key order. Keys
    /// order as byte strings.
    pub async fn range(
        &self,
        root: &R,
        bounds: impl RangeBounds<Key>,
    ) -> Result<Cursor<'_, S, F, R>, ReaderError> {
        let (start, end) = half_open(&bounds);
        Cursor::seek(self.store(), root, &start, end).await
    }

    /// Every `(key, value)` whose key starts with `prefix`, in ascending order.
    ///
    /// The prefix range is `[prefix, successor(prefix))`; an all-`0xFF` or empty
    /// prefix has no successor and the scan runs unbounded to the last key.
    pub async fn prefix(&self, root: &R, prefix: &Key) -> Result<Cursor<'_, S, F, R>, ReaderError> {
        let end = successor(prefix.as_bytes());
        Cursor::seek(self.store(), root, prefix.as_bytes(), end).await
    }

    /// The greatest key `<= key` and its value, or `None` when every key is
    /// larger.
    ///
    /// Follows the target down the trie and, where the path dead-ends, takes the
    /// rightmost key of the largest branch left of it, so the cost stays
    /// O(depth) rather than a scan of the level.
    pub async fn floor(&self, root: &R, key: &Key) -> Result<Option<(Key, Entry<F>)>, ReaderError> {
        let store = self.store();
        let target = key.as_bytes();
        let mut base: Vec<u8> = Vec::new();
        let mut reference = root.clone();
        let mut is_root = true;
        // The greatest branch strictly left of the target found at a shallower
        // level; a deeper left branch always outranks it, so one slot suffices.
        let mut fallback: Option<(Bytes, Step<F, R>)> = None;
        loop {
            let node = store.get_node::<F, R>(&reference).await?;
            let steps = flatten(&node, is_root);
            let remaining = target.get(base.len()..).unwrap_or(&[]);
            let mut left: Option<Step<F, R>> = None;
            let mut descend: Option<(R, Bytes)> = None;
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
                            reference: child,
                        } => {
                            if remaining.starts_with(step.suffix()) {
                                descend = Some((child.clone(), suffix.clone()));
                                break;
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
                reference = child;
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
/// referenced child is its rightmost key.
async fn max_key<S, F, R>(
    store: &S,
    base: Bytes,
    step: Step<F, R>,
) -> Result<Option<(Key, Entry<F>)>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    let mut path = base.to_vec();
    match step {
        Step::Value { suffix, entry } => {
            path.extend_from_slice(&suffix);
            Ok(Some((Key::new(Bytes::from(path)), entry)))
        }
        Step::Ref { suffix, reference } => {
            path.extend_from_slice(&suffix);
            rightmost(store, path, reference).await
        }
    }
}

/// The rightmost key of the subtree at `reference`: the greatest step of each
/// chunk on the descent is the last one, so one hop per level reaches it.
async fn rightmost<S, F, R>(
    store: &S,
    mut path: Vec<u8>,
    mut reference: R,
) -> Result<Option<(Key, Entry<F>)>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
    R: NodeRef,
{
    loop {
        let node = store.get_node::<F, R>(&reference).await?;
        let steps = flatten(&node, false);
        match steps.last() {
            None => return Ok(None),
            Some(Step::Value { suffix, entry }) => {
                path.extend_from_slice(suffix);
                return Ok(Some((Key::new(Bytes::from(path)), entry.clone())));
            }
            Some(Step::Ref {
                suffix,
                reference: child,
            }) => {
                path.extend_from_slice(suffix);
                reference = child.clone();
            }
        }
    }
}

/// A chunk's contents flattened into ascending-key steps. The root chunk's own
/// value is the empty key, the least of all, so it leads the list.
pub(crate) fn flatten<F: Format, R: NodeRef>(node: &Node<F, R>, is_root: bool) -> Vec<Step<F, R>> {
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
fn flatten_table<F: Format, R: NodeRef>(
    table: &ForkTable<F, R>,
    prefix: &mut Vec<u8>,
    steps: &mut Vec<Step<F, R>>,
) {
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
            Some(Child::Ref(reference)) => steps.push(Step::Ref {
                suffix: Bytes::copy_from_slice(prefix.as_slice()),
                reference: reference.clone(),
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

/// The half-open byte range `bounds` selects: the first key the walk may yield
/// and the exclusive key it stops at.
pub(crate) fn half_open(bounds: &impl RangeBounds<Key>) -> (Vec<u8>, Option<Bytes>) {
    let start = match bounds.start_bound() {
        Bound::Unbounded => Vec::new(),
        Bound::Included(key) => key.as_bytes().to_vec(),
        Bound::Excluded(key) => join(key.as_bytes(), &[0]),
    };
    let end = match bounds.end_bound() {
        Bound::Unbounded => None,
        Bound::Excluded(key) => Some(Bytes::copy_from_slice(key.as_bytes())),
        Bound::Included(key) => Some(Bytes::from(join(key.as_bytes(), &[0]))),
    };
    (start, end)
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
    use crate::store::Plaintext;
    use core::pin::pin;
    use core::task::Poll;
    use std::vec;

    use nectar_primitives::store::{
        ChunkGet, ChunkStoreError, ContentGet, ContentGetError, MemoryStore,
    };
    use nectar_primitives::{Chunk, ChunkAddress, ChunkRef, ContentOnlyChunkSet, Verified};
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

    fn drain(mut cursor: Cursor<'_, &ContentGet<MemoryStore>>) -> Vec<(Vec<u8>, Entry)> {
        let mut out = Vec::new();
        while let Some((key, value)) = run(cursor.next()).unwrap() {
            out.push((key.as_bytes().to_vec(), value));
        }
        out
    }

    // A two-level manifest: a root fork "a" behind an embedded child holding
    // "aa"/"ab", and "b" behind a referenced leaf holding "ba".
    fn sample(store: &ContentGet<MemoryStore>) -> ChunkRef {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"a"), entry(0xBA).into(), None).unwrap();
        let leaf_ref = run(store.put_node(&Node::new(None, leaf), &Plaintext)).unwrap();

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
            .insert(prefix(b"b"), Child::Ref(leaf_ref).into(), None)
            .unwrap();
        run(store.put_node(&Node::new(None, forks), &Plaintext)).unwrap()
    }

    #[test]
    fn iteration_is_ascending_across_embedded_and_referenced_children() {
        let store = ContentGet::new(MemoryStore::default());
        let root = sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        let got = drain(run(reader.iter(&root)).unwrap());
        assert_eq!(
            got,
            vec![
                (b"aa".to_vec(), entry(0xAA)),
                (b"ab".to_vec(), entry(0xAB)),
                (b"ba".to_vec(), entry(0xBA)),
            ]
        );
    }

    #[test]
    fn the_root_value_is_the_empty_key_and_leads_iteration() {
        let store = ContentGet::new(MemoryStore::default());
        let root_ext = crate::node::RootExtension::new(Some(entry(9)), None);
        let mut forks = ForkTable::new();
        forks.insert(prefix(b"k"), entry(1).into(), None).unwrap();
        let root = run(store.put_node(&Node::new(root_ext, forks), &Plaintext)).unwrap();
        let reader: Reader<_> = Reader::new(&store);
        let got = drain(run(reader.iter(&root)).unwrap());
        assert_eq!(got, vec![(Vec::new(), entry(9)), (b"k".to_vec(), entry(1))]);
    }

    #[test]
    fn range_is_half_open() {
        let store = ContentGet::new(MemoryStore::default());
        let root = sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        let got = drain(
            run(reader.range(&root, &Key::from(&b"aa"[..])..&Key::from(&b"ba"[..]))).unwrap(),
        );
        // "aa" is included, "ba" is the excluded upper bound.
        assert_eq!(
            got,
            vec![(b"aa".to_vec(), entry(0xAA)), (b"ab".to_vec(), entry(0xAB))]
        );
    }

    #[test]
    fn range_starting_between_keys_seeks_to_the_ceiling() {
        let store = ContentGet::new(MemoryStore::default());
        let root = sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        let got =
            drain(run(reader.range(&root, &Key::from(&b"ac"[..])..&Key::from(&b"z"[..]))).unwrap());
        assert_eq!(got, vec![(b"ba".to_vec(), entry(0xBA))]);
    }

    #[test]
    fn prefix_selects_one_subtree() {
        let store = ContentGet::new(MemoryStore::default());
        let root = sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        let got = drain(run(reader.prefix(&root, &Key::from(&b"a"[..]))).unwrap());
        assert_eq!(
            got,
            vec![(b"aa".to_vec(), entry(0xAA)), (b"ab".to_vec(), entry(0xAB))]
        );
    }

    #[test]
    fn floor_resolves_present_absent_and_below_all_keys() {
        let store = ContentGet::new(MemoryStore::default());
        let root = sample(&store);
        let reader: Reader<_> = Reader::new(&store);
        // Exact hit.
        assert_eq!(
            run(reader.floor(&root, &Key::from(&b"ab"[..]))).unwrap(),
            Some((Key::from(&b"ab"[..]), entry(0xAB)))
        );
        // Between "ab" and "ba": floor is "ab".
        assert_eq!(
            run(reader.floor(&root, &Key::from(&b"az"[..]))).unwrap(),
            Some((Key::from(&b"ab"[..]), entry(0xAB)))
        );
        // Past the last key: floor is the greatest key, reached through the ref.
        assert_eq!(
            run(reader.floor(&root, &Key::from(&b"zz"[..]))).unwrap(),
            Some((Key::from(&b"ba"[..]), entry(0xBA)))
        );
        // Below every key: nothing.
        assert_eq!(
            run(reader.floor(&root, &Key::from(&b"a"[..]))).unwrap(),
            None
        );
    }

    // The walks are width-generic: a structurally encrypted database iterates,
    // ranges and floors exactly as the plaintext one does, opening each node
    // with the key its own reference carries.
    #[cfg(feature = "encryption")]
    #[test]
    fn the_walks_hold_over_an_encrypted_database() {
        use nectar_primitives::EncryptedChunkRef;

        use crate::builder::Builder;
        use crate::encryption::Encrypted;

        let store = ContentGet::new(MemoryStore::default());
        let rows: [(&[u8], u8); 3] = [(b"aa", 0xAA), (b"ab", 0xAB), (b"ba", 0xBA)];
        let mut builder = Builder::<V1>::new();
        for (key, fill) in rows {
            builder.insert(Key::from(key), entry(fill), None);
        }
        let root = run(builder.build(&store, &Encrypted::<V1>::new(b"secret")))
            .unwrap()
            .root()
            .clone();

        let reader = Reader::<&ContentGet<MemoryStore>, V1, EncryptedChunkRef>::new(&store);
        let mut cursor = run(reader.iter(&root)).unwrap();
        let mut got = Vec::new();
        while let Some((key, value)) = run(cursor.next()).unwrap() {
            got.push((key.as_bytes().to_vec(), value));
        }
        assert_eq!(
            got,
            vec![
                (b"aa".to_vec(), entry(0xAA)),
                (b"ab".to_vec(), entry(0xAB)),
                (b"ba".to_vec(), entry(0xBA)),
            ]
        );

        // A half-open range and a floor lookup follow the same encrypted hops.
        let mut cursor =
            run(reader.range(&root, &Key::from(&b"aa"[..])..&Key::from(&b"ba"[..]))).unwrap();
        let mut ranged = Vec::new();
        while let Some((key, _)) = run(cursor.next()).unwrap() {
            ranged.push(key.as_bytes().to_vec());
        }
        assert_eq!(ranged, vec![b"aa".to_vec(), b"ab".to_vec()]);
        assert_eq!(
            run(reader.floor(&root, &Key::from(&b"az"[..]))).unwrap(),
            Some((Key::from(&b"ab"[..]), entry(0xAB)))
        );
    }

    // A root value "a" plus a referenced leaf under "b" holding "ba".
    fn with_ref(store: &ContentGet<MemoryStore>) -> (ChunkRef, ChunkRef) {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"a"), entry(0xBA).into(), None).unwrap();
        let leaf_addr = run(store.put_node(&Node::new(None, leaf), &Plaintext)).unwrap();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), entry(0xA1).into(), None)
            .unwrap();
        forks
            .insert(prefix(b"b"), Child::Ref(leaf_addr).into(), None)
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks), &Plaintext)).unwrap();
        (root, leaf_addr)
    }

    /// Yield once, waking immediately, so the caller observes a pending poll.
    async fn yield_once() {
        let mut yielded = false;
        poll_fn(|cx| {
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

    /// Store wrapper that yields once per get, so a `next` future can be
    /// observed mid-fetch.
    struct SlowStore {
        inner: ContentGet<MemoryStore>,
    }

    impl ChunkGet<ContentOnlyChunkSet> for SlowStore {
        type Trust = Verified;
        type Error = ContentGetError<ChunkStoreError>;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
            yield_once().await;
            ChunkGet::get(&self.inner, address).await
        }
    }

    #[test]
    fn a_dropped_next_future_loses_no_keys() {
        let store = ContentGet::new(MemoryStore::default());
        let (root, _) = with_ref(&store);
        let slow = SlowStore { inner: store };
        let reader: Reader<_> = Reader::new(&slow);
        run(async {
            let mut cursor = reader.iter(&root).await.unwrap();
            assert_eq!(
                cursor.next().await.unwrap(),
                Some((Key::from(&b"a"[..]), entry(0xA1)))
            );
            // Drop a next future mid-fetch of the referenced leaf.
            {
                let mut fut = pin!(cursor.next());
                let state = poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx))).await;
                assert!(state.is_pending());
            }
            // The descent replays; no key under the leaf is lost.
            assert_eq!(
                cursor.next().await.unwrap(),
                Some((Key::from(&b"ba"[..]), entry(0xBA)))
            );
            assert_eq!(cursor.next().await.unwrap(), None);
        });
    }

    /// Store wrapper that fails the first `failures` gets of one address.
    struct FlakyStore {
        inner: ContentGet<MemoryStore>,
        deny: ChunkAddress,
        failures: std::sync::Mutex<usize>,
    }

    impl ChunkGet<ContentOnlyChunkSet> for FlakyStore {
        type Trust = Verified;
        type Error = ContentGetError<ChunkStoreError>;

        async fn get(
            &self,
            address: &ChunkAddress,
        ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
            if *address == self.deny {
                let mut left = self.failures.lock().unwrap();
                if *left > 0 {
                    *left = left.saturating_sub(1);
                    return Err(ContentGetError::Inner(ChunkStoreError::not_found(address)));
                }
            }
            ChunkGet::get(&self.inner, address).await
        }
    }

    #[test]
    fn a_failed_resolve_replays_the_same_descent() {
        let store = ContentGet::new(MemoryStore::default());
        let (root, leaf) = with_ref(&store);
        let flaky = FlakyStore {
            inner: store,
            deny: *leaf.address(),
            failures: std::sync::Mutex::new(1),
        };
        let reader: Reader<_> = Reader::new(&flaky);
        run(async {
            let mut cursor = reader.iter(&root).await.unwrap();
            assert_eq!(
                cursor.next().await.unwrap(),
                Some((Key::from(&b"a"[..]), entry(0xA1)))
            );
            // The leaf fetch fails once; the step stays unconsumed.
            assert!(matches!(
                cursor.next().await.unwrap_err(),
                ReaderError::Store(_)
            ));
            // The retry replays the descent and reads the leaf.
            assert_eq!(
                cursor.next().await.unwrap(),
                Some((Key::from(&b"ba"[..]), entry(0xBA)))
            );
            assert_eq!(cursor.next().await.unwrap(), None);
        });
    }

    #[test]
    fn successor_bounds_the_prefix_range() {
        assert_eq!(successor(b"ab").as_deref(), Some(&b"ac"[..]));
        assert_eq!(successor(b"a\xff").as_deref(), Some(&b"b"[..]));
        assert_eq!(successor(b"\xff\xff"), None);
        assert_eq!(successor(b""), None);
    }
}
