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
use core::convert::Infallible;
use core::future::poll_fn;

use bytes::Bytes;
use nectar_governor::{BoxFuture, Driver, FuturesUnordered, WalkPolicy};
use nectar_primitives::ChunkAddress;
#[cfg(feature = "encryption")]
use nectar_primitives::EncryptedChunkRef;
use nectar_primitives::store::MaybeSync;

use crate::fork::{Child, ForkTable};
use crate::format::{Format, V1};
use crate::frontier::{Completion, Frame, Plan, claim, fill};
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
pub(crate) enum Step<F: Format> {
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
    /// The trie continues into an encrypted child the plain cursor cannot
    /// open.
    Encrypted {
        /// Key bytes below the chunk root leading to the child.
        suffix: Bytes,
        /// The child's reference: address plus decryption key, carried for
        /// the traversal that can open it.
        #[cfg(feature = "encryption")]
        reference: EncryptedChunkRef,
    },
}

impl<F: Format> Step<F> {
    /// The key bytes below the chunk root.
    fn suffix(&self) -> &[u8] {
        match self {
            Self::Value { suffix, .. }
            | Self::Ref { suffix, .. }
            | Self::Encrypted { suffix, .. } => suffix,
        }
    }
}

/// One completed prefetch payload: the fetched child's flattened steps.
type Fetched<F> = Completion<Vec<Step<F>>>;

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
#[derive(Debug)]
pub struct Cursor<'a, S, F: Format = V1> {
    /// The walk, advanced by the policy and driven by the kernel driver.
    driver: Driver<'a, ScanPolicy<'a, S, F>, Fetched<F>>,
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
    /// An encrypted child blocks the walk at this key prefix.
    Encrypted(Vec<u8>),
}

/// The cursor's walk policy over the kernel driver.
///
/// Every fault is non-terminal (a failed descent replays, an encrypted edge
/// is stepped past), so faults ride the delivered turn and the driver error
/// is uninhabited. The walk advances inside `admit`, where the launch a
/// fresh descent needs is reachable; `take_ready` hands over the staged
/// turn.
struct ScanPolicy<'a, S, F: Format> {
    store: &'a S,
    /// One frame per referenced hop on the current path.
    stack: Vec<Frame<F>>,
    /// Exclusive upper bound on yielded keys.
    end: Option<Bytes>,
    /// Completions that arrived before the descent awaiting them; drained by
    /// sequence id and bounded with the in-flight set by the window.
    ready: Vec<Fetched<F>>,
    /// The next fetch sequence id to hand out.
    next_seq: usize,
    /// The turn advanced to under `admit`, awaiting hand-over.
    staged: Option<Turn<F>>,
}

impl<'a, S, F> WalkPolicy<'a> for ScanPolicy<'a, S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    type Fetched = Fetched<F>;
    type Frame = Turn<F>;
    type Error = Infallible;
    type Drain = ();

    fn admit(&mut self, in_flight: &mut FuturesUnordered<BoxFuture<'a, Fetched<F>>>) {
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
            in_flight,
            |base, step| {
                let key = join(base, step.suffix());
                if end.is_some_and(|end| key.as_slice() >= end.as_ref()) {
                    // The walk stops at this bound; nothing beyond it is fetched.
                    return Plan::Stop;
                }
                match step {
                    // The walk errors here; no deeper node is fetched.
                    Step::Encrypted { .. } => Plan::Stop,
                    Step::Value { .. } => Plan::Skip,
                    Step::Ref { addr, .. } => {
                        let addr = *addr;
                        Plan::Fetch(async move {
                            store
                                .get_node::<F>(&addr)
                                .await
                                .map(|node| flatten(&node, false))
                                .map_err(ReaderError::from)
                        })
                    }
                }
            },
        );
    }

    fn take_ready(&mut self, (): ()) -> Option<Result<Turn<F>, Infallible>> {
        self.staged.take().map(Ok)
    }

    fn absorb(&mut self, completion: Fetched<F>) -> Result<(), Infallible> {
        self.ready.push(completion);
        Ok(())
    }

    fn drained(&self) -> Result<(), Infallible> {
        Ok(())
    }
}

impl<S, F> ScanPolicy<'_, S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
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
                            Step::Encrypted { suffix, .. } => {
                                frame.index = index.saturating_add(1);
                                Advance::Encrypted(join(&frame.base, suffix))
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
                Advance::Encrypted(child_base) => {
                    if self.past_end(&child_base) {
                        return Some(Ok(None));
                    }
                    return Some(Err(ReaderError::EncryptedChild));
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
            driver: Driver::new(ScanPolicy {
                store,
                stack,
                end,
                ready: Vec::new(),
                next_seq: 0,
                staged: None,
            }),
            done: false,
            remaining: None,
        })
    }

    /// An already-exhausted cursor: yields nothing. Used when a paginated seek
    /// starts past the last key.
    pub(crate) fn exhausted(store: &'a S) -> Self {
        Self {
            driver: Driver::new(ScanPolicy {
                store,
                stack: Vec::new(),
                end: None,
                ready: Vec::new(),
                next_seq: 0,
                staged: None,
            }),
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
        let Some(turn) = poll_fn(|cx| self.driver.poll(cx, ())).await else {
            self.done = true;
            return Ok(None);
        };
        match turn {
            Ok(turn) => match turn {
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
            },
            Err(error) => match error {},
        }
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
pub(crate) fn flatten<F: Format>(node: &Node<F>, is_root: bool) -> Vec<Step<F>> {
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
            #[cfg(feature = "encryption")]
            Some(Child::Ref64(reference)) => steps.push(Step::Encrypted {
                suffix: Bytes::copy_from_slice(prefix.as_slice()),
                reference: reference.clone(),
            }),
            #[cfg(not(feature = "encryption"))]
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
    use core::task::Poll;
    use std::vec;

    use nectar_primitives::store::{
        ChunkGet, ChunkStoreError, ContentGet, ContentGetError, MemoryStore,
    };
    use nectar_primitives::{
        Chunk, ChunkAddress, ChunkRef, ContentOnlyChunkSet, EncryptedChunkRef, EncryptionKey,
        Verified,
    };
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
    fn sample(store: &ContentGet<MemoryStore>) -> ChunkAddress {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"a"), entry(0xBA).into(), None).unwrap();
        let leaf_ref = run(store.put_node(&Node::new(None, leaf))).unwrap();

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
        run(store.put_node(&Node::new(None, forks))).unwrap()
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
        let root = run(store.put_node(&Node::new(root_ext, forks))).unwrap();
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
            run(reader.range(&root, &Key::from(&b"aa"[..]), &Key::from(&b"ba"[..]))).unwrap(),
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
            drain(run(reader.range(&root, &Key::from(&b"ac"[..]), &Key::from(&b"z"[..]))).unwrap());
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

    // An encrypted (ref64) child the plain reader cannot open.
    fn encrypted(byte: u8) -> Child {
        Child::Ref64(EncryptedChunkRef::new(
            ChunkAddress::new([byte; 32]),
            EncryptionKey::from([byte ^ 0xFF; 32]),
        ))
    }

    // A root holding "a" and "z" as plain values with an encrypted subtree
    // wedged between them under "m".
    fn with_encrypted(store: &ContentGet<MemoryStore>) -> ChunkAddress {
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
        run(store.put_node(&Node::new(None, forks))).unwrap()
    }

    #[test]
    fn iteration_surfaces_an_encrypted_subtree_as_an_error() {
        let store = ContentGet::new(MemoryStore::default());
        let root = with_encrypted(&store);
        let reader: Reader<_> = Reader::new(&store);
        let mut cursor = run(reader.iter(&root)).unwrap();
        // The plain value before the encrypted edge reads back.
        assert_eq!(
            run(cursor.next()).unwrap(),
            Some((Key::from(&b"a"[..]), entry(0xA1)))
        );
        // Reaching the encrypted child stops the walk with an error.
        assert!(matches!(
            run(cursor.next()).unwrap_err(),
            ReaderError::EncryptedChild
        ));
    }

    #[test]
    fn a_bound_short_of_the_encrypted_edge_prunes_it() {
        let store = ContentGet::new(MemoryStore::default());
        let root = with_encrypted(&store);
        let reader: Reader<_> = Reader::new(&store);
        // "m" is the exclusive upper bound, so the encrypted child at "m" is
        // pruned rather than fetched, and the scan completes without error.
        let got =
            drain(run(reader.range(&root, &Key::from(&b"a"[..]), &Key::from(&b"m"[..]))).unwrap());
        assert_eq!(got, vec![(b"a".to_vec(), entry(0xA1))]);
    }

    #[test]
    fn floor_past_an_encrypted_edge_reads_the_plain_key() {
        let store = ContentGet::new(MemoryStore::default());
        let root = with_encrypted(&store);
        let reader: Reader<_> = Reader::new(&store);
        // The floor of "z" is "z" itself; the encrypted subtree is left of the
        // path and never opened.
        assert_eq!(
            run(reader.floor(&root, &Key::from(&b"z"[..]))).unwrap(),
            Some((Key::from(&b"z"[..]), entry(0x2C)))
        );
    }

    #[test]
    fn floor_landing_in_an_encrypted_subtree_cannot_be_read() {
        let store = ContentGet::new(MemoryStore::default());
        let root = with_encrypted(&store);
        let reader: Reader<_> = Reader::new(&store);
        // Every key at or below "n" that could be the floor lives in the
        // encrypted subtree under "m", so the answer is unreadable.
        assert!(matches!(
            run(reader.floor(&root, &Key::from(&b"n"[..]))).unwrap_err(),
            ReaderError::EncryptedChild
        ));
    }

    // A root value "a" plus a referenced leaf under "b" holding "ba".
    fn with_ref(store: &ContentGet<MemoryStore>) -> (ChunkAddress, ChunkAddress) {
        let mut leaf = ForkTable::new();
        leaf.insert(prefix(b"a"), entry(0xBA).into(), None).unwrap();
        let leaf_addr = run(store.put_node(&Node::new(None, leaf))).unwrap();
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), entry(0xA1).into(), None)
            .unwrap();
        forks
            .insert(
                prefix(b"b"),
                Child::Ref32(ChunkRef::new(leaf_addr)).into(),
                None,
            )
            .unwrap();
        let root = run(store.put_node(&Node::new(None, forks))).unwrap();
        (root, leaf_addr)
    }

    /// Yield once, waking immediately, so the caller observes a pending poll.
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
                let fut = cursor.next();
                futures::pin_mut!(fut);
                let state = futures::future::poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx))).await;
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
            deny: leaf,
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
