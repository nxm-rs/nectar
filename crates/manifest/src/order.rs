//! Order-statistic queries: rank, select, count and rank-directed pagination
//! in O(depth), not O(window).
//!
//! Every fork's subtree carries its distinct key-count on the wire (spec 5.6):
//! a referenced child's count is the stored annotation, an embedded child's is
//! summed in the parent buffer, and a spilled node's segment descriptors carry a
//! per-segment sum. A rank or index descent therefore skips a whole subtree by
//! reading its count and routes a spilled node's segments by `seg_count`, so it
//! fetches one node per referenced hop on the path and never the window it steps
//! over.

use bytes::Bytes;
use nectar_primitives::store::MaybeSync;
use nectar_primitives::{ChunkAddress, ChunkOps};

use crate::codec::{DecodeError, DecodedChunk, SegDesc, SegmentDir};
use crate::fork::{Child, ForkTable};
use crate::format::Format;
use crate::node::{Node, RootExtension};
use crate::reader::{Reader, ReaderError};
use crate::scan::{Cursor, successor};
use crate::store::{NodeGet, StoreError};
use crate::value::{Entry, Key};

/// One flattened position of a chunk's fork table, in ascending key order, with
/// the count of the keys it stands for. A value stands for itself; a referenced
/// or encrypted child stands for its whole subtree, counted without a fetch.
enum Ranked<F: Format> {
    /// A key terminates here with this value; it counts once.
    Value {
        /// Key bytes below the chunk root.
        suffix: Vec<u8>,
        /// The bound value.
        entry: Entry<F>,
    },
    /// The trie continues into a referenced child holding `count` keys.
    Ref {
        /// Key bytes below the chunk root leading to the child.
        suffix: Vec<u8>,
        /// The child chunk address.
        addr: ChunkAddress,
        /// The child subtree's distinct key-count.
        count: u64,
    },
    /// An encrypted child the plain reader cannot open, holding `count` keys.
    /// Its count still routes a rank past it; only a descent into it fails.
    Encrypted {
        /// Key bytes below the chunk root leading to the child.
        suffix: Vec<u8>,
        /// The child subtree's distinct key-count.
        count: u64,
    },
}

impl<F: Format> Ranked<F> {
    /// The key bytes below the chunk root.
    fn suffix(&self) -> &[u8] {
        match self {
            Self::Value { suffix, .. }
            | Self::Ref { suffix, .. }
            | Self::Encrypted { suffix, .. } => suffix,
        }
    }

    /// The number of keys this position stands for.
    const fn count(&self) -> u64 {
        match self {
            Self::Value { .. } => 1,
            Self::Ref { count, .. } | Self::Encrypted { count, .. } => *count,
        }
    }
}

/// Flatten a chunk's fork table into ascending-key ranked positions, recursing
/// embedded children in the parent buffer so only referenced hops leave a step.
///
/// A referenced child's count is the stored annotation; an embedded child is
/// expanded in place, so the positions of one chunk carry no fetch between them.
fn ranked<F: Format>(table: &ForkTable<F>) -> Vec<Ranked<F>> {
    let mut steps = Vec::new();
    let mut prefix = Vec::new();
    ranked_table(table, &mut prefix, &mut steps);
    steps
}

/// Walk a fork table in wire order, appending each terminal and referenced child
/// as a ranked position and recursing embedded children in place.
fn ranked_table<F: Format>(table: &ForkTable<F>, prefix: &mut Vec<u8>, steps: &mut Vec<Ranked<F>>) {
    for (first, record) in table.iter() {
        let mark = prefix.len();
        prefix.push(first);
        prefix.extend_from_slice(record.tail().as_bytes());
        if let Some(entry) = record.entry() {
            steps.push(Ranked::Value {
                suffix: prefix.clone(),
                entry: entry.clone(),
            });
        }
        match record.child() {
            Some(Child::Embedded(inner)) => ranked_table(inner, prefix, steps),
            Some(Child::Ref32(reference)) => steps.push(Ranked::Ref {
                suffix: prefix.clone(),
                addr: *reference.address(),
                count: record.child_count().unwrap_or_default().get(),
            }),
            Some(Child::Ref64(_)) => steps.push(Ranked::Encrypted {
                suffix: prefix.clone(),
                count: record.child_count().unwrap_or_default().get(),
            }),
            None => {}
        }
        prefix.truncate(mark);
    }
}

/// The on-path contents of one node: its fork-table positions and the keys in
/// the segments strictly before them. A spilled node contributes only the
/// covering leaf's positions; `Before` means the target precedes every fork.
enum OnPath<F: Format> {
    /// The target precedes every fork in the node; nothing at or below it.
    Before,
    /// The covering fragment's positions and the count skipped before them.
    Fragment {
        /// Keys in the segments strictly before the covering fragment.
        before: u64,
        /// The covering fragment's ranked positions.
        steps: Vec<Ranked<F>>,
    },
}

/// The empty-key value a decoded root carries: its root extension entry.
fn root_entry<F: Format>(decoded: &DecodedChunk<F>) -> Option<Entry<F>> {
    match decoded {
        DecodedChunk::Node(node) => node.entry().cloned(),
        DecodedChunk::Segmented(root, _) => root.as_ref().and_then(RootExtension::entry).cloned(),
        DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => None,
    }
}

/// The descriptor covering `byte`: the one with the greatest first key not past
/// it, and the summed count of every descriptor before it. `None` when `byte`
/// precedes the first descriptor.
fn covering(dir: &SegmentDir, byte: u8) -> Option<(u64, &SegDesc)> {
    let mut before = 0u64;
    let mut cover: Option<&SegDesc> = None;
    for desc in &dir.descriptors {
        if desc.first_key > byte {
            break;
        }
        if let Some(prior) = cover {
            before = before.saturating_add(prior.seg_count.get());
        }
        cover = Some(desc);
    }
    cover.map(|desc| (before, desc))
}

/// Where an index falls across a directory's descriptors: the residual index
/// into the covering descriptor and the descriptor itself, subtracting the
/// skipped segments' counts. `None` when the index runs past the node's total.
fn covering_index(dir: &SegmentDir, index: u64) -> Option<(u64, &SegDesc)> {
    let mut index = index;
    for desc in &dir.descriptors {
        let count = desc.seg_count.get();
        if index < count {
            return Some((index, desc));
        }
        index = index.saturating_sub(count);
    }
    None
}

impl<S, F> Reader<S, F>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    /// The number of keys with `lo <= key < hi`, computed as `rank(hi) -
    /// rank(lo)` in two O(depth) descents, so a window's size never fetches it.
    pub async fn count(&self, root: &ChunkAddress, lo: &Key, hi: &Key) -> Result<u64, ReaderError> {
        let high = self.rank(root, hi).await?;
        let low = self.rank(root, lo).await?;
        Ok(high.saturating_sub(low))
    }

    /// The number of keys strictly less than `key`: its position in ascending
    /// key order.
    ///
    /// Descends one referenced hop per level, adding the whole-subtree count of
    /// every fork and segment strictly before the target and recursing only the
    /// one child on its path. An encrypted subtree on the path cannot be opened;
    /// one merely before the target routes by its stored count.
    pub async fn rank(&self, root: &ChunkAddress, key: &Key) -> Result<u64, ReaderError> {
        let target = key.as_bytes();
        let mut address = *root;
        let mut consumed = 0usize;
        let mut acc = 0u64;
        let mut is_root = true;
        loop {
            let decoded = decode_at::<S, F>(self.store(), &address).await?;
            if is_root && !target.is_empty() && root_entry(&decoded).is_some() {
                acc = acc.saturating_add(1);
            }
            let Some(remaining) = target.get(consumed..).filter(|rest| !rest.is_empty()) else {
                return Ok(acc);
            };
            let steps = match on_path(self.store(), &decoded, remaining).await? {
                OnPath::Before => return Ok(acc),
                OnPath::Fragment { before, steps } => {
                    acc = acc.saturating_add(before);
                    steps
                }
            };
            match rank_fragment(&steps, remaining) {
                Step::Here(count) => return Ok(acc.saturating_add(count)),
                Step::Encrypted => return Err(ReaderError::EncryptedChild),
                Step::Cross {
                    addr,
                    matched,
                    before,
                } => {
                    acc = acc.saturating_add(before);
                    consumed = consumed.saturating_add(matched);
                    address = addr;
                    is_root = false;
                }
            }
        }
    }

    /// The key and value at position `index` in ascending key order, or `None`
    /// when `index` is at or past the key count.
    ///
    /// Descends one referenced hop per level, skipping any subtree whose count
    /// the index clears and routing a spilled node's segments by `seg_count`, so
    /// the offset costs O(depth), never O(index).
    pub async fn select(
        &self,
        root: &ChunkAddress,
        index: u64,
    ) -> Result<Option<(Key, Entry<F>)>, ReaderError> {
        let mut address = *root;
        let mut index = index;
        let mut path: Vec<u8> = Vec::new();
        let mut is_root = true;
        loop {
            let decoded = decode_at::<S, F>(self.store(), &address).await?;
            if is_root && let Some(entry) = root_entry(&decoded) {
                if index == 0 {
                    return Ok(Some((Key::new(Bytes::from(path)), entry)));
                }
                index = index.saturating_sub(1);
            }
            let Some(steps) = index_path(self.store(), &decoded, &mut index).await? else {
                return Ok(None);
            };
            match select_fragment(steps, &mut index) {
                Some(Reach::Found { suffix, entry }) => {
                    path.extend_from_slice(&suffix);
                    return Ok(Some((Key::new(Bytes::from(path)), entry)));
                }
                Some(Reach::Encrypted) => return Err(ReaderError::EncryptedChild),
                Some(Reach::Cross { addr, suffix }) => {
                    path.extend_from_slice(&suffix);
                    address = addr;
                    is_root = false;
                }
                None => return Ok(None),
            }
        }
    }

    /// A cursor over the keys with `lo <= key < hi`, positioned `offset` keys in
    /// and yielding at most `limit`.
    ///
    /// The offset is a rank-directed seek, so paging deep into a listing costs
    /// O(depth), not O(offset); the yielded slice equals `range(lo, hi)` skipped
    /// `offset` and taken `limit`.
    pub async fn paginate(
        &self,
        root: &ChunkAddress,
        lo: &Key,
        hi: &Key,
        offset: u64,
        limit: usize,
    ) -> Result<Cursor<'_, S, F>, ReaderError> {
        let end = Some(Bytes::copy_from_slice(hi.as_bytes()));
        let start = self.rank(root, lo).await?.saturating_add(offset);
        self.page(root, start, end, limit).await
    }

    /// A cursor over the keys carrying `prefix`, positioned `offset` keys in and
    /// yielding at most `limit`; the empty prefix pages the whole manifest.
    ///
    /// The offset is a rank-directed seek, so it costs O(depth), not O(offset);
    /// the yielded slice equals `prefix(prefix)` skipped `offset` and taken
    /// `limit`.
    pub async fn paginate_prefix(
        &self,
        root: &ChunkAddress,
        prefix: &Key,
        offset: u64,
        limit: usize,
    ) -> Result<Cursor<'_, S, F>, ReaderError> {
        let end = successor(prefix.as_bytes());
        let start = self.rank(root, prefix).await?.saturating_add(offset);
        self.page(root, start, end, limit).await
    }

    /// Position a bounded cursor at the key of absolute rank `start`: select that
    /// key, then seek to it under `end`, capped at `limit`. An out-of-range start
    /// yields an exhausted cursor.
    async fn page(
        &self,
        root: &ChunkAddress,
        start: u64,
        end: Option<Bytes>,
        limit: usize,
    ) -> Result<Cursor<'_, S, F>, ReaderError> {
        match self.select(root, start).await? {
            None => Ok(Cursor::exhausted(self.store())),
            Some((key, _)) => Ok(Cursor::seek(self.store(), root, key.as_bytes(), end)
                .await?
                .with_limit(limit)),
        }
    }
}

/// Fetch and decode the chunk at `address` without materializing a spilled
/// node's segments: the descent routes them itself by `seg_count`.
async fn decode_at<S, F>(store: &S, address: &ChunkAddress) -> Result<DecodedChunk<F>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    let chunk = store.get(address).await.map_err(StoreError::store)?;
    Node::<F>::decode_chunk(chunk.envelope().data())
        .map_err(|error| ReaderError::Store(StoreError::Decode(error)))
}

/// Resolve the ranked positions on the target's path through a decoded chunk,
/// routing a spilled node's segments by `seg_count` so only the covering one is
/// fetched. `remaining` is the target key from this node's root, never empty.
async fn on_path<S, F>(
    store: &S,
    decoded: &DecodedChunk<F>,
    remaining: &[u8],
) -> Result<OnPath<F>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    match decoded {
        DecodedChunk::Node(node) => Ok(OnPath::Fragment {
            before: 0,
            steps: ranked(node.forks()),
        }),
        DecodedChunk::Segmented(_, dir) => route_key(store, dir, remaining).await,
        DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => Err(segment_context()),
    }
}

/// Route a spilled node's directory to the leaf fragment covering `remaining`,
/// summing the counts of the segments strictly before it. Descends at most the
/// two directory levels the frozen bounds allow (spec 5.4).
async fn route_key<S, F>(
    store: &S,
    dir: &SegmentDir,
    remaining: &[u8],
) -> Result<OnPath<F>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    let Some(&byte) = remaining.first() else {
        return Ok(OnPath::Before);
    };
    let mut before = 0u64;
    let mut current = match covering(dir, byte) {
        Some((skip, desc)) => {
            before = before.saturating_add(skip);
            desc.clone()
        }
        None => return Ok(OnPath::Before),
    };
    for _ in 0..2 {
        if current.key.is_some() {
            return Err(ReaderError::EncryptedChild);
        }
        match decode_at::<S, F>(store, &current.address).await? {
            DecodedChunk::Leaf(table) => {
                return Ok(OnPath::Fragment {
                    before,
                    steps: ranked(&table),
                });
            }
            DecodedChunk::Directory(inner) => match covering(&inner, byte) {
                Some((skip, desc)) => {
                    before = before.saturating_add(skip);
                    current = desc.clone();
                }
                None => return Ok(OnPath::Before),
            },
            _ => return Err(segment_context()),
        }
    }
    Err(segment_context())
}

/// Route a decoded chunk to the ranked positions holding the `index`th key,
/// subtracting a spilled node's skipped-segment counts from `index`. `None` when
/// the index runs past the node's total.
async fn index_path<S, F>(
    store: &S,
    decoded: &DecodedChunk<F>,
    index: &mut u64,
) -> Result<Option<Vec<Ranked<F>>>, ReaderError>
where
    S: NodeGet + MaybeSync,
    F: Format,
{
    match decoded {
        DecodedChunk::Node(node) => Ok(Some(ranked(node.forks()))),
        DecodedChunk::Segmented(_, dir) => {
            let mut dir = dir.clone();
            for _ in 0..2 {
                let Some((residual, desc)) = covering_index(&dir, *index) else {
                    return Ok(None);
                };
                *index = residual;
                if desc.key.is_some() {
                    return Err(ReaderError::EncryptedChild);
                }
                match decode_at::<S, F>(store, &desc.address).await? {
                    DecodedChunk::Leaf(table) => return Ok(Some(ranked(&table))),
                    DecodedChunk::Directory(inner) => dir = inner,
                    _ => return Err(segment_context()),
                }
            }
            Err(segment_context())
        }
        DecodedChunk::Leaf(_) | DecodedChunk::Directory(_) => Err(segment_context()),
    }
}

/// A malformed-segment decode error: a reference named a node but a bare segment
/// decoded, or a directory nested past its bound.
const fn segment_context() -> ReaderError {
    ReaderError::Store(StoreError::Decode(DecodeError::SegmentContext))
}

/// The outcome of ranking `remaining` through one chunk's fork-table fragment.
enum Step {
    /// The rank resolves here, adding this many in-fragment keys before it.
    Here(u64),
    /// The target descends into a referenced child at `addr`, `matched` key
    /// bytes below the fragment root, with this many in-fragment keys before it.
    Cross {
        addr: ChunkAddress,
        matched: usize,
        before: u64,
    },
    /// The target descends into an encrypted child that cannot be opened.
    Encrypted,
}

/// Count the keys strictly before `remaining` within one fragment's positions,
/// stopping where the target descends into a referenced child.
///
/// A position wholly before the target adds its whole-subtree count; the one the
/// target funnels into is recursed, not counted; the rest are past the target.
fn rank_fragment<F: Format>(steps: &[Ranked<F>], remaining: &[u8]) -> Step {
    let mut before = 0u64;
    for step in steps {
        let suffix = step.suffix();
        if suffix >= remaining {
            return Step::Here(before);
        }
        // `suffix < remaining`: a prefix of it means the target descends here.
        let descends = remaining.starts_with(suffix);
        match step {
            Ranked::Value { .. } => before = before.saturating_add(1),
            Ranked::Ref { addr, count, .. } => {
                if descends {
                    return Step::Cross {
                        addr: *addr,
                        matched: suffix.len(),
                        before,
                    };
                }
                before = before.saturating_add(*count);
            }
            Ranked::Encrypted { count, .. } => {
                if descends {
                    return Step::Encrypted;
                }
                before = before.saturating_add(*count);
            }
        }
    }
    Step::Here(before)
}

/// Where an index lands within one fragment's positions.
enum Reach<F: Format> {
    /// The index is this fragment's value at `suffix`.
    Found { suffix: Vec<u8>, entry: Entry<F> },
    /// The index falls inside the referenced child at `addr`, `suffix` below the
    /// fragment root; the residual index stays in the caller's counter.
    Cross { addr: ChunkAddress, suffix: Vec<u8> },
    /// The index falls inside an encrypted child that cannot be opened.
    Encrypted,
}

/// Resolve `index` within one fragment's positions, subtracting the counts of
/// the positions before it. `None` when the index runs past the fragment total.
fn select_fragment<F: Format>(steps: Vec<Ranked<F>>, index: &mut u64) -> Option<Reach<F>> {
    for step in steps {
        let count = step.count();
        if *index < count {
            return Some(match step {
                Ranked::Value { suffix, entry } => Reach::Found { suffix, entry },
                Ranked::Ref { suffix, addr, .. } => Reach::Cross { addr, suffix },
                Ranked::Encrypted { .. } => Reach::Encrypted,
            });
        }
        *index = index.saturating_sub(count);
    }
    None
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use nectar_primitives::store::MemoryStore;
    use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef, EncryptionKey};

    use crate::bounded::Prefix;
    use crate::count::SubtreeCount;
    use crate::fork::{Child, ForkTable};
    use crate::format::V1;
    use crate::node::{Node, RootExtension};
    use crate::store::NodePut;
    use crate::value::{Entry, Key};

    use super::*;

    fn entry(byte: u8) -> Entry<V1> {
        ChunkRef::new(ChunkAddress::new([byte; 32])).into()
    }

    fn prefix(bytes: &[u8]) -> Prefix<V1> {
        Prefix::try_from(bytes).unwrap()
    }

    #[test]
    fn the_root_entry_is_index_zero_and_lifts_every_rank() {
        let store = MemoryStore::default();
        let root_ext = RootExtension::new(Some(entry(9)), None);
        let mut forks = ForkTable::new();
        forks.insert(prefix(b"k"), entry(1).into(), None).unwrap();
        let root = block_on(store.put_node(&Node::<V1>::new(root_ext, forks))).unwrap();
        let reader = Reader::<&MemoryStore, V1>::new(&store);

        // The empty key leads iteration at index 0; "k" follows at index 1.
        assert_eq!(
            block_on(reader.select(&root, 0)).unwrap(),
            Some((Key::empty(), entry(9)))
        );
        assert_eq!(
            block_on(reader.select(&root, 1)).unwrap(),
            Some((Key::from(&b"k"[..]), entry(1)))
        );
        assert_eq!(block_on(reader.select(&root, 2)).unwrap(), None);

        // Nothing is strictly before the empty key; the root entry sits before
        // every other key, so it lifts their ranks by one.
        assert_eq!(block_on(reader.rank(&root, &Key::empty())).unwrap(), 0);
        assert_eq!(
            block_on(reader.rank(&root, &Key::from(&b"k"[..]))).unwrap(),
            1
        );
        assert_eq!(
            block_on(reader.rank(&root, &Key::from(&b"z"[..]))).unwrap(),
            2
        );
        assert_eq!(
            block_on(reader.count(&root, &Key::empty(), &Key::from(&b"z"[..]))).unwrap(),
            2
        );
    }

    // A root holding "a" and "z" as plain values with an encrypted five-key
    // subtree wedged between them under "m"; the count rides the fork record.
    fn with_encrypted(store: &MemoryStore) -> ChunkAddress {
        let mut forks = ForkTable::new();
        forks
            .insert(prefix(b"a"), entry(0xA1).into(), None)
            .unwrap();
        forks
            .insert(
                prefix(b"m"),
                Child::Ref64(EncryptedChunkRef::new(
                    ChunkAddress::new([0x4D; 32]),
                    EncryptionKey::from([0xC7; 32]),
                ))
                .into(),
                None,
            )
            .unwrap();
        forks
            .get_mut(b'm')
            .unwrap()
            .set_child_count(Some(SubtreeCount::new(5)));
        forks
            .insert(prefix(b"z"), entry(0x2C).into(), None)
            .unwrap();
        block_on(store.put_node(&Node::<V1>::new(None, forks))).unwrap()
    }

    #[test]
    fn an_encrypted_subtree_is_counted_without_being_opened() {
        let store = MemoryStore::default();
        let root = with_encrypted(&store);
        let reader = Reader::<&MemoryStore, V1>::new(&store);

        // Ranking past the encrypted subtree adds its stored count: "a", then its
        // five keys, all strictly before "z".
        assert_eq!(
            block_on(reader.rank(&root, &Key::from(&b"z"[..]))).unwrap(),
            6
        );
        // A key at the encrypted prefix does not descend; its keys are longer.
        assert_eq!(
            block_on(reader.rank(&root, &Key::from(&b"m"[..]))).unwrap(),
            1
        );
        // Selecting the key just past the subtree skips all five by count.
        assert_eq!(
            block_on(reader.select(&root, 6)).unwrap(),
            Some((Key::from(&b"z"[..]), entry(0x2C)))
        );
    }

    #[test]
    fn descending_into_an_encrypted_subtree_is_an_error() {
        let store = MemoryStore::default();
        let root = with_encrypted(&store);
        let reader = Reader::<&MemoryStore, V1>::new(&store);

        // A rank whose key funnels into the encrypted subtree cannot be answered.
        assert!(matches!(
            block_on(reader.rank(&root, &Key::from(&b"mx"[..]))),
            Err(ReaderError::EncryptedChild)
        ));
        // An index landing inside the encrypted subtree cannot be opened.
        assert!(matches!(
            block_on(reader.select(&root, 3)),
            Err(ReaderError::EncryptedChild)
        ));
    }

    #[test]
    fn an_empty_manifest_has_no_keys() {
        let store = MemoryStore::default();
        let root = block_on(store.put_node(&Node::<V1>::empty())).unwrap();
        let reader = Reader::<&MemoryStore, V1>::new(&store);
        assert_eq!(
            block_on(reader.rank(&root, &Key::from(&b"x"[..]))).unwrap(),
            0
        );
        assert_eq!(block_on(reader.select(&root, 0)).unwrap(), None);
        let mut cursor = block_on(reader.paginate_prefix(&root, &Key::empty(), 0, 10)).unwrap();
        assert!(block_on(cursor.next()).unwrap().is_none());
    }
}
