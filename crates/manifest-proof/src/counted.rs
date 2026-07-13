//! Authenticated rank, count, select and pagination proofs over the counted
//! node grammar.
//!
//! The node grammar carries every subtree's distinct key-count in the
//! authenticated node bytes (spec 5.6), so an order-statistic answer is provable
//! with the same O(depth) descent the reader walks: a [rank](prove_rank) or
//! [select](prove_select) proof is the chain of nodes the descent visits, each
//! authenticated against the reference the one before it yielded, and the
//! verifier re-derives the answer from those bytes alone. A [count](prove_count)
//! proof is two rank descents whose difference is the window size; a
//! [page](prove_page) proof pins a listing slice by its absolute ranks.
//!
//! TRUST BOUNDARY - counted proofs assume an HONEST BUILDER. A referenced
//! child's `child_count` is bound to its parent chunk's bytes but NOT to the
//! child's real subtree: re-encoding a chunk reproduces the same count, it does
//! not re-derive it from the child, and an inflated count stays internally
//! consistent all the way to the root. So these proofs establish the count as
//! COMMITTED BY THE ROOT - sound against an honest builder (a gateway, indexer
//! or registry operator whose manifest you already rely on), NOT against an
//! adversarial root that plants false counts on the subtrees a descent skips.
//! The strictly trustless answers ride the count-independent inclusion,
//! exclusion and range-completeness proofs, which are sound against adversarial
//! roots by canonicalness.
//!
//! The residual trust is bounded where free: every hop the descent DOES take
//! fetches the child, so the verifier cross-checks the parent-asserted
//! `child_count` against the child's own derived total (the sum of its fork
//! counts) and rejects an on-path inconsistency. Only the counts of the
//! un-fetched siblings a descent skips remain purely author-asserted.

use nectar_manifest::{Entry, Format, Key, SubtreeCount};
use nectar_primitives::wire::Cursor;
use nectar_primitives::{
    ChunkAddress, ChunkOps, ChunkRef, ContentChunk, DEFAULT_BODY_SIZE, EncryptedChunkRef,
};

use crate::descent::{DescentError, Width, child_width, entry_width, flag, take_u16, take_value};
use crate::error::{ProveError, VerifyError};
use crate::prove::NodeSource;

/// An authenticated chain of node payloads: the nodes one counted descent
/// visits, root first, each addressed by the BMT of its bytes.
///
/// Carries no answer of its own; the verifier re-derives it from the
/// authenticated bytes, so the chain cannot assert a count its nodes do not.
#[derive(Clone, Debug)]
pub struct CountedPath {
    nodes: Vec<Vec<u8>>,
}

impl CountedPath {
    /// Assemble a path from its ordered node payloads, root first.
    #[must_use]
    pub const fn new(nodes: Vec<Vec<u8>>) -> Self {
        Self { nodes }
    }

    /// The node payloads in descent order, root first.
    #[must_use]
    pub fn nodes(&self) -> &[Vec<u8>] {
        &self.nodes
    }

    /// The number of authenticated nodes on the path.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the path carries no nodes; never true for one a verify accepts.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

/// A proof of `rank(key)`: the authenticated descent path to the key's position.
#[derive(Clone, Debug)]
pub struct RankProof {
    path: CountedPath,
}

impl RankProof {
    /// Assemble a rank proof from its descent path.
    #[must_use]
    pub const fn new(path: CountedPath) -> Self {
        Self { path }
    }

    /// The authenticated rank-descent path.
    #[must_use]
    pub const fn path(&self) -> &CountedPath {
        &self.path
    }
}

/// A proof of `count(lo, hi)`: the two rank descents whose difference is the
/// number of keys in the half-open window.
#[derive(Clone, Debug)]
pub struct CountProof {
    lo: CountedPath,
    hi: CountedPath,
}

impl CountProof {
    /// Assemble a count proof from the `lo` and `hi` rank descents.
    #[must_use]
    pub const fn new(lo: CountedPath, hi: CountedPath) -> Self {
        Self { lo, hi }
    }

    /// The rank descent to the lower bound.
    #[must_use]
    pub const fn lo(&self) -> &CountedPath {
        &self.lo
    }

    /// The rank descent to the upper bound.
    #[must_use]
    pub const fn hi(&self) -> &CountedPath {
        &self.hi
    }
}

/// A proof of `select(index)`: the authenticated descent to the key at a rank.
#[derive(Clone, Debug)]
pub struct SelectProof {
    path: CountedPath,
}

impl SelectProof {
    /// Assemble a select proof from its descent path.
    #[must_use]
    pub const fn new(path: CountedPath) -> Self {
        Self { path }
    }

    /// The authenticated select-descent path.
    #[must_use]
    pub const fn path(&self) -> &CountedPath {
        &self.path
    }
}

/// A proof of one pagination slice: the rank descents bounding the window and a
/// rank-pinned select descent for each returned key.
///
/// The upper bound is `None` for an unbounded prefix page (the empty or
/// all-`0xFF` prefix), whose total is read from the authenticated root.
#[derive(Clone, Debug)]
pub struct PageProof {
    lo: CountedPath,
    hi: Option<CountedPath>,
    entries: Vec<CountedPath>,
}

impl PageProof {
    /// Assemble a page proof from its bound descents and per-key select
    /// descents.
    #[must_use]
    pub const fn new(lo: CountedPath, hi: Option<CountedPath>, entries: Vec<CountedPath>) -> Self {
        Self { lo, hi, entries }
    }

    /// The rank descent to the lower bound.
    #[must_use]
    pub const fn lo(&self) -> &CountedPath {
        &self.lo
    }

    /// The rank descent to the upper bound, absent for an unbounded page.
    #[must_use]
    pub const fn hi(&self) -> Option<&CountedPath> {
        self.hi.as_ref()
    }

    /// The rank-pinned select descents, one per returned key.
    #[must_use]
    pub fn entries(&self) -> &[CountedPath] {
        &self.entries
    }
}

/// One flattened fork-table position, in ascending key order, with the count of
/// keys it stands for: a value counts once, a referenced or encrypted child for
/// its whole author-asserted subtree.
enum Pos<F: Format> {
    /// A key terminates here with this value.
    Value {
        /// Key bytes below the node root.
        suffix: Vec<u8>,
        /// The bound value.
        entry: Entry<F>,
    },
    /// A referenced child holding `count` keys.
    Ref {
        /// Key bytes below the node root leading to the child.
        suffix: Vec<u8>,
        /// The child chunk address.
        addr: ChunkAddress,
        /// The child subtree's asserted distinct key-count.
        count: u64,
    },
    /// An encrypted child holding `count` keys; its count still routes a rank
    /// past it, only a descent into it fails.
    Encrypted {
        /// Key bytes below the node root leading to the child.
        suffix: Vec<u8>,
        /// The child subtree's asserted distinct key-count.
        count: u64,
    },
}

impl<F: Format> Pos<F> {
    /// The key bytes below the node root.
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

/// A node flattened to its root entry and ascending-key positions, embedded
/// children folded in place so only referenced hops leave the buffer.
struct Flat<F: Format> {
    root_entry: Option<Entry<F>>,
    positions: Vec<Pos<F>>,
}

impl<F: Format> Flat<F> {
    /// The node's derived total: the summed counts of its positions. A
    /// referenced child node carries no root entry, so this is exactly the count
    /// its parent asserts for it, and the two must agree.
    fn derived_total(&self) -> u64 {
        self.positions
            .iter()
            .map(Pos::count)
            .fold(0, u64::saturating_add)
    }
}

/// Parse an authenticated node payload into its flattened positions, reading the
/// trailing `child_count` on every referenced fork.
///
/// Rejects a spilled node: a proof over a segment directory is out of scope, the
/// same bound the single-key descent keeps.
fn flatten<F: Format>(bytes: &[u8]) -> Result<Flat<F>, DescentError> {
    let mut cur = Cursor::new(bytes);
    if cur.take::<[u8; 2]>()? != F::PREAMBLE {
        return Err(DescentError::NotAManifest);
    }
    let flags = cur.take::<u8>()?;
    if flags & (flag::SEGMENT | flag::SEGMENTED) != 0 {
        return Err(DescentError::Spilled);
    }
    let root_entry = take_value::<F>(&mut cur, entry_width(flags))?;
    if flags & flag::HAS_META != 0 {
        let len = take_u16(&mut cur)?;
        let _ = cur.take_slice(len)?;
    }
    let mut positions = Vec::new();
    let mut prefix = Vec::new();
    read_table::<F>(&mut cur, &mut prefix, &mut positions)?;
    Ok(Flat {
        root_entry,
        positions,
    })
}

/// Read a fork table at the cursor: the count, the index of first bytes, then
/// each record parsed in place and appended as ascending-key positions.
fn read_table<F: Format>(
    cur: &mut Cursor<'_>,
    prefix: &mut Vec<u8>,
    positions: &mut Vec<Pos<F>>,
) -> Result<(), DescentError> {
    let fcount = take_u16(cur)?;
    let mut firsts = Vec::with_capacity(fcount);
    for _ in 0..fcount {
        let first = cur.take::<u8>()?;
        let _off = take_u16(cur)?;
        firsts.push(first);
    }
    for first in firsts {
        read_record::<F>(cur, first, prefix, positions)?;
    }
    Ok(())
}

/// Parse one fork record in wire order (flags, tail, entry, child, metadata,
/// trailing count), appending its value and its child's positions.
fn read_record<F: Format>(
    cur: &mut Cursor<'_>,
    first: u8,
    prefix: &mut Vec<u8>,
    positions: &mut Vec<Pos<F>>,
) -> Result<(), DescentError> {
    let flags = cur.take::<u8>()?;
    let plen = usize::from(cur.take::<u8>()?);
    let tail_len = plen.checked_sub(1).ok_or(DescentError::EmptyPrefix)?;
    let tail = cur.take_slice(tail_len)?;
    let mark = prefix.len();
    prefix.push(first);
    prefix.extend_from_slice(tail);

    if let Some(entry) = take_value::<F>(cur, entry_width(flags))? {
        positions.push(Pos::Value {
            suffix: prefix.clone(),
            entry,
        });
    }

    // The child field. A referenced child defers its position until its trailing
    // count is read; an embedded child folds its own positions in place now.
    let width = child_width(flags);
    let referenced = matches!(width, Width::Ref32 | Width::Ref64);
    let deferred = match width {
        Width::None => None,
        Width::Ref32 => {
            let raw = cur.take::<[u8; ChunkRef::SIZE]>()?;
            Some(Deferred::Ref(ChunkAddress::new(raw)))
        }
        Width::Ref64 => {
            let _raw = cur.take::<[u8; EncryptedChunkRef::SIZE]>()?;
            Some(Deferred::Encrypted)
        }
        Width::Inline => {
            let _len = take_u16(cur)?;
            let _zero = cur.take::<u8>()?;
            read_table::<F>(cur, prefix, positions)?;
            None
        }
    };

    if flags & flag::HAS_META != 0 {
        let len = take_u16(cur)?;
        let _ = cur.take_slice(len)?;
    }
    let count = if referenced {
        cur.take::<SubtreeCount>()?.get()
    } else {
        0
    };
    match deferred {
        Some(Deferred::Ref(addr)) => positions.push(Pos::Ref {
            suffix: prefix.clone(),
            addr,
            count,
        }),
        Some(Deferred::Encrypted) => positions.push(Pos::Encrypted {
            suffix: prefix.clone(),
            count,
        }),
        None => {}
    }
    prefix.truncate(mark);
    Ok(())
}

/// A referenced child parsed before its trailing count, held until the count is
/// read so its position lands with the right count.
enum Deferred {
    /// A plain reference at this address.
    Ref(ChunkAddress),
    /// An encrypted reference the plain descent cannot open.
    Encrypted,
}

/// Where ranking the target through one node's positions lands.
enum RankStep {
    /// The rank resolves in this node, with this many in-node keys before it.
    Here(u64),
    /// The target descends into a referenced child, `matched` key bytes below
    /// the node root, with this many in-node keys strictly before it.
    Cross {
        /// The child chunk address.
        addr: ChunkAddress,
        /// Key bytes consumed reaching the child.
        matched: usize,
        /// In-node keys strictly before the target.
        before: u64,
        /// The child's asserted count, cross-checked against the child node.
        count: u64,
    },
    /// The target descends into an encrypted child that cannot be opened.
    Encrypted,
}

/// Count the keys strictly before `remaining` within one node's positions,
/// stopping where the target descends into a referenced child.
fn rank_step<F: Format>(positions: &[Pos<F>], remaining: &[u8]) -> RankStep {
    let mut before = 0u64;
    for pos in positions {
        let suffix = pos.suffix();
        if suffix >= remaining {
            return RankStep::Here(before);
        }
        let descends = remaining.starts_with(suffix);
        match pos {
            Pos::Value { .. } => before = before.saturating_add(1),
            Pos::Ref { addr, count, .. } => {
                if descends {
                    return RankStep::Cross {
                        addr: *addr,
                        matched: suffix.len(),
                        before,
                        count: *count,
                    };
                }
                before = before.saturating_add(*count);
            }
            Pos::Encrypted { count, .. } => {
                if descends {
                    return RankStep::Encrypted;
                }
                before = before.saturating_add(*count);
            }
        }
    }
    RankStep::Here(before)
}

/// Where an absolute index lands within one node's positions.
enum SelectStep<F: Format> {
    /// The index is this node's value at `suffix`.
    Found {
        /// Key bytes below the node root.
        suffix: Vec<u8>,
        /// The resolved value.
        entry: Entry<F>,
    },
    /// The index falls inside the referenced child at `addr`, `suffix` below the
    /// node root, with `residual` its index within the child.
    Cross {
        /// The child chunk address.
        addr: ChunkAddress,
        /// Key bytes consumed reaching the child.
        suffix: Vec<u8>,
        /// The index within the child subtree.
        residual: u64,
        /// The child's asserted count, cross-checked against the child node.
        count: u64,
    },
    /// The index falls inside an encrypted child that cannot be opened.
    Encrypted,
    /// The index runs past this node's total.
    Past,
}

/// Resolve `index` within one node's positions, subtracting the counts of the
/// positions before it.
fn select_step<F: Format>(positions: Vec<Pos<F>>, index: u64) -> SelectStep<F> {
    let mut index = index;
    for pos in positions {
        let count = pos.count();
        if index < count {
            return match pos {
                Pos::Value { suffix, entry } => SelectStep::Found { suffix, entry },
                Pos::Ref {
                    suffix,
                    addr,
                    count,
                } => SelectStep::Cross {
                    addr,
                    suffix,
                    residual: index,
                    count,
                },
                Pos::Encrypted { .. } => SelectStep::Encrypted,
            };
        }
        index = index.saturating_sub(count);
    }
    SelectStep::Past
}

/// Authenticate a node payload against `trusted`, then flatten it. The seal
/// re-BMTs the bytes, so a tampered node or wrong reference fails here.
fn authenticated<F: Format>(
    payload: &[u8],
    trusted: &ChunkAddress,
    index: usize,
) -> Result<Flat<F>, VerifyError> {
    let chunk =
        ContentChunk::<DEFAULT_BODY_SIZE>::new(payload.to_vec()).map_err(VerifyError::Seal)?;
    if chunk.address() != trusted {
        return Err(VerifyError::Unauthenticated(index));
    }
    Ok(flatten::<F>(payload)?)
}

/// Descend the manifest for `key`, recording each visited node payload and
/// accumulating the rank the descent proves.
fn rank_walk<F, S>(
    source: &S,
    root: &ChunkAddress,
    key: &Key,
) -> Result<(Vec<Vec<u8>>, u64), ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let target = key.as_bytes();
    let mut nodes = Vec::new();
    let mut address = *root;
    let mut consumed = 0usize;
    let mut acc = 0u64;
    let mut is_root = true;
    loop {
        let node = source
            .node(&address)
            .ok_or(ProveError::NodeMissing(address))?;
        let payload = node.encode()?;
        let flat = flatten::<F>(&payload)?;
        nodes.push(payload);
        if is_root && !target.is_empty() && flat.root_entry.is_some() {
            acc = acc.saturating_add(1);
        }
        let Some(remaining) = target.get(consumed..).filter(|rest| !rest.is_empty()) else {
            return Ok((nodes, acc));
        };
        match rank_step(&flat.positions, remaining) {
            RankStep::Here(before) => return Ok((nodes, acc.saturating_add(before))),
            RankStep::Encrypted => return Err(ProveError::Encrypted),
            RankStep::Cross {
                addr,
                matched,
                before,
                ..
            } => {
                acc = acc.saturating_add(before);
                consumed = consumed.saturating_add(matched);
                address = addr;
                is_root = false;
            }
        }
    }
}

/// Replay a rank descent over authenticated bytes, cross-checking every on-path
/// `child_count` against the child node's derived total.
fn replay_rank<F: Format>(
    root: &ChunkAddress,
    key: &Key,
    path: &CountedPath,
) -> Result<u64, VerifyError> {
    let target = key.as_bytes();
    let mut acc = 0u64;
    let mut consumed = 0usize;
    let mut trusted = *root;
    let mut is_root = true;
    let mut expected: Option<u64> = None;
    for (index, payload) in path.nodes().iter().enumerate() {
        let flat = authenticated::<F>(payload, &trusted, index)?;
        if let Some(want) = expected
            && flat.derived_total() != want
        {
            return Err(VerifyError::CountMismatch);
        }
        if is_root && !target.is_empty() && flat.root_entry.is_some() {
            acc = acc.saturating_add(1);
        }
        let Some(remaining) = target.get(consumed..).filter(|rest| !rest.is_empty()) else {
            return Ok(acc);
        };
        match rank_step(&flat.positions, remaining) {
            RankStep::Here(before) => return Ok(acc.saturating_add(before)),
            RankStep::Encrypted => return Err(VerifyError::Encrypted),
            RankStep::Cross {
                addr,
                matched,
                before,
                count,
            } => {
                acc = acc.saturating_add(before);
                consumed = consumed.saturating_add(matched);
                trusted = addr;
                is_root = false;
                expected = Some(count);
            }
        }
    }
    Err(VerifyError::Malformed)
}

/// Descend the manifest for absolute rank `index`, recording each visited node.
/// `None` when the index runs past the manifest's key count.
fn select_walk<F, S>(
    source: &S,
    root: &ChunkAddress,
    index: u64,
) -> Result<Option<Vec<Vec<u8>>>, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let mut nodes = Vec::new();
    let mut address = *root;
    let mut index = index;
    let mut is_root = true;
    loop {
        let node = source
            .node(&address)
            .ok_or(ProveError::NodeMissing(address))?;
        let payload = node.encode()?;
        let flat = flatten::<F>(&payload)?;
        nodes.push(payload);
        if is_root && flat.root_entry.is_some() {
            if index == 0 {
                return Ok(Some(nodes));
            }
            index = index.saturating_sub(1);
        }
        match select_step(flat.positions, index) {
            SelectStep::Found { .. } => return Ok(Some(nodes)),
            SelectStep::Encrypted => return Err(ProveError::Encrypted),
            SelectStep::Past => return Ok(None),
            SelectStep::Cross { addr, residual, .. } => {
                index = residual;
                address = addr;
                is_root = false;
            }
        }
    }
}

/// Replay a select descent over authenticated bytes, cross-checking every
/// on-path `child_count` and returning the resolved key and value.
fn replay_select<F: Format>(
    root: &ChunkAddress,
    index: u64,
    path: &CountedPath,
) -> Result<(Key, Entry<F>), VerifyError> {
    let mut trusted = *root;
    let mut index = index;
    let mut key = Vec::new();
    let mut is_root = true;
    let mut expected: Option<u64> = None;
    for (step, payload) in path.nodes().iter().enumerate() {
        let flat = authenticated::<F>(payload, &trusted, step)?;
        if let Some(want) = expected
            && flat.derived_total() != want
        {
            return Err(VerifyError::CountMismatch);
        }
        if is_root && let Some(entry) = &flat.root_entry {
            if index == 0 {
                return Ok((Key::from(key.as_slice()), entry.clone()));
            }
            index = index.saturating_sub(1);
        }
        match select_step(flat.positions, index) {
            SelectStep::Found { suffix, entry } => {
                key.extend_from_slice(&suffix);
                return Ok((Key::from(key.as_slice()), entry));
            }
            SelectStep::Encrypted => return Err(VerifyError::Encrypted),
            SelectStep::Past => return Err(VerifyError::IndexOutOfRange),
            SelectStep::Cross {
                addr,
                suffix,
                residual,
                count,
            } => {
                key.extend_from_slice(&suffix);
                index = residual;
                trusted = addr;
                is_root = false;
                expected = Some(count);
            }
        }
    }
    Err(VerifyError::Malformed)
}

/// The whole manifest's key count from the authenticated root: its derived total
/// plus its own empty-key entry.
fn whole_count<F: Format>(root: &ChunkAddress, path: &CountedPath) -> Result<u64, VerifyError> {
    let payload = path.nodes().first().ok_or(VerifyError::Empty)?;
    let flat = authenticated::<F>(payload, root, 0)?;
    let entry = u64::from(flat.root_entry.is_some());
    Ok(flat.derived_total().saturating_add(entry))
}

/// The least byte string strictly greater than every string starting with
/// `prefix`. `None` when the prefix is empty or all `0xFF`, i.e. unbounded.
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bytes = prefix.to_vec();
    loop {
        match bytes.last() {
            None => return None,
            Some(&0xFF) => {
                bytes.pop();
            }
            Some(&last) => {
                if let Some(slot) = bytes.last_mut() {
                    *slot = last.saturating_add(1);
                }
                return Some(bytes);
            }
        }
    }
}

/// Prove `rank(key)`: the number of keys strictly less than `key`, under `root`.
///
/// The proof is the O(depth) descent path; [`verify_rank`] re-derives the rank
/// and cross-checks every on-path count. See the [module](self) trust boundary:
/// the rank is sound relative to the committed root, under an honest builder.
pub fn prove_rank<F, S>(source: &S, root: &ChunkAddress, key: &Key) -> Result<RankProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let (nodes, _) = rank_walk::<F, S>(source, root, key)?;
    Ok(RankProof::new(CountedPath::new(nodes)))
}

/// Verify a [`prove_rank`] proof, returning the authenticated rank.
///
/// Rejects an on-path count inconsistency ([`VerifyError::CountMismatch`]); the
/// counts of skipped siblings stay author-asserted (see the [module](self)
/// trust boundary).
pub fn verify_rank<F: Format>(
    root: &ChunkAddress,
    key: &Key,
    proof: &RankProof,
) -> Result<u64, VerifyError> {
    replay_rank::<F>(root, key, proof.path())
}

/// Prove `count(lo, hi)`: the number of keys with `lo <= key < hi`, under
/// `root`, as the two rank descents whose difference is the window size.
///
/// See the [module](self) trust boundary: the count is sound relative to the
/// committed root, under an honest builder.
pub fn prove_count<F, S>(
    source: &S,
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
) -> Result<CountProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let (lo_nodes, _) = rank_walk::<F, S>(source, root, lo)?;
    let (hi_nodes, _) = rank_walk::<F, S>(source, root, hi)?;
    Ok(CountProof::new(
        CountedPath::new(lo_nodes),
        CountedPath::new(hi_nodes),
    ))
}

/// Verify a [`prove_count`] proof, returning `rank(hi) - rank(lo)`.
///
/// Both descents are cross-checked; an on-path inconsistency in either is
/// rejected.
pub fn verify_count<F: Format>(
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
    proof: &CountProof,
) -> Result<u64, VerifyError> {
    let low = replay_rank::<F>(root, lo, proof.lo())?;
    let high = replay_rank::<F>(root, hi, proof.hi())?;
    Ok(high.saturating_sub(low))
}

/// Prove `select(index)`: the key and value at position `index` in ascending key
/// order, under `root`.
///
/// Errors with [`ProveError::IndexOutOfRange`] when the index is at or past the
/// key count. See the [module](self) trust boundary.
pub fn prove_select<F, S>(
    source: &S,
    root: &ChunkAddress,
    index: u64,
) -> Result<SelectProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    select_walk::<F, S>(source, root, index)?.map_or(Err(ProveError::IndexOutOfRange), |nodes| {
        Ok(SelectProof::new(CountedPath::new(nodes)))
    })
}

/// Verify a [`prove_select`] proof, returning the authenticated key and value.
///
/// Rejects an on-path count inconsistency; the position is sound relative to the
/// committed root, under an honest builder.
pub fn verify_select<F: Format>(
    root: &ChunkAddress,
    index: u64,
    proof: &SelectProof,
) -> Result<(Key, Entry<F>), VerifyError> {
    replay_select::<F>(root, index, proof.path())
}

/// Assemble a page proof over `[lo, hi)` (an unbounded upper bound when `hi` is
/// `None`), skipping `offset` keys and returning at most `limit`.
fn page_core<F, S>(
    source: &S,
    root: &ChunkAddress,
    lo: &Key,
    hi: Option<&Key>,
    offset: u64,
    limit: usize,
) -> Result<PageProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let (lo_nodes, rank_lo) = rank_walk::<F, S>(source, root, lo)?;
    let (hi_path, rank_hi) = match hi {
        Some(key) => {
            let (nodes, rank) = rank_walk::<F, S>(source, root, key)?;
            (Some(CountedPath::new(nodes)), rank)
        }
        None => (None, whole_count_prove::<F, S>(source, root)?),
    };
    let total = rank_hi.saturating_sub(rank_lo);
    let take = u64::try_from(limit)
        .unwrap_or(u64::MAX)
        .min(total.saturating_sub(offset));
    let start = rank_lo.saturating_add(offset);
    let mut entries = Vec::new();
    let mut i = 0u64;
    while i < take {
        match select_walk::<F, S>(source, root, start.saturating_add(i))? {
            Some(nodes) => entries.push(CountedPath::new(nodes)),
            None => break,
        }
        i = i.saturating_add(1);
    }
    Ok(PageProof::new(CountedPath::new(lo_nodes), hi_path, entries))
}

/// The whole manifest's key count, walked from the fetched root.
fn whole_count_prove<F, S>(source: &S, root: &ChunkAddress) -> Result<u64, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    let node = source.node(root).ok_or(ProveError::NodeMissing(*root))?;
    let payload = node.encode()?;
    let flat = flatten::<F>(&payload)?;
    let entry = u64::from(flat.root_entry.is_some());
    Ok(flat.derived_total().saturating_add(entry))
}

/// Verify a page proof over `[lo, hi)`, returning the authenticated slice.
fn verify_page_core<F: Format>(
    root: &ChunkAddress,
    lo: &Key,
    hi: Option<&Key>,
    offset: u64,
    limit: usize,
    proof: &PageProof,
) -> Result<Vec<(Key, Entry<F>)>, VerifyError> {
    let rank_lo = replay_rank::<F>(root, lo, proof.lo())?;
    let rank_hi = match (hi, proof.hi()) {
        (Some(key), Some(path)) => replay_rank::<F>(root, key, path)?,
        (None, None) => whole_count::<F>(root, proof.lo())?,
        _ => return Err(VerifyError::PageShape),
    };
    let total = rank_hi.saturating_sub(rank_lo);
    let expected = u64::try_from(limit)
        .unwrap_or(u64::MAX)
        .min(total.saturating_sub(offset));
    let entries = proof.entries();
    if u64::try_from(entries.len()).unwrap_or(u64::MAX) != expected {
        return Err(VerifyError::PageShape);
    }
    let start = rank_lo.saturating_add(offset);
    let lo_bytes = lo.as_bytes();
    let hi_bytes = hi.map(Key::as_bytes);
    let mut out = Vec::new();
    let mut previous: Option<Vec<u8>> = None;
    for (i, path) in entries.iter().enumerate() {
        let rank = start.saturating_add(u64::try_from(i).unwrap_or(u64::MAX));
        let (key, entry) = replay_select::<F>(root, rank, path)?;
        let bytes = key.as_bytes();
        // Each key must sit in the window and strictly after the last: the slice
        // is the ordered range page, not an unordered set.
        if bytes < lo_bytes || hi_bytes.is_some_and(|hi| bytes >= hi) {
            return Err(VerifyError::PageShape);
        }
        if previous.as_deref().is_some_and(|prev| bytes <= prev) {
            return Err(VerifyError::PageShape);
        }
        previous = Some(bytes.to_vec());
        out.push((key, entry));
    }
    Ok(out)
}

/// Prove a pagination slice over the half-open range `[lo, hi)`: skip `offset`
/// keys and return at most `limit`.
///
/// The proof is the rank descents bounding the window plus a rank-pinned select
/// descent for each returned key, so paging deep costs O(depth * limit). See the
/// [module](self) trust boundary.
pub fn prove_page<F, S>(
    source: &S,
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
    offset: u64,
    limit: usize,
) -> Result<PageProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    page_core::<F, S>(source, root, lo, Some(hi), offset, limit)
}

/// Verify a [`prove_page`] proof over `[lo, hi)`, returning the authenticated
/// slice.
///
/// Rejects a slice whose length, ordering or bounds disagree with the proven
/// window size ([`VerifyError::PageShape`]), or an on-path count inconsistency.
pub fn verify_page<F: Format>(
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
    offset: u64,
    limit: usize,
    proof: &PageProof,
) -> Result<Vec<(Key, Entry<F>)>, VerifyError> {
    verify_page_core::<F>(root, lo, Some(hi), offset, limit, proof)
}

/// Prove a pagination slice over the keys carrying `prefix`: skip `offset` keys
/// and return at most `limit`. The empty prefix pages the whole manifest.
///
/// See the [module](self) trust boundary.
pub fn prove_page_prefix<F, S>(
    source: &S,
    root: &ChunkAddress,
    prefix: &Key,
    offset: u64,
    limit: usize,
) -> Result<PageProof, ProveError>
where
    F: Format,
    S: NodeSource<F>,
{
    successor(prefix.as_bytes()).map_or_else(
        || page_core::<F, S>(source, root, prefix, None, offset, limit),
        |bytes| {
            page_core::<F, S>(
                source,
                root,
                prefix,
                Some(&Key::from(bytes.as_slice())),
                offset,
                limit,
            )
        },
    )
}

/// Verify a [`prove_page_prefix`] proof over the keys carrying `prefix`,
/// returning the authenticated slice.
pub fn verify_page_prefix<F: Format>(
    root: &ChunkAddress,
    prefix: &Key,
    offset: u64,
    limit: usize,
    proof: &PageProof,
) -> Result<Vec<(Key, Entry<F>)>, VerifyError> {
    successor(prefix.as_bytes()).map_or_else(
        || verify_page_core::<F>(root, prefix, None, offset, limit, proof),
        |bytes| {
            verify_page_core::<F>(
                root,
                prefix,
                Some(&Key::from(bytes.as_slice())),
                offset,
                limit,
                proof,
            )
        },
    )
}
