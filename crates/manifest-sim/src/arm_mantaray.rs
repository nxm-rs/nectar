//! The mantaray 0.2 arm: [`MantarayArm`] over the production
//! `nectar_loadsave::NodeLoadSaver` seam, so a node of one chunk keeps the
//! content-chunk address and a node above one chunk spans several through the
//! file pipeline. Every chunk get behind a node load is counted, so a
//! multi-chunk 0.2 node is charged its true fetch count.
//!
//! [`SharedCounting`] is the one store instance the arm owns: it is an
//! `Arc<CountingStore<AnyChunkSet>>` with the get/put/has traits forwarded to
//! the inner store, so the loadsaver seam can clone the handle while every
//! clone shares one set of counters. There is no second store.
//!
//! Point lookup is native; every ordered and listing operation is an honest
//! public-API emulation over the crate in tree, and the emulation label rides
//! each measurement. The floor/ceiling asymmetry (a forward `after` seek
//! serves ceiling but not floor) is itself a rendered finding.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nectar_testing::run;

use nectar_loadsave::NodeLoadSaver;
use nectar_mantaray::{Cursor, ManifestEditor, Reader};
use nectar_primitives::AnyChunkSet;
use nectar_primitives::chunk::{Chunk, ChunkAddress, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, ChunkPut, MemoryStore};

use crate::arm::{Arm, BatchMode, BuildReport, Capability, Err, FrontierClass, OpCost, OpOutcome};
use crate::corpus::{GenKey, KeyStreamHasher, tagged_addr, value_addr};
use crate::store::{Counters, CountingStore};

// Emulation labels, shared with the capability matrix so the rendered class of
// each op and the class each measurement carries never drift apart.
pub(crate) const FLOOR_HOW: &str = "ordered walk to rank";
pub(crate) const FLOOR_CLASS: &str = "O(rank(k)) fetches, worst O(N)";
pub(crate) const CEILING_HOW: &str = "after-bound pruned seek";
pub(crate) const CEILING_CLASS: &str = "O(depth + window)";
pub(crate) const RANGE_HOW: &str = "after-bound seek + ordered drain";
pub(crate) const RANGE_CLASS: &str = "O(depth + window nodes)";
pub(crate) const RANGE_PESSIMAL_HOW: &str = "full entries walk + range filter";
pub(crate) const PESSIMAL_CLASS: &str = "O(N)";
pub(crate) const PREFIX_HOW: &str = "pruned prefix walk";
pub(crate) const PREFIX_CLASS: &str = "O(depth + subtree nodes)";
pub(crate) const PREFIX_PESSIMAL_HOW: &str = "full entries walk";
pub(crate) const FULL_ITER_HOW: &str = "trie DFS stream, documented path order";
pub(crate) const FULL_ITER_CLASS: &str = "O(N) fetches, O(depth + window) memory";
pub(crate) const BATCH_HOW: &str = "multi-op editor commit";
pub(crate) const BATCH_CLASS: &str = "O(touched spine), whole-trie RAM";
pub(crate) const RESUME_HOW: &str = "resume-token page walk";
pub(crate) const RESUME_CLASS: &str = "O(offset) fetches";
pub(crate) const INLINE_UNSUPPORTED: &str = "0.2 entries are 32/64-byte references only";
pub(crate) const RECANON_UNSUPPORTED: &str = "no canonical-form guarantee in 0.2";

/// Shared handle so the loadsaver seam can clone while counters stay one.
///
/// The get/put/has traits are forwarded to the inner [`CountingStore`]; the
/// `Arc` is the whole store the arm owns. `NodeLoadSaver`'s loader bound is
/// `S: TrustedGet<AnyChunkSet> + Clone + 'static` and its saver bound is
/// `S: ChunkPut<AnyChunkSet>`; the forwarding impls below satisfy both,
/// resolving the unit-A risk without a second store.
#[derive(Clone, Debug)]
pub struct SharedCounting(Arc<CountingStore<AnyChunkSet>>);

impl SharedCounting {
    /// A shared handle over a fresh, empty counting store.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(CountingStore::new()))
    }

    /// The inner store's counter snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Counters {
        self.0.snapshot()
    }
}

impl Default for SharedCounting {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkGet<AnyChunkSet> for SharedCounting {
    type Trust = Verified;
    type Error = <CountingStore<AnyChunkSet> as ChunkGet<AnyChunkSet>>::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, AnyChunkSet>, Self::Error> {
        ChunkGet::get(&*self.0, address).await
    }
}

impl ChunkPut<AnyChunkSet> for SharedCounting {
    type Error = <CountingStore<AnyChunkSet> as ChunkPut<AnyChunkSet>>::Error;

    async fn put(&self, chunk: Chunk<Verified, AnyChunkSet>) -> Result<(), Self::Error> {
        ChunkPut::put(&*self.0, chunk).await
    }
}

impl ChunkHas for SharedCounting {
    async fn has(&self, address: &ChunkAddress) -> bool {
        ChunkHas::has(&*self.0, address).await
    }
}

/// The production loadsaver over the shared counting store.
type LoadSaver = NodeLoadSaver<SharedCounting>;

/// One pre-materialised put for the timed build: path, reference, metadata.
type PreparedPut<'a> = (&'a [u8], ChunkAddress, Option<BTreeMap<String, String>>);

/// mantaray 0.2 over the same counting semantics, through the production
/// loadsave adapter.
#[derive(Debug)]
pub struct MantarayArm {
    store: SharedCounting,
    loadsaver: LoadSaver,
    root: Option<ChunkAddress>,
    consumed: Option<[u8; 32]>,
}

impl Default for MantarayArm {
    fn default() -> Self {
        Self::new()
    }
}

impl MantarayArm {
    /// An arm over a fresh, empty shared counting store.
    #[must_use]
    pub fn new() -> Self {
        let store = SharedCounting::new();
        let loadsaver = NodeLoadSaver::new(store.clone());
        Self {
            store,
            loadsaver,
            root: None,
            consumed: None,
        }
    }

    /// The arm's built root, or an error before the first build.
    fn root(&self) -> Result<ChunkAddress, Err> {
        self.root.ok_or_else(|| "mantaray arm not built".into())
    }

    /// The metadata map for a key, when the corpus carries a content type.
    fn metadata(k: &GenKey) -> Option<BTreeMap<String, String>> {
        k.content_type.map(|ct| {
            let mut m = BTreeMap::new();
            m.insert("Content-Type".to_string(), ct.to_string());
            m
        })
    }

    /// Record one put into `editor`, with metadata when the corpus carries it.
    fn put(
        editor: &mut ManifestEditor<LoadSaver>,
        path: &[u8],
        reference: ChunkAddress,
        meta: Option<BTreeMap<String, String>>,
    ) {
        match meta {
            Some(m) => {
                editor.put_with_metadata(path, reference, m);
            }
            None => {
                editor.put(path, reference);
            }
        }
    }

    /// The whitepaper 4.3 emulation: reach the page at `offset` by resume
    /// token, then read it.
    ///
    /// 0.2 has no rank-directed seek, so a client walks `offset / limit` pages
    /// of `limit` entries, carrying the last path of each page into
    /// [`Cursor::after`], and then reads the page it wanted. The cost is
    /// O(offset), which is the finding; the 1.0 side serves the same page in
    /// O(depth). The walk is exact when `limit` divides `offset`, which every
    /// swept offset does.
    ///
    /// # Errors
    ///
    /// Returns an error before the first build, or if the store fails.
    pub fn resume_paginate(&self, offset: u64, limit: usize) -> Result<OpOutcome, Err> {
        let root = self.root()?;
        let before = self.store.snapshot();
        let capability = Capability::emulated(RESUME_HOW, RESUME_CLASS);
        if limit == 0 {
            return Ok(self.outcome(before, capability, 0));
        }
        let pages = offset / limit as u64;
        let mut token: Option<Vec<u8>> = None;
        let mut returned = 0u64;
        for page in 0..=pages {
            let mut cursor = Cursor::new(self.loadsaver.clone(), root).with_limit(limit);
            if let Some(t) = &token {
                cursor = cursor.after(t);
            }
            let mut last: Option<Vec<u8>> = None;
            let mut seen = 0u64;
            while let Some(item) = run(cursor.next()) {
                let entry = item?;
                last = Some(entry.path().to_vec());
                seen = seen.saturating_add(1);
            }
            if page == pages {
                returned = seen;
            }
            match last {
                Some(l) => token = Some(l),
                // The manifest ran out before the offset; the page is empty and
                // the walk stops rather than looping on a stale token.
                None => break,
            }
        }
        Ok(self.outcome(before, capability, returned))
    }

    /// Close a measured operation as its counter delta plus a capability.
    fn outcome(&self, before: Counters, capability: Capability, keys_returned: u64) -> OpOutcome {
        let after = self.store.snapshot();
        OpOutcome {
            capability,
            cost: Some(OpCost {
                fetches: after.gets.saturating_sub(before.gets),
                puts: after.puts.saturating_sub(before.puts),
                keys_returned,
            }),
        }
    }
}

impl Arm for MantarayArm {
    fn label(&self) -> &'static str {
        "mantaray-0.2"
    }

    fn build(&mut self, keys: &[GenKey]) -> Result<BuildReport, Err> {
        let store = SharedCounting::new();
        let loadsaver = NodeLoadSaver::new(store.clone());
        let mut editor = ManifestEditor::new(loadsaver);
        // The digest is taken in the loop that feeds the editor, so it witnesses
        // what the format consumed and not what the caller held.
        let mut stream = KeyStreamHasher::new();
        for k in keys {
            stream.push(&k.raw);
            let reference = ChunkAddress::new(value_addr(&k.raw));
            Self::put(&mut editor, &k.raw, reference, Self::metadata(k));
        }
        let (root, loadsaver) = run(editor.commit())?;
        let counters = store.snapshot();
        self.store = store;
        self.loadsaver = loadsaver;
        self.root = Some(root);
        self.consumed = Some(stream.finish());
        // The commit persists a fully materialised trie post-order, so every
        // node is resident at once: O(N) RAM, witnessed by the resident node
        // chunks. The format has no embedding.
        Ok(BuildReport {
            frontier: FrontierClass::WholeTrie {
                resident_nodes: counters.total_chunks,
            },
            nodes_written: counters.total_chunks,
            nodes_embedded: None,
        })
    }

    fn consumed_digest(&self) -> Option<[u8; 32]> {
        self.consumed
    }

    fn counters(&self) -> Counters {
        self.store.snapshot()
    }

    fn get(&self, key: &[u8]) -> Result<OpOutcome, Err> {
        let root = self.root()?;
        let reader = Reader::new(self.loadsaver.clone());
        let before = self.store.snapshot();
        let found = run(reader.get(root, key))?;
        Ok(self.outcome(before, Capability::native(), u64::from(found.is_some())))
    }

    fn floor(&self, key: &[u8]) -> Result<OpOutcome, Err> {
        // No public-API backward seek: walk ascending and keep the last key
        // <= `key`, stopping at the first key past it.
        let root = self.root()?;
        let before = self.store.snapshot();
        let mut cursor = Cursor::new(self.loadsaver.clone(), root);
        let mut found = false;
        while let Some(item) = run(cursor.next()) {
            let entry = item?;
            if entry.path() > key {
                break;
            }
            found = true;
        }
        Ok(self.outcome(
            before,
            Capability::emulated(FLOOR_HOW, FLOOR_CLASS),
            u64::from(found),
        ))
    }

    fn ceiling(&self, key: &[u8]) -> Result<OpOutcome, Err> {
        // Exact hit, else the first key strictly after `key`: the after-bound
        // prunes every subtree wholly at or before it, so the seek is
        // seek-grade.
        let root = self.root()?;
        let before = self.store.snapshot();
        let reader = Reader::new(self.loadsaver.clone());
        let mut found = run(reader.get(root, key))?.is_some();
        if !found {
            let mut cursor = Cursor::new(self.loadsaver.clone(), root)
                .after(key)
                .with_limit(1);
            if let Some(item) = run(cursor.next()) {
                item?;
                found = true;
            }
        }
        Ok(self.outcome(
            before,
            Capability::emulated(CEILING_HOW, CEILING_CLASS),
            u64::from(found),
        ))
    }

    fn range(&self, lo: &[u8], hi: &[u8]) -> Result<OpOutcome, Err> {
        // `get(lo)` for the inclusive bound, then an after-bound ordered drain
        // stopping at the first key >= `hi`.
        let root = self.root()?;
        let before = self.store.snapshot();
        let reader = Reader::new(self.loadsaver.clone());
        let mut count = u64::from(run(reader.get(root, lo))?.is_some());
        let mut cursor = Cursor::new(self.loadsaver.clone(), root).after(lo);
        while let Some(item) = run(cursor.next()) {
            let entry = item?;
            if entry.path() >= hi {
                break;
            }
            count = count.saturating_add(1);
        }
        Ok(self.outcome(before, Capability::emulated(RANGE_HOW, RANGE_CLASS), count))
    }

    fn range_pessimal(&self, lo: &[u8], hi: &[u8]) -> Result<OpOutcome, Err> {
        // The whitepaper's fallback shape: a full manifest walk with a
        // client-side range filter.
        let root = self.root()?;
        let before = self.store.snapshot();
        let mut cursor = Cursor::new(self.loadsaver.clone(), root);
        let mut count = 0u64;
        while let Some(item) = run(cursor.next()) {
            let entry = item?;
            let path = entry.path();
            if path >= lo && path < hi {
                count = count.saturating_add(1);
            }
        }
        Ok(self.outcome(
            before,
            Capability::emulated(RANGE_PESSIMAL_HOW, PESSIMAL_CLASS),
            count,
        ))
    }

    fn prefix_list(&self, prefix: &[u8]) -> Result<OpOutcome, Err> {
        // The pruned prefix walk: the cursor prunes subtrees the prefix
        // excludes, so this is the fair `walk_from`.
        let root = self.root()?;
        let before = self.store.snapshot();
        let mut cursor = Cursor::new(self.loadsaver.clone(), root).with_prefix(prefix);
        let mut count = 0u64;
        while let Some(item) = run(cursor.next()) {
            item?;
            count = count.saturating_add(1);
        }
        Ok(self.outcome(
            before,
            Capability::emulated(PREFIX_HOW, PREFIX_CLASS),
            count,
        ))
    }

    fn prefix_list_pessimal(&self, prefix: &[u8]) -> Result<OpOutcome, Err> {
        // The pessimal column: a full manifest walk with a client-side prefix
        // filter.
        let root = self.root()?;
        let before = self.store.snapshot();
        let mut cursor = Cursor::new(self.loadsaver.clone(), root);
        let mut count = 0u64;
        while let Some(item) = run(cursor.next()) {
            let entry = item?;
            if entry.path().starts_with(prefix) {
                count = count.saturating_add(1);
            }
        }
        Ok(self.outcome(
            before,
            Capability::emulated(PREFIX_PESSIMAL_HOW, PESSIMAL_CLASS),
            count,
        ))
    }

    fn full_iter(&self) -> Result<OpOutcome, Err> {
        // The trie DFS stream in documented path order under a bounded window.
        let root = self.root()?;
        let before = self.store.snapshot();
        let mut cursor = Cursor::new(self.loadsaver.clone(), root);
        let mut count = 0u64;
        while let Some(item) = run(cursor.next()) {
            item?;
            count = count.saturating_add(1);
        }
        Ok(self.outcome(
            before,
            Capability::emulated(FULL_ITER_HOW, FULL_ITER_CLASS),
            count,
        ))
    }

    fn batch_update(&self, edits: &[GenKey], mode: BatchMode) -> Result<OpOutcome, Err> {
        let root = self.root()?;
        let before = self.store.snapshot();
        match mode {
            BatchMode::Batched => {
                // One editor, K puts, one commit; the root is not advanced.
                let mut editor = ManifestEditor::open(root, self.loadsaver.clone());
                for k in edits {
                    let reference = ChunkAddress::new(tagged_addr(b"upd", &k.raw));
                    Self::put(&mut editor, &k.raw, reference, Self::metadata(k));
                }
                let _ = run(editor.commit())?;
            }
            BatchMode::PerEdit => {
                // K sequential open-put-commit cycles, each from the same root.
                for k in edits {
                    let mut editor = ManifestEditor::open(root, self.loadsaver.clone());
                    let reference = ChunkAddress::new(tagged_addr(b"upd", &k.raw));
                    Self::put(&mut editor, &k.raw, reference, Self::metadata(k));
                    let _ = run(editor.commit())?;
                }
            }
        }
        let capability = match mode {
            BatchMode::Batched => Capability::emulated(BATCH_HOW, BATCH_CLASS),
            BatchMode::PerEdit => Capability::native(),
        };
        Ok(self.outcome(before, capability, edits.len() as u64))
    }

    fn timed_build(&self, keys: &[GenKey]) -> Result<Duration, Err> {
        // Pre-materialise the references and metadata; only the put loop and
        // the commit ride the timer, over a plain (uncounted) store.
        let prepared: Vec<PreparedPut<'_>> = keys
            .iter()
            .map(|k| {
                (
                    k.raw.as_slice(),
                    ChunkAddress::new(value_addr(&k.raw)),
                    Self::metadata(k),
                )
            })
            .collect();
        let loadsaver = NodeLoadSaver::new(MemoryStore::<AnyChunkSet>::new());
        let t0 = Instant::now();
        let mut editor = ManifestEditor::new(loadsaver);
        for (path, reference, meta) in &prepared {
            match meta {
                Some(m) => {
                    editor.put_with_metadata(path, *reference, m.clone());
                }
                None => {
                    editor.put(path, *reference);
                }
            }
        }
        let _ = run(editor.commit())?;
        Ok(t0.elapsed())
    }
}
