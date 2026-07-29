//! The mantaray 1.0 arm: [`LdbArm`] over a counting store, generic over the
//! format `F` (`V1` or `V1Read`).
//!
//! Nearly every operation is native: the reader and cursor are the format's own
//! O(depth) primitives, so the pessimal columns return the same native cost
//! (there is no degraded 1.0 path; the pessimal columns exist to price the 0.2
//! side). [`Arm::ceiling`] is the one exception and is labelled as one: 1.0 has
//! no dedicated ceiling primitive, so the arm composes `range(key, MAX)` with
//! one `next()`. That cursor launches up to `READ_AHEAD` speculative child
//! fetches on its first poll, so the figure is an UPPER BOUND on a dedicated
//! seek, not the cost of one. The label rides every ceiling measurement and the
//! capability matrix, so no reader meets the number without meeting the bound.

use std::marker::PhantomData;
use std::time::{Duration, Instant};

use bytes::Bytes;
use nectar_testing::run;

use nectar_ldb::{
    Builder, Changeset, Entry, Format, Key, KeyId, Metadata, Plaintext, Reader, V1, apply,
};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkAddress, ChunkRef, StandardChunkSet};

use crate::arm::{Arm, BatchMode, BuildReport, Capability, Err, FrontierClass, OpCost, OpOutcome};
use crate::corpus::{GenKey, KeyStreamHasher, tagged_addr, value_addr};
use crate::store::{Counters, CountingStore};

/// A key sorting strictly above every corpus key: the open upper bound for the
/// ceiling seek.
const MAX_KEY: [u8; 48] = [0xff; 48];

/// How the 1.0 arm serves `ceiling`: there is no dedicated primitive.
pub(crate) const CEILING_HOW: &str = "range(key, MAX) cursor, first item";

/// What that composition costs, stated as the bound it is.
///
/// The cursor's first poll stages the descent and then fills a read-ahead
/// window of referenced children ahead of the walk position
/// (`crates/ldb/src/scan.rs`), so the measured figure includes speculative
/// fetches a dedicated seek would never make.
pub(crate) const CEILING_CLASS: &str = "O(depth) descent plus up to READ_AHEAD speculative child fetches: \
     an upper bound on a dedicated seek";

/// Why the 1.0 pessimal columns repeat the native cost.
///
/// A pessimal column prices the degraded path a client without the primitive
/// must run. 1.0 has no degraded path, so no whole-manifest walk was measured
/// here and the cell repeats the native figure.
pub(crate) const NO_FALLBACK: &str = "native (no 1.0 fallback): 1.0 has no degraded path, so the \
pessimal column repeats the native cost; no whole-manifest walk was measured";

/// mantaray 1.0 over a counting store; `F` is `V1` or `V1Read`.
#[derive(Debug)]
pub struct LdbArm<F: Format> {
    store: CountingStore<StandardChunkSet>,
    root: Option<ChunkRef>,
    consumed: Option<[u8; 32]>,
    _f: PhantomData<F>,
}

impl<F: Format> Default for LdbArm<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: Format> LdbArm<F> {
    /// An arm over a fresh, empty counting store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: CountingStore::new(),
            root: None,
            consumed: None,
            _f: PhantomData,
        }
    }

    /// The arm's built root, or an error before the first build.
    fn root(&self) -> Result<&ChunkRef, Err> {
        self.root.as_ref().ok_or_else(|| "ldb arm not built".into())
    }
}

/// The ref32 entry for a key: the corpus value address.
fn entry_for<F: Format>(bytes: &[u8]) -> Entry<F> {
    Entry::<F>::from(ChunkRef::new(ChunkAddress::new(value_addr(bytes))))
}

/// A distinct ref32 entry for an update to a key.
fn alt_entry_for<F: Format>(bytes: &[u8]) -> Entry<F> {
    Entry::<F>::from(ChunkRef::new(ChunkAddress::new(tagged_addr(b"upd", bytes))))
}

/// The content-type metadata for a key, when the corpus carries it.
fn meta_for<F: Format>(k: &GenKey) -> Option<Metadata<F>> {
    k.content_type.and_then(|ct| {
        Metadata::<F>::new(KeyId::ContentType, Bytes::from_static(ct.as_bytes())).ok()
    })
}

impl<F: Format> LdbArm<F> {
    /// A reader over the arm's store.
    fn reader(&self) -> Reader<ContentGet<&CountingStore<StandardChunkSet>>, F> {
        Reader::<_, F>::new(ContentGet::new(&self.store))
    }

    /// The cost of `op`, measured as the counter delta around it, classed
    /// [`Capability::Native`].
    fn native<T>(
        &self,
        keys_returned: u64,
        op: impl FnOnce() -> Result<T, Err>,
    ) -> Result<OpOutcome, Err> {
        let before = self.store.snapshot();
        op()?;
        let after = self.store.snapshot();
        Ok(OpOutcome {
            capability: Capability::native(),
            cost: Some(OpCost {
                fetches: after.gets.saturating_sub(before.gets),
                puts: after.puts.saturating_sub(before.puts),
                keys_returned,
            }),
        })
    }
}

impl<F: Format> Arm for LdbArm<F> {
    fn label(&self) -> &'static str {
        match F::VERSION {
            V1_VERSION => "ldb-v1",
            _ => "ldb-v1read",
        }
    }

    fn build(&mut self, keys: &[GenKey]) -> Result<BuildReport, Err> {
        let store = CountingStore::<StandardChunkSet>::new();
        let mut builder = Builder::<F>::new();
        // The digest is taken in the loop that feeds the format, so it witnesses
        // what the builder consumed and not what the caller held.
        let mut stream = KeyStreamHasher::new();
        for k in keys {
            stream.push(&k.raw);
            builder.insert(
                Key::from(k.raw.as_slice()),
                entry_for::<F>(&k.raw),
                meta_for::<F>(k),
            );
        }
        let built = run(builder.build(&store, &Plaintext))?;
        let stats = *built.stats();
        self.root = Some(*built.root());
        self.store = store;
        self.consumed = Some(stream.finish());
        Ok(BuildReport {
            frontier: FrontierClass::Bounded {
                peak_open_nodes: stats.peak_open_nodes() as u64,
            },
            nodes_written: stats.nodes_written() as u64,
            nodes_embedded: Some(stats.nodes_embedded() as u64),
        })
    }

    fn consumed_digest(&self) -> Option<[u8; 32]> {
        self.consumed
    }

    fn counters(&self) -> Counters {
        self.store.snapshot()
    }

    fn get(&self, key: &[u8]) -> Result<OpOutcome, Err> {
        let root = *self.root()?;
        let reader = self.reader();
        let before = self.store.snapshot();
        let found = run(reader.get(&root, &Key::from(key)))?;
        let after = self.store.snapshot();
        Ok(OpOutcome {
            capability: Capability::native(),
            cost: Some(OpCost {
                fetches: after.gets.saturating_sub(before.gets),
                puts: after.puts.saturating_sub(before.puts),
                keys_returned: u64::from(found.is_some()),
            }),
        })
    }

    fn floor(&self, key: &[u8]) -> Result<OpOutcome, Err> {
        let root = *self.root()?;
        let reader = self.reader();
        let before = self.store.snapshot();
        let found = run(reader.floor(&root, &Key::from(key)))?;
        let after = self.store.snapshot();
        Ok(OpOutcome {
            capability: Capability::native(),
            cost: Some(OpCost {
                fetches: after.gets.saturating_sub(before.gets),
                puts: after.puts.saturating_sub(before.puts),
                keys_returned: u64::from(found.is_some()),
            }),
        })
    }

    fn ceiling(&self, key: &[u8]) -> Result<OpOutcome, Err> {
        // The least key >= `key`: the first key of the open-ended range. 1.0
        // has no dedicated ceiling primitive, so this composes the range cursor
        // and takes one item. The first poll also fills the cursor's read-ahead
        // window, so the delta is an upper bound on a dedicated seek. The
        // capability says so on every measurement.
        let root = *self.root()?;
        let reader = self.reader();
        let before = self.store.snapshot();
        let mut cursor = run(reader.range(&root, &Key::from(key), &Key::from(MAX_KEY.as_slice())))?;
        let first = run(cursor.next())?;
        let after = self.store.snapshot();
        Ok(OpOutcome {
            capability: Capability::emulated(CEILING_HOW, CEILING_CLASS),
            cost: Some(OpCost {
                fetches: after.gets.saturating_sub(before.gets),
                puts: after.puts.saturating_sub(before.puts),
                keys_returned: u64::from(first.is_some()),
            }),
        })
    }

    fn range(&self, lo: &[u8], hi: &[u8]) -> Result<OpOutcome, Err> {
        let root = *self.root()?;
        let reader = self.reader();
        let mut count = 0u64;
        self.native(0, || {
            let mut cursor = run(reader.range(&root, &Key::from(lo), &Key::from(hi)))?;
            while run(cursor.next())?.is_some() {
                count = count.saturating_add(1);
            }
            Ok(())
        })
        .map(|mut outcome| {
            if let Some(cost) = outcome.cost.as_mut() {
                cost.keys_returned = count;
            }
            outcome
        })
    }

    fn range_pessimal(&self, lo: &[u8], hi: &[u8]) -> Result<OpOutcome, Err> {
        // No degraded 1.0 path, so no whole-manifest walk is measured here:
        // the pessimal column repeats the native cost. The outcome stays
        // classed [`Capability::Native`], which is what the renderer reads to
        // label the cell `native (no 1.0 fallback)` rather than printing it
        // under whole-walk prose (see [`NO_FALLBACK`]).
        self.range(lo, hi)
    }

    fn prefix_list(&self, prefix: &[u8]) -> Result<OpOutcome, Err> {
        let root = *self.root()?;
        let reader = self.reader();
        let mut count = 0u64;
        self.native(0, || {
            let mut cursor = run(reader.prefix(&root, &Key::from(prefix)))?;
            while run(cursor.next())?.is_some() {
                count = count.saturating_add(1);
            }
            Ok(())
        })
        .map(|mut outcome| {
            if let Some(cost) = outcome.cost.as_mut() {
                cost.keys_returned = count;
            }
            outcome
        })
    }

    fn prefix_list_pessimal(&self, prefix: &[u8]) -> Result<OpOutcome, Err> {
        // As [`Arm::range_pessimal`]: the native cost, labelled as such.
        self.prefix_list(prefix)
    }

    fn full_iter(&self) -> Result<OpOutcome, Err> {
        let root = *self.root()?;
        let reader = self.reader();
        let mut count = 0u64;
        self.native(0, || {
            let mut cursor = run(reader.iter(&root))?;
            while run(cursor.next())?.is_some() {
                count = count.saturating_add(1);
            }
            Ok(())
        })
        .map(|mut outcome| {
            if let Some(cost) = outcome.cost.as_mut() {
                cost.keys_returned = count;
            }
            outcome
        })
    }

    fn batch_update(&self, edits: &[GenKey], mode: BatchMode) -> Result<OpOutcome, Err> {
        let root = *self.root()?;
        let store = ContentGet::new(&self.store);
        let before = self.store.snapshot();
        match mode {
            BatchMode::Batched => {
                let mut cs = Changeset::<F>::new();
                for k in edits {
                    cs.put(
                        Key::from(k.raw.as_slice()),
                        alt_entry_for::<F>(&k.raw),
                        meta_for::<F>(k),
                    );
                }
                // The root is not advanced: the returned root is discarded.
                let _ = run(apply(&store, &Plaintext, &root, &cs))?;
            }
            BatchMode::PerEdit => {
                for k in edits {
                    let mut cs = Changeset::<F>::new();
                    cs.put(
                        Key::from(k.raw.as_slice()),
                        alt_entry_for::<F>(&k.raw),
                        meta_for::<F>(k),
                    );
                    let _ = run(apply(&store, &Plaintext, &root, &cs))?;
                }
            }
        }
        let after = self.store.snapshot();
        Ok(OpOutcome {
            capability: Capability::native(),
            cost: Some(OpCost {
                fetches: after.gets.saturating_sub(before.gets),
                puts: after.puts.saturating_sub(before.puts),
                keys_returned: edits.len() as u64,
            }),
        })
    }

    fn timed_build(&self, keys: &[GenKey]) -> Result<Duration, Err> {
        // Pre-materialise the entries and metadata; only the insert loop and
        // the build ride the timer, over a plain (uncounted) store.
        let prepared: Vec<(Key, Entry<F>, Option<Metadata<F>>)> = keys
            .iter()
            .map(|k| {
                (
                    Key::from(k.raw.as_slice()),
                    entry_for::<F>(&k.raw),
                    meta_for::<F>(k),
                )
            })
            .collect();
        let store = MemoryStore::<StandardChunkSet>::new();
        let t0 = Instant::now();
        let mut builder = Builder::<F>::new();
        for (key, entry, meta) in &prepared {
            builder.insert(key.clone(), entry.clone(), meta.clone());
        }
        let _ = run(builder.build(&store, &Plaintext))?;
        Ok(t0.elapsed())
    }
}

/// The `V1` version byte, matched to label the read-optimised sibling apart.
const V1_VERSION: u8 = V1::VERSION;
