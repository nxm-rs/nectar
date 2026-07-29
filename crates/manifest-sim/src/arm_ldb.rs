//! The mantaray 1.0 arm: [`LdbArm`] over a counting store, generic over the
//! format `F` (`V1` or `V1Read`).
//!
//! Every operation is native: the reader and cursor are the format's own
//! O(depth) primitives, so the pessimal columns return the same native cost
//! (there is no degraded 1.0 path; the pessimal columns exist to price the 0.2
//! side).

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
use crate::corpus::{GenKey, tagged_addr, value_addr};
use crate::store::{Counters, CountingStore};

/// A key sorting strictly above every corpus key: the open upper bound for the
/// ceiling seek.
const MAX_KEY: [u8; 48] = [0xff; 48];

/// mantaray 1.0 over a counting store; `F` is `V1` or `V1Read`.
#[derive(Debug)]
pub struct LdbArm<F: Format> {
    store: CountingStore<StandardChunkSet>,
    root: Option<ChunkRef>,
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
        for k in keys {
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
        Ok(BuildReport {
            frontier: FrontierClass::Bounded {
                peak_open_nodes: stats.peak_open_nodes() as u64,
            },
            nodes_written: stats.nodes_written() as u64,
            nodes_embedded: Some(stats.nodes_embedded() as u64),
        })
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
        // The least key >= `key`: the first key of the open-ended range.
        let root = *self.root()?;
        let reader = self.reader();
        let before = self.store.snapshot();
        let mut cursor = run(reader.range(&root, &Key::from(key), &Key::from(MAX_KEY.as_slice())))?;
        let first = run(cursor.next())?;
        let after = self.store.snapshot();
        Ok(OpOutcome {
            capability: Capability::native(),
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
        // No degraded 1.0 path: the pessimal column is the same native cost.
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
