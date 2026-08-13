//! One arm per manifest format, measured over the seam rather than over each
//! format's own API, so the two columns are the same call sequence.
//!
//! Every arm owns its counting store, so a cost is a counter delta and never a
//! shared figure. A verb the format leaves to the seam default is labelled
//! [`Capability::Emulated`] at the point of measurement, and the label rides
//! into the result document beside the cost it explains.

use std::error::Error;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nectar_ldb::Database;
use nectar_manifest::{Batch, Manifest, ManifestCursor, ManifestPath, ManifestView};
use nectar_mantaray::MantarayManifest;
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkRef, StandardChunkSet};
use nectar_testing::bench::CountingStore;
use nectar_testing::run;
use serde::Serialize;

use crate::corpus::reference;

pub type Err = Box<dyn Error>;

/// The instrumented store an arm builds into.
pub type Counting = Arc<CountingStore<StandardChunkSet>>;

/// The uncounted store the wall-clock lane builds into: no atomic sits in the
/// timed path.
pub type Plain = Arc<MemoryStore<StandardChunkSet>>;

/// The verbs under measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Get,
    Floor,
    Dir,
    Range,
    Update,
}

impl Op {
    /// The JSON label.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Floor => "floor",
            Self::Dir => "dir",
            Self::Range => "range",
            Self::Update => "update",
        }
    }
}

/// How an arm serves one verb.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Capability {
    /// The format's own primitive.
    Native,
    /// A seam default over the format's walk; `how` names it and `cost_class`
    /// states its asymptote.
    Emulated { how: String, cost_class: String },
}

impl Capability {
    fn emulated(how: &'static str, cost_class: &'static str) -> Self {
        Self::Emulated {
            how: how.to_string(),
            cost_class: cost_class.to_string(),
        }
    }
}

/// Store-counter cost of one operation, read by snapshot delta.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpCost {
    pub fetches: u64,
    pub puts: u64,
    pub keys_returned: u64,
}

/// What a build left in the store.
#[derive(Clone, Copy, Debug, Default)]
pub struct Storage {
    pub chunks: u64,
    pub puts: u64,
    pub distinct_puts: u64,
    pub put_bytes: u64,
    pub live_bytes: u64,
}

/// One manifest format under measurement.
pub trait Arm {
    /// The JSON label.
    fn label(&self) -> &'static str;
    /// How this format serves `op`.
    fn capability(&self, op: Op) -> Capability;
    /// Build every key into a fresh counting store and keep the root.
    fn build(&mut self, keys: &[ManifestPath]) -> Result<Storage, Err>;
    /// Point lookup.
    fn get(&self, key: &ManifestPath) -> Result<OpCost, Err>;
    /// Greatest key at or below `key`.
    fn floor(&self, key: &ManifestPath) -> Result<OpCost, Err>;
    /// One directory level.
    fn dir(&self, prefix: &ManifestPath) -> Result<OpCost, Err>;
    /// Ascending drain of `[lo, hi)`.
    fn range(&self, lo: &ManifestPath, hi: &ManifestPath) -> Result<OpCost, Err>;
    /// One key rebound against the built root; the root is not advanced, so
    /// every sample starts from the same tree.
    fn update(&self, key: &ManifestPath) -> Result<OpCost, Err>;
    /// Every key the built root holds, in walk order: the same-corpus witness.
    fn keys(&self) -> Result<Vec<ManifestPath>, Err>;
    /// One timed cold build over an uncounted store.
    fn timed_build(&self, keys: &[ManifestPath]) -> Result<Duration, Err>;
}

/// An arm over any [`Manifest`], counted through its own store.
pub struct SeamArm<M, P> {
    label: &'static str,
    seek: Capability,
    listing: Capability,
    scan: Capability,
    make: fn(&Counting) -> M,
    make_plain: fn(&Plain) -> P,
    store: Counting,
    manifest: M,
    root: ChunkRef,
}

impl<M, P> core::fmt::Debug for SeamArm<M, P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SeamArm")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl<M, P> SeamArm<M, P>
where
    M: Manifest<ChunkRef>,
    P: Manifest<ChunkRef>,
{
    fn new(
        label: &'static str,
        seek: Capability,
        listing: Capability,
        scan: Capability,
        make: fn(&Counting) -> M,
        make_plain: fn(&Plain) -> P,
    ) -> Self {
        let store: Counting = Arc::new(CountingStore::new());
        let manifest = make(&store);
        Self {
            label,
            seek,
            listing,
            scan,
            make,
            make_plain,
            store,
            manifest,
            root: ChunkRef::new(ChunkAddress::new([0; 32])),
        }
    }

    fn cost<T>(
        &self,
        keys_returned: u64,
        body: impl FnOnce() -> Result<T, Err>,
    ) -> Result<OpCost, Err> {
        let (fetches, puts) = (self.store.gets(), self.store.puts());
        body()?;
        Ok(OpCost {
            fetches: self.store.gets().saturating_sub(fetches),
            puts: self.store.puts().saturating_sub(puts),
            keys_returned,
        })
    }
}

/// Stage every key into one batch, each bound to its own reference.
fn staged<Meta: Default>(keys: &[ManifestPath], salt: u64) -> Batch<ChunkRef, Meta> {
    let mut batch = Batch::new();
    for key in keys {
        batch.insert(key.clone(), reference(key, salt));
    }
    batch
}

impl<M, P> Arm for SeamArm<M, P>
where
    M: Manifest<ChunkRef>,
    P: Manifest<ChunkRef>,
{
    fn label(&self) -> &'static str {
        self.label
    }

    fn capability(&self, op: Op) -> Capability {
        match op {
            Op::Get | Op::Update => Capability::Native,
            Op::Floor => self.seek.clone(),
            Op::Dir => self.listing.clone(),
            Op::Range => self.scan.clone(),
        }
    }

    fn build(&mut self, keys: &[ManifestPath]) -> Result<Storage, Err> {
        self.store = Arc::new(CountingStore::new());
        self.manifest = (self.make)(&self.store);
        let empty = run(self.manifest.empty())?;
        self.root = run(self.manifest.apply(empty, staged(keys, 1)))?;
        let counts = self.store.snapshot();
        Ok(Storage {
            chunks: counts.total_chunks,
            puts: counts.puts,
            distinct_puts: counts.distinct_puts,
            put_bytes: counts.put_bytes,
            live_bytes: counts.live_bytes,
        })
    }

    fn get(&self, key: &ManifestPath) -> Result<OpCost, Err> {
        let view = self.manifest.at(self.root);
        let mut found = 0;
        let cost = self.cost(0, || {
            found = u64::from(run(view.get(key))?.is_some());
            Ok(())
        })?;
        Ok(OpCost {
            keys_returned: found,
            ..cost
        })
    }

    fn floor(&self, key: &ManifestPath) -> Result<OpCost, Err> {
        let view = self.manifest.at(self.root);
        let mut found = 0;
        let cost = self.cost(0, || {
            found = u64::from(run(view.floor(key))?.is_some());
            Ok(())
        })?;
        Ok(OpCost {
            keys_returned: found,
            ..cost
        })
    }

    fn dir(&self, prefix: &ManifestPath) -> Result<OpCost, Err> {
        let view = self.manifest.at(self.root);
        let mut listed = 0;
        let cost = self.cost(0, || {
            listed = run(view.dir(prefix))?.entries().len() as u64;
            Ok(())
        })?;
        Ok(OpCost {
            keys_returned: listed,
            ..cost
        })
    }

    fn range(&self, lo: &ManifestPath, hi: &ManifestPath) -> Result<OpCost, Err> {
        let view = self.manifest.at(self.root);
        let bounds = (Bound::Included(lo.clone()), Bound::Excluded(hi.clone()));
        let mut drained = 0u64;
        let cost = self.cost(0, || {
            let mut cursor = run(view.range(bounds))?;
            while run(ManifestCursor::next(&mut cursor))?.is_some() {
                drained = drained.saturating_add(1);
            }
            Ok(())
        })?;
        Ok(OpCost {
            keys_returned: drained,
            ..cost
        })
    }

    fn update(&self, key: &ManifestPath) -> Result<OpCost, Err> {
        self.cost(1, || {
            let batch = staged(std::slice::from_ref(key), 2);
            run(self.manifest.apply(self.root, batch))?;
            Ok(())
        })
    }

    fn keys(&self) -> Result<Vec<ManifestPath>, Err> {
        let view = self.manifest.at(self.root);
        let mut cursor = run(view.iter())?;
        let mut out = Vec::new();
        while let Some((path, _)) = run(ManifestCursor::next(&mut cursor))? {
            out.push(path);
        }
        Ok(out)
    }

    fn timed_build(&self, keys: &[ManifestPath]) -> Result<Duration, Err> {
        let store: Plain = Arc::new(MemoryStore::new());
        let manifest = (self.make_plain)(&store);
        let empty = run(manifest.empty())?;
        let batch = staged(keys, 1);
        let started = Instant::now();
        run(manifest.apply(empty, batch))?;
        Ok(started.elapsed())
    }
}

type Trie<S> = MantarayManifest<nectar_mantaray::NodeLoadSaver<S>, ContentGet<S>>;
type Kv<S> = Database<ContentGet<S>>;

fn trie<S>(store: &Arc<S>) -> Trie<Arc<S>> {
    MantarayManifest::over(Arc::clone(store))
}

fn kv<S>(store: &Arc<S>) -> Kv<Arc<S>> {
    Database::plain(ContentGet::new(Arc::clone(store)))
}

/// The trie arm. Its ordered verbs ride the seam over a full walk: the trie
/// has no ordered seek.
#[must_use]
pub fn mantaray_arm() -> impl Arm {
    SeamArm::new(
        "mantaray",
        Capability::emulated("seam floor over a full walk to rank", "O(N)"),
        Capability::Native,
        Capability::emulated("seam bound filter over a full walk", "O(N)"),
        trie,
        trie,
    )
}

/// The key-value arm. Every ordered verb is a native descent.
#[must_use]
pub fn ldb_arm() -> impl Arm {
    SeamArm::new(
        "ldb",
        Capability::Native,
        Capability::Native,
        Capability::Native,
        kv,
        kv,
    )
}

#[cfg(test)]
mod tests {
    use super::{Arm, Op, ldb_arm, mantaray_arm};
    use crate::corpus::{Corpus, generate};

    /// Both arms hold exactly the corpus after a build, so every cost below is
    /// measured over the same key set rather than assumed to be.
    #[test]
    fn both_arms_hold_the_same_keys() {
        let keys = generate(Corpus::Site, 200);
        for arm in [&mut mantaray_arm() as &mut dyn Arm, &mut ldb_arm()] {
            arm.build(&keys).unwrap();
            assert_eq!(arm.keys().unwrap(), keys, "{}", arm.label());
        }
    }

    /// The floor labels are measured, not asserted: the trie's seam floor is a
    /// walk whose cost tracks the corpus, and the database's floor is a
    /// descent whose cost does not.
    #[test]
    fn the_floor_capability_labels_match_the_measured_law() {
        let probe = |arm: &mut dyn Arm, n: usize| -> u64 {
            let keys = generate(Corpus::Uniform, n);
            arm.build(&keys).unwrap();
            arm.floor(&keys[n / 2]).unwrap().fetches
        };

        let trie = &mut mantaray_arm();
        let (small, large) = (probe(trie, 200), probe(trie, 1_000));
        assert!(
            large > small.saturating_mul(2),
            "trie floor {small} -> {large} does not track the corpus"
        );
        assert!(matches!(
            trie.capability(Op::Floor),
            super::Capability::Emulated { .. }
        ));

        let kv = &mut ldb_arm();
        let (small, large) = (probe(kv, 200), probe(kv, 1_000));
        assert!(
            large <= small.saturating_mul(2),
            "database floor {small} -> {large} tracks the corpus: not a descent"
        );
        assert!(matches!(
            kv.capability(Op::Floor),
            super::Capability::Native
        ));
    }
}
