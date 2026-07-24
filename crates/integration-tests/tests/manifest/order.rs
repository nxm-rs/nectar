//! Order-statistic conformance: rank, select and count
//! agree with a full-walk oracle across corpora and shuffled builds; paginate
//! returns the same slice as `iter().skip(offset).take(limit)`; and the offset
//! seek costs O(depth), not O(offset), witnessed through a fetch-counting store.

use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, ensure};
use nectar_manifest::{Builder, Entry, Key, Reader, V1};
use nectar_primitives::store::{ChunkGet, MemoryStore};
use nectar_primitives::{Chunk, ChunkAddress, ChunkRef, StandardChunkSet, Verified};
use nectar_testing::run;

/// A key set paired with the value each key carries.
type KeySet = Vec<(Key, u8)>;

/// The `(key, value)` pairs a walk yields, in key order.
type Pairs = Vec<(Key, Entry<V1>)>;

fn entry(fill: u8) -> Entry<V1> {
    Entry::from(ChunkRef::new(ChunkAddress::new([fill; 32])))
}

/// A store that counts every `get`, so a test reads how many nodes a query
/// fetched.
#[derive(Debug, Default)]
struct CountingStore {
    inner: MemoryStore,
    gets: AtomicUsize,
}

impl CountingStore {
    fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
    }

    fn gets(&self) -> usize {
        self.gets.load(Ordering::Relaxed)
    }
}

impl ChunkGet<StandardChunkSet> for CountingStore {
    type Trust = Verified;
    type Error = <MemoryStore as ChunkGet>::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, StandardChunkSet>, Self::Error> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        ChunkGet::get(&self.inner, address).await
    }
}

/// Every `radix`-ary key of length `len`, ascending, each valued on its last
/// byte. A multi-byte alphabet fans the trie into referenced children and, at a
/// full first-byte spread, spills interior nodes across segments.
fn radix_keys(radix: u8, len: usize) -> KeySet {
    let mut out = Vec::new();
    let mut key = Vec::new();
    fill(radix, len, &mut key, &mut out);
    out
}

fn fill(radix: u8, remaining: usize, key: &mut Vec<u8>, out: &mut KeySet) {
    match remaining {
        0 => out.push((Key::from(key.as_slice()), key.last().copied().unwrap_or(0))),
        _ => {
            for digit in 0..radix {
                key.push(digit);
                fill(radix, remaining.saturating_sub(1), key, out);
                key.pop();
            }
        }
    }
}

/// A corpus whose root spills into a segment directory: a full 256 first-byte
/// spread, each carrying a 50-key subtree too large to embed, so the root
/// references 256 children and its own fork table overruns one chunk. Distinct
/// values keep the children from collapsing, so the descent routes real
/// segments. Order-statistic queries must route these by `seg_count`.
fn wide_keys() -> KeySet {
    let mut out = Vec::new();
    for a in 0u16..256 {
        for c in 0u8..50 {
            let byte = u8::try_from(a).unwrap_or(0);
            out.push((Key::from(&[byte, c][..]), byte.wrapping_add(c)));
        }
    }
    out
}

fn build(store: &MemoryStore, keys: &[(Key, u8)]) -> Result<ChunkAddress> {
    let mut builder = Builder::<V1>::new();
    for (key, fill) in keys {
        builder.insert(key.clone(), entry(*fill), None);
    }
    Ok(*run(builder.build(store))?.root())
}

/// The ground-truth ordering: every `(key, value)` a full walk yields.
fn oracle(reader: &Reader<MemoryStore, V1>, root: &ChunkAddress) -> Result<Pairs> {
    let mut out = Vec::new();
    let mut cursor = run(reader.iter(root))?;
    while let Some(pair) = run(cursor.next())? {
        out.push(pair);
    }
    Ok(out)
}

#[test]
fn rank_select_count_agree_with_a_full_walk_oracle() -> Result<()> {
    let store = MemoryStore::default();
    let keys = radix_keys(12, 3);
    let root = build(&store, &keys)?;
    let reader = Reader::<MemoryStore, V1>::new(store);
    let all = oracle(&reader, &root)?;
    ensure!(all.len() == keys.len(), "oracle lost keys");

    // select(i) is the i-th key; one past the end is None.
    run(async {
        for (i, (key, value)) in all.iter().enumerate() {
            let index = u64::try_from(i)?;
            let got = reader.select(&root, index).await?;
            ensure!(
                got.as_ref() == Some(&(key.clone(), value.clone())),
                "select({i}) mismatch",
            );
        }
        anyhow::Ok(())
    })?;
    let end = u64::try_from(all.len())?;
    ensure!(
        run(reader.select(&root, end))?.is_none(),
        "select past the end must be None",
    );

    // rank(key) is the count of strictly-smaller keys, checked at present keys
    // and at the absent points that bracket, precede and follow the key set.
    let probes = [
        Key::empty(),
        Key::from(&[0u8][..]),
        Key::from(&[3u8, 5][..]),
        Key::from(&[3u8, 5, 5][..]),
        Key::from(&[3u8, 5, 6][..]),
        Key::from(&[6u8, 0, 0][..]),
        Key::from(&[11u8, 11, 11][..]),
        Key::from(&[12u8][..]),
        Key::from(&[255u8][..]),
    ];
    run(async {
        for probe in &probes {
            let expected = u64::try_from(all.iter().filter(|(k, _)| k < probe).count())?;
            let got = reader.rank(&root, probe).await?;
            ensure!(got == expected, "rank mismatch: {got} != {expected}");
        }
        anyhow::Ok(())
    })?;

    // count(lo, hi) is the size of the half-open window.
    run(async {
        for (lo, hi) in [
            (Key::from(&[0u8, 0, 0][..]), Key::from(&[3u8, 0, 0][..])),
            (Key::from(&[3u8, 5, 5][..]), Key::from(&[7u8, 2, 2][..])),
            (Key::empty(), Key::from(&[12u8][..])),
            (Key::from(&[9u8][..]), Key::from(&[3u8][..])),
        ] {
            let expected = u64::try_from(all.iter().filter(|(k, _)| lo <= *k && *k < hi).count())?;
            let got = reader.count(&root, &lo, &hi).await?;
            ensure!(got == expected, "count mismatch: {got} != {expected}");
        }
        Ok(())
    })
}

#[test]
fn spilled_node_order_statistics_agree_with_the_oracle() -> Result<()> {
    // A corpus whose root is a segmented node, so every query routes the root's
    // segment directory by seg_count before descending a referenced child.
    let store = MemoryStore::default();
    let keys = wide_keys();
    let root = build(&store, &keys)?;
    let reader = Reader::<MemoryStore, V1>::new(store);
    let all = oracle(&reader, &root)?;
    ensure!(all.len() == keys.len(), "oracle lost keys");

    // Sampled select and its inverse rank agree with the oracle across the whole
    // spilled listing, including both ends.
    let last = all.len().saturating_sub(1);
    run(async {
        for i in (0..all.len()).step_by(311).chain([last]) {
            let (key, value) = all.get(i).context("oracle index out of range")?;
            let index = u64::try_from(i)?;
            let got = reader.select(&root, index).await?;
            ensure!(
                got.as_ref() == Some(&(key.clone(), value.clone())),
                "spilled select({i}) mismatch",
            );
            let rank = reader.rank(&root, key).await?;
            ensure!(rank == index, "spilled rank at {i} mismatch");
        }
        anyhow::Ok(())
    })?;

    // Windows spanning several segments count exactly.
    run(async {
        for (lo, hi) in [
            (Key::from(&[0u8, 0][..]), Key::from(&[100u8, 0][..])),
            (Key::from(&[40u8, 25][..]), Key::from(&[210u8, 10][..])),
            (Key::empty(), Key::from(&[255u8, 255][..])),
        ] {
            let expected = u64::try_from(all.iter().filter(|(k, _)| lo <= *k && *k < hi).count())?;
            let got = reader.count(&root, &lo, &hi).await?;
            ensure!(
                got == expected,
                "spilled count mismatch: {got} != {expected}"
            );
        }
        anyhow::Ok(())
    })?;

    // A page deep in the spilled listing is the matching oracle slice.
    let mut got = Vec::new();
    let mut cursor = run(reader.paginate_prefix(&root, &Key::empty(), 9000, 25))?;
    run(async {
        while let Some(pair) = cursor.next().await? {
            got.push(pair);
        }
        anyhow::Ok(())
    })?;
    let want: Vec<_> = all.iter().skip(9000).take(25).cloned().collect();
    ensure!(got == want, "spilled paginate mismatch");
    Ok(())
}

#[test]
fn order_statistics_are_stable_under_shuffled_build_order() -> Result<()> {
    let forward = radix_keys(10, 3);
    let mut reversed = forward.clone();
    reversed.reverse();

    let a_store = MemoryStore::default();
    let a = build(&a_store, &forward)?;
    let b_store = MemoryStore::default();
    let b = build(&b_store, &reversed)?;
    ensure!(a == b, "shuffled build must root identically");

    let reader = Reader::<MemoryStore, V1>::new(a_store);
    // A shuffled build is the same tree, so every rank and select is unchanged.
    run(async {
        for index in [0u64, 1, 250, 500, 999] {
            let got = reader.select(&a, index).await?;
            let (key, _) = got.context("select fell off the shuffled build")?;
            let rank = reader.rank(&a, &key).await?;
            ensure!(rank == index, "rank/select disagree at {index}");
        }
        Ok(())
    })
}

#[test]
fn paginate_matches_iter_skip_take() -> Result<()> {
    let store = MemoryStore::default();
    let keys = radix_keys(10, 3);
    let root = build(&store, &keys)?;
    let reader = Reader::<MemoryStore, V1>::new(store);
    let all = oracle(&reader, &root)?;

    // Whole-manifest pagination is exactly iter().skip(offset).take(limit).
    run(async {
        for (offset, limit) in [(0u64, 7usize), (13, 20), (500, 33), (995, 50), (2000, 4)] {
            let mut got = Vec::new();
            let mut cursor = reader
                .paginate_prefix(&root, &Key::empty(), offset, limit)
                .await?;
            while let Some(pair) = cursor.next().await? {
                got.push(pair);
            }
            let start = usize::try_from(offset)?;
            let want: Vec<_> = all.iter().skip(start).take(limit).cloned().collect();
            ensure!(got == want, "paginate_prefix({offset}, {limit}) mismatch");
        }
        anyhow::Ok(())
    })?;

    // Range pagination is the same over the range's own slice.
    let lo = Key::from(&[3u8, 0, 0][..]);
    let hi = Key::from(&[7u8, 0, 0][..]);
    let window: Vec<_> = all
        .iter()
        .filter(|(k, _)| lo <= *k && *k < hi)
        .cloned()
        .collect();
    run(async {
        for (offset, limit) in [(0u64, 10usize), (25, 40), (300, 12)] {
            let mut got = Vec::new();
            let mut cursor = reader.paginate(&root, &lo, &hi, offset, limit).await?;
            while let Some(pair) = cursor.next().await? {
                got.push(pair);
            }
            let start = usize::try_from(offset)?;
            let want: Vec<_> = window.iter().skip(start).take(limit).cloned().collect();
            ensure!(got == want, "paginate({offset}, {limit}) mismatch");
        }
        Ok(())
    })
}

#[test]
fn the_offset_seek_costs_depth_not_offset() -> Result<()> {
    // A wide, spilled corpus: a full first-byte spread forces the root to spill
    // into a segment directory, so the seek must route by seg_count.
    let inner = MemoryStore::default();
    let keys = wide_keys();
    let root = build(&inner, &keys)?;
    let store = CountingStore {
        inner,
        gets: AtomicUsize::new(0),
    };
    let reader = Reader::<CountingStore, V1>::new(store);
    let total = u64::try_from(keys.len())?;

    // Selecting near the start and deep into the listing fetches the same handful
    // of nodes: a few directory and node hops per level, never the offset.
    let mut costs = Vec::new();
    for index in [0u64, total / 2, total.saturating_sub(1)] {
        reader.store().reset();
        let got = run(reader.select(&root, index))?;
        ensure!(got.is_some(), "select({index}) fell off");
        costs.push(reader.store().gets());
    }
    for cost in &costs {
        ensure!(*cost <= 32, "select fetched {cost} nodes, not O(depth)");
    }

    // Paginating at a large offset fetches no more than at offset zero plus the
    // page and its read-ahead: the offset itself is free.
    reader.store().reset();
    let mut head = run(reader.paginate_prefix(&root, &Key::empty(), 0, 5))?;
    run(async {
        while head.next().await?.is_some() {}
        anyhow::Ok(())
    })?;
    let head_cost = reader.store().gets();

    reader.store().reset();
    let deep_offset = total.saturating_sub(5);
    let mut deep = run(reader.paginate_prefix(&root, &Key::empty(), deep_offset, 5))?;
    run(async {
        while deep.next().await?.is_some() {}
        anyhow::Ok(())
    })?;
    let deep_cost = reader.store().gets();

    // The deep page pays for its own depth and window, not the offset it skipped.
    ensure!(
        deep_cost <= head_cost.saturating_mul(2).saturating_add(32),
        "deep page fetched {deep_cost} vs head {head_cost}: offset is not free",
    );
    ensure!(
        u64::try_from(deep_cost)? < deep_offset,
        "deep page fetched {deep_cost}, proportional to the offset",
    );
    Ok(())
}
