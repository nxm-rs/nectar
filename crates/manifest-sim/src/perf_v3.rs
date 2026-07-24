//! Measurements for the v3 range-query performance run.
//!
//! Every figure is produced by executing a real reader or cursor over one of
//! the shared corpora. The single modelled quantity is wall-clock latency, and
//! it is always a MEASURED count times a stated RTT:
//! the serial baseline is `fetch_count * rtt`, and the bounded-concurrent figure
//! is `rounds * rtt` where `rounds` is read off the real cursor under a paused
//! virtual clock. A capability gap is a `null` with a reason, never an estimate.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nectar_testing::run;

use nectar_manifest::{
    Builder, Changeset, Entry, Format, Key, KeyId, Metadata, Reader, V1, V1Read, apply,
};
use nectar_mantaray::{Cursor, ManifestEditor};
use nectar_primitives::store::MemoryStore;
use nectar_primitives::{AnyChunkSet, ChunkAddress, ChunkRef, StandardChunkSet};

use crate::corpus::{Corpus, GenKey, tagged_addr, value_addr};
use crate::results_v3::{
    CursorLatency, PaginateCell, ParallelCursorCell, ReadProfileCell, ReadProfileSide,
};
use crate::store::{CountingStore, LatencyStore};

type Err = Box<dyn Error>;

/// Range-window fractions of the key domain: 0.1% up to 100%.
pub const RANGE_WS: [f64; 4] = [0.001, 0.01, 0.10, 1.0];
/// RTT values (ms) for the serial-vs-concurrent latency model.
pub const RTT_SET: [u32; 3] = [25, 50, 75];
/// Rank offsets for the pagination sweep.
pub const PAGE_OFFSETS: [u64; 5] = [0, 100, 1_000, 10_000, 100_000];
/// Keys returned per pagination request.
pub const PAGE_LIMIT: usize = 20;
/// One virtual millisecond of modelled latency per node fetch.
const RTT_UNIT: Duration = Duration::from_millis(1);
/// A key sorting strictly above every corpus key: the open upper bound.
const MAX_KEY: [u8; 48] = [0xff; 48];

// ---- shared builders -----------------------------------------------------

fn ref32(addr: [u8; 32]) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new(addr))
}

fn entry_for<F: Format>(bytes: &[u8]) -> Entry<F> {
    Entry::<F>::from(ref32(value_addr(bytes)))
}

fn alt_entry_for<F: Format>(bytes: &[u8]) -> Entry<F> {
    Entry::<F>::from(ref32(tagged_addr(b"upd", bytes)))
}

fn meta_for<F: Format>(k: &GenKey) -> Option<Metadata<F>> {
    k.content_type.and_then(|ct| {
        Metadata::<F>::new(KeyId::ContentType, Bytes::from_static(ct.as_bytes())).ok()
    })
}

/// Build every key into a fresh in-memory store, returning the root.
fn build_mem<F: Format>(keys: &[GenKey]) -> Result<(MemoryStore, ChunkAddress), Err> {
    let store = MemoryStore::default();
    let mut builder = Builder::<F>::new();
    for k in keys {
        builder.insert(
            Key::from(k.raw.as_slice()),
            entry_for::<F>(&k.raw),
            meta_for::<F>(k),
        );
    }
    let built = run(builder.build(&store))?;
    let root = *built.root();
    Ok((store, root))
}

/// Build every key into a fresh counting store, returning the root.
fn build_counting<F: Format>(
    keys: &[GenKey],
) -> Result<(CountingStore<StandardChunkSet>, ChunkAddress), Err> {
    let store = CountingStore::<StandardChunkSet>::new();
    let mut builder = Builder::<F>::new();
    for k in keys {
        builder.insert(
            Key::from(k.raw.as_slice()),
            entry_for::<F>(&k.raw),
            meta_for::<F>(k),
        );
    }
    let built = run(builder.build(&store))?;
    let root = *built.root();
    Ok((store, root))
}

/// Deterministic evenly-spaced sample of `count` indices in `0..n`.
fn sample_indices(n: usize, count: usize) -> Vec<usize> {
    if n == 0 {
        return Vec::new();
    }
    if n <= count {
        return (0..n).collect();
    }
    let stride = n / count;
    (0..count).map(|j| (j * stride).min(n - 1)).collect()
}

/// The `[lo, hi)` sorted-key indices of a centred window of width `w`.
fn window_indices(n: usize, w: f64) -> (usize, usize) {
    if n == 0 {
        return (0, 0);
    }
    let width = ((n as f64) * w).round() as usize;
    let width = width.clamp(1, n);
    let lo = (n - width) / 2;
    let hi = (lo + width).min(n - 1);
    (lo, hi)
}

/// The `lo` and open `hi` keys of a centred window; a full window opens the
/// upper bound past every key so the scan runs to the last.
fn window_keys(keys: &[GenKey], w: f64) -> (Key, Key) {
    let n = keys.len();
    let (lo_i, hi_i) = window_indices(n, w);
    let lo = keys
        .get(lo_i)
        .map_or_else(Key::empty, |k| Key::from(k.raw.as_slice()));
    let hi = if w >= 1.0 {
        Key::from(MAX_KEY.as_slice())
    } else {
        keys.get(hi_i).map_or_else(
            || Key::from(MAX_KEY.as_slice()),
            |k| Key::from(k.raw.as_slice()),
        )
    };
    (lo, hi)
}

/// The first listing prefix of a path corpus (the bytes up to the first '/'),
/// or a one-byte prefix for the uniform corpus.
fn first_prefix(corpus: Corpus, keys: &[GenKey]) -> Option<Vec<u8>> {
    let mid = keys.get(keys.len() / 2)?;
    let raw = &mid.raw;
    let p = match corpus {
        Corpus::Uniform => raw.iter().take(1).copied().collect(),
        _ => match raw.iter().rposition(|&b| b == b'/') {
            Some(pos) => raw.get(..=pos).map(<[u8]>::to_vec).unwrap_or_default(),
            None => raw.iter().take(1).copied().collect(),
        },
    };
    Some(p)
}

// ---- parallel cursor -----------------------------------------------------

/// Drain a range scan under a paused virtual clock, returning
/// `(fetch_count, rounds, keys_returned)`. One virtual millisecond is charged
/// per node fetch, so the elapsed virtual time is exactly `rounds` and the
/// bounded-concurrency read-ahead collapses independent fetches into one round.
// The rounds figure is virtual time under tokio's paused clock, so the
// runtime's own `block_on` is the entry point here.
#[allow(clippy::disallowed_methods)]
fn range_rounds<F: Format>(
    store: &MemoryStore,
    root: &ChunkAddress,
    lo: &Key,
    hi: &Key,
) -> Result<(u64, u64, u64), Err> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()?;
    let out: Result<(u64, u64, u64), nectar_manifest::ReaderError> = rt.block_on(async {
        let latency = LatencyStore::<StandardChunkSet>::new(store, RTT_UNIT);
        let reader = Reader::<&LatencyStore<'_, StandardChunkSet>, F>::new(&latency);
        let t0 = tokio::time::Instant::now();
        let mut cursor = reader.range(root, lo, hi).await?;
        let mut keys = 0u64;
        while cursor.next().await?.is_some() {
            keys = keys.saturating_add(1);
        }
        let rounds = t0.elapsed().as_millis() as u64;
        Ok((latency.gets(), rounds, keys))
    });
    Ok(out?)
}

/// Drain a prefix scan under the paused virtual clock, as [`range_rounds`].
// See `range_rounds` on the paused-clock `block_on` entry point.
#[allow(clippy::disallowed_methods)]
fn prefix_rounds<F: Format>(
    store: &MemoryStore,
    root: &ChunkAddress,
    prefix: &Key,
) -> Result<(u64, u64, u64), Err> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()?;
    let out: Result<(u64, u64, u64), nectar_manifest::ReaderError> = rt.block_on(async {
        let latency = LatencyStore::<StandardChunkSet>::new(store, RTT_UNIT);
        let reader = Reader::<&LatencyStore<'_, StandardChunkSet>, F>::new(&latency);
        let t0 = tokio::time::Instant::now();
        let mut cursor = reader.prefix(root, prefix).await?;
        let mut keys = 0u64;
        while cursor.next().await?.is_some() {
            keys = keys.saturating_add(1);
        }
        let rounds = t0.elapsed().as_millis() as u64;
        Ok((latency.gets(), rounds, keys))
    });
    Ok(out?)
}

/// The serial and bounded-concurrent latency at each RTT, and their speedup.
fn cursor_latency(fetch_count: u64, rounds: u64) -> BTreeMap<String, CursorLatency> {
    let mut by = BTreeMap::new();
    for rtt in RTT_SET {
        let r = f64::from(rtt);
        let serial = fetch_count as f64 * r;
        let concurrent = rounds as f64 * r;
        by.insert(
            rtt.to_string(),
            CursorLatency {
                serial_ms: serial,
                concurrent_ms: concurrent,
                speedup: (concurrent > 0.0).then(|| serial / concurrent),
            },
        );
    }
    by
}

const PC_MODEL: &str = "serial = fetch_count * rtt; concurrent = rounds * rtt; rounds measured from the real \
bounded-concurrency cursor under a paused virtual clock (READ_AHEAD=16), one RTT charged per node \
fetch; fetch_count is identical for both.";

/// Parallel-cursor cells for one `(corpus, scale)`: a range sweep plus one
/// prefix scan.
pub fn parallel_cursor_cells(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
) -> Result<Vec<ParallelCursorCell>, Err> {
    let (store, root) = build_mem::<V1>(keys)?;
    let mut cells = Vec::new();
    for &w in &RANGE_WS {
        let (lo, hi) = window_keys(keys, w);
        let (fetch_count, rounds, keys_returned) = range_rounds::<V1>(&store, &root, &lo, &hi)?;
        cells.push(ParallelCursorCell {
            corpus: corpus.name().to_string(),
            scale,
            op: "range".to_string(),
            window: Some(w),
            keys_returned,
            fetch_count,
            rounds,
            read_ahead: V1::READ_AHEAD as u32,
            by_rtt_ms: cursor_latency(fetch_count, rounds),
            model: PC_MODEL.to_string(),
        });
    }
    if let Some(prefix) = first_prefix(corpus, keys) {
        let pk = Key::from(prefix.as_slice());
        let (fetch_count, rounds, keys_returned) = prefix_rounds::<V1>(&store, &root, &pk)?;
        cells.push(ParallelCursorCell {
            corpus: corpus.name().to_string(),
            scale,
            op: "prefix".to_string(),
            window: None,
            keys_returned,
            fetch_count,
            rounds,
            read_ahead: V1::READ_AHEAD as u32,
            by_rtt_ms: cursor_latency(fetch_count, rounds),
            model: PC_MODEL.to_string(),
        });
    }
    Ok(cells)
}

// ---- V1Read vs V1 --------------------------------------------------------

/// Mean and max get-depth over a key sample, counted as store fetches per get.
fn get_depth<F: Format>(
    store: &CountingStore<StandardChunkSet>,
    reader: &Reader<&CountingStore<StandardChunkSet>, F>,
    root: &ChunkAddress,
    keys: &[GenKey],
    idxs: &[usize],
) -> Result<(f64, u64), Err> {
    let mut hops: Vec<u64> = Vec::with_capacity(idxs.len());
    for &i in idxs {
        if let Some(k) = keys.get(i) {
            let before = store.gets();
            let _ = run(reader.get(root, &Key::from(k.raw.as_slice())))?;
            hops.push(store.gets().saturating_sub(before));
        }
    }
    let sum: u128 = hops.iter().map(|&x| u128::from(x)).sum();
    let mean = if hops.is_empty() {
        0.0
    } else {
        sum as f64 / hops.len() as f64
    };
    let max = hops.iter().copied().max().unwrap_or(0);
    Ok((mean, max))
}

/// Fetches to drain each range window, and mean single-update write-amp, for one
/// format over one corpus.
fn read_side<F: Format>(keys: &[GenKey]) -> Result<ReadProfileSide, Err> {
    let (store, root) = build_counting::<F>(keys)?;
    let reader = Reader::<&CountingStore<StandardChunkSet>, F>::new(&store);
    let n = keys.len();
    let idxs = sample_indices(n, 256);
    let (depth_mean, depth_max) = get_depth::<F>(&store, &reader, &root, keys, &idxs)?;

    let mut range_fetch = BTreeMap::new();
    for &w in &RANGE_WS {
        let (lo, hi) = window_keys(keys, w);
        let before = store.gets();
        let mut cursor = run(reader.range(&root, &lo, &hi))?;
        while run(cursor.next())?.is_some() {}
        range_fetch.insert(fmt_w(w), store.gets().saturating_sub(before));
    }

    // Single-key update write-amplification: one-op changesets over the sample.
    let usample = sample_indices(n, 48);
    let mut rewrites: Vec<u64> = Vec::with_capacity(usample.len());
    for &i in &usample {
        if let Some(k) = keys.get(i) {
            let mut cs = Changeset::<F>::new();
            cs.put(
                Key::from(k.raw.as_slice()),
                alt_entry_for::<F>(&k.raw),
                meta_for::<F>(k),
            );
            let before = store.puts();
            let _ = run(apply(&store, &root, &cs))?;
            rewrites.push(store.puts().saturating_sub(before));
        }
    }
    let sum: u128 = rewrites.iter().map(|&x| u128::from(x)).sum();
    let wa_mean = if rewrites.is_empty() {
        0.0
    } else {
        sum as f64 / rewrites.len() as f64
    };

    Ok(ReadProfileSide {
        version_byte: F::VERSION,
        inline_max: F::INLINE_MAX as u32,
        tree_depth_mean: depth_mean,
        tree_depth_max: depth_max,
        range_fetch_by_window: range_fetch,
        single_update_chunks_mean: wa_mean,
    })
}

fn fmt_w(w: f64) -> String {
    format!("{w}")
}

/// The V1Read-vs-V1 cell for one `(corpus, scale)`.
pub fn read_profile_cell(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
) -> Result<ReadProfileCell, Err> {
    let v1 = read_side::<V1>(keys)?;
    let v1read = read_side::<V1Read>(keys)?;
    let mut fetch_ratio = BTreeMap::new();
    for &w in &RANGE_WS {
        let key = fmt_w(w);
        if let (Some(&a), Some(&b)) = (
            v1read.range_fetch_by_window.get(&key),
            v1.range_fetch_by_window.get(&key),
        ) && b > 0
        {
            fetch_ratio.insert(key, a as f64 / b as f64);
        }
    }
    let depth_ratio =
        (v1.tree_depth_mean > 0.0).then(|| v1read.tree_depth_mean / v1.tree_depth_mean);
    let delta = v1read.single_update_chunks_mean - v1.single_update_chunks_mean;
    let ratio = (v1.single_update_chunks_mean > 0.0)
        .then(|| v1read.single_update_chunks_mean / v1.single_update_chunks_mean);
    Ok(ReadProfileCell {
        corpus: corpus.name().to_string(),
        scale,
        v1,
        v1read,
        fetch_ratio_by_window: fetch_ratio,
        depth_ratio,
        single_update_wa_delta: delta,
        single_update_wa_ratio: ratio,
    })
}

// ---- paginate ------------------------------------------------------------

type V02Store = Arc<CountingStore<AnyChunkSet<4096>>>;

/// Build every key into a 0.2 editor manifest over a counting store.
fn build_v02(keys: &[GenKey]) -> Result<(V02Store, ChunkAddress), Err> {
    let store: V02Store = Arc::new(CountingStore::new());
    let mut ed = ManifestEditor::new(Arc::clone(&store));
    for k in keys {
        ed.put(k.path.as_bytes(), ref32(value_addr(k.path.as_bytes())));
    }
    let (root, _) = run(ed.commit())?;
    Ok((store, root))
}

/// Fetches for the 0.2 cursor to page through to `offset + limit` entries via
/// resume tokens: restartable, but unable to skip, so the walk pays the full
/// scan to the offset.
fn v02_resume_walk(
    store: &V02Store,
    root: ChunkAddress,
    offset: u64,
    limit: usize,
) -> Result<u64, Err> {
    let target = offset.saturating_add(limit as u64);
    let before = store.gets();
    let mut resume: Option<Vec<u8>> = None;
    let mut yielded = 0u64;
    'pages: while yielded < target {
        let mut cursor = Cursor::new(Arc::clone(store), root).with_limit(limit);
        if let Some(after) = &resume {
            cursor = cursor.after(after);
        }
        let mut got = 0u64;
        while let Some(entry) = run(cursor.next()) {
            let entry = entry?;
            resume = Some(entry.path().to_vec());
            yielded = yielded.saturating_add(1);
            got = got.saturating_add(1);
            if yielded >= target {
                break 'pages;
            }
        }
        if got == 0 {
            break;
        }
    }
    Ok(store.gets().saturating_sub(before))
}

/// The pagination sweep for one `(corpus, scale)`: rank-directed paginate.
pub fn paginate_cells(
    corpus: Corpus,
    scale: u64,
    keys: &[GenKey],
) -> Result<Vec<PaginateCell>, Err> {
    let (store, root) = build_counting::<V1>(keys)?;
    let reader = Reader::<&CountingStore<StandardChunkSet>, V1>::new(&store);
    let (v02_store, v02_root) = build_v02(keys)?;
    let n = keys.len() as u64;
    let empty = Key::empty();
    let mut cells = Vec::new();
    for &offset in &PAGE_OFFSETS {
        if offset >= n {
            continue;
        }
        // Rank-directed paginate: O(depth) regardless of offset.
        let before = store.gets();
        let mut cursor = run(reader.paginate_prefix(&root, &empty, offset, PAGE_LIMIT))?;
        let mut returned = 0u64;
        while run(cursor.next())?.is_some() {
            returned = returned.saturating_add(1);
        }
        let paginate_fetch = store.gets().saturating_sub(before);

        // Baseline: iter().skip(offset).take(limit); fetches grow with offset.
        let before = store.gets();
        let mut it = run(reader.iter(&root))?;
        let mut seen = 0u64;
        let target = offset.saturating_add(PAGE_LIMIT as u64);
        while seen < target {
            if run(it.next())?.is_none() {
                break;
            }
            seen = seen.saturating_add(1);
        }
        let skip_fetch = store.gets().saturating_sub(before);

        // The 0.2 resume-token page walk: ordered and restartable, no skip.
        let v02_fetch = v02_resume_walk(&v02_store, v02_root, offset, PAGE_LIMIT)?;

        cells.push(PaginateCell {
            corpus: corpus.name().to_string(),
            scale,
            offset,
            limit: PAGE_LIMIT as u32,
            keys_returned: returned,
            paginate_fetch_count: paginate_fetch,
            skip_baseline_fetch_count: skip_fetch,
            skip_over_paginate: (paginate_fetch > 0)
                .then(|| skip_fetch as f64 / paginate_fetch as f64),
            v02_emulated: true,
            v02_resume_walk_fetch_count: v02_fetch,
            v02_over_paginate: (paginate_fetch > 0)
                .then(|| v02_fetch as f64 / paginate_fetch as f64),
        });
    }
    Ok(cells)
}
