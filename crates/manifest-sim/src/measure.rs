//! Metric collection for one `(format, corpus, scale)` cell.
//!
//! Every number here comes from executing the real builder/reader/apply path
//! over the shared `CountingStore`; nulls are only ever produced by a
//! capability gap, never by estimate.

use std::error::Error;
use std::time::Instant;

use bytes::Bytes;
use nectar_testing::run;

use mantaray_old::{Manifest, PlainManifest};
use nectar_manifest::{Builder, Changeset, Entry, Key, KeyId, Metadata, Reader, V1, apply};
use nectar_primitives::{ChunkAddress, ChunkRef, StandardChunkSet};
use primitives_old::chunk::ChunkAddress as ChunkAddress02;

use crate::corpus::{Corpus, GenKey, tagged_addr, value_addr};
use crate::results::{
    BatchPoint, BatchUpdate, Build, Cdf, Ceiling, Cell, Depth, Floor, Get, Histogram, IterFull,
    KfCount, LatQuad, Listing, LoadLatency, OpCost, Range, RangeWindow, SingleUpdate, Storage,
    SubtreeDelete, Update, ValueRead, WallLatency,
};
use crate::store::CountingStore;
use crate::store02::OldCountingStore;

type Err = Box<dyn Error>;

const BODY: f64 = 4096.0;

/// RTT values (ms) for the hop x RTT illustrative wall-clock model.
const RTT_SET: [u32; 3] = [25, 50, 75];
/// Batch-size sweep for the write-amplification amortisation curve.
const BATCH_KS: [usize; 5] = [1, 10, 100, 1_000, 10_000];
/// Range-window widths (fraction of the sorted key domain) for the sweep.
const RANGE_WS: [f64; 4] = [0.001, 0.01, 0.10, 1.0];
/// A key that sorts strictly above every corpus key (ceiling upper bound).
const MAX_KEY: [u8; 48] = [0xff; 48];

/// Config knobs shared across cells.
#[derive(Clone, Copy, Debug)]
pub struct Cfg {
    pub sample_keys: usize,
    pub update_sample: usize,
    pub batch_ops: usize,
    pub rtt_ms: u32,
}

// ---- small stats helpers -------------------------------------------------

fn mean(v: &[u64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let sum: u128 = v.iter().map(|&x| u128::from(x)).sum();
    sum as f64 / v.len() as f64
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted.get(idx).copied().unwrap_or(0)
}

/// Discrete PMF of a small-integer sample (hops are 1..~150).
fn histogram(values: &[u64]) -> Histogram {
    let mut h: Histogram = Histogram::new();
    for &v in values {
        *h.entry(v).or_insert(0) += 1;
    }
    h
}

fn cdf(sorted: &[u64]) -> Cdf {
    Cdf {
        p50: pct(sorted, 0.50),
        p90: pct(sorted, 0.90),
        p99: pct(sorted, 0.99),
        max: sorted.last().copied().unwrap_or(0),
    }
}

/// The hop x RTT illustrative wall-clock model, one quad per RTT.
fn wall_latency(sorted: &[u64]) -> WallLatency {
    let c = cdf(sorted);
    let mut by_rtt = std::collections::BTreeMap::new();
    for rtt in RTT_SET {
        let r = f64::from(rtt);
        by_rtt.insert(
            rtt.to_string(),
            LatQuad {
                p50: c.p50 as f64 * r,
                p90: c.p90 as f64 * r,
                p99: c.p99 as f64 * r,
                max: c.max as f64 * r,
            },
        );
    }
    WallLatency {
        by_rtt_ms: by_rtt,
        model: "hops * rtt, sequential fetch, no pipelining or caching".to_string(),
    }
}

fn depth_from(hops: &mut [u64]) -> Depth {
    hops.sort_unstable();
    Depth {
        min: hops.first().copied(),
        mean: Some(mean(hops)),
        p95: Some(pct(hops, 0.95)),
        max: hops.last().copied(),
        histogram: Some(histogram(hops)),
        fanout_mean: None,
    }
}

fn get_block(hops: &[u64], rtt_ms: u32) -> Get {
    let mut sorted = hops.to_vec();
    sorted.sort_unstable();
    let m = mean(hops);
    let p95 = pct(&sorted, 0.95);
    let mx = sorted.last().copied().unwrap_or(0);
    let rtt = f64::from(rtt_ms);
    Get {
        sampled_keys: Some(hops.len() as u64),
        hops_mean: Some(m),
        hops_p95: Some(p95),
        hops_max: Some(mx),
        load_latency_ms: Some(LoadLatency {
            mean: Some(m * rtt),
            p95: Some(p95 as f64 * rtt),
            max: Some(mx as f64 * rtt),
            derived_from_hops: true,
        }),
        criterion_ns_per_op: None,
        hops_histogram: Some(histogram(hops)),
        hops_cdf: Some(cdf(&sorted)),
        wall_latency_ms: Some(wall_latency(&sorted)),
    }
}

/// Per-key `bytes/key`, `chunks/key`, embedding ratio.
fn storage_block(snap_chunks: u64, live_bytes: u64, n: u64, embedded: Option<u64>) -> Storage {
    let total_chunks = snap_chunks;
    let bpk = if n == 0 {
        0.0
    } else {
        live_bytes as f64 / n as f64
    };
    let cpk = if n == 0 {
        0.0
    } else {
        total_chunks as f64 / n as f64
    };
    // 0.2 never embeds: caller passes None -> explicit 0.0, not null.
    let emb = match embedded {
        Some(e) => {
            let denom = e + total_chunks;
            if denom == 0 {
                0.0
            } else {
                e as f64 / denom as f64
            }
        }
        None => 0.0,
    };
    Storage {
        total_chunks: Some(total_chunks),
        total_payload_bytes: Some(live_bytes),
        storage_utilisation: Some(if total_chunks == 0 {
            0.0
        } else {
            live_bytes as f64 / (total_chunks as f64 * BODY)
        }),
        bytes_per_key: Some(bpk),
        chunks_per_key: Some(cpk),
        embedding_ratio: Some(emb),
    }
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

/// Peak resident set of the process, sampled from `/proc/self/status` VmHWM.
/// Cumulative over the process lifetime, so it is a floor on the current
/// build's peak, not a clean per-cell figure (see `peak_live_store_bytes`).
fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}

// ---- value / key builders ------------------------------------------------

fn ref32(addr: [u8; 32]) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new(addr))
}

fn entry10(bytes: &[u8]) -> Entry<V1> {
    Entry::from(ref32(value_addr(bytes)))
}

fn alt_entry10(bytes: &[u8]) -> Entry<V1> {
    Entry::from(ref32(tagged_addr(b"upd", bytes)))
}

fn meta10(k: &GenKey) -> Option<Metadata<V1>> {
    k.content_type.map(|ct| {
        Metadata::<V1>::new(KeyId::ContentType, Bytes::from_static(ct.as_bytes()))
            .expect("content-type fits the metadata bound")
    })
}

/// A synthetic insert key in a namespace disjoint from every corpus key
/// (uniform hex is `0-9a-f`, kiwix starts `A/M/-/I`, osm starts with a digit),
/// so an insert adds a fresh branch and never nests under an existing key.
fn insert_key(tag: usize) -> (Vec<u8>, String) {
    let path = format!("~~ins~~{tag}");
    let mut raw = vec![0xffu8, 0xff, 0xff];
    raw.extend_from_slice(&(tag as u64).to_le_bytes());
    (raw, path)
}

// ========================================================================
// mantaray 1.0
// ========================================================================

/// Measure one 1.0 cell. `store` is fresh; only manifest node chunks land in
/// it (values are bare ref32, never split), so `total_chunks` is exactly the
/// resident node count.
pub fn measure_10(corpus: Corpus, keys: &[GenKey], cfg: Cfg) -> Result<Cell, Err> {
    let store = CountingStore::<StandardChunkSet>::new();
    let n = keys.len();

    // --- build ---
    let mut builder = Builder::<V1>::new();
    for k in keys {
        builder.insert(Key::from(k.raw.as_slice()), entry10(&k.raw), meta10(k));
    }
    let rss_before = peak_rss_bytes();
    let t = Instant::now();
    let built = run(builder.build(&store))?;
    let build_ns = t.elapsed().as_nanos() as u64;
    let rss_after = peak_rss_bytes();
    let root = *built.root();
    let stats = built.stats();
    let snap = store.snapshot();

    // Second identical build into the same store: by I6 no new distinct chunk
    // should land. Capped to protect the 1e6 wall-clock budget.
    let (dedup, dedup_reason) = if n <= 100_000 {
        let first_distinct = snap.distinct_puts;
        let before = store.snapshot().distinct_puts;
        let mut b2 = Builder::<V1>::new();
        for k in keys {
            b2.insert(Key::from(k.raw.as_slice()), entry10(&k.raw), meta10(k));
        }
        let _ = run(b2.build(&store))?;
        let after = store.snapshot().distinct_puts;
        let ratio = if first_distinct == 0 {
            0.0
        } else {
            (after - before) as f64 / first_distinct as f64
        };
        (Some(ratio), None)
    } else {
        (
            None,
            Some("second build skipped above 1e5 to bound wall-clock".to_string()),
        )
    };

    let build = Build {
        wall_ns: Some(build_ns),
        criterion_ns_per_op: None,
        criterion_stddev_ns: None,
        peak_open_nodes: Some(stats.peak_open_nodes() as u64),
        nodes_written: Some(stats.nodes_written() as u64),
        nodes_embedded: Some(stats.nodes_embedded() as u64),
        peak_rss_bytes: rss_after.or(rss_before),
        peak_live_store_bytes: Some(snap.peak_live_bytes),
        builder_frontier_nodes: Some(stats.peak_open_nodes() as u64),
        cpu_loglog: None,
        dedup_ratio_second_build: dedup,
        dedup_reason,
    };
    let total_chunks = snap.total_chunks;
    let storage = storage_block(
        total_chunks,
        snap.live_bytes,
        n as u64,
        Some(stats.nodes_embedded() as u64),
    );

    // --- get hops (== referenced chunk-path depth) over the sample ---
    let reader: Reader<&CountingStore<StandardChunkSet>, V1> = Reader::new(&store);
    let idxs = sample_indices(n, cfg.sample_keys);
    let mut hops: Vec<u64> = Vec::with_capacity(idxs.len());
    for &i in &idxs {
        let key = Key::from(keys[i].raw.as_slice());
        let before = store.gets();
        let _ = run(reader.get(&root, &key))?;
        hops.push(store.gets() - before);
    }
    let get = get_block(&hops, cfg.rtt_ms);
    let mut depth_src = hops.clone();
    let tree_depth = depth_from(&mut depth_src);

    // --- prefix listing (folder view) over a fixed set of prefixes ---
    let mut fetch_total = 0u64;
    let mut keys_total = 0u64;
    for p in listing_prefixes(corpus, keys) {
        let pk = Key::from(p.as_slice());
        let before = store.gets();
        let mut cursor = run(reader.prefix(&root, &pk))?;
        let mut count = 0u64;
        while let Some(_pair) = run(cursor.next())? {
            count += 1;
        }
        fetch_total += store.gets() - before;
        keys_total += count;
    }
    let listing = Listing {
        method: "prefix".to_string(),
        fetch_count: Some(fetch_total),
        keys_returned: Some(keys_total),
        fetches_per_key: Some(if keys_total == 0 {
            0.0
        } else {
            fetch_total as f64 / keys_total as f64
        }),
        fallback_02_full_entries: None,
        fallback_02_walk_from: None,
        multiplier_fair: None,
    };

    // --- floor (1.0 only) over the sample ---
    let mut floor_hops: Vec<u64> = Vec::with_capacity(idxs.len());
    for &i in &idxs {
        let key = Key::from(keys[i].raw.as_slice());
        let before = store.gets();
        let _ = run(reader.floor(&root, &key))?;
        floor_hops.push(store.gets() - before);
    }
    floor_hops.sort_unstable();
    let floor = Floor {
        supported: true,
        reason: None,
        hops_mean: Some(mean(&floor_hops)),
        hops_p95: Some(pct(&floor_hops, 0.95)),
        hops_max: floor_hops.last().copied(),
        hops_histogram: Some(histogram(&floor_hops)),
        fallback_02: None,
    };

    // --- ceiling (neither format names it): 1.0 emulates by seek, so measure
    //     the range(key, MAX).next() hop cost per sampled key ---
    let hi_all = Key::from(MAX_KEY.as_slice());
    let mut ceil_hops: Vec<u64> = Vec::with_capacity(idxs.len());
    for &i in &idxs {
        let key = Key::from(keys[i].raw.as_slice());
        let before = store.gets();
        let mut cursor = run(reader.range(&root, &key, &hi_all))?;
        let _ = run(cursor.next())?;
        ceil_hops.push(store.gets() - before);
    }
    ceil_hops.sort_unstable();
    let ceiling = Ceiling {
        supported: true,
        class: "seek_emulated".to_string(),
        native_seek_hops_mean: Some(mean(&ceil_hops)),
        native_seek_hops_max: ceil_hops.last().copied(),
        fallback_02: None,
    };

    // --- range (1.0 only): selectivity sweep of centred windows ---
    let mut windows: Vec<RangeWindow> = Vec::with_capacity(RANGE_WS.len());
    for &w in &RANGE_WS {
        let (lo_i, hi_i) = window_indices(n, w);
        let lo = Key::from(keys[lo_i].raw.as_slice());
        let hi = Key::from(keys[hi_i].raw.as_slice());
        let before = store.gets();
        let mut cursor = run(reader.range(&root, &lo, &hi))?;
        let mut count = 0u64;
        while let Some(_pair) = run(cursor.next())? {
            count += 1;
        }
        windows.push(RangeWindow {
            w,
            fetch_count: Some(store.gets() - before),
            keys_returned: Some(count),
            fallback_02_fetches: None,
            multiplier: None,
            reason_if_null: None,
        });
    }
    let range = Range {
        supported: true,
        reason: None,
        windows,
    };

    // --- ordered full iter: fetches to first key, then to drain all ---
    let before = store.gets();
    let mut it = run(reader.iter(&root))?;
    let _first = run(it.next())?;
    let to_first = store.gets() - before;
    let mut all = to_first;
    loop {
        let g0 = store.gets();
        if run(it.next())?.is_none() {
            break;
        }
        all += store.gets() - g0;
    }
    let iter_full = IterFull {
        native_fetch_to_first_key: Some(to_first),
        native_fetch_all: Some(all),
        fallback_02_materialise_fetches: None,
        ordered_guaranteed: true,
    };

    // --- value read: this harness binds ref32 values that are never stored as
    //     content chunks, so a read-through / inline study is not exercised ---
    let value_read = ValueRead {
        inline_fraction: None,
        fetches_native_mean: None,
        fetches_02_mean: None,
        chunks_saved_by_inline: None,
        reason: Some(
            "values are synthetic ref32 addresses, not stored content chunks; inline read-through not exercised"
                .to_string(),
        ),
    };

    // --- single-key update / insert / delete (apply, 1-op changeset) ---
    let usample = sample_indices(n, cfg.update_sample);
    let mut upd = (Vec::new(), Vec::new());
    let mut ins = (Vec::new(), Vec::new());
    let mut del = (Vec::new(), Vec::new());
    for (tag, &i) in usample.iter().enumerate() {
        let k = &keys[i];
        // update
        let mut cs = Changeset::<V1>::new();
        cs.put(Key::from(k.raw.as_slice()), alt_entry10(&k.raw), meta10(k));
        apply_measure(&store, &root, &cs, &mut upd)?;
        // insert
        let (iraw, _ipath) = insert_key(tag);
        let mut cs = Changeset::<V1>::new();
        cs.put(Key::from(iraw.as_slice()), entry10(&iraw), None);
        apply_measure(&store, &root, &cs, &mut ins)?;
        // delete
        let mut cs = Changeset::<V1>::new();
        cs.remove(Key::from(k.raw.as_slice()));
        apply_measure(&store, &root, &cs, &mut del)?;
    }

    // --- batch apply (one changeset) ---
    let kops = n.min(cfg.batch_ops);
    let mut cs = Changeset::<V1>::new();
    let all_update = matches!(corpus, Corpus::OsmPyramid | Corpus::OsmBbox);
    for (i, k) in keys.iter().take(kops).enumerate() {
        let slot = i % 10;
        if all_update || slot < 8 {
            cs.put(Key::from(k.raw.as_slice()), alt_entry10(&k.raw), meta10(k));
        } else if slot == 8 {
            let (iraw, _ip) = insert_key(1_000_000 + i);
            cs.put(Key::from(iraw.as_slice()), entry10(&iraw), None);
        } else {
            cs.remove(Key::from(k.raw.as_slice()));
        }
    }
    let before_p = store.puts();
    let t = Instant::now();
    let _new_root = run(apply(&store, &root, &cs))?;
    let batch_ns = t.elapsed().as_nanos() as u64;
    let batch_chunks = store.puts() - before_p;
    let batch = BatchUpdate {
        k_ops: Some(kops as u64),
        mix: if all_update {
            "100/0/0".to_string()
        } else {
            "80/10/10".to_string()
        },
        chunks_rewritten: Some(batch_chunks),
        wall_ns: Some(batch_ns),
        write_amplification: Some(if kops == 0 {
            0.0
        } else {
            batch_chunks as f64 / kops as f64
        }),
        criterion_ns_per_op: None,
    };

    // --- batch-size sweep: one changeset per K, same mix ---
    let mut batch_sweep: Vec<BatchPoint> = Vec::with_capacity(BATCH_KS.len());
    let mix_label = if all_update { "100/0/0" } else { "80/10/10" };
    for &k in &BATCH_KS {
        let kk = n.min(k);
        let mut cs = Changeset::<V1>::new();
        for (i, key) in keys.iter().take(kk).enumerate() {
            let slot = i % 10;
            if all_update || slot < 8 {
                cs.put(
                    Key::from(key.raw.as_slice()),
                    alt_entry10(&key.raw),
                    meta10(key),
                );
            } else if slot == 8 {
                let (iraw, _ip) = insert_key(2_000_000 + i);
                cs.put(Key::from(iraw.as_slice()), entry10(&iraw), None);
            } else {
                cs.remove(Key::from(key.raw.as_slice()));
            }
        }
        let before_p = store.puts();
        let t = Instant::now();
        let _ = run(apply(&store, &root, &cs))?;
        let ns = t.elapsed().as_nanos() as u64;
        let chunks = store.puts() - before_p;
        batch_sweep.push(BatchPoint {
            k: kk as u64,
            mix: mix_label.to_string(),
            chunks_rewritten: chunks,
            wall_ns: ns,
            write_amplification: if kk == 0 {
                0.0
            } else {
                chunks as f64 / kk as f64
            },
            ns_per_op: if kk == 0 { 0.0 } else { ns as f64 / kk as f64 },
        });
    }

    // --- subtree delete: remove every key under one listing prefix ---
    let subtree_delete = subtree_delete_10(&store, &reader, &root, corpus, keys)?;

    let update = Update {
        single: Some(SingleUpdate {
            update: Some(op_cost(&upd)),
            insert: Some(op_cost(&ins)),
            delete: Some(op_cost(&del)),
            criterion_ns_per_op: None,
        }),
        batch: Some(batch),
        batch_sweep,
        subtree_delete: Some(subtree_delete),
    };

    Ok(Cell {
        ran: true,
        reason: None,
        n_keys: Some(n as u64),
        key_encoding: Some("raw".to_string()),
        build: Some(build),
        storage: Some(storage),
        tree_depth: Some(tree_depth),
        get: Some(get),
        listing: Some(listing),
        floor: Some(floor),
        ceiling: Some(ceiling),
        range: Some(range),
        iter_full: Some(iter_full),
        value_read: Some(value_read),
        update: Some(update),
        full_entries_walk_fetches: None,
    })
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

/// Delete every key under the first listing prefix in one changeset.
fn subtree_delete_10(
    store: &CountingStore<StandardChunkSet>,
    reader: &Reader<&CountingStore<StandardChunkSet>, V1>,
    root: &ChunkAddress,
    corpus: Corpus,
    keys: &[GenKey],
) -> Result<SubtreeDelete, Err> {
    let prefixes = listing_prefixes(corpus, keys);
    let Some(prefix) = prefixes.first() else {
        return Ok(SubtreeDelete {
            prefix_utf8: None,
            keys_deleted: 0,
            chunks_rewritten: 0,
            wall_ns: 0,
            reason: Some("no prefix available".to_string()),
        });
    };
    let pk = Key::from(prefix.as_slice());
    let mut cs = Changeset::<V1>::new();
    let mut deleted = 0u64;
    let mut cursor = run(reader.prefix(root, &pk))?;
    while let Some((key, _entry)) = run(cursor.next())? {
        cs.remove(key);
        deleted += 1;
    }
    let before_p = store.puts();
    let t = Instant::now();
    let _ = run(apply(store, root, &cs))?;
    let ns = t.elapsed().as_nanos() as u64;
    Ok(SubtreeDelete {
        prefix_utf8: String::from_utf8(prefix.clone()).ok(),
        keys_deleted: deleted,
        chunks_rewritten: store.puts() - before_p,
        wall_ns: ns,
        reason: None,
    })
}

fn apply_measure(
    store: &CountingStore<StandardChunkSet>,
    root: &ChunkAddress,
    cs: &Changeset<V1>,
    acc: &mut (Vec<u64>, Vec<u64>),
) -> Result<(), Err> {
    let before = store.puts();
    let t = Instant::now();
    let _ = run(apply(store, root, cs))?;
    acc.0.push(store.puts() - before);
    acc.1.push(t.elapsed().as_nanos() as u64);
    Ok(())
}

fn op_cost(acc: &(Vec<u64>, Vec<u64>)) -> OpCost {
    OpCost {
        chunks_rewritten_mean: Some(mean(&acc.0)),
        wall_ns: Some(mean(&acc.1).round() as u64),
    }
}

fn listing_prefixes(corpus: Corpus, keys: &[GenKey]) -> Vec<Vec<u8>> {
    let n = keys.len();
    if n == 0 {
        return Vec::new();
    }
    let mut out: Vec<Vec<u8>> = Vec::new();
    for j in 0..8 {
        let idx = (j * n / 8).min(n - 1);
        let raw = &keys[idx].raw;
        let p: Vec<u8> = match corpus {
            Corpus::Uniform => raw.iter().take(1).copied().collect(),
            _ => match raw.iter().rposition(|&b| b == b'/') {
                Some(pos) => raw[..=pos].to_vec(),
                None => raw.iter().take(1).copied().collect(),
            },
        };
        if !out.contains(&p) {
            out.push(p);
        }
    }
    out
}

// ========================================================================
// mantaray 0.2
// ========================================================================

type Store02 = OldCountingStore;

/// Measure one 0.2 cell as a full reader+writer over the plain (ref32) trie.
pub fn measure_02(corpus: Corpus, keys: &[GenKey], cfg: Cfg) -> Result<Cell, Err> {
    let store = Store02::new();
    let n = keys.len();

    // --- build: add-loop then one save ---
    let mut m: PlainManifest<&Store02> = Manifest::new(&store);
    let rss_before = peak_rss_bytes();
    let t = Instant::now();
    for k in keys {
        let r = value_addr(k.path.as_bytes());
        match k.content_type {
            Some(ct) => {
                let mut md = std::collections::BTreeMap::new();
                md.insert("content-type".to_string(), ct.to_string());
                run(m.add_with_metadata(&k.path, r, md))?;
            }
            None => run(m.add(&k.path, r))?,
        }
    }
    let root = run(m.save())?;
    let build_ns = t.elapsed().as_nanos() as u64;
    let rss_after = peak_rss_bytes();
    let snap = store.snapshot();

    // Second identical build: 0.2 obfuscation randomisation may defeat dedup,
    // so measure how many new distinct chunks a re-save produces.
    let (dedup, dedup_reason) = if n <= 100_000 {
        let first_distinct = snap.distinct_puts;
        let before = store.snapshot().distinct_puts;
        let mut m2: PlainManifest<&Store02> = Manifest::new(&store);
        for k in keys {
            let r = value_addr(k.path.as_bytes());
            run(m2.add(&k.path, r))?;
        }
        let _ = run(m2.save())?;
        let after = store.snapshot().distinct_puts;
        let ratio = if first_distinct == 0 {
            0.0
        } else {
            (after - before) as f64 / first_distinct as f64
        };
        (Some(ratio), None)
    } else {
        (
            None,
            Some("second build skipped above 1e5 to bound wall-clock".to_string()),
        )
    };

    let build = Build {
        wall_ns: Some(build_ns),
        criterion_ns_per_op: None,
        criterion_stddev_ns: None,
        peak_open_nodes: None,
        nodes_written: Some(snap.distinct_puts),
        nodes_embedded: None,
        peak_rss_bytes: rss_after.or(rss_before),
        peak_live_store_bytes: Some(snap.peak_live_bytes),
        // 0.2 holds the whole mutable trie in RAM at save: frontier = O(N).
        builder_frontier_nodes: Some(snap.distinct_puts),
        cpu_loglog: None,
        dedup_ratio_second_build: dedup,
        dedup_reason,
    };
    let total_chunks = snap.total_chunks;
    // 0.2 never embeds children: pass None so embedding_ratio is explicit 0.0.
    let storage = storage_block(total_chunks, snap.live_bytes, n as u64, None);

    // --- get hops over the sample (fresh open, no trie caching) ---
    let mut reader: PlainManifest<&Store02> = Manifest::open(root, &store);
    let idxs = sample_indices(n, cfg.sample_keys);
    let mut hops: Vec<u64> = Vec::with_capacity(idxs.len());
    for &i in &idxs {
        let before = store.gets();
        let _ = run(reader.lookup(&keys[i].path))?;
        hops.push(store.gets() - before);
    }
    let get = get_block(&hops, cfg.rtt_ms);
    let mut depth_src = hops.clone();
    let mut tree_depth = depth_from(&mut depth_src);

    // Fanout: mean live children over internal (forked) nodes, via a full walk.
    let mut fork_sum = 0u64;
    let mut internal = 0u64;
    let mut mw: PlainManifest<&Store02> = Manifest::open(root, &store);
    run(mw.walk(&mut |_path, node| {
        let f = node.forks().len() as u64;
        if f > 0 {
            fork_sum += f;
            internal += 1;
        }
        Ok(())
    }))?;
    tree_depth.fanout_mean = Some(if internal == 0 {
        0.0
    } else {
        fork_sum as f64 / internal as f64
    });

    // --- listing: pessimal full entries() walk, plus the FAIR walk_from on the
    //     same prefixes a 1.0 caller would list ---
    let before = store.gets();
    let entries = run(reader.entries())?;
    let walk_fetches = store.gets() - before;
    let keys_returned = entries.len() as u64;
    let full_entries = KfCount {
        fetches: walk_fetches,
        keys_returned,
        fetches_per_key: if keys_returned == 0 {
            0.0
        } else {
            walk_fetches as f64 / keys_returned as f64
        },
    };
    // Fair fallback: walk_from(prefix) over the same logical prefixes as 1.0.
    // walk_from only resolves an EXACT node-boundary path; a prefix that lands
    // mid-edge raises NoForkFound. That is itself a 0.2 limitation, so such a
    // prefix contributes nothing and is skipped rather than aborting the run.
    let mut wf_fetches = 0u64;
    let mut wf_keys = 0u64;
    let mut wf_resolved = 0u64;
    for pstr in listing_prefixes_02(corpus, keys) {
        let before = store.gets();
        let mut leaves = 0u64;
        let mut mwf: PlainManifest<&Store02> = Manifest::open(root, &store);
        let r = run(mwf.walk_from(&pstr, &mut |_p, node| {
            if node.is_value() {
                leaves += 1;
            }
            Ok(())
        }));
        if r.is_ok() {
            wf_fetches += store.gets() - before;
            wf_keys += leaves;
            wf_resolved += 1;
        }
    }
    let walk_from_opt = (wf_resolved > 0).then(|| KfCount {
        fetches: wf_fetches,
        keys_returned: wf_keys,
        fetches_per_key: if wf_keys == 0 {
            0.0
        } else {
            wf_fetches as f64 / wf_keys as f64
        },
    });
    let listing = Listing {
        method: "walk_from_fair + full_entries_pessimal".to_string(),
        fetch_count: Some(wf_fetches),
        keys_returned: Some(wf_keys),
        fetches_per_key: walk_from_opt.as_ref().map(|k| k.fetches_per_key),
        fallback_02_full_entries: Some(full_entries),
        fallback_02_walk_from: walk_from_opt,
        multiplier_fair: None,
    };

    // floor / ceiling / range: no ordered/seekable API; the real 0.2 emulation
    // is the full entries() walk (O(N)), whose fetch count == walk_fetches.
    let floor = Floor {
        supported: false,
        reason: Some("no ordered/seekable API; emulated by full entries() walk".to_string()),
        hops_mean: None,
        hops_p95: None,
        hops_max: None,
        hops_histogram: None,
        fallback_02: None,
    };
    let ceiling = Ceiling {
        supported: false,
        class: "unsupported".to_string(),
        native_seek_hops_mean: None,
        native_seek_hops_max: None,
        fallback_02: None,
    };
    let range = Range {
        supported: false,
        reason: Some("no ordered/seekable API; emulated by full entries() walk".to_string()),
        windows: Vec::new(),
    };
    let iter_full = IterFull {
        native_fetch_to_first_key: None,
        native_fetch_all: None,
        fallback_02_materialise_fetches: Some(walk_fetches),
        // entries() DFS order is undocumented and entries_concurrent is
        // explicitly unordered: no ordered-iter guarantee.
        ordered_guaranteed: false,
    };
    let value_read = ValueRead {
        inline_fraction: None,
        fetches_native_mean: None,
        fetches_02_mean: None,
        chunks_saved_by_inline: None,
        reason: Some("no inline entries in 0.2; values are references".to_string()),
    };

    // --- single-key update / insert / delete: add/remove + save ---
    let usample = sample_indices(n, cfg.update_sample);
    let mut upd = (Vec::new(), Vec::new());
    let mut ins = (Vec::new(), Vec::new());
    let mut del = (Vec::new(), Vec::new());
    for (tag, &i) in usample.iter().enumerate() {
        let k = &keys[i];
        // update
        let r = tagged_addr(b"upd", k.path.as_bytes());
        save_measure(&store, root, &mut upd, |mm| run(mm.add(&k.path, r)))?;
        // insert
        let (_iraw, ipath) = insert_key(tag);
        let ir = value_addr(ipath.as_bytes());
        save_measure(&store, root, &mut ins, |mm| run(mm.add(&ipath, ir)))?;
        // delete
        save_measure(&store, root, &mut del, |mm| run(mm.remove(&k.path)))?;
    }

    // --- batch: {K adds/removes} + one save ---
    let kops = n.min(cfg.batch_ops);
    let all_update = matches!(corpus, Corpus::OsmPyramid | Corpus::OsmBbox);
    let mut mb: PlainManifest<&Store02> = Manifest::open(root, &store);
    let before_p = store.puts();
    let t = Instant::now();
    for (i, k) in keys.iter().take(kops).enumerate() {
        let slot = i % 10;
        if all_update || slot < 8 {
            let r = tagged_addr(b"upd", k.path.as_bytes());
            run(mb.add(&k.path, r))?;
        } else if slot == 8 {
            let (_ir, ip) = insert_key(1_000_000 + i);
            let r = value_addr(ip.as_bytes());
            run(mb.add(&ip, r))?;
        } else {
            run(mb.remove(&k.path))?;
        }
    }
    let _ = run(mb.save())?;
    let batch_ns = t.elapsed().as_nanos() as u64;
    let batch_chunks = store.puts() - before_p;
    let batch = BatchUpdate {
        k_ops: Some(kops as u64),
        mix: if all_update {
            "100/0/0 (adds+save)".to_string()
        } else {
            "80/10/10 (adds+save)".to_string()
        },
        chunks_rewritten: Some(batch_chunks),
        wall_ns: Some(batch_ns),
        write_amplification: Some(if kops == 0 {
            0.0
        } else {
            batch_chunks as f64 / kops as f64
        }),
        criterion_ns_per_op: None,
    };

    // --- batch-size sweep: K mutations + one save per K ---
    let mut batch_sweep: Vec<BatchPoint> = Vec::with_capacity(BATCH_KS.len());
    let mix_label = if all_update {
        "100/0/0 (adds+save)"
    } else {
        "80/10/10 (adds+save)"
    };
    for &k in &BATCH_KS {
        let kk = n.min(k);
        let mut ms: PlainManifest<&Store02> = Manifest::open(root, &store);
        let before_p = store.puts();
        let t = Instant::now();
        for (i, key) in keys.iter().take(kk).enumerate() {
            let slot = i % 10;
            if all_update || slot < 8 {
                let r = tagged_addr(b"upd", key.path.as_bytes());
                run(ms.add(&key.path, r))?;
            } else if slot == 8 {
                let (_ir, ip) = insert_key(2_000_000 + i);
                let r = value_addr(ip.as_bytes());
                run(ms.add(&ip, r))?;
            } else {
                run(ms.remove(&key.path))?;
            }
        }
        let _ = run(ms.save())?;
        let ns = t.elapsed().as_nanos() as u64;
        let chunks = store.puts() - before_p;
        batch_sweep.push(BatchPoint {
            k: kk as u64,
            mix: mix_label.to_string(),
            chunks_rewritten: chunks,
            wall_ns: ns,
            write_amplification: if kk == 0 {
                0.0
            } else {
                chunks as f64 / kk as f64
            },
            ns_per_op: if kk == 0 { 0.0 } else { ns as f64 / kk as f64 },
        });
    }

    // --- subtree delete: remove every key under one listing prefix + save ---
    let subtree_delete = subtree_delete_02(&store, root, corpus, keys)?;

    let update = Update {
        single: Some(SingleUpdate {
            update: Some(op_cost(&upd)),
            insert: Some(op_cost(&ins)),
            delete: Some(op_cost(&del)),
            criterion_ns_per_op: None,
        }),
        batch: Some(batch),
        batch_sweep,
        subtree_delete: Some(subtree_delete),
    };

    Ok(Cell {
        ran: true,
        reason: None,
        n_keys: Some(n as u64),
        key_encoding: Some(corpus.key_encoding().to_string()),
        build: Some(build),
        storage: Some(storage),
        tree_depth: Some(tree_depth),
        get: Some(get),
        listing: Some(listing),
        floor: Some(floor),
        ceiling: Some(ceiling),
        range: Some(range),
        iter_full: Some(iter_full),
        value_read: Some(value_read),
        update: Some(update),
        full_entries_walk_fetches: Some(walk_fetches),
    })
}

/// 0.2 listing prefixes matching the 1.0 logical prefixes: for uniform (hex
/// paths) the 1-raw-byte prefix maps to its 2-hex-char string; path corpora
/// are byte-identical.
fn listing_prefixes_02(corpus: Corpus, keys: &[GenKey]) -> Vec<String> {
    listing_prefixes(corpus, keys)
        .into_iter()
        .filter_map(|p| match corpus {
            Corpus::Uniform => Some(bytes_to_hex(&p)),
            _ => String::from_utf8(p).ok(),
        })
        .collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[usize::from(b >> 4)] as char);
        s.push(HEX[usize::from(b & 0x0f)] as char);
    }
    s
}

/// Delete every 0.2 key under the first listing prefix, one save.
fn subtree_delete_02(
    store: &Store02,
    root: ChunkAddress02,
    corpus: Corpus,
    keys: &[GenKey],
) -> Result<SubtreeDelete, Err> {
    let Some(prefix) = listing_prefixes_02(corpus, keys).into_iter().next() else {
        return Ok(SubtreeDelete {
            prefix_utf8: None,
            keys_deleted: 0,
            chunks_rewritten: 0,
            wall_ns: 0,
            reason: Some("no prefix available".to_string()),
        });
    };
    let victims: Vec<&str> = keys
        .iter()
        .map(|k| k.path.as_str())
        .filter(|p| p.starts_with(&prefix))
        .collect();
    let mut mm: PlainManifest<&Store02> = Manifest::open(root, store);
    let before_p = store.puts();
    let t = Instant::now();
    for p in &victims {
        run(mm.remove(p))?;
    }
    let _ = run(mm.save())?;
    let ns = t.elapsed().as_nanos() as u64;
    Ok(SubtreeDelete {
        prefix_utf8: Some(prefix),
        keys_deleted: victims.len() as u64,
        chunks_rewritten: store.puts() - before_p,
        wall_ns: ns,
        reason: None,
    })
}

fn save_measure<Fn>(
    store: &Store02,
    root: ChunkAddress02,
    acc: &mut (Vec<u64>, Vec<u64>),
    mutate: Fn,
) -> Result<(), Err>
where
    Fn: FnOnce(&mut PlainManifest<&Store02>) -> Result<(), mantaray_old::MantarayError>,
{
    let mut mm: PlainManifest<&Store02> = Manifest::open(root, store);
    let before = store.puts();
    let t = Instant::now();
    mutate(&mut mm)?;
    let _ = run(mm.save())?;
    acc.0.push(store.puts() - before);
    acc.1.push(t.elapsed().as_nanos() as u64);
    Ok(())
}
