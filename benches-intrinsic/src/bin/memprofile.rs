//! Single-shot memory and work profile: runs every scenario once under a
//! counting global allocator and writes JSON lines per suite.
//!
//! Kept apart from the criterion targets so allocator accounting never
//! contaminates the timing runs. `--smoke` restricts to the smallest sizes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use nectar_benches_intrinsic::corpus::{self, Corpus, Shape, SplitMix64};
use nectar_benches_intrinsic::file_api::{FileLegacy, FilePipeline, FileStreaming};
use nectar_benches_intrinsic::manifest_api::{
    Manifest10, ManifestApi, Mantaray02, manifest10_count, manifest10_select,
};
use nectar_benches_intrinsic::results::Suite;
use nectar_benches_intrinsic::store::CountingStore;
use nectar_primitives::chunk::ChunkAddress;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static BASELINE: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicU64 = AtomicU64::new(0);

/// System allocator wrapper tracking current and peak bytes plus counts.
struct CountingAlloc;

fn track_alloc(size: usize) {
    let current = CURRENT.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(current, Ordering::Relaxed);
    ALLOCS.fetch_add(1, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            track_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            track_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                track_alloc(new_size - layout.size());
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

/// One scenario's cost: peak heap over baseline and allocation count.
/// No timing here: the counting allocator would contaminate it. Criterion
/// owns all speed numbers.
#[derive(Clone, Copy, Debug)]
struct Cost {
    peak_bytes: u64,
    allocs: u64,
}

/// Run `f` once with reset allocator accounting.
fn measured<T>(f: impl FnOnce() -> T) -> (T, Cost) {
    let base = CURRENT.load(Ordering::Relaxed);
    BASELINE.store(base, Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    ALLOCS.store(0, Ordering::Relaxed);
    let out = std::hint::black_box(f());
    let peak = PEAK.load(Ordering::Relaxed);
    let base = BASELINE.load(Ordering::Relaxed);
    let cost = Cost {
        peak_bytes: u64::try_from(peak.saturating_sub(base)).unwrap_or(u64::MAX),
        allocs: ALLOCS.load(Ordering::Relaxed),
    };
    (out, cost)
}

fn emit_cost(out: &mut Suite, impl_name: &str, scenario: &str, cost: Cost) {
    out.emit(
        impl_name,
        scenario,
        "alloc_peak_bytes",
        cost.peak_bytes as f64,
        "bytes",
    );
    out.emit(impl_name, scenario, "alloc_count", cost.allocs as f64, "allocs");
}

fn manifest_impl<M: ManifestApi>(out: &mut Suite, corpus: &Corpus, label: &str) -> ChunkAddress {
    let store = CountingStore::new();
    let n = corpus.entries.len() as u64;

    let scenario = format!("build/{label}");
    let (root, cost) = measured(|| M::build(&store, &corpus.entries));
    emit_cost(out, M::NAME, &scenario, cost);
    out.emit(M::NAME, &scenario, "chunks_written", store.puts() as f64, "chunks");
    out.emit(M::NAME, &scenario, "bytes_written", store.put_bytes() as f64, "bytes");

    let scenario = format!("lookup/{label}");
    store.reset_counters();
    let (found, cost) = measured(|| {
        let mut found = 0u64;
        for key in &corpus.lookup_hits {
            if M::get(&store, &root, key).is_some() {
                found += 1;
            }
        }
        for key in &corpus.lookup_misses {
            assert!(M::get(&store, &root, key).is_none(), "miss key found");
        }
        found
    });
    assert_eq!(found, corpus.lookup_hits.len() as u64, "hit key missing");
    emit_cost(out, M::NAME, &scenario, cost);
    let lookups = (corpus.lookup_hits.len() + corpus.lookup_misses.len()) as f64;
    out.emit(M::NAME, &scenario, "chunks_fetched", store.gets() as f64, "chunks");
    out.emit(
        M::NAME,
        &scenario,
        "chunks_fetched_per_lookup",
        store.gets() as f64 / lookups,
        "chunks",
    );

    let scenario = format!("iter/{label}");
    store.reset_counters();
    let (count, cost) = measured(|| M::iter_all(&store, &root));
    assert_eq!(count, n, "full iteration count");
    emit_cost(out, M::NAME, &scenario, cost);
    out.emit(M::NAME, &scenario, "chunks_fetched", store.gets() as f64, "chunks");

    let scenario = format!("prefix/{label}");
    store.reset_counters();
    let (count, cost) = measured(|| M::iter_prefix(&store, &root, &corpus.prefix));
    assert!(count >= 1, "prefix scan empty");
    emit_cost(out, M::NAME, &scenario, cost);
    out.emit(M::NAME, &scenario, "entries", count as f64, "entries");
    out.emit(M::NAME, &scenario, "chunks_fetched", store.gets() as f64, "chunks");

    let scenario = format!("range/{label}");
    store.reset_counters();
    let (count, cost) =
        measured(|| M::iter_range(&store, &root, &corpus.range_lo, &corpus.range_hi));
    assert_eq!(count, corpus.range_len(), "range count");
    emit_cost(out, M::NAME, &scenario, cost);
    out.emit(M::NAME, &scenario, "chunks_fetched", store.gets() as f64, "chunks");

    let scenario = format!("edit/{label}");
    let fork = store.deep_fork();
    let (new_root, cost) = measured(|| M::edit(&fork, &root, &corpus.inserts, &corpus.removes));
    assert_ne!(new_root, root, "edit left the root unchanged");
    emit_cost(out, M::NAME, &scenario, cost);
    out.emit(M::NAME, &scenario, "chunks_written", fork.puts() as f64, "chunks");
    out.emit(
        M::NAME,
        &scenario,
        "chunks_written_new",
        fork.puts_new() as f64,
        "chunks",
    );
    root
}

fn manifest_suite(out: &mut Suite, sizes: &[usize]) {
    for shape in Shape::ALL {
        for &n in sizes {
            let corpus = Corpus::generate(n, shape, corpus::SEED);
            let label = format!("{}/{n}", shape.name());
            let root10 = manifest_impl::<Manifest10>(out, &corpus, &label);
            manifest_impl::<Mantaray02>(out, &corpus, &label);

            // Order statistics exist only in 1.0; recorded for the record,
            // never folded into comparative aggregates. N/A for 0.2.
            let store = CountingStore::new();
            let root = Manifest10::build(&store, &corpus.entries);
            assert_eq!(root, root10, "rebuild diverged");
            let scenario = format!("order-count/{label}");
            let (count, cost) =
                measured(|| manifest10_count(&store, &root, &corpus.range_lo, &corpus.range_hi));
            assert_eq!(count, corpus.range_len(), "order count");
            emit_cost(out, Manifest10::NAME, &scenario, cost);
            let scenario = format!("order-select/{label}");
            let (len, cost) = measured(|| manifest10_select(&store, &root, (n as u64) / 2));
            assert!(len > 0, "select missed");
            emit_cost(out, Manifest10::NAME, &scenario, cost);
        }
    }
}

fn file_impl<P: FilePipeline>(out: &mut Suite, data: &[u8], label: &str) {
    let store = CountingStore::new();
    let scenario = format!("split/{label}");
    let (root, cost) = measured(|| P::split(&store, data));
    emit_cost(out, P::NAME, &scenario, cost);
    out.emit(P::NAME, &scenario, "chunks_written", store.puts() as f64, "chunks");
    out.emit(P::NAME, &scenario, "bytes_written", store.put_bytes() as f64, "bytes");

    let scenario = format!("join/{label}");
    store.reset_counters();
    let (joined, cost) = measured(|| P::join(&store, &root));
    assert_eq!(joined, data, "join mismatch");
    emit_cost(out, P::NAME, &scenario, cost);
    out.emit(P::NAME, &scenario, "chunks_fetched", store.gets() as f64, "chunks");
}

fn file_suite(out: &mut Suite, payloads: &[usize]) {
    for &len in payloads {
        let data = corpus::payload(len, corpus::SEED);
        let label = format!("{len}");
        file_impl::<FileStreaming>(out, &data, &label);
        file_impl::<FileLegacy>(out, &data, &label);
    }
}

fn main() {
    let smoke = std::env::args().any(|arg| arg == "--smoke");
    let sizes: Vec<usize> = if smoke {
        vec![corpus::SIZES[0]]
    } else {
        corpus::SIZES.to_vec()
    };
    let payloads: Vec<usize> = if smoke {
        vec![4 << 10]
    } else {
        vec![4 << 10, 1 << 20, 32 << 20]
    };

    // Exercise the RNG once so a broken stream fails loud before any suite.
    let mut rng = SplitMix64::new(corpus::SEED);
    assert_ne!(rng.next_u64(), 0);

    let mut manifest_out = Suite::create("manifest");
    manifest_suite(&mut manifest_out, &sizes);
    let mut file_out = Suite::create("file");
    file_suite(&mut file_out, &payloads);
    println!(
        "wrote {}",
        nectar_benches_intrinsic::results::results_dir().display()
    );
}
