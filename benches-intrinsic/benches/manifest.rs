//! Manifest timing suite: 1.0 vs 0.2 through the shared drivers.

#![allow(missing_docs)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::as_conversions
)]

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use nectar_benches_intrinsic::corpus::{Corpus, SEED, SIZES, Shape};
use nectar_benches_intrinsic::manifest_api::{
    Manifest10, ManifestApi, Mantaray02, manifest10_count, manifest10_select,
};
use nectar_benches_intrinsic::store::CountingStore;

fn manifest_suite<M: ManifestApi>(c: &mut Criterion) {
    for shape in Shape::ALL {
        for &n in &SIZES {
            let corpus = Corpus::generate(n, shape, SEED);
            let param = format!("{}/{n}", shape.name());

            {
                let mut group = c.benchmark_group("build");
                group.sample_size(10);
                group.bench_function(BenchmarkId::new(M::NAME, &param), |b| {
                    b.iter_batched(
                        CountingStore::new,
                        |store| black_box(M::build(&store, &corpus.entries)),
                        BatchSize::LargeInput,
                    );
                });
                group.finish();
            }

            let store = CountingStore::new();
            let root = M::build(&store, &corpus.entries);

            {
                let mut group = c.benchmark_group("lookup");
                group.sample_size(10);
                group.bench_function(BenchmarkId::new(M::NAME, &param), |b| {
                    b.iter(|| {
                        let mut found = 0u64;
                        for key in &corpus.lookup_hits {
                            if M::get(&store, &root, key).is_some() {
                                found += 1;
                            }
                        }
                        for key in &corpus.lookup_misses {
                            if M::get(&store, &root, key).is_some() {
                                found += 1;
                            }
                        }
                        black_box(found)
                    });
                });
                group.finish();
            }

            {
                let mut group = c.benchmark_group("iter");
                group.sample_size(10);
                group.bench_function(BenchmarkId::new(M::NAME, &param), |b| {
                    b.iter(|| black_box(M::iter_all(&store, &root)));
                });
                group.finish();
            }

            {
                let mut group = c.benchmark_group("prefix");
                group.sample_size(10);
                group.bench_function(BenchmarkId::new(M::NAME, &param), |b| {
                    b.iter(|| black_box(M::iter_prefix(&store, &root, &corpus.prefix)));
                });
                group.finish();
            }

            {
                let mut group = c.benchmark_group("range");
                group.sample_size(10);
                group.bench_function(BenchmarkId::new(M::NAME, &param), |b| {
                    b.iter(|| {
                        black_box(M::iter_range(&store, &root, &corpus.range_lo, &corpus.range_hi))
                    });
                });
                group.finish();
            }

            {
                let mut group = c.benchmark_group("edit");
                group.sample_size(10);
                group.bench_function(BenchmarkId::new(M::NAME, &param), |b| {
                    b.iter_batched(
                        || store.deep_fork(),
                        |fork| black_box(M::edit(&fork, &root, &corpus.inserts, &corpus.removes)),
                        BatchSize::LargeInput,
                    );
                });
                group.finish();
            }
        }
    }
}

/// Order statistics exist only in 1.0: recorded for the record, N/A for 0.2
/// and never folded into comparative aggregates.
fn order_suite(c: &mut Criterion) {
    for shape in Shape::ALL {
        for &n in &SIZES {
            let corpus = Corpus::generate(n, shape, SEED);
            let param = format!("{}/{n}", shape.name());
            let store = CountingStore::new();
            let root = Manifest10::build(&store, &corpus.entries);

            let mut group = c.benchmark_group("order");
            group.sample_size(10);
            group.bench_function(BenchmarkId::new("manifest10/count", &param), |b| {
                b.iter(|| {
                    black_box(manifest10_count(
                        &store,
                        &root,
                        &corpus.range_lo,
                        &corpus.range_hi,
                    ))
                });
            });
            group.bench_function(BenchmarkId::new("manifest10/select", &param), |b| {
                b.iter(|| black_box(manifest10_select(&store, &root, (n as u64) / 2)));
            });
            group.finish();
        }
    }
}

criterion_group!(
    benches,
    manifest_suite::<Manifest10>,
    manifest_suite::<Mantaray02>,
    order_suite
);
criterion_main!(benches);
