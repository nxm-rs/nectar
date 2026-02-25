#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use nectar_mantaray::{PlainManifest, MemorySink};
use nectar_mantaray::node::Node;
use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
use nectar_primitives::chunk::ChunkAddress;

type Store = MemorySink<DEFAULT_BODY_SIZE>;

/// Create a ChunkAddress from a path, left-padded with zeroes.
fn make_addr(path: &[u8]) -> ChunkAddress {
    let mut buf = [0u8; 32];
    let len = path.len().min(32);
    buf[32 - len..].copy_from_slice(&path[..len]);
    ChunkAddress::from(buf)
}

/// SPA website paths used in multiple benchmarks.
const SPA_PATHS: &[&str] = &[
    "css/",
    "css/app.css",
    "favicon.ico",
    "img/",
    "img/logo.png",
    "index.html",
    "js/",
    "js/chunk-vendors.js.map",
    "js/chunk-vendors.js",
    "js/app.js.map",
    "js/app.js",
];

/// Build a PlainManifest with the SPA website dataset.
fn build_spa_manifest() -> PlainManifest<Store> {
    let store = Store::new();
    let mut m = PlainManifest::new(store);
    for &p in SPA_PATHS {
        let addr = make_addr(p.as_bytes());
        m.add(p, addr).unwrap();
    }
    m
}

/// Build a PlainManifest with many paths for larger-scale benchmarks.
fn build_large_manifest(count: usize) -> PlainManifest<Store> {
    let store = Store::new();
    let mut m = PlainManifest::new(store);
    for i in 0..count {
        let path = format!("dir{}/subdir{}/file{}.dat", i / 100, i / 10, i);
        let addr = make_addr(path.as_bytes());
        m.add(&path, addr).unwrap();
    }
    m
}

fn bench_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("add");

    let paths: &[&str] = &[
        "index.html",
        "img/1.png",
        "img/2.png",
        "img/test/oho.png",
        "img/test/old/test.png",
        "robots.txt",
        "css/app.css",
        "js/app.js",
    ];

    group.bench_function("8_paths", |b| {
        b.iter(|| {
            let store = Store::new();
            let mut m = PlainManifest::new(store);
            for &p in paths {
                let addr = make_addr(p.as_bytes());
                m.add(p, addr).unwrap();
            }
            m
        });
    });

    for &count in &[100, 500, 1000] {
        let entries: Vec<(String, ChunkAddress)> = (0..count)
            .map(|i| {
                let path = format!("dir{}/subdir{}/file{}.dat", i / 100, i / 10, i);
                let addr = make_addr(path.as_bytes());
                (path, addr)
            })
            .collect();

        group.bench_with_input(BenchmarkId::new("paths", count), &entries, |b, entries| {
            b.iter(|| {
                let store = Store::new();
                let mut m = PlainManifest::new(store);
                for (path, addr) in entries {
                    m.add(path, *addr).unwrap();
                }
                m
            });
        });
    }

    group.finish();
}

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup");

    let mut m = build_spa_manifest();

    group.bench_function("existing_path", |b| {
        b.iter(|| {
            let entry = m.lookup("js/app.js").unwrap();
            entry.address().is_some()
        });
    });

    // lookup in a larger trie
    let mut large = build_large_manifest(500);

    group.bench_function("500_paths_deep", |b| {
        b.iter(|| {
            let entry = large.lookup("dir4/subdir49/file499.dat").unwrap();
            entry.address().is_some()
        });
    });

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove");

    group.bench_function("single_leaf", |b| {
        b.iter_batched(
            build_spa_manifest,
            |mut m| {
                m.remove("js/app.js").unwrap();
                m
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_has_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("has_prefix");

    let mut m = build_spa_manifest();

    group.bench_function("existing_prefix", |b| {
        b.iter(|| m.has_prefix("js/").unwrap());
    });

    group.bench_function("missing_prefix", |b| {
        b.iter(|| m.has_prefix("nonexistent/").unwrap());
    });

    group.finish();
}

fn bench_marshal(c: &mut Criterion) {
    let mut group = c.benchmark_group("marshal");

    // Build a trie via PlainManifest, save to get references assigned, reload.
    let mut m = build_spa_manifest();
    let root_ref = m.save().unwrap();
    let (_, store) = m.into_parts();
    let mut m2 = PlainManifest::open(root_ref, store);
    // Force-load the entire trie
    m2.walk(&mut |_, _| Ok(())).unwrap();
    let (n, _) = m2.into_parts();

    group.bench_function("spa_trie", |b| {
        b.iter(|| Vec::<u8>::try_from(&n).unwrap());
    });

    group.finish();
}

fn bench_unmarshal(c: &mut Criterion) {
    let mut group = c.benchmark_group("unmarshal");

    // Build trie via PlainManifest, save, marshal to get binary data.
    let mut m = build_spa_manifest();
    let root_ref = m.save().unwrap();
    let (_, store) = m.into_parts();
    let mut m2 = PlainManifest::open(root_ref, store);
    m2.walk(&mut |_, _| Ok(())).unwrap();
    let (n, _) = m2.into_parts();
    let data = Vec::<u8>::try_from(&n).unwrap();

    group.bench_function("spa_trie", |b| {
        b.iter(|| Node::try_from(data.as_slice()).unwrap());
    });

    group.finish();
}

fn bench_walk(c: &mut Criterion) {
    let mut group = c.benchmark_group("walk");

    group.bench_function("spa_trie", |b| {
        let mut m = build_spa_manifest();
        b.iter(|| {
            let mut count = 0u32;
            m.walk(&mut |_path, _node| {
                count += 1;
                Ok(())
            })
            .unwrap();
            count
        });
    });

    for &count in &[100, 500] {
        group.bench_with_input(BenchmarkId::new("paths", count), &count, |b, &count| {
            let mut m = build_large_manifest(count);
            b.iter(|| {
                let mut visited = 0u32;
                m.walk(&mut |_path, _node| {
                    visited += 1;
                    Ok(())
                })
                .unwrap();
                visited
            });
        });
    }

    group.finish();
}

fn bench_save_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("save_load");

    group.bench_function("save_spa_trie", |b| {
        b.iter_batched(
            build_spa_manifest,
            |mut m| {
                m.save().unwrap();
                m
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("load_spa_trie", |b| {
        let mut m = build_spa_manifest();
        let root_ref = m.save().unwrap();
        let (_, store) = m.into_parts();

        b.iter(|| {
            let mut m2 = PlainManifest::open(root_ref, store.clone());
            let _ = m2.lookup("index.html").unwrap();
            m2
        });
    });

    group.bench_function("save_load_roundtrip", |b| {
        b.iter_batched(
            build_spa_manifest,
            |mut m| {
                let root_ref = m.save().unwrap();
                let (_, store) = m.into_parts();
                let mut m2 = PlainManifest::open(root_ref, store);
                let _ = m2.lookup("index.html").unwrap();
                m2
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_full_workflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_workflow");

    group.bench_function("add_save_load_lookup", |b| {
        b.iter(|| {
            let mut m = build_spa_manifest();
            let root_ref = m.save().unwrap();
            let (_, store) = m.into_parts();
            let mut m2 = PlainManifest::open(root_ref, store);

            let paths: &[&str] = &[
                "css/app.css",
                "favicon.ico",
                "img/logo.png",
                "index.html",
                "js/app.js",
            ];
            for &p in paths {
                m2.lookup(p).unwrap();
            }
        });
    });

    group.finish();
}

fn bench_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter");

    // In-memory iteration (no save/load)
    group.bench_function("spa_trie_in_memory", |b| {
        let mut m = build_spa_manifest();

        b.iter(|| {
            let mut count = 0u32;
            for result in m.iter() {
                result.unwrap();
                count += 1;
            }
            count
        });
    });

    // Lazy iteration after save/load (exercises storage loading)
    group.bench_function("spa_trie_lazy", |b| {
        let mut m = build_spa_manifest();
        let root_ref = m.save().unwrap();
        let (_, store) = m.into_parts();

        b.iter(|| {
            let mut m2 = PlainManifest::open(root_ref, store.clone());
            let mut count = 0u32;
            for result in m2.iter() {
                result.unwrap();
                count += 1;
            }
            count
        });
    });

    // Compare with entries() (walk-based collection)
    group.bench_function("entries_spa_trie", |b| {
        let mut m = build_spa_manifest();

        b.iter(|| {
            let entries = m.entries().unwrap();
            entries.len()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_add,
    bench_lookup,
    bench_remove,
    bench_has_prefix,
    bench_marshal,
    bench_unmarshal,
    bench_walk,
    bench_save_load,
    bench_full_workflow,
    bench_iter,
);
criterion_main!(benches);
