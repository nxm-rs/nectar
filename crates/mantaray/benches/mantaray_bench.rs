#![allow(missing_docs)]
// Bench, example, and integration-test code: unwraps, direct indexing,
// casts, and assertions are setup and illustration, not shipped surface.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic,
    clippy::panic_in_result_fn,
    clippy::as_conversions,
    clippy::missing_panics_doc
)]
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use nectar_mantaray::{Cursor, ManifestEditor, MemoryStore, Reader, hazmat};
use nectar_primitives::StandardChunkSet;
use nectar_primitives::chunk::{ChunkAddress, ChunkOps};
use nectar_primitives::store::ChunkGet;
use nectar_testing::run;

type Store = MemoryStore<StandardChunkSet>;
type Editor = ManifestEditor<Store>;

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

/// Paths for the larger-scale benchmarks.
fn large_paths(count: usize) -> Vec<(String, ChunkAddress)> {
    (0..count)
        .map(|i| {
            let path = format!("dir{}/subdir{}/file{}.dat", i / 100, i / 10, i);
            let addr = make_addr(path.as_bytes());
            (path, addr)
        })
        .collect()
}

/// Commit the SPA website dataset, returning root and store.
fn build_spa() -> (ChunkAddress, Store) {
    let mut editor = Editor::new(Store::new());
    for &p in SPA_PATHS {
        editor.put(p, make_addr(p.as_bytes()));
    }
    run(editor.commit()).unwrap()
}

/// Commit `count` generated paths, returning root and store.
fn build_large(count: usize) -> (ChunkAddress, Store) {
    let mut editor = Editor::new(Store::new());
    for (path, addr) in large_paths(count) {
        editor.put(path, addr);
    }
    run(editor.commit()).unwrap()
}

fn bench_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit");

    group.bench_function("spa_paths", |b| {
        b.iter(build_spa);
    });

    for &count in &[100, 500, 1000] {
        let entries = large_paths(count);
        group.bench_with_input(BenchmarkId::new("paths", count), &entries, |b, entries| {
            b.iter(|| {
                let mut editor = Editor::new(Store::new());
                for (path, addr) in entries {
                    editor.put(path, *addr);
                }
                run(editor.commit()).unwrap()
            });
        });
    }

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    let (root, store) = build_spa();
    let reader = Reader::new(store);

    group.bench_function("existing_path", |b| {
        b.iter(|| {
            let entry = run(reader.get(&root, b"js/app.js")).unwrap();
            entry.is_some()
        });
    });

    let (large_root, large_store) = build_large(500);
    let large_reader = Reader::new(large_store);

    group.bench_function("500_paths_deep", |b| {
        b.iter(|| {
            let entry = run(large_reader.get(&large_root, b"dir4/subdir49/file499.dat")).unwrap();
            entry.is_some()
        });
    });

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove");

    group.bench_function("single_leaf", |b| {
        b.iter_batched(
            build_spa,
            |(root, store)| {
                let mut editor = Editor::open(root, store);
                editor.remove("js/app.js");
                run(editor.commit()).unwrap()
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_has_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("has_prefix");

    let (root, store) = build_spa();
    let reader = Reader::new(store);

    group.bench_function("existing_prefix", |b| {
        b.iter(|| run(reader.has_prefix(&root, b"js/")).unwrap());
    });

    group.bench_function("missing_prefix", |b| {
        b.iter(|| run(reader.has_prefix(&root, b"nonexistent/")).unwrap());
    });

    group.finish();
}

/// The committed root node's wire image, for the raw codec benches.
fn root_node_bytes() -> Vec<u8> {
    let (root, store) = build_spa();
    let chunk = run(ChunkGet::get(&store, &root)).unwrap();
    chunk.envelope().data().to_vec()
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");

    let data = root_node_bytes();
    let node = hazmat::decode::<nectar_primitives::chunk::ChunkRef>(&data).unwrap();

    group.bench_function("spa_root_node", |b| {
        b.iter(|| hazmat::encode(&node).unwrap());
    });

    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    let data = root_node_bytes();

    group.bench_function("spa_root_node", |b| {
        b.iter(|| hazmat::decode::<nectar_primitives::chunk::ChunkRef>(data.as_slice()).unwrap());
    });

    group.finish();
}

/// Drain the ordered listing cursor, returning the entry count.
fn drain_cursor(root: ChunkAddress, store: &Store) -> u32 {
    run(async {
        let mut cursor: Cursor<Store> = Cursor::new(store.clone(), root);
        let mut count = 0u32;
        while let Some(entry) = cursor.next().await {
            entry.unwrap();
            count += 1;
        }
        count
    })
}

fn bench_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list");

    group.bench_function("spa_trie", |b| {
        let (root, store) = build_spa();
        b.iter(|| drain_cursor(root, &store));
    });

    for &count in &[100, 500] {
        group.bench_with_input(BenchmarkId::new("paths", count), &count, |b, &count| {
            let (root, store) = build_large(count);
            b.iter(|| drain_cursor(root, &store));
        });
    }

    group.finish();
}

fn bench_full_workflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_workflow");

    group.bench_function("commit_then_lookup", |b| {
        b.iter(|| {
            let (root, store) = build_spa();
            let reader = Reader::new(store);
            let paths: &[&[u8]] = &[
                b"css/app.css",
                b"favicon.ico",
                b"img/logo.png",
                b"index.html",
                b"js/app.js",
            ];
            for &p in paths {
                run(reader.get(&root, p)).unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_commit,
    bench_get,
    bench_remove,
    bench_has_prefix,
    bench_encode,
    bench_decode,
    bench_list,
    bench_full_workflow,
);
criterion_main!(benches);
