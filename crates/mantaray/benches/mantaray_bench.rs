#![allow(missing_docs)]

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use nectar_mantaray::node::Node;
use nectar_mantaray::persist::MockStoreCell;
use nectar_mantaray::{walk, walk_node};

/// Create a 32-byte entry from a path, left-padded with zeroes.
fn make_entry(path: &[u8]) -> Vec<u8> {
    let mut entry = vec![0u8; 32 - path.len().min(32)];
    entry.extend_from_slice(&path[..path.len().min(32)]);
    entry.truncate(32);
    entry
}

/// Build a trie with the "spa website" dataset (11 paths).
fn build_spa_trie() -> Node {
    let paths: &[&[u8]] = &[
        b"css/",
        b"css/app.css",
        b"favicon.ico",
        b"img/",
        b"img/logo.png",
        b"index.html",
        b"js/",
        b"js/chunk-vendors.js.map",
        b"js/chunk-vendors.js",
        b"js/app.js.map",
        b"js/app.js",
    ];
    let mut n = Node::default();
    n.obfuscation_key = vec![0u8; 32];
    for &p in paths {
        let e = make_entry(p);
        n.add(p, &e, BTreeMap::new(), None).unwrap();
    }
    n
}

/// Build a trie with many paths for larger-scale benchmarks.
fn build_large_trie(count: usize) -> Node {
    let mut n = Node::default();
    n.obfuscation_key = vec![0u8; 32];
    for i in 0..count {
        let path = format!("dir{}/subdir{}/file{}.dat", i / 100, i / 10, i);
        let e = make_entry(path.as_bytes());
        n.add(path.as_bytes(), &e, BTreeMap::new(), None).unwrap();
    }
    n
}

fn bench_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("add");

    let paths: &[&[u8]] = &[
        b"index.html",
        b"img/1.png",
        b"img/2.png",
        b"img/test/oho.png",
        b"img/test/old/test.png",
        b"robots.txt",
        b"css/app.css",
        b"js/app.js",
    ];

    group.bench_function("8_paths", |b| {
        b.iter(|| {
            let mut n = Node::default();
            n.obfuscation_key = vec![0u8; 32];
            for &p in paths {
                let e = make_entry(p);
                n.add(p, &e, BTreeMap::new(), None).unwrap();
            }
            n
        });
    });

    for &count in &[100, 500, 1000] {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
            .map(|i| {
                let path = format!("dir{}/subdir{}/file{}.dat", i / 100, i / 10, i);
                let entry = make_entry(path.as_bytes());
                (path.into_bytes(), entry)
            })
            .collect();

        group.bench_with_input(BenchmarkId::new("paths", count), &entries, |b, entries| {
            b.iter(|| {
                let mut n = Node::default();
                n.obfuscation_key = vec![0u8; 32];
                for (path, entry) in entries {
                    n.add(path, entry, BTreeMap::new(), None).unwrap();
                }
                n
            });
        });
    }

    group.finish();
}

fn bench_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup");

    let mut n = build_spa_trie();

    group.bench_function("existing_path", |b| {
        b.iter(|| {
            let r = n.lookup(b"js/app.js", None).unwrap();
            r.len()
        });
    });

    group.bench_function("root_path", |b| {
        b.iter(|| {
            let r = n.lookup(b"", None).unwrap();
            r.len()
        });
    });

    // lookup in a larger trie
    let mut large = build_large_trie(500);

    group.bench_function("500_paths_deep", |b| {
        b.iter(|| {
            let r = large.lookup(b"dir4/subdir49/file499.dat", None).unwrap();
            r.len()
        });
    });

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    let mut group = c.benchmark_group("remove");

    group.bench_function("single_leaf", |b| {
        b.iter_batched(
            build_spa_trie,
            |mut n| {
                n.remove(b"js/app.js", None).unwrap();
                n
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_has_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("has_prefix");

    let mut n = build_spa_trie();

    group.bench_function("existing_prefix", |b| {
        b.iter(|| n.has_prefix(b"js/", None).unwrap());
    });

    group.bench_function("missing_prefix", |b| {
        b.iter(|| n.has_prefix(b"nonexistent/", None).unwrap());
    });

    group.finish();
}

fn bench_marshal(c: &mut Criterion) {
    let mut group = c.benchmark_group("marshal");

    // build and assign fake refs so marshal works
    let mut n = build_spa_trie();
    let mut counter = 0u32;
    assign_refs(&mut n, &mut counter);

    group.bench_function("spa_trie", |b| {
        b.iter(|| n.marshal_binary().unwrap());
    });

    group.finish();
}

/// Recursively assign deterministic references to all forks.
fn assign_refs(node: &mut Node, counter: &mut u32) {
    for fork in node.forks.values_mut() {
        let mut ref_ = vec![0u8; 32];
        let bytes = counter.to_be_bytes();
        ref_[28..].copy_from_slice(&bytes);
        fork.node.ref_ = ref_;
        *counter += 1;
        assign_refs(&mut fork.node, counter);
    }
}

fn bench_unmarshal(c: &mut Criterion) {
    let mut group = c.benchmark_group("unmarshal");

    // build, assign refs, marshal to get the binary data
    let mut n = build_spa_trie();
    let mut counter = 0u32;
    assign_refs(&mut n, &mut counter);
    let data = n.marshal_binary().unwrap();

    group.bench_function("spa_trie", |b| {
        b.iter(|| {
            let mut node = Node::default();
            let mut d = data.clone();
            node.unmarshal_binary(&mut d).unwrap();
            node
        });
    });

    group.finish();
}

fn bench_walk(c: &mut Criterion) {
    let mut group = c.benchmark_group("walk");

    group.bench_function("spa_trie", |b| {
        let mut n = build_spa_trie();
        b.iter(|| {
            let mut count = 0u32;
            walk(&mut n, None, &mut |_path, _node| {
                count += 1;
                Ok(())
            })
            .unwrap();
            count
        });
    });

    for &count in &[100, 500] {
        group.bench_with_input(BenchmarkId::new("paths", count), &count, |b, &count| {
            let mut n = build_large_trie(count);
            b.iter(|| {
                let mut visited = 0u32;
                walk(&mut n, None, &mut |_path, _node| {
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

fn bench_walk_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("walk_node");

    let mut n = build_spa_trie();

    group.bench_function("from_root", |b| {
        b.iter(|| {
            let mut count = 0u32;
            walk_node(&mut n, b"", None, &mut |_path, _node| {
                count += 1;
                Ok(())
            })
            .unwrap();
            count
        });
    });

    group.bench_function("from_subtree", |b| {
        b.iter(|| {
            let mut count = 0u32;
            walk_node(&mut n, b"js/", None, &mut |_path, _node| {
                count += 1;
                Ok(())
            })
            .unwrap();
            count
        });
    });

    group.finish();
}

fn bench_save_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("save_load");

    group.bench_function("save_spa_trie", |b| {
        b.iter_batched(
            || {
                let store = MockStoreCell::new();
                let n = build_spa_trie();
                (n, store)
            },
            |(mut n, store)| {
                n.save(&store).unwrap();
                (n, store)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("load_spa_trie", |b| {
        let store = MockStoreCell::new();
        let mut n = build_spa_trie();
        n.save(&store).unwrap();
        let ref_ = n.ref_.clone();

        b.iter(|| {
            let mut node = Node::new_node_ref(&ref_);
            node.load(Some(&store)).unwrap();
            node
        });
    });

    group.bench_function("save_load_roundtrip", |b| {
        b.iter_batched(
            || {
                let store = MockStoreCell::new();
                let n = build_spa_trie();
                (n, store)
            },
            |(mut n, store)| {
                n.save(&store).unwrap();
                let mut n2 = Node::new_node_ref(&n.ref_);
                n2.load(Some(&store)).unwrap();
                n2
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
            let store = MockStoreCell::new();
            let mut n = build_spa_trie();

            // save
            n.save(&store).unwrap();
            let ref_ = n.ref_.clone();

            // load into fresh node
            let mut n2 = Node::new_node_ref(&ref_);

            // lookup all paths
            let paths: &[&[u8]] = &[
                b"css/app.css",
                b"favicon.ico",
                b"img/logo.png",
                b"index.html",
                b"js/app.js",
            ];
            for &p in paths {
                n2.lookup(p, Some(&store)).unwrap();
            }
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
    bench_walk_node,
    bench_save_load,
    bench_full_workflow,
);
criterion_main!(benches);
