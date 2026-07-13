//! Criterion ns/op benches for build / get / apply, both formats.
//!
//! Timed on the kiwix corpus (a natively byte-identical path corpus, the fair
//! headline comparison) at N in {1e3, 1e4, 1e5}. Build is timed to 1e4 to keep
//! the 0.2 whole-trie seal within a sane wall budget; get and single-key apply
//! are cheap and timed to 1e5. Ids match `criterion_fold::bench_id`, so the
//! sim bin folds these straight into the result cells.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;

use nectar_manifest::{Builder, Changeset, Entry, Key, KeyId, Metadata, Reader, V1, apply};
use nectar_mantaray::{Manifest, PlainManifest};
use nectar_primitives::{AnyChunkSet, ChunkAddress, ChunkRef, StandardChunkSet};

use nectar_manifest_sim::corpus::{self, Corpus, GenKey, tagged_addr, value_addr};
use nectar_manifest_sim::criterion_fold::bench_id;
use nectar_manifest_sim::store::CountingStore;

const F10: &str = "mantaray_1_0";
const F02: &str = "mantaray_0_2";
const BUILD_SCALES: [u64; 2] = [1_000, 10_000];
const OP_SCALES: [u64; 3] = [1_000, 10_000, 100_000];

fn ref32(addr: [u8; 32]) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new(addr))
}

fn meta10(k: &GenKey) -> Option<Metadata<V1>> {
    k.content_type.map(|ct| {
        Metadata::<V1>::new(KeyId::ContentType, Bytes::from_static(ct.as_bytes())).unwrap()
    })
}

fn build_10(keys: &[GenKey]) -> ChunkAddress {
    let store = CountingStore::<StandardChunkSet>::new();
    let mut b = Builder::<V1>::new();
    for k in keys {
        b.insert(
            Key::from(k.raw.as_slice()),
            Entry::from(ref32(value_addr(&k.raw))),
            meta10(k),
        );
    }
    *block_on(b.build(&store)).unwrap().root()
}

fn build_02(keys: &[GenKey]) -> ChunkAddress {
    let store = CountingStore::<AnyChunkSet<4096>>::new();
    let mut m: PlainManifest<&CountingStore<AnyChunkSet<4096>>> = Manifest::new(&store);
    for k in keys {
        block_on(m.add(&k.path, ref32(value_addr(k.path.as_bytes())))).unwrap();
    }
    block_on(m.save()).unwrap()
}

fn bench_build(c: &mut Criterion) {
    let corpus = Corpus::Kiwix;
    let mut g = c.benchmark_group("build");
    for &scale in &BUILD_SCALES {
        let keys = corpus::generate(corpus, scale as usize);
        g.bench_with_input(
            BenchmarkId::from_parameter(bench_id(F10, corpus.name(), scale)),
            &keys,
            |b, keys| b.iter(|| build_10(keys)),
        );
        g.bench_with_input(
            BenchmarkId::from_parameter(bench_id(F02, corpus.name(), scale)),
            &keys,
            |b, keys| b.iter(|| build_02(keys)),
        );
    }
    g.finish();
}

fn bench_get(c: &mut Criterion) {
    let corpus = Corpus::Kiwix;
    let mut g = c.benchmark_group("get");
    for &scale in &OP_SCALES {
        let keys = corpus::generate(corpus, scale as usize);
        let probe = keys[keys.len() / 2].clone();

        // 1.0
        let store10 = CountingStore::<StandardChunkSet>::new();
        let mut b10 = Builder::<V1>::new();
        for k in &keys {
            b10.insert(
                Key::from(k.raw.as_slice()),
                Entry::from(ref32(value_addr(&k.raw))),
                meta10(k),
            );
        }
        let root10 = *block_on(b10.build(&store10)).unwrap().root();
        let reader10: Reader<&CountingStore<StandardChunkSet>, V1> = Reader::new(&store10);
        let key10 = Key::from(probe.raw.as_slice());
        g.bench_with_input(
            BenchmarkId::from_parameter(bench_id(F10, corpus.name(), scale)),
            &scale,
            |b, _| b.iter(|| block_on(reader10.get(&root10, &key10)).unwrap()),
        );

        // 0.2
        let store02 = CountingStore::<AnyChunkSet<4096>>::new();
        let root02 = {
            let mut m: PlainManifest<&CountingStore<AnyChunkSet<4096>>> = Manifest::new(&store02);
            for k in &keys {
                block_on(m.add(&k.path, ref32(value_addr(k.path.as_bytes())))).unwrap();
            }
            block_on(m.save()).unwrap()
        };
        let reader02: PlainManifest<&CountingStore<AnyChunkSet<4096>>> =
            Manifest::open(root02, &store02);
        let path = probe.path.clone();
        g.bench_with_input(
            BenchmarkId::from_parameter(bench_id(F02, corpus.name(), scale)),
            &scale,
            |b, _| b.iter(|| block_on(reader02.get(&path)).unwrap()),
        );
    }
    g.finish();
}

fn bench_apply(c: &mut Criterion) {
    let corpus = Corpus::Kiwix;
    let mut g = c.benchmark_group("apply");
    for &scale in &OP_SCALES {
        let keys = corpus::generate(corpus, scale as usize);
        let probe = keys[keys.len() / 2].clone();

        // 1.0: single-key apply
        let store10 = CountingStore::<StandardChunkSet>::new();
        let mut b10 = Builder::<V1>::new();
        for k in &keys {
            b10.insert(
                Key::from(k.raw.as_slice()),
                Entry::from(ref32(value_addr(&k.raw))),
                meta10(k),
            );
        }
        let root10 = *block_on(b10.build(&store10)).unwrap().root();
        let key10 = Key::from(probe.raw.as_slice());
        let val10 = Entry::from(ref32(tagged_addr(b"upd", &probe.raw)));
        g.bench_with_input(
            BenchmarkId::from_parameter(bench_id(F10, corpus.name(), scale)),
            &scale,
            |b, _| {
                b.iter(|| {
                    let mut cs = Changeset::<V1>::new();
                    cs.put(key10.clone(), val10.clone(), None);
                    block_on(apply(&store10, &root10, &cs)).unwrap()
                })
            },
        );

        // 0.2: single-key add + save
        let store02 = CountingStore::<AnyChunkSet<4096>>::new();
        let root02 = {
            let mut m: PlainManifest<&CountingStore<AnyChunkSet<4096>>> = Manifest::new(&store02);
            for k in &keys {
                block_on(m.add(&k.path, ref32(value_addr(k.path.as_bytes())))).unwrap();
            }
            block_on(m.save()).unwrap()
        };
        let path = probe.path.clone();
        let upd = ref32(tagged_addr(b"upd", probe.path.as_bytes()));
        g.bench_with_input(
            BenchmarkId::from_parameter(bench_id(F02, corpus.name(), scale)),
            &scale,
            |b, _| {
                b.iter(|| {
                    let mut m: PlainManifest<&CountingStore<AnyChunkSet<4096>>> =
                        Manifest::open(root02, &store02);
                    block_on(m.add(&path, upd)).unwrap();
                    block_on(m.save()).unwrap()
                })
            },
        );
    }
    g.finish();
}

criterion_group!(benches, bench_build, bench_get, bench_apply);
criterion_main!(benches);
