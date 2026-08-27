//! Both adapters carry a caller retrieval policy into the seam's data loads.
//!
//! The accessor alone proves nothing, so the fetch window is observed through
//! a store that parks every get on a gate: a single-slot window admits one
//! get at a time, where the default window overlaps them.

#![allow(
    clippy::as_conversions,
    clippy::missing_const_for_fn,
    clippy::unwrap_used
)]

use std::sync::Arc;
use std::task::Poll;

use nectar_file::{Policy, Window};
use nectar_ldb::Database;
use nectar_manifest::{Manifest, ManifestPath, ManifestView};
use nectar_mantaray::{MantarayManifest, NodeLoadSaver};
use nectar_primitives::store::{ChunkGet, ChunkPut};
use nectar_primitives::{
    Chunk, ChunkAddress, ChunkRef, ContentOnlyChunkSet, DEFAULT_BODY_SIZE, Verified,
};
use nectar_testing::MemWriteAt;
use nectar_testing::{Drive, GateStore, run};

mod common;
use common::{Nodes, Raw, Store, save_file, stores};

type Trie = MantarayManifest<Nodes, GatedStore, DEFAULT_BODY_SIZE>;

/// A single-slot fetch window: far from the default, so a dropped policy is
/// visible in the fan-out.
fn tight() -> Policy {
    Policy::DEFAULT.with_window(Window::new(1).unwrap())
}

/// The loaded path.
fn path() -> ManifestPath {
    ManifestPath::from("data.bin")
}

/// Enough bytes to span several leaves, so a wide window has something to
/// overlap.
fn payload() -> Vec<u8> {
    (0u32..20_000).map(|i| (i % 251) as u8).collect()
}

/// A store that parks every chunk get on a gate, so a test can hold gets
/// un-settled and read off how many the load admitted at once.
#[derive(Clone, Debug)]
struct GatedStore {
    inner: Store,
    gate: GateStore,
}

impl ChunkGet<ContentOnlyChunkSet> for GatedStore {
    type Trust = Verified;
    type Error = <Store as ChunkGet<ContentOnlyChunkSet>>::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, ContentOnlyChunkSet>, Self::Error> {
        self.gate.enter().await;
        self.inner.get(address).await
    }
}

/// Writes pass straight through: the gate measures reads alone.
impl ChunkPut<Chunk> for GatedStore {
    type Error = <Store as ChunkPut<Chunk>>::Error;

    async fn put(&self, chunk: Chunk<Verified>) -> Result<(), Self::Error> {
        self.inner.put(chunk).await
    }
}

/// Insert `file` at the loaded path and hand back the new root.
async fn rooted<M: Manifest<ChunkRef>>(manifest: &M, file: ChunkRef) -> ChunkRef {
    let empty = manifest.empty().await.unwrap();
    manifest.insert(empty, path(), file).await.unwrap()
}

/// Single-step one load to completion, releasing every parked get, and report
/// the gate's high-water mark of concurrent gets.
fn peak_gets<M: Manifest<ChunkRef>>(
    manifest: M,
    root: ChunkRef,
    gate: &GateStore,
    data: &[u8],
) -> usize {
    let mut drive = Drive::new(async move {
        let mut sink = MemWriteAt::new();
        manifest.at(root).load(&path(), &mut sink).await.unwrap();
        sink
    });
    loop {
        match drive.poll() {
            Poll::Ready(sink) => {
                assert_eq!(sink.as_bytes(), data, "the load served the wrong bytes");
                break;
            }
            Poll::Pending => {
                let waiting = gate.waiting();
                assert!(waiting > 0, "the load parked with no get in flight");
                gate.release(waiting);
            }
        }
    }
    gate.peak()
}

/// The gate the format's data store parks on, and the store over it.
fn gated(store: &Store) -> (GateStore, GatedStore) {
    let gate = GateStore::new();
    let gated = GatedStore {
        inner: store.clone(),
        gate: gate.clone(),
    };
    (gate, gated)
}

#[test]
fn both_adapters_thread_a_caller_policy() {
    run(async {
        let (raw, store): (Raw, Store) = stores();
        let data = payload();
        let file = save_file(&raw, &data).await;
        let nodes: Nodes = NodeLoadSaver::new(Arc::clone(&raw));

        // The roots are built over the plain store; only the loads below run
        // against the gate.
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes.clone(), store.clone())
            .with_policy(tight());
        assert_eq!(trie.policy(), tight());
        let trie_root = rooted(&trie, file).await;

        let kv = Database::<_>::plain(store.clone()).with_policy(tight());
        assert_eq!(kv.policy(), tight());
        let kv_root = rooted(&kv, file).await;

        for (format, policy, want_serial) in
            [("tight", tight(), true), ("wide", Policy::DEFAULT, false)]
        {
            let (gate, gated_store) = gated(&store);
            let trie: Trie = MantarayManifest::new(nodes.clone(), gated_store).with_policy(policy);
            let peak = peak_gets(trie, trie_root, &gate, &data);
            assert_eq!(peak == 1, want_serial, "trie {format}: peak {peak}");

            // The database reads its nodes from the same store, so only the
            // windowed file load can overlap gets here too.
            let (gate, gated_store) = gated(&store);
            let kv = Database::<_>::plain(gated_store).with_policy(policy);
            let peak = peak_gets(kv, kv_root, &gate, &data);
            assert_eq!(peak == 1, want_serial, "kv {format}: peak {peak}");
        }
    });
}
