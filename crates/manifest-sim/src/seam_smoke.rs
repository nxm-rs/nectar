//! Seam smoke test: build the kiwix corpus at 1e3 on all three arms, read
//! every sampled key back through `get`, assert each emulated op's capability
//! label and a non-zero cost, and prove a 0.2 node above one chunk body
//! charges more than one fetch through the loadsave path (red-team check 10).

use nectar_ldb::{V1, V1Read};
use nectar_loadsave::NodeLoadSaver;
use nectar_mantaray::{ManifestEditor, NodeLoader};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::ChunkAddress;
use nectar_testing::run;

use crate::arm::{Arm, BatchMode, Capability, FrontierClass};
use crate::arm_ldb::LdbArm;
use crate::arm_mantaray::{
    BATCH_CLASS, BATCH_HOW, CEILING_CLASS, CEILING_HOW, FLOOR_CLASS, FLOOR_HOW, FULL_ITER_CLASS,
    FULL_ITER_HOW, MantarayArm, PREFIX_CLASS, PREFIX_HOW, RANGE_CLASS, RANGE_HOW, SharedCounting,
};
use crate::corpus::{self, Corpus, value_addr};

/// Assert an outcome is an emulation named `how`/`class` with a non-zero fetch
/// cost.
fn assert_emulated(outcome: &crate::arm::OpOutcome, how: &str, class: &str) {
    match &outcome.capability {
        Capability::Emulated {
            how: h,
            cost_class: c,
        } => {
            assert_eq!(h, how, "emulation how");
            assert_eq!(c, class, "emulation cost_class");
        }
        other => panic!("expected an emulation, got {other:?}"),
    }
    let cost = outcome.cost.expect("an emulation carries a cost");
    assert!(cost.fetches > 0, "emulation charged no fetch: {cost:?}");
}

#[test]
fn three_arms_build_read_and_class_their_ops() {
    let keys = corpus::generate(Corpus::Kiwix, 1_000);

    // All three arms build the same key set in the same order.
    let mut ldb_v1 = LdbArm::<V1>::new();
    let mut ldb_v1read = LdbArm::<V1Read>::new();
    let mut mantaray = MantarayArm::new();

    let r1 = ldb_v1.build(&keys).unwrap();
    let r2 = ldb_v1read.build(&keys).unwrap();
    let rm = mantaray.build(&keys).unwrap();

    assert_eq!(ldb_v1.label(), "ldb-v1");
    assert_eq!(ldb_v1read.label(), "ldb-v1read");
    assert_eq!(mantaray.label(), "mantaray-0.2");

    // The frontier laws differ: 1.0 is bounded, 0.2 materialises the whole trie.
    assert!(matches!(r1.frontier, FrontierClass::Bounded { .. }));
    assert!(matches!(r2.frontier, FrontierClass::Bounded { .. }));
    assert!(matches!(rm.frontier, FrontierClass::WholeTrie { .. }));
    assert!(r1.nodes_embedded.is_some());
    assert!(rm.nodes_embedded.is_none());
    assert!(rm.nodes_written > 0);

    // Every sampled key reads back Some on every arm.
    for i in (0..keys.len()).step_by(97) {
        let key = keys[i].raw.as_slice();
        for arm in [&ldb_v1 as &dyn Arm, &ldb_v1read, &mantaray] {
            let got = arm.get(key).unwrap();
            assert!(matches!(got.capability, Capability::Native));
            let cost = got.cost.expect("get carries a cost");
            assert_eq!(cost.keys_returned, 1, "{}: get({key:?})", arm.label());
            assert!(cost.fetches > 0, "{}: get charged no fetch", arm.label());
        }
    }

    // The 0.2 arm's emulated ops carry their labels and a non-zero cost.
    let lo = keys[100].raw.as_slice();
    let hi = keys[700].raw.as_slice();
    assert_emulated(&mantaray.floor(lo).unwrap(), FLOOR_HOW, FLOOR_CLASS);
    assert_emulated(&mantaray.ceiling(lo).unwrap(), CEILING_HOW, CEILING_CLASS);
    assert_emulated(&mantaray.range(lo, hi).unwrap(), RANGE_HOW, RANGE_CLASS);
    assert_emulated(
        &mantaray.full_iter().unwrap(),
        FULL_ITER_HOW,
        FULL_ITER_CLASS,
    );

    // A directory prefix present in the corpus.
    let prefix = prefix_of(keys[500].raw.as_slice());
    assert_emulated(
        &mantaray.prefix_list(&prefix).unwrap(),
        PREFIX_HOW,
        PREFIX_CLASS,
    );

    // The fair range prunes below the pessimal full walk.
    let fair = mantaray.range(lo, hi).unwrap().cost.unwrap().fetches;
    let pess = mantaray
        .range_pessimal(lo, hi)
        .unwrap()
        .cost
        .unwrap()
        .fetches;
    assert!(fair <= pess, "fair range {fair} exceeded pessimal {pess}");

    // Batch update: Batched is an emulation, PerEdit is native; both write.
    let edits = &keys[0..8];
    let batched = mantaray.batch_update(edits, BatchMode::Batched).unwrap();
    match &batched.capability {
        Capability::Emulated { how, cost_class } => {
            assert_eq!(how, BATCH_HOW);
            assert_eq!(cost_class, BATCH_CLASS);
        }
        other => panic!("expected a batched emulation, got {other:?}"),
    }
    assert!(
        batched.cost.unwrap().puts > 0,
        "batched commit wrote nothing"
    );
    let per_edit = mantaray.batch_update(edits, BatchMode::PerEdit).unwrap();
    assert!(matches!(per_edit.capability, Capability::Native));
    assert!(
        per_edit.cost.unwrap().puts > 0,
        "per-edit commits wrote nothing"
    );

    // The 1.0 arms serve the same ops natively.
    assert!(matches!(
        ldb_v1.floor(lo).unwrap().capability,
        Capability::Native
    ));
    assert!(matches!(
        ldb_v1.range(lo, hi).unwrap().capability,
        Capability::Native
    ));
}

/// The directory prefix of a path: bytes through the first `/`, else the whole
/// key.
fn prefix_of(key: &[u8]) -> Vec<u8> {
    match key.iter().position(|&b| b == b'/') {
        Some(pos) => key[..=pos].to_vec(),
        None => key.to_vec(),
    }
}

#[test]
fn a_multichunk_02_node_load_charges_more_than_one_fetch() {
    // A 256-fork root node whose encoded image exceeds one chunk body: the
    // loadsaver stores it as a file across several chunks, so loading that one
    // node fetches more than one chunk through the counting store.
    let store = SharedCounting::new();
    let loadsaver = NodeLoadSaver::new(store.clone());
    let mut editor = ManifestEditor::new(loadsaver);
    for b in 0u8..=255 {
        editor.put([b], ChunkAddress::new(value_addr(&[b])));
    }
    let (root, loadsaver) = run(editor.commit()).unwrap();

    let before = store.snapshot();
    let bytes = run(NodeLoader::load(&loadsaver, &EntryRef::from(root))).unwrap();
    let after = store.snapshot();
    assert!(
        bytes.len() > 4096,
        "root node image is not above one chunk body"
    );
    let fetches = after.gets.saturating_sub(before.gets);
    assert!(
        fetches > 1,
        "a multi-chunk node load charged only {fetches} fetch(es)"
    );
}
