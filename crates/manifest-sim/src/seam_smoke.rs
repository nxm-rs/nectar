//! Seam smoke test: build the kiwix corpus at 1e3 on all three arms, read
//! every sampled key back through `get`, assert each emulated op's capability
//! label and a non-zero cost, and prove a 0.2 node above one chunk body
//! charges more than one fetch through the loadsave path (red-team check 10).
//!
//! Three further apples-for-apples gates live here. The arms must agree on the
//! key stream they consumed, per `(corpus, scale)` (check 9). The 0.2 fair
//! listing path must ride the pruned cursor at 1e4 and not the full walk
//! (check 2). The 0.2 emulation labels must survive a cost-class measurement at
//! two scales two orders of magnitude apart (check 6).

use nectar_ldb::{V1, V1Read};
use nectar_loadsave::NodeLoadSaver;
use nectar_mantaray::{ManifestEditor, NodeLoader};
use nectar_primitives::EntryRef;
use nectar_primitives::chunk::ChunkAddress;
use nectar_testing::run;

use crate::arm::{Arm, BatchMode, Capability, FrontierClass, build_checked};
use crate::arm_ldb::LdbArm;
use crate::arm_mantaray::{
    BATCH_CLASS, BATCH_HOW, CEILING_CLASS, CEILING_HOW, FLOOR_CLASS, FLOOR_HOW, FULL_ITER_CLASS,
    FULL_ITER_HOW, MantarayArm, PREFIX_CLASS, PREFIX_HOW, RANGE_CLASS, RANGE_HOW, SharedCounting,
};
use crate::corpus::{self, Corpus, GenKey, key_stream_digest, value_addr};

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

/// Red-team check 9: both arms hash the key stream their own build loop fed the
/// format, and the harness gate refuses a build whose digest is not the
/// corpus's.
///
/// The digest is taken inside each arm's insert loop, so it witnesses what the
/// format consumed rather than what the caller held. A truncated stream is the
/// counter-example: it builds, and its digest is not the corpus digest, which is
/// exactly the divergence `build_checked` rejects.
#[test]
fn both_arms_consume_one_key_stream_per_corpus_and_scale() {
    for corpus in Corpus::all() {
        for scale in [1_000usize, 10_000] {
            let keys = corpus::generate(corpus, scale);
            let expected = key_stream_digest(&keys);
            let mut v1 = LdbArm::<V1>::new();
            let mut v1read = LdbArm::<V1Read>::new();
            let mut mantaray = MantarayArm::new();
            for arm in [
                &mut v1 as &mut dyn Arm,
                &mut v1read as &mut dyn Arm,
                &mut mantaray as &mut dyn Arm,
            ] {
                let label = arm.label();
                build_checked(arm, &keys)
                    .unwrap_or_else(|e| panic!("{} {scale} {label}: {e}", corpus.name()));
                assert_eq!(
                    arm.consumed_digest(),
                    Some(expected),
                    "{} {scale} {label}: key stream diverged",
                    corpus.name()
                );
            }
        }
    }

    // The gate is not a tautology: a shorter stream digests differently and the
    // gate names the mismatch instead of publishing the build.
    let keys = corpus::generate(Corpus::Kiwix, 1_000);
    let short = &keys[..keys.len() - 1];
    assert_ne!(key_stream_digest(short), key_stream_digest(&keys));
    let mut arm = LdbArm::<V1>::new();
    build_checked(&mut arm, short).unwrap();
    assert_ne!(
        arm.consumed_digest(),
        Some(key_stream_digest(&keys)),
        "a truncated stream digested as the full corpus"
    );

    // The length framing keeps the digest injective: a re-split of the same
    // bytes is a different stream.
    let raw = |s: &str| GenKey {
        raw: s.as_bytes().to_vec(),
        content_type: None,
    };
    assert_ne!(
        key_stream_digest(&[raw("ab"), raw("c")]),
        key_stream_digest(&[raw("a"), raw("bc")]),
        "the digest is blind to where one key ends"
    );
}

/// Red-team check 2: the 0.2 fair listing path rides the pruned cursor at 1e4.
///
/// The pruned prefix walk and the ordered `after`-bound drain must both fetch
/// strictly below the whole-manifest walk that serves the pessimal column, at a
/// scale where a full walk is tens of thousands of fetches. The
/// `fair_multiplier <= pessimal_multiplier` half of the check is asserted over
/// every published cell in `ordered_prefix::tests`.
#[test]
fn the_02_fair_paths_prune_below_the_full_walk_at_1e4() {
    let keys = corpus::generate(Corpus::Kiwix, 10_000);
    let mut mantaray = MantarayArm::new();
    build_checked(&mut mantaray, &keys).unwrap();

    let prefix = prefix_of(keys[5_000].raw.as_slice());
    let fair = mantaray.prefix_list(&prefix).unwrap().cost.unwrap();
    let pessimal = mantaray
        .prefix_list_pessimal(&prefix)
        .unwrap()
        .cost
        .unwrap();
    assert_eq!(
        fair.keys_returned, pessimal.keys_returned,
        "the pruned and full listings disagree on the selection"
    );
    assert!(
        fair.fetches < pessimal.fetches,
        "the pruned prefix walk cost {} against a full walk of {}",
        fair.fetches,
        pessimal.fetches
    );

    let (lo, hi) = (keys[4_000].raw.as_slice(), keys[4_100].raw.as_slice());
    let fair = mantaray.range(lo, hi).unwrap().cost.unwrap();
    let pessimal = mantaray.range_pessimal(lo, hi).unwrap().cost.unwrap();
    assert_eq!(fair.keys_returned, pessimal.keys_returned);
    assert!(
        fair.fetches < pessimal.fetches,
        "the after-bound drain cost {} against a full walk of {}",
        fair.fetches,
        pessimal.fetches
    );
}

/// The 0.2 ceiling and floor fetch counts at one scale: the ceiling over three
/// spread ranks, and the floor at one hundredth of the way in.
///
/// Both probe ranks are fractional, not absolute, so the two scales price the
/// same point in the key order and any growth reads as a cost class rather than
/// as a change of probe. The floor rank is kept shallow because the walk it
/// prices is O(rank) and a deeper probe buys no extra evidence.
fn ceiling_and_floor_at(scale: usize) -> (f64, u64) {
    let keys = corpus::generate(Corpus::Kiwix, scale);
    let mut arm = MantarayArm::new();
    build_checked(&mut arm, &keys).unwrap();
    let ranks = [0usize, scale / 2, scale - 1];
    let mut ceiling = 0u64;
    for r in ranks {
        ceiling += arm
            .ceiling(keys[r].raw.as_slice())
            .unwrap()
            .cost
            .unwrap()
            .fetches;
    }
    let floor = arm
        .floor(keys[scale / 100].raw.as_slice())
        .unwrap()
        .cost
        .unwrap()
        .fetches;
    (ceiling as f64 / ranks.len() as f64, floor)
}

/// Red-team check 6: the 0.2 emulation labels are measured, not asserted.
///
/// Across a hundredfold scale change the `after`-bound ceiling stays seek-grade
/// and the ordered floor walk grows with the rank it must reach. A label that
/// claimed the other class would fail here.
///
/// 1e5 is the 0.2 scale cap, so this is the widest span the arm publishes. The
/// unoptimised 1e5 editor commit is the whole cost of the test; the probes are
/// a rounding error beside it.
#[test]
fn the_02_ceiling_stays_seek_grade_while_the_floor_grows_with_rank() {
    let (ceiling_1e3, floor_1e3) = ceiling_and_floor_at(1_000);
    let (ceiling_1e5, floor_1e5) = ceiling_and_floor_at(100_000);

    // Seek-grade: a hundredfold corpus costs a bounded multiple, not a
    // hundredfold, of the fetches. O(depth + window) survives; O(N) would not.
    assert!(
        ceiling_1e5 <= ceiling_1e3 * 4.0,
        "{CEILING_CLASS}: ceiling went {ceiling_1e3} to {ceiling_1e5} over a hundredfold corpus"
    );

    // O(rank): the same fractional rank is a hundredfold deeper, so the walk
    // must grow by roughly that much. A seek would not move at all.
    let growth = floor_1e5 as f64 / floor_1e3 as f64;
    assert!(
        (20.0..500.0).contains(&growth),
        "{FLOOR_CLASS}: floor went {floor_1e3} to {floor_1e5}, a factor of {growth}"
    );
    assert!(
        floor_1e5 > (ceiling_1e5 * 100.0) as u64,
        "the floor walk did not outgrow the ceiling seek: {floor_1e5} against {ceiling_1e5}"
    );
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
