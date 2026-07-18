//! Differential merge gate against the registry-pinned legacy manifest.
//!
//! Identical submission-order op sequences are replayed on the pinned
//! `mantaray-old` crate and on the editor; the resulting roots must match
//! byte for byte. The legacy replay is a fresh single-session build with one
//! save at the end, which is the sequence's well-defined root.

// Integration-test code: unwraps, direct indexing, and assertions are setup
// and illustration, not shipped surface.
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

use std::collections::BTreeMap;

use futures::executor::block_on;
use nectar_mantaray::ManifestEditor;
use nectar_primitives::StandardChunkSet;
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::store::MemoryStore;
use proptest::prelude::*;

type Store = MemoryStore<StandardChunkSet>;
type Editor = ManifestEditor<Store>;

type OldStore = mantaray_old::DefaultMemoryStore;
type OldManifest = mantaray_old::PlainManifest<OldStore>;

/// One scripted mutation, replayable on both implementations.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ScriptOp {
    Add(String, [u8; 32]),
    AddMeta(String, [u8; 32], String, String),
    Rm(String),
    SetIndex(String),
    SetError(String),
}

/// Deterministic per-path entry address.
fn addr_bytes(seed: &str) -> [u8; 32] {
    let bytes = seed.as_bytes();
    let mut buf = [0u8; 32];
    let len = bytes.len().min(32);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

/// A replay result: the root, or the zero-based index of the op that failed
/// (a remove aimed past a subtree an earlier remove already dropped, say).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Root([u8; 32]),
    FailedAt(usize),
}

/// Replay on the pinned legacy crate: fresh build, one save. A failing op
/// stops the replay and is part of the differential contract.
fn legacy_outcome(script: &[ScriptOp]) -> Outcome {
    let mut m = OldManifest::new(OldStore::new());
    for (index, op) in script.iter().enumerate() {
        let result = match op {
            ScriptOp::Add(p, a) => block_on(m.add(p, *a)),
            ScriptOp::AddMeta(p, a, k, v) => {
                let meta: BTreeMap<String, String> = [(k.clone(), v.clone())].into();
                block_on(m.add_with_metadata(p, *a, meta))
            }
            ScriptOp::Rm(p) => block_on(m.remove(p)),
            ScriptOp::SetIndex(v) => block_on(m.set_index_document(v)),
            ScriptOp::SetError(v) => block_on(m.set_error_document(v)),
        };
        if result.is_err() {
            return Outcome::FailedAt(index);
        }
    }
    let root = block_on(m.save()).unwrap();
    let mut out = [0u8; 32];
    out.copy_from_slice(root.as_bytes());
    Outcome::Root(out)
}

/// The legacy root for a script known to be valid.
fn legacy_root(script: &[ScriptOp]) -> [u8; 32] {
    match legacy_outcome(script) {
        Outcome::Root(root) => root,
        Outcome::FailedAt(index) => panic!("legacy replay failed at op {index}"),
    }
}

/// Record a script into an editor.
fn record(editor: &mut Editor, script: &[ScriptOp]) {
    for op in script {
        match op {
            ScriptOp::Add(p, a) => {
                editor.put(p.as_str(), ChunkAddress::from(*a));
            }
            ScriptOp::AddMeta(p, a, k, v) => {
                let meta: BTreeMap<String, String> = [(k.clone(), v.clone())].into();
                editor.put_with_metadata(p.as_str(), ChunkAddress::from(*a), meta);
            }
            ScriptOp::Rm(p) => {
                editor.remove(p.as_str());
            }
            ScriptOp::SetIndex(v) => {
                editor.set_index_document(v);
            }
            ScriptOp::SetError(v) => {
                editor.set_error_document(v);
            }
        }
    }
}

/// Map a commit result onto an outcome, offsetting apply indices by the
/// number of ops committed earlier.
fn outcome_from(
    result: Result<(ChunkAddress, Store), nectar_mantaray::EditorError>,
    offset: usize,
) -> Result<(ChunkAddress, Store), Outcome> {
    match result {
        Ok(ok) => Ok(ok),
        Err(nectar_mantaray::EditorError::Apply { index, .. }) => {
            Err(Outcome::FailedAt(offset + index))
        }
        Err(other) => panic!("editor commit failed outside op application: {other}"),
    }
}

/// Editor replay from an empty manifest, committing once.
fn editor_outcome(script: &[ScriptOp]) -> Outcome {
    let mut editor = Editor::new(Store::new());
    record(&mut editor, script);
    match outcome_from(block_on(editor.commit()), 0) {
        Ok((root, _)) => {
            let mut out = [0u8; 32];
            out.copy_from_slice(root.as_bytes());
            Outcome::Root(out)
        }
        Err(failed) => failed,
    }
}

/// Editor replay with a commit boundary after `split` ops.
fn editor_outcome_split(script: &[ScriptOp], split: usize) -> Outcome {
    let (head, tail) = script.split_at(split.min(script.len()));
    let mut editor = Editor::new(Store::new());
    record(&mut editor, head);
    let (root, store) = match outcome_from(block_on(editor.commit()), 0) {
        Ok(ok) => ok,
        Err(failed) => return failed,
    };
    let mut editor = Editor::open(root, store);
    record(&mut editor, tail);
    match outcome_from(block_on(editor.commit()), head.len()) {
        Ok((root, _)) => {
            let mut out = [0u8; 32];
            out.copy_from_slice(root.as_bytes());
            Outcome::Root(out)
        }
        Err(failed) => failed,
    }
}

/// The editor root for a script known to be valid.
fn editor_root(script: &[ScriptOp]) -> [u8; 32] {
    match editor_outcome(script) {
        Outcome::Root(root) => root,
        Outcome::FailedAt(index) => panic!("editor replay failed at op {index}"),
    }
}

/// The editor root for a valid script with a commit boundary after `split`.
fn editor_root_split(script: &[ScriptOp], split: usize) -> [u8; 32] {
    match editor_outcome_split(script, split) {
        Outcome::Root(root) => root,
        Outcome::FailedAt(index) => panic!("editor split replay failed at op {index}"),
    }
}

fn add(p: &str) -> ScriptOp {
    ScriptOp::Add(p.to_string(), addr_bytes(p))
}

fn add_seed(p: &str, seed: &str) -> ScriptOp {
    ScriptOp::Add(p.to_string(), addr_bytes(seed))
}

fn rm(p: &str) -> ScriptOp {
    ScriptOp::Rm(p.to_string())
}

/// Hostile deterministic corpora: prefix splits at and around values,
/// removes that leave non-canonical edges, re-adds, overwrites, long edges,
/// and root metadata interleavings.
fn corpora() -> Vec<Vec<ScriptOp>> {
    vec![
        vec![add("app.js.map"), add("app.js")],
        vec![add("app.js"), add("app.js.map")],
        vec![add("abcdef"), add("abc"), rm("abcdef"), add("abcxyz")],
        vec![add("a"), add("ab"), add("abc"), rm("ab"), rm("a")],
        vec![
            add("img/1.png"),
            add("img/2.png"),
            add("index.html"),
            rm("img/1.png"),
            add_seed("img/1.png", "1v2"),
        ],
        vec![add("d/x"), add("d/y"), rm("d/x"), rm("d/y"), add("da")],
        // A boundary remove drops the whole subtree below it.
        vec![add("ab"), add("a"), rm("a"), add("abc")],
        vec![
            add("img/1.png"),
            add("img/2.png"),
            rm("img/"),
            add("img/3.png"),
        ],
        vec![add_seed("same", "old"), add_seed("same", "new")],
        vec![
            add("oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure"),
            add("oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsurely"),
            rm("oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure"),
        ],
        vec![
            add("/"),
            ScriptOp::SetIndex("index.html".to_string()),
            ScriptOp::SetError("404.html".to_string()),
            ScriptOp::SetIndex("start.html".to_string()),
            add("index.html"),
        ],
        vec![
            ScriptOp::SetIndex("index.html".to_string()),
            add("a/b/c/d/e/f/g/h/file00.dat"),
            add("a/b/c/d/e/f/g/h/file01.dat"),
            add("a/b/c/x.txt"),
            rm("a/b/c/d/e/f/g/h/file00.dat"),
        ],
        vec![
            ScriptOp::AddMeta(
                "logo.png".to_string(),
                addr_bytes("logo"),
                "Content-Type".to_string(),
                "image/png".to_string(),
            ),
            add_seed("logo.png", "logo2"),
            ScriptOp::AddMeta(
                "logo.png".to_string(),
                addr_bytes("logo3"),
                "Filename".to_string(),
                "logo.png".to_string(),
            ),
        ],
    ]
}

#[test]
fn corpora_roots_match_legacy() {
    for (i, script) in corpora().iter().enumerate() {
        assert_eq!(
            editor_root(script),
            legacy_root(script),
            "corpus {i} diverges from the pinned legacy root"
        );
    }
}

#[test]
fn corpora_split_commits_match_legacy() {
    for (i, script) in corpora().iter().enumerate() {
        let want = legacy_root(script);
        for split in 0..=script.len() {
            assert_eq!(
                editor_root_split(script, split),
                want,
                "corpus {i} split {split} diverges from the pinned legacy root"
            );
        }
    }
}

/// A failing op must fail at the same submission index on both sides: the
/// boundary remove at op 2 drops the "ab" subtree, so op 3 misses.
#[test]
fn failing_op_index_matches_legacy() {
    let script = vec![add("ab"), add("a"), rm("a"), rm("ab")];
    assert_eq!(legacy_outcome(&script), Outcome::FailedAt(3));
    assert_eq!(editor_outcome(&script), Outcome::FailedAt(3));
    for split in 0..=script.len() {
        assert_eq!(editor_outcome_split(&script, split), Outcome::FailedAt(3));
    }
}

/// Submission-order permutations of one path set must each match the legacy
/// root for the same permutation.
#[test]
fn permutations_match_legacy() {
    let paths = ["app.js", "app.js.map", "a", "ab"];
    let perms: &[[usize; 4]] = &[[0, 1, 2, 3], [3, 2, 1, 0], [1, 0, 3, 2], [2, 0, 3, 1]];
    for perm in perms {
        let script: Vec<ScriptOp> = perm.iter().map(|&i| add(paths[i])).collect();
        assert_eq!(
            editor_root(&script),
            legacy_root(&script),
            "permutation {perm:?} diverges from the pinned legacy root"
        );
    }
}

/// The clean-ancestor hazard, pinned: legacy drops root metadata set after a
/// save (the second save returns the stale root), while the editor commits
/// it across the same boundary and lands on the well-defined root.
#[test]
fn clean_ancestor_hazard_regression() {
    let mut legacy = OldManifest::new(OldStore::new());
    block_on(legacy.add("index.html", addr_bytes("index.html"))).unwrap();
    let stale = block_on(legacy.save()).unwrap();
    block_on(legacy.set_index_document("index.html")).unwrap();
    assert_eq!(
        block_on(legacy.save()).unwrap(),
        stale,
        "the pinned legacy no longer exhibits the clean-ancestor hazard"
    );

    let script = vec![
        add("index.html"),
        ScriptOp::SetIndex("index.html".to_string()),
    ];
    let want = legacy_root(&script);
    assert_ne!(want.as_slice(), stale.as_bytes());

    let got = editor_root_split(&script, 1);
    assert_eq!(got, want, "the editor reproduced the clean-ancestor hazard");
}

/// Path pool for randomized scripts: split-prone stems, nested folders, the
/// root path, and edges past the 30-byte prefix limit.
const PATHS: &[&str] = &[
    "a",
    "ab",
    "abc",
    "app.js",
    "app.js.map",
    "index.html",
    "img/1.png",
    "img/2.png",
    "img/sub/deep.png",
    "dir/sub/file00.dat",
    "oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure/x",
    "/",
];

/// Map raw fuzz words onto a script. Removes are biased towards previously
/// added paths so most sequences stay on the happy path, but a remove may
/// still miss (a boundary remove drops whole subtrees); the outcome
/// comparison covers those runs as error-parity cases.
fn build_script(raw: &[(u8, u8, u8)]) -> Vec<ScriptOp> {
    let mut added: Vec<&str> = Vec::new();
    let mut script = Vec::new();
    for &(kind, path_idx, seed) in raw {
        let path = PATHS[usize::from(path_idx) % PATHS.len()];
        match kind % 8 {
            0..=3 => {
                let mut a = addr_bytes(path);
                a[31] = seed;
                script.push(ScriptOp::Add(path.to_string(), a));
                if !added.contains(&path) {
                    added.push(path);
                }
            }
            4 => {
                if added.is_empty() {
                    script.push(add(path));
                    added.push(path);
                } else {
                    let victim = added.remove(usize::from(seed) % added.len());
                    script.push(rm(victim));
                }
            }
            5 => {
                let mut a = addr_bytes(path);
                a[31] = seed;
                script.push(ScriptOp::AddMeta(
                    path.to_string(),
                    a,
                    "Content-Type".to_string(),
                    format!("type/{seed}"),
                ));
                if !added.contains(&path) {
                    added.push(path);
                }
            }
            6 => script.push(ScriptOp::SetIndex(format!("index{seed}.html"))),
            _ => script.push(ScriptOp::SetError(format!("error{seed}.html"))),
        }
    }
    script
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    /// Randomized differential: any submission-order script lands on the
    /// pinned legacy outcome (root bytes, or the same failing op index),
    /// both in one commit and across a mid-script commit boundary.
    #[test]
    fn random_scripts_match_legacy(raw in proptest::collection::vec(any::<(u8, u8, u8)>(), 1..24)) {
        let script = build_script(&raw);
        let want = legacy_outcome(&script);
        prop_assert_eq!(editor_outcome(&script), want);
        prop_assert_eq!(editor_outcome_split(&script, script.len() / 2), want);
    }
}
