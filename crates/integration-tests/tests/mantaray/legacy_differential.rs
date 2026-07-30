//! Differential merge gate against the registry-pinned legacy manifest.
//!
//! Identical submission-order op sequences are replayed on the pinned
//! `mantaray-old` crate and on the editor; the resulting roots must match
//! byte for byte. The legacy replay is a fresh single-session build with one
//! save at the end, which is the sequence's well-defined root.
//!
//! # Scope: the merge algorithm, not the key space
//!
//! A manifest path is absolute now, so every key this crate writes is rooted at
//! `/`. The pinned 0.3.0 oracle stores whatever key bytes it is handed, and the
//! reference client wrote bare keys, so a 0.3.0 manifest and a 0.4 manifest of
//! the same content are different byte images: 0.4 supersedes 0.3.0 on the
//! wire, deliberately. That break is not what this gate measures.
//!
//! What it measures is the merge algorithm, so it feeds both sides the same
//! `/`-rooted keys through [`rooted`] and compares the roots. Submission order,
//! boundary removes, mid-edge splits, prefix-bound chains and the root-metadata
//! interleavings are all still pinned against the reference implementation; only
//! the key space they run over moved.

use std::collections::BTreeMap;

use nectar_loadsave::NodeLoadSaver;
use nectar_mantaray::{ManifestEditor, Reader, metadata};
use nectar_primitives::StandardChunkSet;
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::store::MemoryStore;
use nectar_testing::run;
use proptest::prelude::*;

type Store = MemoryStore<StandardChunkSet>;
type LoadSaver = NodeLoadSaver<Store>;
type Editor = ManifestEditor<LoadSaver>;

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

/// `path` rooted at the separator, which is the canonical manifest key.
///
/// Both replays key through this, so the differential compares the merge
/// algorithm over one key space rather than two.
fn rooted(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
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
async fn legacy_outcome(script: &[ScriptOp]) -> Outcome {
    let mut m = OldManifest::new(OldStore::new());
    for (index, op) in script.iter().enumerate() {
        let result = match op {
            ScriptOp::Add(p, a) => m.add(p, *a).await,
            ScriptOp::AddMeta(p, a, k, v) => {
                let meta: BTreeMap<String, String> = [(k.clone(), v.clone())].into();
                m.add_with_metadata(p, *a, meta).await
            }
            ScriptOp::Rm(p) => m.remove(p).await,
            ScriptOp::SetIndex(v) => m.set_index_document(v).await,
            ScriptOp::SetError(v) => m.set_error_document(v).await,
        };
        if result.is_err() {
            return Outcome::FailedAt(index);
        }
    }
    let root = m.save().await.unwrap();
    let mut out = [0u8; 32];
    out.copy_from_slice(root.as_bytes());
    Outcome::Root(out)
}

/// The legacy root for a script known to be valid.
fn legacy_root(script: &[ScriptOp]) -> [u8; 32] {
    match run(legacy_outcome(script)) {
        Outcome::Root(root) => root,
        Outcome::FailedAt(index) => panic!("legacy replay failed at op {index}"),
    }
}

/// Record a script into an editor.
fn record(editor: &mut Editor, script: &[ScriptOp]) {
    for op in script {
        match op {
            ScriptOp::Add(p, a) => {
                editor.insert(p.as_str(), ChunkAddress::from(*a));
            }
            ScriptOp::AddMeta(p, a, k, v) => {
                let meta: BTreeMap<String, String> = [(k.clone(), v.clone())].into();
                editor.insert(p.as_str(), ChunkAddress::from(*a)).meta(meta);
            }
            ScriptOp::Rm(p) => {
                editor.remove(p.as_str());
            }
            // The typed `set_index_document` sugar is gone: the site documents
            // are well-known metadata, and a root-scope merge is the op the
            // legacy verb was always shorthand for.
            ScriptOp::SetIndex(v) => {
                editor.set_root_metadata(metadata::WEBSITE_INDEX_DOCUMENT, v);
            }
            ScriptOp::SetError(v) => {
                editor.set_root_metadata(metadata::WEBSITE_ERROR_DOCUMENT, v);
            }
        }
    }
}

/// Map a commit result onto an outcome, offsetting apply indices by the
/// number of ops committed earlier.
fn outcome_from(
    result: Result<(ChunkAddress, LoadSaver), nectar_mantaray::EditorError>,
    offset: usize,
) -> Result<(ChunkAddress, LoadSaver), Outcome> {
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
    let mut editor = Editor::new(LoadSaver::new(Store::new()));
    record(&mut editor, script);
    match outcome_from(run(editor.commit()), 0) {
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
    let mut editor = Editor::new(LoadSaver::new(Store::new()));
    record(&mut editor, head);
    let (root, store) = match outcome_from(run(editor.commit()), 0) {
        Ok(ok) => ok,
        Err(failed) => return failed,
    };
    let mut editor = Editor::open(root, store);
    record(&mut editor, tail);
    match outcome_from(run(editor.commit()), head.len()) {
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
    ScriptOp::Add(rooted(p), addr_bytes(p))
}

fn add_seed(p: &str, seed: &str) -> ScriptOp {
    ScriptOp::Add(rooted(p), addr_bytes(seed))
}

fn rm(p: &str) -> ScriptOp {
    ScriptOp::Rm(rooted(p))
}

/// An add carrying one metadata pair, keyed the same way [`add`] is.
fn add_meta(p: &str, seed: &str, key: &str, value: &str) -> ScriptOp {
    ScriptOp::AddMeta(
        rooted(p),
        addr_bytes(seed),
        key.to_string(),
        value.to_string(),
    )
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
            add_meta("logo.png", "logo", "Content-Type", "image/png"),
            add_seed("logo.png", "logo2"),
            add_meta("logo.png", "logo3", "Filename", "logo.png"),
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
    assert_eq!(run(legacy_outcome(&script)), Outcome::FailedAt(3));
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
    // The same absolute key the script below replays, so the stale root and the
    // well-defined one are two roots over one key set.
    run(legacy.add(&rooted("index.html"), addr_bytes("index.html"))).unwrap();
    let stale = run(legacy.save()).unwrap();
    run(legacy.set_index_document("index.html")).unwrap();
    assert_eq!(
        run(legacy.save()).unwrap(),
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

/// A bare insert replaces the whole binding, so it clears the metadata the
/// path carried; the pinned legacy keeps it. The divergence is the map contract
/// and is pinned here, which is why the randomized generator holds the shape
/// out.
#[test]
fn bare_insert_clears_metadata_unlike_legacy() {
    let address = addr_bytes("logo");
    let with_meta = add_meta("logo.png", "logo", "Content-Type", "image/png");
    let bare = ScriptOp::Add(rooted("logo.png"), address);
    let script = vec![with_meta.clone(), bare.clone()];

    // The editor lands on the root the path would have had with no metadata at
    // all, in one commit and across a commit boundary.
    let cleared = editor_root(std::slice::from_ref(&bare));
    assert_eq!(editor_root(&script), cleared, "a bare insert clears");
    assert_eq!(editor_root_split(&script, 1), cleared, "across a commit");

    // The pinned legacy keeps it: the bare add changed nothing there.
    assert_eq!(legacy_root(&script), legacy_root(&[with_meta]));
    assert_ne!(
        editor_root(&script),
        legacy_root(&script),
        "the contract diverges from the pinned legacy on purpose",
    );

    // Read the cleared binding back: the entry stands, its metadata is gone.
    let mut editor = Editor::new(LoadSaver::new(Store::new()));
    let meta: BTreeMap<String, String> =
        [("Content-Type".to_string(), "image/png".to_string())].into();
    editor
        .insert("/logo.png", ChunkAddress::from(address))
        .meta(meta);
    let (root, store) = run(editor.commit()).unwrap();
    let mut editor = Editor::open(root, store);
    editor.insert("/logo.png", ChunkAddress::from(address));
    let (root, store) = run(editor.commit()).unwrap();
    let entry = run(Reader::new(store).get(root, b"/logo.png"))
        .unwrap()
        .expect("the entry survives the re-insert");
    assert_eq!(entry.address(), Some(&ChunkAddress::from(address)));
    assert!(entry.metadata().is_empty(), "the metadata is cleared");
}

/// Path pool for randomized scripts: split-prone stems, mid-edge splits,
/// nested and deep folders, boundary-remove-prone parents, the root path,
/// and long edges at and past the 30-byte prefix limit, including a pair
/// diverging inside the long edge. Every shape is exercised by the
/// deterministic corpora above, so the pool stays within pinned-crate
/// support.
const PATHS: &[&str] = &[
    "a",
    "ab",
    "abc",
    "abcdef",
    "abcxyz",
    "app.js",
    "app.js.map",
    "app.js.map.gz",
    "index.html",
    "img/1.png",
    "img/2.png",
    "img/3.png",
    "img/sub/deep.png",
    "d/x",
    "d/y",
    "da",
    "dir/sub/file00.dat",
    "dir/sub/file01.dat",
    "a/b/c/d/e/f/g/h/file00.dat",
    "a/b/c/x.txt",
    "oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure",
    "oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsurely",
    "oneverylongpathsegmentthatexceedsthethirtybyteprefixlimitforsure/x",
    "/",
];

/// Map raw fuzz words onto a script. Removes are biased towards previously
/// added paths so most sequences stay on the happy path, but a remove may
/// still miss (a boundary remove drops whole subtrees); the outcome
/// comparison covers those runs as error-parity cases.
///
/// One shape is held out on purpose: a bare add over a path that already
/// carries metadata. The editor takes an insert as a whole-binding replace and
/// clears the metadata, where the pinned legacy keeps it, so the two roots
/// diverge by design. That divergence is pinned on its own by
/// [`bare_insert_clears_metadata_unlike_legacy`], and the generator emits a
/// metadata-carrying add there instead, which both sides treat alike.
fn build_script(raw: &[(u8, u8, u8)]) -> Vec<ScriptOp> {
    let mut added: Vec<&str> = Vec::new();
    // Paths that have carried metadata; a bare add over one of these is the
    // held-out shape, so it is emitted with metadata instead.
    let mut metaed: Vec<&str> = Vec::new();
    let mut script = Vec::new();
    for &(kind, path_idx, seed) in raw {
        let path = PATHS[usize::from(path_idx) % PATHS.len()];
        // The site documents live on the root path node, so setting one leaves
        // metadata behind on "/" exactly as an add with metadata does.
        let with_metadata = metaed.contains(&path);
        match kind % 8 {
            0..=3 if !with_metadata => {
                let mut a = addr_bytes(path);
                a[31] = seed;
                script.push(ScriptOp::Add(rooted(path), a));
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
            6 => {
                script.push(ScriptOp::SetIndex(format!("index{seed}.html")));
                if !metaed.contains(&"/") {
                    metaed.push("/");
                }
            }
            7 => {
                script.push(ScriptOp::SetError(format!("error{seed}.html")));
                if !metaed.contains(&"/") {
                    metaed.push("/");
                }
            }
            // Every remaining word, and any bare add held out above, lands as
            // an add carrying metadata.
            _ => {
                let mut a = addr_bytes(path);
                a[31] = seed;
                script.push(ScriptOp::AddMeta(
                    rooted(path),
                    a,
                    "Content-Type".to_string(),
                    format!("type/{seed}"),
                ));
                if !added.contains(&path) {
                    added.push(path);
                }
                if !metaed.contains(&path) {
                    metaed.push(path);
                }
            }
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
        let want = run(legacy_outcome(&script));
        prop_assert_eq!(editor_outcome(&script), want);
        prop_assert_eq!(editor_outcome_split(&script, script.len() / 2), want);
    }
}
