//! Fuzz the manifest editor against a reader-based path-set model.
//!
//! A fuzzed op sequence is committed through the editor; on success the
//! reader must see exactly the model's surviving paths, each at its last
//! put reference, with removed paths absent and the root documents equal
//! to their last set values. Two independent commits of the same log must
//! also agree.
//!
//! Paths come from a four-symbol alphabet so fork splits, nested prefixes
//! and separator edges stay dense in the search space.

#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use nectar_testing::run;
use libfuzzer_sys::fuzz_target;
use nectar_mantaray::{DefaultMemoryStore, ManifestEditor, Reader};
use nectar_primitives::chunk::ChunkAddress;

/// Op-sequence cap per exec.
const MAX_OPS: usize = 24;
/// Path-length cap in alphabet symbols.
const MAX_PATH: usize = 12;

/// One fuzzed manifest mutation.
#[derive(Arbitrary, Debug)]
enum FuzzOp {
    /// Set the entry at the path to a reference filled with one byte.
    Put { path: Vec<u8>, fill: u8 },
    /// Set the entry with one metadata pair.
    PutMeta {
        path: Vec<u8>,
        fill: u8,
        key: u8,
        value: u8,
    },
    /// Remove the value at the path.
    Remove { path: Vec<u8> },
    /// Set the website index document.
    SetIndex { name: u8 },
    /// Set the website error document.
    SetError { name: u8 },
}

/// Map raw bytes onto a dense four-symbol path.
fn path(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(MAX_PATH)
        .map(|byte| match byte % 4 {
            0 => 'a',
            1 => 'b',
            2 => 'c',
            _ => '/',
        })
        .collect()
}

fn address(fill: u8) -> ChunkAddress {
    ChunkAddress::new([fill; 32])
}

fn document(name: u8) -> String {
    format!("doc{name}")
}

fn metadata(key: u8, value: u8) -> BTreeMap<String, String> {
    BTreeMap::from([(format!("k{key}"), format!("v{value}"))])
}

/// Record the ops into a fresh editor.
fn record(ops: &[FuzzOp]) -> ManifestEditor<DefaultMemoryStore> {
    let mut editor = ManifestEditor::new(DefaultMemoryStore::new());
    for op in ops {
        match op {
            FuzzOp::Put { path: raw, fill } => {
                editor.put(path(raw), address(*fill));
            }
            FuzzOp::PutMeta {
                path: raw,
                fill,
                key,
                value,
            } => {
                editor.put_with_metadata(path(raw), address(*fill), metadata(*key, *value));
            }
            FuzzOp::Remove { path: raw } => {
                editor.remove(path(raw));
            }
            FuzzOp::SetIndex { name } => {
                editor.set_index_document(&document(*name));
            }
            FuzzOp::SetError { name } => {
                editor.set_error_document(&document(*name));
            }
        }
    }
    editor
}

/// The surviving state a successful commit must expose to the reader.
struct Model {
    entries: BTreeMap<String, ChunkAddress>,
    index_document: Option<String>,
    error_document: Option<String>,
}

/// Replay the ops on the path-set model; only meaningful when every op
/// applied, which a successful commit guarantees. Remove mirrors the trie's
/// boundary prune: the named path and everything under it.
fn model(ops: &[FuzzOp]) -> Model {
    let mut entries = BTreeMap::new();
    let mut index_document = None;
    let mut error_document = None;
    for op in ops {
        match op {
            FuzzOp::Put { path: raw, fill }
            | FuzzOp::PutMeta {
                path: raw, fill, ..
            } => {
                entries.insert(path(raw), address(*fill));
            }
            FuzzOp::Remove { path: raw } => {
                // Remove prunes the fork whose boundary the path names, so
                // every key under the prefix goes with it.
                let p = path(raw);
                entries.retain(|key, _| !key.starts_with(&p));
            }
            FuzzOp::SetIndex { name } => index_document = Some(document(*name)),
            FuzzOp::SetError { name } => error_document = Some(document(*name)),
        }
    }
    Model {
        entries,
        index_document,
        error_document,
    }
}

fuzz_target!(|ops: Vec<FuzzOp>| {
    let ops = &ops[..ops.len().min(MAX_OPS)];

    let first = run(record(ops).commit());
    let second = run(record(ops).commit());
    match (&first, &second) {
        (Ok((root_a, _)), Ok((root_b, _))) => {
            assert_eq!(root_a, root_b, "two commits of one log diverged");
        }
        (Err(_), Err(_)) => return,
        _ => panic!("two commits of one log disagreed on success"),
    }

    let (root, store) = first.expect("matched as Ok above");
    let want = model(ops);
    let reader = Reader::new(store);
    for (p, addr) in &want.entries {
        // The empty path is never addressable through the reader.
        if p.is_empty() {
            continue;
        }
        let entry = run(reader.get(&root, p.as_bytes()))
            .expect("lookup over a complete store succeeds")
            .unwrap_or_else(|| panic!("committed path {p:?} is unreadable"));
        assert_eq!(
            entry.reference().map(|r| *r.address()),
            Some(*addr),
            "reference diverged at {p:?}"
        );
    }
    // A removed or never-put probe must stay absent; probe removed paths.
    // The "/" node also carries the root documents, so a removed root value
    // may legitimately read back as a metadata-only entry.
    for op in ops {
        if let FuzzOp::Remove { path: raw } = op {
            let p = path(raw);
            if p.is_empty() || want.entries.contains_key(&p) {
                continue;
            }
            let got = run(reader.get(&root, p.as_bytes()))
                .expect("lookup over a complete store succeeds");
            if p == "/" {
                assert!(
                    got.as_ref().is_none_or(|e| e.reference().is_none()),
                    "removed root value still carries a reference"
                );
            } else {
                assert!(got.is_none(), "removed path {p:?} is still readable");
            }
        }
    }
    // The root documents read back as the last set values; a value op on
    // the "/" path itself may rewrite that node, so the check stands only
    // when no op touched it.
    let slash_touched = ops.iter().any(|op| match op {
        FuzzOp::Put { path: raw, .. }
        | FuzzOp::PutMeta { path: raw, .. }
        | FuzzOp::Remove { path: raw } => path(raw) == "/",
        FuzzOp::SetIndex { .. } | FuzzOp::SetError { .. } => false,
    });
    if !slash_touched && (want.index_document.is_some() || want.error_document.is_some()) {
        let root_entry = run(reader.get(&root, b"/"))
            .expect("lookup over a complete store succeeds")
            .unwrap_or_else(|| panic!("root documents set but no root entry"));
        assert_eq!(
            root_entry.metadata().get("website-index-document"),
            want.index_document.as_ref(),
            "index document diverged"
        );
        assert_eq!(
            root_entry.metadata().get("website-error-document"),
            want.error_document.as_ref(),
            "error document diverged"
        );
    }
});
