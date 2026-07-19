//! Fuzz the manifest editor against the legacy mutation path.
//!
//! A fuzzed op sequence is committed through the editor and replayed
//! eagerly through the legacy manifest. The committed root is defined as
//! the legacy root for the same sequence, so the oracle is exact: equal
//! roots on success, and a failure at the same submission index otherwise.
//! Two independent commits of the same log must also agree.
//!
//! Paths come from a four-symbol alphabet so fork splits, nested prefixes
//! and separator edges stay dense in the search space.

#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use futures::executor::block_on;
use libfuzzer_sys::fuzz_target;
use nectar_mantaray::{DefaultMemoryStore, EditorError, ManifestEditor};
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

/// Outcome of the eager legacy replay.
enum Legacy {
    /// Every op applied and the trie persisted to this root.
    Root(ChunkAddress),
    /// The op at this submission index failed.
    OpFailed(usize),
    /// All ops applied but persisting failed.
    SaveFailed,
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

/// Replay the ops eagerly through the legacy manifest, stopping at the
/// first failure the way an aborted commit would.
#[allow(deprecated)]
fn run_legacy(ops: &[FuzzOp]) -> Legacy {
    use nectar_mantaray::Manifest;
    block_on(async {
        let mut manifest = Manifest::new(DefaultMemoryStore::new());
        for (index, op) in ops.iter().enumerate() {
            let result = match op {
                FuzzOp::Put { path: raw, fill } => manifest.add(&path(raw), address(*fill)).await,
                FuzzOp::PutMeta {
                    path: raw,
                    fill,
                    key,
                    value,
                } => {
                    manifest
                        .add_with_metadata(&path(raw), address(*fill), metadata(*key, *value))
                        .await
                }
                FuzzOp::Remove { path: raw } => manifest.remove(&path(raw)).await,
                FuzzOp::SetIndex { name } => manifest.set_index_document(&document(*name)).await,
                FuzzOp::SetError { name } => manifest.set_error_document(&document(*name)).await,
            };
            if result.is_err() {
                return Legacy::OpFailed(index);
            }
        }
        match manifest.save().await {
            Ok(root) => Legacy::Root(root),
            Err(_) => Legacy::SaveFailed,
        }
    })
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

fuzz_target!(|ops: Vec<FuzzOp>| {
    let ops = &ops[..ops.len().min(MAX_OPS)];

    let first = block_on(record(ops).commit());
    let second = block_on(record(ops).commit());
    match (&first, &second) {
        (Ok((root_a, _)), Ok((root_b, _))) => {
            assert_eq!(root_a, root_b, "two commits of one log diverged");
        }
        (Err(_), Err(_)) => {}
        _ => panic!("two commits of one log disagreed on success"),
    }

    match run_legacy(ops) {
        Legacy::Root(root) => {
            let (committed, _) = first.expect("commit must succeed where the legacy path did");
            assert_eq!(
                committed, root,
                "committed root diverged from the legacy path"
            );
        }
        Legacy::OpFailed(index) => match first {
            Ok(_) => panic!("commit succeeded where legacy op {index} failed"),
            Err(EditorError::Apply { index: failed, .. }) => {
                assert_eq!(
                    failed, index,
                    "commit failed at a different op than the legacy path"
                );
            }
            Err(EditorError::Commit(_)) => {
                panic!("commit failed persisting where legacy op {index} failed")
            }
            Err(_) => panic!("commit failed atypically where legacy op {index} failed"),
        },
        Legacy::SaveFailed => {
            assert!(
                first.is_err(),
                "commit succeeded where the legacy save failed"
            );
        }
    }
});
