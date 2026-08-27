//! The bounded in-memory load over both formats: a bound the entry fits
//! assembles it, a bound it outruns refuses typed, and the refusal names the
//! end of the first frame past the bound.

#![allow(
    clippy::as_conversions,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::unwrap_used
)]

use nectar_ldb::{Entry, Key};
use nectar_manifest::{CollectError, Manifest, ManifestError, ManifestPath, ManifestView};
use nectar_primitives::ChunkRef;
use nectar_testing::run;

mod common;
use common::{both_formats, p, save_file, stores};

/// Enough bytes to span several frames.
fn payload() -> Vec<u8> {
    (0u32..10_000).map(|i| (i % 251) as u8).collect()
}

/// The loaded path.
fn path() -> ManifestPath {
    p("doc.html")
}

/// The (exceeds, max) witness of a refusal, so the test reads the refusal
/// structurally, not by name.
fn witness(error: CollectError<impl core::error::Error>) -> (u64, u64) {
    match error {
        CollectError::TooLarge { exceeds, max } => (exceeds, max),
        other => panic!("the refusal lost its witness: {other:?}"),
    }
}

/// `view` refuses a load of `path()`: the refusal is a refusal, names the
/// bound, and names a frame end past the bound and at most the entry size.
async fn outrun<V: ManifestView<ChunkRef>>(view: V, hint: &str) {
    let bound = 2_500u64;
    let total = u64::try_from(payload().len()).unwrap();
    let error = view.collect(&path(), bound).await.unwrap_err();
    assert!(error.is_too_large(), "{hint} refuses too large");
    assert!(
        !error.is_load_failure(),
        "{hint} a refusal is not a load failure"
    );
    let (exceeds, max) = witness(error);
    assert_eq!(max, bound, "{hint} the refusal names the bound");
    assert!(
        exceeds > bound && exceeds <= total,
        "{hint} the refusal ends past the bound and at most at the entry size, got {exceeds}"
    );
}

/// `view` loads nothing at an absent path: the failure crosses as a load
/// failure, and it is the seam's own not found.
async fn loads_as_not_found<V, F>(view: V, hint: &str)
where
    F: core::error::Error + nectar_marker::MaybeSend + nectar_marker::MaybeSync + 'static,
    V: ManifestView<ChunkRef, Error = ManifestError<F>>,
{
    let error = view.collect(&p("absent.html"), 0).await.unwrap_err();
    assert!(
        error.is_load_failure(),
        "{hint} a load failure is a load failure"
    );
    assert!(
        !error.is_too_large(),
        "{hint} a load failure is not a refusal"
    );
    let is_not_found = matches!(&error, CollectError::Load(ManifestError::NotFound(_)));
    assert!(
        is_not_found,
        "{hint} the load failure is the seam's not found"
    );
}

#[test]
fn a_reference_entry_assembles_under_a_fitting_bound() {
    run(async {
        let (raw, store) = stores();
        let data = payload();
        let file = save_file(&raw, &data).await;
        let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
        let bound = u64::try_from(data.len()).unwrap();

        let trie_root = trie.insert(trie_empty, path(), file).await.unwrap();
        let bytes = trie.at(trie_root).collect(&path(), bound).await.unwrap();
        assert_eq!(&bytes[..], &data[..], "trie assembles the entry");

        let kv_root = Manifest::insert(&kv, kv_empty, path(), file).await.unwrap();
        let bytes = kv.at(&kv_root).collect(&path(), bound).await.unwrap();
        assert_eq!(&bytes[..], &data[..], "kv assembles the entry");
    });
}

#[test]
fn a_bound_below_a_reference_entry_refuses_with_the_outrun_end() {
    run(async {
        let (raw, store) = stores();
        let data = payload();
        let file = save_file(&raw, &data).await;
        let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;

        let trie_root = trie.insert(trie_empty, path(), file).await.unwrap();
        outrun(trie.at(trie_root), "trie").await;

        let kv_root = Manifest::insert(&kv, kv_empty, path(), file).await.unwrap();
        outrun(kv.at(&kv_root), "kv").await;
    });
}

#[test]
fn an_inline_value_assembles_and_refuses_the_same_way() {
    run(async {
        let (raw, store) = stores();
        let (_, (kv, kv_empty)) = both_formats(&raw, &store).await;
        let value: Vec<u8> = (0u32..100).map(|i| (i % 251) as u8).collect();
        let note = p("note.txt");
        let key: Key = note.as_bytes().into();
        let entry = Entry::inline(value.clone().into()).unwrap();
        let root = kv.insert(&kv_empty, key, entry).await.unwrap();

        // The bound is the value's own length: it fits exactly.
        let bytes = kv.at(&root).collect(&note, 100).await.unwrap();
        assert_eq!(&bytes[..], &value[..], "kv assembles the inline value");

        let error = kv.at(&root).collect(&note, 40).await.unwrap_err();
        assert!(error.is_too_large(), "kv refuses too large");
        // The inline value is one frame, so the witness names its whole end.
        let (exceeds, max) = witness(error);
        assert_eq!((exceeds, max), (100, 40), "kv the witness is the value end");
    });
}

#[test]
fn a_load_failure_crosses_as_a_load_failure() {
    run(async {
        let (raw, store) = stores();
        let data = payload();
        let file = save_file(&raw, &data).await;
        let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;

        let trie_root = trie.insert(trie_empty, path(), file).await.unwrap();
        loads_as_not_found(trie.at(trie_root), "trie").await;

        let kv_root = Manifest::insert(&kv, kv_empty, path(), file).await.unwrap();
        loads_as_not_found(kv.at(&kv_root), "kv").await;
    });
}
