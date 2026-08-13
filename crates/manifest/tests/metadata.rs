//! The bidirectional metadata seam: the seam's own view and key table, and
//! the cross-format copy, where the target rebuilds its own metadata type
//! from the source's `MetadataSource` pairs. No format names the other, and
//! what a copy drops is the target's stated limit.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use nectar_ldb::{CustomKey, Format, KeyId, Metadata, MetadataKey, V1};
use nectar_manifest::WellKnownKey::{ContentType, Custom, ErrorDocument, Filename, IndexDocument};
use nectar_manifest::{
    ErasedManifest, ManifestMeta, ManifestPath, MetadataSource, MetadataView, WellKnownKey,
};
use nectar_mantaray::{MantarayManifest, NodeLoadSaver, Reader as MantarayReader, metadata};
use nectar_primitives::{ChunkAddress, ChunkRef, DEFAULT_BODY_SIZE};
use nectar_testing::run;

mod common;
use common::stores;

/// The registry rebuilt from `source`.
fn kv(source: &dyn MetadataSource) -> Option<Metadata<V1>> {
    <Option<Metadata<V1>>>::from_source(source)
}

/// The trie's string map rebuilt from `source`.
fn map(source: &dyn MetadataSource) -> BTreeMap<String, String> {
    <BTreeMap<String, String>>::from_source(source)
}

/// `get` as text, for one-line assertions.
fn text<'a>(source: &'a dyn MetadataSource, key: &WellKnownKey<'_>) -> Option<&'a str> {
    source.get(key).map(|value| str::from_utf8(value).unwrap())
}

/// The enumerated pairs, as text.
fn pairs(source: &dyn MetadataSource) -> Vec<(String, String)> {
    let mut out = Vec::new();
    source.for_each(&mut |key, value| {
        out.push((key.to_owned(), String::from_utf8(value.to_vec()).unwrap()));
    });
    out
}

/// Text pairs, owned.
fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

/// A string map from text pairs.
fn text_map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    owned(pairs).into_iter().collect()
}

/// The value the registry stores under the id, as bytes.
fn stored(kv_meta: &Option<Metadata<V1>>, id: KeyId) -> Option<&[u8]> {
    kv_meta.as_ref()?.get(&id.into()).map(Bytes::as_ref)
}

/// Any spelling of a registered name addresses one slot, REGISTERED is the
/// one exhaustive table, and a round trip never forks the vocabulary.
#[test]
fn the_key_table_resolves_every_spelling_to_one_slot() {
    let view = MetadataView::new()
        .with(ContentType, "text/html")
        .with(Custom("etag"), "abc");
    assert_eq!(text(&view, &ContentType), Some("text/html"));
    assert_eq!(text(&view, &Custom("etag")), Some("abc"));
    assert_eq!(view.get(&Custom("missing")), None);
    let view = MetadataView::new().with(Custom("content-type"), "text/css");
    assert_eq!(text(&view, &ContentType), Some("text/css"));
    assert_eq!(Custom("CONTENT-TYPE").resolve(), ContentType);
    assert_eq!(WellKnownKey::registered("x-note"), None);

    // Exhaustive on purpose: a new variant fails to compile here until it is
    // placed in REGISTERED, which is what `registered` matches against.
    const fn slot(key: &WellKnownKey<'_>) -> Option<usize> {
        match key {
            ContentType => Some(0),
            Filename => Some(1),
            IndexDocument => Some(2),
            ErrorDocument => Some(3),
            Custom(_) => None,
        }
    }
    assert_eq!(slot(&Custom("x-note")), None);
    for (index, key) in WellKnownKey::REGISTERED.iter().enumerate() {
        assert_eq!(slot(key), Some(index), "{key:?} is out of place");
        let name = key.name();
        assert_eq!(WellKnownKey::registered(name), Some(*key));
        assert_eq!(WellKnownKey::registered(&name.to_lowercase()), Some(*key));
        assert_eq!(WellKnownKey::registered(&name.to_uppercase()), Some(*key));
    }
}

/// The trie's map answers a registered key under either spelling, rebuilds
/// canonically from any source with zero pair loss, and enumerates verbatim.
#[test]
fn a_string_map_is_a_source_and_rebuilds_canonically() {
    let source = text_map(&[("content-type", "text/css"), ("x-note", "hi")]);
    assert_eq!(text(&source, &ContentType), Some("text/css"));
    assert_eq!(text(&source, &Custom("Content-Type")), Some("text/css"));
    assert_eq!(text(&source, &Custom("x-note")), Some("hi"));
    assert_eq!(MetadataSource::get(&source, &Custom("gone")), None);

    // A registered key lands under the canonical spelling on rebuild; a
    // custom pair copies verbatim.
    let canon = text_map(&[("Content-Type", "text/css"), ("x-note", "hi")]);
    let rebuilt = map(&source);
    assert_eq!(rebuilt, canon);
    assert!(!rebuilt.contains_key("content-type"));
    let view = MetadataView::new()
        .with(ContentType, "text/html")
        .with(Custom("x-note"), "hi");
    let html = owned(&[("Content-Type", "text/html"), ("x-note", "hi")]);
    let want: BTreeMap<_, _> = html.clone().into_iter().collect();
    assert_eq!(map(&view), want);

    // Case folding collapses two spellings of one name: the copy keeps one
    // pair, and this pins which - the last enumerated spelling wins.
    let both = text_map(&[("Content-Type", "a"), ("content-type", "b")]);
    assert_eq!(map(&both), text_map(&[("Content-Type", "b")]));

    // `for_each` is verbatim, and `()` carries nothing.
    assert_eq!(pairs(&view), html);
    assert!(pairs(&()).is_empty());
    assert_eq!(().get(&ContentType), None);
}

/// The trie's native map and the key-value registry copy through the seam
/// with no format naming the other, and the round trip is exact.
#[test]
fn metadata_crosses_formats_without_naming_them() {
    let trie_meta = text_map(&[("Content-Type", "text/html"), ("x-note", "hi")]);

    // Into the registry: the registered key lands as its KeyId, not as a
    // custom key, and the custom pair survives byte-for-byte.
    let kv_meta = kv(&trie_meta);
    let block = kv_meta.as_ref().expect("two pairs crossed");
    let got = stored(&kv_meta, KeyId::ContentType);
    assert_eq!(got, Some("text/html".as_bytes()));
    let custom: MetadataKey<V1> = CustomKey::try_from(&b"x-note"[..]).unwrap().into();
    assert_eq!(block.get(&custom), Some(&Bytes::from_static(b"hi")));
    assert_eq!(block.pair_count(), 2);

    // Back to the trie's map: the registered key keeps the reference-client
    // spelling, so the round trip is the identity.
    assert_eq!(map(&kv_meta), trie_meta);

    // The file name crosses the same way: `Filename` in the trie, the
    // `filename` id in the registry, each side keeping its own spelling.
    let named = text_map(&[(metadata::FILENAME, "logo.png")]);
    let kv_meta = kv(&named);
    assert_eq!(stored(&kv_meta, KeyId::Filename), Some(&b"logo.png"[..]));
    assert_eq!(map(&kv_meta), named);
    assert_eq!(text(&kv_meta, &Filename), Some("logo.png"));
}

/// The seam-registered keys all cross into the registry as ids, driven by
/// `WellKnownKey::REGISTERED` rather than a per-format table, and the
/// registry's encoded bound is its stated limit: an over-budget pair is
/// dropped there, and every pair that fits still lands.
#[test]
fn every_registered_key_crosses_and_the_bound_is_the_registrys() {
    let mut view = MetadataView::new();
    for key in WellKnownKey::REGISTERED {
        view.set(key, "value");
    }
    let kv_meta = kv(&view);
    let block = kv_meta.as_ref().expect("every registered key crossed");
    assert_eq!(block.pair_count(), WellKnownKey::REGISTERED.len());
    for id in [
        KeyId::ContentType,
        KeyId::Filename,
        KeyId::WebsiteIndexDocument,
        KeyId::WebsiteErrorDocument,
    ] {
        assert!(block.get(&id.into()).is_some(), "{id:?} travelled by name");
    }
    for key in WellKnownKey::REGISTERED {
        assert_eq!(text(&kv_meta, &key), Some("value"));
    }

    let view = MetadataView::new()
        .with(ContentType, "text/html")
        .with(Custom("x-big"), "a".repeat(V1::META_MAX));
    let kv_meta = kv(&view);
    let block = kv_meta.as_ref().expect("the fitting pair landed");
    assert_eq!(block.pair_count(), 1);
    assert_eq!(text(&kv_meta, &ContentType), Some("text/html"));
    assert_eq!(kv_meta.get(&Custom("x-big")), None);
    // The trie's map has no such bound: the same source copies whole.
    assert_eq!(map(&view).len(), 2);
}

/// Registry values are bytes: a non-UTF-8 value crosses the erased read
/// verbatim, and only the trie's text rebuild replaces it.
#[test]
fn registry_values_cross_as_bytes() {
    let block = Metadata::<V1>::new(KeyId::ContentType, Bytes::from_static(&[0xFF, 0xFE])).unwrap();
    let kv_meta = Some(block);
    assert_eq!(kv_meta.get(&ContentType), Some(&[0xFF, 0xFE][..]));
    assert_eq!(map(&kv_meta)["Content-Type"], "\u{FFFD}\u{FFFD}");

    // A custom key that names no UTF-8 string stays behind the static path.
    let key = CustomKey::try_from(&[0xFF][..]).unwrap();
    let unnamed = Some(Metadata::<V1>::new(key, Bytes::from_static(b"v")).unwrap());
    unnamed.for_each(&mut |key, _| panic!("a non-UTF-8 custom key enumerated as {key}"));
}

/// An erased write into the trie lands the reference client's `Content-Type`
/// spelling on the fork record, exactly as the native path would, and reads
/// back through the erased seam by key.
#[test]
fn an_erased_write_lands_the_reference_client_spelling_in_the_trie() {
    run(async {
        let (raw, store) = stores();
        let nodes = NodeLoadSaver::new(Arc::clone(&raw));
        let trie = MantarayManifest::<_, _, DEFAULT_BODY_SIZE>::new(nodes.clone(), store);
        let base = trie.dyn_empty().await.unwrap();
        let path = ManifestPath::from("index.html");
        let meta = MetadataView::new().with(ContentType, "text/html");
        let file = ChunkRef::new(ChunkAddress::new([7; 32]));
        let root = trie.dyn_insert(&base, path.clone(), file, &meta).await;
        let root = root.unwrap();

        let reader = MantarayReader::new(nodes);
        let entry = reader.get(root, b"index.html").await.unwrap();
        let entry = entry.expect("the erased insert landed");
        let got = entry.metadata().get(metadata::CONTENT_TYPE);
        assert_eq!(got, Some(&"text/html".to_owned()), "native spelling");

        let read = trie.dyn_metadata(&root, &path).await.unwrap();
        assert_eq!(text(&*read, &ContentType), Some("text/html"));
    });
}
