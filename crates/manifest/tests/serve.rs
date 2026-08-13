//! Both formats resolve a request path under the site conventions the same
//! way: exact wins, then the index-document join, then the error document.
//! The trie rides the seam's provided default; the key-value database
//! overrides `serve` with its native resolver, so every case runs on both.

#![allow(clippy::indexing_slicing, clippy::unwrap_used)]

use bytes::Bytes;
use nectar_ldb::{Entry, Key, KeyId};
use nectar_manifest::{Batch, Manifest, ManifestPath, ManifestView, MapEntry, Served};
use nectar_primitives::ChunkRef;
use nectar_testing::run;

mod common;
use common::{both_formats, reference, stores};

/// The site under test: distinct references tell the resolved entry apart.
const PAGES: &[(&str, u8)] = &[
    ("index.html", 1),
    ("docs/index.html", 2),
    ("404.html", 3),
    ("a.html", 4),
];

/// A resolved `Served` variant bound to `reference(byte)`.
macro_rules! hit {
    ($kind:ident, $path:expr, $byte:expr) => {
        Served::$kind {
            path: ManifestPath::from($path),
            entry: MapEntry::Reference(reference($byte)),
        }
    };
}

/// Build one root from [`PAGES`] plus the optional conventions.
async fn built<M: Manifest<ChunkRef>>(
    manifest: &M,
    empty: ChunkRef,
    index: Option<&str>,
    error: Option<&str>,
) -> ChunkRef {
    let mut batch: Batch<ChunkRef, M::Metadata> = Batch::new();
    for (path, byte) in PAGES {
        batch.insert(ManifestPath::from(*path), reference(*byte));
    }
    if let Some(index) = index {
        batch.set_index_document(ManifestPath::from(index));
    }
    if let Some(error) = error {
        batch.set_error_document(ManifestPath::from(error));
    }
    manifest.apply(empty, batch).await.unwrap()
}

async fn served<M: Manifest<ChunkRef>>(manifest: &M, root: ChunkRef, path: &str) -> Served {
    let view = manifest.at(root);
    view.serve(&ManifestPath::from(path)).await.unwrap()
}

/// Run the whole request matrix over one format.
async fn conforms<M: Manifest<ChunkRef>>(manifest: &M, empty: ChunkRef, format: &str) {
    // Site shapes: both conventions, neither, index only, error only.
    let sites = [
        built(manifest, empty, Some("index.html"), Some("404.html")).await,
        built(manifest, empty, None, None).await,
        built(manifest, empty, Some("index.html"), None).await,
        built(manifest, empty, None, Some("404.html")).await,
    ];
    let cases: &[(usize, &str, Served)] = &[
        // An exact content path wins over both conventions.
        (0, "a.html", hit!(Exact, "a.html", 4)),
        (0, "index.html", hit!(Exact, "index.html", 1)),
        // The root serves its index document; a directory path resolves its
        // own, with or without the trailing separator.
        (0, "", hit!(Index, "index.html", 1)),
        (0, "docs/", hit!(Index, "docs/index.html", 2)),
        (0, "docs", hit!(Index, "docs/index.html", 2)),
        // An unresolved path falls back to the error document, whole.
        (0, "missing", hit!(Error, "404.html", 3)),
        (0, "docs/nope", hit!(Error, "404.html", 3)),
        // The reserved separator never answers exactly: its index join binds
        // nothing, so it falls through to the error document.
        (0, "/", hit!(Error, "404.html", 3)),
        // Without conventions the fallbacks vanish.
        (1, "missing", Served::Missing),
        (1, "", Served::Missing),
        (1, "a.html", hit!(Exact, "a.html", 4)),
        // Index only: an unresolved path has nowhere left to fall.
        (2, "missing", Served::Missing),
        // Error only: even the root request falls to the error document.
        (3, "", hit!(Error, "404.html", 3)),
    ];
    for (site, path, want) in cases {
        let got = served(manifest, sites[*site], path).await;
        assert_eq!(&got, want, "{format} site {site} {path:?}");
    }
}

#[test]
fn both_formats_serve_the_same_matrix() {
    run(async {
        let (raw, store) = stores();
        let ((trie, trie_empty), (kv, kv_empty)) = both_formats(&raw, &store).await;
        conforms(&trie, trie_empty, "trie").await;
        conforms(&kv, kv_empty, "kv").await;
    });
}

/// The database's native resolver reads the reserved slots; the seam's
/// override must not let them answer.
#[test]
fn a_natively_bound_reserved_slot_never_serves() {
    run(async {
        let (raw, store) = stores();
        let ((_, _), (kv, kv_empty)) = both_formats(&raw, &store).await;
        let site = built(&kv, kv_empty, Some("index.html"), None).await;

        // Bind the two reserved slots through the native editor, below the
        // seam's refusal. Binding the empty key clears the root metadata, so
        // the index-document convention is restaged with it.
        let mut editor = kv.edit(&site);
        editor.insert(Key::empty(), Entry::from(reference(9)));
        editor.insert(Key::from(&b"/"[..]), Entry::from(reference(10)));
        let doc = Some(Bytes::from_static(b"index.html"));
        editor.set_root_metadata(KeyId::WebsiteIndexDocument, doc);
        let bound = editor.commit().await.unwrap();

        // The exact probe skips both slots: the empty path still serves the
        // index document, and the separator has nothing to fall back to.
        let got = served(&kv, bound, "").await;
        assert_eq!(got, hit!(Index, "index.html", 1), "empty slot answered");
        let got = served(&kv, bound, "/").await;
        assert!(got.is_missing(), "separator slot must not serve exactly");

        // An error document pointed at a reserved path names no content, so
        // the bound slot must not answer the fallback either.
        let mut editor = kv.edit(&bound);
        editor.set_root_metadata(KeyId::WebsiteIndexDocument, None);
        editor.set_root_metadata(KeyId::WebsiteErrorDocument, Some(Bytes::from_static(b"/")));
        let reserved_error = editor.commit().await.unwrap();
        let got = served(&kv, reserved_error, "nope").await;
        assert!(got.is_missing(), "a reserved error document must not serve");
    });
}
