//! The other half of the vocabulary gate: what a map surface must NOT own.
//!
//! `manifest/vocabulary.rs` pins presence by calling every map verb by name. A
//! rename fails that file. It says nothing about a map surface growing `save`,
//! `publish` or `build`, which the doctrine reserves for structured content, so
//! this file pins the absence.
//!
//! How it works: [`ContentVerbsAbsent`] is blanket-implemented for every type
//! and each method returns [`NotAContentVerb`]. Rust prefers an inherent method
//! over a trait one, so if a map surface grows its own `save`, `publish` or
//! `build`, the call below binds to that instead and this file stops compiling
//! on the return type. A trait method in scope makes the call ambiguous, which
//! also stops the build. Nothing is called at runtime: the checks live in
//! functions that are type-checked and never run.
//!
//! The gap: this workspace has no `trybuild` compile-fail suite, so a negative
//! compile test is out of scope here. The shadowing pin above is the
//! best-effort stand-in, and it does not cover a verb added to a surface this
//! file does not name.

use std::sync::Arc;

use nectar_ldb::{Database, Editor, LdbManifest, Plaintext, Reader as LdbReader, View};
use nectar_loadsave::NodeLoadSaver;
use nectar_manifest::{DynManifest, Manifest};
use nectar_mantaray::{ManifestEditor, Reader as MantarayReader, TrieView, TrieWriter};
use nectar_primitives::store::{ContentGet, MemoryStore};
use nectar_primitives::{ChunkRef, DEFAULT_BODY_SIZE, StandardChunkSet};

type Raw = Arc<MemoryStore<StandardChunkSet>>;
type Store = ContentGet<Raw>;
type Nodes = NodeLoadSaver<Raw>;

/// Marker the extension methods return; a surface owning the verb itself would
/// return something else.
struct NotAContentVerb;

/// The content verbs, blanket-implemented so a map surface shadows them only by
/// owning one itself.
trait ContentVerbsAbsent {
    fn save(&self) -> NotAContentVerb {
        NotAContentVerb
    }

    fn publish(&self) -> NotAContentVerb {
        NotAContentVerb
    }

    fn build(&self) -> NotAContentVerb {
        NotAContentVerb
    }
}

impl<T: ?Sized> ContentVerbsAbsent for T {}

/// Take one verb the surface must not own.
fn absent(_: NotAContentVerb) {}

/// Assert that every content verb on `$surface` resolves to the extension.
///
/// A macro, not a function: resolution has to happen at the surface's own type.
/// Inside a function generic over the surface it would resolve once, against
/// the type parameter, and see no inherent method at all.
macro_rules! owns_no_content_verb {
    ($surface:expr) => {{
        absent($surface.save());
        absent($surface.publish());
        absent($surface.build());
    }};
}

/// The seam's own handles, through the trait rather than a format.
#[expect(dead_code, reason = "type-checked, never called: the pin is the body")]
fn the_seam_owns_no_content_verbs<M: Manifest<ChunkRef>>(
    manifest: &M,
    view: &M::View<'_>,
    writer: &M::Writer<'_>,
    erased: &dyn DynManifest,
) {
    owns_no_content_verb!(manifest);
    owns_no_content_verb!(view);
    owns_no_content_verb!(writer);
    owns_no_content_verb!(erased);
}

/// The key-value database's own handles.
#[expect(dead_code, reason = "type-checked, never called: the pin is the body")]
fn the_database_owns_no_content_verbs(
    db: &Database<Store>,
    view: &View<'_, Store>,
    editor: &Editor<'_, Store, Plaintext>,
    reader: &LdbReader<Store>,
    seam: &LdbManifest<Store>,
) {
    owns_no_content_verb!(db);
    owns_no_content_verb!(view);
    owns_no_content_verb!(editor);
    owns_no_content_verb!(reader);
    owns_no_content_verb!(seam);
}

/// The trie's own handles.
#[expect(dead_code, reason = "type-checked, never called: the pin is the body")]
fn the_trie_owns_no_content_verbs(
    editor: &ManifestEditor<Nodes>,
    reader: &MantarayReader<Nodes>,
    view: &TrieView<Nodes, Store, ChunkRef, DEFAULT_BODY_SIZE>,
    writer: &TrieWriter<Nodes, ChunkRef>,
) {
    owns_no_content_verb!(editor);
    owns_no_content_verb!(reader);
    owns_no_content_verb!(view);
    owns_no_content_verb!(writer);
}

/// The pin is the two signatures above: this test only witnesses that the file
/// was compiled.
#[test]
fn map_surfaces_own_no_content_verbs() {
    absent(NotAContentVerb);
}
