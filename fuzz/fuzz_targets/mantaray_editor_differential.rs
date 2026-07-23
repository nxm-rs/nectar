//! Fuzz the manifest editor against a reader-based path-set model.
//!
//! Raw bytes decode into an op log through the crate's `EditorOp` grammar;
//! the shared `nectar_mantaray::oracles::editor_differential` oracle commits
//! the log twice, checks both roots agree, and holds the reader to exactly
//! the model's surviving paths, references and root documents.
//!
//! Seeds live in `fuzz/seeds/mantaray_editor_differential/` and are replayed
//! on stable by `seed_replay_mantaray_editor_differential` in
//! `crates/mantaray/src/editor.rs`, through the same oracle.

#![no_main]

use libfuzzer_sys::fuzz_target;
use nectar_mantaray::oracles::{self, EditorOp};
use nectar_testing::run;

fuzz_target!(|ops: Vec<EditorOp>| {
    run(oracles::editor_differential(&ops)).expect("the editor and the reader model must agree");
});
