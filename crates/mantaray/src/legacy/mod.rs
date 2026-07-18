//! Deprecated lazy-trie manifest surface, superseded by the streaming
//! modules. Removal is gated on the manifest 1.0 key-value store replacing
//! this crate's manifest surface. The trie substrate the editor replays
//! (node add, remove, and load) is not legacy and stays with the editor.
//!
//! # Migration
//!
//! | Legacy | Replacement |
//! |---|---|
//! | `Manifest::get`, `Manifest::lookup` | [`Reader::get`](crate::Reader::get) |
//! | `Manifest::has_prefix` | [`Reader::has_prefix`](crate::Reader::has_prefix) |
//! | `Manifest::add`, `add_with_metadata`, `remove` | [`ManifestEditor::put`](crate::ManifestEditor::put), `put_with_metadata`, `remove` |
//! | `Manifest::save` | [`ManifestEditor::commit`](crate::ManifestEditor) |
//! | `Manifest::set_index_document`, `set_error_document` | the [`ManifestEditor`](crate::ManifestEditor) twins |
//! | `Manifest::entries`, `entries_concurrent`, `stream`, `walk` | [`Cursor`](crate::Cursor) |
//! | `Manifest::iterate_addresses` | [`AddressStream`](crate::AddressStream) |
//! | `Manifest::read`, `ManifestBuilder::put_file` | [`Reader::get`](crate::Reader::get) plus the file pipeline |
//! | `ManifestIter` | [`Cursor`](crate::Cursor) |
//! | `ManifestBuilder` | [`ManifestEditor`](crate::ManifestEditor) |

#![allow(deprecated)]

mod builder;
mod manifest;
mod node;

pub use builder::ManifestBuilder;
pub use manifest::{DEFAULT_LIST_CONCURRENCY, Manifest, ManifestIter};
