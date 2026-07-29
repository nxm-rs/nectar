//! Mantaray 1.0: a content-addressed key-value database, stored as a
//! compacted radix-256 trie of content chunks.
//!
//! Every frozen layout parameter of the wire format lives as an associated
//! const on the sealed [`Format`] trait; [`V1`] is the frozen `tag_version
//! 0x01` parameter set, and public types default their format parameter to
//! [`V1`]. Bounded newtypes ([`Prefix`], [`MetadataLen`], [`SegmentWeight`])
//! check a format bound once at construction and carry it as a type
//! invariant. The node grammar weaves order-statistic [`SubtreeCount`]s into
//! every referenced child and segment descriptor, so navigation by rank costs
//! O(depth) instead of O(window). [`V1Read`] is an opt-in read-optimized
//! sibling: the same layout with a heavier embedding budget, trading
//! single-update write-amplification for fewer chunks touched per range or
//! listing window.
//!
//! The value model is [`Key`] (arbitrary bytes), [`Entry`] (a chunk
//! reference or an inline value; absence is `Option` at the use site) and
//! [`Metadata`] (typed key-registry pairs, sorted-unique and bounded).
//!
//! The data model is [`Node`]: an optional [`RootExtension`] (the root's
//! own entry and manifest metadata, complete in the root's own bytes) over
//! a [`ForkTable`] of [`ForkRecord`]s keyed on the first prefix byte, so
//! fork order and the radix-256 bound are structural. Only presence bits are
//! derived from the structure at encode time.
//!
//! Node addressing is generic over the sealed `Reference` trait: `Node<F, R>`
//! links its children by `R::SIZE` references, defaulting to `ChunkRef`. The
//! width is one whole-database fact, witnessed once per chunk in its flags
//! byte, so a plaintext and a structurally encrypted database are distinct
//! types that can neither mix on the wire nor be read as one another.
//!
//! The codec is [`Node::encode`] and [`Node::decode`] over the primitives
//! wire cursor and writer. Decode is reject-or-accept and dispatches on the
//! in-payload preamble, failing loud on anything that is not a 1.0
//! manifest; no other format is co-decoded.
//!
//! The packing layer is the deterministic tree shape: [`embed`] (child-local
//! inlining), [`h64`]/[`cut`]/[`segment`] (content-defined boundaries keyed on
//! the fork-relative prefix) and [`spill`] (a <= depth-2 [`Directory`] for an
//! oversized fork table). Every boundary is a pure function of content, so an
//! insert disturbs `O(1)` boundaries and re-rooting does not churn.
//!
//! Encryption is per-reference: an encrypted reference carries `address ||
//! key`, transporting the child's decryption key in the parent record with no
//! side channel, so reading a node opens every child it references,
//! recursively. Structural encryption is the reference parameter: a database
//! keyed by `EncryptedChunkRef` stores every node and segment as ciphertext.
//! Reading needs the reference alone, so a build without the `encryption`
//! feature still opens and re-serializes an encrypted database losslessly; only
//! the write side (key derivation and sealing, [`Encrypted`]) sits behind the
//! feature. The key derivation is deterministic, so an encrypted tree keeps
//! canonical bytes and cross-build dedup. Values stay width-free: an
//! [`Entry`] names a plain or an encrypted chunk whatever the structure's own
//! width.
//!
//! The folder view ([`Reader::list`], [`Reader::serve`]) is a path
//! interpretation over the flat KV core: the separator is [`Format::SEPARATOR`],
//! derived from the key bytes at read time and never stored, and the website
//! index- and error-document conventions ride in the root's typed metadata, not
//! magic keys. A listing collapses deeper keys at the next separator and seeks
//! past each named subtree, so it stays O(depth) and fetches no value chunk.
//!
//! PRIVACY: an encrypted reference IS a read capability for its whole
//! subtree. Confidentiality rests solely on the outermost reference being
//! distributed privately. See the `encryption` module.
//!
//! ```
//! use nectar_ldb::{Format, Prefix, V1};
//!
//! assert_eq!(V1::PREAMBLE, [0x6D, 0x01]);
//! let prefix: Prefix = Prefix::try_from(&b"index.html"[..]).unwrap();
//! assert!(prefix.len() <= V1::PLEN_MAX);
//! ```

#![no_std]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::get_unwrap,
        clippy::indexing_slicing,
        clippy::string_slice,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::unreachable,
        clippy::panic_in_result_fn,
        clippy::as_conversions
    )
)]

extern crate alloc;
#[cfg(any(test, feature = "std"))]
extern crate std;

mod apply;
mod bounded;
mod builder;
mod codec;
mod count;
#[cfg(feature = "encryption")]
mod encryption;
mod error;
mod folder;
mod fork;
mod format;
mod frontier;
#[cfg(any(test, feature = "arbitrary"))]
#[cfg_attr(docsrs, doc(cfg(feature = "arbitrary")))]
pub mod generators;
#[cfg(feature = "manifest")]
#[cfg_attr(docsrs, doc(cfg(feature = "manifest")))]
pub mod manifest;
mod meta;
mod node;
/// Shared fuzz and test oracle for the node codec. Compiled for in-crate
/// tests and for fuzz builds (`arbitrary`); exempt from semver guarantees.
#[cfg(any(test, feature = "arbitrary"))]
#[doc(hidden)]
pub mod oracles;
mod order;
mod packing;
mod reader;
mod scan;
mod store;
mod traverse;
mod value;

pub use apply::{ApplyError, Changeset, apply};
pub use bounded::{MetadataLen, Prefix, SegmentWeight};
pub use builder::{BuildError, BuildStats, Builder, Built};
pub use codec::{DecodeError, EncodeError, recanonicalize};
pub use count::{CountError, SubtreeCount};
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub use encryption::{Encrypted, derive_key};
pub use error::{
    CustomKeyError, ForkPrefixEmpty, MetadataTooLong, NotAReference, PrefixTooLong, ValueTooLong,
    WeightOverBudget,
};
pub use folder::{DirEntry, Listing, Served, Website};
#[cfg(feature = "manifest")]
pub use manifest::{LdbManifest, ManifestError};
pub use fork::{Child, ForkPayload, ForkRecord, ForkTable};
pub use format::{Format, V1, V1Read};
pub use meta::{CustomKey, KeyId, Metadata, MetadataKey};
pub use node::{Node, NodeRef, RootExtension};
pub use packing::{Directory, SegmentKind, cut, embed, h64, segment, spill};
pub use reader::{Reader, ReaderError};
pub use scan::Cursor;
pub use store::{NodeChunk, NodeGet, NodePut, Plaintext, Seal, StoreError};
pub use traverse::AddressStream;
pub use value::{Entry, InlineValue, Key};
