//! Mantaray 1.0: a content-addressed key-value manifest, stored as a
//! compacted radix-256 trie of content chunks.
//!
//! Every frozen layout parameter of the wire format lives as an associated
//! const on the sealed [`Format`] trait; [`V1`] is the frozen `tag_version
//! 0x01` parameter set, and public types default their format parameter to
//! [`V1`]. Bounded newtypes ([`Prefix`], [`MetadataLen`], [`SegmentWeight`])
//! check a format bound once at construction and carry it as a type
//! invariant.
//!
//! The value model is [`Key`] (arbitrary bytes), [`Entry`] (a chunk
//! reference or an inline value; absence is `Option` at the use site) and
//! [`Metadata`] (typed key-registry pairs, sorted-unique and bounded).
//!
//! The data model is [`Node`]: an optional [`RootExtension`] (the root's
//! own entry and manifest metadata, complete in the root's own bytes) over
//! a [`ForkTable`] of [`ForkRecord`]s keyed on the first prefix byte, so
//! fork order and the radix-256 bound are structural. No flags are stored;
//! presence bits are derived from the structure at encode time.
//!
//! The codec is [`Node::encode`] and [`Node::decode`] over the primitives
//! wire cursor and writer. Decode is reject-or-accept and dispatches on the
//! in-payload preamble, failing loud on anything that is not a 1.0
//! manifest; no other format is co-decoded.
//!
//! ```
//! use nectar_manifest::{Format, Prefix, V1};
//!
//! assert_eq!(V1::PREAMBLE, [0x6D, 0x01]);
//! let prefix: Prefix = Prefix::try_from(&b"index.html"[..]).unwrap();
//! assert!(prefix.len() <= V1::PLEN_MAX);
//! ```

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
        clippy::panic_in_result_fn
    )
)]

mod bounded;
mod codec;
mod error;
mod fork;
mod format;
mod meta;
mod node;
mod value;

pub use bounded::{MetadataLen, Prefix, SegmentWeight};
pub use codec::{DecodeError, EncodeError};
pub use error::{
    CustomKeyError, ForkPrefixEmpty, MetadataTooLong, PrefixTooLong, ValueTooLong, WeightOverBudget,
};
pub use fork::{Child, ForkPayload, ForkRecord, ForkTable};
pub use format::{Format, V1};
pub use meta::{CustomKey, KeyId, Metadata, MetadataKey};
pub use node::{Node, RootExtension};
pub use value::{Entry, InlineValue, Key};
