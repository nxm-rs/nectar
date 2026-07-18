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
mod error;
mod format;

pub use bounded::{MetadataLen, Prefix, SegmentWeight};
pub use error::{MetadataTooLong, PrefixTooLong, WeightOverBudget};
pub use format::{Format, V1};
