//! Inclusion and exclusion proofs over a mantaray 1.0 manifest, authenticated
//! against a trusted root address.
//!
//! A proof is the authenticated descent path for a key `K` under a root `R`: an
//! ordered chain of nodes from `R` down, each authenticated against the
//! reference the node before it yielded, so the whole path is a BMT hash chain
//! anchored at the root. An [inclusion](prove_inclusion) proof terminates at the
//! entry for `K`; an [exclusion](prove_exclusion) proof terminates where the
//! descent provably cannot continue to `K` (no fork for the next byte, a
//! compacted edge that diverges, or `K` exhausting with no terminal entry).
//! Soundness rests on canonicalness (spec 6.2): an authenticated node's fork set
//! is complete and its edges maximal, so local absence is global absence.
//!
//! Two granularities carry the same authentication (see [`Granularity`]): a
//! full-chunk proof ships whole node bytes the verifier re-BMTs; a BMT-segment
//! proof ships only the leading segments the descent reads, each behind its
//! sibling path, the on-chain-cheap form. Both reuse the primitives BMT
//! ([`Hasher`](nectar_primitives::Hasher)); this crate never re-hashes by hand.
//!
//! Embedding shortens proofs: an embedded child rides in its parent's bytes, so
//! the descent crosses it with no extra step; only a referenced edge is a hop.
//! [`verify`] replays the descent over the authenticated bytes and reports what
//! it finds, so a proof asserts no verdict its bytes do not.
//!
//! Two compositions ride the single-key primitives. A
//! [range-completeness](prove_range_complete) proof attests a listing is every
//! key in `[lo, hi)`, authenticating the frontier of nodes the range spans so an
//! omitted key has no witness. A [state-transition](prove_transition) proof
//! attests one key changed between two roots - an insertion, or its deletion and
//! update duals - as its state under each root.
//!
//! A further family rides the authenticated subtree counts the node grammar
//! carries: [`prove_rank`],
//! [`prove_count`], [`prove_select`] and [`prove_page`] answer order-statistic
//! questions in O(depth). These assume an HONEST BUILDER: a referenced child's
//! count is author-asserted, bound to its parent chunk but not to the child's
//! real subtree, so a counted proof establishes the count as committed by the
//! root, not against an adversarial root. The strictly trustless answers ride
//! the count-independent inclusion, exclusion and range-completeness proofs. See
//! the [`prove_count`] trust-boundary notes for the full statement.
//!
//! The model is generic over [`Format`](nectar_manifest::Format) and defaults to
//! the frozen `V1` plaintext layout.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::panic
    )
)]

mod counted;
mod descent;
mod error;
mod proof;
mod prove;
mod range;
mod transition;
mod verify;

pub use counted::{
    CountProof, CountedPath, PageProof, RankProof, SelectProof, prove_count, prove_page,
    prove_page_prefix, prove_rank, prove_select, verify_count, verify_page, verify_page_prefix,
    verify_rank, verify_select,
};
pub use descent::DescentError;
pub use error::{ProveError, VerifyError};
pub use proof::{ForkPathProof, Granularity, PathStep, Verdict};
pub use prove::{NodeSource, prove_exclusion, prove_inclusion};
pub use range::{RangeProof, prove_range_complete, verify_range};
pub use transition::{
    Transition, TransitionProof, prove_deletion, prove_transition, prove_update, verify_transition,
};
pub use verify::verify;
