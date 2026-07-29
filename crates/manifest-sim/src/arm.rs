//! The two-arm seam: one trait, [`Arm`], that both manifest formats implement
//! over their own instrumented store, plus the shared vocabulary the result
//! schema is written in.
//!
//! Store fetches and puts are the currency; a capability gap is a
//! null-with-reason, never an estimate. Every wrapper is sync over
//! `nectar_testing::run`, matching the existing harness style
//! (`crates/manifest-sim/src/perf.rs`).
//!
//! Schema note: [`Capability`], [`OpCost`], [`OpOutcome`], [`FrontierClass`]
//! and [`NullWithReason`] carry both `Serialize` and `Deserialize` because the
//! result cells in `results.rs` embed them and the document must round-trip
//! through the renderer. `Capability`'s `how`/`cost_class`/`reason` are
//! therefore owned `String`s (a `&'static str` cannot deserialize); the
//! constructor helpers take `&'static str`, so the arms still name their
//! emulations with string literals.

use std::error::Error;

use serde::{Deserialize, Serialize};

use crate::corpus::GenKey;
use crate::store::Counters;

/// The harness error type, boxed like the rest of the harness
/// (`crates/manifest-sim/src/perf.rs`).
pub type Err = Box<dyn Error>;

/// How one arm serves one operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Capability {
    /// An O(depth) primitive of the format.
    Native,
    /// A public-API emulation; `how` names it, `cost_class` states its
    /// asymptote.
    Emulated {
        /// The public-API path the emulation rides.
        how: String,
        /// The emulation's measured asymptote.
        cost_class: String,
    },
    /// No primitive and no honest emulation; the reason is the finding.
    Unsupported {
        /// Why no honest measurement exists.
        reason: String,
    },
}

impl Capability {
    /// A native O(depth) primitive.
    #[must_use]
    pub const fn native() -> Self {
        Self::Native
    }

    /// A public-API emulation named by `how` with asymptote `cost_class`.
    #[must_use]
    pub fn emulated(how: &'static str, cost_class: &'static str) -> Self {
        Self::Emulated {
            how: how.to_string(),
            cost_class: cost_class.to_string(),
        }
    }

    /// An unsupported operation carrying its reason.
    #[must_use]
    pub fn unsupported(reason: &'static str) -> Self {
        Self::Unsupported {
            reason: reason.to_string(),
        }
    }
}

/// Store-counter cost of one operation, read by snapshot delta.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct OpCost {
    /// Node fetches (`get` delta) the operation charged.
    pub fetches: u64,
    /// Chunk puts (`put` delta) the operation charged.
    pub puts: u64,
    /// Keys the operation returned.
    pub keys_returned: u64,
}

/// One measured operation: its class and, unless unsupported, its cost.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpOutcome {
    /// How the arm served the operation.
    pub capability: Capability,
    /// The measured cost; `None` only when the capability is unsupported.
    pub cost: Option<OpCost>,
}

/// The memory law of one build.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum FrontierClass {
    /// O(depth): peak simultaneously-open nodes, from `BuildStats`.
    Bounded {
        /// Peak open nodes over the build.
        peak_open_nodes: u64,
    },
    /// O(N): the commit persists a fully materialised trie, so every node is
    /// live at once; the count is the node total, witnessed by node saves.
    WholeTrie {
        /// Resident node chunks after the commit.
        resident_nodes: u64,
    },
}

/// One build over a fresh counting store.
#[derive(Clone, Copy, Debug)]
pub struct BuildReport {
    /// The build's memory law.
    pub frontier: FrontierClass,
    /// Node chunks written to the store.
    pub nodes_written: u64,
    /// `None` on 0.2: the format has no embedding.
    pub nodes_embedded: Option<u64>,
}

/// Batch-update mode for the write-amplification sweep.
#[derive(Clone, Copy, Debug)]
pub enum BatchMode {
    /// Each arm's best batching: one `Changeset` on 1.0, one multi-op
    /// `ManifestEditor::commit` on 0.2.
    Batched,
    /// One commit or apply per edit on both arms: the naive client.
    PerEdit,
}

/// A recorded gap: which arm, which field, why.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NullWithReason {
    /// The arm the gap belongs to.
    pub arm: String,
    /// The field left null.
    pub field: String,
    /// Why the field is null.
    pub reason: String,
}

/// One manifest arm over its own instrumented store.
///
/// Wrappers are sync over `nectar_testing::run`, matching the existing
/// harness style (`crates/manifest-sim/src/perf.rs`). Each arm owns one
/// `CountingStore`, so the comparison is between counters, never shared state.
pub trait Arm {
    /// JSON label: "mantaray-0.2", "ldb-v1" or "ldb-v1read".
    fn label(&self) -> &'static str;
    /// Build every key into a fresh store; sets the arm's root.
    fn build(&mut self, keys: &[GenKey]) -> Result<BuildReport, Err>;
    /// Counter snapshot of the arm's store (storage metrics after build).
    fn counters(&self) -> Counters;
    /// Point lookup; both formats native.
    fn get(&self, key: &[u8]) -> Result<OpOutcome, Err>;
    /// Greatest key `<= key`.
    fn floor(&self, key: &[u8]) -> Result<OpOutcome, Err>;
    /// Smallest key `>= key`.
    fn ceiling(&self, key: &[u8]) -> Result<OpOutcome, Err>;
    /// Ascending drain of `[lo, hi)`, best public-API path.
    fn range(&self, lo: &[u8], hi: &[u8]) -> Result<OpOutcome, Err>;
    /// The pessimal fallback for the same window (full walk plus filter).
    fn range_pessimal(&self, lo: &[u8], hi: &[u8]) -> Result<OpOutcome, Err>;
    /// Fair prefix listing (pruned walk on 0.2, prefix scan on 1.0).
    fn prefix_list(&self, prefix: &[u8]) -> Result<OpOutcome, Err>;
    /// The pessimal whole-manifest walk for the same prefix.
    fn prefix_list_pessimal(&self, prefix: &[u8]) -> Result<OpOutcome, Err>;
    /// Ascending full iteration; records the ordering guarantee in `how`.
    fn full_iter(&self) -> Result<OpOutcome, Err>;
    /// K updates against the current root; the root is not advanced, so every
    /// sweep starts from the same tree.
    fn batch_update(&self, edits: &[GenKey], mode: BatchMode) -> Result<OpOutcome, Err>;
    /// One timed cold build over a fresh plain store; wall-time lane only
    /// (spec section 5).
    fn timed_build(&self, keys: &[GenKey]) -> Result<core::time::Duration, Err>;
}
