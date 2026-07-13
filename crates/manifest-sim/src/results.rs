//! Serializable result schema: `format -> corpus -> scale -> cell`.
//!
//! Every numeric field is `Option`; a `None` serializes to JSON `null` and is
//! only ever set by a real measurement or left null with a reason. No field is
//! back-filled by estimate. The top-level `capability_matrix` and `headlines`
//! are the machine-readable op x format matrix and the cross-cell multiplier
//! summary; both are populated only from executed cells (headlines) or from
//! fixed API facts verified on the source tree (the matrix rows).

use std::collections::BTreeMap;

use serde::Serialize;

/// A discrete PMF: value -> frequency, serialized with stringised integer keys.
pub type Histogram = BTreeMap<u64, u64>;

/// The whole result document.
#[derive(Debug, Serialize)]
pub struct Document {
    pub meta: Meta,
    /// Exhaustive op x format capability matrix (fixed API facts).
    pub capability_matrix: Vec<CapabilityRow>,
    /// Cross-cell headline multipliers, computed from executed cells only.
    pub headlines: Headlines,
    pub formats: BTreeMap<String, FormatBlock>,
}

/// Run-level metadata.
#[derive(Debug, Serialize)]
pub struct Meta {
    pub generated: String,
    pub git_branch: String,
    pub git_commit: String,
    pub harness_version: String,
    pub seed_master: String,
    pub rtt_ms: u32,
    pub rtt_ms_set: Vec<u32>,
    pub batch_k_sweep: Vec<u64>,
    pub range_windows: Vec<f64>,
    pub value_read_corpus: String,
    pub chunk_body_size: u32,
    pub criterion_iters_note: String,
    pub sample_keys: u32,
    pub update_sample: u32,
    pub batch_ops: u32,
    /// Honest caveats embedded so numbers are never read out of context.
    pub caveats: Vec<String>,
}

/// One row of the capability matrix (fixed API facts, not a measurement).
#[derive(Debug, Clone, Serialize)]
pub struct CapabilityRow {
    pub n: u32,
    pub op: String,
    pub v1_supported: bool,
    pub v1_api: Option<String>,
    pub v1_class: String,
    pub v02_supported: bool,
    pub v02_api: Option<String>,
    pub v02_class: String,
    pub v02_fallback: Option<String>,
    pub v02_asymptote: Option<String>,
    pub notes: String,
}

/// Cross-cell headline multipliers, all derived from executed cells only.
#[derive(Debug, Default, Serialize)]
pub struct Headlines {
    pub notes: String,
    pub entries: Vec<Headline>,
}

/// One headline multiplier at a `(corpus, scale)` where both sides ran.
#[derive(Debug, Serialize)]
pub struct Headline {
    pub metric: String,
    pub corpus: String,
    pub scale: u64,
    pub native: f64,
    pub native_unit: String,
    pub fallback: Option<f64>,
    pub fallback_unit: String,
    pub multiplier: Option<f64>,
    pub reason_if_null: Option<String>,
}

/// One format's corpora.
#[derive(Debug, Serialize)]
pub struct FormatBlock {
    #[serde(rename = "crate")]
    pub crate_name: String,
    pub registry: String,
    pub corpora: BTreeMap<String, CorpusBlock>,
}

/// One corpus' scales.
#[derive(Debug, Serialize)]
pub struct CorpusBlock {
    pub scales: BTreeMap<String, Cell>,
}

/// One measured `(format, corpus, scale)` cell.
#[derive(Debug, Default, Serialize)]
pub struct Cell {
    pub ran: bool,
    pub reason: Option<String>,
    pub n_keys: Option<u64>,
    pub key_encoding: Option<String>,
    pub build: Option<Build>,
    pub storage: Option<Storage>,
    pub tree_depth: Option<Depth>,
    pub get: Option<Get>,
    pub listing: Option<Listing>,
    pub floor: Option<Floor>,
    pub ceiling: Option<Ceiling>,
    pub range: Option<Range>,
    pub iter_full: Option<IterFull>,
    pub value_read: Option<ValueRead>,
    pub update: Option<Update>,
    /// The full O(N) entries-walk fetch count (0.2 only), used by the bin to
    /// cross-fill floor/ceiling/range fallback multipliers into the 1.0 cell.
    pub full_entries_walk_fetches: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Build {
    pub wall_ns: Option<u64>,
    pub criterion_ns_per_op: Option<f64>,
    pub criterion_stddev_ns: Option<f64>,
    pub peak_open_nodes: Option<u64>,
    pub nodes_written: Option<u64>,
    pub nodes_embedded: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub peak_live_store_bytes: Option<u64>,
    /// 1.0 = peak_open_nodes (O(depth) frontier); 0.2 = nodes_written (O(N)).
    pub builder_frontier_nodes: Option<u64>,
    /// Least-squares fit of log(ns) vs log(N); filled in the bin.
    pub cpu_loglog: Option<LogLog>,
    /// distinct puts of a second identical build / distinct puts of the first.
    pub dedup_ratio_second_build: Option<f64>,
    pub dedup_reason: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct LogLog {
    pub slope: f64,
    pub intercept: f64,
    pub r2: f64,
    pub n_points: u32,
    pub basis: String,
}

#[derive(Debug, Default, Serialize)]
pub struct Storage {
    pub total_chunks: Option<u64>,
    pub total_payload_bytes: Option<u64>,
    pub storage_utilisation: Option<f64>,
    pub bytes_per_key: Option<f64>,
    pub chunks_per_key: Option<f64>,
    /// embedded/(embedded+written); 0.2 has no embedding so this is 0.0.
    pub embedding_ratio: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Depth {
    pub min: Option<u64>,
    pub mean: Option<f64>,
    pub p95: Option<u64>,
    pub max: Option<u64>,
    pub histogram: Option<Histogram>,
    pub fanout_mean: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Get {
    pub sampled_keys: Option<u64>,
    pub hops_mean: Option<f64>,
    pub hops_p95: Option<u64>,
    pub hops_max: Option<u64>,
    pub load_latency_ms: Option<LoadLatency>,
    pub criterion_ns_per_op: Option<f64>,
    pub hops_histogram: Option<Histogram>,
    pub hops_cdf: Option<Cdf>,
    pub wall_latency_ms: Option<WallLatency>,
}

#[derive(Debug, Default, Serialize)]
pub struct LoadLatency {
    pub mean: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<f64>,
    pub derived_from_hops: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct Cdf {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub max: u64,
}

/// Multi-RTT hop x RTT model. Never a measurement: the `model` string states so.
#[derive(Debug, Serialize)]
pub struct WallLatency {
    pub by_rtt_ms: BTreeMap<String, LatQuad>,
    pub model: String,
}

#[derive(Debug, Default, Serialize)]
pub struct LatQuad {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct Listing {
    pub method: String,
    pub fetch_count: Option<u64>,
    pub keys_returned: Option<u64>,
    pub fetches_per_key: Option<f64>,
    /// 0.2 pessimal fallback: a full entries() walk over ALL keys.
    pub fallback_02_full_entries: Option<KfCount>,
    /// 0.2 fair fallback: walk_from(prefix) on the SAME prefixes as 1.0.
    pub fallback_02_walk_from: Option<KfCount>,
    /// 0.2 fair fetch / 1.0 fetch on identical prefixes; filled in the bin.
    pub multiplier_fair: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct KfCount {
    pub fetches: u64,
    pub keys_returned: u64,
    pub fetches_per_key: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct Floor {
    pub supported: bool,
    pub reason: Option<String>,
    pub hops_mean: Option<f64>,
    pub hops_p95: Option<u64>,
    pub hops_max: Option<u64>,
    pub hops_histogram: Option<Histogram>,
    pub fallback_02: Option<Fallback>,
}

/// A 0.2 emulation cost and its multiplier vs a native cost; filled in the bin
/// for the matching `(corpus, scale)` where both sides ran.
#[derive(Debug, Default, Serialize)]
pub struct Fallback {
    pub method: String,
    pub fetches: Option<u64>,
    pub multiplier: Option<f64>,
    pub reason_if_null: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct Ceiling {
    pub supported: bool,
    pub class: String,
    pub native_seek_hops_mean: Option<f64>,
    pub native_seek_hops_max: Option<u64>,
    pub fallback_02: Option<Fallback>,
}

#[derive(Debug, Default, Serialize)]
pub struct Range {
    pub supported: bool,
    pub reason: Option<String>,
    pub windows: Vec<RangeWindow>,
}

#[derive(Debug, Default, Serialize)]
pub struct RangeWindow {
    pub w: f64,
    pub fetch_count: Option<u64>,
    pub keys_returned: Option<u64>,
    pub fallback_02_fetches: Option<u64>,
    pub multiplier: Option<f64>,
    pub reason_if_null: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct IterFull {
    pub native_fetch_to_first_key: Option<u64>,
    pub native_fetch_all: Option<u64>,
    pub fallback_02_materialise_fetches: Option<u64>,
    pub ordered_guaranteed: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct ValueRead {
    pub inline_fraction: Option<f64>,
    pub fetches_native_mean: Option<f64>,
    pub fetches_02_mean: Option<f64>,
    pub chunks_saved_by_inline: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Default, Serialize)]
pub struct Update {
    pub single: Option<SingleUpdate>,
    pub batch: Option<BatchUpdate>,
    pub batch_sweep: Vec<BatchPoint>,
    pub subtree_delete: Option<SubtreeDelete>,
}

#[derive(Debug, Default, Serialize)]
pub struct SingleUpdate {
    pub update: Option<OpCost>,
    pub insert: Option<OpCost>,
    pub delete: Option<OpCost>,
    pub criterion_ns_per_op: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct OpCost {
    pub chunks_rewritten_mean: Option<f64>,
    pub wall_ns: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchUpdate {
    pub k_ops: Option<u64>,
    pub mix: String,
    pub chunks_rewritten: Option<u64>,
    pub wall_ns: Option<u64>,
    pub write_amplification: Option<f64>,
    pub criterion_ns_per_op: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct BatchPoint {
    pub k: u64,
    pub mix: String,
    pub chunks_rewritten: u64,
    pub wall_ns: u64,
    pub write_amplification: f64,
    pub ns_per_op: f64,
}

#[derive(Debug, Default, Serialize)]
pub struct SubtreeDelete {
    pub prefix_utf8: Option<String>,
    pub keys_deleted: u64,
    pub chunks_rewritten: u64,
    pub wall_ns: u64,
    pub reason: Option<String>,
}
