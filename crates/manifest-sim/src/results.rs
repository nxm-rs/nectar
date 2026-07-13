//! Serializable result schema: `format -> corpus -> scale -> cell`.
//!
//! Every numeric field is `Option`; a `None` serializes to JSON `null` and is
//! only ever set by a real measurement or left null with a reason. No field is
//! back-filled by estimate.

use std::collections::BTreeMap;

use serde::Serialize;

/// The whole result document.
#[derive(Debug, Serialize)]
pub struct Document {
    pub meta: Meta,
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
    pub chunk_body_size: u32,
    pub criterion_iters_note: String,
    pub sample_keys: u32,
    pub update_sample: u32,
    pub batch_ops: u32,
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
    pub range: Option<Range>,
    pub update: Option<Update>,
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
}

#[derive(Debug, Default, Serialize)]
pub struct Storage {
    pub total_chunks: Option<u64>,
    pub total_payload_bytes: Option<u64>,
    pub storage_utilisation: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Depth {
    pub min: Option<u64>,
    pub mean: Option<f64>,
    pub p95: Option<u64>,
    pub max: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Get {
    pub sampled_keys: Option<u64>,
    pub hops_mean: Option<f64>,
    pub hops_p95: Option<u64>,
    pub hops_max: Option<u64>,
    pub load_latency_ms: Option<LoadLatency>,
    pub criterion_ns_per_op: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct LoadLatency {
    pub mean: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<f64>,
    pub derived_from_hops: bool,
}

#[derive(Debug, Default, Serialize)]
pub struct Listing {
    pub method: String,
    pub fetch_count: Option<u64>,
    pub keys_returned: Option<u64>,
    pub fetches_per_key: Option<f64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Floor {
    pub supported: bool,
    pub reason: Option<String>,
    pub hops_mean: Option<f64>,
    pub hops_p95: Option<u64>,
    pub hops_max: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Range {
    pub supported: bool,
    pub reason: Option<String>,
    pub fetch_count: Option<u64>,
    pub keys_returned: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct Update {
    pub single: Option<SingleUpdate>,
    pub batch: Option<BatchUpdate>,
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
