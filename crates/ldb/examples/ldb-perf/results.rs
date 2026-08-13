//! Serializable schema for the range-query performance run.
//!
//! Every numeric field is measured by executing the real reader or cursor; a
//! `None` serializes to JSON `null` and is only ever set by a capability gap
//! left with a reason, never back-filled by estimate. The one
//! modelled quantity is the wall-clock latency, and it is always the product of
//! a MEASURED count (fetches or cursor rounds) with a stated RTT: the `model`
//! string on every latency block says so.

use std::collections::BTreeMap;

use nectar_testing::bench::RunMeta;
use serde::Serialize;

/// The whole result document.
#[derive(Debug, Serialize)]
pub struct Document {
    pub meta: Meta,
    /// Parallel-cursor rounds and the serial-vs-concurrent latency model.
    pub parallel_cursor: Vec<ParallelCursorCell>,
    /// V1Read vs V1: fetches-per-window, depth, single-update write-amp.
    pub v1read: Vec<ReadProfileCell>,
    /// Rank-directed paginate vs the O(offset) skip baseline.
    pub paginate: Vec<PaginateCell>,
    /// Subtree-ref handoff vs the full cursor walk for a folder listing.
    pub subtree_serve: Vec<SubtreeServeCell>,
}

/// Run-level metadata. The shared header flattens in first, so the document
/// keeps the key order the checked-in results were written at.
#[derive(Debug, Serialize)]
pub struct Meta {
    #[serde(flatten)]
    pub run: RunMeta,
    pub seed_master: String,
    pub rtt_ms_set: Vec<u32>,
    pub read_ahead: u32,
    pub scales: Vec<u64>,
    pub corpora: Vec<String>,
    pub range_windows: Vec<f64>,
    pub paginate_offsets: Vec<u64>,
    pub paginate_limit: u32,
    pub chunk_body_size: u32,
    pub caveats: Vec<String>,
}

/// One `(corpus, scale, op, window)` parallel-cursor cell.
#[derive(Debug, Serialize)]
pub struct ParallelCursorCell {
    pub corpus: String,
    pub scale: u64,
    /// `range` (fractional window sweep) or `prefix` (a natural subtree).
    pub op: String,
    /// Range width as a fraction of the key domain; `null` for a prefix op.
    pub window: Option<f64>,
    pub keys_returned: u64,
    /// Node fetches to drain the scan; identical for serial and concurrent.
    pub fetch_count: u64,
    /// Sequential fetch rounds the bounded-concurrency cursor actually takes,
    /// read off a paused virtual clock (READ_AHEAD cap), never guessed.
    pub rounds: u64,
    pub read_ahead: u32,
    /// Per-RTT serial and bounded-concurrent latency and their speedup.
    pub by_rtt_ms: BTreeMap<String, CursorLatency>,
    pub model: String,
}

/// Serial and concurrent wall-clock at one RTT, with the speedup between them.
#[derive(Debug, Serialize)]
pub struct CursorLatency {
    /// `fetch_count * rtt`: one round trip per node.
    pub serial_ms: f64,
    /// `rounds * rtt`: the measured bounded-concurrency round count times RTT.
    pub concurrent_ms: f64,
    /// `serial_ms / concurrent_ms` (== `fetch_count / rounds`).
    pub speedup: Option<f64>,
}

/// One `(corpus, scale)` V1Read-vs-V1 cell.
#[derive(Debug, Serialize)]
pub struct ReadProfileCell {
    pub corpus: String,
    pub scale: u64,
    pub v1: ReadProfileSide,
    pub v1read: ReadProfileSide,
    /// Per-window `v1read_fetch / v1_fetch`; below 1.0 is the read win.
    pub fetch_ratio_by_window: BTreeMap<String, f64>,
    /// `v1read` mean get-depth over `v1` mean get-depth.
    pub depth_ratio: Option<f64>,
    /// The honest cost: `v1read - v1` mean chunks rewritten per single update.
    pub single_update_wa_delta: f64,
    /// `v1read / v1` single-update write-amplification.
    pub single_update_wa_ratio: Option<f64>,
}

/// One format's read-profile figures.
#[derive(Debug, Serialize)]
pub struct ReadProfileSide {
    pub version_byte: u8,
    pub inline_max: u32,
    pub tree_depth_mean: f64,
    pub tree_depth_max: u64,
    /// Fetches to drain each range window, keyed by window fraction.
    pub range_fetch_by_window: BTreeMap<String, u64>,
    /// Mean chunks rewritten by a single-key update over the sample.
    pub single_update_chunks_mean: f64,
}

/// One `(corpus, scale, offset)` pagination cell.
#[derive(Debug, Serialize)]
pub struct PaginateCell {
    pub corpus: String,
    pub scale: u64,
    pub offset: u64,
    pub limit: u32,
    pub keys_returned: u64,
    /// Rank-directed `paginate` node fetches: O(depth), ~constant.
    pub paginate_fetch_count: u64,
    /// Baseline `iter().skip(offset).take(limit)` node fetches: grows with
    /// offset.
    pub skip_baseline_fetch_count: u64,
    /// `skip_baseline / paginate`; grows with offset as the win widens.
    pub skip_over_paginate: Option<f64>,
}

/// One `(corpus, scale)` subtree-serve cell: a folder listing handed off as a
/// single subtree reference versus walked in full from the database root.
#[derive(Debug, Serialize)]
pub struct SubtreeServeCell {
    pub corpus: String,
    pub scale: u64,
    /// The listing prefix (lossy UTF-8).
    pub prefix: String,
    pub keys_returned: u64,
    /// Whether one chunk holds exactly the prefix's keys, so its reference
    /// can be handed off.
    pub handoff_found: bool,
    /// Node fetches to resolve the handoff reference: O(depth) to the
    /// boundary, nothing below it.
    pub handoff_fetch_count: u64,
    /// Node fetches to drain the full prefix cursor from the root.
    pub cursor_walk_fetch_count: u64,
    /// `cursor_walk / handoff`; `null` without a handoff.
    pub walk_over_handoff: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::{Document, Meta};
    use nectar_testing::bench::RunMeta;

    /// The flattened header keeps the four provenance keys ahead of the
    /// harness fields, so a rerun under one `SOURCE_DATE_EPOCH` stays
    /// byte-identical to the documents already published.
    #[test]
    fn the_document_opens_with_the_shared_header() {
        let doc = Document {
            meta: Meta {
                run: RunMeta {
                    generated: "1970-01-01T00:00:00Z".to_string(),
                    git_branch: "main".to_string(),
                    git_commit: "abc".to_string(),
                    harness_version: "4".to_string(),
                },
                seed_master: "0x0".to_string(),
                rtt_ms_set: Vec::new(),
                read_ahead: 16,
                scales: Vec::new(),
                corpora: Vec::new(),
                range_windows: Vec::new(),
                paginate_offsets: Vec::new(),
                paginate_limit: 20,
                chunk_body_size: 4096,
                caveats: Vec::new(),
            },
            parallel_cursor: Vec::new(),
            v1read: Vec::new(),
            paginate: Vec::new(),
            subtree_serve: Vec::new(),
        };
        let want = [
            "{",
            r#"  "meta": {"#,
            r#"    "generated": "1970-01-01T00:00:00Z","#,
            r#"    "git_branch": "main","#,
            r#"    "git_commit": "abc","#,
            r#"    "harness_version": "4","#,
            r#"    "seed_master": "0x0","#,
        ]
        .join("\n");
        let json = serde_json::to_string_pretty(&doc).unwrap();
        assert!(json.starts_with(&want), "{json}");
    }
}
