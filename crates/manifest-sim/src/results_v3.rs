//! Serializable schema for the v3 range-query performance run.
//!
//! Every numeric field is measured by executing the real reader or cursor; a
//! `None` serializes to JSON `null` and is only ever set by a capability gap
//! left with a reason, never back-filled by estimate. The one
//! modelled quantity is the wall-clock latency, and it is always the product of
//! a MEASURED count (fetches or cursor rounds) with a stated RTT: the `model`
//! string on every latency block says so.

use std::collections::BTreeMap;

use serde::Serialize;

/// The whole v3 result document.
#[derive(Debug, Serialize)]
pub struct DocumentV3 {
    pub meta: MetaV3,
    /// Parallel-cursor rounds and the serial-vs-concurrent latency model.
    pub parallel_cursor: Vec<ParallelCursorCell>,
    /// V1Read vs V1: fetches-per-window, depth, single-update write-amp.
    pub v1read: Vec<ReadProfileCell>,
    /// Rank-directed paginate vs the O(offset) skip baseline.
    pub paginate: Vec<PaginateCell>,
}

/// Run-level metadata.
#[derive(Debug, Serialize)]
pub struct MetaV3 {
    pub generated: String,
    pub git_branch: String,
    pub git_commit: String,
    pub harness_version: String,
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
    /// The 0.2 resume-token walk is an emulation: the cursor restarts pages
    /// but cannot skip, so it pays the full scan to the offset.
    pub v02_emulated: bool,
    /// Fetches for the 0.2 editor's cursor to page through to the offset.
    pub v02_resume_walk_fetch_count: u64,
    /// `v02_resume_walk / paginate`.
    pub v02_over_paginate: Option<f64>,
}
