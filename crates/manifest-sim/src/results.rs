//! Serializable schema for the two-arm manifest measurement run (harness
//! version 5).
//!
//! Every numeric field is measured by executing the real reader, cursor or
//! editor over one of the shared corpora; a `None` serializes to JSON `null`
//! and is only ever set by a capability gap left with a reason, never
//! back-filled by estimate. The one modelled quantity in the deterministic
//! section is wall-clock latency, always the product of a MEASURED count with a
//! stated RTT; the `model` string on every latency block says so. The build
//! wall-time axis is the one sanctioned addition of real timing and lives in
//! its own [`WallTimeSection`].
//!
//! The whole document carries both `Serialize` and `Deserialize` so the
//! renderer can read a run back and emit tables without re-measuring.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use crate::arm::NullWithReason;
use crate::arm::{Capability, FrontierClass, OpOutcome};

/// The whole result document: metadata, the bit-reproducible deterministic
/// section, and the wall-clock section that varies between runs by design.
#[derive(Debug, Serialize, Deserialize)]
pub struct Document {
    /// Run-level provenance.
    pub meta: Meta,
    /// Bit-identical across runs under the master seed.
    pub deterministic: DeterministicSection,
    /// Wall-clock; varies between runs by design.
    pub wall_time: WallTimeSection,
}

/// The deterministic (fetch-and-count) section.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DeterministicSection {
    /// The measured capability matrix, one row per operation.
    pub capability_matrix: Vec<CapabilityRow>,
    /// Whitepaper 2.1: storage, utilisation and embed fraction.
    pub storage: Vec<StorageCell>,
    /// Whitepaper 2.2: get-hop distributions with the RTT columns.
    pub get_hops: Vec<GetHopsCell>,
    /// Whitepaper 2.3: fair and pessimal prefix listing.
    pub prefix_listing: Vec<PrefixListingCell>,
    /// Whitepaper section 3: floor/ceiling/range multipliers.
    pub ordered_ops: Vec<OrderedOpCell>,
    /// Whitepaper 6.2: write amplification versus K.
    pub write_amp: Vec<WriteAmpCell>,
    /// Whitepaper 6.1: build frontier and profile.
    pub build_profile: Vec<BuildProfileCell>,
    /// Parallel-cursor rounds and the serial-vs-concurrent latency model.
    pub parallel_cursor: Vec<ParallelCursorCell>,
    /// V1Read vs V1: fetches-per-window, depth, single-update write-amp.
    pub v1read: Vec<ReadProfileCell>,
    /// Rank-directed paginate vs the O(offset) skip baseline.
    pub paginate: Vec<PaginateCell>,
    /// Subtree-ref handoff vs the full cursor walk for a folder listing.
    pub subtree_serve: Vec<SubtreeServeCell>,
}

/// The non-deterministic build wall-time section.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct WallTimeSection {
    /// Cold-pass build wall-time cells.
    pub build_wall: Vec<BuildWallCell>,
    /// Gaps: an arm above its scale cap is a null-with-reason.
    pub nulls: Vec<NullWithReason>,
}

/// One arm's provenance line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArmMeta {
    /// JSON label ("mantaray-0.2", "ldb-v1", "ldb-v1read").
    pub label: String,
    /// The crate the arm drives.
    pub package: String,
    /// The workspace version the arm crate was built at.
    pub version: String,
}

/// Run-level metadata.
#[derive(Debug, Serialize, Deserialize)]
pub struct Meta {
    /// Run timestamp; `SOURCE_DATE_EPOCH` pins it so two runs are
    /// byte-identical.
    pub generated: String,
    /// Current git branch.
    pub git_branch: String,
    /// Current git commit.
    pub git_commit: String,
    /// The single version authority for the harness and its schema.
    pub harness_version: String,
    /// The two arms and the crates they drive.
    pub arms: Vec<ArmMeta>,
    /// Master corpus seed, hex.
    pub seed_master: String,
    /// RTT set for the latency columns, including the 100 ms mobile column.
    pub rtt_ms_set: Vec<u32>,
    /// The parallel cursor read-ahead.
    pub read_ahead: u32,
    /// Scales run.
    pub scales: Vec<u64>,
    /// The 0.2 arm scale cap; above it every 0.2 cell is a null-with-reason.
    pub max_mantaray_scale: u64,
    /// Timed build passes per (arm, corpus, scale).
    pub build_samples: u32,
    /// The write-amplification K sweep.
    pub write_amp_ks: Vec<u64>,
    /// Corpora run.
    pub corpora: Vec<String>,
    /// Range-window fractions.
    pub range_windows: Vec<f64>,
    /// Pagination offsets.
    pub paginate_offsets: Vec<u64>,
    /// Keys per pagination request.
    pub paginate_limit: u32,
    /// The chunk body size both arms divide utilisation by.
    pub chunk_body_size: u32,
    /// Run-level caveats.
    pub caveats: Vec<String>,
}

// ---- new two-arm cells (spec sections 3 and 6) ---------------------------

/// The measured capability matrix (whitepaper 2.0), re-derived from the crates
/// in tree, one row per operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityRow {
    /// The operation name.
    pub op: String,
    /// Capability per arm, keyed by arm label.
    pub per_arm: BTreeMap<String, Capability>,
}

/// Whitepaper 2.1: storage, slot utilisation and embed fraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// Distinct resident chunks after build, per arm.
    pub total_chunks: BTreeMap<String, u64>,
    /// live_bytes / (total_chunks * chunk_body_size), per arm.
    pub slot_utilisation: BTreeMap<String, f64>,
    /// nodes_embedded / (nodes_embedded + nodes_written); 1.0 arms only.
    pub embed_fraction: BTreeMap<String, f64>,
    /// 0.2 chunks over 1.0 chunks (the "fewer" column).
    pub chunk_ratio_02_over_10: Option<f64>,
    /// Gaps.
    pub nulls: Vec<NullWithReason>,
}

/// Whitepaper 2.2: get-hop distributions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetHopsCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// Deterministic evenly spaced probe count.
    pub sample: u64,
    /// Hop statistics per arm.
    pub per_arm: BTreeMap<String, HopStats>,
    /// 0.2 mean hops over 1.0 mean hops (the "cut" column).
    pub mean_ratio_02_over_10: Option<f64>,
    /// Gaps.
    pub nulls: Vec<NullWithReason>,
}

/// One arm's get-hop statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HopStats {
    /// Mean hops over the probe set.
    pub mean: f64,
    /// Maximum hops observed.
    pub max: u64,
    /// Full distribution: hop count -> probe count; the CDF's raw data.
    pub histogram: BTreeMap<u64, u64>,
    /// hops * rtt for rtt in the RTT set; illustrative.
    pub latency_ms_by_rtt: BTreeMap<String, f64>,
    /// The latency model string.
    pub model: String,
}

/// Whitepaper 2.3: fair and pessimal prefix listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefixListingCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// The listing prefix (lossy UTF-8).
    pub prefix: String,
    /// Keys the prefix selects.
    pub keys_returned: u64,
    /// Fair walk per arm (1.0 prefix scan; 0.2 pruned walk_from).
    pub fair: BTreeMap<String, OpOutcome>,
    /// Pessimal whole-manifest walk per arm (labels carry the class).
    pub pessimal: BTreeMap<String, OpOutcome>,
    /// fair 0.2 fetches over fair 1.0 fetches: the apples-to-apples figure.
    pub fair_multiplier: Option<f64>,
    /// pessimal 0.2 fetches over fair 1.0 fetches: labelled pessimal.
    pub pessimal_multiplier: Option<f64>,
    /// Gaps.
    pub nulls: Vec<NullWithReason>,
}

/// Whitepaper section 3: floor/ceiling/range multipliers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderedOpCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// "floor" | "ceiling" | "range".
    pub op: String,
    /// Window fraction for range; null for floor and ceiling.
    pub window: Option<f64>,
    /// Deterministic probe count (present and mutated-absent keys).
    pub probes: u64,
    /// AGGREGATE cost over the cell's `probes` probes, per arm, best
    /// public-API path. `OpCost` is integral, so the per-probe mean cannot be
    /// stored without loss; it is `fetches / probes` and the renderer divides.
    pub fair: BTreeMap<String, OpOutcome>,
    /// ONE pessimal measurement per arm, not an aggregate: a whole-manifest
    /// walk costs the same whichever probe asked for it, so the figure is
    /// already a per-probe cost. On the 1.0 arms there is no degraded path, so
    /// the entry repeats the native cost and stays classed
    /// [`Capability::Native`] for the renderer to label as such.
    pub pessimal: BTreeMap<String, OpOutcome>,
    /// fair 0.2 mean fetches / 1.0 native mean fetches.
    pub fair_multiplier: Option<f64>,
    /// pessimal 0.2 mean fetches / 1.0 native mean fetches (the whitepaper's
    /// headline shape).
    pub pessimal_multiplier: Option<f64>,
    /// The honest cost: 1.0's absolute native mean fetches.
    pub native_abs_mean: Option<f64>,
    /// The honest cost: 1.0's absolute native max fetches.
    pub native_abs_max: Option<u64>,
    /// Gaps.
    pub nulls: Vec<NullWithReason>,
}

/// Whitepaper 6.2: write amplification versus K.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteAmpCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// The batch size.
    pub k: u64,
    /// chunks written / K, per arm, best batching (Changeset vs one commit).
    pub wa_batched: BTreeMap<String, f64>,
    /// chunks written / K, per arm, one commit or apply per edit.
    pub wa_per_edit: BTreeMap<String, f64>,
    /// Gaps.
    pub nulls: Vec<NullWithReason>,
}

/// Whitepaper 6.1: build frontier and profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildProfileCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// Build profile per arm.
    pub per_arm: BTreeMap<String, ArmBuildProfile>,
    /// Gaps.
    pub nulls: Vec<NullWithReason>,
}

/// One arm's build profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArmBuildProfile {
    /// The build's memory law.
    pub frontier: FrontierClass,
    /// Node chunks written.
    pub nodes_written: u64,
    /// Node chunks embedded (1.0 arms only).
    pub nodes_embedded: Option<u64>,
    /// From CountingStore::peak_live_bytes; never process RSS.
    pub peak_live_store_bytes: u64,
    /// Total bytes over every put.
    pub total_put_bytes: u64,
}

/// Wall-time build cell; lives only in the non-deterministic section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildWallCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// The arm.
    pub arm: String,
    /// Timed passes.
    pub samples: u32,
    /// Mean nanoseconds over the timed passes.
    pub mean_ns: u64,
    /// Fastest timed pass, nanoseconds.
    pub min_ns: u64,
    /// n * 1e9 / mean_ns.
    pub keys_per_sec: f64,
    /// The cold-pass caveat, verbatim on every cell.
    pub caveat: String,
}

// ---- v4 cells (reused, now round-trippable) ------------------------------

/// One `(corpus, scale, op, window)` parallel-cursor cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelCursorCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// `range` (fractional window sweep) or `prefix` (a natural subtree).
    pub op: String,
    /// Range width as a fraction of the key domain; `null` for a prefix op.
    pub window: Option<f64>,
    /// Keys the scan returned.
    pub keys_returned: u64,
    /// Node fetches to drain the scan; identical for serial and concurrent.
    pub fetch_count: u64,
    /// Sequential fetch rounds the bounded-concurrency cursor actually takes,
    /// read off a paused virtual clock (READ_AHEAD cap), never guessed.
    pub rounds: u64,
    /// The read-ahead window.
    pub read_ahead: u32,
    /// Per-RTT serial and bounded-concurrent latency and their speedup.
    pub by_rtt_ms: BTreeMap<String, CursorLatency>,
    /// The latency model string.
    pub model: String,
}

/// Serial and concurrent wall-clock at one RTT, with the speedup between them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorLatency {
    /// `fetch_count * rtt`: one round trip per node.
    pub serial_ms: f64,
    /// `rounds * rtt`: the measured bounded-concurrency round count times RTT.
    pub concurrent_ms: f64,
    /// `serial_ms / concurrent_ms` (== `fetch_count / rounds`).
    pub speedup: Option<f64>,
}

/// One `(corpus, scale)` V1Read-vs-V1 cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadProfileCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// The V1 side.
    pub v1: ReadProfileSide,
    /// The V1Read side.
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadProfileSide {
    /// The format version byte.
    pub version_byte: u8,
    /// The format's inline-max budget.
    pub inline_max: u32,
    /// Mean tree get-depth over the sample.
    pub tree_depth_mean: f64,
    /// Maximum tree get-depth over the sample.
    pub tree_depth_max: u64,
    /// Fetches to drain each range window, keyed by window fraction.
    pub range_fetch_by_window: BTreeMap<String, u64>,
    /// Mean chunks rewritten by a single-key update over the sample.
    pub single_update_chunks_mean: f64,
}

/// One `(corpus, scale, offset)` pagination cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginateCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// The page offset.
    pub offset: u64,
    /// The page limit.
    pub limit: u32,
    /// Keys the page returned.
    pub keys_returned: u64,
    /// Rank-directed `paginate` node fetches: O(depth), ~constant.
    pub paginate_fetch_count: u64,
    /// Baseline `iter().skip(offset).take(limit)` node fetches: grows with
    /// offset.
    pub skip_baseline_fetch_count: u64,
    /// `skip_baseline / paginate`; grows with offset as the win widens.
    pub skip_over_paginate: Option<f64>,
    /// 0.2 resume-token page walk to the same offset: cursor.after(token)
    /// pages of `limit` repeated offset/limit times; O(offset) fetches.
    pub v02_resume_fetch_count: Option<u64>,
    /// How the 0.2 side served the page, so the rendered column carries its
    /// emulation label instead of reading as a native figure.
    #[serde(default)]
    pub v02_resume_capability: Option<Capability>,
    /// Reason when absent (scale above the 0.2 cap).
    pub v02_resume_null_reason: Option<String>,
}

/// One `(corpus, scale)` subtree-serve cell: a folder listing handed off as a
/// single subtree reference versus walked in full from the database root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtreeServeCell {
    /// The corpus.
    pub corpus: String,
    /// The scale.
    pub scale: u64,
    /// The listing prefix (lossy UTF-8).
    pub prefix: String,
    /// Keys the prefix selects.
    pub keys_returned: u64,
    /// Whether one chunk holds exactly the prefix's keys, so its reference can
    /// be handed off.
    pub handoff_found: bool,
    /// Node fetches to resolve the handoff reference: O(depth) to the
    /// boundary, nothing below it.
    pub handoff_fetch_count: u64,
    /// Node fetches to drain the full prefix cursor from the root.
    pub cursor_walk_fetch_count: u64,
    /// `cursor_walk / handoff`; `null` without a handoff.
    pub walk_over_handoff: Option<f64>,
}

/// RFC 3339 UTC seconds for `epoch_secs`, or the current wall clock when
/// `None`; the bin passes `SOURCE_DATE_EPOCH` here.
#[must_use]
pub fn generated_iso(epoch_secs: Option<u64>) -> String {
    let secs = epoch_secs.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    });
    iso_utc(secs)
}

/// Proleptic-Gregorian UTC render of a Unix timestamp, seconds precision.
fn iso_utc(secs: u64) -> String {
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(mo <= 2);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::{generated_iso, iso_utc};

    #[test]
    fn iso_render_is_correct_at_known_instants() {
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso_utc(951_868_800), "2000-03-01T00:00:00Z");
        assert_eq!(iso_utc(1_767_225_600), "2026-01-01T00:00:00Z");
        assert_eq!(generated_iso(Some(86_399)), "1970-01-01T23:59:59Z");
    }
}
