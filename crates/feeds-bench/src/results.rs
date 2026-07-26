//! Serializable schema for the finder lookup-cost run.
//!
//! Every numeric field is a work count measured by driving the real getter;
//! a `null` is only ever a capability gap left with a reason, never
//! back-filled by estimate.

use serde::Serialize;

use crate::measure::Cell;

/// The whole result document.
#[derive(Debug, Serialize)]
pub struct Document {
    pub meta: Meta,
    /// Exponential-ladder-then-binary finder cells (`latest`).
    pub latest: Vec<FinderCell>,
    /// Stepwise finder cells (`latest_linear_from`).
    pub linear: Vec<FinderCell>,
    /// Ported reference-client finder cells, one series: `width` reports its
    /// fixed lookahead concurrency.
    pub reference: Vec<FinderCell>,
    /// Comparison against the reference client's concurrent finder.
    pub reference_comparison: Vec<String>,
}

/// Run-level metadata.
#[derive(Debug, Serialize)]
pub struct Meta {
    /// Run timestamp; `SOURCE_DATE_EPOCH` pins it so two runs are
    /// byte-identical.
    pub generated: String,
    pub git_branch: String,
    pub git_commit: String,
    /// The single version authority for the harness and its schema.
    pub harness_version: String,
    pub topic_label: String,
    pub owner: String,
    pub widths: Vec<usize>,
    pub lengths: Vec<u64>,
    pub linear_budget: u64,
    pub caveats: Vec<String>,
}

/// One `(n, width)` cell; all-`null` counts carry a `gap` reason.
#[derive(Debug, Serialize)]
pub struct FinderCell {
    pub n: u64,
    pub width: usize,
    /// Concurrent probe batches until the boundary committed: one network
    /// round trip each.
    pub rounds: Option<u64>,
    /// Presence probes issued, speculation included.
    pub total_probes: Option<u64>,
    /// Probes answered absent (at or past the first free slot).
    pub wasted_probes: Option<u64>,
    /// Certified retrievals of the committed update.
    pub verified_gets: Option<u64>,
    pub committed: Option<u64>,
    pub next: Option<u64>,
    /// Reason a cell is unmeasured; `null` on measured cells.
    pub gap: Option<String>,
}

impl FinderCell {
    /// A measured cell.
    #[must_use]
    pub const fn measured(cell: Cell) -> Self {
        Self {
            n: cell.n,
            width: cell.width,
            rounds: Some(cell.rounds),
            total_probes: Some(cell.total_probes),
            wasted_probes: Some(cell.wasted_probes),
            verified_gets: Some(cell.verified_gets),
            committed: cell.committed,
            next: cell.next,
            gap: None,
        }
    }

    /// An unmeasured cell carrying its reason.
    #[must_use]
    pub const fn gap(n: u64, width: usize, reason: String) -> Self {
        Self {
            n,
            width,
            rounds: None,
            total_probes: None,
            wasted_probes: None,
            verified_gets: None,
            committed: None,
            next: None,
            gap: Some(reason),
        }
    }
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
