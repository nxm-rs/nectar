//! Serializable schema for the finder lookup-cost run.
//!
//! Every numeric field is a work count measured by driving the real reader;
//! a `null` is only ever a capability gap left with a reason, never
//! back-filled by estimate.

use nectar_testing::bench::RunMeta;
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

/// Run-level metadata. The shared header flattens in first, so the document
/// keeps the key order the checked-in results were written at.
#[derive(Debug, Serialize)]
pub struct Meta {
    #[serde(flatten)]
    pub run: RunMeta,
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
                    harness_version: "2".to_string(),
                },
                topic_label: "finder-cost".to_string(),
                owner: "0x0".to_string(),
                widths: Vec::new(),
                lengths: Vec::new(),
                linear_budget: 7,
                caveats: Vec::new(),
            },
            latest: Vec::new(),
            linear: Vec::new(),
            reference: Vec::new(),
            reference_comparison: Vec::new(),
        };
        let want = [
            "{",
            r#"  "meta": {"#,
            r#"    "generated": "1970-01-01T00:00:00Z","#,
            r#"    "git_branch": "main","#,
            r#"    "git_commit": "abc","#,
            r#"    "harness_version": "2","#,
            r#"    "topic_label": "finder-cost","#,
            r#"    "owner": "0x0","#,
        ]
        .join("\n");
        let json = serde_json::to_string_pretty(&doc).unwrap();
        assert!(json.starts_with(&want), "{json}");
    }
}
