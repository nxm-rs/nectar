//! Serializable schema for the two-arm manifest run.
//!
//! The document has two sections and they never mix. `deterministic` holds
//! store-counter figures that are identical between two runs of one commit.
//! `wall_time` holds the cold-build clock, which varies by machine and by run
//! and is never the currency of a comparison.

use nectar_testing::bench::RunMeta;
use serde::Serialize;

use crate::arm::Capability;

/// The whole result document.
#[derive(Debug, Serialize)]
pub struct Document {
    pub meta: Meta,
    pub deterministic: Deterministic,
    /// The fenced wall-clock lane; no figure here divides one in
    /// `deterministic`.
    pub wall_time: Vec<BuildTimeCell>,
}

/// Run-level metadata. The shared header flattens in first.
#[derive(Debug, Serialize)]
pub struct Meta {
    #[serde(flatten)]
    pub run: RunMeta,
    pub seed_master: String,
    pub scales: Vec<u64>,
    pub corpora: Vec<String>,
    pub arms: Vec<String>,
    pub op_samples: u64,
    pub build_samples: u64,
    pub caveats: Vec<String>,
}

/// Everything measured by counter delta.
#[derive(Debug, Serialize)]
pub struct Deterministic {
    pub storage: Vec<StorageCell>,
    pub ops: Vec<OpCell>,
}

/// What one `(corpus, scale, arm)` build left in the store.
#[derive(Debug, Serialize)]
pub struct StorageCell {
    pub corpus: String,
    pub scale: u64,
    pub arm: String,
    /// Distinct resident chunks after the build.
    pub chunks: u64,
    /// Chunk writes the build issued, rewrites included.
    pub puts: u64,
    pub distinct_puts: u64,
    pub put_bytes: u64,
    pub live_bytes: u64,
}

/// One `(corpus, scale, arm, op)` cell.
#[derive(Debug, Serialize)]
pub struct OpCell {
    pub corpus: String,
    pub scale: u64,
    pub arm: String,
    pub op: String,
    /// How the arm served the op; an emulation names itself here.
    pub capability: Capability,
    pub samples: u64,
    /// Node fetches per call, meaned over `samples`.
    pub fetches_mean: f64,
    pub fetches_max: u64,
    /// Chunk writes per call, meaned over `samples`.
    pub puts_mean: f64,
    /// Keys the op returned, summed over `samples`.
    pub keys_returned: u64,
}

/// One `(corpus, scale, arm)` cold build, timed.
#[derive(Debug, Serialize)]
pub struct BuildTimeCell {
    pub corpus: String,
    pub scale: u64,
    pub arm: String,
    pub samples: u64,
    pub mean_ns: u64,
    pub min_ns: u64,
    pub keys_per_second: f64,
}

#[cfg(test)]
mod tests {
    use super::{Deterministic, Document, Meta};
    use nectar_testing::bench::RunMeta;

    /// The flattened header keeps the four provenance keys ahead of the
    /// harness fields, and the wall-clock lane stays a section of its own.
    #[test]
    fn the_document_opens_with_the_shared_header() {
        let doc = Document {
            meta: Meta {
                run: RunMeta {
                    generated: "1970-01-01T00:00:00Z".to_string(),
                    git_branch: "main".to_string(),
                    git_commit: "abc".to_string(),
                    harness_version: "1".to_string(),
                },
                seed_master: "0x0".to_string(),
                scales: Vec::new(),
                corpora: Vec::new(),
                arms: Vec::new(),
                op_samples: 0,
                build_samples: 0,
                caveats: Vec::new(),
            },
            deterministic: Deterministic {
                storage: Vec::new(),
                ops: Vec::new(),
            },
            wall_time: Vec::new(),
        };
        let want = [
            "{",
            r#"  "meta": {"#,
            r#"    "generated": "1970-01-01T00:00:00Z","#,
            r#"    "git_branch": "main","#,
            r#"    "git_commit": "abc","#,
            r#"    "harness_version": "1","#,
            r#"    "seed_master": "0x0","#,
        ]
        .join("\n");
        let json = serde_json::to_string_pretty(&doc).unwrap();
        assert!(json.starts_with(&want), "{json}");
        let (deterministic, wall) = json.split_once(r#""wall_time""#).unwrap();
        assert!(!deterministic.contains("_ns"), "{deterministic}");
        assert!(!wall.contains("fetches"), "{wall}");
    }
}
