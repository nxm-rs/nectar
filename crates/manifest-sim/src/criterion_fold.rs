//! Read criterion's `estimates.json` and fold mean/stddev ns-per-op into the
//! result cells. Criterion writes
//! `target/criterion/<group>/<id>/new/estimates.json` where the flat id is
//! `<format>__<corpus>__<scale>`; a missing file means that timed cell was not
//! benched and stays null.

use std::path::Path;

/// The mean point estimate (ns) and std-dev point estimate (ns) for a timed
/// benchmark, if criterion produced one.
#[derive(Clone, Copy, Debug)]
pub struct Estimate {
    pub mean_ns: f64,
    pub stddev_ns: f64,
}

/// The flat benchmark id shared by the bench target and the fold reader.
#[must_use]
pub fn bench_id(format: &str, corpus: &str, scale: u64) -> String {
    format!("{format}__{corpus}__{scale}")
}

/// Load an estimate for a `(group, format, corpus, scale)` benchmark id.
#[must_use]
pub fn load(base: &Path, group: &str, format: &str, corpus: &str, scale: u64) -> Option<Estimate> {
    let path = base
        .join("criterion")
        .join(group)
        .join(bench_id(format, corpus, scale))
        .join("new")
        .join("estimates.json");
    let text = std::fs::read_to_string(&path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let mean_ns = value.get("mean")?.get("point_estimate")?.as_f64()?;
    let stddev_ns = value
        .get("std_dev")
        .and_then(|v| v.get("point_estimate"))
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0);
    Some(Estimate { mean_ns, stddev_ns })
}
