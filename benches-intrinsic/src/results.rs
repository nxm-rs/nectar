//! Machine-readable results: one JSON lines file per suite.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

/// `<target>/bench-results/`, resolved from the running executable so the
/// configured target directory is honoured; falls back to `./target`.
#[must_use]
pub fn results_dir() -> PathBuf {
    let target = std::env::current_exe()
        .ok()
        .and_then(|exe| {
            exe.ancestors()
                .find(|dir| dir.file_name().is_some_and(|name| name == "target"))
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| PathBuf::from("target"));
    target.join("bench-results")
}

/// A suite's JSON lines sink.
#[derive(Debug)]
pub struct Suite {
    name: &'static str,
    file: fs::File,
}

impl Suite {
    /// Create (truncating) `<target>/bench-results/<name>.jsonl`.
    #[must_use]
    pub fn create(name: &'static str) -> Self {
        let dir = results_dir();
        fs::create_dir_all(&dir).unwrap();
        let file = fs::File::create(dir.join(format!("{name}.jsonl"))).unwrap();
        Self { name, file }
    }

    /// Append one result line.
    pub fn emit(&mut self, impl_name: &str, scenario: &str, metric: &str, value: f64, unit: &str) {
        writeln!(
            self.file,
            "{{\"suite\":\"{}\",\"impl\":\"{}\",\"scenario\":\"{}\",\"metric\":\"{}\",\"value\":{},\"unit\":\"{}\"}}",
            self.name, impl_name, scenario, metric, value, unit
        )
        .unwrap();
    }
}
