//! Filesystem sink oracles: assembly and idempotent overwrite on disk.
//! The in-memory oracles live with the vocabulary in `nectar_primitives`.

#[cfg(feature = "std")]
use super::{DataSink, FsSink};

/// Temp file path unique to this process; removed by the returned guard.
#[cfg(feature = "std")]
struct TempPath(std::path::PathBuf);

#[cfg(feature = "std")]
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(feature = "std")]
fn temp_path(name: &str) -> TempPath {
    let mut path = std::env::temp_dir();
    path.push(std::format!(
        "nectar-file-sink-{}-{name}",
        std::process::id()
    ));
    TempPath(path)
}

#[cfg(feature = "std")]
#[test]
fn fs_sink_assembles_and_overwrites_like_mem_sink() {
    let path = temp_path("assemble");
    let mut sink = FsSink::create(&path.0).unwrap();
    sink.write_at(6, b"world").unwrap();
    sink.write_at(0, b"hello ").unwrap();
    sink.write_at(0, b"hello ").unwrap();
    assert_eq!(std::fs::read(&path.0).unwrap(), b"hello world");

    // Reopening without truncation keeps the bytes for a re-run.
    let mut sink = FsSink::open(&path.0).unwrap();
    sink.write_at(6, b"world").unwrap();
    assert_eq!(std::fs::read(&path.0).unwrap(), b"hello world");

    // Create truncates.
    let sink = FsSink::create(&path.0).unwrap();
    let file: std::fs::File = sink.into();
    assert_eq!(file.metadata().unwrap().len(), 0);
}
