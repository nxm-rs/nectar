//! Sink oracles: out-of-order assembly, idempotent overwrite, growth edges.

use std::vec::Vec;

#[cfg(feature = "std")]
use super::FsSink;
use super::{DataSink, MemSink, MemSinkError};

#[test]
fn mem_sink_assembles_out_of_order_writes() {
    let mut sink = MemSink::new();
    sink.write_at(6, b"world").unwrap();
    sink.write_at(0, b"hello ").unwrap();
    assert_eq!(sink.as_ref(), b"hello world");
    assert_eq!(sink.len(), 11);
    assert!(!sink.is_empty());
    assert_eq!(Vec::from(sink), b"hello world");
}

#[test]
fn mem_sink_overwrites_are_idempotent() {
    let mut sink = MemSink::new();
    sink.write_at(0, b"abcdef").unwrap();
    let before = sink.clone();
    // Rewriting the same bytes at the same offsets changes nothing.
    sink.write_at(2, b"cd").unwrap();
    sink.write_at(0, b"abcdef").unwrap();
    assert_eq!(sink, before);
    // A genuine overwrite replaces exactly the covered region.
    sink.write_at(2, b"XY").unwrap();
    assert_eq!(sink.as_ref(), b"abXYef");
}

#[test]
fn mem_sink_zero_fills_gaps() {
    let mut sink = MemSink::new();
    sink.write_at(4, b"z").unwrap();
    assert_eq!(sink.as_ref(), b"\0\0\0\0z");
    // An empty write past the end still marks the extent.
    sink.write_at(8, b"").unwrap();
    assert_eq!(sink.len(), 8);
}

#[test]
fn mem_sink_rejects_end_overflow() {
    let mut sink = MemSink::new();
    let err = sink.write_at(u64::MAX, b"x").unwrap_err();
    assert_eq!(
        err,
        MemSinkError::EndOverflow {
            offset: u64::MAX,
            len: 1,
        }
    );
    assert!(sink.is_empty(), "a rejected write must leave no trace");
}

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
