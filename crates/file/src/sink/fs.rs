//! Filesystem sink over a seekable file handle.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use super::DataSink;

/// Positional sink over an open file: each write seeks then writes, so the
/// handle needs no platform positional-write support.
#[derive(Debug)]
pub struct FsSink {
    file: File,
}

impl FsSink {
    /// Create the file at `path`, truncating any existing content.
    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        File::create(path.as_ref()).map(Self::from)
    }

    /// Open (or create) the file at `path` without truncating, keeping any
    /// partially downloaded bytes for an idempotent re-run.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())
            .map(Self::from)
    }
}

impl From<File> for FsSink {
    fn from(file: File) -> Self {
        Self { file }
    }
}

impl From<FsSink> for File {
    fn from(sink: FsSink) -> Self {
        sink.file
    }
}

impl DataSink for FsSink {
    type Error = std::io::Error;

    fn write_at(&mut self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)
    }
}
