//! File splitter for producing BMT chunks from data streams.

use std::fmt;
use std::io::{self, Write};

use bytes::Bytes;

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{Chunk, ChunkAddress, ContentChunk};

use super::constants::{LEVEL_LIMIT, REF_SIZE, SPANS, SPAN_SIZE};
use super::error::{FileError, Result};
use super::sink::ChunkSink;

/// Splits data into BMT chunks, producing intermediate chunks for large files.
///
/// The splitter uses a multi-level buffer to build the chunk tree:
/// - Level 0: Raw file data (up to 4096 bytes per chunk)
/// - Level 1+: Hash references (128 x 32-byte refs per chunk)
///
/// Chunks are emitted to the sink as buffers fill. Call `finish()` to
/// finalize the tree and get the root address.
pub struct Splitter<S, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    S: ChunkSink<BODY_SIZE>,
{
    sink: S,
    span_length: u64,
    length: u64,
    sum_counts: [usize; LEVEL_LIMIT],
    cursors: [usize; LEVEL_LIMIT],
    buffer: Vec<u8>,
}

impl<S, const BODY_SIZE: usize> fmt::Debug for Splitter<S, BODY_SIZE>
where
    S: ChunkSink<BODY_SIZE>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Splitter")
            .field("span_length", &self.span_length)
            .field("length", &self.length)
            .field("sum_counts", &self.sum_counts)
            .field("cursors", &self.cursors)
            .finish_non_exhaustive()
    }
}

impl<S, const BODY_SIZE: usize> Splitter<S, BODY_SIZE>
where
    S: ChunkSink<BODY_SIZE>,
{
    /// Create a splitter for data of known size.
    pub fn new(sink: S, span_length: u64) -> Self {
        // Buffer size: each level can hold one full chunk worth of data
        // We allocate 2x to handle edge cases during finalization
        let buffer_size = (BODY_SIZE + SPAN_SIZE) * LEVEL_LIMIT * 2;

        Self {
            sink,
            span_length,
            length: 0,
            sum_counts: [0; LEVEL_LIMIT],
            cursors: [0; LEVEL_LIMIT],
            buffer: vec![0u8; buffer_size],
        }
    }

    /// Get the number of bytes written so far.
    pub fn len(&self) -> u64 {
        self.length
    }

    /// Check if any data has been written.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Get the declared span length.
    pub fn span_length(&self) -> u64 {
        self.span_length
    }

    /// Write data to a specific level's buffer.
    fn write_to_level(&mut self, level: usize, data: &[u8]) -> Result<()> {
        let start = self.cursors[level];
        let end = start + data.len();

        self.buffer[start..end].copy_from_slice(data);
        self.cursors[level] = end;

        // Check if this level's buffer is full (reached chunk boundary)
        let level_start = self.cursors[level + 1];
        if self.cursors[level] - level_start == BODY_SIZE {
            let reference = self.sum_level(level)?;
            self.write_to_level(level + 1, &reference)?;
            // Reset this level's cursor to next level's position
            self.cursors[level] = self.cursors[level + 1];
        }

        Ok(())
    }

    /// Create a chunk from the current level's buffer and return its reference.
    fn sum_level(&mut self, level: usize) -> Result<[u8; REF_SIZE]> {
        self.sum_counts[level] += 1;

        // Calculate span for this chunk
        let span_size = SPANS[level] * BODY_SIZE as u64;
        let span = (self.length - 1) % span_size + 1;

        // Extract chunk data from buffer
        let level_start = self.cursors[level + 1];
        let level_end = self.cursors[level];
        let chunk_data = &self.buffer[level_start..level_end];

        // Build chunk with span header
        let mut chunk_bytes = Vec::with_capacity(SPAN_SIZE + chunk_data.len());
        chunk_bytes.extend_from_slice(&span.to_le_bytes());
        chunk_bytes.extend_from_slice(chunk_data);

        let chunk = ContentChunk::<BODY_SIZE>::try_from(Bytes::from(chunk_bytes))
            .map_err(|e| match e {
                crate::error::PrimitivesError::Chunk(c) => FileError::Chunk(c),
                other => FileError::Sink(Box::new(other)),
            })?;
        let address = *chunk.address();

        self.sink.put(chunk).map_err(FileError::sink)?;

        Ok(address.into())
    }

    /// Hash any remaining data at level 0 that doesn't fill a complete chunk.
    fn hash_unfinished(&mut self) -> Result<()> {
        if self.length % BODY_SIZE as u64 != 0 {
            let reference = self.sum_level(0)?;
            let next_cursor = self.cursors[1];
            self.buffer[next_cursor..next_cursor + REF_SIZE].copy_from_slice(&reference);
            self.cursors[1] += REF_SIZE;
            self.cursors[0] = self.cursors[1];
        }
        Ok(())
    }

    /// Handle unbalanced tree: carry single refs up instead of wrapping.
    fn move_dangling_chunk(&mut self) -> Result<()> {
        let target_level = super::constants::levels(self.length, BODY_SIZE);

        for i in 1..target_level {
            // Check if there's a single dangling reference at this level
            if self.sum_counts[i] > 0 {
                let prev_spans = SPANS[target_level - 1 - i] as i64;
                if (self.sum_counts[i - 1] as i64) - prev_spans <= 1 {
                    // Carry the reference to the next level without wrapping
                    self.cursors[i + 1] = self.cursors[i];
                    self.cursors[i] = self.cursors[i - 1];
                    continue;
                }
            }

            // Hash this level and write reference to next level
            let reference = self.sum_level(i)?;
            let next_cursor = self.cursors[i + 1];
            self.buffer[next_cursor..next_cursor + REF_SIZE].copy_from_slice(&reference);
            self.cursors[i + 1] += REF_SIZE;
            self.cursors[i] = self.cursors[i + 1];
        }

        Ok(())
    }

    /// Finalize the splitter and return the root address and sink.
    pub fn finish(mut self) -> Result<(ChunkAddress, S)> {
        if self.length != self.span_length {
            return Err(FileError::SpanMismatch {
                expected: self.span_length,
                actual: self.length,
            });
        }

        // Handle empty file case
        if self.length == 0 {
            let chunk = ContentChunk::<BODY_SIZE>::new(Bytes::new()).map_err(|e| match e {
                crate::error::PrimitivesError::Chunk(c) => FileError::Chunk(c),
                other => FileError::Sink(Box::new(other)),
            })?;
            let address = *chunk.address();
            self.sink.put(chunk).map_err(FileError::sink)?;
            return Ok((address, self.sink));
        }

        self.hash_unfinished()?;
        self.move_dangling_chunk()?;

        // Root hash is in the first REF_SIZE bytes of the buffer
        let root_bytes: [u8; 32] = self.buffer[..REF_SIZE].try_into().unwrap();
        let root = ChunkAddress::from(root_bytes);

        Ok((root, self.sink))
    }

    /// Write a single chunk's worth of data (internal helper).
    fn write_chunk(&mut self, data: &[u8]) -> Result<()> {
        debug_assert!(data.len() <= BODY_SIZE);

        self.length += data.len() as u64;
        if self.length > self.span_length {
            return Err(FileError::WritePastSpan {
                span: self.span_length,
                written: self.length,
            });
        }

        self.write_to_level(0, data)
    }
}

impl<S, const BODY_SIZE: usize> Write for Splitter<S, BODY_SIZE>
where
    S: ChunkSink<BODY_SIZE>,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Write in chunk-sized pieces
        let mut written = 0;
        while written < buf.len() {
            let remaining = buf.len() - written;
            let chunk_space = BODY_SIZE - (self.cursors[0] - self.cursors[1]);
            let to_write = remaining.min(chunk_space);

            if to_write == 0 {
                break;
            }

            let data = &buf[written..written + to_write];
            self.write_chunk(data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            written += to_write;
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::sink::VecSink;

    const REFS_PER_CHUNK: usize = DEFAULT_BODY_SIZE / REF_SIZE; // 128

    #[test]
    fn test_splitter_empty() {
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let splitter = Splitter::new(sink, 0);

        let (root, sink) = splitter.finish().unwrap();
        assert_eq!(sink.len(), 1); // One empty chunk
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_small() {
        let data = b"hello world";
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, data.len() as u64);

        splitter.write_all(data).unwrap();
        let (root, sink) = splitter.finish().unwrap();

        assert_eq!(sink.len(), 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_exact_chunk() {
        let data = vec![0xAB; DEFAULT_BODY_SIZE];
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, sink) = splitter.finish().unwrap();

        assert_eq!(sink.len(), 1);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_two_chunks() {
        let data = vec![0xCD; DEFAULT_BODY_SIZE + 1];
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, sink) = splitter.finish().unwrap();

        // 2 data chunks + 1 intermediate
        assert_eq!(sink.len(), 3);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_128_chunks_exact() {
        // 128 data chunks = exactly fills one level-1 intermediate
        let data = vec![0xEF; DEFAULT_BODY_SIZE * REFS_PER_CHUNK];
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, sink) = splitter.finish().unwrap();

        // 128 data chunks + 1 intermediate at level 1 + 1 root at level 2
        assert_eq!(sink.len(), REFS_PER_CHUNK + 2);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_129_chunks() {
        // 129 data chunks = overflows into level 2
        let data = vec![0x12; DEFAULT_BODY_SIZE * (REFS_PER_CHUNK + 1)];
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, data.len() as u64);

        splitter.write_all(&data).unwrap();
        let (root, sink) = splitter.finish().unwrap();

        // 129 data chunks + 2 intermediate (one full at level 1, one partial at level 2)
        assert_eq!(sink.len(), REFS_PER_CHUNK + 1 + 2);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_incremental_writes() {
        let data = vec![0x34; DEFAULT_BODY_SIZE * 2 + 100];
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, data.len() as u64);

        // Write in small chunks
        for chunk in data.chunks(100) {
            splitter.write_all(chunk).unwrap();
        }
        let (root, sink) = splitter.finish().unwrap();

        // 3 data chunks + 1 intermediate
        assert_eq!(sink.len(), 4);
        assert!(!root.is_zero());
    }

    #[test]
    fn test_splitter_deterministic() {
        let data = vec![0x56; DEFAULT_BODY_SIZE * 3];

        // Split twice with same data
        let (root1, _) = {
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = Splitter::new(sink, data.len() as u64);
            splitter.write_all(&data).unwrap();
            splitter.finish().unwrap()
        };

        let (root2, _) = {
            let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
            let mut splitter = Splitter::new(sink, data.len() as u64);
            splitter.write_all(&data).unwrap();
            splitter.finish().unwrap()
        };

        assert_eq!(root1, root2);
    }

    #[test]
    fn test_splitter_write_past_span() {
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, 10);

        let result = splitter.write_all(b"this is more than 10 bytes");
        assert!(result.is_err());
    }

    #[test]
    fn test_splitter_span_mismatch() {
        let sink = VecSink::<DEFAULT_BODY_SIZE>::new();
        let mut splitter = Splitter::new(sink, 100);

        splitter.write_all(b"short").unwrap();
        let result = splitter.finish();

        assert!(matches!(result, Err(FileError::SpanMismatch { .. })));
    }
}
