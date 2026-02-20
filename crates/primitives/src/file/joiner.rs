//! File joiner for reconstructing data from BMT chunks.

use std::fmt;
use std::io::{self, Read, Seek, SeekFrom};

use crate::bmt::DEFAULT_BODY_SIZE;
use crate::chunk::{BmtChunk, Chunk, ChunkAddress};

use super::constants::{LEVEL_LIMIT, REF_SIZE, SPANS};
use super::error::{FileError, Result};
use crate::store::ChunkGet;

/// Joins chunks back into file data.
pub struct Joiner<G, const BODY_SIZE: usize = DEFAULT_BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
{
    getter: G,
    root: ChunkAddress,
    span: u64,
    position: u64,
}

impl<G, const BODY_SIZE: usize> fmt::Debug for Joiner<G, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Joiner")
            .field("root", &self.root)
            .field("span", &self.span)
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

impl<G, const BODY_SIZE: usize> Joiner<G, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
{
    /// Create a joiner from a root address.
    pub fn new(getter: G, root: ChunkAddress) -> Result<Self> {
        let root_chunk = getter.get(&root).map_err(FileError::getter)?;
        let span = root_chunk.span();

        Ok(Self {
            getter,
            root,
            span,
            position: 0,
        })
    }

    /// Total file size.
    pub const fn size(&self) -> u64 {
        self.span
    }

    /// Current read position.
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Root address.
    pub const fn root(&self) -> &ChunkAddress {
        &self.root
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if offset >= self.span {
            return Ok(0);
        }

        let to_read = buf.len().min((self.span - offset) as usize);
        if to_read == 0 {
            return Ok(0);
        }

        self.read_from_tree(&self.root, self.span, offset, &mut buf[..to_read])?;
        Ok(to_read)
    }

    fn read_from_tree(
        &self,
        address: &ChunkAddress,
        span: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<()> {
        let chunk = self.getter.get(address).map_err(FileError::getter)?;
        let chunk_data = chunk.data();

        if span <= BODY_SIZE as u64 {
            let start = offset as usize;
            let end = start + buf.len();
            buf.copy_from_slice(&chunk_data[start..end]);
            return Ok(());
        }

        let refs_per_chunk = BODY_SIZE / REF_SIZE;
        let subspan = self.subspan_size(span);

        let mut remaining = buf;
        let mut current_offset = offset;

        while !remaining.is_empty() {
            let child_index = (current_offset / subspan) as usize;
            let child_offset = current_offset % subspan;

            let ref_start = child_index * REF_SIZE;
            let ref_end = ref_start + REF_SIZE;

            if ref_end > chunk_data.len() {
                return Err(FileError::InvalidReference { level: 0 });
            }

            let child_addr_bytes: [u8; 32] = chunk_data[ref_start..ref_end]
                .try_into()
                .map_err(|_| FileError::InvalidReference { level: 0 })?;
            let child_addr = ChunkAddress::from(child_addr_bytes);

            let child_span = if child_index == refs_per_chunk - 1 {
                let preceding = child_index as u64 * subspan;
                span.saturating_sub(preceding)
            } else {
                subspan.min(span - child_index as u64 * subspan)
            };

            let available = (child_span - child_offset) as usize;
            let to_read = remaining.len().min(available);

            self.read_from_tree(
                &child_addr,
                child_span,
                child_offset,
                &mut remaining[..to_read],
            )?;

            remaining = &mut remaining[to_read..];
            current_offset += to_read as u64;
        }

        Ok(())
    }

    fn subspan_size(&self, span: u64) -> u64 {
        for i in 0..LEVEL_LIMIT {
            let level_span = SPANS[i] * BODY_SIZE as u64;
            if span <= level_span {
                return if i == 0 { 1 } else { SPANS[i - 1] * BODY_SIZE as u64 };
            }
        }
        SPANS[LEVEL_LIMIT - 2] * BODY_SIZE as u64
    }
}

impl<G, const BODY_SIZE: usize> Read for Joiner<G, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self
            .read_at(buf, self.position)
            .map_err(io::Error::other)?;
        self.position += read as u64;
        Ok(read)
    }
}

impl<G, const BODY_SIZE: usize> Seek for Joiner<G, BODY_SIZE>
where
    G: ChunkGet<BODY_SIZE>,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.span as i64 + offset,
            SeekFrom::Current(offset) => self.position as i64 + offset,
        };

        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }

        self.position = new_pos as u64;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::Splitter;
    use crate::store::MemorySink;
    use std::io::Write;

    const REFS_PER_CHUNK: usize = DEFAULT_BODY_SIZE / REF_SIZE;

    fn split_and_store(data: &[u8]) -> (ChunkAddress, MemorySink) {
        let sink = MemorySink::new();
        let mut splitter = Splitter::new(sink, data.len() as u64);
        splitter.write_all(data).unwrap();
        splitter.finish().unwrap()
    }

    #[test]
    fn test_joiner_empty() {
        let (root, sink) = split_and_store(b"");
        let mut joiner = Joiner::new(sink, root).unwrap();

        assert_eq!(joiner.size(), 0);

        let mut buf = [0u8; 10];
        let read = joiner.read(&mut buf).unwrap();
        assert_eq!(read, 0);
    }

    #[test]
    fn test_joiner_small() {
        let data = b"hello world";
        let (root, sink) = split_and_store(data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        assert_eq!(joiner.size(), data.len() as u64);

        let mut buf = vec![0u8; data.len()];
        joiner.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, data);
    }

    #[test]
    fn test_joiner_seek() {
        let data = b"hello world";
        let (root, sink) = split_and_store(data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        joiner.seek(SeekFrom::Start(6)).unwrap();

        let mut buf = vec![0u8; 5];
        joiner.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_joiner_two_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE + 100).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        assert_eq!(joiner.size(), data.len() as u64);

        let mut buf = vec![0u8; data.len()];
        joiner.read_exact(&mut buf).unwrap();
        assert_eq!(buf, data);
    }

    #[test]
    fn test_round_trip_exact_chunk() {
        let data = vec![0xAB; DEFAULT_BODY_SIZE];
        let (root, sink) = split_and_store(&data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        let mut recovered = vec![0u8; data.len()];
        joiner.read_exact(&mut recovered).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_round_trip_128_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * REFS_PER_CHUNK)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, sink) = split_and_store(&data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        let mut recovered = vec![0u8; data.len()];
        joiner.read_exact(&mut recovered).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_round_trip_129_chunks() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * (REFS_PER_CHUNK + 1))
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, sink) = split_and_store(&data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        let mut recovered = vec![0u8; data.len()];
        joiner.read_exact(&mut recovered).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_joiner_seek_operations() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 3).map(|i| (i % 256) as u8).collect();
        let (root, sink) = split_and_store(&data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        let offset = DEFAULT_BODY_SIZE + 100;
        joiner.seek(SeekFrom::Start(offset as u64)).unwrap();
        assert_eq!(joiner.position(), offset as u64);

        let mut buf = vec![0u8; 50];
        joiner.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &data[offset..offset + 50]);

        joiner.seek(SeekFrom::Current(-50)).unwrap();
        let mut buf2 = vec![0u8; 50];
        joiner.read_exact(&mut buf2).unwrap();
        assert_eq!(buf, buf2);

        joiner.seek(SeekFrom::End(-100)).unwrap();
        let mut buf3 = vec![0u8; 100];
        joiner.read_exact(&mut buf3).unwrap();
        assert_eq!(&buf3, &data[data.len() - 100..]);
    }

    #[test]
    fn test_joiner_partial_reads() {
        let data: Vec<u8> = (0..DEFAULT_BODY_SIZE * 2 + 500)
            .map(|i| (i % 256) as u8)
            .collect();
        let (root, sink) = split_and_store(&data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        let mut recovered = Vec::new();
        let mut buf = [0u8; 100];
        loop {
            let n = joiner.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            recovered.extend_from_slice(&buf[..n]);
        }
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_joiner_read_at_eof() {
        let data = b"test data";
        let (root, sink) = split_and_store(data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        let mut buf = vec![0u8; data.len()];
        joiner.read_exact(&mut buf).unwrap();

        let mut buf2 = [0u8; 10];
        let n = joiner.read(&mut buf2).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_joiner_seek_past_end() {
        let data = b"test data";
        let (root, sink) = split_and_store(data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        joiner.seek(SeekFrom::Start(1000)).unwrap();
        assert_eq!(joiner.position(), 1000);

        let mut buf = [0u8; 10];
        let n = joiner.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_joiner_seek_negative() {
        let data = b"test data";
        let (root, sink) = split_and_store(data);
        let mut joiner = Joiner::new(sink, root).unwrap();

        let result = joiner.seek(SeekFrom::Current(-100));
        assert!(result.is_err());
    }
}
