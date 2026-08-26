//! Counting probe store: probes answered from the corpus table for a
//! present-then-absent feed of `n` updates; a not-yet-resident present slot
//! is served as a signed update over the tick, so a probe get is a retrieval
//! like the reference client's.

use core::error::Error;
use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;
use nectar_feeds::{Feed, Sequence};
use nectar_primitives::chunk::{
    Chunk, ChunkAddress, SingleOwnerChunk, SingleOwnerOnlyChunkSet, Verified,
};
use nectar_primitives::store::{ChunkGet, ChunkPut, ChunkStoreError, MemoryStore};
use nectar_testing::bench::{Counters, Counts};

use crate::corpus::Corpus;

/// One virtual millisecond per probe get: under a paused clock the gets of
/// one concurrent round share a deadline, so elapsed virtual time reads back
/// the round count.
pub const ROUND_TICK: Duration = Duration::from_millis(1);

/// Probe store over a corpus prefix of `n` present slots.
#[derive(Debug)]
pub struct ProbeStore<'a> {
    corpus: &'a Corpus,
    n: u64,
    feed: Feed,
    signer: PrivateKeySigner,
    inner: MemoryStore<SingleOwnerOnlyChunkSet>,
    counters: Counters,
}

impl<'a> ProbeStore<'a> {
    /// Store for a feed of `n` present-then-absent updates, `1 <= n <=
    /// corpus.len()`. A present slot not yet resident is served as a signed
    /// and verified update over the tick; the committing get is a real
    /// certification.
    pub fn new(corpus: &'a Corpus, n: u64) -> Result<Self, Box<dyn Error>> {
        if n == 0 || n > corpus.len() {
            return Err(format!("feed length {n} outside corpus 1..={}", corpus.len()).into());
        }
        Ok(ProbeStore {
            corpus,
            n,
            feed: corpus.feed(),
            signer: corpus.signer().clone(),
            inner: MemoryStore::new(),
            counters: Counters::new(),
        })
    }

    /// Read every counter.
    #[must_use]
    pub fn counts(&self) -> Counts {
        self.counters.snapshot(self.inner.len() as u64)
    }
}

impl ChunkGet<SingleOwnerOnlyChunkSet> for ProbeStore<'_> {
    type Trust = Verified;
    type Error = <MemoryStore<SingleOwnerOnlyChunkSet> as ChunkGet<SingleOwnerOnlyChunkSet>>::Error;

    async fn get(
        &self,
        address: &ChunkAddress,
    ) -> Result<Chunk<Verified, SingleOwnerOnlyChunkSet>, Self::Error> {
        self.counters.record_get();
        tokio::time::sleep(ROUND_TICK).await;
        let Some(slot) = self.corpus.slot(address).filter(|slot| *slot < self.n) else {
            // Off-table addresses are slots past the corpus ceiling: absent.
            self.counters.record_absent();
            return Err(ChunkStoreError::not_found(address));
        };
        if let Ok(chunk) = ChunkGet::get(&self.inner, address).await {
            return Ok(chunk);
        }
        let seq = Sequence::new(slot);
        let sealed = SingleOwnerChunk::seal::<SingleOwnerOnlyChunkSet>(
            self.feed.update_id(&seq),
            slot.to_be_bytes().to_vec(),
            &self.signer,
        )
        .map_err(|error| ChunkStoreError::Other(Box::new(error)))?;
        // The local resident put cannot fail; a sealed chunk is a chunk.
        ChunkPut::put(&self.inner, sealed)
            .await
            .map_err(|error| match error {})?;
        ChunkGet::get(&self.inner, address).await
    }
}
