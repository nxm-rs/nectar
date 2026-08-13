//! Counting presence store: probes answered from the corpus table for a
//! present-then-absent feed of `n` updates, the boundary update resident for
//! the one certified retrieval.

use std::error::Error;
use std::time::Duration;

use nectar_feeds::{Publisher, Sequence};
use nectar_primitives::chunk::{Chunk, ChunkAddress, SingleOwnerOnlyChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, MemoryStore};
use nectar_testing::bench::{Counters, Counts};

use crate::corpus::Corpus;

/// One virtual millisecond per presence probe: under a paused clock the
/// probes of one concurrent round share a deadline, so elapsed virtual time
/// reads back the round count.
pub const ROUND_TICK: Duration = Duration::from_millis(1);

/// Presence-probe store over a corpus prefix of `n` present slots.
#[derive(Debug)]
pub struct ProbeStore<'a> {
    corpus: &'a Corpus,
    n: u64,
    inner: MemoryStore<SingleOwnerOnlyChunkSet>,
    counters: Counters,
}

impl<'a> ProbeStore<'a> {
    /// Store for a feed of `n` present-then-absent updates, `1 <= n <=
    /// corpus.len()`, with the boundary update `n - 1` signed and resident so
    /// the committing get is a real certification.
    pub async fn new(corpus: &'a Corpus, n: u64) -> Result<ProbeStore<'a>, Box<dyn Error>> {
        if n == 0 || n > corpus.len() {
            return Err(format!("feed length {n} outside corpus 1..={}", corpus.len()).into());
        }
        let inner = MemoryStore::new();
        let publisher = Publisher::new(corpus.feed(), &inner, corpus.signer());
        let boundary = n - 1;
        publisher
            .publish_at(Sequence::new(boundary), boundary.to_be_bytes().to_vec())
            .await?;
        Ok(ProbeStore {
            corpus,
            n,
            inner,
            counters: Counters::new(),
        })
    }

    /// Read every counter.
    #[must_use]
    pub fn counts(&self) -> Counts {
        self.counters.snapshot(self.inner.len() as u64)
    }
}

impl ChunkHas for ProbeStore<'_> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        self.counters.record_has();
        tokio::time::sleep(ROUND_TICK).await;
        // Off-table addresses are slots past the corpus ceiling: absent.
        let present = self.corpus.slot(address).is_some_and(|slot| slot < self.n);
        if !present {
            self.counters.record_absent();
        }
        present
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
        // Only the boundary update is resident: a get anywhere else fails the
        // measurement loudly instead of skewing it.
        ChunkGet::get(&self.inner, address).await
    }
}
