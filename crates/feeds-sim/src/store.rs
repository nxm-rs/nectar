//! Counting presence store: probes answered from the corpus table for a
//! present-then-absent feed of `n` updates, the boundary update resident for
//! the one certified retrieval.

use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::Duration;

use nectar_feeds::{Sequence, Updater};
use nectar_primitives::chunk::{Chunk, ChunkAddress, SingleOwnerOnlyChunkSet, Verified};
use nectar_primitives::store::{ChunkGet, ChunkHas, MemoryStore};

use crate::corpus::Corpus;

/// One virtual millisecond per presence probe: under a paused clock the
/// probes of one concurrent round share a deadline, so elapsed virtual time
/// reads back the round count.
pub const ROUND_TICK: Duration = Duration::from_millis(1);

/// Probe and retrieval counters at one point in time.
#[derive(Debug, Clone, Copy, Default)]
pub struct Counts {
    /// Presence probes issued, speculation included.
    pub probes: u64,
    /// Probes answered absent: every slot at or past the first free index.
    pub absent: u64,
    /// Certified retrievals served.
    pub gets: u64,
}

/// Presence-probe store over a corpus prefix of `n` present slots.
#[derive(Debug)]
pub struct ProbeStore<'a> {
    corpus: &'a Corpus,
    n: u64,
    inner: MemoryStore<SingleOwnerOnlyChunkSet>,
    probes: AtomicU64,
    absent: AtomicU64,
    gets: AtomicU64,
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
        let updater = Updater::new(corpus.feed(), &inner, corpus.signer());
        let boundary = n - 1;
        updater
            .put_at(Sequence::new(boundary), boundary.to_be_bytes().to_vec())
            .await?;
        Ok(ProbeStore {
            corpus,
            n,
            inner,
            probes: AtomicU64::new(0),
            absent: AtomicU64::new(0),
            gets: AtomicU64::new(0),
        })
    }

    /// Read every counter.
    #[must_use]
    pub fn counts(&self) -> Counts {
        Counts {
            probes: self.probes.load(SeqCst),
            absent: self.absent.load(SeqCst),
            gets: self.gets.load(SeqCst),
        }
    }
}

impl ChunkHas for ProbeStore<'_> {
    async fn has(&self, address: &ChunkAddress) -> bool {
        self.probes.fetch_add(1, SeqCst);
        tokio::time::sleep(ROUND_TICK).await;
        // Off-table addresses are slots past the corpus ceiling: absent.
        let present = self.corpus.slot(address).is_some_and(|slot| slot < self.n);
        if !present {
            self.absent.fetch_add(1, SeqCst);
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
        self.gets.fetch_add(1, SeqCst);
        // Only the boundary update is resident: a get anywhere else fails the
        // measurement loudly instead of skewing it.
        ChunkGet::get(&self.inner, address).await
    }
}
