//! Deterministic key corpora: one hierarchical site tree with heavy prefix
//! sharing, one uniform hex control with none.
//!
//! Keys are generated from a splitmix64 stream over the master seed, then
//! deduplicated and sorted, so a corpus is fully determined by its name and
//! its size.

use std::collections::BTreeSet;

use nectar_manifest::ManifestPath;
use nectar_primitives::ChunkRef;
use nectar_primitives::chunk::ChunkAddress;

/// Master seed of every corpus stream.
pub const MASTER_SEED: u64 = 0x6d61_6e69_6665_7374;

/// The two corpora.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Corpus {
    /// `sec/sub/page.html` paths: deep prefix sharing.
    Site,
    /// 32 hex characters per key: no prefix sharing.
    Uniform,
}

impl Corpus {
    /// The JSON label.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Site => "site",
            Self::Uniform => "uniform",
        }
    }

    /// Both corpora.
    #[must_use]
    pub const fn all() -> [Self; 2] {
        [Self::Site, Self::Uniform]
    }
}

const fn mix(seed: u64) -> u64 {
    let z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

const fn stream(corpus: Corpus, index: u64) -> u64 {
    let tag = match corpus {
        Corpus::Site => 1,
        Corpus::Uniform => 2,
    };
    mix(MASTER_SEED ^ mix(tag) ^ mix(index))
}

/// Exactly `n` distinct keys of `corpus`, ascending.
#[must_use]
pub fn generate(corpus: Corpus, n: usize) -> Vec<ManifestPath> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut index = 0u64;
    while seen.len() < n {
        let h = stream(corpus, index);
        let key = match corpus {
            Corpus::Site => format!(
                "sec{:03}/sub{:02}/page{:05}.html",
                h % 64,
                (h >> 8) % 32,
                (h >> 16) % 100_000
            ),
            Corpus::Uniform => format!("{:016x}{:016x}", h, mix(h)),
        };
        seen.insert(key);
        index += 1;
    }
    seen.into_iter()
        .map(|key| ManifestPath::from(key.as_str()))
        .collect()
}

/// The reference a key binds: no chunk stands behind it, so no load reads it.
#[must_use]
pub fn reference(key: &ManifestPath, salt: u64) -> ChunkRef {
    let mut bytes = [0u8; 32];
    let mut acc = mix(salt);
    for (index, byte) in key.as_bytes().iter().enumerate() {
        acc = mix(acc ^ (u64::from(*byte) << (index % 8)));
    }
    for (slot, byte) in bytes.chunks_exact_mut(8).enumerate() {
        byte.copy_from_slice(&mix(acc ^ slot as u64).to_be_bytes());
    }
    ChunkRef::new(ChunkAddress::new(bytes))
}

#[cfg(test)]
mod tests {
    use super::{Corpus, generate};

    /// Every corpus is seed-reproducible and yields exactly `n` sorted,
    /// distinct keys: the anti-fabrication guarantee the results rest on.
    #[test]
    fn corpora_are_reproducible_sorted_and_distinct() {
        for corpus in Corpus::all() {
            let first = generate(corpus, 300);
            let second = generate(corpus, 300);
            assert_eq!(first.len(), 300, "{}", corpus.name());
            assert_eq!(first, second, "{}", corpus.name());
            assert!(
                first.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes()),
                "{}: sorted and distinct",
                corpus.name()
            );
        }
    }
}
