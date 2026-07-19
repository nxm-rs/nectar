//! Deterministic key and payload corpora shared by every implementation.

use std::collections::BTreeMap;

use nectar_primitives::chunk::ChunkAddress;

/// One RNG seed for every implementation and every run.
pub const SEED: u64 = 0x6E65_6374_6172;

/// Comparative entry counts.
pub const SIZES: [usize; 3] = [64, 1024, 16384];

/// Random point lookups that hit.
pub const LOOKUP_HITS: usize = 1024;

/// Point lookups that miss.
pub const LOOKUP_MISSES: usize = 256;

/// Fresh keys inserted by the incremental edit.
pub const EDIT_INSERTS: usize = 32;

/// Existing keys removed by the incremental edit.
pub const EDIT_REMOVES: usize = 16;

/// SplitMix64: tiny, seedable, identical stream on every side.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    /// A generator over `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next word of the stream.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Fill `buf` from the stream.
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }

    /// A uniform index below `bound`.
    pub fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next_u64() % u64::try_from(bound.max(1)).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// Key shape of a corpus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// Random 32-hex names, no separators.
    Flat,
    /// Eight-segment slash paths.
    Deep,
    /// Long front-loaded common prefixes.
    Shared,
}

impl Shape {
    /// Every shape, in reporting order.
    pub const ALL: [Self; 3] = [Self::Flat, Self::Deep, Self::Shared];

    /// Stable scenario label.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Deep => "deep",
            Self::Shared => "shared",
        }
    }
}

const SHARED_PREFIXES: [&str; 8] = [
    "static/app/css/",
    "static/app/js/",
    "static/app/img/",
    "static/app/fonts/",
    "static/lib/js/",
    "static/lib/css/",
    "media/images/thumbs/",
    "media/video/clips/",
];

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_str(rng: &mut SplitMix64, chars: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(chars);
    for _ in 0..chars {
        out.push(HEX[rng.below(16)]);
    }
    out
}

fn gen_key(rng: &mut SplitMix64, shape: Shape) -> Vec<u8> {
    match shape {
        Shape::Flat => hex_str(rng, 32),
        Shape::Deep => {
            let mut out = Vec::with_capacity(39);
            for seg in 0..8 {
                if seg > 0 {
                    out.push(b'/');
                }
                out.extend_from_slice(&hex_str(rng, 4));
            }
            out
        }
        Shape::Shared => {
            let mut out = SHARED_PREFIXES[rng.below(SHARED_PREFIXES.len())]
                .as_bytes()
                .to_vec();
            out.extend_from_slice(&hex_str(rng, 16));
            out
        }
    }
}

fn gen_address(rng: &mut SplitMix64) -> ChunkAddress {
    let mut buf = [0u8; 32];
    rng.fill(&mut buf);
    ChunkAddress::from(buf)
}

/// One comparative manifest workload: entries plus every derived op input.
#[derive(Clone, Debug)]
pub struct Corpus {
    /// Key and plain-reference pairs, in generation order.
    pub entries: Vec<(Vec<u8>, ChunkAddress)>,
    /// Keys present in `entries`, sampled with replacement.
    pub lookup_hits: Vec<Vec<u8>>,
    /// Same-shape keys absent from `entries`.
    pub lookup_misses: Vec<Vec<u8>>,
    /// Fresh keys for the incremental edit.
    pub inserts: Vec<(Vec<u8>, ChunkAddress)>,
    /// Existing keys the incremental edit removes.
    pub removes: Vec<Vec<u8>>,
    /// Prefix-scan input, guaranteed to match at least one key.
    pub prefix: Vec<u8>,
    /// Range lower bound: the sorted key at rank n/4, so it exists.
    pub range_lo: Vec<u8>,
    /// Range upper bound (exclusive): the sorted key at rank 3n/4.
    pub range_hi: Vec<u8>,
}

impl Corpus {
    /// Generate the workload for `n` entries of `shape` from `seed`.
    ///
    /// The generator, not the adapters, neutralizes every semantic divergence
    /// between the two manifest implementations, so both sides run the same
    /// byte-identical driver over natively legal inputs:
    /// - removes only ever name existing keys: a 0.2 commit fails on an
    ///   absent-path remove, while 1.0 treats it as a no-op;
    /// - no per-entry metadata anywhere: the typed-registry model of 1.0 and
    ///   the string-map model of 0.2 are not comparable;
    /// - values are plain 32-byte references only: the encryption models
    ///   (per-reference ref64 vs whole-trie obfuscation) differ;
    /// - no empty key: 1.0 stores it in the root extension, 0.2 rejects it.
    #[must_use]
    pub fn generate(n: usize, shape: Shape, seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed);
        let mut map: BTreeMap<Vec<u8>, ChunkAddress> = BTreeMap::new();
        while map.len() < n {
            let key = gen_key(&mut rng, shape);
            let address = gen_address(&mut rng);
            map.entry(key).or_insert(address);
        }
        let entries: Vec<(Vec<u8>, ChunkAddress)> =
            map.iter().map(|(k, a)| (k.clone(), *a)).collect();
        let sorted: Vec<&Vec<u8>> = map.keys().collect();

        let lookup_hits: Vec<Vec<u8>> = (0..LOOKUP_HITS)
            .map(|_| entries[rng.below(n)].0.clone())
            .collect();
        let mut lookup_misses = Vec::with_capacity(LOOKUP_MISSES);
        while lookup_misses.len() < LOOKUP_MISSES {
            let key = gen_key(&mut rng, shape);
            if !map.contains_key(&key) {
                lookup_misses.push(key);
            }
        }
        let mut inserts = Vec::with_capacity(EDIT_INSERTS);
        while inserts.len() < EDIT_INSERTS {
            let key = gen_key(&mut rng, shape);
            if !map.contains_key(&key) {
                let address = gen_address(&mut rng);
                inserts.push((key, address));
            }
        }
        let mut removes: Vec<Vec<u8>> = Vec::with_capacity(EDIT_REMOVES);
        while removes.len() < EDIT_REMOVES.min(n) {
            let key = entries[rng.below(n)].0.clone();
            if !removes.contains(&key) {
                removes.push(key);
            }
        }

        let median = sorted[n / 2];
        let prefix = scan_prefix(shape, median);
        let range_lo = sorted[n / 4].clone();
        let range_hi = sorted[(n / 4) * 3].clone();
        Self {
            entries,
            lookup_hits,
            lookup_misses,
            inserts,
            removes,
            prefix,
            range_lo,
            range_hi,
        }
    }

    /// Keys with `range_lo <= key < range_hi`: exactly `n/2` by construction.
    #[must_use]
    pub fn range_len(&self) -> u64 {
        u64::try_from((self.entries.len() / 4) * 3 - self.entries.len() / 4).unwrap_or(0)
    }
}

/// A shape-appropriate scan prefix taken from the median key.
fn scan_prefix(shape: Shape, median: &[u8]) -> Vec<u8> {
    match shape {
        Shape::Flat => median[..1].to_vec(),
        Shape::Deep => median[..2].to_vec(),
        Shape::Shared => {
            let mut slashes = 0usize;
            for (i, byte) in median.iter().enumerate() {
                if *byte == b'/' {
                    slashes += 1;
                    if slashes == 2 {
                        return median[..=i].to_vec();
                    }
                }
            }
            median[..1].to_vec()
        }
    }
}

/// Deterministic random payload for the file suite.
#[must_use]
pub fn payload(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = SplitMix64::new(seed);
    let mut data = vec![0u8; len];
    rng.fill(&mut data);
    data
}
