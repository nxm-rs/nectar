//! Deterministic, seeded corpus generators shaped like the calibration traces.
//!
//! Master seed `0x6d616e7472617931`. Per-corpus stream seed is
//! `sha256(master_le || corpus_name)`; a per-index block is
//! `sha256(stream_seed || u64le(i))`, extended with a counter when a single
//! index needs more than 32 bytes. Keys are generated then sorted, so the
//! published set is order-independent. Values are ref32 addresses derived as
//! `sha256(b"val" || key_bytes)`.
//!
//! Each corpus yields a logical key that both formats express: 1.0 takes raw
//! key bytes; 0.2 needs a UTF-8 `&str`, so uniform binary keys are lowercase
//! hex (the ACT hex pattern) while path corpora are byte-identical in both.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

/// Master seed, little-endian.
pub const MASTER_SEED: u64 = 0x6d61_6e74_7261_7931;

/// A generated key with both format encodings and optional content type.
#[derive(Clone, Debug)]
pub struct GenKey {
    /// Raw key bytes fed to mantaray 1.0.
    pub raw: Vec<u8>,
    /// UTF-8 path fed to mantaray 0.2 (hex of `raw` for the uniform corpus).
    pub path: String,
    /// Content-type metadata value, when the corpus carries it.
    pub content_type: Option<&'static str>,
}

impl GenKey {
    /// Whether the 0.2 path is a byte-identical view of the 1.0 raw key.
    #[must_use]
    pub fn encodings_match(&self) -> bool {
        self.raw.as_slice() == self.path.as_bytes()
    }
}

/// The four corpora.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Corpus {
    /// Zero-prefix-sharing control: 32 raw bytes per key.
    Uniform,
    /// Deep '/'-separated article paths with heavy prefix sharing.
    Kiwix,
    /// Full tile pyramid `z/x/y`.
    OsmPyramid,
    /// Bounding-box tile region `z/x/y` (contiguous runs).
    OsmBbox,
}

impl Corpus {
    /// The JSON key for this corpus.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Kiwix => "kiwix",
            Self::OsmPyramid => "osm_pyramid",
            Self::OsmBbox => "osm_bbox",
        }
    }

    /// Whether the 0.2 key encoding is `hex` (uniform) or `raw` (paths).
    #[must_use]
    pub const fn key_encoding(self) -> &'static str {
        match self {
            Self::Uniform => "hex",
            _ => "raw",
        }
    }

    /// All four corpora.
    #[must_use]
    pub const fn all() -> [Self; 4] {
        [Self::Uniform, Self::Kiwix, Self::OsmPyramid, Self::OsmBbox]
    }
}

fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    h.finalize().into()
}

/// Per-corpus stream seed.
fn stream_seed(corpus: Corpus) -> [u8; 32] {
    sha256(&[&MASTER_SEED.to_le_bytes(), corpus.name().as_bytes()])
}

/// A deterministic byte stream for index `i`: concatenated sha256 blocks so a
/// single index can furnish more than 32 bytes of entropy.
struct IdxStream {
    seed: [u8; 32],
    idx: u64,
    block: [u8; 32],
    ctr: u32,
    pos: usize,
}

impl IdxStream {
    fn new(seed: [u8; 32], idx: u64) -> Self {
        let block = sha256(&[&seed, &idx.to_le_bytes(), &0u32.to_le_bytes()]);
        Self {
            seed,
            idx,
            block,
            ctr: 0,
            pos: 0,
        }
    }

    fn next_byte(&mut self) -> u8 {
        if self.pos == 32 {
            self.ctr = self.ctr.wrapping_add(1);
            self.block = sha256(&[&self.seed, &self.idx.to_le_bytes(), &self.ctr.to_le_bytes()]);
            self.pos = 0;
        }
        let b = self.block[self.pos];
        self.pos += 1;
        b
    }
}

/// A tagged deterministic address over a key's bytes: `sha256(tag || key)`.
#[must_use]
pub fn tagged_addr(tag: &[u8], key_bytes: &[u8]) -> [u8; 32] {
    sha256(&[tag, key_bytes])
}

/// The ref32 value address for a key's bytes: `sha256(b"val" || key)`.
#[must_use]
pub fn value_addr(key_bytes: &[u8]) -> [u8; 32] {
    tagged_addr(b"val", key_bytes)
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[usize::from(b >> 4)] as char);
        s.push(HEX[usize::from(b & 0x0f)] as char);
    }
    s
}

/// Generate exactly `n` keys of `corpus`, sorted by raw bytes.
#[must_use]
pub fn generate(corpus: Corpus, n: usize) -> Vec<GenKey> {
    match corpus {
        Corpus::Uniform => uniform(n),
        Corpus::Kiwix => kiwix(n),
        Corpus::OsmPyramid => osm_pyramid(n),
        Corpus::OsmBbox => osm_bbox(n),
    }
}

fn uniform(n: usize) -> Vec<GenKey> {
    let seed = stream_seed(Corpus::Uniform);
    let mut set: BTreeSet<[u8; 32]> = BTreeSet::new();
    let mut i: u64 = 0;
    while set.len() < n {
        let block = sha256(&[&seed, &i.to_le_bytes()]);
        set.insert(block);
        i += 1;
    }
    set.into_iter()
        .take(n)
        .map(|raw| GenKey {
            path: to_hex(&raw),
            raw: raw.to_vec(),
            content_type: None,
        })
        .collect()
}

const KIWIX_ALPHABET: &[u8; 36] = b"abcdefghijklmnopqrstuvwxyz0123456789";
const KIWIX_NAMESPACES: [&str; 4] = ["A/", "M/", "-/", "I/"];
const KIWIX_EXTS: [&str; 3] = [".html", ".png", ".css"];
const KIWIX_MIMES: [&str; 3] = ["text/html", "image/png", "text/css"];

fn kiwix(n: usize) -> Vec<GenKey> {
    let seed = stream_seed(Corpus::Kiwix);
    // Order-of-emission list for prefix reuse, then dedup+sort at the end.
    let mut emitted: Vec<(String, &'static str)> = Vec::with_capacity(n);
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut i: u64 = 0;
    while seen.len() < n {
        let mut s = IdxStream::new(seed, i);
        i += 1;
        let b0 = s.next_byte();
        let b1 = s.next_byte();
        let b2 = s.next_byte();

        // Namespace by Zipf-ish threshold on b0 (70/15/10/5).
        let ns = if b0 < 179 {
            KIWIX_NAMESPACES[0]
        } else if b0 < 217 {
            KIWIX_NAMESPACES[1]
        } else if b0 < 243 {
            KIWIX_NAMESPACES[2]
        } else {
            KIWIX_NAMESPACES[3]
        };

        let mut path = String::new();
        // Prefix reuse (prob 0.6) to force shared LCP. The reused span is
        // trimmed back to a '/' directory boundary so a reused prefix is always
        // a well-formed directory path: this preserves heavy prefix sharing
        // while keeping every full key terminal (ends in an extension, never a
        // '/'), so no key is ever a strict prefix of another. That keeps the
        // corpus faithful to real hierarchical article paths and avoids the
        // "file.png/dir" splice artifact.
        let mut reused = false;
        if b1 < 154 && !emitted.is_empty() {
            let pick = (usize::from(b2) << 8 | usize::from(s.next_byte())) % emitted.len();
            let k = 4 + usize::from(s.next_byte()) % 20;
            let src = &emitted[pick].0;
            let take = k.min(src.len());
            // Trim back to the last '/' within the first `take` bytes.
            if let Some(pos) = src.as_bytes()[..take].iter().rposition(|&c| c == b'/') {
                path.push_str(&src[..=pos]);
                reused = true;
            }
        }
        if !reused {
            path.push_str(ns);
        }

        let segs = 1 + usize::from(s.next_byte()) % 3;
        for seg in 0..segs {
            if !path.is_empty() && !path.ends_with('/') {
                path.push('/');
            }
            let len = 3 + usize::from(s.next_byte()) % 22;
            for _ in 0..len {
                let c = KIWIX_ALPHABET[usize::from(s.next_byte()) % 36];
                path.push(c as char);
            }
            let _ = seg;
        }
        let ext_pick = usize::from(s.next_byte()) % 3;
        path.push_str(KIWIX_EXTS[ext_pick]);
        let mime = KIWIX_MIMES[ext_pick];

        if seen.insert(path.clone()) {
            emitted.push((path, mime));
        }
    }

    let mut out: Vec<GenKey> = emitted
        .into_iter()
        .map(|(path, mime)| GenKey {
            raw: path.clone().into_bytes(),
            path,
            content_type: Some(mime),
        })
        .collect();
    out.sort_by(|a, b| a.raw.cmp(&b.raw));
    out
}

fn ascii_key(z: u32, x: u64, y: u64) -> GenKey {
    let path = format!("{z}/{x}/{y}");
    GenKey {
        raw: path.clone().into_bytes(),
        path,
        content_type: None,
    }
}

fn osm_pyramid(n: usize) -> Vec<GenKey> {
    let mut out: Vec<GenKey> = Vec::with_capacity(n);
    let mut z: u32 = 0;
    while out.len() < n {
        let side: u64 = 1u64 << z;
        for x in 0..side {
            for y in 0..side {
                if out.len() == n {
                    break;
                }
                out.push(ascii_key(z, x, y));
            }
            if out.len() == n {
                break;
            }
        }
        z += 1;
        if z > 40 {
            break;
        }
    }
    out.sort_by(|a, b| a.raw.cmp(&b.raw));
    out
}

fn osm_bbox(n: usize) -> Vec<GenKey> {
    // Germany-like contiguous window: x in [0.51,0.55], y in [0.33,0.40].
    let mut out: Vec<GenKey> = Vec::with_capacity(n);
    for z in 0..=15u32 {
        let side: f64 = (1u64 << z) as f64;
        let x_lo = (0.51 * side) as u64;
        let x_hi = (0.55 * side).ceil() as u64;
        let y_lo = (0.33 * side) as u64;
        let y_hi = (0.40 * side).ceil() as u64;
        for x in x_lo..x_hi.max(x_lo + 1).min(1u64 << z) {
            for y in y_lo..y_hi.max(y_lo + 1).min(1u64 << z) {
                if out.len() == n {
                    break;
                }
                out.push(ascii_key(z, x, y));
            }
            if out.len() == n {
                break;
            }
        }
        if out.len() == n {
            break;
        }
    }
    out.sort_by(|a, b| a.raw.cmp(&b.raw));
    out.truncate(n);
    out
}
