//! Pins `docs/spec/sbu1.md` against the codec.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use std::fs;
use std::path::PathBuf;

use alloy_primitives::hex;
use nectar_postage_usage::{
    MAGIC, MAX_BUCKET_DEPTH, MAX_COUNTER_BITS, MAX_EXCEPTIONS, MAX_PAYLOAD_SIZE, Mutability,
    PublishedSequence, ROOT_HEADER_SIZE, Snapshot, SwarmSpec, USAGE_DOMAIN, UsageTable,
};
use nectar_testing::{LowFloor, low_floor};

mod common;

use common::{batch_id, bucket_depth, owner};

const SPEC: &str = "docs/spec/sbu1.md";
const README: &str = "crates/postage-usage/README.md";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(path: &str) -> String {
    fs::read_to_string(repo_root().join(path)).unwrap()
}

/// Offset, size and field text of each root-header row, stripped of its
/// markdown decoration.
fn header_rows(doc: &str) -> Vec<(usize, usize, String)> {
    let mut rows = Vec::new();
    let mut inside = false;
    for line in doc.lines() {
        if line.starts_with("| offset | size | field |") {
            inside = true;
            continue;
        }
        if !inside {
            continue;
        }
        if !line.starts_with('|') {
            break;
        }
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        if cells[0].starts_with("---") {
            continue;
        }
        let field: String = cells[2].chars().filter(|c| !"`\"".contains(*c)).collect();
        rows.push((
            cells[0].parse().unwrap(),
            cells[1].parse().unwrap(),
            field.split_whitespace().collect::<Vec<_>>().join(" "),
        ));
    }
    assert!(
        !rows.is_empty(),
        "the specification has no root-header table"
    );
    rows
}

fn field(rows: &[(usize, usize, String)], name: &str) -> (usize, usize) {
    rows.iter()
        .find(|(_, _, field)| field == name)
        .map(|&(offset, size, _)| (offset, size))
        .unwrap_or_else(|| panic!("the specification names no field {name}"))
}

/// The first specified vector: `d = 12`, `u = 8`, one exception, inline deltas.
fn small_root(mutability: Mutability) -> (Vec<u8>, Snapshot<LowFloor>) {
    let mut counts: Vec<u32> = (0..256u32).map(|b| 3 + (b & 3)).collect();
    counts[200] = 16;
    let table = UsageTable::from_counts(batch_id(), 12, low_floor(8), counts, mutability).unwrap();
    persist(table)
}

/// The second specified vector: `d = 29`, `u = 16`, two exceptions, 13 leaves.
fn large_root() -> Vec<u8> {
    let mut counts: Vec<u32> = (0..65536u32).map(|b| 100 + (b % 50)).collect();
    counts[0x1234] = 5000;
    counts[0xcbe5] = 8192;
    let table = UsageTable::from_counts(
        batch_id(),
        29,
        bucket_depth(),
        counts,
        Mutability::Immutable,
    )
    .unwrap();
    persist(table).0
}

fn persist<S: SwarmSpec>(table: UsageTable<S>) -> (Vec<u8>, Snapshot<S>) {
    let mut snapshot = Snapshot::new(table);
    let plan = snapshot
        .revalidate(PublishedSequence::NONE)
        .unwrap()
        .plan_persist(&owner())
        .unwrap();
    (plan.chunks[0].payload.to_vec(), snapshot)
}

#[test]
fn the_root_header_table_matches_the_encoder() {
    let rows = header_rows(&read(SPEC));
    let (root, snapshot) = small_root(Mutability::Immutable);
    let view = snapshot.table();

    let mut next = 0usize;
    for (offset, size, name) in &rows {
        assert_eq!(*offset, next, "field {name} does not follow the last one");
        next += size;
    }
    assert_eq!(
        next, ROOT_HEADER_SIZE,
        "the header table is the wrong width"
    );

    let be = |name: &str| -> u64 {
        let (offset, size) = field(&rows, name);
        root[offset..offset + size]
            .iter()
            .fold(0u64, |value, &byte| (value << 8) | u64::from(byte))
    };

    let (offset, size) = field(&rows, "magic SBU1");
    assert_eq!(&root[offset..offset + size], &MAGIC);
    let (offset, size) = field(&rows, "batch id");
    assert_eq!(
        &root[offset..offset + size],
        &<[u8; 32]>::from(batch_id())[..]
    );
    assert_eq!(be("batch depth d"), u64::from(view.depth()));
    assert_eq!(be("bucket depth u"), u64::from(view.bucket_depth().get()));
    assert_eq!(be("flags"), 0);
    // The first persist of a never-published snapshot emits sequence 1.
    assert_eq!(be("sequence"), 1);
    assert_eq!(be("counter sum"), view.total_issued());
    assert_eq!(be("base"), u64::from(view.min_count()));
    let allocated = be("allocated count A");
    assert_eq!(allocated, snapshot.allocated_slots().len() as u64);

    // The documented offsets must explain the payload length exactly, which
    // catches a wrong offset the value checks cannot see.
    let width = be("delta width w") as usize;
    let leaves = be("leaf count L") as usize;
    let exceptions = be("exception count E") as usize;
    let buckets = 1usize << view.bucket_depth().get();
    let tail = if leaves > 0 {
        32 * leaves
    } else {
        (buckets * width).div_ceil(8)
    };
    assert_eq!(
        root.len(),
        ROOT_HEADER_SIZE + 8 * exceptions + 4 * allocated as usize + tail
    );
}

#[test]
fn the_documented_vectors_match_the_encoder() {
    let spec = read(SPEC);
    let (flags, _) = field(&header_rows(&spec), "flags");
    let (immutable, _) = small_root(Mutability::Immutable);
    let (mutable, _) = small_root(Mutability::Mutable);

    assert!(
        spec.contains(&hex::encode(&immutable)),
        "the single-chunk vector drifted from the encoder"
    );
    assert!(
        spec.contains(&hex::encode(large_root())),
        "the multi-leaf vector drifted from the encoder"
    );

    let mut expected = immutable;
    expected[flags] = 0x01;
    assert_eq!(
        mutable, expected,
        "the mutable vector differs by more than the flags byte"
    );
}

#[test]
fn every_repository_path_the_documents_cite_exists() {
    let mut cited = 0usize;
    for doc in [SPEC, README] {
        let text = read(doc);
        for token in text.split(['`', '(', ')', '[', ']', ' ', '\n', ',', ';']) {
            let token = token.trim_end_matches('.');
            if token.starts_with("http")
                || !token.contains('/')
                || !(token.ends_with(".rs") || token.ends_with(".md") || token.ends_with(".toml"))
            {
                continue;
            }
            assert!(
                repo_root().join(token).exists(),
                "{doc} cites {token}, which does not exist"
            );
            cited += 1;
        }
    }
    assert!(
        cited > 0,
        "the path scan matched nothing, so it guards nothing"
    );
}

#[test]
fn the_documented_limits_match_the_crate_constants() {
    let text = read(SPEC);
    for needle in [
        format!("\"{}\"", core::str::from_utf8(&MAGIC).unwrap()),
        core::str::from_utf8(USAGE_DOMAIN).unwrap().to_owned(),
        format!("fixed {ROOT_HEADER_SIZE}-byte header"),
        format!("at most {MAX_EXCEPTIONS} entries"),
        format!("at most {MAX_PAYLOAD_SIZE} bytes"),
        format!("`u` is above {MAX_BUCKET_DEPTH}"),
        format!("`d - u` is above {MAX_COUNTER_BITS}"),
        format!("floor({} / w)", MAX_PAYLOAD_SIZE * 8),
    ] {
        assert!(text.contains(&needle), "the specification omits {needle:?}");
    }
}
