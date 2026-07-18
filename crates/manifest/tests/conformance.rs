//! Conformance to the frozen mantaray 1.0 wire format, through the public
//! API only: the normative byte vectors, the content-cut H64 anchors, and
//! the canonical-or-reject bijection between logical trees and byte strings.

use std::collections::HashSet;
use std::error::Error;

use alloy_primitives::{b256, keccak256};
use bytes::Bytes;
use nectar_manifest::{
    Child, CustomKeyError, DecodeError, Domain, Entry, ForkPayload, ForkTable, Format, KeyId,
    Metadata, Node, Prefix, RootExtension, SegmentKind, SegmentWeight, V1, cut, embed, h64,
    segment,
};
use nectar_primitives::{ChunkAddress, ChunkRef, EncryptedChunkRef, EncryptionKey};

type TestResult = Result<(), Box<dyn Error>>;

/// A fallible assertion: Result-returning tests report failures as errors.
fn ensure(cond: bool, what: &str) -> TestResult {
    if cond { Ok(()) } else { Err(what.into()) }
}

/// A fallible equality assertion.
fn ensure_eq<T: PartialEq + core::fmt::Debug>(left: T, right: T, what: &str) -> TestResult {
    if left == right {
        Ok(())
    } else {
        Err(format!("{what}: {left:?} != {right:?}").into())
    }
}

const fn ref32(byte: u8) -> ChunkRef {
    ChunkRef::new(ChunkAddress::new([byte; 32]))
}

fn ref64(addr: u8, key: u8) -> EncryptedChunkRef {
    EncryptedChunkRef::new(
        ChunkAddress::new([addr; 32]),
        EncryptionKey::from([key; 32]),
    )
}

fn prefix(bytes: &[u8]) -> Result<Prefix, Box<dyn Error>> {
    Ok(Prefix::try_from(bytes)?)
}

/// A payload from node flags and the bytes that follow them.
fn payload(flags: u8, rest: &[u8]) -> Vec<u8> {
    let mut image = vec![0x6D, 0x01, flags];
    image.extend_from_slice(rest);
    image
}

/// A payload with node flags `HAS_META`, the given metadata block, and an
/// empty fork table.
fn with_meta(block: &[u8]) -> Vec<u8> {
    let mut rest = block.to_vec();
    rest.extend_from_slice(&[0x00, 0x00]);
    payload(0x10, &rest)
}

/// A minimal 34-byte fork record: an empty tail and a ref32 entry.
fn record32(byte: u8) -> Vec<u8> {
    let mut record = vec![0x01, 0x01];
    record.extend_from_slice(&[byte; 32]);
    record
}

// The empty-map root payload is the five bytes 6D 01 00 00 00.
#[test]
fn empty_map_root_is_the_frozen_five_byte_vector() -> TestResult {
    let node: Node = Node::empty();
    let image = node.encode()?;
    ensure_eq(
        image.as_slice(),
        [0x6D, 0x01, 0x00, 0x00, 0x00].as_slice(),
        "empty-map root payload",
    )?;

    let decoded: Node = Node::decode(&image)?;
    ensure(decoded.is_empty(), "decoded root must be empty")?;
    ensure_eq(&decoded.encode()?, &image, "re-encode")?;
    Ok(())
}

// The worked example: a two-file website in one 150-byte payload.
#[test]
fn worked_two_file_example_is_bit_exact() -> TestResult {
    let ref_a = [0xAA; 32]; // "index.html" content
    let ref_b = [0xBB; 32]; // "img/logo.png" content

    let mut child = ForkTable::new();
    child.insert(
        prefix(b"mg/logo.png")?,
        Entry::from(ChunkRef::new(ChunkAddress::new(ref_b))).into(),
        Some(Metadata::new(
            KeyId::ContentType,
            Bytes::from_static(b"image/png"),
        )?),
    )?;
    child.insert(
        prefix(b"ndex.html")?,
        Entry::from(ChunkRef::new(ChunkAddress::new(ref_a))).into(),
        Some(Metadata::new(
            KeyId::ContentType,
            Bytes::from_static(b"text/html"),
        )?),
    )?;

    let mut forks = ForkTable::new();
    forks.insert(prefix(b"i")?, Child::Embedded(child).into(), None)?;

    let node: Node = Node::new(
        RootExtension::new(
            None,
            Some(Metadata::new(
                KeyId::WebsiteIndexDocument,
                Bytes::from_static(b"index.html"),
            )?),
        ),
        forks,
    );

    // The spec's offset-by-offset listing.
    let mut expected = Vec::new();
    expected.extend_from_slice(&[0x6D, 0x01]); // 0: magic 'm', version 1
    expected.push(0x10); // 2: root flags: HAS_META
    expected.extend_from_slice(&[0x0D, 0x00]); // 3: mlen = 13
    expected.extend_from_slice(&[0x03, 0x0A, 0x00]); // 5: website-index-document, vlen = 10
    expected.extend_from_slice(b"index.html");
    expected.extend_from_slice(&[0x01, 0x00]); // 18: fcount = 1
    expected.extend_from_slice(&[0x69, 0x00, 0x00]); // 20: index[0]: key 'i', off 0
    expected.push(0x0C); // 23: fflags: CHILD_FMT = 3 (inline)
    expected.push(0x01); // 24: plen = 1 (prefix "i", empty tail)
    expected.extend_from_slice(&[0x7B, 0x00]); // 25: ilen = 123
    expected.push(0x00); // 27: embedded node flags
    expected.extend_from_slice(&[0x02, 0x00]); // 28: fcount = 2
    expected.extend_from_slice(&[0x6D, 0x00, 0x00]); // 30: index: 'm' @ 0
    expected.extend_from_slice(&[0x6E, 0x3A, 0x00]); // 33: index: 'n' @ 58
    expected.extend_from_slice(&[0x11, 0x0B]); // 36: fork 'm': ENTRY_FMT=1|HAS_META, plen = 11
    expected.extend_from_slice(b"g/logo.png");
    expected.extend_from_slice(&ref_b); // 48
    expected.extend_from_slice(&[0x0C, 0x00, 0x01, 0x09, 0x00]); // 80: mlen = 12; content-type
    expected.extend_from_slice(b"image/png");
    expected.extend_from_slice(&[0x11, 0x09]); // 94: fork 'n': ENTRY_FMT=1|HAS_META, plen = 9
    expected.extend_from_slice(b"dex.html");
    expected.extend_from_slice(&ref_a); // 104
    expected.extend_from_slice(&[0x0C, 0x00, 0x01, 0x09, 0x00]); // 136: mlen = 12; content-type
    expected.extend_from_slice(b"text/html");
    ensure_eq(expected.len(), 150, "vector length")?;

    let image = node.encode()?;
    ensure_eq(&image, &expected, "worked example payload")?;

    let decoded: Node = Node::decode(&image)?;
    ensure_eq(&decoded, &node, "round trip")?;
    ensure_eq(&decoded.encode()?, &image, "re-encode")?;
    Ok(())
}

// The H64 anchors, with the keccak sanity anchor.
#[test]
fn h64_anchors_match_the_spec() {
    assert_eq!(
        keccak256([]),
        b256!("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")
    );

    let anchors: [(&[u8], u64); 5] = [
        (b"", 0x3c23_f786_0146_d2c5),
        (&[0x00], 0x1428_1e7a_9e78_36bc),
        (b"a", 0x1242_f58d_1625_c23a),
        (b"abc", 0x4fa9_45ea_7a65_034e),
        (b"index.html", 0xb4c3_763c_9ea4_174d),
    ];
    for (input, expected) in anchors {
        assert_eq!(h64(input), expected, "H64({input:02X?})");
    }
}

// The worked leaf partition: eight forks a..h, weights per the spec table.
// Each row pins H64(P), the w * CUT_SCALE threshold, and the hash-cut bit;
// the partition run pins the segmentation itself.
#[test]
fn worked_leaf_partition_reproduces_the_spec_trace() -> TestResult {
    // (fork key, record weight w, H64 of the one-byte prefix, hash cut)
    let rows: [(u8, u64, u64, bool); 8] = [
        (b'a', 207, 0x1242_f58d_1625_c23a, true),
        (b'b', 707, 0xf5ed_e015_e33d_55b5, false),
        (b'c', 307, 0x0653_1f3c_39b6_420b, true),
        (b'd', 1007, 0xb16e_2362_858e_91f1, false),
        (b'e', 1031, 0xfb87_09d8_892c_98a8, false),
        (b'f', 807, 0x6e49_0095_b7ae_e8d1, false),
        (b'g', 104, 0x0d13_9df4_35c4_bc14, false),
        (b'h', 1150, 0x906e_cc20_2493_66a7, false),
    ];
    // The spec's stated w * CUT_SCALE thresholds for the worked weights.
    let thresholds: [u64; 8] = [
        0x19e0_0000_0000_0000,
        0x5860_0000_0000_0000,
        0x2660_0000_0000_0000,
        0x7de0_0000_0000_0000,
        0x80e0_0000_0000_0000,
        0x64e0_0000_0000_0000,
        0x0d00_0000_0000_0000,
        0x8fc0_0000_0000_0000,
    ];

    let seg_target = u64::try_from(V1::SEG_TARGET)?;

    for ((key, w, hash, hash_cut), threshold) in rows.into_iter().zip(thresholds) {
        let name = char::from(key);
        ensure_eq(h64(&[key]), hash, &format!("H64({name})"))?;
        ensure_eq(
            w.checked_mul(V1::CUT_SCALE),
            Some(threshold),
            &format!("threshold of {name}"),
        )?;
        ensure(w < seg_target, "every worked weight is below SEG_TARGET")?;
        ensure_eq(hash < threshold, hash_cut, &format!("cut bit of {name}"))?;
        // Below SEG_TARGET the real predicate reduces to the hash comparison.
        ensure_eq(
            cut::<V1>(&[key], usize::try_from(w)?),
            hash_cut,
            &format!("cut predicate of {name}"),
        )?;
    }

    // The real leaf partition over (fork-relative prefix, weight), CAP_FORK.
    let forks: Vec<(Prefix, SegmentWeight)> = rows
        .into_iter()
        .map(|(key, w, _, _)| {
            Ok::<_, Box<dyn Error>>((prefix(&[key])?, SegmentWeight::new(usize::try_from(w)?)?))
        })
        .collect::<Result<_, _>>()?;
    let ranges = segment::<V1>(&forks, SegmentKind::Leaf);

    // Reconstruct the key groups from the returned index ranges.
    let mut segments: Vec<Vec<u8>> = Vec::new();
    for range in &ranges {
        let mut group: Vec<u8> = Vec::new();
        for i in range.clone() {
            let (key, ..) = *rows.get(i).ok_or("segment index out of range")?;
            group.push(key);
        }
        segments.push(group);
    }

    let expected = [b"abc".to_vec(), b"defg".to_vec(), b"h".to_vec()];
    ensure_eq(segments.as_slice(), expected.as_slice(), "segments")?;
    let first_keys: Vec<u8> = segments.iter().filter_map(|s| s.first().copied()).collect();
    ensure_eq(
        first_keys.as_slice(),
        [0x61, 0x64, 0x68].as_slice(),
        "directory first keys",
    )?;
    Ok(())
}

// The tightness anchors: exact integer comparisons an implementation must
// reproduce.
#[test]
fn cut_thresholds_are_exact_integer_comparisons() {
    assert_eq!(V1::CUT_SCALE, 1u64 << 53);

    let g = h64(b"g");
    assert_eq!(g, 942_270_419_250_232_340);
    assert_eq!(
        104u64.checked_mul(V1::CUT_SCALE),
        Some(936_748_722_493_063_168)
    );
    assert_eq!(
        105u64.checked_mul(V1::CUT_SCALE),
        Some(945_755_921_747_804_160)
    );
    assert!(g >= 936_748_722_493_063_168, "g must not cut at w = 104");
    assert!(g < 945_755_921_747_804_160, "g must cut at w = 105");

    let h = h64(b"h");
    assert_eq!(h, 10_407_480_227_324_454_567);
    assert!(1155u64.checked_mul(V1::CUT_SCALE).is_some_and(|t| h >= t));
    assert!(1156u64.checked_mul(V1::CUT_SCALE).is_some_and(|t| h < t));
}

// Child-local embedding: a child inlines iff its flat body fits INLINE_MAX
// and shares its parent's encryption domain. The worked example's shared
// child is a 123-byte plaintext body, so it embeds (its ilen is 123 there).
#[test]
fn child_embedding_gates_on_inline_max_and_domain() -> TestResult {
    ensure(
        embed::<V1>(123, Domain::Plain, Domain::Plain),
        "the worked child within INLINE_MAX embeds",
    )?;
    ensure(
        embed::<V1>(V1::INLINE_MAX, Domain::Plain, Domain::Plain),
        "a body at INLINE_MAX embeds",
    )?;
    ensure(
        !embed::<V1>(V1::INLINE_MAX + 1, Domain::Plain, Domain::Plain),
        "a body over INLINE_MAX spills",
    )?;
    ensure(
        !embed::<V1>(123, Domain::Plain, Domain::Encrypted),
        "a cross-domain child spills",
    )?;
    Ok(())
}

/// The bijection family: pairwise-distinct logical trees, including the
/// near-miss pairs (absent value against empty value, the all-zero
/// reference as a legal value, not a sentinel).
fn tree_family() -> Result<Vec<Node>, Box<dyn Error>> {
    let mut family: Vec<Node> = vec![Node::empty()];

    // Root extension variants: the empty inline value is a value, distinct
    // from no value at all.
    family.push(Node::new(
        Some(Entry::inline(Bytes::new())?.into()),
        ForkTable::new(),
    ));
    family.push(Node::new(
        Some(Entry::inline(Bytes::from_static(b"v"))?.into()),
        ForkTable::new(),
    ));
    family.push(Node::new(
        Some(
            Metadata::new(
                KeyId::WebsiteIndexDocument,
                Bytes::from_static(b"index.html"),
            )?
            .into(),
        ),
        ForkTable::new(),
    ));

    // The all-zero reference is a legal value, not an absence sentinel.
    let mut forks = ForkTable::new();
    forks.insert(prefix(b"a")?, Entry::from(ref32(0x00)).into(), None)?;
    family.push(Node::new(None, forks));

    let mut forks = ForkTable::new();
    forks.insert(prefix(b"a")?, Entry::from(ref64(0x11, 0x22)).into(), None)?;
    family.push(Node::new(None, forks));

    // A child alone, and the same child with an empty inline entry: one
    // wire bit apart, two distinct trees.
    let mut forks = ForkTable::new();
    forks.insert(prefix(b"a")?, Child::from(ref32(0x33)).into(), None)?;
    family.push(Node::new(None, forks));

    let mut forks = ForkTable::new();
    forks.insert(
        prefix(b"a")?,
        ForkPayload::new(
            Some(Entry::inline(Bytes::new())?),
            Some(Child::from(ref32(0x33))),
        )
        .ok_or("empty payload")?,
        None,
    )?;
    family.push(Node::new(None, forks));

    // A longer prefix, metadata, an embedded child, and a two-fork table.
    let mut forks = ForkTable::new();
    forks.insert(prefix(b"abc")?, Entry::from(ref32(0x44)).into(), None)?;
    family.push(Node::new(None, forks));

    let mut forks = ForkTable::new();
    forks.insert(
        prefix(b"a")?,
        Entry::from(ref32(0x44)).into(),
        Some(Metadata::new(KeyId::Filename, Bytes::from_static(b"a"))?),
    )?;
    family.push(Node::new(None, forks));

    let mut inner = ForkTable::new();
    inner.insert(prefix(b"b")?, Entry::from(ref32(0x55)).into(), None)?;
    let mut forks = ForkTable::new();
    forks.insert(prefix(b"a")?, Child::Embedded(inner).into(), None)?;
    family.push(Node::new(None, forks));

    let mut forks = ForkTable::new();
    forks.insert(prefix(b"a")?, Entry::from(ref32(0x66)).into(), None)?;
    forks.insert(prefix(b"b")?, Entry::from(ref32(0x77)).into(), None)?;
    family.push(Node::new(None, forks));

    Ok(family)
}

// One logical tree, one byte string: every family member round-trips to
// itself, re-encodes to the same bytes, and no two members share an image.
#[test]
fn bijection_holds_over_the_tree_family() -> TestResult {
    let family = tree_family()?;
    let mut images: HashSet<Vec<u8>> = HashSet::new();
    for node in &family {
        let image = node.encode()?;
        let decoded: Node = Node::decode(&image)?;
        ensure_eq(&decoded, node, "round trip")?;
        ensure_eq(&decoded.encode()?, &image, "re-encode")?;
        ensure(images.insert(image), "two trees share one byte string")?;
    }
    ensure_eq(images.len(), family.len(), "family image count")?;
    Ok(())
}

// Out-of-order and duplicate fork index keys must fail to decode, never
// silently reorder.
#[test]
fn out_of_order_forks_reject() {
    for (first, second) in [(b'b', b'a'), (b'a', b'a')] {
        let mut table = vec![0x02, 0x00, first, 0x00, 0x00, second, 0x22, 0x00];
        table.extend(record32(1));
        table.extend(record32(2));
        assert!(matches!(
            Node::<V1>::decode(&payload(0x00, &table)),
            Err(DecodeError::ForkIndexOrder)
        ));
    }
}

// Non-cumulative offsets and padded record spans must fail to decode, never
// silently repack.
#[test]
fn non_minimal_fork_layout_rejects() {
    // A nonzero first offset.
    let mut table = vec![0x01, 0x00, b'a', 0x01, 0x00];
    table.extend(record32(1));
    assert!(matches!(
        Node::<V1>::decode(&payload(0x00, &table)),
        Err(DecodeError::ForkOffsets)
    ));

    // A padding byte between records.
    let mut table = vec![0x01, 0x00, b'a', 0x00, 0x00];
    table.extend(record32(1));
    table.push(0x00);
    assert!(matches!(
        Node::<V1>::decode(&payload(0x00, &table)),
        Err(DecodeError::RecordSpan {
            span: 35,
            consumed: 34
        })
    ));
}

// Non-minimal metadata must fail to decode, never silently sort, dedupe or
// intern.
#[test]
fn non_minimal_metadata_rejects() {
    // Pairs out of wire order, then duplicated.
    for (first, second) in [(0x02, 0x01), (0x01, 0x01)] {
        let block = [0x06, 0x00, first, 0x00, 0x00, second, 0x00, 0x00];
        assert!(matches!(
            Node::<V1>::decode(&with_meta(&block)),
            Err(DecodeError::MetadataOrder)
        ));
    }

    // A registered name behind the custom-key escape: the id is the only
    // canonical spelling.
    let mut block = vec![0x0C, 0x00, 0xFF, 0x08];
    block.extend_from_slice(b"filename");
    block.extend_from_slice(&[0x00, 0x00]);
    assert!(matches!(
        Node::<V1>::decode(&with_meta(&block)),
        Err(DecodeError::CustomKey(CustomKeyError::Registered(
            KeyId::Filename
        )))
    ));
}

// In-band nulls must fail to decode: absence travels as an unset flag or a
// format discriminant, never as an empty in-band object.
#[test]
fn in_band_null_misuse_rejects() {
    // An empty metadata block behind HAS_META.
    assert!(matches!(
        Node::<V1>::decode(&with_meta(&[0x00, 0x00])),
        Err(DecodeError::MetadataEmpty)
    ));

    // An empty prefix: plen counts the index key byte, so zero is void.
    let mut table = vec![0x01, 0x00, b'a', 0x00, 0x00, 0x01, 0x00];
    table.extend_from_slice(&[0x22; 32]);
    assert!(matches!(
        Node::<V1>::decode(&payload(0x00, &table)),
        Err(DecodeError::EmptyPrefix(_))
    ));

    // A fork with neither entry nor child.
    let table = [0x01, 0x00, b'a', 0x00, 0x00, 0x10, 0x01];
    assert!(matches!(
        Node::<V1>::decode(&payload(0x00, &table)),
        Err(DecodeError::ForkFlags(0x10))
    ));

    // An embedded child with no forks: an empty node is never embedded.
    let table = [
        0x01, 0x00, b'a', 0x00, 0x00, 0x0C, 0x01, 0x03, 0x00, 0x00, 0x00, 0x00,
    ];
    assert!(matches!(
        Node::<V1>::decode(&payload(0x00, &table)),
        Err(DecodeError::EmbeddedEmpty)
    ));
}

// Padding after the body must fail to decode, never be skipped.
#[test]
fn trailing_bytes_reject() {
    assert!(matches!(
        Node::<V1>::decode(&[0x6D, 0x01, 0x00, 0x00, 0x00, 0xFF]),
        Err(DecodeError::Trailing(1))
    ));
}
