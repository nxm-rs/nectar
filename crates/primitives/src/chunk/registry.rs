//! Compile-time chunk type registry
//!
//! This module provides the [`ChunkRegistry`] trait: the closed envelope type
//! IS the type-level set of chunk types a network accepts. [`StandardChunkSet`]
//! and [`ContentOnlyChunkSet`] are the built-in registries.

use bytes::Bytes;

use crate::error::Result;

use super::address::ChunkAddress;
use super::any_chunk::AnyChunk;
use super::content::{CacHeader, ContentChunk};
use super::error::ChunkError;
use super::single_owner::SocHeader;
use super::traits::{ChunkHeader, ChunkOps};
use super::type_id::ChunkTypeId;
use super::type_tag::ChunkTypeTag;

/// Inspectable metadata of one registry member: its tag, name, and wire
/// header width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkTypeInfo {
    /// Versioned `(id, version)` tag of the member's acceptance rule.
    pub tag: ChunkTypeTag,
    /// Human-readable type name.
    pub name: &'static str,
    /// Exact wire width of the member's header in bytes.
    pub header_size: usize,
}

impl ChunkTypeInfo {
    /// Build the metadata entry for a chunk header type.
    pub const fn of<H: ChunkHeader>() -> Self {
        Self {
            tag: ChunkTypeTag::new(H::TYPE_ID, H::VERSION),
            name: H::NAME,
            header_size: H::SIZE,
        }
    }

    /// Find a tag shared by two entries of `members`, if any. `const` so
    /// registries can assert tag uniqueness at compile time
    /// ([`ChunkRegistry::DISTINCT_TAGS`]).
    pub const fn duplicate_tag(members: &[Self]) -> Option<ChunkTypeTag> {
        let mut rest = members;
        while let [head, tail @ ..] = rest {
            let mut probe = tail;
            while let [candidate, remainder @ ..] = probe {
                if head.tag.to_u16() == candidate.tag.to_u16() {
                    return Some(head.tag);
                }
                probe = remainder;
            }
            rest = tail;
        }
        None
    }
}

/// Compile-time registry of the chunk types one network accepts.
///
/// The closed envelope type is the type-level set: [`Envelope`](Self::Envelope)
/// carries the body size, so no const generic appears on the trait, and
/// [`MEMBERS`](Self::MEMBERS) is its inspectable description. Implementations
/// are hand-written per network; force the duplicate-tag guard with a
/// `const _: () = MyRegistry::DISTINCT_TAGS;` item next to the impl.
///
/// # Example
///
/// ```
/// use nectar_primitives::{AnyChunk, ChunkOps, ChunkRegistry, ContentChunk, StandardChunkSet};
///
/// let content = ContentChunk::new(&b"hello registry"[..]).unwrap();
/// let address = *content.address();
/// let any: AnyChunk = content.into();
///
/// let encoded = StandardChunkSet::encode_typed(&any);
/// let decoded = StandardChunkSet::decode_typed(&address, &encoded).unwrap();
/// assert_eq!(decoded.address(), any.address());
/// ```
pub trait ChunkRegistry: Send + Sync + 'static {
    /// The closed envelope this registry decodes into. Its body size is a
    /// fact of the envelope type, not a trait generic.
    type Envelope: ChunkOps + Clone + Send + Sync + 'static;

    /// The registry's members, one entry per accepted chunk type.
    const MEMBERS: &'static [ChunkTypeInfo];

    /// Compile-time duplicate-tag guard: evaluating this const fails the
    /// build when two [`MEMBERS`](Self::MEMBERS) entries share a tag.
    const DISTINCT_TAGS: () = assert!(
        ChunkTypeInfo::duplicate_tag(Self::MEMBERS).is_none(),
        "duplicate chunk type tag in registry MEMBERS"
    );

    /// Whether this registry accepts the exact `(id, version)` tag.
    fn supports(tag: ChunkTypeTag) -> bool {
        Self::MEMBERS.iter().any(|member| member.tag == tag)
    }

    /// Whether this registry accepts any version of `id`.
    fn supports_id(id: ChunkTypeId) -> bool {
        Self::MEMBERS.iter().any(|member| member.tag.id == id)
    }

    /// Decode the self-describing typed form produced by
    /// [`encode_typed`](Self::encode_typed): the tag routes to a member,
    /// the address certifies the payload.
    ///
    /// # Errors
    ///
    /// A tag outside [`MEMBERS`](Self::MEMBERS) is a distinct
    /// [`ChunkError::UnsupportedTag`] carrying the tag, never a format
    /// error; a payload that fails the member's acceptance rule errors.
    fn decode_typed(address: &ChunkAddress, bytes: &[u8]) -> Result<Self::Envelope>;

    /// Decode bare wire bytes (no type tag), trial-parsing members in
    /// declaration order; the address is the disambiguator.
    ///
    /// # Errors
    ///
    /// Errors when no member's interpretation of `data` certifies against
    /// `address`.
    fn decode_wire(address: &ChunkAddress, data: Bytes) -> Result<Self::Envelope>;

    /// Encode into the typed form [`decode_typed`](Self::decode_typed)
    /// accepts: the member's two-byte tag, then its bare wire bytes.
    fn encode_typed(chunk: &Self::Envelope) -> Vec<u8>;
}

/// Standard Swarm registry: content-addressed and single-owner chunks at the
/// default body size, carried in [`AnyChunk`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StandardChunkSet;

impl ChunkRegistry for StandardChunkSet {
    type Envelope = AnyChunk;

    const MEMBERS: &'static [ChunkTypeInfo] = &[
        ChunkTypeInfo::of::<CacHeader>(),
        ChunkTypeInfo::of::<SocHeader>(),
    ];

    fn decode_typed(address: &ChunkAddress, bytes: &[u8]) -> Result<Self::Envelope> {
        AnyChunk::from_typed_bytes(address, bytes)
    }

    fn decode_wire(address: &ChunkAddress, data: Bytes) -> Result<Self::Envelope> {
        AnyChunk::from_wire_bytes(address, data)
    }

    fn encode_typed(chunk: &Self::Envelope) -> Vec<u8> {
        chunk.to_typed_bytes()
    }
}

const _: () = StandardChunkSet::DISTINCT_TAGS;

/// Registry that accepts only content-addressed chunks, carried directly as
/// [`ContentChunk`]: a single-member set needs no envelope enum.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContentOnlyChunkSet;

impl ChunkRegistry for ContentOnlyChunkSet {
    type Envelope = ContentChunk;

    const MEMBERS: &'static [ChunkTypeInfo] = &[ChunkTypeInfo::of::<CacHeader>()];

    fn decode_typed(address: &ChunkAddress, bytes: &[u8]) -> Result<Self::Envelope> {
        let (tag, payload) = bytes.split_first_chunk::<2>().ok_or_else(|| {
            ChunkError::invalid_format("typed-chunk encoding shorter than the two-byte type tag")
        })?;
        let tag = ChunkTypeTag::from(*tag);
        if !Self::supports(tag) {
            return Err(ChunkError::unsupported_tag(tag).into());
        }
        let chunk = ContentChunk::try_from(Bytes::copy_from_slice(payload))?;
        chunk.verify(address)?;
        Ok(chunk)
    }

    fn decode_wire(address: &ChunkAddress, data: Bytes) -> Result<Self::Envelope> {
        let chunk = ContentChunk::try_from(data)?;
        chunk.verify(address)?;
        Ok(chunk)
    }

    fn encode_typed(chunk: &Self::Envelope) -> Vec<u8> {
        let tag = ChunkTypeInfo::of::<CacHeader>().tag.to_bytes();
        let wire = chunk.clone().into_bytes();
        let mut out = Vec::with_capacity(wire.len().saturating_add(tag.len()));
        out.extend_from_slice(&tag);
        out.extend_from_slice(&wire);
        out
    }
}

const _: () = ContentOnlyChunkSet::DISTINCT_TAGS;

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::super::single_owner::SingleOwnerChunk;
    use super::super::type_tag::ChunkVersion;
    use super::*;
    use crate::bmt::DEFAULT_BODY_SIZE;
    use crate::error::PrimitivesError;

    type DefaultContentChunk = ContentChunk<DEFAULT_BODY_SIZE>;

    const CAC_TAG: ChunkTypeTag = ChunkTypeTag::new(CacHeader::TYPE_ID, CacHeader::VERSION);
    const SOC_TAG: ChunkTypeTag = ChunkTypeTag::new(SocHeader::TYPE_ID, SocHeader::VERSION);

    fn test_signer() -> alloy_signer_local::PrivateKeySigner {
        // Fixed key so addresses are deterministic across runs.
        let pk = [0x42u8; 32];
        alloy_signer_local::PrivateKeySigner::from_slice(&pk).unwrap()
    }

    fn sample_single_owner() -> SingleOwnerChunk<DEFAULT_BODY_SIZE> {
        let id = crate::SocId::ZERO;
        SingleOwnerChunk::new(id, b"single owner payload".to_vec(), &test_signer()).unwrap()
    }

    #[test]
    fn standard_supports_exact_tags() {
        assert!(StandardChunkSet::supports(CAC_TAG));
        assert!(StandardChunkSet::supports(SOC_TAG));
        // An unknown version of a known id is a distinct acceptance rule.
        assert!(!StandardChunkSet::supports(ChunkTypeTag::new(
            ChunkTypeId::CONTENT,
            ChunkVersion::new(1)
        )));
        assert!(!StandardChunkSet::supports(ChunkTypeTag::new(
            ChunkTypeId::custom(200),
            ChunkVersion::new(0)
        )));
    }

    #[test]
    fn standard_supports_ids() {
        assert!(StandardChunkSet::supports_id(ChunkTypeId::CONTENT));
        assert!(StandardChunkSet::supports_id(ChunkTypeId::SINGLE_OWNER));
        assert!(!StandardChunkSet::supports_id(ChunkTypeId::custom(200)));
        assert!(!StandardChunkSet::supports_id(ChunkTypeId::new(50)));
    }

    #[test]
    fn standard_members_are_inspectable() {
        let members = StandardChunkSet::MEMBERS;
        assert_eq!(members.len(), 2);
        assert_eq!(members[0].tag, CAC_TAG);
        assert_eq!(members[0].name, "content");
        assert_eq!(members[0].header_size, 0);
        assert_eq!(members[1].tag, SOC_TAG);
        assert_eq!(members[1].name, "single_owner");
        assert_eq!(members[1].header_size, 97);
    }

    #[test]
    fn content_only_supports() {
        assert!(ContentOnlyChunkSet::supports(CAC_TAG));
        assert!(!ContentOnlyChunkSet::supports(SOC_TAG));
        assert!(ContentOnlyChunkSet::supports_id(ChunkTypeId::CONTENT));
        assert!(!ContentOnlyChunkSet::supports_id(ChunkTypeId::SINGLE_OWNER));
        assert_eq!(ContentOnlyChunkSet::MEMBERS.len(), 1);
    }

    #[test]
    fn duplicate_tag_scan() {
        assert_eq!(ChunkTypeInfo::duplicate_tag(&[]), None);
        assert_eq!(
            ChunkTypeInfo::duplicate_tag(StandardChunkSet::MEMBERS),
            None
        );
        assert_eq!(
            ChunkTypeInfo::duplicate_tag(ContentOnlyChunkSet::MEMBERS),
            None
        );

        let dup = [
            ChunkTypeInfo::of::<CacHeader>(),
            ChunkTypeInfo::of::<SocHeader>(),
            ChunkTypeInfo::of::<CacHeader>(),
        ];
        assert_eq!(ChunkTypeInfo::duplicate_tag(&dup), Some(CAC_TAG));
    }

    #[test]
    fn standard_typed_round_trip() {
        let content = DefaultContentChunk::new(&b"hello registry"[..]).unwrap();
        let address = *content.address();
        let any: AnyChunk = content.into();

        let encoded = StandardChunkSet::encode_typed(&any);
        assert_eq!(encoded[..2], [0, 0], "CONTENT tag must be id 0, version 0");

        let decoded = StandardChunkSet::decode_typed(&address, &encoded).unwrap();
        assert!(decoded.is_content());
        assert_eq!(decoded.address(), any.address());
        assert_eq!(decoded.data(), any.data());
    }

    #[test]
    fn standard_typed_unknown_tag_is_unsupported_not_invalid() {
        let tag = ChunkTypeTag::new(ChunkTypeId::custom(200), ChunkVersion::new(0));
        let address: ChunkAddress = [0x11u8; 32].into();
        let mut encoded = tag.to_bytes().to_vec();
        encoded.extend_from_slice(b"opaque payload");

        let err = StandardChunkSet::decode_typed(&address, &encoded).unwrap_err();
        match err {
            PrimitivesError::Chunk(ChunkError::UnsupportedTag(t)) => assert_eq!(t, tag),
            other => panic!("expected UnsupportedTag, got {other:?}"),
        }
    }

    #[test]
    fn standard_wire_round_trip() {
        let soc = sample_single_owner();
        let address = *soc.address();
        let any: AnyChunk = soc.into();
        let wire = any.clone().into_bytes();

        let decoded = StandardChunkSet::decode_wire(&address, wire.clone()).unwrap();
        assert!(decoded.is_single_owner());
        assert_eq!(decoded.address(), any.address());
        assert_eq!(decoded.into_bytes(), wire);
    }

    #[test]
    fn standard_wire_empty_bytes_fails() {
        let result = StandardChunkSet::decode_wire(&ChunkAddress::default(), Bytes::new());
        assert!(result.is_err());
    }

    #[test]
    fn content_only_typed_round_trip() {
        let content = DefaultContentChunk::new(&b"content only"[..]).unwrap();
        let address = *content.address();

        let encoded = ContentOnlyChunkSet::encode_typed(&content);
        // The typed form must agree with the standard registry's encoding.
        assert_eq!(encoded, StandardChunkSet::encode_typed(&content.into()));

        let decoded = ContentOnlyChunkSet::decode_typed(&address, &encoded).unwrap();
        assert_eq!(*decoded.address(), address);
    }

    #[test]
    fn content_only_typed_rejects_soc_tag_as_unsupported() {
        let soc = sample_single_owner();
        let address = *soc.address();
        let encoded = StandardChunkSet::encode_typed(&soc.into());

        let err = ContentOnlyChunkSet::decode_typed(&address, &encoded).unwrap_err();
        match err {
            PrimitivesError::Chunk(ChunkError::UnsupportedTag(t)) => assert_eq!(t, SOC_TAG),
            other => panic!("expected UnsupportedTag, got {other:?}"),
        }
    }

    #[test]
    fn content_only_typed_short_input_errors() {
        let address: ChunkAddress = [0u8; 32].into();
        assert!(ContentOnlyChunkSet::decode_typed(&address, &[]).is_err());
        assert!(ContentOnlyChunkSet::decode_typed(&address, &[0]).is_err());
    }

    #[test]
    fn content_only_typed_address_mismatch_errors() {
        let content = DefaultContentChunk::new(&b"chunk A"[..]).unwrap();
        let encoded = ContentOnlyChunkSet::encode_typed(&content);

        let wrong: ChunkAddress = [0xFFu8; 32].into();
        assert!(ContentOnlyChunkSet::decode_typed(&wrong, &encoded).is_err());
    }

    #[test]
    fn content_only_wire_round_trip() {
        let content = DefaultContentChunk::new(&b"bare wire"[..]).unwrap();
        let address = *content.address();
        let wire: Bytes = content.clone().into_bytes();

        let decoded = ContentOnlyChunkSet::decode_wire(&address, wire).unwrap();
        assert_eq!(*decoded.address(), address);
        assert_eq!(decoded.data(), content.data());
    }

    #[test]
    fn content_only_wire_rejects_soc_bytes() {
        // A SOC's wire bytes parse structurally as a CAC (span plus payload)
        // but derive a different address, so certification fails.
        let soc = sample_single_owner();
        let address = *soc.address();
        let wire = soc.into_bytes();

        assert!(ContentOnlyChunkSet::decode_wire(&address, wire).is_err());
    }

    /// Mirrors the body of the `chunk_decode` fuzz target: run the input
    /// through every decode entry point the fuzzer drives and force the lazy
    /// address/owner computations. The fuzz oracle is "no panic"; `Err` is an
    /// acceptable outcome for arbitrary bytes.
    fn exercise_chunk_decode(data: &[u8]) -> Result<AnyChunk<DEFAULT_BODY_SIZE>> {
        let bytes = Bytes::copy_from_slice(data);

        // Address-mismatch arm: the zero address matches (almost) no input,
        // so both trial parses and their address computations run to `Err`.
        let _ =
            AnyChunk::<DEFAULT_BODY_SIZE>::from_wire_bytes(&ChunkAddress::default(), bytes.clone());

        let content = ContentChunk::<DEFAULT_BODY_SIZE>::try_from(data);
        let soc = SingleOwnerChunk::<DEFAULT_BODY_SIZE>::try_from(data);
        if let Ok(soc) = &soc {
            // ECDSA public-key recovery over bytes 32..97 must not panic.
            let _ = soc.owner();
            let _ = soc.address();
        }

        // Ok arm: key the wire decoder by the address of whichever direct
        // parse succeeded, CAC first (the same trial order the decoder uses).
        let address = content
            .ok()
            .map(|c| *c.address())
            .or_else(|| soc.ok().map(|s| *s.address()))
            .ok_or_else(|| ChunkError::invalid_format("no structural parse"))?;
        let result = AnyChunk::from_wire_bytes(&address, bytes);
        if let Ok(chunk) = &result {
            let _ = chunk.address();
        }
        result
    }

    /// Replay crafted edge inputs through the exact entry points the
    /// `chunk_decode` fuzz target exercises: length boundaries around the
    /// 8-byte span, the 97-byte SOC id+signature header, and the maximum
    /// CAC/SOC encodings, in all-zero and all-0xff flavours.
    #[test]
    fn chunk_decode_edge_inputs_do_not_panic() {
        let edge_inputs: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0x00],
            vec![0xff; 7],                              // one short of a CAC span
            vec![0x00; 8],                              // zero span, empty payload
            vec![0xff; 8],                              // max span, empty payload
            vec![0xff; 96],                             // one short of the SOC header
            vec![0xff; 97],                             // SOC header, no body
            vec![0xff; 105],                            // SOC header + span, empty payload
            vec![0xff; 8 + DEFAULT_BODY_SIZE],          // max CAC encoding
            vec![0xff; 8 + DEFAULT_BODY_SIZE + 1],      // one past max CAC
            vec![0x00; 97 + 8 + DEFAULT_BODY_SIZE],     // max SOC encoding
            vec![0xff; 97 + 8 + DEFAULT_BODY_SIZE + 1], // one past max SOC
        ];
        for data in &edge_inputs {
            let _ = exercise_chunk_decode(data);
        }
    }

    /// Replay the committed seed corpus of the `chunk_decode` fuzz target
    /// (`fuzz/seeds/chunk_decode/`). Seed intent is pinned by name:
    /// `valid-*` must deserialize `Ok` (and `valid-soc-*` must also decode as
    /// a SOC directly), `invalid-*` must stay `Err`. This keeps the fuzz
    /// seeds meaningful on stable without running the fuzzer itself.
    #[test]
    fn seed_replay_chunk_decode() {
        let seed_dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/seeds/chunk_decode");
        let mut replayed = 0usize;
        for entry in std::fs::read_dir(&seed_dir)
            .unwrap_or_else(|e| panic!("seed dir {} must exist: {e}", seed_dir.display()))
        {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            let data = std::fs::read(&path).unwrap();

            let result = exercise_chunk_decode(&data);

            if name.starts_with("valid-") {
                assert!(result.is_ok(), "seed {name} must deserialize successfully");
            } else if name.starts_with("invalid-") {
                assert!(result.is_err(), "seed {name} must remain an Err input");
            }
            if name.starts_with("valid-soc-") {
                assert!(
                    SingleOwnerChunk::<DEFAULT_BODY_SIZE>::try_from(data.as_slice()).is_ok(),
                    "seed {name} must decode as a single-owner chunk"
                );
            }
            replayed += 1;
        }
        assert!(
            replayed >= 4,
            "expected at least the 4 curated seeds, found {replayed}"
        );
    }
}
