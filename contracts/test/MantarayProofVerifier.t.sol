// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {Harness} from "./Harness.sol";
import {MantarayProofVerifier, Proof} from "../src/MantarayProofVerifier.sol";

/// On-chain replay of deterministic fixtures emitted by the
/// nectar-manifest-proof generator. Each fixture bundles root, key, presence,
/// value and the segment-granularity proof in the byte layout the verifier
/// consumes; the tests load them with `vm.readFileBinary`, never hand-authoring
/// proof bytes.
contract MantarayProofVerifierTest is Harness {
    /// A parsed fixture: the verifier inputs plus the recorded presence.
    struct Fixture {
        bytes32 root;
        bytes key;
        bool present;
        bytes value;
        Proof proof;
    }

    string internal constant DIR = "test/fixtures/";

    // Inclusion over the single embedded node.
    function testInclusionEmbedded() public view {
        Fixture memory fx = _load("incl_embedded.bin");
        assertTrue(fx.present, "fixture is an inclusion case");
        assertTrue(
            MantarayProofVerifier.verifyInclusion(fx.root, fx.key, fx.value, fx.proof), "present key must verify"
        );
        // The same proof cannot attest absence of a present key.
        assertFalse(MantarayProofVerifier.verifyExclusion(fx.root, fx.key, fx.proof), "present key is not absent");
    }

    // Inclusion across a referenced hop into a spilled child chunk.
    function testInclusionReferenced() public view {
        Fixture memory fx = _load("incl_referenced.bin");
        assertTrue(
            MantarayProofVerifier.verifyInclusion(fx.root, fx.key, fx.value, fx.proof),
            "present key across a hop must verify"
        );
    }

    // Every exclusion shape: gap, divergence, key exhausted inside an edge, the
    // empty key, and absence across a referenced hop.
    function testExclusionGap() public view {
        _assertAbsent("excl_gap.bin");
    }

    function testExclusionDivergent() public view {
        _assertAbsent("excl_divergent.bin");
    }

    function testExclusionExhausted() public view {
        _assertAbsent("excl_exhausted.bin");
    }

    function testExclusionEmptyKey() public view {
        _assertAbsent("excl_empty.bin");
    }

    function testExclusionReferenced() public view {
        _assertAbsent("excl_referenced.bin");
    }

    // A single flipped segment byte breaks BMT authentication.
    function testTamperedProofRejected() public view {
        Fixture memory fx = _load("tampered.bin");
        assertFalse(
            MantarayProofVerifier.verifyInclusion(fx.root, fx.key, fx.value, fx.proof), "tampered proof must reject"
        );
    }

    // A wrong root breaks the very first hop, for both verdicts.
    function testWrongRootRejected() public view {
        Fixture memory fx = _load("incl_embedded.bin");
        bytes32 wrong = fx.root ^ bytes32(uint256(1));
        assertFalse(
            MantarayProofVerifier.verifyInclusion(wrong, fx.key, fx.value, fx.proof), "wrong root must reject inclusion"
        );
        assertFalse(MantarayProofVerifier.verifyExclusion(wrong, fx.key, fx.proof), "wrong root must reject exclusion");
    }

    // The authenticated entry must equal the claimed value.
    function testWrongValueRejected() public view {
        Fixture memory fx = _load("incl_embedded.bin");
        bytes memory wrong = fx.value;
        wrong[0] = bytes1(uint8(wrong[0]) ^ 0xFF);
        assertFalse(MantarayProofVerifier.verifyInclusion(fx.root, fx.key, wrong, fx.proof), "wrong value must reject");
    }

    /// Assert an exclusion fixture verifies absent and refuses an inclusion.
    function _assertAbsent(string memory name) internal view {
        Fixture memory fx = _load(name);
        assertFalse(fx.present, "fixture is an exclusion case");
        assertTrue(MantarayProofVerifier.verifyExclusion(fx.root, fx.key, fx.proof), "absent key must verify absent");
        assertFalse(
            MantarayProofVerifier.verifyInclusion(fx.root, fx.key, fx.value, fx.proof), "absent key admits no inclusion"
        );
    }

    /// Load and parse a fixture file.
    ///
    /// Framing (big-endian integers): root[32], u32 keyLen || key, u8 present,
    /// u32 valueLen || value, then the proof wire.
    function _load(string memory name) internal view returns (Fixture memory fx) {
        bytes memory raw = vm.readFileBinary(string.concat(DIR, name));
        uint256 cur = 0;
        fx.root = _word(raw, cur);
        cur += 32;
        uint256 keyLen = _u32be(raw, cur);
        cur += 4;
        fx.key = _slice(raw, cur, keyLen);
        cur += keyLen;
        fx.present = uint8(raw[cur]) == 1;
        cur += 1;
        uint256 valueLen = _u32be(raw, cur);
        cur += 4;
        fx.value = _slice(raw, cur, valueLen);
        cur += valueLen;
        fx.proof = MantarayProofVerifier.decode(_slice(raw, cur, raw.length - cur));
    }

    function _u32be(bytes memory buf, uint256 off) internal pure returns (uint256 v) {
        for (uint256 i = 0; i < 4; i++) {
            v = (v << 8) | uint256(uint8(buf[off + i]));
        }
    }

    function _word(bytes memory buf, uint256 off) internal pure returns (bytes32 out) {
        assembly {
            out := mload(add(add(buf, 32), off))
        }
    }

    function _slice(bytes memory buf, uint256 off, uint256 n) internal pure returns (bytes memory out) {
        out = new bytes(n);
        for (uint256 i = 0; i < n; i++) {
            out[i] = buf[off + i];
        }
    }
}
