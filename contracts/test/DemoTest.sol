// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {Harness} from "./Harness.sol";

/// Fixture loading for the demo-contract suites: the byte readers and the three
/// typed fixture parsers the deterministic nectar-manifest-proof generator
/// emits. Kept apart from the library suite so each stays a single import.
abstract contract DemoTest is Harness {
    /// A single-key fixture: the verifier inputs plus the raw proof wire the
    /// demo contracts decode.
    struct KeyFixture {
        bytes32 root;
        bytes key;
        bool present;
        bytes value;
        bytes proofBytes;
    }

    /// A range-completeness fixture: the bounds, the expected listing digest and
    /// count, and the frontier node payloads.
    struct RangeFixture {
        bytes32 root;
        bytes lo;
        bytes hi;
        bytes32 digest;
        uint256 count;
        bytes[] nodes;
    }

    /// A state-transition fixture: the two roots, the key and value, and the two
    /// proof halves (exclusion under the prior root, inclusion under the new).
    struct TransitionFixture {
        bytes32 rootBefore;
        bytes32 rootAfter;
        bytes key;
        bytes value;
        bytes beforeBytes;
        bytes afterBytes;
    }

    string internal constant DIR = "test/fixtures/";

    /// The raw bytes of a fixture file.
    function _read(string memory name) internal view returns (bytes memory) {
        return vm.readFileBinary(string.concat(DIR, name));
    }

    /// Parse a single-key fixture: root[32], u32 keyLen || key, u8 present,
    /// u32 valueLen || value, then the proof wire.
    function _keyFixture(string memory name) internal view returns (KeyFixture memory fx) {
        bytes memory raw = _read(name);
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
        fx.proofBytes = _slice(raw, cur, raw.length - cur);
    }

    /// Parse a range fixture: root[32], u32 loLen || lo, u32 hiLen || hi,
    /// digest[32], u32 count, u32 nNodes, then (u32 payloadLen || payload) each.
    function _rangeFixture(string memory name) internal view returns (RangeFixture memory fx) {
        bytes memory raw = _read(name);
        uint256 cur = 0;
        fx.root = _word(raw, cur);
        cur += 32;
        uint256 loLen = _u32be(raw, cur);
        cur += 4;
        fx.lo = _slice(raw, cur, loLen);
        cur += loLen;
        uint256 hiLen = _u32be(raw, cur);
        cur += 4;
        fx.hi = _slice(raw, cur, hiLen);
        cur += hiLen;
        fx.digest = _word(raw, cur);
        cur += 32;
        fx.count = _u32be(raw, cur);
        cur += 4;
        uint256 nNodes = _u32be(raw, cur);
        cur += 4;
        fx.nodes = new bytes[](nNodes);
        for (uint256 i = 0; i < nNodes; i++) {
            uint256 plen = _u32be(raw, cur);
            cur += 4;
            fx.nodes[i] = _slice(raw, cur, plen);
            cur += plen;
        }
    }

    /// Parse a transition fixture: rootBefore[32], rootAfter[32], u32 keyLen ||
    /// key, u32 valueLen || value, u32 beforeLen || before, u32 afterLen ||
    /// after.
    function _transitionFixture(string memory name) internal view returns (TransitionFixture memory fx) {
        bytes memory raw = _read(name);
        uint256 cur = 0;
        fx.rootBefore = _word(raw, cur);
        cur += 32;
        fx.rootAfter = _word(raw, cur);
        cur += 32;
        uint256 keyLen = _u32be(raw, cur);
        cur += 4;
        fx.key = _slice(raw, cur, keyLen);
        cur += keyLen;
        uint256 valueLen = _u32be(raw, cur);
        cur += 4;
        fx.value = _slice(raw, cur, valueLen);
        cur += valueLen;
        uint256 beforeLen = _u32be(raw, cur);
        cur += 4;
        fx.beforeBytes = _slice(raw, cur, beforeLen);
        cur += beforeLen;
        uint256 afterLen = _u32be(raw, cur);
        cur += 4;
        fx.afterBytes = _slice(raw, cur, afterLen);
        cur += afterLen;
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
