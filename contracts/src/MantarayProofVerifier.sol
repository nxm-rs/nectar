// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

/// A contiguous run's single BMT segment: 32 raw node bytes behind the seven
/// sibling hashes that authenticate it against the node address.
struct Segment {
    bytes32 data;
    bytes32[7] siblings;
}

/// One authenticated node on the descent path, shipped as the leading segments
/// the descent reads (from index zero) plus the BMT span they share.
struct Step {
    uint64 span;
    Segment[] segments;
}

/// The authenticated descent path for a key under a root: the ordered nodes the
/// descent visits, root first.
struct Proof {
    Step[] steps;
}

/// On-chain inclusion and exclusion verification for mantaray 1.0 manifests at
/// BMT-segment granularity, against a trusted root address.
///
/// The verifier follows the key itself and accepts a node only when its address
/// equals the reference the previous authenticated hop supplied, so a whole
/// proof is a BMT hash chain anchored at the root. It re-hashes the shipped
/// leading segments rather than parsing whole nodes: each segment is
/// authenticated by its sibling path, the covered prefix is reassembled, and
/// the node-body grammar the descent reads (preamble, flags, root entry, fork
/// index, the followed record) is replayed over it. Soundness rests on
/// canonicalness (spec 6.2): an authenticated node's fork set is complete and
/// its edges maximal, so local absence is global absence.
///
/// Restricted profile: frozen tag_version 0x01, plaintext 32-byte references.
/// A referenced-child fork trails an author-asserted subtree count; the descent
/// bounds records by the fork-index offsets, so the count bytes are crossed but
/// never read, present-but-skipped, not verified. An encrypted 64-byte child
/// reference terminates verification as unsupported rather than being followed;
/// the 0x02 read profile, spilled segment-directory nodes, and encrypted
/// subtrees are out of scope and would each need their own descent extension.
library MantarayProofVerifier {
    // Node-body grammar (spec 5.1). Flags byte bit layout.
    uint8 private constant ENTRY_MASK = 0x03; // bits 0-1: entry presence/width
    uint8 private constant CHILD_MASK = 0x0C; // bits 2-3: child presence/width
    uint8 private constant HAS_META = 0x10; // bit 4: a metadata block follows
    uint8 private constant SEGMENTED = 0x20; // bit 5: fork table is a directory
    uint8 private constant SEGMENT = 0x40; // bit 6: body is a segment

    // Width discriminants shared by the entry (bits 0-1) and child (bits 2-3)
    // positions, read after masking and shifting to the low two bits.
    uint8 private constant W_NONE = 0;
    uint8 private constant W_REF32 = 1;
    uint8 private constant W_REF64 = 2;
    uint8 private constant W_INLINE = 3;

    // Descent outcomes.
    uint8 private constant K_FOUND = 0;
    uint8 private constant K_FOLLOW = 1;
    uint8 private constant K_ABSENT = 2;
    uint8 private constant K_ENCRYPTED = 3;
    uint8 private constant K_INVALID = 4;

    // Walk terminal states.
    uint8 private constant S_PRESENT = 0;
    uint8 private constant S_ABSENT = 1;
    uint8 private constant S_FAIL = 2;

    // The frozen preamble: magic 'm' then version 0x01.
    bytes2 private constant PREAMBLE = 0x6d01;

    /// Whether `key` maps to `value` under `root`, per `proof`.
    ///
    /// The value bytes are the authenticated entry as carried on the wire: the
    /// 32-byte reference for a ref32 entry, or the inline bytes for an inline
    /// entry.
    function verifyInclusion(bytes32 root, bytes memory key, bytes memory value, Proof memory proof)
        internal
        pure
        returns (bool)
    {
        (uint8 status, bytes memory found) = _walk(root, key, proof);
        return status == S_PRESENT && keccak256(found) == keccak256(value);
    }

    /// Whether `key` is provably absent under `root`, per `proof`.
    function verifyExclusion(bytes32 root, bytes memory key, Proof memory proof) internal pure returns (bool) {
        (uint8 status,) = _walk(root, key, proof);
        return status == S_ABSENT;
    }

    /// Replay the authenticated descent, returning the terminal state and, when
    /// present, the entry value.
    function _walk(bytes32 root, bytes memory key, Proof memory proof) private pure returns (uint8, bytes memory) {
        uint256 n = proof.steps.length;
        if (n == 0) {
            return (S_FAIL, "");
        }
        bytes32 trusted = root;
        uint256 pos = 0;
        for (uint256 i = 0; i < n; i++) {
            (bool ok, bytes memory buf) = _reassemble(proof.steps[i], trusted);
            if (!ok) {
                return (S_FAIL, "");
            }
            (uint8 kind, bytes32 child, uint256 newpos, bytes memory value) = _descend(buf, key, pos);
            bool last = i + 1 == n;
            if (kind == K_FOUND) {
                return last ? (S_PRESENT, value) : (S_FAIL, bytes(""));
            }
            if (kind == K_ABSENT) {
                return last ? (S_ABSENT, bytes("")) : (S_FAIL, bytes(""));
            }
            if (kind == K_FOLLOW) {
                if (last) {
                    return (S_FAIL, "");
                }
                trusted = child;
                pos = newpos;
                continue;
            }
            // Encrypted or invalid: neither present nor provably absent.
            return (S_FAIL, "");
        }
        return (S_FAIL, "");
    }

    /// Authenticate a contiguous leading run of segments against `trusted` and
    /// reassemble their bytes. The run must start at segment zero and leave no
    /// gap, so the reassembled prefix is anchored and contiguous.
    function _reassemble(Step memory step, bytes32 trusted) private pure returns (bool, bytes memory) {
        uint256 count = step.segments.length;
        if (count == 0) {
            return (false, "");
        }
        bytes memory buf = new bytes(count * 32);
        for (uint256 e = 0; e < count; e++) {
            Segment memory seg = step.segments[e];
            if (_bmtRoot(seg.data, seg.siblings, e, step.span) != trusted) {
                return (false, "");
            }
            bytes32 word = seg.data;
            assembly {
                mstore(add(add(buf, 32), mul(e, 32)), word)
            }
        }
        return (true, buf);
    }

    /// The BMT root of a chunk given one authenticated segment and its sibling
    /// path. Mirrors nectar's prefix-free content-chunk BMT: keccak256 up the
    /// binary tree of 128 segments, then keccak256 of the little-endian span and
    /// the tree root.
    function _bmtRoot(bytes32 data, bytes32[7] memory siblings, uint256 index, uint64 span)
        private
        pure
        returns (bytes32)
    {
        bytes32 h = data;
        for (uint256 level = 0; level < 7; level++) {
            bytes32 sib = siblings[level];
            if (index & 1 == 0) {
                h = keccak256(abi.encodePacked(h, sib));
            } else {
                h = keccak256(abi.encodePacked(sib, h));
            }
            index >>= 1;
        }
        return keccak256(abi.encodePacked(_spanLE(span), h));
    }

    /// The span as eight little-endian bytes, matching nectar's BMT span header.
    function _spanLE(uint64 v) private pure returns (bytes memory b) {
        b = new bytes(8);
        for (uint256 i = 0; i < 8; i++) {
            b[i] = bytes1(uint8(v >> (8 * i)));
        }
    }

    /// Descend one node over its authenticated bytes for `key` from `pos`.
    ///
    /// Reads only the leading node-body fields the trie follows; an embedded
    /// child rides in these same bytes and is crossed in place, so only a
    /// referenced edge yields a hop. Any read past the authenticated bytes is a
    /// short segment run, returned as invalid rather than reverting.
    function _descend(bytes memory buf, bytes memory key, uint256 pos)
        private
        pure
        returns (uint8 kind, bytes32 child, uint256 newpos, bytes memory value)
    {
        uint256 len = buf.length;
        if (len < 3) {
            return (K_INVALID, 0, 0, "");
        }
        if (bytes2(buf[0]) | (bytes2(buf[1]) >> 8) != PREAMBLE) {
            return (K_INVALID, 0, 0, "");
        }
        uint8 flags = uint8(buf[2]);
        if (flags & (SEGMENT | SEGMENTED) != 0) {
            // A spilled segment or segment directory is out of profile.
            return (K_INVALID, 0, 0, "");
        }
        uint256 cur = 3;

        // The root extension: an entry then a metadata block, each gated by the
        // flags. A fork-child node carries neither.
        bool ok;
        bytes memory rootEntry;
        bool hasEntry;
        (ok, cur, rootEntry, hasEntry) = _readEntry(buf, cur, flags & ENTRY_MASK);
        if (!ok) {
            return (K_INVALID, 0, 0, "");
        }
        if (flags & HAS_META != 0) {
            (bool mok, uint256 mcur) = _skipMeta(buf, cur);
            if (!mok) {
                return (K_INVALID, 0, 0, "");
            }
            cur = mcur;
        }

        // The empty key, or a key wholly consumed before this node, reads the
        // node's own value.
        if (pos >= key.length) {
            return hasEntry ? (K_FOUND, bytes32(0), pos, rootEntry) : (K_ABSENT, bytes32(0), pos, bytes(""));
        }
        return _descendTable(buf, cur, key, pos);
    }

    /// Walk `key` down the fork table rooted at `tableStart`, crossing embedded
    /// child tables in place and stopping at the first terminal, dead end, or
    /// referenced hop.
    function _descendTable(bytes memory buf, uint256 tableStart, bytes memory key, uint256 pos)
        private
        pure
        returns (uint8, bytes32, uint256, bytes memory)
    {
        uint256 len = buf.length;
        while (true) {
            if (pos >= key.length) {
                return (K_ABSENT, bytes32(0), 0, "");
            }
            uint8 wanted = uint8(key[pos]);

            // The fork index: a count then (first_byte, offset) slots. Absence of
            // a slot for the wanted byte is the compact gap that proves no fork
            // continues the key.
            uint256 cur = tableStart;
            if (cur + 2 > len) {
                return (K_INVALID, bytes32(0), 0, "");
            }
            uint256 fcount = _u16le(buf, cur);
            cur += 2;
            bool found = false;
            uint256 recordOff = 0;
            for (uint256 f = 0; f < fcount; f++) {
                if (cur + 3 > len) {
                    return (K_INVALID, bytes32(0), 0, "");
                }
                uint8 first = uint8(buf[cur]);
                uint256 off = _u16le(buf, cur + 1);
                cur += 3;
                if (first == wanted) {
                    found = true;
                    recordOff = off;
                }
            }
            uint256 recordsStart = cur;
            if (!found) {
                return (K_ABSENT, bytes32(0), 0, "");
            }

            // The record for the byte: flags, the tail behind its full-prefix
            // length, then the flag-gated entry and child.
            uint256 rc = recordsStart + recordOff;
            if (rc + 2 > len) {
                return (K_INVALID, bytes32(0), 0, "");
            }
            uint8 rflags = uint8(buf[rc]);
            uint8 plen = uint8(buf[rc + 1]);
            rc += 2;
            if (plen == 0) {
                return (K_INVALID, bytes32(0), 0, "");
            }
            uint256 tailLen = uint256(plen) - 1;
            if (rc + tailLen > len) {
                return (K_INVALID, bytes32(0), 0, "");
            }
            uint256 start = pos + 1;
            uint256 end = start + tailLen;
            // The compacted edge must match byte for byte; a divergence or a key
            // that ends inside the edge proves the key leaves the trie here.
            if (end > key.length || !_edgeMatches(buf, rc, key, start, tailLen)) {
                return (K_ABSENT, bytes32(0), 0, "");
            }
            rc += tailLen;
            uint256 newpos = end;

            (bool ok, uint256 ncur, bytes memory entry, bool hasEntry) = _readEntry(buf, rc, rflags & ENTRY_MASK);
            if (!ok) {
                return (K_INVALID, bytes32(0), 0, "");
            }
            rc = ncur;
            if (newpos >= key.length) {
                // The key ends at this fork: its own value, or nothing.
                return hasEntry ? (K_FOUND, bytes32(0), newpos, entry) : (K_ABSENT, bytes32(0), 0, bytes(""));
            }

            uint8 cw = (rflags & CHILD_MASK) >> 2;
            if (cw == W_NONE) {
                return (K_ABSENT, bytes32(0), 0, "");
            }
            if (cw == W_REF64) {
                return (K_ENCRYPTED, bytes32(0), 0, "");
            }
            if (cw == W_REF32) {
                if (rc + 32 > len) {
                    return (K_INVALID, bytes32(0), 0, "");
                }
                return (K_FOLLOW, _addr32(buf, rc), newpos, "");
            }
            // Embedded child body: its length, a forced zero flags byte, then its
            // own fork table, all in these bytes. Cross it in place.
            if (rc + 2 > len) {
                return (K_INVALID, bytes32(0), 0, "");
            }
            uint256 body = rc + 2;
            tableStart = body + 1;
            pos = newpos;
        }
        // Unreachable: the loop only exits via return.
        return (K_INVALID, bytes32(0), 0, "");
    }

    /// Read a width-tagged reference or value, advancing past it. Bytes are
    /// consumed regardless so the cursor lands on whatever follows.
    function _readEntry(bytes memory buf, uint256 cur, uint8 width)
        private
        pure
        returns (bool ok, uint256 next, bytes memory value, bool present)
    {
        uint256 len = buf.length;
        if (width == W_NONE) {
            return (true, cur, "", false);
        }
        if (width == W_REF32) {
            if (cur + 32 > len) {
                return (false, cur, "", false);
            }
            return (true, cur + 32, _range(buf, cur, 32), true);
        }
        if (width == W_REF64) {
            if (cur + 64 > len) {
                return (false, cur, "", false);
            }
            return (true, cur + 64, _range(buf, cur, 64), true);
        }
        // Inline: a one-byte length then that many bytes.
        if (cur + 1 > len) {
            return (false, cur, "", false);
        }
        uint256 vlen = uint8(buf[cur]);
        cur += 1;
        if (cur + vlen > len) {
            return (false, cur, "", false);
        }
        return (true, cur + vlen, _range(buf, cur, vlen), true);
    }

    /// Skip a metadata block: a little-endian u16 length then that many bytes.
    function _skipMeta(bytes memory buf, uint256 cur) private pure returns (bool, uint256) {
        uint256 len = buf.length;
        if (cur + 2 > len) {
            return (false, cur);
        }
        uint256 mlen = _u16le(buf, cur);
        cur += 2;
        if (cur + mlen > len) {
            return (false, cur);
        }
        return (true, cur + mlen);
    }

    /// A little-endian u16 read at `off`; the caller has bounds-checked.
    function _u16le(bytes memory buf, uint256 off) private pure returns (uint256) {
        return uint256(uint8(buf[off])) | (uint256(uint8(buf[off + 1])) << 8);
    }

    /// The 32-byte reference at `off`; the caller has bounds-checked.
    function _addr32(bytes memory buf, uint256 off) private pure returns (bytes32 out) {
        assembly {
            out := mload(add(add(buf, 32), off))
        }
    }

    /// Whether `buf[off..off+n]` equals `key[start..start+n]`; the caller has
    /// bounds-checked both ranges.
    function _edgeMatches(bytes memory buf, uint256 off, bytes memory key, uint256 start, uint256 n)
        private
        pure
        returns (bool)
    {
        for (uint256 i = 0; i < n; i++) {
            if (buf[off + i] != key[start + i]) {
                return false;
            }
        }
        return true;
    }

    /// A copy of `buf[off..off+n]`; the caller has bounds-checked.
    function _range(bytes memory buf, uint256 off, uint256 n) private pure returns (bytes memory out) {
        out = new bytes(n);
        for (uint256 i = 0; i < n; i++) {
            out[i] = buf[off + i];
        }
    }

    /// Decode the proof wire into a `Proof`. The layout is the Rust-to-Solidity
    /// byte contract the fixture generator emits and the counted on-chain
    /// verifier reuses:
    ///
    ///   proof   := u32 n_steps || step[n_steps]
    ///   step    := u64 span || u32 n_seg || segment[n_seg]
    ///   segment := data[32] || sibling[7][32]
    ///
    /// All framing integers are big-endian; the raw node bytes inside each
    /// segment keep their own little-endian u16 fields. Reverts on truncation.
    function decode(bytes memory raw) internal pure returns (Proof memory proof) {
        uint256 cur = 0;
        uint256 nSteps = _u32be(raw, cur);
        cur += 4;
        proof.steps = new Step[](nSteps);
        for (uint256 s = 0; s < nSteps; s++) {
            uint64 span = uint64(_u64be(raw, cur));
            cur += 8;
            uint256 nSeg = _u32be(raw, cur);
            cur += 4;
            Segment[] memory segments = new Segment[](nSeg);
            for (uint256 g = 0; g < nSeg; g++) {
                Segment memory seg;
                seg.data = _word(raw, cur);
                cur += 32;
                for (uint256 l = 0; l < 7; l++) {
                    seg.siblings[l] = _word(raw, cur);
                    cur += 32;
                }
                segments[g] = seg;
            }
            proof.steps[s] = Step({span: span, segments: segments});
        }
    }

    /// A big-endian u32 read at `off`.
    function _u32be(bytes memory buf, uint256 off) private pure returns (uint256 v) {
        require(off + 4 <= buf.length, "proof: truncated u32");
        for (uint256 i = 0; i < 4; i++) {
            v = (v << 8) | uint256(uint8(buf[off + i]));
        }
    }

    /// A big-endian u64 read at `off`.
    function _u64be(bytes memory buf, uint256 off) private pure returns (uint256 v) {
        require(off + 8 <= buf.length, "proof: truncated u64");
        for (uint256 i = 0; i < 8; i++) {
            v = (v << 8) | uint256(uint8(buf[off + i]));
        }
    }

    /// A 32-byte word read at `off`.
    function _word(bytes memory buf, uint256 off) private pure returns (bytes32 out) {
        require(off + 32 <= buf.length, "proof: truncated word");
        assembly {
            out := mload(add(add(buf, 32), off))
        }
    }
}
