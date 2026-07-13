// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

/// On-chain range-completeness verification for mantaray 1.0 manifests: a
/// listing over a half-open key range that provably omits nothing.
///
/// A single-key descent authenticates one path; a complete listing needs the
/// whole frontier of nodes whose subtrees overlap the range. This verifier is
/// therefore chunk-granularity: it is given the frontier node payloads, derives
/// each one's content address by re-BMT (mirroring nectar's prefix-free
/// content-chunk hash), and re-walks the frontier from the trusted root. Each
/// referenced child whose subtree overlaps the range must be present, matched by
/// the address the parent supplied, so a withheld node leaves an overlapping
/// edge with no witness and the listing cannot verify. The gaps between emitted
/// keys are thus provably empty without a per-gap exclusion proof.
///
/// Enumeration mirrors the ordered reader (spec 8): the root's own value leads,
/// each fork contributes its value then its continuation, and an embedded child
/// folds in place, so the emitted order is the trie's total key order. The walk
/// returns a digest over that ordered listing rather than the listing itself,
/// keeping the entrypoint's return shape flat; the digest frames each pair as a
/// big-endian u32 key length and bytes then a big-endian u32 value length and
/// bytes, concatenated in order.
///
/// Restricted profile, as the single-key verifier: frozen tag_version 0x01,
/// plaintext 32-byte references. A referenced-child fork's trailing subtree
/// count is crossed but never read: each record is walked within its
/// offset-bounded span, so the count bytes are present-but-skipped, not
/// verified. An encrypted child that overlaps the range fails the listing
/// closed, since the plain walk cannot enumerate it.
library MantarayRangeVerifier {
    // Node-body grammar (spec 5.1). Flags byte bit layout, shared with the
    // single-key verifier.
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

    // The frozen preamble: magic 'm' then version 0x01.
    bytes2 private constant PREAMBLE = 0x6d01;

    // Content-chunk BMT: 128 leaves over a 4096-byte zero-padded body, so seven
    // levels of pair hashing reach the tree root.
    uint256 private constant BRANCHES = 128;

    /// The mutable frontier-walk state threaded through the recursion: the
    /// carried nodes and their derived addresses, the range bounds, and the
    /// listing digest built so far. `ok` latches false on any structural fault,
    /// a withheld overlapping child, or an encrypted child in range.
    struct Walk {
        bytes32[] addrs;
        bytes[] nodes;
        bytes lo;
        bytes hi;
        bytes buf;
        uint256 count;
        bool ok;
    }

    /// Whether the carried nodes are a complete listing of `[lo, hi)` under
    /// `root`, and if so the count and digest of that authenticated listing.
    ///
    /// A false `ok` is total: any missing overlapping node, malformed byte, or
    /// encrypted child in range fails closed with a zero count and digest.
    function verifyRangeComplete(bytes32 root, bytes memory lo, bytes memory hi, bytes[] memory nodes)
        internal
        pure
        returns (bool ok, uint256 count, bytes32 digest)
    {
        Walk memory w;
        w.nodes = nodes;
        w.addrs = new bytes32[](nodes.length);
        for (uint256 i = 0; i < nodes.length; i++) {
            w.addrs[i] = _chunkAddress(nodes[i]);
        }
        w.lo = lo;
        w.hi = hi;
        w.ok = true;
        (bool found, uint256 idx) = _find(w.addrs, root);
        if (!found) {
            return (false, 0, bytes32(0));
        }
        _walkNode(w, idx, "", true);
        if (!w.ok) {
            return (false, 0, bytes32(0));
        }
        return (true, w.count, keccak256(w.buf));
    }

    /// Walk one authenticated node: its own value (only at the root, the least
    /// key), then its fork table.
    function _walkNode(Walk memory w, uint256 index, bytes memory base, bool isRoot) private pure {
        bytes memory buf = w.nodes[index];
        uint256 len = buf.length;
        if (len < 3) {
            w.ok = false;
            return;
        }
        if (bytes2(buf[0]) | (bytes2(buf[1]) >> 8) != PREAMBLE) {
            w.ok = false;
            return;
        }
        uint8 flags = uint8(buf[2]);
        if (flags & (SEGMENT | SEGMENTED) != 0) {
            w.ok = false;
            return;
        }
        uint256 cur = 3;
        (bool eok, uint256 ecur, bytes memory entry, bool hasEntry) = _readEntry(buf, cur, len, flags & ENTRY_MASK);
        if (!eok) {
            w.ok = false;
            return;
        }
        cur = ecur;
        if (flags & HAS_META != 0) {
            (bool mok, uint256 mcur) = _skipMeta(buf, cur, len);
            if (!mok) {
                w.ok = false;
                return;
            }
            cur = mcur;
        }
        if (isRoot && hasEntry) {
            _emit(w, base, entry);
        }
        _walkTable(w, buf, cur, len, base);
    }

    /// Walk a fork table bounded by `[tableStart, tableEnd)`: the count, the
    /// index of (first byte, offset) slots, then each record over its span.
    function _walkTable(Walk memory w, bytes memory buf, uint256 tableStart, uint256 tableEnd, bytes memory base)
        private
        pure
    {
        uint256 cur = tableStart;
        if (cur + 2 > tableEnd) {
            w.ok = false;
            return;
        }
        uint256 fcount = _u16le(buf, cur);
        cur += 2;
        uint8[] memory firsts = new uint8[](fcount);
        uint256[] memory offs = new uint256[](fcount);
        for (uint256 f = 0; f < fcount; f++) {
            if (cur + 3 > tableEnd) {
                w.ok = false;
                return;
            }
            firsts[f] = uint8(buf[cur]);
            offs[f] = _u16le(buf, cur + 1);
            cur += 3;
        }
        uint256 recordsStart = cur;
        for (uint256 f = 0; f < fcount; f++) {
            uint256 recStart = recordsStart + offs[f];
            uint256 recEnd = f + 1 < fcount ? recordsStart + offs[f + 1] : tableEnd;
            if (recStart > recEnd || recEnd > tableEnd) {
                w.ok = false;
                return;
            }
            _walkRecord(w, buf, recStart, recEnd, base, firsts[f]);
            if (!w.ok) {
                return;
            }
        }
    }

    /// Walk one fork record bounded by `[recStart, recEnd)`: its value, if any,
    /// under the reassembled prefix, then its continuation (a referenced hop, an
    /// embedded table folded in place, or a dead end). Span-tolerant: bytes
    /// after the fields it reads, such as a referenced child's trailing count,
    /// are skipped by the offset bounds.
    function _walkRecord(
        Walk memory w,
        bytes memory buf,
        uint256 recStart,
        uint256 recEnd,
        bytes memory base,
        uint8 first
    ) private pure {
        uint256 rc = recStart;
        if (rc + 2 > recEnd) {
            w.ok = false;
            return;
        }
        uint8 rflags = uint8(buf[rc]);
        uint8 plen = uint8(buf[rc + 1]);
        rc += 2;
        if (plen == 0) {
            w.ok = false;
            return;
        }
        uint256 tailLen = uint256(plen) - 1;
        if (rc + tailLen > recEnd) {
            w.ok = false;
            return;
        }
        bytes memory prefix = bytes.concat(base, bytes1(first), _range(buf, rc, tailLen));
        rc += tailLen;

        (bool eok, uint256 ecur, bytes memory entry, bool hasEntry) = _readEntry(buf, rc, recEnd, rflags & ENTRY_MASK);
        if (!eok) {
            w.ok = false;
            return;
        }
        rc = ecur;
        if (hasEntry) {
            _emit(w, prefix, entry);
        }

        uint8 cw = (rflags & CHILD_MASK) >> 2;
        if (cw == W_NONE) {
            return;
        }
        if (cw == W_REF32) {
            if (rc + 32 > recEnd) {
                w.ok = false;
                return;
            }
            if (_overlaps(prefix, w.lo, w.hi)) {
                (bool found, uint256 idx) = _find(w.addrs, _addr32(buf, rc));
                if (!found) {
                    w.ok = false;
                    return;
                }
                _walkNode(w, idx, prefix, false);
            }
            return;
        }
        if (cw == W_REF64) {
            // An encrypted child the plain walk cannot open: complete only if it
            // sits wholly outside the range.
            if (_overlaps(prefix, w.lo, w.hi)) {
                w.ok = false;
            }
            return;
        }
        if (cw != W_INLINE) {
            w.ok = false;
            return;
        }
        // Embedded child body: its length, a forced zero flags byte, then its
        // own fork table, all in these bytes. Fold it in place under `prefix`.
        if (rc + 2 > recEnd) {
            w.ok = false;
            return;
        }
        uint256 ilen = _u16le(buf, rc);
        uint256 childStart = rc + 2;
        uint256 childEnd = childStart + ilen;
        if (ilen == 0 || childEnd > recEnd) {
            w.ok = false;
            return;
        }
        _walkTable(w, buf, childStart + 1, childEnd, prefix);
    }

    /// Append an in-range key and its value to the listing digest preimage.
    function _emit(Walk memory w, bytes memory key, bytes memory value) private pure {
        if (_inRange(key, w.lo, w.hi)) {
            w.buf = bytes.concat(w.buf, _u32be(key.length), key, _u32be(value.length), value);
            w.count += 1;
        }
    }

    /// Read a width-tagged entry within `[cur, end)`, advancing past it. The
    /// value bytes are the reference for a reference entry, or the inline bytes.
    function _readEntry(bytes memory buf, uint256 cur, uint256 end, uint8 width)
        private
        pure
        returns (bool ok, uint256 next, bytes memory value, bool present)
    {
        if (width == W_NONE) {
            return (true, cur, "", false);
        }
        if (width == W_REF32) {
            if (cur + 32 > end) {
                return (false, cur, "", false);
            }
            return (true, cur + 32, _range(buf, cur, 32), true);
        }
        if (width == W_REF64) {
            if (cur + 64 > end) {
                return (false, cur, "", false);
            }
            return (true, cur + 64, _range(buf, cur, 64), true);
        }
        if (cur + 1 > end) {
            return (false, cur, "", false);
        }
        uint256 vlen = uint8(buf[cur]);
        cur += 1;
        if (cur + vlen > end) {
            return (false, cur, "", false);
        }
        return (true, cur + vlen, _range(buf, cur, vlen), true);
    }

    /// Skip a metadata block within `[cur, end)`: a little-endian u16 length
    /// then that many bytes.
    function _skipMeta(bytes memory buf, uint256 cur, uint256 end) private pure returns (bool, uint256) {
        if (cur + 2 > end) {
            return (false, cur);
        }
        uint256 mlen = _u16le(buf, cur);
        cur += 2;
        if (cur + mlen > end) {
            return (false, cur);
        }
        return (true, cur + mlen);
    }

    /// The content address of a node payload: nectar's prefix-free content-chunk
    /// BMT. keccak256 up the binary tree of 128 zero-padded 32-byte leaves, then
    /// keccak256 of the little-endian span (the payload length) and the root.
    function _chunkAddress(bytes memory payload) private pure returns (bytes32) {
        bytes32[] memory level = new bytes32[](BRANCHES);
        uint256 n = payload.length;
        for (uint256 i = 0; i < BRANCHES; i++) {
            level[i] = _leaf(payload, i * 32, n);
        }
        uint256 width = BRANCHES;
        while (width > 1) {
            uint256 half = width / 2;
            for (uint256 j = 0; j < half; j++) {
                level[j] = keccak256(abi.encodePacked(level[2 * j], level[2 * j + 1]));
            }
            width = half;
        }
        return keccak256(abi.encodePacked(_spanLE(uint64(n)), level[0]));
    }

    /// The 32-byte leaf at `off`, zero-padded past the payload length `n`.
    function _leaf(bytes memory payload, uint256 off, uint256 n) private pure returns (bytes32 out) {
        for (uint256 i = 0; i < 32; i++) {
            uint256 p = off + i;
            if (p < n) {
                out |= bytes32(uint256(uint8(payload[p])) << (8 * (31 - i)));
            }
        }
    }

    /// The index of the node whose address is `target`, if carried.
    function _find(bytes32[] memory addrs, bytes32 target) private pure returns (bool, uint256) {
        for (uint256 i = 0; i < addrs.length; i++) {
            if (addrs[i] == target) {
                return (true, i);
            }
        }
        return (false, 0);
    }

    /// Whether `key` falls in the half-open range `[lo, hi)`.
    function _inRange(bytes memory key, bytes memory lo, bytes memory hi) private pure returns (bool) {
        return !_lt(key, lo) && _lt(key, hi);
    }

    /// Whether the subtree under `prefix` can hold a key in `[lo, hi)`: its keys
    /// span `[prefix, successor(prefix))`, so it overlaps unless it sits wholly
    /// at or past `hi` or wholly below `lo`.
    function _overlaps(bytes memory prefix, bytes memory lo, bytes memory hi) private pure returns (bool) {
        if (!_lt(prefix, hi)) {
            return false;
        }
        (bool bounded, bytes memory end) = _successor(prefix);
        return !bounded || _lt(lo, end);
    }

    /// The least byte string strictly greater than every string starting with
    /// `prefix`: increment the last sub-0xFF byte after dropping the trailing
    /// 0xFF run. Unbounded (false) when the prefix is empty or all 0xFF.
    function _successor(bytes memory prefix) private pure returns (bool, bytes memory) {
        uint256 keep = prefix.length;
        while (keep > 0 && uint8(prefix[keep - 1]) == 0xFF) {
            keep -= 1;
        }
        if (keep == 0) {
            return (false, "");
        }
        bytes memory out = new bytes(keep);
        for (uint256 i = 0; i < keep; i++) {
            out[i] = prefix[i];
        }
        out[keep - 1] = bytes1(uint8(out[keep - 1]) + 1);
        return (true, out);
    }

    /// Whether `a` is lexicographically less than `b`.
    function _lt(bytes memory a, bytes memory b) private pure returns (bool) {
        uint256 n = a.length < b.length ? a.length : b.length;
        for (uint256 i = 0; i < n; i++) {
            uint8 x = uint8(a[i]);
            uint8 y = uint8(b[i]);
            if (x != y) {
                return x < y;
            }
        }
        return a.length < b.length;
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

    /// A big-endian u32 of `v` as four bytes.
    function _u32be(uint256 v) private pure returns (bytes memory b) {
        b = new bytes(4);
        b[0] = bytes1(uint8(v >> 24));
        b[1] = bytes1(uint8(v >> 16));
        b[2] = bytes1(uint8(v >> 8));
        b[3] = bytes1(uint8(v));
    }

    /// The span as eight little-endian bytes, matching nectar's BMT span header.
    function _spanLE(uint64 v) private pure returns (bytes memory b) {
        b = new bytes(8);
        for (uint256 i = 0; i < 8; i++) {
            b[i] = bytes1(uint8(v >> (8 * i)));
        }
    }

    /// A copy of `buf[off..off+n]`; the caller has bounds-checked.
    function _range(bytes memory buf, uint256 off, uint256 n) private pure returns (bytes memory out) {
        out = new bytes(n);
        for (uint256 i = 0; i < n; i++) {
            out[i] = buf[off + i];
        }
    }
}
