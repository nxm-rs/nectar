// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {MantarayRangeVerifier} from "./MantarayRangeVerifier.sol";

/// A verifiable complete-listing acceptor over a mantaray manifest.
///
/// A served directory listing is accepted only against a range-completeness
/// proof, so the listing provably omits nothing under its prefix or range: the
/// gaps between the listed keys are proven empty, not asserted. The accepted
/// listing is recorded as the digest the verifier derives, keyed by its bounds,
/// so a later reader can check a listing it is served against the committed
/// digest. A demo of the range-completeness primitive with no directory
/// semantics of its own.
contract Listing {
    /// The manifest root every listing is proven complete against.
    bytes32 public root;

    /// The accepted listing digest for a range, keyed by its bounds.
    mapping(bytes32 => bytes32) public digestOf;

    /// A complete listing over `[lo, hi)` was accepted.
    event Accepted(bytes lo, bytes hi, uint256 count, bytes32 digest);

    constructor(bytes32 manifestRoot) {
        root = manifestRoot;
    }

    /// Accept the listing carried by `nodes` over `[lo, hi)` and record its
    /// digest, returning the listing's key count and digest.
    ///
    /// Reverts unless the frontier is complete, so an incomplete listing (one
    /// missing an overlapping node) cannot be accepted.
    function accept(bytes calldata lo, bytes calldata hi, bytes[] calldata nodes)
        external
        returns (uint256 count, bytes32 digest)
    {
        bool ok;
        (ok, count, digest) = MantarayRangeVerifier.verifyRangeComplete(root, lo, hi, nodes);
        require(ok, "listing not provably complete");
        digestOf[keccak256(abi.encode(lo, hi))] = digest;
        emit Accepted(lo, hi, count, digest);
    }
}
