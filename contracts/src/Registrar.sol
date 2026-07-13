// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {MantarayProofVerifier, Proof} from "./MantarayProofVerifier.sol";

/// A squatting-proof name registrar over a mantaray names manifest.
///
/// Availability is proven, not trusted: a name is claimable only against an
/// exclusion proof that it is absent under the current root. The claim advances
/// the root to the caller-supplied successor (the manifest that now binds the
/// name), so the next claim proves absence against the updated set. This is a
/// demo of the exclusion primitive, so it carries no ordering, fees or
/// challenge window, and trusts the submitter for the successor root.
contract Registrar {
    /// The current names-manifest root every claim proves absence against.
    bytes32 public root;

    /// The claimant of each name, keyed by the name's hash.
    mapping(bytes32 => address) public ownerOf;

    /// A name was claimed and the root advanced to bind it.
    event Claimed(bytes name, address indexed owner, bytes32 newRoot);

    constructor(bytes32 initialRoot) {
        root = initialRoot;
    }

    /// Claim `name` against `proofBytes`, an exclusion proof of its absence under
    /// the current root, and advance the root to `newRoot`.
    ///
    /// Reverts unless the name is provably available, so a taken name (which has
    /// no exclusion proof) cannot be claimed.
    function claim(bytes calldata name, bytes calldata proofBytes, bytes32 newRoot) external {
        Proof memory proof = MantarayProofVerifier.decode(proofBytes);
        require(MantarayProofVerifier.verifyExclusion(root, name, proof), "name not provably available");
        ownerOf[keccak256(name)] = msg.sender;
        root = newRoot;
        emit Claimed(name, msg.sender, newRoot);
    }
}
