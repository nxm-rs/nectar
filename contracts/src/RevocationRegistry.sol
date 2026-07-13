// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {MantarayProofVerifier, Proof} from "./MantarayProofVerifier.sol";

/// A trustless revocation registry over a mantaray revocation manifest.
///
/// The manifest is the blocklist (a CRL or OCSP responder's set); its root is
/// the only trusted state. A serial is valid exactly when an exclusion proof
/// shows it absent from the list, so validity is checked against the committed
/// set rather than a trusted responder's say-so. The inclusion-gated variant is
/// the dual: it confirms a serial is on the list against the same root. Both are
/// pure gating checks over the committed root; a demo of the exclusion and
/// inclusion primitives with no revocation-issuance logic of its own.
contract RevocationRegistry {
    /// The revocation-manifest root every check authenticates against.
    bytes32 public root;

    constructor(bytes32 revocationRoot) {
        root = revocationRoot;
    }

    /// Whether `serial` is provably not revoked: an exclusion proof shows it
    /// absent from the revocation manifest under the current root.
    function isValid(bytes calldata serial, bytes calldata proofBytes) external view returns (bool) {
        Proof memory proof = MantarayProofVerifier.decode(proofBytes);
        return MantarayProofVerifier.verifyExclusion(root, serial, proof);
    }

    /// Whether `serial` is provably revoked: an inclusion proof binds it to
    /// `entry` in the revocation manifest under the current root.
    ///
    /// The entry bytes are the manifest value as carried on the wire: the
    /// 32-byte reference for a reference entry, or the inline bytes.
    function isRevoked(bytes calldata serial, bytes calldata entry, bytes calldata proofBytes)
        external
        view
        returns (bool)
    {
        Proof memory proof = MantarayProofVerifier.decode(proofBytes);
        return MantarayProofVerifier.verifyInclusion(root, serial, entry, proof);
    }
}
