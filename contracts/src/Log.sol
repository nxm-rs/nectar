// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {MantarayProofVerifier, Proof} from "./MantarayProofVerifier.sol";

/// An append-only transparency log over a mantaray manifest.
///
/// A key is appended only against a state-transition proof that it was absent
/// under the prior root and present under the new one, so the log cannot
/// equivocate: it can neither drop an entry it committed nor forge one it did
/// not, since each append authenticates both endpoints. The new root becomes the
/// log head. A demo of the state-transition primitive with no ordering or
/// witness-cosigning of its own.
contract Log {
    /// The current log head every append transitions from.
    bytes32 public head;

    /// The number of appended entries.
    uint256 public size;

    /// An entry was appended and the head advanced.
    event Appended(bytes key, bytes value, bytes32 newHead);

    constructor(bytes32 genesisRoot) {
        head = genesisRoot;
    }

    /// Append `key` mapping to `value` against the transition from the current
    /// head to `newHead`: absent under the head, present under `newHead`.
    ///
    /// `beforeBytes` is the exclusion proof under the head; `afterBytes` the
    /// inclusion proof under `newHead`. Reverts unless the insertion shape
    /// holds, so a false transition (one that reuses a root, or claims a change
    /// that did not happen) cannot advance the head.
    function append(
        bytes calldata key,
        bytes calldata value,
        bytes calldata beforeBytes,
        bytes calldata afterBytes,
        bytes32 newHead
    ) external {
        Proof memory before = MantarayProofVerifier.decode(beforeBytes);
        Proof memory afterProof = MantarayProofVerifier.decode(afterBytes);
        require(
            MantarayProofVerifier.verifyTransition(head, newHead, key, value, before, afterProof),
            "not a valid append transition"
        );
        head = newHead;
        size += 1;
        emit Appended(key, value, newHead);
    }
}
