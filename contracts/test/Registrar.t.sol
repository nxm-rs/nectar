// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {DemoTest} from "./DemoTest.sol";
import {Registrar} from "../src/Registrar.sol";

/// The squatting-proof registrar over deterministic fixtures: an available name
/// claims and advances the root, a taken name (which has no exclusion proof)
/// cannot.
contract RegistrarTest is DemoTest {
    bytes32 internal constant SUCCESSOR = bytes32(uint256(0xABCD));

    // An available name (absent under the root) claims and advances the root.
    function testClaimAvailableName() public {
        KeyFixture memory fx = _keyFixture("excl_gap.bin");
        Registrar reg = new Registrar(fx.root);
        reg.claim(fx.key, fx.proofBytes, SUCCESSOR);
        assertTrue(reg.root() == SUCCESSOR, "root advances to the successor");
        assertTrue(reg.ownerOf(keccak256(fx.key)) == address(this), "claimant recorded");
    }

    // A taken name cannot be claimed: its inclusion proof does not attest
    // absence, so the claim reverts and the root is unchanged.
    function testClaimTakenNameRejected() public {
        KeyFixture memory fx = _keyFixture("incl_embedded.bin");
        Registrar reg = new Registrar(fx.root);
        try reg.claim(fx.key, fx.proofBytes, SUCCESSOR) {
            assertTrue(false, "claiming a taken name must revert");
        } catch {
            assertTrue(reg.root() == fx.root, "root unchanged on rejection");
        }
    }
}
