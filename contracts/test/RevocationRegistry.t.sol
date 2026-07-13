// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {DemoTest} from "./DemoTest.sol";
import {RevocationRegistry} from "../src/RevocationRegistry.sol";

/// The trustless revocation registry over deterministic fixtures: an absent
/// serial is valid and not revoked, a listed serial is revoked and not valid.
contract RevocationRegistryTest is DemoTest {
    // An absent serial is provably not revoked.
    function testAbsentSerialIsValid() public {
        KeyFixture memory fx = _keyFixture("excl_gap.bin");
        RevocationRegistry rr = new RevocationRegistry(fx.root);
        assertTrue(rr.isValid(fx.key, fx.proofBytes), "absent serial is valid");
    }

    // A listed serial has no exclusion proof, so it is not provably valid.
    function testListedSerialNotValid() public {
        KeyFixture memory fx = _keyFixture("incl_embedded.bin");
        RevocationRegistry rr = new RevocationRegistry(fx.root);
        assertFalse(rr.isValid(fx.key, fx.proofBytes), "listed serial is not provably valid");
    }

    // A listed serial verifies revoked against its inclusion proof.
    function testListedSerialIsRevoked() public {
        KeyFixture memory fx = _keyFixture("incl_embedded.bin");
        RevocationRegistry rr = new RevocationRegistry(fx.root);
        assertTrue(rr.isRevoked(fx.key, fx.value, fx.proofBytes), "listed serial verifies revoked");
    }

    // An absent serial does not verify revoked.
    function testAbsentSerialNotRevoked() public {
        KeyFixture memory fx = _keyFixture("excl_gap.bin");
        RevocationRegistry rr = new RevocationRegistry(fx.root);
        assertFalse(rr.isRevoked(fx.key, fx.value, fx.proofBytes), "absent serial does not verify revoked");
    }
}
