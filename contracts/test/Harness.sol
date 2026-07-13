// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

/// The subset of the Foundry cheatcode interface the fixtures need.
///
/// Vendored rather than pulled from forge-std so the suite builds with no
/// network access; the address is the standard cheatcode account.
interface Vm {
    function readFileBinary(string calldata path) external view returns (bytes memory);
}

/// A minimal test base: a `test`-prefixed function is run by forge, and a
/// reverting assertion marks it failed. Vendored to avoid a forge-std fetch.
abstract contract Harness {
    Vm internal constant vm = Vm(0x7109709ECfa91a80626fF3989D68f67F5b1DD12D);

    function assertTrue(bool cond, string memory reason) internal pure {
        require(cond, reason);
    }

    function assertFalse(bool cond, string memory reason) internal pure {
        require(!cond, reason);
    }
}
