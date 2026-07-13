// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {DemoTest} from "./DemoTest.sol";
import {Log} from "../src/Log.sol";

/// The append-only transparency log over deterministic fixtures: a valid
/// insertion transition appends and advances the head, a false one (a change
/// that did not happen at the claimed root) cannot.
contract LogTest is DemoTest {
    // A key absent under the head and present under the new root appends.
    function testAppendValidTransition() public {
        TransitionFixture memory fx = _transitionFixture("transition_insert.bin");
        Log log = new Log(fx.rootBefore);
        log.append(fx.key, fx.value, fx.beforeBytes, fx.afterBytes, fx.rootAfter);
        assertTrue(log.head() == fx.rootAfter, "head advances to the new root");
        assertTrue(log.size() == 1, "one entry appended");
    }

    // Claiming the key present at the prior root, where it is absent, cannot
    // authenticate its inclusion half, so the append reverts.
    function testFalseTransitionRejected() public {
        TransitionFixture memory fx = _transitionFixture("transition_insert.bin");
        Log log = new Log(fx.rootBefore);
        try log.append(fx.key, fx.value, fx.beforeBytes, fx.afterBytes, fx.rootBefore) {
            assertTrue(false, "false transition must revert");
        } catch {
            assertTrue(log.head() == fx.rootBefore, "head unchanged on rejection");
            assertTrue(log.size() == 0, "no entry appended");
        }
    }
}
