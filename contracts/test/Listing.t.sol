// SPDX-License-Identifier: AGPL-3.0-or-later
pragma solidity ^0.8.19;

import {DemoTest} from "./DemoTest.sol";
import {Listing} from "../src/Listing.sol";

/// The verifiable complete-listing acceptor over deterministic fixtures: a full
/// frontier accepts and matches the generator's digest, an incomplete one (a
/// withheld overlapping node) cannot.
contract ListingTest is DemoTest {
    // A single embedded node lists its whole contents in key order.
    function testAcceptEmbeddedListing() public {
        RangeFixture memory fx = _rangeFixture("range_all.bin");
        Listing listing = new Listing(fx.root);
        (uint256 count, bytes32 digest) = listing.accept(fx.lo, fx.hi, fx.nodes);
        assertTrue(count == fx.count, "embedded listing count matches");
        assertTrue(digest == fx.digest, "embedded listing digest matches the generator");
    }

    // A sub-range across a referenced hop lists only the in-range keys.
    function testAcceptRangeAcrossHop() public {
        RangeFixture memory fx = _rangeFixture("range_prefix.bin");
        Listing listing = new Listing(fx.root);
        (uint256 count, bytes32 digest) = listing.accept(fx.lo, fx.hi, fx.nodes);
        assertTrue(count == fx.count, "sub-range listing count matches");
        assertTrue(digest == fx.digest, "sub-range listing digest matches the generator");
        assertTrue(listing.digestOf(keccak256(abi.encode(fx.lo, fx.hi))) == digest, "accepted digest recorded");
    }

    // Withholding an overlapping frontier node leaves an edge with no witness,
    // so the listing cannot be accepted.
    function testIncompleteListingRejected() public {
        RangeFixture memory fx = _rangeFixture("range_prefix.bin");
        Listing listing = new Listing(fx.root);
        bytes[] memory withheld = new bytes[](1);
        withheld[0] = fx.nodes[0];
        try listing.accept(fx.lo, fx.hi, withheld) {
            assertTrue(false, "incomplete listing must revert");
        } catch {}
    }
}
