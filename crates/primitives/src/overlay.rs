//! Canonical overlay address derivation.
//!
//! The Swarm overlay address is `keccak256(ethereum_address || network_id || nonce)`
//! per bee `pkg/crypto/crypto.go:45-57`. Network ID is encoded in
//! **little-endian** in this hash (distinct from the big-endian encoding used
//! in the BzzAddress sign-data, see [`crate::signing::sign_data`]).

use alloy_primitives::{Address, Keccak256};

use crate::{NetworkId, Nonce, OverlayAddress};

/// Compute the Swarm overlay address from `eth_addr`, `network_id`, `nonce`.
///
/// Layout: `keccak256(eth_addr(20) || network_id_le(8) || nonce(32))`.
///
/// This is the **only** canonical derivation - any other formula creates a
/// peer with a different overlay that won't be reachable on the same network.
#[must_use]
pub fn compute_overlay(eth_addr: &Address, network_id: NetworkId, nonce: &Nonce) -> OverlayAddress {
    let mut hasher = Keccak256::new();
    hasher.update(eth_addr.as_slice());
    hasher.update(network_id.to_le_bytes());
    hasher.update(nonce.as_slice());
    OverlayAddress::from(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256};

    #[test]
    fn deterministic_zero_nonce_zero_addr() {
        let eth = Address::ZERO;
        let nonce = Nonce::ZERO;
        let net = NetworkId::MAINNET;
        let overlay = compute_overlay(&eth, net, &nonce);
        // Reproduce the exact bytes: keccak256(20 zeros || 1u64.to_le_bytes() || 32 zeros).
        let mut h = Keccak256::new();
        h.update([0u8; 20]);
        h.update(1u64.to_le_bytes());
        h.update([0u8; 32]);
        let expected = OverlayAddress::from(h.finalize());
        assert_eq!(overlay, expected);
    }

    #[test]
    fn different_network_yields_different_overlay() {
        let eth = address!("0102030405060708091011121314151617181920");
        let nonce = Nonce::new([0x55; 32]);
        let m = compute_overlay(&eth, NetworkId::MAINNET, &nonce);
        let t = compute_overlay(&eth, NetworkId::TESTNET, &nonce);
        assert_ne!(m, t);
    }

    #[test]
    fn different_nonce_yields_different_overlay() {
        let eth = address!("0102030405060708091011121314151617181920");
        let a = compute_overlay(&eth, NetworkId::MAINNET, &Nonce::new([0; 32]));
        let b = compute_overlay(&eth, NetworkId::MAINNET, &Nonce::new([1; 32]));
        assert_ne!(a, b);
    }

    #[test]
    fn network_id_is_little_endian() {
        // If the implementation accidentally used big-endian, this would change.
        let eth = address!("aabbccddeeff00112233445566778899aabbccdd");
        let nonce = Nonce::new([0x42; 32]);
        let net = NetworkId::new(0x0102_0304_0506_0708);
        let overlay = compute_overlay(&eth, net, &nonce);

        let mut h = Keccak256::new();
        h.update(eth.as_slice());
        h.update([0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]); // LE bytes
        h.update(nonce.as_slice());
        assert_eq!(overlay, OverlayAddress::from(h.finalize()));

        // Sanity-check the BE encoding would have produced a different overlay.
        let mut h_be = Keccak256::new();
        h_be.update(eth.as_slice());
        h_be.update([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]); // BE bytes
        h_be.update(nonce.as_slice());
        let be_overlay = OverlayAddress::from(h_be.finalize());
        assert_ne!(overlay, be_overlay);
        let _ = b256!("0000000000000000000000000000000000000000000000000000000000000000");
    }
}
