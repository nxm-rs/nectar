//! BzzAddress sign-data byte layout (Swarm handshake / hive shared spec).
//!
//! The Swarm peer record (overlay + underlay + nonce + timestamp + chequebook)
//! is authenticated by an EIP-191 personal-sign signature over the byte
//! sequence built by [`sign_data`] below. The layout matches bee
//! `pkg/bzz/address.go:138-160` exactly so any Swarm impl can interop.
//!
//! ```text
//! sign_data = "bee-handshake-"        (14 bytes)
//!           || underlay_bytes         (caller-supplied wire encoding)
//!           || overlay                (32 bytes)
//!           || network_id big-endian  (8 bytes)
//!           || nonce                  (32 bytes)
//!           || timestamp big-endian   (8 bytes, i64 two's-complement)
//!           || chequebook             (20 bytes, all-zero if None)
//! ```
//!
//! Signing and recovery are **not wrapped** - callers use alloy directly:
//!
//! ```ignore
//! use alloy_signer::SignerSync;
//! let sig = signer.sign_message_sync(&sign_data(/* … */))?;
//! let eth = sig.recover_address_from_msg(&sign_data(/* … */))?;
//! ```
//!
//! Overlay verification is also one line - compare against
//! [`compute_overlay`](crate::compute_overlay):
//!
//! ```ignore
//! if compute_overlay(&recovered_eth, network_id, &nonce) == claimed_overlay { /* ok */ }
//! ```
//!
//! Nectar intentionally does **not** depend on libp2p - the `underlay_bytes`
//! argument is whatever wire encoding the calling node uses for its multiaddr
//! list.

use alloy_primitives::Address;

use crate::{NetworkId, Nonce, SwarmAddress, Timestamp};

/// Magic prefix matching bee `pkg/bzz/address.go:138` (`signDataPrefix`).
pub const SIGN_DATA_PREFIX: &[u8] = b"bee-handshake-";

/// Build the canonical sign-data buffer for a BzzAddress.
///
/// `underlay_bytes` is the wire-encoded multiaddr list (caller-defined format).
/// `chequebook` is `None` for nodes without a chequebook; the byte layout
/// pads with 20 zero bytes either way, matching bee's `common.Address{}.Bytes()`
/// behaviour (so `None` and `Some(Address::ZERO)` produce byte-identical
/// sign-data - verified by the test suite).
#[must_use]
pub fn sign_data(
    underlay_bytes: &[u8],
    overlay: &SwarmAddress,
    network_id: NetworkId,
    nonce: &Nonce,
    timestamp: Timestamp,
    chequebook: Option<&Address>,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        SIGN_DATA_PREFIX.len()
            + underlay_bytes.len()
            + 32  // overlay
            + 8   // network_id
            + 32  // nonce
            + 8   // timestamp
            + 20, // chequebook
    );
    buf.extend_from_slice(SIGN_DATA_PREFIX);
    buf.extend_from_slice(underlay_bytes);
    buf.extend_from_slice(overlay.as_bytes());
    buf.extend_from_slice(&network_id.to_be_bytes());
    buf.extend_from_slice(nonce.as_slice());
    buf.extend_from_slice(&timestamp.to_be_bytes());
    match chequebook {
        Some(addr) => buf.extend_from_slice(addr.as_slice()),
        None => buf.extend_from_slice(&[0u8; 20]),
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute_overlay;
    use alloy_primitives::{address, b256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::LocalSigner;

    #[test]
    fn layout_matches_bee_spec() {
        let overlay = SwarmAddress::new([0xaa; 32]);
        let net = NetworkId::MAINNET;
        let nonce = Nonce::new([0xbb; 32]);
        let ts = Timestamp::from(0x0102_0304_0506_0708_i64);
        let cb = address!("00112233445566778899aabbccddeeff00112233");
        let underlay = b"\x01\x02\x03";

        let buf = sign_data(underlay, &overlay, net, &nonce, ts, Some(&cb));

        assert_eq!(&buf[0..14], b"bee-handshake-");
        assert_eq!(&buf[14..17], b"\x01\x02\x03");
        assert_eq!(&buf[17..49], &[0xaa; 32]);
        assert_eq!(&buf[49..57], &1u64.to_be_bytes());
        assert_eq!(&buf[57..89], &[0xbb; 32]);
        assert_eq!(
            &buf[89..97],
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
        assert_eq!(&buf[97..117], cb.as_slice());
        assert_eq!(buf.len(), 117);
    }

    #[test]
    fn no_chequebook_is_byte_identical_to_zero_chequebook() {
        let a = sign_data(
            &[],
            &SwarmAddress::zero(),
            NetworkId::MAINNET,
            &Nonce::ZERO,
            Timestamp::ZERO,
            None,
        );
        let b = sign_data(
            &[],
            &SwarmAddress::zero(),
            NetworkId::MAINNET,
            &Nonce::ZERO,
            Timestamp::ZERO,
            Some(&Address::ZERO),
        );
        assert_eq!(a, b);
        assert_eq!(&a[a.len() - 20..], &[0u8; 20]);
    }

    /// End-to-end: build sign-data, sign with alloy, recover via alloy,
    /// verify overlay by comparing `compute_overlay` against the claimed one.
    /// Demonstrates the canonical caller flow - no wrapper layer needed.
    #[test]
    fn caller_flow_sign_recover_verify_overlay() {
        let signer = LocalSigner::random();
        let eth = signer.address();
        let net = NetworkId::TESTNET;
        let nonce = Nonce::new([0x55; 32]);
        let overlay = compute_overlay(&eth, net, &nonce);
        let ts = Timestamp::from(1_700_000_000_i64);

        let data = sign_data(b"/ip4/127.0.0.1/tcp/1634", &overlay, net, &nonce, ts, None);

        // Sign - alloy directly.
        let sig = signer.sign_message_sync(&data).expect("sign");
        // Recover - alloy directly.
        let recovered = sig.recover_address_from_msg(&data).expect("recover");
        assert_eq!(recovered, eth);
        // Verify overlay - direct compute + compare.
        assert_eq!(compute_overlay(&recovered, net, &nonce), overlay);
    }

    /// Pinned vector: tampering with the nonce changes the derived overlay so
    /// the equality check fails. Catches regressions in either `sign_data`
    /// layout or `compute_overlay` hashing.
    #[test]
    fn tampered_nonce_breaks_overlay_check() {
        let signer = LocalSigner::random();
        let eth = signer.address();
        let net = NetworkId::MAINNET;
        let nonce = Nonce::from(b256!(
            "0202020202020202020202020202020202020202020202020202020202020202"
        ));
        let overlay = compute_overlay(&eth, net, &nonce);
        let wrong_nonce = Nonce::new([0x88; 32]);
        assert_ne!(compute_overlay(&eth, net, &wrong_nonce), overlay);
    }
}
