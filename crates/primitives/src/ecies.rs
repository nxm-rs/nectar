//! secp256k1 ECIES: ephemeral ECDH feeding the Keccak-256 CTR cipher.
//!
//! The cipher key is `keccak256(x || salt)` where `x` is the ECDH shared
//! point's x coordinate serialised with leading zero bytes dropped, matching
//! the reference client's big-integer encoding. Wire-critical: ciphertexts
//! must decrypt cross-client.

use alloy_primitives::Keccak256;
use thiserror::Error;

pub use k256::{PublicKey, SecretKey};

use crate::chunk::encryption::{EncryptionKey, transcrypt_in_place};
use crate::error::WrongLength;

/// Errors from ECIES operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EciesError {
    /// Plaintext exceeds the requested padded length.
    #[error("plaintext too long: {len} bytes, padded length {padded}")]
    PlaintextTooLong {
        /// Plaintext length.
        len: usize,
        /// Requested padded length.
        padded: usize,
    },
}

/// Derive the shared cipher key: `keccak256(x || salt)`.
///
/// Symmetric: the encryptor passes `(ephemeral_secret, recipient_public)`,
/// the decryptor `(recipient_secret, ephemeral_public)`.
#[must_use]
pub fn shared_key(secret: &SecretKey, public: &PublicKey, salt: &[u8]) -> EncryptionKey {
    let shared = k256::ecdh::diffie_hellman(secret.to_nonzero_scalar(), public.as_affine());

    // The reference client serialises x as a big integer: leading zero
    // bytes are dropped before hashing.
    let mut x: &[u8] = shared.raw_secret_bytes();
    while let [0, rest @ ..] = x {
        x = rest;
    }

    let mut hasher = Keccak256::new();
    hasher.update(x);
    hasher.update(salt);
    EncryptionKey::from(hasher.finalize())
}

/// Topic-match hint: `keccak256(key || salt)[..8]`.
///
/// Lets a recipient cheaply test a salt before attempting a full decrypt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hint([u8; Self::SIZE]);

impl Hint {
    /// Byte length of a hint.
    pub const SIZE: usize = 8;

    /// Derive the hint for a shared key and salt.
    #[must_use]
    pub fn derive(key: &EncryptionKey, salt: &[u8]) -> Self {
        let mut hasher = Keccak256::new();
        hasher.update(key.as_bytes());
        hasher.update(salt);
        let digest: [u8; 32] = hasher.finalize().0;
        let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = digest;
        Self([b0, b1, b2, b3, b4, b5, b6, b7])
    }

    /// Access the raw hint bytes.
    pub const fn as_bytes(&self) -> &[u8; Self::SIZE] {
        &self.0
    }
}

impl From<[u8; Self::SIZE]> for Hint {
    fn from(bytes: [u8; Self::SIZE]) -> Self {
        Self(bytes)
    }
}

impl TryFrom<&[u8]> for Hint {
    type Error = WrongLength;

    fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; Self::SIZE] = slice.try_into().map_err(|_| WrongLength {
            expected: Self::SIZE,
            got: slice.len(),
        })?;
        Ok(Self(bytes))
    }
}

impl AsRef<[u8]> for Hint {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Output of an ECIES encryption.
#[derive(Debug)]
pub struct Encrypted {
    /// Ephemeral public key the recipient needs to derive the key.
    pub ephemeral: PublicKey,
    /// Derived cipher key, kept for hint derivation.
    pub key: EncryptionKey,
    /// Ciphertext: `ctr(plaintext)` followed by random padding.
    pub ciphertext: Vec<u8>,
}

/// Generate a random secp256k1 secret key.
#[cfg(any(test, feature = "encryption"))]
#[must_use]
pub fn generate_secret() -> SecretKey {
    use rand::RngExt;
    use zeroize::Zeroize;
    loop {
        let mut bytes = rand::rng().random::<[u8; 32]>();
        let candidate = SecretKey::from_slice(&bytes);
        bytes.zeroize();
        if let Ok(secret) = candidate {
            return secret;
        }
    }
}

/// Encrypt for `recipient` under a fresh ephemeral keypair.
///
/// With `pad_to`, the ciphertext is extended to that length with random
/// bytes; the padding is appended raw, outside the keystream.
#[cfg(any(test, feature = "encryption"))]
pub fn encrypt(
    recipient: &PublicKey,
    salt: &[u8],
    plaintext: &[u8],
    pad_to: Option<usize>,
) -> Result<Encrypted, EciesError> {
    encrypt_with(&generate_secret(), recipient, salt, plaintext, pad_to)
}

/// Encrypt for `recipient` under a caller-supplied ephemeral key.
///
/// Deterministic apart from the random padding bytes.
#[cfg(any(test, feature = "encryption"))]
pub fn encrypt_with(
    ephemeral: &SecretKey,
    recipient: &PublicKey,
    salt: &[u8],
    plaintext: &[u8],
    pad_to: Option<usize>,
) -> Result<Encrypted, EciesError> {
    let pad_len = match pad_to {
        Some(padded) => {
            padded
                .checked_sub(plaintext.len())
                .ok_or(EciesError::PlaintextTooLong {
                    len: plaintext.len(),
                    padded,
                })?
        }
        None => 0,
    };

    let key = shared_key(ephemeral, recipient, salt);
    let mut ciphertext = plaintext.to_vec();
    transcrypt_in_place(&key, 0, &mut ciphertext);

    if pad_len > 0 {
        use rand::RngExt;
        let mut padding = vec![0u8; pad_len];
        rand::rng().fill(padding.as_mut_slice());
        ciphertext.extend_from_slice(&padding);
    }

    Ok(Encrypted {
        ephemeral: ephemeral.public_key(),
        key,
        ciphertext,
    })
}

/// Decrypt a full ciphertext with a key from [`shared_key`].
///
/// The whole buffer runs through the keystream; for padded ciphertext the
/// caller truncates to the known plaintext length afterwards.
#[must_use]
pub fn decrypt(key: &EncryptionKey, ciphertext: &[u8]) -> Vec<u8> {
    let mut plaintext = ciphertext.to_vec();
    transcrypt_in_place(key, 0, &mut plaintext);
    plaintext
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;

    /// Reference-client vector inputs: the secret scalars are
    /// `keccak256` of the ASCII labels below, the salt is
    /// `keccak256("nectar ecies test topic")`, the plaintext is bytes
    /// `0..40`. Expected values captured from the reference client.
    const EPH_SK: [u8; 32] =
        hex!("448e201b834a12adc567044d9b7c691882b520c92716e5850cfb81da44986047"); // keccak("nectar ecies test ephemeral")
    const RECIP_SK: [u8; 32] =
        hex!("17e2ba35468223438bdcd15a58e70da0560d6683eccc3fe37981d324d2941d5e"); // keccak("nectar ecies test recipient")
    const RECIP_PUB: [u8; 33] =
        hex!("027e7d4e5e5ed4c81f9a31542148aaddcce596f6c97248a75c52e9c3f2e006852a");
    const TOPIC: [u8; 32] =
        hex!("4f724e7d0f578ce9545f892880ca785e3b79e62679b59a0bf592d103f9eab1ae");

    fn plaintext40() -> Vec<u8> {
        (0u8..40).collect()
    }

    fn keys() -> (SecretKey, SecretKey, PublicKey) {
        let eph = SecretKey::from_slice(&EPH_SK).unwrap();
        let recip = SecretKey::from_slice(&RECIP_SK).unwrap();
        let recip_pub = PublicKey::from_sec1_bytes(&RECIP_PUB).unwrap();
        (eph, recip, recip_pub)
    }

    #[test]
    fn conformance_basic() {
        let (eph, recip, recip_pub) = keys();
        let expected_key = hex!("68aa2544cf561c7d8b014ac748825abaa435abf8bf47f2b436b0470db423685b");
        let expected_ct = hex!(
            "4a44080be348dc2e52afc1c441c35af054cfa397ce0da076c3f65aa65ca659f166736e30923400d5"
        );

        let out = encrypt_with(&eph, &recip_pub, &TOPIC, &plaintext40(), None).unwrap();
        assert_eq!(out.key.as_bytes(), &expected_key);
        assert_eq!(out.ciphertext, expected_ct);
        assert_eq!(
            Hint::derive(&out.key, &TOPIC),
            Hint::from(hex!("523bcd55e968e22b"))
        );

        // Decryptor direction: recipient secret with the ephemeral public.
        let key = shared_key(&recip, &out.ephemeral, &TOPIC);
        assert_eq!(key.as_bytes(), &expected_key);
        assert_eq!(decrypt(&key, &out.ciphertext), plaintext40());
    }

    #[test]
    fn conformance_short_salt() {
        let (eph, _, recip_pub) = keys();
        let out = encrypt_with(&eph, &recip_pub, b"salt", &[0xaa; 32], None).unwrap();
        assert_eq!(
            out.key.as_bytes(),
            &hex!("7cd1824531c584a2465e3bf27b00bc94b6501de09b761541079ba4c40220b92f")
        );
        assert_eq!(
            out.ciphertext,
            hex!("48068375611c6a1ab294f597ad32f75ba37bf12d6f0dae4d9eda997075ef3551")
        );
        assert_eq!(
            Hint::derive(&out.key, b"salt"),
            Hint::from(hex!("6266792501751968"))
        );
    }

    #[test]
    fn conformance_empty_salt_empty_plaintext() {
        let (eph, _, recip_pub) = keys();
        let out = encrypt_with(&eph, &recip_pub, &[], &[], None).unwrap();
        assert_eq!(
            out.key.as_bytes(),
            &hex!("65755c74f04311cbf5701c4011ed8e9738e9e91ab261ae1a3f3ea4356f7d001f")
        );
        assert!(out.ciphertext.is_empty());
        assert_eq!(
            Hint::derive(&out.key, &[]),
            Hint::from(hex!("b82a360cac8110d6"))
        );
    }

    /// The shared x coordinate for this ephemeral scalar (the integer 308)
    /// has a leading zero byte, so the big-integer serialisation drops it
    /// before hashing. Expected values captured from the reference client.
    #[test]
    fn conformance_leading_zero_x() {
        let (_, _, recip_pub) = keys();
        let mut eph_sk = [0u8; 32];
        eph_sk[30] = 0x01;
        eph_sk[31] = 0x34;
        let eph = SecretKey::from_slice(&eph_sk).unwrap();

        let out = encrypt_with(&eph, &recip_pub, &TOPIC, &plaintext40(), None).unwrap();
        assert_eq!(
            out.key.as_bytes(),
            &hex!("c6ea100a183c558bfc0c9b0d0322c5ca3845504e78b40354f6e4c1acc5f7dbe3")
        );
        assert_eq!(
            out.ciphertext,
            hex!(
                "b84e99580a6ef91cd114ba44a4f7672b56421c753c88f0c9d221464dfec59316dbb27afaf3e267d6"
            )
        );
        assert_eq!(
            Hint::derive(&out.key, &TOPIC),
            Hint::from(hex!("946860fd5c210b9c"))
        );
    }

    #[test]
    fn roundtrip_random_ephemeral_with_padding() {
        let (_, recip, recip_pub) = keys();
        let plaintext = plaintext40();
        let out = encrypt(&recip_pub, &TOPIC, &plaintext, Some(4032)).unwrap();
        assert_eq!(out.ciphertext.len(), 4032);

        let key = shared_key(&recip, &out.ephemeral, &TOPIC);
        assert_eq!(Hint::derive(&key, &TOPIC), Hint::derive(&out.key, &TOPIC));
        let recovered = decrypt(&key, &out.ciphertext);
        assert_eq!(&recovered[..plaintext.len()], &plaintext[..]);
    }

    #[test]
    fn padding_extends_ciphertext_only() {
        let (eph, _, recip_pub) = keys();
        let plaintext = plaintext40();
        let plain = encrypt_with(&eph, &recip_pub, &TOPIC, &plaintext, None).unwrap();
        let padded = encrypt_with(&eph, &recip_pub, &TOPIC, &plaintext, Some(100)).unwrap();
        assert_eq!(padded.ciphertext.len(), 100);
        assert_eq!(&padded.ciphertext[..40], &plain.ciphertext[..]);
    }

    #[test]
    fn plaintext_longer_than_padding_errors() {
        let (eph, _, recip_pub) = keys();
        let err = encrypt_with(&eph, &recip_pub, &TOPIC, &plaintext40(), Some(39)).unwrap_err();
        assert!(matches!(
            err,
            EciesError::PlaintextTooLong {
                len: 40,
                padded: 39
            }
        ));
    }

    #[test]
    fn wrong_salt_derives_different_key() {
        let (eph, recip, recip_pub) = keys();
        let out = encrypt_with(&eph, &recip_pub, &TOPIC, &plaintext40(), None).unwrap();
        let wrong = shared_key(&recip, &out.ephemeral, b"other");
        assert_ne!(wrong.as_bytes(), out.key.as_bytes());
        assert_ne!(
            Hint::derive(&wrong, b"other"),
            Hint::derive(&out.key, &TOPIC)
        );
    }

    #[test]
    fn hint_try_from_slice() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
        assert_eq!(Hint::try_from(bytes.as_slice()).unwrap(), Hint::from(bytes));

        let err = Hint::try_from([0u8; 4].as_slice()).unwrap_err();
        assert_eq!(
            err,
            WrongLength {
                expected: Hint::SIZE,
                got: 4
            }
        );
    }

    #[test]
    fn generate_secret_produces_distinct_keys() {
        let a = generate_secret();
        let b = generate_secret();
        assert_ne!(a.to_bytes(), b.to_bytes());
    }
}
