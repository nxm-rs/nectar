//! Chunk-level encryption and decryption.
//!
//! Encrypts a chunk's span and data separately with different initial counters,
//! matching Go's `ChunkEncrypter` in `pkg/encryption/chunk_encryption.go`.

use crate::bmt::SPAN_SIZE;

use super::cipher::transcrypt;
use super::error::EncryptionError;
use super::key::EncryptionKey;
use super::KEY_SIZE;

/// Encrypt chunk data (span + body), returning the key and ciphertext.
///
/// The output is always `SPAN_SIZE + BODY_SIZE` bytes: the span is encrypted
/// with `init_ctr = BODY_SIZE / KEY_SIZE`, and the data is encrypted with
/// `init_ctr = 0` and padded to `BODY_SIZE` with random bytes.
///
/// `chunk_data` must be `SPAN_SIZE..=SPAN_SIZE + BODY_SIZE` bytes.
#[cfg(feature = "encryption")]
pub fn encrypt_chunk<const BODY_SIZE: usize>(
    chunk_data: &[u8],
) -> Result<(EncryptionKey, Vec<u8>), EncryptionError> {
    if chunk_data.len() < SPAN_SIZE {
        return Err(EncryptionError::DataTooShort {
            len: chunk_data.len(),
            min: SPAN_SIZE,
        });
    }
    if chunk_data.len() > SPAN_SIZE + BODY_SIZE {
        return Err(EncryptionError::DataTooLong {
            len: chunk_data.len(),
            max: SPAN_SIZE + BODY_SIZE,
        });
    }

    let key = EncryptionKey::generate();
    let span = &chunk_data[..SPAN_SIZE];
    let data = &chunk_data[SPAN_SIZE..];

    let mut output = vec![0u8; SPAN_SIZE + BODY_SIZE];

    // Encrypt span with init_ctr = BODY_SIZE / KEY_SIZE (128 for default 4096)
    let span_ctr = (BODY_SIZE / KEY_SIZE) as u32;
    transcrypt(&key, span_ctr, span, &mut output[..SPAN_SIZE]);

    // Encrypt data with init_ctr = 0, pad remainder with random bytes
    transcrypt(&key, 0, data, &mut output[SPAN_SIZE..]);

    // Fill padding beyond actual data with random bytes
    let padding_start = SPAN_SIZE + data.len();
    if padding_start < output.len() {
        use rand::Rng;
        rand::rng().fill(&mut output[padding_start..]);
    }

    Ok((key, output))
}

/// Decrypt encrypted chunk data, returning `span || data[..data_length]`.
///
/// `encrypted_data` must be exactly `SPAN_SIZE + BODY_SIZE` bytes.
/// `data_length` specifies the actual data length (excluding padding).
pub fn decrypt_chunk_data<const BODY_SIZE: usize>(
    encrypted_data: &[u8],
    key: &EncryptionKey,
    data_length: usize,
) -> Result<Vec<u8>, EncryptionError> {
    let expected_len = SPAN_SIZE + BODY_SIZE;
    if encrypted_data.len() != expected_len {
        return Err(EncryptionError::DataTooShort {
            len: encrypted_data.len(),
            min: expected_len,
        });
    }
    if data_length > BODY_SIZE {
        return Err(EncryptionError::DataTooLong {
            len: data_length,
            max: BODY_SIZE,
        });
    }

    let enc_span = &encrypted_data[..SPAN_SIZE];
    let enc_data = &encrypted_data[SPAN_SIZE..SPAN_SIZE + data_length];

    let mut output = vec![0u8; SPAN_SIZE + data_length];

    // Decrypt span with init_ctr = BODY_SIZE / KEY_SIZE
    let span_ctr = (BODY_SIZE / KEY_SIZE) as u32;
    transcrypt(key, span_ctr, enc_span, &mut output[..SPAN_SIZE]);

    // Decrypt data with init_ctr = 0
    transcrypt(key, 0, enc_data, &mut output[SPAN_SIZE..]);

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bmt::DEFAULT_BODY_SIZE;

    #[cfg(feature = "encryption")]
    #[test]
    fn roundtrip_full_chunk() {
        let mut chunk_data = vec![0u8; SPAN_SIZE + DEFAULT_BODY_SIZE];
        // Set span to the data length
        let data_len = DEFAULT_BODY_SIZE as u64;
        chunk_data[..SPAN_SIZE].copy_from_slice(&data_len.to_le_bytes());
        // Fill data with a pattern
        for (i, byte) in chunk_data[SPAN_SIZE..].iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let (key, encrypted) = encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data).unwrap();
        assert_eq!(encrypted.len(), SPAN_SIZE + DEFAULT_BODY_SIZE);
        assert_ne!(&encrypted[..], &chunk_data[..]);

        let decrypted =
            decrypt_chunk_data::<DEFAULT_BODY_SIZE>(&encrypted, &key, DEFAULT_BODY_SIZE).unwrap();
        assert_eq!(decrypted, chunk_data);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn roundtrip_small_data() {
        let data_len = 100usize;
        let mut chunk_data = vec![0u8; SPAN_SIZE + data_len];
        chunk_data[..SPAN_SIZE].copy_from_slice(&(data_len as u64).to_le_bytes());
        for (i, byte) in chunk_data[SPAN_SIZE..].iter_mut().enumerate() {
            *byte = (i * 7 % 256) as u8;
        }

        let (key, encrypted) = encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data).unwrap();
        // Output is always full size
        assert_eq!(encrypted.len(), SPAN_SIZE + DEFAULT_BODY_SIZE);

        let decrypted =
            decrypt_chunk_data::<DEFAULT_BODY_SIZE>(&encrypted, &key, data_len).unwrap();
        assert_eq!(decrypted, chunk_data);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn roundtrip_span_only() {
        let chunk_data = 0u64.to_le_bytes().to_vec(); // 8 bytes, no data
        let (key, encrypted) = encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data).unwrap();
        assert_eq!(encrypted.len(), SPAN_SIZE + DEFAULT_BODY_SIZE);

        let decrypted = decrypt_chunk_data::<DEFAULT_BODY_SIZE>(&encrypted, &key, 0).unwrap();
        assert_eq!(decrypted, chunk_data);
    }

    #[test]
    fn encrypt_too_short() {
        let short = [0u8; 4]; // Less than SPAN_SIZE
        #[cfg(feature = "encryption")]
        {
            let err = encrypt_chunk::<DEFAULT_BODY_SIZE>(&short).unwrap_err();
            assert!(matches!(
                err,
                EncryptionError::DataTooShort { len: 4, min: 8 }
            ));
        }
        let _ = short; // suppress unused warning without feature
    }

    #[test]
    fn decrypt_wrong_size() {
        let key = EncryptionKey::from([0u8; 32]);
        let short = [0u8; 100];
        let err = decrypt_chunk_data::<DEFAULT_BODY_SIZE>(&short, &key, 0).unwrap_err();
        assert!(matches!(err, EncryptionError::DataTooShort { .. }));
    }

    #[test]
    fn span_uses_different_counter_than_data() {
        // Verify the span and data are encrypted with different counters by
        // checking that encrypting span bytes at position 0 with data counter
        // gives different results than span counter.
        let key = EncryptionKey::from([0x42; 32]);
        let span = [0u8; SPAN_SIZE];

        let mut with_data_ctr = [0u8; SPAN_SIZE];
        let mut with_span_ctr = [0u8; SPAN_SIZE];

        transcrypt(&key, 0, &span, &mut with_data_ctr);
        transcrypt(
            &key,
            (DEFAULT_BODY_SIZE / KEY_SIZE) as u32,
            &span,
            &mut with_span_ctr,
        );

        assert_ne!(with_data_ctr, with_span_ctr);
    }
}
