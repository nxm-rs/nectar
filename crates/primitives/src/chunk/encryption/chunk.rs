//! Chunk-level encryption and decryption.
//!
//! Encrypts a chunk's span and data separately with different initial counters,
//! matching Go's `ChunkEncrypter` in `pkg/encryption/chunk_encryption.go`.

use crate::bmt::SPAN_SIZE;

use super::cipher::transcrypt;
use super::error::EncryptionError;
use super::key::EncryptionKey;

/// Span encryption counter: `BODY_SIZE / EncryptionKey::SIZE` (128 for default 4096).
const fn span_ctr(body_size: usize) -> u32 {
    (body_size / EncryptionKey::SIZE) as u32
}

/// Encrypt chunk data (span + body) with the given key, returning ciphertext.
///
/// The output is always `SPAN_SIZE + BODY_SIZE` bytes: the span is encrypted
/// with `init_ctr = BODY_SIZE / EncryptionKey::SIZE`, and the data is encrypted with
/// `init_ctr = 0` and padded to `BODY_SIZE` with random bytes.
///
/// `chunk_data` must be `SPAN_SIZE..=SPAN_SIZE + BODY_SIZE` bytes.
#[cfg(feature = "encryption")]
pub(crate) fn encrypt_chunk<const BODY_SIZE: usize>(
    chunk_data: &[u8],
    key: &EncryptionKey,
) -> Result<Vec<u8>, EncryptionError> {
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

    let span = &chunk_data[..SPAN_SIZE];
    let data = &chunk_data[SPAN_SIZE..];

    let mut output = vec![0u8; SPAN_SIZE + BODY_SIZE];

    // Encrypt span with init_ctr = BODY_SIZE / EncryptionKey::SIZE (128 for default 4096)
    transcrypt(key, span_ctr(BODY_SIZE), span, &mut output[..SPAN_SIZE])?;

    // Encrypt data with init_ctr = 0
    transcrypt(key, 0, data, &mut output[SPAN_SIZE..])?;

    // Fill padding beyond actual data with random bytes
    let padding_start = SPAN_SIZE + data.len();
    if padding_start < output.len() {
        use rand::Rng;
        rand::rng().fill(&mut output[padding_start..]);
    }

    Ok(output)
}

/// Decrypt encrypted chunk data, returning `span || data[..data_length]`.
///
/// `encrypted_data` must be exactly `SPAN_SIZE + BODY_SIZE` bytes.
/// `data_length` specifies the actual data length (excluding padding).
pub(crate) fn decrypt_chunk_data<const BODY_SIZE: usize>(
    encrypted_data: &[u8],
    key: &EncryptionKey,
    data_length: usize,
) -> Result<Vec<u8>, EncryptionError> {
    let mut output = vec![0u8; SPAN_SIZE + data_length];
    decrypt_chunk_into::<BODY_SIZE>(encrypted_data, key, data_length, &mut output)?;
    Ok(output)
}

/// Decrypt encrypted chunk data into a caller-provided buffer.
///
/// `output` must be at least `SPAN_SIZE + data_length` bytes.
/// `encrypted_data` must be exactly `SPAN_SIZE + BODY_SIZE` bytes.
pub(crate) fn decrypt_chunk_into<const BODY_SIZE: usize>(
    encrypted_data: &[u8],
    key: &EncryptionKey,
    data_length: usize,
    output: &mut [u8],
) -> Result<(), EncryptionError> {
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

    let required = SPAN_SIZE + data_length;
    if output.len() < required {
        return Err(EncryptionError::OutputBufferTooSmall {
            len: output.len(),
            required,
        });
    }

    // Decrypt span
    transcrypt(
        key,
        span_ctr(BODY_SIZE),
        &encrypted_data[..SPAN_SIZE],
        &mut output[..SPAN_SIZE],
    )?;

    // Decrypt data (only the actual data, not padding)
    transcrypt(
        key,
        0,
        &encrypted_data[SPAN_SIZE..SPAN_SIZE + data_length],
        &mut output[SPAN_SIZE..SPAN_SIZE + data_length],
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bmt::DEFAULT_BODY_SIZE;

    #[cfg(feature = "encryption")]
    #[test]
    fn roundtrip_full_chunk() {
        let mut chunk_data = vec![0u8; SPAN_SIZE + DEFAULT_BODY_SIZE];
        let data_len = DEFAULT_BODY_SIZE as u64;
        chunk_data[..SPAN_SIZE].copy_from_slice(&data_len.to_le_bytes());
        for (i, byte) in chunk_data[SPAN_SIZE..].iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let key = EncryptionKey::generate();
        let encrypted = encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data, &key).unwrap();
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

        let key = EncryptionKey::generate();
        let encrypted = encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data, &key).unwrap();
        assert_eq!(encrypted.len(), SPAN_SIZE + DEFAULT_BODY_SIZE);

        let decrypted =
            decrypt_chunk_data::<DEFAULT_BODY_SIZE>(&encrypted, &key, data_len).unwrap();
        assert_eq!(decrypted, chunk_data);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn roundtrip_span_only() {
        let chunk_data = 0u64.to_le_bytes().to_vec();
        let key = EncryptionKey::generate();
        let encrypted = encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data, &key).unwrap();
        assert_eq!(encrypted.len(), SPAN_SIZE + DEFAULT_BODY_SIZE);

        let decrypted = decrypt_chunk_data::<DEFAULT_BODY_SIZE>(&encrypted, &key, 0).unwrap();
        assert_eq!(decrypted, chunk_data);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn decrypt_into_avoids_allocation() {
        let data_len = 512usize;
        let mut chunk_data = vec![0u8; SPAN_SIZE + data_len];
        chunk_data[..SPAN_SIZE].copy_from_slice(&(data_len as u64).to_le_bytes());
        for (i, byte) in chunk_data[SPAN_SIZE..].iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }

        let key = EncryptionKey::generate();
        let encrypted = encrypt_chunk::<DEFAULT_BODY_SIZE>(&chunk_data, &key).unwrap();

        // Decrypt into a pre-allocated buffer
        let mut buf = vec![0u8; SPAN_SIZE + data_len];
        decrypt_chunk_into::<DEFAULT_BODY_SIZE>(&encrypted, &key, data_len, &mut buf).unwrap();
        assert_eq!(buf, chunk_data);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypt_too_short() {
        let short = [0u8; 4];
        let key = EncryptionKey::generate();
        let err = encrypt_chunk::<DEFAULT_BODY_SIZE>(&short, &key).unwrap_err();
        assert!(matches!(
            err,
            EncryptionError::DataTooShort { len: 4, min: 8 }
        ));
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
        let key = EncryptionKey::from([0x42; 32]);
        let span = [0u8; SPAN_SIZE];

        let mut with_data_ctr = [0u8; SPAN_SIZE];
        let mut with_span_ctr = [0u8; SPAN_SIZE];

        transcrypt(&key, 0, &span, &mut with_data_ctr).unwrap();
        transcrypt(
            &key,
            (DEFAULT_BODY_SIZE / EncryptionKey::SIZE) as u32,
            &span,
            &mut with_span_ctr,
        )
        .unwrap();

        assert_ne!(with_data_ctr, with_span_ctr);
    }
}
