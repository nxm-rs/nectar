//! Typed unix-seconds timestamp used in BzzAddress sign-data.
//!
//! The bee handshake sign-data carries an `int64` timestamp (big-endian,
//! signed). Verification rejects records whose timestamp drifts outside a
//! caller-supplied window from local clock. See bee `pkg/bzz/timestamp.go`.

use derive_more::{Display, From, Into};
use std::time::Duration;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Unix-seconds timestamp (signed, matching bee's `int64`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[display("{_0}")]
pub struct Timestamp(i64);

/// Errors from timestamp validation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TimestampError {
    /// The remote timestamp drifted outside the accepted window.
    #[error("timestamp drifted by {drift_seconds}s (window ±{window_seconds}s)")]
    OutsideSkewWindow {
        /// Signed drift `remote - local` in seconds. Positive = future-dated.
        drift_seconds: i64,
        /// Configured tolerance window (`|drift_seconds|` must be `<= window_seconds`).
        window_seconds: i64,
    },
}

impl Timestamp {
    /// Zero timestamp (1970-01-01T00:00:00Z).
    pub const ZERO: Self = Self(0);

    /// Construct from raw seconds.
    #[inline]
    pub const fn from_seconds(s: i64) -> Self {
        Self(s)
    }

    /// Underlying signed seconds.
    #[inline]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Eight-byte big-endian representation (used in the BzzAddress sign-data).
    #[inline]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Capture the current wall-clock time as a [`Timestamp`].
    ///
    /// The clock comes from `web-time`, which is `std::time` on native targets
    /// and the browser clock on `wasm32`, so this runs on every supported
    /// target instead of panicking through the std unsupported-platform stub.
    ///
    /// Panics only if the system clock is set before the unix epoch, which
    /// would already break far more than this primitive. Pre-1970 callers
    /// can construct via [`Self::from_seconds`] manually.
    #[allow(clippy::expect_used)] // documented invariants: panics only on a pre-1970 or absurdly far-future system clock
    pub fn now() -> Self {
        use web_time::{SystemTime, UNIX_EPOCH};
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock set before unix epoch")
            .as_secs();
        // u64 -> i64: safe for the next ~290 billion years.
        Self(i64::try_from(secs).expect("system clock exceeds i64 unix seconds"))
    }

    /// Verify this timestamp is within `window` of `local`.
    ///
    /// Both `self` and `local` are interpreted as unix-seconds; the absolute
    /// difference must be `<= window.as_secs()`.
    pub fn skew_check(self, local: Self, window: Duration) -> Result<(), TimestampError> {
        let drift = self.0.saturating_sub(local.0);
        let window_secs = i64::try_from(window.as_secs()).unwrap_or(i64::MAX);
        if drift.unsigned_abs() <= window_secs.unsigned_abs() {
            Ok(())
        } else {
            Err(TimestampError::OutsideSkewWindow {
                drift_seconds: drift,
                window_seconds: window_secs,
            })
        }
    }
}

#[cfg(any(test, feature = "arbitrary"))]
impl<'a> arbitrary::Arbitrary<'a> for Timestamp {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Any i64 is representable, including pre-1970 values; validity
        // policies (skew windows) are the caller's concern.
        Ok(Self::from_seconds(u.arbitrary()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skew_check_within_window() {
        let local = Timestamp::from_seconds(1_000_000);
        let remote = Timestamp::from_seconds(1_000_030); // +30s
        assert!(remote.skew_check(local, Duration::from_secs(60)).is_ok());
    }

    #[test]
    fn skew_check_negative_within_window() {
        let local = Timestamp::from_seconds(1_000_000);
        let remote = Timestamp::from_seconds(999_940); // -60s
        assert!(remote.skew_check(local, Duration::from_secs(60)).is_ok());
    }

    #[test]
    fn skew_check_outside_window() {
        let local = Timestamp::from_seconds(1_000_000);
        let remote = Timestamp::from_seconds(1_000_120); // +120s
        let err = remote
            .skew_check(local, Duration::from_secs(60))
            .unwrap_err();
        assert!(matches!(
            err,
            TimestampError::OutsideSkewWindow {
                drift_seconds: 120,
                window_seconds: 60
            }
        ));
    }

    #[test]
    fn be_bytes_signed() {
        let t = Timestamp::from_seconds(-1);
        assert_eq!(t.to_be_bytes(), [0xff; 8]);
        let t = Timestamp::from_seconds(1);
        assert_eq!(t.to_be_bytes(), [0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn now_is_positive() {
        assert!(Timestamp::now().get() > 1_700_000_000);
    }
}
