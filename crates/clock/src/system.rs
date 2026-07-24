use web_time::{SystemTime, UNIX_EPOCH};

use crate::Clock;

/// System wall clock: `std::time` on native targets, the browser clock on
/// wasm32.
///
/// Readings saturate at the `i64` nanosecond range (years 1677 to 2262).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(since) => i64::try_from(since.as_nanos()).unwrap_or(i64::MAX),
            Err(err) => {
                i64::try_from(err.duration().as_nanos()).map_or(i64::MIN, i64::wrapping_neg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_after_2023() {
        let clock = SystemClock;
        assert!(clock.now_ns() > 1_700_000_000 * 1_000_000_000);
        assert!(clock.now_secs() > 1_700_000_000);
    }
}
