//! Chunk address definition and operations

use std::cmp::Ordering;
use std::ops::Deref;

use crate::constants::*;
use crate::error::Result;
use alloy_primitives::{B256, U256, hex};

/// A 256 bit address for a chunk in the network
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SwarmAddress(B256);

impl SwarmAddress {
    /// Creates a new ChunkAddress from raw bytes
    pub fn new(bytes: [u8; std::mem::size_of::<Self>()]) -> Self {
        Self(B256::from_slice(&bytes))
    }

    /// Returns the underlying bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Creates a new address from a slice, checking the length
    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        let address = B256::try_from(slice)?;
        Ok(Self(address))
    }

    /// Checks if this address is zeros
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Create a new zero-filled address
    pub fn zero() -> Self {
        Self(B256::ZERO)
    }

    /// Calculate the distance between Self and address `y` in big-endian
    #[inline(always)]
    pub fn distance(&self, y: &Self) -> U256 {
        let mut result = [0u8; std::mem::size_of::<SwarmAddress>()];

        for (i, (&a, &b)) in self
            .0
            .as_slice()
            .iter()
            .zip(y.0.as_slice().iter())
            .enumerate()
        {
            result[i] = a ^ b;
        }

        U256::from_be_bytes(result)
    }

    /// Compares `x` and `y` to self in terms of the distance metric defined in the Swarm specification.
    /// It returns:
    ///   - `Ordering::Greater` if `self` is closer to `x` than `y`
    ///   - `Ordering::Less` if `self` is farther from `x` than `y`
    ///   - `Ordering::Equal` if `self` and `y` are equally close to `x`
    #[inline(always)]
    pub fn distance_cmp(&self, x: &Self, y: &Self) -> Ordering {
        let (ab, xb, yb) = (self.0.0, x.0.0, y.0.0);

        for i in 0..ab.len() {
            let dx = xb[i] ^ ab[i];
            let dy = yb[i] ^ ab[i];

            if dx != dy {
                return match dx < dy {
                    true => Ordering::Greater,
                    false => Ordering::Less,
                };
            }
        }

        Ordering::Equal
    }

    /// Determine if self is closer to `a` than `y`
    pub fn closer(&self, x: &Self, y: &Self) -> bool {
        self.distance_cmp(x, y) == Ordering::Less
    }

    #[inline(always)]
    pub fn proximity(&self, other: &Self) -> u8 {
        self.proximity_helper(other, MAX_PO)
    }

    #[inline(always)]
    pub fn extended_proximity(&self, other: &Self) -> u8 {
        self.proximity_helper(other, EXTENDED_PO)
    }

    /// Proximity returns the proximity order of the MSB distance between `x` and `y`
    ///
    /// The distance metric MSB(x, y) of two equal length byte sequences `x` and `y`
    /// is the value of the binary integer cast of the x^y, ie., `x` and `y` bitwise
    /// xor-ed. The binary cast is big endian: most significant bit first (MSB).
    ///
    /// Proximity(x, y) is a discrete logarithmic scaling of the MSB distance.
    /// It is defined as the reverse rank of the integer part of the base 2 logarithm
    /// of the distance.
    ///
    /// It is calculated by counting the number of common leading zeros in the (MSB)
    /// binary representation of the x ^ y.
    #[inline(always)]
    fn proximity_helper(&self, other: &Self, max: usize) -> u8 {
        let max_bytes = max / 8;
        let max_bits = max as u8;

        let bytes1 = self.0.as_slice();
        let bytes2 = other.0.as_slice();

        for i in 0..=max_bytes {
            let xor = bytes1[i] ^ bytes2[i];
            if xor != 0 {
                // Found a difference - use leading_zeros to count matching bits
                let leading_zeros = xor.leading_zeros() as u8;
                let proximity = (i as u8 * 8) + leading_zeros;

                // Return the smaller of proximity or max_bits
                return if proximity < max_bits {
                    proximity
                } else {
                    max_bits
                };
            }

            // If we're at the last byte we might need to check
            if i == max_bytes {
                return max_bits; // All bits match up to max
            }
        }

        // If we've examined all bytes and found no differences
        max_bits
    }
}

impl Default for SwarmAddress {
    fn default() -> Self {
        Self(B256::ZERO)
    }
}

impl std::fmt::Display for SwarmAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0.as_slice()[..8]))
    }
}

impl Deref for SwarmAddress {
    type Target = B256;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<B256> for SwarmAddress {
    fn from(value: B256) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{FixedBytes, b256};
    use std::{cmp::Ordering, str::FromStr};

    #[test]
    fn distance_closer() {
        let a: SwarmAddress =
            b256!("9100000000000000000000000000000000000000000000000000000000000000").into();
        let x: SwarmAddress =
            b256!("8200000000000000000000000000000000000000000000000000000000000000").into();
        let y: SwarmAddress =
            b256!("1200000000000000000000000000000000000000000000000000000000000000").into();

        assert!(!x.closer(&a, &y));
    }

    #[test]
    fn distance_matches() {
        let x: SwarmAddress =
            b256!("9100000000000000000000000000000000000000000000000000000000000000").into();
        let y: SwarmAddress =
            b256!("8200000000000000000000000000000000000000000000000000000000000000").into();

        assert_eq!(
            x.distance(&y),
            U256::from_str(
                "8593944123082061379093159043613555660984881674403010612303492563087302590464"
            )
            .unwrap()
        );
    }

    macro_rules! distance_cmp_test {
        ($test_name:ident, $ordering:expr, $a:expr, $x:expr, $y:expr) => {
            #[test]
            fn $test_name() {
                let a: SwarmAddress = b256!($a).into();
                assert_eq!(
                    a.distance_cmp(&b256!($x).into(), &b256!($y).into()),
                    $ordering
                );
            }
        };
    }

    distance_cmp_test!(
        distance_cmp_eq,
        Ordering::Equal,
        "9100000000000000000000000000000000000000000000000000000000000000",
        "1200000000000000000000000000000000000000000000000000000000000000",
        "1200000000000000000000000000000000000000000000000000000000000000"
    );

    distance_cmp_test!(
        distance_cmp_lt,
        Ordering::Less,
        "9100000000000000000000000000000000000000000000000000000000000000",
        "1200000000000000000000000000000000000000000000000000000000000000",
        "8200000000000000000000000000000000000000000000000000000000000000"
    );

    distance_cmp_test!(
        distance_cmp_gt,
        Ordering::Greater,
        "9100000000000000000000000000000000000000000000000000000000000000",
        "8200000000000000000000000000000000000000000000000000000000000000",
        "1200000000000000000000000000000000000000000000000000000000000000"
    );

    // Function to limit the proximity to MAX_PO
    const fn limit_po(po: usize) -> usize {
        if po > MAX_PO { MAX_PO } else { po }
    }

    /// Table-driven test case structure
    struct TestCase {
        addr: FixedBytes<32>,
        expected_po: usize,
    }

    /// Macro for generating the test cases
    macro_rules! proximity_test_cases {
        ($($addr:expr => $po:expr),* $(,)?) => {
            &[
                $(
                    TestCase {
                        addr: $addr,
                        expected_po: limit_po($po),
                    }
                ),*
            ]
        };
    }

    #[test]
    fn test_proximity() {
        let test_cases = proximity_test_cases!(
            // All zeros matches completely with itself (MAX_PO)
            b256!("0000000000000000000000000000000000000000000000000000000000000000") => MAX_PO,

            // First bit set (binary: 10000000...) => proximity 0
            b256!("8000000000000000000000000000000000000000000000000000000000000000") => 0,

            // Second bit set (binary: 01000000...) => proximity 1
            b256!("4000000000000000000000000000000000000000000000000000000000000000") => 1,

            // Third bit set (binary: 00100000...) => proximity 2
            b256!("2000000000000000000000000000000000000000000000000000000000000000") => 2,

            // Fourth bit set (binary: 00010000...) => proximity 3
            b256!("1000000000000000000000000000000000000000000000000000000000000000") => 3,

            // Fifth bit set (binary: 00001000...) => proximity 4
            b256!("0800000000000000000000000000000000000000000000000000000000000000") => 4,

            // Sixth bit set (binary: 00000100...) => proximity 5
            b256!("0400000000000000000000000000000000000000000000000000000000000000") => 5,

            // Seventh bit set (binary: 00000010...) => proximity 6
            b256!("0200000000000000000000000000000000000000000000000000000000000000") => 6,

            // Eighth bit set (binary: 00000001...) => proximity 7
            b256!("0100000000000000000000000000000000000000000000000000000000000000") => 7,

            // Ninth bit set (binary: 00000000 10000000...) => proximity 8
            b256!("0080000000000000000000000000000000000000000000000000000000000000") => 8,

            // Tenth bit set => proximity 9
            b256!("0040000000000000000000000000000000000000000000000000000000000000") => 9,

            // Eleventh bit set => proximity 10
            b256!("0020000000000000000000000000000000000000000000000000000000000000") => 10,

            // Twelfth bit set => proximity 11
            b256!("0010000000000000000000000000000000000000000000000000000000000000") => 11,

            // Thirteenth bit set => proximity 12
            b256!("0008000000000000000000000000000000000000000000000000000000000000") => 12,

            // Fourteenth bit set => proximity 13
            b256!("0004000000000000000000000000000000000000000000000000000000000000") => 13,

            // Fifteenth bit set => proximity 14
            b256!("0002000000000000000000000000000000000000000000000000000000000000") => 14,

            // Sixteenth bit set => proximity 15
            b256!("0001000000000000000000000000000000000000000000000000000000000000") => 15,

            // Seventeenth bit set => proximity 16
            b256!("0000800000000000000000000000000000000000000000000000000000000000") => 16,

            // Eighteenth bit set => proximity 17
            b256!("0000400000000000000000000000000000000000000000000000000000000000") => 17,

            // Nineteenth bit set => proximity 18
            b256!("0000200000000000000000000000000000000000000000000000000000000000") => 18,

            // Twentieth bit set => proximity 19
            b256!("0000100000000000000000000000000000000000000000000000000000000000") => 19,

            // Twenty-first bit set => proximity 20
            b256!("0000080000000000000000000000000000000000000000000000000000000000") => 20,

            // Twenty-second bit set => proximity 21
            b256!("0000040000000000000000000000000000000000000000000000000000000000") => 21,

            // Twenty-third bit set => proximity 22
            b256!("0000020000000000000000000000000000000000000000000000000000000000") => 22,

            // Twenty-fourth bit set => proximity 23
            b256!("0000010000000000000000000000000000000000000000000000000000000000") => 23,

            // Twenty-fifth bit set => proximity 24
            b256!("0000008000000000000000000000000000000000000000000000000000000000") => 24,

            // Twenty-sixth bit set => proximity 25
            b256!("0000004000000000000000000000000000000000000000000000000000000000") => 25,

            // Twenty-seventh bit set => proximity 26
            b256!("0000002000000000000000000000000000000000000000000000000000000000") => 26,

            // Twenty-eighth bit set => proximity 27
            b256!("0000001000000000000000000000000000000000000000000000000000000000") => 27,

            // Twenty-ninth bit set => proximity 28
            b256!("0000000800000000000000000000000000000000000000000000000000000000") => 28,

            // Thirtieth bit set => proximity 29
            b256!("0000000400000000000000000000000000000000000000000000000000000000") => 29,

            // Thirty-first bit set => proximity 30
            b256!("0000000200000000000000000000000000000000000000000000000000000000") => 30,

            // Thirty-second bit set => proximity 31
            b256!("0000000100000000000000000000000000000000000000000000000000000000") => 31,

            // Last test case: bit set at position 131 (16 bytes + 3 bits in) => proximity should be 31 (MAX_PO)
            b256!("0000000000000000000020000000000000000000000000000000000000000000") => 31,
        );

        let base = SwarmAddress::from(B256::ZERO);

        for tc in test_cases {
            let tc_addr = SwarmAddress::from(tc.addr);
            let got = base.proximity(&tc_addr) as usize;
            assert_eq!(
                got, tc.expected_po,
                "Test failed for addr: {:?}, got {}, expected {}",
                tc.addr, got, tc.expected_po
            );

            let got_reverse = tc_addr.proximity(&base) as usize;
            assert_eq!(
                got_reverse, tc.expected_po,
                "Test failed for reversed addr: {:?}, got {}, expected {}",
                tc.addr, got_reverse, tc.expected_po
            );
        }
    }
}
