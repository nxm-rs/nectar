//! Feed index abstraction.

/// Position of an update within a feed.
///
/// The marshalled bytes enter the update id preimage raw
/// (`keccak256(topic || marshal)`), so the byte layout is wire critical.
pub trait Index: Clone {
    /// The wire bytes appended to the topic in the id preimage.
    fn marshal(&self) -> impl AsRef<[u8]>;

    /// The following position, or `None` when the scheme is spent.
    fn next(&self) -> Option<Self>;
}
