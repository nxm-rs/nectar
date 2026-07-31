use alloc::vec::Vec;
use core::convert::Infallible;

use nectar_primitives::chunk::ChunkRef;
use nectar_testing::run;

use super::*;

/// A scripted raw walk counting the keys it served.
struct Script(Vec<Vec<u8>>, usize);

impl RawCursor<ChunkRef> for Script {
    type Error = Infallible;

    async fn next(&mut self) -> Result<Option<RawItem<ChunkRef>>, Infallible> {
        if self.0.is_empty() {
            return Ok(None);
        }
        self.1 += 1;
        Ok(Some((self.0.remove(0), MapEntry::Opaque)))
    }
}

fn keys(keys: &[&[u8]]) -> Vec<Vec<u8>> {
    keys.iter().map(|key| key.to_vec()).collect()
}

/// The paths a bounded walk over `script` yields, plus the keys it pulled.
fn walk(script: &[&[u8]], bounds: (Bound<&str>, Bound<&str>)) -> (Vec<Vec<u8>>, usize) {
    let mut cursor = PathCursor::bounded(
        Script(keys(script), 0),
        (
            bounds.0.map(ManifestPath::from),
            bounds.1.map(ManifestPath::from),
        ),
    );
    let got = run(async {
        let mut out = Vec::new();
        while let Some((path, _)) = ManifestCursor::<ChunkRef>::next(&mut cursor).await.unwrap() {
            out.push(path.into_bytes());
        }
        out
    });
    (got, cursor.raw.1)
}

#[test]
fn reserved_keys_never_surface() {
    let script: &[&[u8]] = &[b"", b"/", b"a", b"a/b"];
    let (got, _) = walk(script, (Bound::Unbounded, Bound::Unbounded));
    assert_eq!(got, keys(&[b"a", b"a/b"]));
}

#[test]
#[allow(clippy::type_complexity)]
fn bounds_filter_each_edge_kind() {
    let cases: [(Bound<&str>, Bound<&str>, &[&[u8]]); 4] = [
        (Bound::Included("b"), Bound::Included("c"), &[b"b", b"c"]),
        (Bound::Excluded("b"), Bound::Excluded("d"), &[b"c"]),
        (Bound::Unbounded, Bound::Excluded("b"), &[b"a"]),
        (Bound::Excluded("d"), Bound::Unbounded, &[]),
    ];
    for (start, end, want) in cases {
        let (got, _) = walk(&[b"a", b"b", b"c", b"d"], (start, end));
        assert_eq!(got, keys(want), "bounds {start:?}..{end:?}");
    }
}

#[test]
fn the_upper_bound_ends_the_walk_without_draining_the_raw_cursor() {
    let script: &[&[u8]] = &[b"a", b"b", b"y", b"z"];
    let (got, served) = walk(script, (Bound::Unbounded, Bound::Included("b")));
    assert_eq!(got, keys(&[b"a", b"b"]));
    // The walk stopped at "y": "z" was never pulled.
    assert_eq!(served, 3);
}
