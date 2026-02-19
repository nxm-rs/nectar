//! Tree traversal for mantaray nodes.

extern crate alloc;

use alloc::vec::Vec;

use crate::error::Result;
use crate::node::Node;
use crate::persist::MantarayLoader;

/// Walk the trie depth-first starting from a sub-path, calling `f` for each node.
///
/// Looks up the node at `root` first, then walks the subtree rooted there.
/// This matches Go bee's `WalkNode` method.
pub fn walk_node<F>(
    node: &mut Node,
    root: &[u8],
    loader: Option<&dyn MantarayLoader>,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8], &Node) -> Result<()>,
{
    if root.is_empty() {
        return walk_inner(&[], node, loader, f);
    }

    let target = node.lookup_node(root, loader)?;
    walk_inner(root, target, loader, f)
}

/// Walk the trie depth-first, calling `f` for each node with its accumulated path.
pub fn walk<F>(
    node: &mut Node,
    loader: Option<&dyn MantarayLoader>,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8], &Node) -> Result<()>,
{
    walk_inner(&[], node, loader, f)
}

fn walk_inner<F>(
    path: &[u8],
    node: &mut Node,
    loader: Option<&dyn MantarayLoader>,
    f: &mut F,
) -> Result<()>
where
    F: FnMut(&[u8], &Node) -> Result<()>,
{
    if node.forks.is_empty() {
        node.load(loader)?;
    }

    f(path, node)?;

    // collect keys to avoid borrow conflict
    let keys: Vec<u8> = node.forks.keys().copied().collect();
    for key in keys {
        let fork = node.forks.get_mut(&key).expect("key from iterator");
        let mut next_path = path.to_vec();
        next_path.extend_from_slice(&fork.prefix);
        walk_inner(&next_path, &mut fork.node, loader, f)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use alloc::collections::BTreeMap;
    use alloc::vec;

    use super::*;
    use crate::persist::MockStoreCell;

    fn make_entry(s: &[u8]) -> Vec<u8> {
        let mut entry = vec![0u8; 32 - s.len()];
        entry.extend_from_slice(s);
        entry
    }

    #[test]
    fn walk_visits_all_nodes() {
        let mut root = Node::default();

        let paths = &["index.html", "img/1.png", "img/2.png", "robots.txt"];
        for &p in paths {
            let entry = make_entry(p.as_bytes());
            root.add(p.as_bytes(), &entry, BTreeMap::new(), None)
                .unwrap();
        }

        let mut visited: Vec<(Vec<u8>, bool)> = Vec::new();
        walk(&mut root, None, &mut |path, node| {
            visited.push((path.to_vec(), node.is_value_type()));
            Ok(())
        })
        .unwrap();

        // all value paths should be visited
        for &p in paths {
            assert!(
                visited
                    .iter()
                    .any(|(vp, is_val)| vp == p.as_bytes() && *is_val),
                "path {p} not visited as value"
            );
        }
    }

    // --- Go bee compatibility: TestWalkNode with exact traversal order ---

    /// Replicates the Go bee walker test with exact expected walk order.
    /// Uses `walk_node` with empty root, matching Go's `n.WalkNode(ctx, []byte{}, nil, walker)`.
    #[test]
    fn walk_node_exact_order() {
        let to_add: &[&[u8]] = &[
            b"index.html.backup",
            b"index.html",
            b"img/test/oho.png",
            b"img/test/old/test.png.backup",
            b"img/test/old/test.png",
            b"img/2.png",
            b"img/1.png",
            b"robots.txt",
        ];

        let expected: &[&[u8]] = &[
            b"",
            b"i",
            b"img/",
            b"img/1.png",
            b"img/2.png",
            b"img/test/o",
            b"img/test/oho.png",
            b"img/test/old/test.png",
            b"img/test/old/test.png.backup",
            b"index.html",
            b"index.html.backup",
            b"robots.txt",
        ];

        let mut n = Node::default();
        for &path in to_add {
            let entry = make_entry(path);
            n.add(path, &entry, BTreeMap::new(), None).unwrap();
        }

        let mut walked: Vec<Vec<u8>> = Vec::new();
        walk_node(&mut n, b"", None, &mut |path, _node| {
            walked.push(path.to_vec());
            Ok(())
        })
        .unwrap();

        assert_eq!(
            walked.len(),
            expected.len(),
            "expected {} nodes, got {}",
            expected.len(),
            walked.len()
        );

        for (i, (got, &want)) in walked.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                got.as_slice(),
                want,
                "walk step {i}: expected {:?}, got {:?}",
                core::str::from_utf8(want).unwrap_or("<non-utf8>"),
                core::str::from_utf8(got).unwrap_or("<non-utf8>"),
            );
        }
    }

    /// Verify `walk_node` with a non-empty root only walks the subtree.
    #[test]
    fn walk_node_from_subtree() {
        let to_add: &[&[u8]] = &[
            b"index.html",
            b"img/1.png",
            b"img/2.png",
            b"robots.txt",
        ];

        let mut n = Node::default();
        for &path in to_add {
            let entry = make_entry(path);
            n.add(path, &entry, BTreeMap::new(), None).unwrap();
        }

        let mut walked: Vec<Vec<u8>> = Vec::new();
        walk_node(&mut n, b"img/", None, &mut |path, _node| {
            walked.push(path.to_vec());
            Ok(())
        })
        .unwrap();

        // should visit the img/ node and its children
        assert!(walked.iter().any(|p| p == b"img/1.png"));
        assert!(walked.iter().any(|p| p == b"img/2.png"));
        // should NOT visit the root or other branches
        assert!(!walked.iter().any(|p| p == b"index.html"));
        assert!(!walked.iter().any(|p| p == b"robots.txt"));
    }

    /// Same as above but with save/load through a mock store,
    /// matching Go's "with load save" walker test variant.
    #[test]
    fn walk_node_exact_order_with_load_save() {
        let to_add: &[&[u8]] = &[
            b"index.html.backup",
            b"index.html",
            b"img/test/oho.png",
            b"img/test/old/test.png.backup",
            b"img/test/old/test.png",
            b"img/2.png",
            b"img/1.png",
            b"robots.txt",
        ];

        let expected: &[&[u8]] = &[
            b"",
            b"i",
            b"img/",
            b"img/1.png",
            b"img/2.png",
            b"img/test/o",
            b"img/test/oho.png",
            b"img/test/old/test.png",
            b"img/test/old/test.png.backup",
            b"index.html",
            b"index.html.backup",
            b"robots.txt",
        ];

        let mut n = Node::default();
        for &path in to_add {
            let entry = make_entry(path);
            n.add(path, &entry, BTreeMap::new(), None).unwrap();
        }

        let store = MockStoreCell::new();
        n.save(&store).unwrap();

        let mut n2 = Node::new_node_ref(&n.ref_);

        let mut walked: Vec<Vec<u8>> = Vec::new();
        walk_node(&mut n2, b"", Some(&store), &mut |path, _node| {
            walked.push(path.to_vec());
            Ok(())
        })
        .unwrap();

        assert_eq!(
            walked.len(),
            expected.len(),
            "expected {} nodes, got {}",
            expected.len(),
            walked.len()
        );

        for (i, (got, &want)) in walked.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                got.as_slice(),
                want,
                "walk step {i}: expected {:?}, got {:?}",
                core::str::from_utf8(want).unwrap_or("<non-utf8>"),
                core::str::from_utf8(got).unwrap_or("<non-utf8>"),
            );
        }
    }
}
