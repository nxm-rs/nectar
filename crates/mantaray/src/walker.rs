//! Tree traversal for mantaray nodes.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nectar_primitives::bmt::DEFAULT_BODY_SIZE;
    use nectar_primitives::store::MemorySink;

    use crate::node::Node;

    type Store = MemorySink<DEFAULT_BODY_SIZE>;

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
            root.add::<Store, { DEFAULT_BODY_SIZE }>(p.as_bytes(), &entry, BTreeMap::new(), None)
                .unwrap();
        }

        let mut visited: Vec<(Vec<u8>, bool)> = Vec::new();
        root.walk::<Store, { DEFAULT_BODY_SIZE }, _>(None, &mut |path, node| {
            visited.push((path.to_vec(), node.is_value()));
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
            n.add::<Store, { DEFAULT_BODY_SIZE }>(path, &entry, BTreeMap::new(), None)
                .unwrap();
        }

        let mut walked: Vec<Vec<u8>> = Vec::new();
        n.walk_node::<Store, { DEFAULT_BODY_SIZE }, _>(b"", None, &mut |path, _node| {
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
            n.add::<Store, { DEFAULT_BODY_SIZE }>(path, &entry, BTreeMap::new(), None)
                .unwrap();
        }

        let mut walked: Vec<Vec<u8>> = Vec::new();
        n.walk_node::<Store, { DEFAULT_BODY_SIZE }, _>(b"img/", None, &mut |path, _node| {
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

    /// Same as above but with save/load through a store,
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
            n.add::<Store, { DEFAULT_BODY_SIZE }>(path, &entry, BTreeMap::new(), None)
                .unwrap();
        }

        let mut store = Store::new();
        n.save(&mut store).unwrap();

        let mut n2 = Node::from_reference(n.reference());

        let mut walked: Vec<Vec<u8>> = Vec::new();
        n2.walk_node(b"", Some(&store), &mut |path, _node| {
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
