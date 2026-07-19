//! Unification layer over the two manifest implementations.
//!
//! Static dispatch only: every scenario driver is generic over
//! [`ManifestApi`] and monomorphized once per implementation, so both sides
//! run the byte-identical driver with near-zero adapter cost.

use futures::executor::block_on;
use nectar_manifest::{Builder, Changeset, Key, V1, apply};
use nectar_primitives::chunk::ChunkAddress;
use nectar_primitives::ChunkRef;

use crate::store::CountingStore;

/// The common manifest surface both implementations answer.
pub trait ManifestApi {
    /// Stable implementation label.
    const NAME: &'static str;

    /// Build from `entries` and persist every node to `store`; the root.
    fn build(store: &CountingStore, entries: &[(Vec<u8>, ChunkAddress)]) -> ChunkAddress;

    /// Point lookup against the persisted root.
    fn get(store: &CountingStore, root: &ChunkAddress, key: &[u8]) -> Option<ChunkAddress>;

    /// Full in-order iteration; the entry count.
    fn iter_all(store: &CountingStore, root: &ChunkAddress) -> u64;

    /// Ordered iteration of keys starting with `prefix`; the entry count.
    fn iter_prefix(store: &CountingStore, root: &ChunkAddress, prefix: &[u8]) -> u64;

    /// Ordered iteration with `lo <= key < hi`; the entry count.
    fn iter_range(store: &CountingStore, root: &ChunkAddress, lo: &[u8], hi: &[u8]) -> u64;

    /// Open the persisted root, apply inserts and removes, re-persist; the
    /// new root.
    fn edit(
        store: &CountingStore,
        root: &ChunkAddress,
        inserts: &[(Vec<u8>, ChunkAddress)],
        removes: &[Vec<u8>],
    ) -> ChunkAddress;
}

/// nectar-manifest 1.0, V1 format, plain references.
#[derive(Clone, Copy, Debug)]
pub struct Manifest10;

impl ManifestApi for Manifest10 {
    const NAME: &'static str = "manifest10";

    fn build(store: &CountingStore, entries: &[(Vec<u8>, ChunkAddress)]) -> ChunkAddress {
        block_on(async {
            let mut builder = Builder::<V1>::new();
            for (key, address) in entries {
                builder.insert(Key::from(&key[..]), ChunkRef::new(*address).into(), None);
            }
            *builder.build(store).await.unwrap().root()
        })
    }

    fn get(store: &CountingStore, root: &ChunkAddress, key: &[u8]) -> Option<ChunkAddress> {
        block_on(async {
            let reader = nectar_manifest::Reader::<_, V1>::new(store.clone());
            reader
                .get(root, &Key::from(key))
                .await
                .unwrap()
                .and_then(|entry| entry.address().copied())
        })
    }

    fn iter_all(store: &CountingStore, root: &ChunkAddress) -> u64 {
        block_on(async {
            let reader = nectar_manifest::Reader::<_, V1>::new(store.clone());
            let mut cursor = reader.iter(root).await.unwrap();
            let mut count = 0u64;
            while let Some((key, entry)) = cursor.next().await.unwrap() {
                std::hint::black_box((key, entry));
                count += 1;
            }
            count
        })
    }

    fn iter_prefix(store: &CountingStore, root: &ChunkAddress, prefix: &[u8]) -> u64 {
        block_on(async {
            let reader = nectar_manifest::Reader::<_, V1>::new(store.clone());
            let mut cursor = reader.prefix(root, &Key::from(prefix)).await.unwrap();
            let mut count = 0u64;
            while let Some(item) = cursor.next().await.unwrap() {
                std::hint::black_box(item);
                count += 1;
            }
            count
        })
    }

    fn iter_range(store: &CountingStore, root: &ChunkAddress, lo: &[u8], hi: &[u8]) -> u64 {
        block_on(async {
            let reader = nectar_manifest::Reader::<_, V1>::new(store.clone());
            let mut cursor = reader
                .range(root, &Key::from(lo), &Key::from(hi))
                .await
                .unwrap();
            let mut count = 0u64;
            while let Some(item) = cursor.next().await.unwrap() {
                std::hint::black_box(item);
                count += 1;
            }
            count
        })
    }

    fn edit(
        store: &CountingStore,
        root: &ChunkAddress,
        inserts: &[(Vec<u8>, ChunkAddress)],
        removes: &[Vec<u8>],
    ) -> ChunkAddress {
        block_on(async {
            let mut changeset = Changeset::<V1>::new();
            for (key, address) in inserts {
                changeset.put(Key::from(&key[..]), ChunkRef::new(*address).into(), None);
            }
            for key in removes {
                changeset.remove(Key::from(&key[..]));
            }
            apply(store, root, &changeset).await.unwrap()
        })
    }
}

/// nectar-mantaray 0.2 streaming surface, plain references.
#[derive(Clone, Copy, Debug)]
pub struct Mantaray02;

impl ManifestApi for Mantaray02 {
    const NAME: &'static str = "mantaray02";

    fn build(store: &CountingStore, entries: &[(Vec<u8>, ChunkAddress)]) -> ChunkAddress {
        block_on(async {
            let mut editor = nectar_mantaray::ManifestEditor::new(store.clone());
            for (key, address) in entries {
                editor.put(&key[..], *address);
            }
            let (root, _store) = editor.commit().await.unwrap();
            root
        })
    }

    fn get(store: &CountingStore, root: &ChunkAddress, key: &[u8]) -> Option<ChunkAddress> {
        block_on(async {
            let reader = nectar_mantaray::Reader::new(store.clone());
            reader
                .get(root, key)
                .await
                .unwrap()
                .and_then(|entry| entry.address().copied())
        })
    }

    fn iter_all(store: &CountingStore, root: &ChunkAddress) -> u64 {
        block_on(async {
            let mut cursor = nectar_mantaray::Cursor::new(store.clone(), *root);
            let mut count = 0u64;
            while let Some(entry) = cursor.next().await {
                std::hint::black_box(entry.unwrap());
                count += 1;
            }
            count
        })
    }

    fn iter_prefix(store: &CountingStore, root: &ChunkAddress, prefix: &[u8]) -> u64 {
        block_on(async {
            let mut cursor = nectar_mantaray::Cursor::new(store.clone(), *root).with_prefix(prefix);
            let mut count = 0u64;
            while let Some(entry) = cursor.next().await {
                std::hint::black_box(entry.unwrap());
                count += 1;
            }
            count
        })
    }

    // 0.2 has no bounded range scan; this is the idiomatic 0.2 spelling, run
    // entirely inside the timed region: a point get decides the inclusive
    // lower bound, then a cursor resumes strictly after it with the upper
    // bound enforced by the caller. Matches `lo <= key < hi` whether or not
    // `lo` is present (the corpus guarantees it is, at rank n/4).
    fn iter_range(store: &CountingStore, root: &ChunkAddress, lo: &[u8], hi: &[u8]) -> u64 {
        block_on(async {
            let mut count = 0u64;
            let reader = nectar_mantaray::Reader::new(store.clone());
            if let Some(entry) = reader.get(root, lo).await.unwrap() {
                std::hint::black_box(entry);
                count += 1;
            }
            let mut cursor = nectar_mantaray::Cursor::new(store.clone(), *root).after(lo);
            while let Some(entry) = cursor.next().await {
                let entry = entry.unwrap();
                if entry.path() >= hi {
                    break;
                }
                std::hint::black_box(entry);
                count += 1;
            }
            count
        })
    }

    fn edit(
        store: &CountingStore,
        root: &ChunkAddress,
        inserts: &[(Vec<u8>, ChunkAddress)],
        removes: &[Vec<u8>],
    ) -> ChunkAddress {
        block_on(async {
            let mut editor = nectar_mantaray::ManifestEditor::open(*root, store.clone());
            for (key, address) in inserts {
                editor.put(&key[..], *address);
            }
            for key in removes {
                editor.remove(&key[..]);
            }
            let (new_root, _store) = editor.commit().await.unwrap();
            new_root
        })
    }
}

/// Order-statistic count, 1.0 only: 0.2 stores no subtree counts, so this is
/// recorded for the record and never folded into comparative aggregates.
#[must_use]
pub fn manifest10_count(
    store: &CountingStore,
    root: &ChunkAddress,
    lo: &[u8],
    hi: &[u8],
) -> u64 {
    block_on(async {
        let reader = nectar_manifest::Reader::<_, V1>::new(store.clone());
        reader
            .count(root, &Key::from(lo), &Key::from(hi))
            .await
            .unwrap()
    })
}

/// Order-statistic select, 1.0 only; the selected key length, 0 when out of
/// range.
#[must_use]
pub fn manifest10_select(store: &CountingStore, root: &ChunkAddress, index: u64) -> u64 {
    block_on(async {
        let reader = nectar_manifest::Reader::<_, V1>::new(store.clone());
        reader
            .select(root, index)
            .await
            .unwrap()
            .map_or(0, |(key, _)| u64::try_from(key.len()).unwrap_or(u64::MAX))
    })
}
