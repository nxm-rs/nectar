//! Structural encryption through the public API: a database keyed by
//! [`EncryptedChunkRef`] builds, applies, scans, traverses and reads back
//! exactly as its plaintext twin, while every stored chunk stays opaque.
//!
//! The reference parameter is the whole difference: nothing but the sealer
//! changes between the two builds, and the read side takes no extra state
//! because each reference carries the key that opens the chunk it names.

#![cfg(feature = "encryption")]

use anyhow::{Result, ensure};
use nectar_ldb::{Builder, Changeset, Encrypted, Entry, Key, Node, Plaintext, Reader, V1, apply};
use nectar_primitives::store::ChunkGet;
use nectar_primitives::{
    ChunkAddress, ChunkOps, ChunkRef, ContentGet, EncryptedChunkRef, MemoryStore,
};
use nectar_testing::run;

const SECRET: &[u8] = b"correct horse battery staple";

fn seal() -> Encrypted<'static, V1> {
    Encrypted::new(SECRET)
}

fn entry(fill: u8) -> Entry<V1> {
    Entry::from(ChunkRef::new(ChunkAddress::new([fill; 32])))
}

/// A key set wide and deep enough to reference sub-nodes and spill the root,
/// so the encrypted path exercises embedding, referenced hops and segments.
fn keys() -> Vec<(Key, u8)> {
    let mut out = Vec::new();
    for p in 0u8..96 {
        for x in 0u8..44 {
            out.push((Key::from(&[p, x][..]), x));
        }
    }
    out
}

fn builder_over(rows: &[(Key, u8)]) -> Builder<V1> {
    let mut builder = Builder::<V1>::new();
    for (key, fill) in rows {
        builder.insert(key.clone(), entry(*fill), None);
    }
    builder
}

/// Publish `rows` as an encrypted database, returning the store and the root
/// reference, which is the whole tree's read capability.
fn build_encrypted(rows: &[(Key, u8)]) -> Result<(MemoryStore, EncryptedChunkRef)> {
    let store = MemoryStore::default();
    let root = run(builder_over(rows).build(&store, &seal()))?
        .root()
        .clone();
    Ok((store, root))
}

#[test]
fn an_encrypted_build_is_deterministic_and_distinct_from_the_plaintext_one() -> Result<()> {
    let rows = keys();
    let (_, first) = build_encrypted(&rows)?;
    let (_, second) = build_encrypted(&rows)?;
    ensure!(
        first == second,
        "the same keys under the same secret must seal to the same root",
    );

    let other = MemoryStore::default();
    let under_other = run(builder_over(&rows).build(&other, &Encrypted::<V1>::new(b"other")))?
        .root()
        .clone();
    ensure!(
        under_other != first,
        "a different secret must yield a different root",
    );

    let plain_store = MemoryStore::default();
    let plain = *run(builder_over(&rows).build(&plain_store, &Plaintext))?.root();
    ensure!(
        plain.address() != first.address(),
        "an encrypted database must not share the plaintext root address",
    );
    Ok(())
}

#[test]
fn every_stored_chunk_is_opaque_to_a_plaintext_reader() -> Result<()> {
    let (store, root) = build_encrypted(&keys())?;
    for (_, chunk) in store.into_chunks() {
        ensure!(
            Node::<V1>::decode(chunk.envelope().data()).is_err(),
            "a stored ciphertext chunk must not decode as a plaintext node",
        );
    }
    // The root reference itself opens the root chunk, so the tree is readable
    // to whoever holds it and to nobody else.
    ensure!(
        root.key().as_bytes().iter().any(|&b| b != 0),
        "a derived key is never all-zero"
    );
    Ok(())
}

#[test]
fn build_then_read_round_trips_every_key() -> Result<()> {
    let rows = keys();
    let (store, root) = build_encrypted(&rows)?;
    let reader = Reader::<_, V1, EncryptedChunkRef>::new(ContentGet::new(store));
    run(async {
        for (key, fill) in &rows {
            ensure!(
                reader.get(&root, key).await? == Some(entry(*fill)),
                "read value mismatch",
            );
        }
        anyhow::Ok(())
    })?;
    ensure!(
        run(reader.get(&root, &Key::from(&b"absent"[..])))?.is_none(),
        "absent key must read as None",
    );
    Ok(())
}

#[test]
fn a_scan_yields_the_same_order_as_the_plaintext_twin() -> Result<()> {
    let rows = keys();
    let (store, root) = build_encrypted(&rows)?;
    let plain_store = MemoryStore::default();
    let plain_root = *run(builder_over(&rows).build(&plain_store, &Plaintext))?.root();

    let reader = Reader::<_, V1, EncryptedChunkRef>::new(ContentGet::new(store));
    let plain_reader = Reader::<_, V1>::new(ContentGet::new(plain_store));
    run(async {
        let mut cursor = reader.iter(&root).await?;
        let mut plain_cursor = plain_reader.iter(&plain_root).await?;
        let mut seen = 0usize;
        loop {
            match (cursor.next().await?, plain_cursor.next().await?) {
                (Some(left), Some(right)) => {
                    ensure!(left == right, "encrypted and plaintext scans must agree");
                    seen += 1;
                }
                (None, None) => break,
                _ => anyhow::bail!("the two scans ended at different points"),
            }
        }
        ensure!(seen == rows.len(), "the scan must yield every key");
        anyhow::Ok(())
    })?;

    // A rank-directed select routes by the counts the encrypted records carry.
    let (key, value) = run(reader.select(&root, 100))?.expect("index within the key set");
    ensure!(
        run(plain_reader.select(&plain_root, 100))? == Some((key, value)),
        "select must agree with the plaintext twin",
    );
    Ok(())
}

#[test]
fn the_traversal_names_every_stored_chunk() -> Result<()> {
    let rows = keys();
    let (store, root) = build_encrypted(&rows)?;
    let stored: std::collections::HashSet<ChunkAddress> =
        store.clone().into_chunks().into_keys().collect();

    let reader = Reader::<_, V1, EncryptedChunkRef>::new(ContentGet::new(store));
    let streamed = run(async {
        let mut stream = reader.addresses(&root);
        let mut out = std::collections::HashSet::new();
        while let Some(address) = stream.next().await? {
            out.insert(address);
        }
        anyhow::Ok(out)
    })?;

    // Entry references name chunks that live outside this store, so the node
    // and segment closure is exactly what the build stored.
    let entries: std::collections::HashSet<ChunkAddress> = rows
        .iter()
        .map(|(_, fill)| ChunkAddress::new([*fill; 32]))
        .collect();
    ensure!(
        streamed
            .difference(&entries)
            .copied()
            .collect::<std::collections::HashSet<_>>()
            == stored,
        "the traversal must name exactly the stored chunk set",
    );
    Ok(())
}

#[test]
fn apply_matches_a_from_scratch_encrypted_build() -> Result<()> {
    let all = keys();
    let split = all.len() * 3 / 4;

    let store = MemoryStore::default();
    let base_root = run(builder_over(&all[..split]).build(&store, &seal()))?
        .root()
        .clone();

    let mut changeset = Changeset::<V1>::new();
    for (key, fill) in all.iter().skip(split) {
        changeset.put(key.clone(), entry(*fill), None);
    }
    // A deletion the merged set never had: it must leave the root untouched.
    changeset.remove(Key::from(&b"absent"[..]));

    let applied = run(apply(
        &ContentGet::new(&store),
        &seal(),
        &base_root,
        &changeset,
    ))?;
    let (_, scratch) = build_encrypted(&all)?;
    ensure!(
        applied == scratch,
        "an encrypted apply must match a from-scratch encrypted build",
    );

    // The updated database still reads back through the new capability.
    let reader = Reader::<_, V1, EncryptedChunkRef>::new(ContentGet::new(store));
    run(async {
        for (key, fill) in &all {
            ensure!(
                reader.get(&applied, key).await? == Some(entry(*fill)),
                "read value mismatch after apply",
            );
        }
        anyhow::Ok(())
    })?;
    Ok(())
}

#[test]
fn a_mis_typed_reader_cannot_open_an_encrypted_database() -> Result<()> {
    let (store, root) = build_encrypted(&keys())?;
    let content = ContentGet::new(store);
    // The root chunk is present, so the failure below is the width witness and
    // the ciphertext, not a missing chunk.
    ensure!(
        run(ChunkGet::get(&content, root.address())).is_ok(),
        "the root chunk must be stored",
    );
    let plain_reader = Reader::<_, V1>::new(content);
    ensure!(
        run(plain_reader.get(&ChunkRef::new(*root.address()), &Key::from(&[0u8, 0][..]))).is_err(),
        "a plaintext-typed reader must fail loud on an encrypted database",
    );
    Ok(())
}
