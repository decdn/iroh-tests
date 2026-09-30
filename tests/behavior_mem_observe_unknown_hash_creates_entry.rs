//! `MemStore::observe` on an unknown hash creates a store entry; `FsStore` does not.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source:
//! - MemStore observe handler calls `get_or_create_entry`:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L195-L202>
//! - `get_or_create_entry` inserts an empty partial entry:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L428-L438>
//! - `BlobStatus` reports any existing entry as `Partial`:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L358-L381>
//!
//! On a hash that was never added, both stores return an empty, incomplete
//! first bitfield (size 0, no ranges, `is_complete() == false`). The side
//! effect differs. After the observe, `MemStore` reports the hash as
//! `BlobStatus::Partial { size: None }`, lists it in `blobs().list()`, and
//! answers `export_bao` with `LeafHashMismatch(0)` rather than `NotFound`.
//! `FsStore` still reports `NotFound` and `export_bao` returns a `NotFound`
//! io error.
//!
//! The test asserts that current behavior. It passes at the pinned version.

use std::time::Duration;

use bao_tree::{ChunkNum, ChunkRanges};
use iroh_blobs::Hash;
use iroh_blobs::api::blobs::{BlobStatus, EncodedItem};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::mem::MemStore;
use n0_future::StreamExt;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

async fn first_export_error(blobs: &iroh_blobs::api::blobs::Blobs, hash: Hash) -> String {
    let mut stream = blobs
        .export_bao(hash, ChunkRanges::from(ChunkNum(0)..ChunkNum(1)))
        .stream();
    while let Some(item) = stream.next().await {
        if let EncodedItem::Error(e) = item {
            return format!("{e:?}");
        }
    }
    "no error item".to_string()
}

#[tokio::test]
async fn mem_observe_creates_partial_entry() -> anyhow::Result<()> {
    let store = MemStore::new();
    let hash = Hash::new(b"never added");
    assert_eq!(
        timeout(T, store.blobs().status(hash)).await??,
        BlobStatus::NotFound
    );

    let bitfield = timeout(T, store.blobs().observe(hash)).await??;
    println!(
        "mem first bitfield: size={} ranges={:?}",
        bitfield.size(),
        bitfield.ranges
    );
    assert_eq!(bitfield.size(), 0);
    assert!(bitfield.ranges.is_empty());
    assert!(!bitfield.is_complete());

    let status = timeout(T, store.blobs().status(hash)).await??;
    let listed = timeout(T, store.blobs().list().hashes()).await??;
    let export_err = timeout(T, first_export_error(store.blobs(), hash)).await?;
    println!("mem after observe: status={status:?} listed={listed:?} export_bao={export_err}");

    assert_eq!(status, BlobStatus::Partial { size: None });
    assert_eq!(listed, vec![hash]);
    assert!(export_err.contains("LeafHashMismatch"), "{export_err}");
    Ok(())
}

#[tokio::test]
async fn fs_observe_leaves_hash_not_found() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    let hash = Hash::new(b"never added");

    let bitfield = timeout(T, store.blobs().observe(hash)).await??;
    println!(
        "fs first bitfield: size={} ranges={:?}",
        bitfield.size(),
        bitfield.ranges
    );
    assert_eq!(bitfield.size(), 0);
    assert!(!bitfield.is_complete());

    let status = timeout(T, store.blobs().status(hash)).await??;
    let export_err = timeout(T, first_export_error(store.blobs(), hash)).await?;
    println!("fs after observe: status={status:?} export_bao={export_err}");
    assert_eq!(status, BlobStatus::NotFound);
    assert!(export_err.contains("NotFound"), "{export_err}");

    store.shutdown().await?;
    Ok(())
}
