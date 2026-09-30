//! `status()` reports `Partial { size: None }` while `observe()` already carries the size.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source:
//! - MemStore `BlobStatus` handler returns `Partial { size: bitfield.validated_size() }`:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L358-L380>
//! - FsStore answers from the metadata entry state `EntryState::Partial { size }`:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs/meta.rs#L325>
//! - `Bitfield::validated_size` is `Some` only once the last chunk is present:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/proto/bitfield.rs#L132-L138>
//! - `BlobStatus::Partial::size` is documented as "The size of the currently
//!   stored partial blob":
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/proto.rs#L677-L681>
//!
//! The test imports only the first chunk group (chunks 0..16) of a 256 KiB
//! blob, with its bao size header, into a `MemStore` and an `FsStore`. For
//! each store it asserts that `status()` returns `Partial { size: None }` and
//! that the first `observe()` bitfield has `size() == 262144`, ranges `0..16`,
//! and `validated_size() == None`. So the size a caller needs to plan the
//! remaining fetch is available only through `observe()`, as an unverified
//! value, and `status()` hides it.
//!
//! Observed at the pinned version: passes on both stores.

use std::time::Duration;

use bao_tree::{ChunkNum, ChunkRanges};
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::blobs::BlobStatus;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::mem::MemStore;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);
const TOTAL: u64 = 256 * 1024;

/// Import the first chunk group of a `TOTAL`-byte blob into `dst`.
async fn import_first_group(dst: &Store) -> anyhow::Result<Hash> {
    let data: Vec<u8> = (0..TOTAL).map(|i| (i % 251) as u8).collect();
    let src = MemStore::new();
    let tt = src.blobs().add_slice(&data).temp_tag().await?;
    let hash = tt.hash();
    let first = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
    let bao = src.blobs().export_bao(hash, first.clone()).bao_to_vec().await?;
    dst.blobs().import_bao_bytes(hash, first, bao).await?;
    Ok(hash)
}

async fn check(store: &Store, label: &str) -> anyhow::Result<()> {
    let hash = timeout(T, import_first_group(store)).await??;
    let status = timeout(T, store.blobs().status(hash)).await??;
    let bitfield = timeout(T, store.blobs().observe(hash)).await??;
    println!(
        "{label}: status={status:?} observe size={} ranges={:?} validated_size={:?}",
        bitfield.size(),
        bitfield.ranges,
        bitfield.validated_size()
    );
    assert_eq!(status, BlobStatus::Partial { size: None });
    assert_eq!(bitfield.size(), TOTAL);
    assert_eq!(bitfield.ranges, ChunkRanges::from(ChunkNum(0)..ChunkNum(16)));
    assert_eq!(bitfield.validated_size(), None);
    Ok(())
}

#[tokio::test]
async fn mem_status_hides_known_size() -> anyhow::Result<()> {
    let store = MemStore::new();
    check(&store, "mem").await
}

#[tokio::test]
async fn fs_status_hides_known_size() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    check(&store, "fs").await?;
    store.shutdown().await?;
    Ok(())
}
