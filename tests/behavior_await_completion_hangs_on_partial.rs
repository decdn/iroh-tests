//! `observe(hash).await_completion()` never resolves for a partial blob that nobody is filling.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source: `ObserveProgress::await_completion` loops over the bitfield stream
//! until an item `is_complete()`, and the stream stays open while the entry
//! exists:
//! <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L772-L780>
//!
//! The test imports only the first chunk group (chunks 0..16) of a 256 KiB
//! blob into a `MemStore` and an `FsStore`. It asserts that
//! `observe(hash).await_completion()` is still pending after 3 s. It then
//! imports the remaining chunks and asserts that a second `await_completion()`
//! resolves with a complete bitfield, which shows the call is a wait for
//! completion, not a query of the current state. A caller who wants "is this
//! blob complete now" must take the first `observe()` item or use `status()`.
//!
//! Observed at the pinned version: passes on both stores.

use std::time::Duration;

use bao_tree::{ChunkNum, ChunkRanges};
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::mem::MemStore;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);
const WAIT: Duration = Duration::from_secs(3);
const TOTAL: u64 = 256 * 1024;

/// Import `ranges` of a `TOTAL`-byte blob from `src` into `dst`.
async fn import_ranges(
    src: &Store,
    dst: &Store,
    hash: Hash,
    ranges: ChunkRanges,
) -> anyhow::Result<()> {
    let bao = src
        .blobs()
        .export_bao(hash, ranges.clone())
        .bao_to_vec()
        .await?;
    dst.blobs().import_bao_bytes(hash, ranges, bao).await?;
    Ok(())
}

async fn check(dst: &Store, label: &str) -> anyhow::Result<()> {
    let data: Vec<u8> = (0..TOTAL).map(|i| (i % 251) as u8).collect();
    let src = MemStore::new();
    let tt = src.blobs().add_slice(&data).temp_tag().await?;
    let hash = tt.hash();

    timeout(
        T,
        import_ranges(
            &src,
            dst,
            hash,
            ChunkRanges::from(ChunkNum(0)..ChunkNum(16)),
        ),
    )
    .await??;
    let first = timeout(T, dst.blobs().observe(hash)).await??;
    assert!(!first.is_complete());

    let pending = timeout(WAIT, dst.blobs().observe(hash).await_completion()).await;
    println!(
        "{label}: await_completion on partial after {WAIT:?}: {}",
        if pending.is_err() {
            "still pending"
        } else {
            "resolved"
        }
    );
    assert!(
        pending.is_err(),
        "await_completion resolved for a partial blob: {pending:?}"
    );

    timeout(
        T,
        import_ranges(&src, dst, hash, ChunkRanges::from(ChunkNum(16)..)),
    )
    .await??;
    let done = timeout(T, dst.blobs().observe(hash).await_completion()).await??;
    println!(
        "{label}: after full import: complete={} size={}",
        done.is_complete(),
        done.size()
    );
    assert!(done.is_complete());
    assert_eq!(done.size(), TOTAL);
    Ok(())
}

#[tokio::test]
async fn mem_await_completion_waits_on_partial() -> anyhow::Result<()> {
    let store = MemStore::new();
    check(&store, "mem").await
}

#[tokio::test]
async fn fs_await_completion_waits_on_partial() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    check(&store, "fs").await?;
    store.shutdown().await?;
    Ok(())
}
