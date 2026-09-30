//! `export_bao` on an absent range reports a hash mismatch or EOF, not "range not present".
//!
//! Crate: iroh-blobs 0.103.0 (with bao-tree 0.16.1).
//!
//! Source:
//! - MemStore `export_bao` passes the partial entry straight to
//!   `traverse_ranges_validated` without checking the bitfield:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L684-L701>
//! - FsStore does the same:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs.rs#L1198-L1215>
//! - `export_ranges` checks the bitfield first and returns "missing range":
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L569-L596>
//!
//! Setup: a 256 KiB blob with chunk groups 0..16 and 32..48 imported, so
//! 16..32 is a hole and 48.. is past the written data.
//!
//! The test asserts, for a `MemStore` and an `FsStore`:
//! - `export_bao(16..32)` (hole) yields `Size`, the present parent nodes, then
//!   `Error(LeafHashMismatch(16))`. The store reads the unwritten bytes,
//!   hashes them, and reports corruption.
//! - `export_bao(48..64)` (past written data) yields `Size`, parents, then an
//!   io `UnexpectedEof` error.
//! - Neither export emits a `Leaf` for the absent range, so no zero bytes reach
//!   the caller.
//! - `export_ranges` on the same byte range returns a "missing range" error.
//!
//! So `export_bao` detects the absent range, but its error reads as data
//! corruption or a short read, and a caller cannot tell it from a damaged
//! store without also calling `observe()`.
//!
//! Observed at the pinned version: passes on both stores.

use std::time::Duration;

use bao_tree::{ChunkNum, ChunkRanges};
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::blobs::EncodedItem;
use iroh_blobs::api::proto::ExportRangesItem;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::mem::MemStore;
use n0_future::StreamExt;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);
const TOTAL: u64 = 256 * 1024;

fn groups(start: u64, end: u64) -> ChunkRanges {
    ChunkRanges::from(ChunkNum(start)..ChunkNum(end))
}

async fn import_groups(dst: &Store) -> anyhow::Result<Hash> {
    let data: Vec<u8> = (0..TOTAL).map(|i| (i % 251) as u8).collect();
    let src = MemStore::new();
    let tt = src.blobs().add_slice(&data).temp_tag().await?;
    let hash = tt.hash();
    for ranges in [groups(0, 16), groups(32, 48)] {
        let bao = src.blobs().export_bao(hash, ranges.clone()).bao_to_vec().await?;
        dst.blobs().import_bao_bytes(hash, ranges, bao).await?;
    }
    Ok(hash)
}

/// Collect the export_bao items as short labels.
async fn export_bao_items(store: &Store, hash: Hash, ranges: ChunkRanges) -> Vec<String> {
    let mut out = Vec::new();
    let mut stream = store.blobs().export_bao(hash, ranges).stream();
    while let Some(item) = stream.next().await {
        out.push(match item {
            EncodedItem::Size(s) => format!("Size({s})"),
            EncodedItem::Parent(_) => "Parent".to_string(),
            EncodedItem::Leaf(l) => format!("Leaf(offset={}, len={})", l.offset, l.data.len()),
            EncodedItem::Done => "Done".to_string(),
            EncodedItem::Error(e) => format!("Error({e:?})"),
        });
    }
    out
}

async fn check(store: &Store, label: &str) -> anyhow::Result<()> {
    let hash = timeout(T, import_groups(store)).await??;
    let bitfield = timeout(T, store.blobs().observe(hash)).await??;
    assert_eq!(bitfield.ranges, &groups(0, 16) | &groups(32, 48));

    let hole = timeout(T, export_bao_items(store, hash, groups(16, 32))).await?;
    println!("{label} export_bao hole 16..32: {hole:?}");
    let tail = timeout(T, export_bao_items(store, hash, groups(48, 64))).await?;
    println!("{label} export_bao tail 48..64: {tail:?}");

    for items in [&hole, &tail] {
        assert_eq!(items.first().map(String::as_str), Some("Size(262144)"));
        assert!(!items.iter().any(|i| i.starts_with("Leaf")), "absent range emitted a Leaf: {items:?}");
        assert!(!items.iter().any(|i| i == "Done"), "{items:?}");
    }
    assert!(hole.last().is_some_and(|l| l.contains("LeafHashMismatch") && l.contains("16")), "{hole:?}");
    assert!(tail.last().is_some_and(|l| l.contains("UnexpectedEof")), "{tail:?}");

    let ranges_err = timeout(T, async {
        let mut stream = store.blobs().export_ranges(hash, 16 * 1024u64..32 * 1024).stream();
        let mut err = None;
        while let Some(item) = stream.next().await {
            match item {
                ExportRangesItem::Data(l) => panic!("export_ranges emitted data at {}", l.offset),
                ExportRangesItem::Error(e) => err = Some(format!("{e:?}")),
                ExportRangesItem::Size(_) => {}
            }
        }
        err
    })
    .await?;
    println!("{label} export_ranges hole: {ranges_err:?}");
    assert!(ranges_err.is_some_and(|e| e.contains("missing range")));
    Ok(())
}

#[tokio::test]
async fn mem_export_bao_absent_range_error_kind() -> anyhow::Result<()> {
    let store = MemStore::new();
    check(&store, "mem").await
}

#[tokio::test]
async fn fs_export_bao_absent_range_error_kind() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    check(&store, "fs").await?;
    store.shutdown().await?;
    Ok(())
}
