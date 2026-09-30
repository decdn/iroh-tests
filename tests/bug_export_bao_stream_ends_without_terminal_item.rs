//! `MemStore` export_bao ends without a `Done` or `Error` item when the store shuts down mid-export.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source:
//! - `ExportBaoProgress::stream` forwards items until `rx.recv()` returns
//!   `Ok(None)` or `Err` and then ends silently; it adds no terminal item of
//!   its own:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L1187-L1201>
//! - `data_to_vec` treats `Ok(None)` from the channel as the normal end and
//!   returns `Ok` with the bytes collected so far:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L1092-L1107>
//! - MemStore runs the export as a task that the actor drops on shutdown:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L684-L701>
//!
//! The tests use a complete 8 MiB blob in a `MemStore`:
//! - `stream_ends_with_terminal_item`: start `export_bao(all)` with a channel
//!   capacity of 1, call `shutdown()` after the third item, drain the stream,
//!   and assert its last item is `Done` or `Error`.
//! - `data_to_vec_errs_on_truncation`: run `export_bao(all).data_to_vec()`
//!   in a task, call `shutdown()` right away, and assert the result is either
//!   the full blob or `Err`, never `Ok` with fewer bytes.
//!
//! Observed at the pinned version: the stream ends after 4 items with a
//! `Parent` as the last item, and `data_to_vec` returns `Ok` with 0 of
//! 8388608 bytes. Both results repeat across three runs. `FsStore` finishes
//! the export with `Done` before it shuts down, so it does not show this.

use std::time::Duration;

use bao_tree::ChunkRanges;
use iroh_blobs::api::blobs::EncodedItem;
use iroh_blobs::api::proto::ExportBaoRequest;
use iroh_blobs::store::mem::MemStore;
use n0_future::StreamExt;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);
const TOTAL: u64 = 8 << 20;

fn blob() -> Vec<u8> {
    (0..TOTAL).map(|i| (i % 251) as u8).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_ends_with_terminal_item() -> anyhow::Result<()> {
    let store = MemStore::new();
    let tt = store.blobs().add_slice(blob()).temp_tag().await?;
    let mut stream = store
        .blobs()
        .export_bao_with_opts(ExportBaoRequest { hash: tt.hash(), ranges: ChunkRanges::all() }, 1)
        .stream();

    let mut count = 0usize;
    let mut last = String::from("<none>");
    timeout(T, async {
        while let Some(item) = stream.next().await {
            count += 1;
            last = match &item {
                EncodedItem::Size(_) => "Size".to_string(),
                EncodedItem::Parent(_) => "Parent".to_string(),
                EncodedItem::Leaf(_) => "Leaf".to_string(),
                EncodedItem::Done => "Done".to_string(),
                EncodedItem::Error(e) => format!("Error({e:?})"),
            };
            if count == 3 {
                store.shutdown().await.ok();
            }
        }
    })
    .await?;
    println!("stream ended after {count} items; last item: {last}");
    assert!(
        last == "Done" || last.starts_with("Error"),
        "export_bao stream ends without a Done or Error item when the store shuts down mid-export (last item: {last}, {count} items)"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn data_to_vec_errs_on_truncation() -> anyhow::Result<()> {
    let store = MemStore::new();
    let tt = store.blobs().add_slice(blob()).temp_tag().await?;
    let hash = tt.hash();
    let blobs = store.blobs().clone();
    let task = tokio::spawn(async move {
        blobs
            .export_bao_with_opts(ExportBaoRequest { hash, ranges: ChunkRanges::all() }, 1)
            .data_to_vec()
            .await
    });
    tokio::task::yield_now().await;
    store.shutdown().await.ok();
    let res = timeout(T, task).await??;
    match &res {
        Ok(v) => println!("data_to_vec returned Ok with {} of {TOTAL} bytes", v.len()),
        Err(e) => println!("data_to_vec returned Err: {e:?}"),
    }
    if let Ok(v) = res {
        assert_eq!(
            v.len() as u64,
            TOTAL,
            "data_to_vec returns Ok with a truncated blob when the store shuts down mid-export"
        );
    }
    Ok(())
}
