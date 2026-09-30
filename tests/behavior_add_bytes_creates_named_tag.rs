//! Awaiting `add_bytes(..)` directly creates a persistent tag; `.temp_tag()` does not.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source: `impl IntoFuture for AddProgress` resolves through `with_tag()`,
//! which calls `tags.create(..)` and so writes a persistent tag named
//! `auto-<timestamp>` (for example `auto-2026-09-30T12:49:58.795Z`):
//! <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L677-L726>
//!
//! The test uses a `MemStore` and asserts:
//! - `add_bytes(a).await` leaves one entry in `tags().list()`, pointing at `a`.
//! - `add_bytes(b).temp_tag().await` and `add_stream(c).await.temp_tag().await`
//!   add no tag, both while the `TempTag` is alive and after it drops.
//!
//! A caller who writes `store.add_bytes(data).await?` to get the hash leaks a
//! tag per call, and that tag protects the blob from GC until it is deleted.
//!
//! Observed at the pinned version: passes.

use std::io;
use std::time::Duration;

use bytes::Bytes;
use iroh_blobs::api::Store;
use iroh_blobs::api::tags::TagInfo;
use iroh_blobs::store::mem::MemStore;
use n0_future::StreamExt;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

async fn tags(store: &Store) -> anyhow::Result<Vec<TagInfo>> {
    let mut stream = store.tags().list().await?;
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

#[tokio::test]
async fn add_bytes_await_creates_tag_temp_tag_does_not() -> anyhow::Result<()> {
    let store = MemStore::new();
    assert!(tags(&store).await?.is_empty());

    let info = timeout(
        T,
        store
            .blobs()
            .add_bytes(Bytes::from_static(b"awaited directly")),
    )
    .await??;
    let after_await = tags(&store).await?;
    println!("after add_bytes(..).await: returned {info:?}; tags={after_await:?}");
    assert_eq!(after_await.len(), 1);
    assert_eq!(after_await[0].hash, info.hash);
    assert_eq!(after_await[0].name, info.name);

    let tt = timeout(
        T,
        store
            .blobs()
            .add_bytes(Bytes::from_static(b"temp tag only"))
            .temp_tag(),
    )
    .await??;
    let with_tt = tags(&store).await?;
    println!(
        "after add_bytes(..).temp_tag(): hash {}; tags={with_tt:?}",
        tt.hash()
    );
    assert_eq!(with_tt.len(), 1, "temp_tag() created a persistent tag");
    drop(tt);

    let items: Vec<io::Result<Bytes>> = vec![Ok(Bytes::from_static(b"streamed"))];
    let tt = timeout(T, async {
        store
            .blobs()
            .add_stream(n0_future::stream::iter(items))
            .await
            .temp_tag()
            .await
    })
    .await??;
    let with_stream_tt = tags(&store).await?;
    println!(
        "after add_stream(..).temp_tag(): hash {}; tags={with_stream_tt:?}",
        tt.hash()
    );
    assert_eq!(
        with_stream_tt.len(),
        1,
        "add_stream temp_tag() created a persistent tag"
    );
    drop(tt);

    let end = tags(&store).await?;
    assert_eq!(end.len(), 1);
    assert_eq!(end[0].hash, info.hash);
    Ok(())
}
