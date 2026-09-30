//! `Blobs::add_stream` discards an `Err` item from the input stream.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source: <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L273-L313>
//!
//! The send half of `add_stream` forwards items with `item?` (L305), so an
//! `Err` item ends the send future with that error. The caller runs the send
//! and receive halves with `let _ = tokio::join!(send, recv)` (L310), which
//! drops the error. The send half exits without sending
//! `ImportByteStreamUpdate::Done`, but the `sender` lives in the enclosing
//! generator until `join!` returns, so the store side keeps waiting for more
//! input, `recv` keeps waiting for the store, and `join!` never returns.
//!
//! The test feeds a stream of one `Ok(Bytes)` followed by an
//! `Err(io::Error)` and asserts that `.temp_tag().await` returns `Err` within
//! 10 s, for a `MemStore` and an `FsStore`.
//!
//! Observed at the pinned version: both stores hang. `temp_tag()` is still
//! pending after 10 s; it neither returns the source error nor imports the
//! prefix.

use std::io;
use std::time::Duration;

use bytes::Bytes;
use iroh_blobs::api::Store;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::store::mem::MemStore;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

async fn check(store: &Store, label: &str) {
    let items: Vec<io::Result<Bytes>> = vec![
        Ok(Bytes::from_static(b"first chunk of data")),
        Err(io::Error::other("source stream failed")),
    ];
    let stream = n0_future::stream::iter(items);
    let res = timeout(T, async { store.blobs().add_stream(stream).await.temp_tag().await }).await;
    match &res {
        Err(_) => println!("{label}: add_stream(..).temp_tag() still pending after {T:?} (hang)"),
        Ok(Ok(tt)) => println!("{label}: add_stream(..).temp_tag() returned Ok, hash {}", tt.hash()),
        Ok(Err(e)) => println!("{label}: add_stream(..).temp_tag() returned Err: {e:?}"),
    }
    match res {
        Ok(Err(_)) => {}
        Err(_) => panic!(
            "{label}: add_stream swallows the input stream error and never completes"
        ),
        Ok(Ok(tt)) => panic!(
            "{label}: add_stream swallows the input stream error and imports the truncated prefix as {}",
            tt.hash()
        ),
    }
}

#[tokio::test]
async fn mem_add_stream_reports_source_error() {
    let store = MemStore::new();
    check(&store, "mem").await;
}

#[tokio::test]
async fn fs_add_stream_reports_source_error() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    check(&store, "fs").await;
    store.shutdown().await?;
    Ok(())
}
