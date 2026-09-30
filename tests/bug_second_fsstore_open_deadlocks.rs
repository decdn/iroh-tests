//! `FsStore::load` hangs forever when store setup fails, for example on a second open of the same dir.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source:
//! - `load_with_opts` builds a dedicated runtime `rt`, moves it into
//!   `Actor::new`, and awaits that future as a task spawned on `rt` itself:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs.rs#L1398-L1431>
//! - `Actor::new` returns early with `?` on any setup error (directory
//!   creation, `meta::Actor::new` opening the redb database), which drops the
//!   `RtWrapper` inside a task running on that same runtime:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs.rs#L635-L678>
//! - `RtWrapper::drop` drops the runtime inside `block_in_place`. The runtime's
//!   blocking-pool shutdown then waits for the thread it is running on:
//!   <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs.rs#L834-L844>
//!
//! A stack sample of the hung second open (macOS `sample`) shows the thread
//! parked in `Actor::new` (fs.rs:678) -> `drop_in_place<RtWrapper>` ->
//! `block_in_place` -> `drop_in_place<Runtime>` -> `BlockingPool::shutdown`
//! -> `shutdown::Receiver::wait`. Every other thread is idle. So the store
//! lock does not block; the lock is reported as an error, and the error path
//! deadlocks while it tears down the runtime.
//!
//! The tests assert that `FsStore::load` returns `Err` within 10 s in two
//! failure cases: a second open of a directory whose store is still alive,
//! and a root path that is a regular file.
//!
//! Observed at the pinned version: both tests fail; `load` is still pending
//! after 10 s.

use std::time::Duration;

use iroh_blobs::store::fs::FsStore;
use tokio::time::timeout;

const T: Duration = Duration::from_secs(10);

/// Run `FsStore::load(path)` in a task and describe the outcome.
async fn load_outcome(path: std::path::PathBuf) -> Result<String, String> {
    let task = tokio::spawn(async move { FsStore::load(path).await });
    match timeout(T, task).await {
        Err(_) => Err(format!("still pending after {T:?}")),
        Ok(Err(join)) => Err(format!("task failed, panic={}", join.is_panic())),
        Ok(Ok(Ok(_))) => Err("returned Ok".to_string()),
        Ok(Ok(Err(e))) => Ok(format!("returned Err({e})")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn second_load_on_open_dir_returns_err() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let first = FsStore::load(dir.path()).await?;
    let tt = first.blobs().add_slice(b"hello").temp_tag().await?;
    println!("first store open, added {}", tt.hash());

    let outcome = load_outcome(dir.path().to_owned()).await;
    println!("second load: {outcome:?}");
    first.shutdown().await?;
    if let Err(what) = outcome {
        panic!("FsStore::load on a dir whose store is still open does not return Err: {what}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn load_on_file_path_returns_err() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"x")?;

    let outcome = load_outcome(file).await;
    println!("load on regular file: {outcome:?}");
    if let Err(what) = outcome {
        panic!("FsStore::load on a regular-file root does not return Err: {what}");
    }
    Ok(())
}
