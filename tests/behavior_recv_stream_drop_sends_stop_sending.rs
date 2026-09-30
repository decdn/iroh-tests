//! Dropping an unfinished `RecvStream` sends `STOP_SENDING` with error code 0.
//!
//! Crates: iroh 1.3.0 (re-exports the noq stream types), noq 1.3.0.
//!
//! Upstream source at the release commit:
//! - `impl Drop for RecvStream` calls `stop(0)` unless all data was read or the connection
//!   already failed:
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/recv_stream.rs#L613-L640>
//! - the peer observes it through `SendStream::stopped` (`Result<Option<VarInt>, _>`) and
//!   `WriteError::Stopped(VarInt)`:
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/send_stream.rs#L255>
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/send_stream.rs#L379-L384>
//!
//! The client opens a bi stream and writes a few bytes. The server accepts it, reads one
//! chunk, and drops the `RecvStream` without reading to the end, while it keeps the
//! connection open. The test asserts that the client's `SendStream::stopped()` resolves to
//! `Ok(Some(0))` and that a later `write` fails with `WriteError::Stopped(0)`. Code 0 is
//! indistinguishable from an application that calls `stop(0u32.into())` on purpose. It
//! passes today and fails if noq changes the implicit stop code or stops sending it.
//!
//! Observed at the pinned versions: `stopped()` resolves to `Ok(Some(0))` and the next
//! `write` returns `Err(Stopped(0))`.

use std::time::Duration;

use iroh::{
    Endpoint, EndpointAddr, RelayMode,
    endpoint::{VarInt, WriteError, presets},
};

const ALPN: &[u8] = b"iroh-tests/recv-stream-drop";
const DEADLINE: Duration = Duration::from_secs(10);

async fn bind_endpoint(alpns: Vec<Vec<u8>>) -> anyhow::Result<Endpoint> {
    let ep = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .alpns(alpns)
        .bind()
        .await?;
    Ok(ep)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_unfinished_recv_stream_stops_peer_with_code_zero() -> anyhow::Result<()> {
    let server = bind_endpoint(vec![ALPN.to_vec()]).await?;
    let client = bind_endpoint(vec![]).await?;
    let server_addr = server
        .bound_sockets()
        .into_iter()
        .fold(EndpointAddr::new(server.id()), |addr, sock| addr.with_ip_addr(sock));

    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
    let server_ep = server.clone();
    let server_task = tokio::spawn(async move {
        let conn = server_ep
            .accept()
            .await
            .ok_or_else(|| anyhow::anyhow!("server endpoint closed"))?
            .await?;
        let (send, mut recv) = conn.accept_bi().await?;
        let _first = recv.read_chunk(1024).await?;
        // The stream is not finished: the client has not called `finish()`.
        drop(recv);
        let _ = dropped_tx.send(());
        // Keep the connection and the send half alive while the client observes the stop.
        let _ = conn.closed().await;
        drop(send);
        anyhow::Ok(())
    });

    let conn = tokio::time::timeout(DEADLINE, client.connect(server_addr, ALPN)).await??;
    let (mut send, _recv) = conn.open_bi().await?;
    send.write_all(b"first chunk").await?;
    tokio::time::timeout(DEADLINE, dropped_rx).await??;

    let stopped = tokio::time::timeout(DEADLINE, send.stopped()).await?;
    let write = tokio::time::timeout(DEADLINE, send.write(b"more")).await?;
    println!("stopped() = {stopped:?}; next write = {write:?}");

    conn.close(0u32.into(), b"done");
    let _ = tokio::time::timeout(DEADLINE, server_task).await;
    let _ = tokio::time::timeout(DEADLINE, client.close()).await;
    let _ = tokio::time::timeout(DEADLINE, server.close()).await;

    assert_eq!(
        stopped?,
        Some(VarInt::from_u32(0)),
        "peer did not observe STOP_SENDING with code 0 after the RecvStream was dropped"
    );
    assert!(
        matches!(write, Err(WriteError::Stopped(code)) if code == VarInt::from_u32(0)),
        "write after the implicit stop did not fail with WriteError::Stopped(0): {write:?}"
    );
    Ok(())
}
