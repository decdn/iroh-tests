//! An iroh server that never opts into 0-RTT still accepts and delivers 0-RTT data.
//!
//! Crates: iroh 1.3.0, noq 1.3.0.
//!
//! Upstream source at the release commits:
//! - iroh sets `max_early_data_size = u32::MAX` on every server TLS config:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/tls.rs#L123-L125>
//!   The client side sets `enable_early_data = true` and stores session tickets:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/tls.rs#L87-L89>
//! - `ProtocolHandler::on_accepting` by default awaits the handshake and never calls
//!   `Accepting::into_0rtt`:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L248-L256>
//! - `RecvStream::is_0rtt` is "handshaking at the moment the stream was accepted":
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/connection.rs#L1155-L1156>
//!   and its docs say it is true only if the stream was accepted before the handshake
//!   completed, which requires the server to call `into_0rtt`:
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/recv_stream.rs#L302-L314>
//!
//! What the tests assert, for 20 resumed connections against one server:
//! - the client's `Connecting::into_0rtt` returns `Ok` (it holds a ticket and early keys),
//! - the client opens a bi stream and writes the payload before the handshake completes,
//! - the client's `handshake_completed` resolves to `ZeroRttStatus::Accepted`, so the
//!   server accepted the early data,
//! - the server, which only ever awaits a completed handshake, reads the exact payload,
//! - the server-side `RecvStream::is_0rtt` is false on every such stream.
//!
//! Two server shapes are covered: a raw `endpoint.accept()` loop and a `Router` with a
//! `ProtocolHandler` that overrides nothing. A third test calls `Accepting::into_0rtt` on
//! the server and only prints how often `is_0rtt` is true; it asserts delivery, not the
//! share, because the flag depends on whether the stream is accepted before the
//! handshake finishes.
//!
//! Observed at 1.3.0 on macOS loopback: all 20 resumptions go 0-RTT and are accepted
//! for both default server shapes, every payload arrives, and `is_0rtt` is false 20/20.
//! The plain server cannot tell that the bytes it read arrived as replayable early
//! data. With a server-side `into_0rtt`, `is_0rtt` is true in 20/20 runs.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use iroh::endpoint::{ConnectOptions, Connection, ConnectionState, ZeroRttStatus, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use tokio::sync::mpsc;

const ALPN: &[u8] = b"iroh-tests/0rtt-default";
const ITERATIONS: usize = 20;
const STEP_TIMEOUT: Duration = Duration::from_secs(10);

/// What the server saw on one stream.
#[derive(Debug)]
struct Seen {
    payload: Vec<u8>,
    is_0rtt: bool,
}

async fn bind() -> Result<Endpoint> {
    let ep = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    Ok(ep)
}

fn dial_addr(server: &Endpoint) -> Result<EndpointAddr> {
    let sock: SocketAddr = server
        .bound_sockets()
        .into_iter()
        .find(|s| s.is_ipv4())
        .context("server has no IPv4 socket")?;
    Ok(EndpointAddr::new(server.id()).with_ip_addr(sock))
}

/// Serves bi streams on a connection: read the request, report it, echo an ack.
async fn serve_streams<S: ConnectionState>(conn: Connection<S>, seen: mpsc::UnboundedSender<Seen>) {
    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
        let is_0rtt = recv.is_0rtt();
        let Ok(payload) = recv.read_to_end(1024).await else {
            return;
        };
        let _ = seen.send(Seen { payload, is_0rtt });
        let _ = send.write_all(b"ack").await;
        let _ = send.finish();
    }
}

/// Server shape 1: raw `endpoint.accept()` that awaits the full handshake.
fn spawn_plain_accept_loop(server: Endpoint, seen: mpsc::UnboundedSender<Seen>) {
    tokio::spawn(async move {
        while let Some(incoming) = server.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                let Ok(accepting) = incoming.accept() else {
                    return;
                };
                let Ok(conn) = accepting.await else { return };
                serve_streams(conn, seen).await;
            });
        }
    });
}

/// Server shape 3: opts into 0-RTT with `Accepting::into_0rtt`.
fn spawn_0rtt_accept_loop(server: Endpoint, seen: mpsc::UnboundedSender<Seen>) {
    tokio::spawn(async move {
        while let Some(incoming) = server.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                let Ok(accepting) = incoming.accept() else {
                    return;
                };
                let conn = accepting.into_0rtt();
                serve_streams(conn, seen).await;
            });
        }
    });
}

/// Server shape 2: a `ProtocolHandler` that keeps the default `on_accepting`.
#[derive(Debug, Clone)]
struct DefaultHandler {
    seen: mpsc::UnboundedSender<Seen>,
}

impl ProtocolHandler for DefaultHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        serve_streams(connection, self.seen.clone()).await;
        Ok(())
    }
}

/// One full-handshake connection so the client stores a session ticket.
async fn warm_up(client: &Endpoint, addr: &EndpointAddr) -> Result<()> {
    let conn = client.connect(addr.clone(), ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(b"warm-up").await?;
    send.finish()?;
    let ack = recv.read_to_end(64).await?;
    ensure!(ack == b"ack", "warm-up ack mismatch");
    sync_after_handshake(&conn).await?;
    conn.close(0u32.into(), b"warm-up done");
    Ok(())
}

/// Payload of the stream that `sync_after_handshake` opens.
const SYNC: &[u8] = b"sync";

/// One more round trip on a stream opened after the client handshake completes.
///
/// A server that answers in 0.5-RTT can ack the first request before it has the
/// client's Finished, and so before it sends its session tickets. The server acks this
/// stream only after it has processed the Finished, so the ticket is ahead of this ack
/// and the next connection can resume.
async fn sync_after_handshake<S: ConnectionState>(conn: &Connection<S>) -> Result<()> {
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(SYNC).await?;
    send.finish()?;
    let ack = recv.read_to_end(64).await?;
    ensure!(ack == b"ack", "sync ack mismatch");
    Ok(())
}

/// Next request the server saw, skipping the `SYNC` streams.
async fn next_seen(seen: &mut mpsc::UnboundedReceiver<Seen>) -> Result<Seen> {
    loop {
        let got = tokio::time::timeout(STEP_TIMEOUT, seen.recv())
            .await
            .context("server report timed out")?
            .context("server channel closed")?;
        if got.payload != SYNC {
            return Ok(got);
        }
    }
}

/// Client side of one resumed connection.
///
/// Returns whether the server accepted the early data.
async fn resumed_request(client: &Endpoint, addr: &EndpointAddr, payload: &[u8]) -> Result<bool> {
    let connecting = client
        .connect_with_opts(addr.clone(), ALPN, ConnectOptions::new())
        .await?;
    let zrtt = connecting
        .into_0rtt()
        .map_err(|_| anyhow!("into_0rtt returned Err: the client did not attempt 0-RTT"))?;
    let (mut send, mut recv) = zrtt.open_bi().await?;
    send.write_all(payload).await?;
    send.finish()?;
    let accepted = match zrtt.handshake_completed().await? {
        ZeroRttStatus::Accepted(_) => true,
        ZeroRttStatus::Rejected(_) => false,
    };
    let ack = recv.read_to_end(64).await?;
    ensure!(ack == b"ack", "ack mismatch");
    sync_after_handshake(&zrtt).await?;
    zrtt.close(0u32.into(), b"done");
    Ok(accepted)
}

/// Runs `ITERATIONS` resumed requests and returns how many streams the server saw
/// with `is_0rtt() == true`.
async fn run(
    client: &Endpoint,
    addr: &EndpointAddr,
    seen: &mut mpsc::UnboundedReceiver<Seen>,
) -> Result<usize> {
    tokio::time::timeout(STEP_TIMEOUT, warm_up(client, addr))
        .await
        .context("warm-up timed out")??;
    let warm = next_seen(seen).await?;
    ensure!(warm.payload == b"warm-up");

    let mut is_0rtt_true = 0;
    for i in 0..ITERATIONS {
        let payload = format!("early request {i}").into_bytes();
        let accepted = tokio::time::timeout(STEP_TIMEOUT, resumed_request(client, addr, &payload))
            .await
            .with_context(|| format!("iteration {i} timed out"))??;
        if !accepted {
            bail!("iteration {i}: the server rejected the 0-RTT data");
        }
        let got = next_seen(seen)
            .await
            .with_context(|| format!("iteration {i}"))?;
        ensure!(
            got.payload == payload,
            "iteration {i}: server read {:?}",
            got.payload
        );
        if got.is_0rtt {
            is_0rtt_true += 1;
        }
    }
    Ok(is_0rtt_true)
}

#[tokio::test]
async fn plain_accept_loop_reads_0rtt_data() -> Result<()> {
    let server = bind().await?;
    let client = bind().await?;
    let addr = dial_addr(&server)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    spawn_plain_accept_loop(server.clone(), tx);

    let is_0rtt_true = run(&client, &addr, &mut rx).await?;
    println!(
        "plain accept loop: {ITERATIONS}/{ITERATIONS} 0-RTT accepted and read; server is_0rtt true in {is_0rtt_true}/{ITERATIONS}"
    );
    assert_eq!(
        is_0rtt_true, 0,
        "server-side is_0rtt without server into_0rtt"
    );

    client.close().await;
    server.close().await;
    Ok(())
}

#[tokio::test]
async fn default_protocol_handler_reads_0rtt_data() -> Result<()> {
    let server = bind().await?;
    let client = bind().await?;
    let addr = dial_addr(&server)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let router = Router::builder(server.clone())
        .accept(ALPN, DefaultHandler { seen: tx })
        .spawn();

    let is_0rtt_true = run(&client, &addr, &mut rx).await?;
    println!(
        "default ProtocolHandler: {ITERATIONS}/{ITERATIONS} 0-RTT accepted and read; server is_0rtt true in {is_0rtt_true}/{ITERATIONS}"
    );
    assert_eq!(
        is_0rtt_true, 0,
        "server-side is_0rtt without server into_0rtt"
    );

    client.close().await;
    router.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn server_into_0rtt_is_0rtt_share() -> Result<()> {
    let server = bind().await?;
    let client = bind().await?;
    let addr = dial_addr(&server)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    spawn_0rtt_accept_loop(server.clone(), tx);

    let is_0rtt_true = run(&client, &addr, &mut rx).await?;
    println!(
        "server into_0rtt: {ITERATIONS}/{ITERATIONS} 0-RTT accepted and read; server is_0rtt true in {is_0rtt_true}/{ITERATIONS}"
    );

    client.close().await;
    server.close().await;
    Ok(())
}
