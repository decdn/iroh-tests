//! `Endpoint::close` hangs after the runtime that drove a connection is dropped.
//!
//! Crates: iroh 1.3.0, noq 1.3.0.
//!
//! Upstream source at the release commits:
//! - noq spawns the `ConnectionDriver` with the endpoint runtime:
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/connection.rs#L74>
//! - iroh's runtime spawns onto the caller's current tokio runtime through a `TaskTracker`:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/runtime.rs#L78-L96>
//! - `State::drop` sends only `EndpointEvent::drained()`:
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/connection.rs#L1957-L1964>
//! - the endpoint decrements `active_connections` only on a draining event:
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/endpoint.rs#L647-L656>
//! - `wait_all_draining` waits for `active_connections == 0`:
//!   <https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/endpoint.rs#L410-L421>
//! - iroh awaits `wait_all_draining` with no deadline inside `Endpoint::close`:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/socket.rs#L1159-L1161>
//!
//! Each round connects from a client endpoint inside a separate current-thread tokio
//! runtime, opens a stream, keeps the connection open, and drops that runtime. The
//! connection driver task dies with the runtime. The round then drops the `Connection` and
//! calls `client.close()` with a 5 s deadline. The test runs 20 rounds concurrently and
//! asserts that every close completes. A control test runs the same flow without the
//! runtime drop.
//!
//! Observed at the pinned versions: the control close completes in about 2 ms. After the
//! runtime drop, 20 of 20 concurrent rounds hang past the 5 s deadline (three runs). A
//! single sequential round hangs in most runs, not all (5 of 6 runs with a 10 s deadline).
//! `RUST_LOG=iroh::socket=trace` shows `socket closing...` and
//! `wait_all_draining start` for the hung endpoint with no matching `wait_all_draining done`.

use std::time::{Duration, Instant};

use iroh::{
    Endpoint, EndpointAddr, RelayMode,
    endpoint::{Connection, presets},
};

const ALPN: &[u8] = b"iroh-tests/close-after-runtime-drop";
const CLOSE_DEADLINE: Duration = Duration::from_secs(5);
const ROUNDS: usize = 20;

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

fn direct_addr(ep: &Endpoint) -> EndpointAddr {
    ep.bound_sockets()
        .into_iter()
        .fold(EndpointAddr::new(ep.id()), |addr, sock| {
            addr.with_ip_addr(sock)
        })
}

/// Binds a server that accepts connections and holds each one until the peer closes it.
async fn spawn_server() -> anyhow::Result<(Endpoint, EndpointAddr)> {
    let server = bind_endpoint(vec![ALPN.to_vec()]).await?;
    let addr = direct_addr(&server);
    let accept_ep = server.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accept_ep.accept().await {
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    let _ = conn.closed().await;
                }
            });
        }
    });
    Ok((server, addr))
}

/// Opens a bi stream and writes a few bytes so the connection carries live stream state.
async fn exercise(conn: &Connection) -> anyhow::Result<()> {
    let (mut send, _recv) = conn.open_bi().await?;
    send.write_all(b"hello").await?;
    Ok(())
}

/// Runs one round and returns whether `client.close()` completed within the deadline.
async fn round_with_runtime_drop(server_addr: EndpointAddr) -> anyhow::Result<bool> {
    let client = bind_endpoint(vec![]).await?;
    let client_in_rt = client.clone();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let conn = rt.block_on(async {
            let conn = tokio::time::timeout(
                Duration::from_secs(10),
                client_in_rt.connect(server_addr, ALPN),
            )
            .await??;
            exercise(&conn).await?;
            anyhow::Ok(conn)
        })?;
        // The connection is open. Dropping the runtime drops the connection driver task.
        drop(rt);
        drop(conn);
        Ok(())
    })
    .await??;

    let start = Instant::now();
    let closed = tokio::time::timeout(CLOSE_DEADLINE, client.close())
        .await
        .is_ok();
    println!(
        "client.close() after runtime drop: {} after {:?}",
        if closed { "completed" } else { "timed out" },
        start.elapsed()
    );
    Ok(closed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_completes_after_connection_runtime_dropped() -> anyhow::Result<()> {
    // `RUST_LOG=iroh::socket=trace` shows `wait_all_draining start` with no matching `done`.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let (server, server_addr) = spawn_server().await?;

    let mut rounds = tokio::task::JoinSet::new();
    for _ in 0..ROUNDS {
        rounds.spawn(round_with_runtime_drop(server_addr.clone()));
    }
    let mut hung = 0usize;
    while let Some(res) = rounds.join_next().await {
        if !res?? {
            hung += 1;
        }
    }
    println!("{hung} of {ROUNDS} client.close() calls hung past {CLOSE_DEADLINE:?}");

    let _ = tokio::time::timeout(CLOSE_DEADLINE, server.close()).await;
    assert_eq!(
        hung, 0,
        "Endpoint::close never returns once the runtime that drove a connection is dropped \
         ({hung} of {ROUNDS} rounds hung), because noq's State::drop sends only a drained \
         event and active_connections never reaches 0"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_close_completes_when_runtime_alive() -> anyhow::Result<()> {
    let (server, server_addr) = spawn_server().await?;
    let client = bind_endpoint(vec![]).await?;

    let conn =
        tokio::time::timeout(Duration::from_secs(10), client.connect(server_addr, ALPN)).await??;
    exercise(&conn).await?;

    let start = Instant::now();
    let closed = tokio::time::timeout(CLOSE_DEADLINE, client.close()).await;
    println!(
        "control client.close(): completed={} after {:?}",
        closed.is_ok(),
        start.elapsed()
    );
    drop(conn);
    let _ = tokio::time::timeout(CLOSE_DEADLINE, server.close()).await;
    assert!(
        closed.is_ok(),
        "control: Endpoint::close does not complete with a live runtime"
    );
    Ok(())
}
