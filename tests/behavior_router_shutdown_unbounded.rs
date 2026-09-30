//! `Router::shutdown` waits on `ProtocolHandler::shutdown` with no deadline.
//!
//! Crate: iroh 1.3.0.
//!
//! Upstream source at the release commit:
//! - `Router::shutdown` awaits the accept-loop task:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L435-L452>
//! - the accept-loop task awaits `protocols.shutdown()` before `endpoint.close()`:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L609-L613>
//! - `ProtocolMap::shutdown` runs `join_all` over every handler's `shutdown` future:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L404-L407>
//! - the trait docs say the future "is awaited before the router closes the endpoint" and
//!   give no bound:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L278-L287>
//!
//! The test registers one handler whose `shutdown` never resolves and asserts that
//! `router.shutdown()` does not return within 3 s, and that the endpoint stays open. It
//! passes today and fails if iroh adds a deadline to handler shutdown.
//!
//! Observed at the pinned version: `router.shutdown()` times out after 3 s and
//! `endpoint.is_closed()` is still `false`.

use std::time::Duration;

use iroh::{
    Endpoint, RelayMode,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};

const ALPN: &[u8] = b"iroh-tests/router-shutdown-unbounded";

#[derive(Debug)]
struct NeverShutsDown;

impl ProtocolHandler for NeverShutsDown {
    async fn accept(&self, _connection: Connection) -> Result<(), AcceptError> {
        Ok(())
    }

    async fn shutdown(&self) {
        std::future::pending::<()>().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn router_shutdown_waits_forever_on_a_stuck_handler() -> anyhow::Result<()> {
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .bind()
        .await?;
    let router = Router::builder(endpoint.clone())
        .accept(ALPN, NeverShutsDown)
        .spawn();

    let res = tokio::time::timeout(Duration::from_secs(3), router.shutdown()).await;
    println!(
        "router.shutdown() within 3 s: {}; endpoint.is_closed() = {}",
        if res.is_ok() { "returned" } else { "timed out" },
        endpoint.is_closed()
    );
    assert!(
        res.is_err(),
        "Router::shutdown returned although a ProtocolHandler::shutdown future never resolves"
    );
    assert!(
        !endpoint.is_closed(),
        "the endpoint closed although the router is still waiting on handler shutdown"
    );
    Ok(())
}
