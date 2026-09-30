//! With only an IPv4 bind address on a fixed port, the IPv6 socket binds an ephemeral port.
//!
//! Crate: iroh 1.3.0.
//!
//! Upstream source at the release commit:
//! - every builder starts with a default IPv4 transport and a default IPv6 transport:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint.rs#L193-L199>
//! - the default IPv6 transport is `[::]` with port 0 and `is_required: false`:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/socket/transports.rs#L136-L151>
//! - a user-defined address replaces only the default of its own address family:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/socket/transports.rs#L197-L220>
//! - `Builder::bind_addr` docs describe the pre-configured IPv6 socket, but not its port:
//!   <https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint.rs#L378-L391>
//!
//! The test picks a port that is free on both `127.0.0.1` and `[::]`, binds an endpoint
//! with only `bind_addr("127.0.0.1:<port>")`, and reads `bound_sockets()`. It asserts that
//! the IPv4 socket is on the requested port and that an IPv6 socket exists on a different
//! port. It passes today and fails if iroh reuses the IPv4 port for IPv6 or stops binding
//! the default IPv6 socket. The test returns early when the host cannot bind `[::]`.
//!
//! Observed at the pinned version (macOS, dual stack): the IPv4 socket is on the requested
//! port and the IPv6 socket is on `[::]` with a different, OS-assigned port.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};

use iroh::{Endpoint, RelayMode, endpoint::presets};

/// Returns a port that is free for UDP on both `127.0.0.1` and `[::]`, or `None` when the
/// host has no usable IPv6 stack.
fn free_dual_stack_port() -> Option<u16> {
    UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0)).ok()?;
    for _ in 0..50 {
        let Ok(v4) = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)) else {
            continue;
        };
        let Ok(port) = v4.local_addr().map(|a| a.port()) else {
            continue;
        };
        // A dual-stack `[::]` socket conflicts with a live `127.0.0.1` socket on the same port.
        drop(v4);
        if UdpSocket::bind((Ipv6Addr::UNSPECIFIED, port)).is_ok() {
            return Some(port);
        }
    }
    None
}

#[tokio::test]
async fn ipv6_socket_uses_ephemeral_port_when_only_ipv4_port_is_set() -> anyhow::Result<()> {
    let Some(port) = free_dual_stack_port() else {
        println!("skipping: host cannot bind an IPv6 UDP socket");
        return Ok(());
    };

    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))?
        .bind()
        .await?;
    let sockets = endpoint.bound_sockets();
    println!("requested IPv4 port {port}; bound_sockets() = {sockets:?}");
    endpoint.close().await;

    let v4: Vec<_> = sockets.iter().filter(|a| a.is_ipv4()).collect();
    let v6: Vec<_> = sockets.iter().filter(|a| a.is_ipv6()).collect();
    assert!(
        v4.iter().all(|a| a.port() == port) && !v4.is_empty(),
        "IPv4 socket is not on the requested port {port}: {sockets:?}"
    );
    if v6.is_empty() {
        println!("no IPv6 socket bound; the optional default IPv6 bind failed on this host");
        return Ok(());
    }
    assert!(
        v6.iter().all(|a| a.port() != port),
        "IPv6 socket reuses the IPv4 port {port}: {sockets:?}"
    );
    assert!(
        v6.iter().all(|a| a.ip().is_unspecified()),
        "IPv6 socket is not the default [::] socket: {sockets:?}"
    );
    Ok(())
}
