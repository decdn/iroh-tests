# iroh-tests

Reproductions for issues found while building [deCDN](https://github.com/decdn) on
[iroh](https://github.com/n0-computer/iroh), [iroh-blobs](https://github.com/n0-computer/iroh-blobs),
[noq](https://github.com/n0-computer/noq) and [bao-tree](https://github.com/n0-computer/bao-tree).

Every test pins exact releases: iroh 1.3.0, noq 1.3.0, iroh-blobs 0.103.0, bao-tree 0.16.1.
Each file's module doc links the upstream source lines at the release commit and records the
result we observed.

## Two kinds of test

- `tests/bug_*.rs` assert the **correct** behavior. They **fail** at the pinned versions. A
  `bug_` test that starts passing after a version bump means the issue is fixed upstream.
  Every operation that can hang runs under a timeout, so a hang shows up as an assertion
  failure, never as a stuck test run.
- `tests/behavior_*.rs` assert the **current** behavior behind a docs or API request. They
  **pass** at the pinned versions and fail if upstream changes that behavior.

## Run

```bash
cargo test --no-fail-fast
```

```bash
cargo test --test bug_endpoint_close_after_runtime_drop -- --nocapture
```

The full run takes about 40 s, most of it spent in the timeouts of the hanging cases. All
networking is local: endpoints bind 127.0.0.1 with relays disabled.

## Bugs (fail today)

| Test | Crate | What goes wrong |
| --- | --- | --- |
| [`bug_endpoint_close_after_runtime_drop`](tests/bug_endpoint_close_after_runtime_drop.rs) | iroh / noq | `Endpoint::close` never returns once the runtime that drove a connection is dropped (20 of 20 rounds hang). |
| [`bug_second_fsstore_open_deadlocks`](tests/bug_second_fsstore_open_deadlocks.rs) | iroh-blobs | `FsStore::load` hangs on a setup error (a second open of the same directory, or a root that is a regular file) instead of returning it. |
| [`bug_add_stream_swallows_error`](tests/bug_add_stream_swallows_error.rs) | iroh-blobs | `add_stream` with a source stream that yields `Err` hangs and never reports the error. |
| [`bug_export_bao_stream_ends_without_terminal_item`](tests/bug_export_bao_stream_ends_without_terminal_item.rs) | iroh-blobs | A `MemStore` shutdown mid-export ends the `export_bao` stream with no `Done` or `Error`; `data_to_vec` returns `Ok` with 0 of 8 MiB. |
| [`bug_hash_from_str_panics`](tests/bug_hash_from_str_panics.rs) | iroh-blobs | `Hash::from_str` panics on wrong-length input instead of returning `Err`. |
| [`bug_bao_tree_async_encode_empty_ranges`](tests/bug_bao_tree_async_encode_empty_ranges.rs) | bao-tree | Empty `ChunkRanges` panic the fsm encoder and both decoders in debug builds; in release the sync and fsm encoders disagree and the sync output does not decode. |
| [`bug_bitfield_total_bytes_underflow`](tests/bug_bitfield_total_bytes_underflow.rs) | iroh-blobs | `Bitfield::total_bytes` overflows on a range past `size` (panics in debug, returns a huge value in release). |

## Current behavior behind a request (pass today)

| Test | Crate | Behavior |
| --- | --- | --- |
| [`behavior_iroh_server_accepts_0rtt_by_default`](tests/behavior_iroh_server_accepts_0rtt_by_default.rs) | iroh | A server that never opts into 0-RTT accepts and reads replayable early data (20 of 20), and cannot tell it was early data. |
| [`behavior_router_shutdown_unbounded`](tests/behavior_router_shutdown_unbounded.rs) | iroh | `Router::shutdown` waits forever on a handler whose `shutdown` never resolves, and the endpoint never closes. |
| [`behavior_recv_stream_drop_sends_stop_sending`](tests/behavior_recv_stream_drop_sends_stop_sending.rs) | noq | Dropping an unfinished `RecvStream` stops the peer's send side with code 0. |
| [`behavior_ipv6_port_ephemeral_when_only_v4_set`](tests/behavior_ipv6_port_ephemeral_when_only_v4_set.rs) | iroh | With only an IPv4 port configured, the IPv6 socket binds a random port. |
| [`behavior_status_partial_size_unknown`](tests/behavior_status_partial_size_unknown.rs) | iroh-blobs | `status()` reports `Partial { size: None }` while `observe()` already knows the size. |
| [`behavior_await_completion_hangs_on_partial`](tests/behavior_await_completion_hangs_on_partial.rs) | iroh-blobs | `await_completion()` stays pending on a partial blob until the rest arrives. |
| [`behavior_export_bao_absent_range_error_kind`](tests/behavior_export_bao_absent_range_error_kind.rs) | iroh-blobs | `export_bao` on a range the store lacks fails with `LeafHashMismatch` or `UnexpectedEof`, which read as corruption, not "not present". |
| [`behavior_mem_observe_unknown_hash_creates_entry`](tests/behavior_mem_observe_unknown_hash_creates_entry.rs) | iroh-blobs | `MemStore::observe` on an unknown hash creates an entry: `status` turns from `NotFound` to `Partial`. `FsStore` does not. |
| [`behavior_add_bytes_creates_named_tag`](tests/behavior_add_bytes_creates_named_tag.rs) | iroh-blobs | Awaiting `add_bytes` creates a persistent `auto-<timestamp>` tag; `add_stream` does not. |

## License

MIT OR Apache-2.0
