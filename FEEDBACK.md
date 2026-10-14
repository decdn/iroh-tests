# iroh feedback from deCDN

As of 2026-09-30.

deCDN is a paid content delivery network built on iroh: nodes cache BLAKE3 blobs in an iroh-blobs `FsStore` and serve verified byte ranges over iroh QUIC, including ranges of blobs that are still filling. This doc lists 28 things we hit, one per section: 7 bugs, 9 surprising behaviors that back a docs or API request, and 12 API or build requests.

**Versions:** [iroh](https://github.com/n0-computer/iroh) 1.3.0, [noq](https://github.com/n0-computer/noq) 1.3.0, [iroh-blobs](https://github.com/n0-computer/iroh-blobs) 0.103.0, [bao-tree](https://github.com/n0-computer/bao-tree) 0.16.1. Code links point to the commit each release was published from.

**Tests:** every bug and behavior item links a test in [decdn/iroh-tests](https://github.com/decdn/iroh-tests), a small Rust package pinned to exactly these versions. Run `cargo test --no-fail-fast`; the whole suite finishes in about 40 s and needs no network.

- **Bug** items link a `bug_*` test that asserts the correct behavior and **fails** today. If it starts passing after a bump, the bug is fixed.
- **Behavior** items link a `behavior_*` test that asserts today's behavior and **passes**. It documents the fact behind the request.
- **API** items name a missing or private capability, with a link to the source that shows it.

We are glad to turn any item into an issue or a PR.

## Bugs

### 1. `Endpoint::close` hangs after a connection's runtime is dropped

**iroh / noq · Bug · Test:** [`bug_endpoint_close_after_runtime_drop`](https://github.com/decdn/iroh-tests/blob/main/tests/bug_endpoint_close_after_runtime_drop.rs), 20 of 20 closes hang; the control closes in about 2 ms.

iroh spawns each connection's driver onto the caller's current tokio runtime ([`runtime.rs:78-96`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/runtime.rs#L78-L96)). If that runtime is dropped while the connection is open, noq's `State::drop` sends only a drained event ([`connection.rs:1957-1964`](https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/connection.rs#L1957-L1964)). The endpoint decrements `active_connections` only on a draining event ([`endpoint.rs:647-656`](https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/endpoint.rs#L647-L656)), so `wait_all_draining` ([`endpoint.rs:410-421`](https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/endpoint.rs#L410-L421)), awaited by `Endpoint::close` ([`socket.rs:1159-1161`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/socket.rs#L1159-L1161)), never returns. With a single connection the hang shows in about 5 of 6 runs; the test runs 20 rounds to make it deterministic.

**Impact:** we ran `!Send` work on short-lived runtimes. CI tests hung for 300 s, and 5 of 7 testnet nodes were SIGKILLed on every deploy.

**Workaround:** all dials run on one long-lived runtime handle, and `router.shutdown()` sits under a deadline.

**In deCDN:** [`client/src/connection.rs:111-119`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/client/src/connection.rs#L111-L119), [`node/src/runtime/mod.rs:2524`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/runtime/mod.rs#L2524)

**Ask:** count a dropped, never-drained connection as drained, or spawn drivers on a runtime the endpoint owns. At minimum, document on `Endpoint::connect` that the driver lives on the caller's runtime.

**Upstream:** [iroh#4604](https://github.com/n0-computer/iroh/issues/4604)

### 2. `FsStore::load` hangs on a setup error

**iroh-blobs · Bug · Test:** [`bug_second_fsstore_open_deadlocks`](https://github.com/decdn/iroh-tests/blob/main/tests/bug_second_fsstore_open_deadlocks.rs), both cases hang past 10 s.

A second `FsStore::load` on a directory that an open store holds hangs, and so does `FsStore::load` on a root path that is a regular file. A stack sample of the hung process shows `Actor::new` ([`fs.rs:678`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs.rs#L678)) failing, then dropping the store's own runtime from a task that runs on that runtime: `RtWrapper` drop, `block_in_place`, `BlockingPool::shutdown`, `Receiver::wait`, with every other thread idle. Any error during actor setup therefore turns into a deadlock.

**Workaround:** we guard against a second open in our own engine.

**In deCDN:** [`cache/src/engine.rs:5966`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L5966)

**Ask:** return the setup error from `FsStore::load`, and drop the runtime off its own worker threads.

**Upstream:** [iroh-blobs#252](https://github.com/n0-computer/iroh-blobs/issues/252), with our repro in [a comment](https://github.com/n0-computer/iroh-blobs/issues/252#issuecomment-6166866518)

### 3. `add_stream` hangs on a source stream error

**iroh-blobs · Bug · Test:** [`bug_add_stream_swallows_error`](https://github.com/decdn/iroh-tests/blob/main/tests/bug_add_stream_swallows_error.rs), pending after 10 s on both `MemStore` and `FsStore`.

In [`api/blobs.rs:273-313`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L273-L313), the send future uses `item?`, so a yielded `Err` ends it without sending `Done`. The sender stays alive until `tokio::join!(send, recv)` returns, and `recv` waits on the store, so `temp_tag()` never resolves. The caller gets neither the source's `io::Error` nor a hash.

**Impact:** we import from S3, R2 and HTTP origins. A dropped origin connection must surface as an origin failure we can retry.

**Workaround:** a wrapper stream stores the error in a side channel and yields `None` in place of `Err`. The import then commits under a wrong hash, and we read the side channel and drop the temp tag.

**In deCDN:** [`cache/src/engine.rs:5452`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L5452), [`5726`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L5726)

**Ask:** send the source error as `AddProgressItem::Error` and drop the sender, so `temp_tag()` returns it.

**Upstream:** [iroh-blobs#286](https://github.com/n0-computer/iroh-blobs/issues/286)

### 4. A `MemStore` export ends silently when the store shuts down

**iroh-blobs · Bug · Test:** [`bug_export_bao_stream_ends_without_terminal_item`](https://github.com/decdn/iroh-tests/blob/main/tests/bug_export_bao_stream_ends_without_terminal_item.rs), fails in 3 of 3 runs.

On an 8 MiB blob, `shutdown()` after the third item ends the `export_bao(..).stream()` after 4 items, the last one a `Parent`, with no `Done` or `Error`. `data_to_vec()` with an immediate shutdown returns **`Ok` with 0 of 8,388,608 bytes**, because a closed channel counts as a normal end ([`api/blobs.rs:1092-1107`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L1092-L1107), [`1187-1201`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L1187-L1201)). `FsStore` finishes the export before it shuts down, so it does not show this.

**Workaround:** we treat end of stream before `Done` as an error.

**In deCDN:** [`cache/src/engine.rs:4846`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L4846)

**Ask:** emit a terminal `Error` when the channel closes early, and make `data_to_vec` and friends return `Err` on a truncated export.

**Upstream:** [iroh-blobs#287](https://github.com/n0-computer/iroh-blobs/issues/287)

### 5. `Hash::from_str` panics on wrong-length input

**iroh-blobs · Bug · Test:** [`bug_hash_from_str_panics`](https://github.com/decdn/iroh-tests/blob/main/tests/bug_hash_from_str_panics.rs), 7 of 7 malformed inputs panic.

In [`hash.rs:151-172`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/hash.rs#L151-L172), only input of exactly 64 characters takes the hex path. Everything else goes to `BASE32_NOPAD.decode_mut` with a fixed 32-byte buffer, and data-encoding panics on the length mismatch. The empty string, `"abc"`, 51 or 53 base32 characters, and 63 or 65 hex characters all panic.

**Workaround:** we parse hashes with our own panic-free parser.

**In deCDN:** [`cache/src/origin/fs.rs:131`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/origin/fs.rs#L131)

**Ask:** check `BASE32_NOPAD.decode_len(input.len())` against 32 before `decode_mut` and return a parse error.

**Upstream:** fixed on `main` by [iroh-blobs#282](https://github.com/n0-computer/iroh-blobs/pull/282) (reported as [#277](https://github.com/n0-computer/iroh-blobs/issues/277)), not yet in a release. The test passes once a release includes it.

### 6. Empty `ChunkRanges` panic bao-tree's encoder and decoders

**bao-tree · Bug · Test:** [`bug_bao_tree_async_encode_empty_ranges`](https://github.com/decdn/iroh-tests/blob/main/tests/bug_bao_tree_async_encode_empty_ranges.rs), 3 of 4 cases fail.

The pre-order chunk iterator has `debug_assert!(!ranges.is_empty())` ([`iter.rs:547`](https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/iter.rs#L547)). The sync encoder short-circuits empty ranges ([`io/sync.rs:484-486`](https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/io/sync.rs#L484-L486)); the fsm encoder ([`io/fsm.rs:586-599`](https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/io/fsm.rs#L586-L599)) and both decoders ([`io/sync.rs:321-339`](https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/io/sync.rs#L321-L339)) do not. In debug builds they panic, for a 0-byte blob and a 48 KiB blob alike. In release builds the two encoders disagree: the fsm encoder writes 64 bytes (the root pair) for the 48 KiB blob, the sync encoder writes 0, and both decoders reject the sync output with `ParentNotFound`.

**Impact:** this panicked our serve task on a 0-byte blob.

**Workaround:** we short-circuit empty ranges and 0-byte blobs before calling bao-tree.

**In deCDN:** [`bao-range/src/lib.rs:86-89`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/bao-range/src/lib.rs#L86-L89)

**Ask:** handle empty ranges the same way in every encoder and decoder, so the sync and fsm paths round-trip.

**Upstream:** [bao-tree#81](https://github.com/n0-computer/bao-tree/issues/81)

### 7. `Bitfield::total_bytes` overflows on a range past `size`

**iroh-blobs · Bug · Test:** [`bug_bitfield_total_bytes_underflow`](https://github.com/decdn/iroh-tests/blob/main/tests/bug_bitfield_total_bytes_underflow.rs), panics in debug.

`Bitfield::new` keeps a chunk range that starts past `size` as given ([`api/proto/bitfield.rs:132-138`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/proto/bitfield.rs#L132-L138)), and so does `Deserialize`, which goes through `new`. `total_bytes` then subtracts without a check ([`141-153`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/proto/bitfield.rs#L141-L153)): chunks `10..11` with `size` 100 panic with "attempt to subtract with overflow" in debug and return 18,446,744,073,709,541,476 in release. We have not seen a store's `observe()` produce such a bitfield; any bitfield decoded from bytes can carry one.

**Workaround:** we compute present bytes with saturating math.

**In deCDN:** [`cache/src/engine.rs:819`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L819)

**Ask:** clamp spans to `size` in `Bitfield::new` or in `total_bytes`.

**Upstream:** [iroh-blobs#288](https://github.com/n0-computer/iroh-blobs/issues/288)

## Behaviors behind a request

### 8. A server accepts 0-RTT data without opting in

**iroh · Behavior · Test:** [`behavior_iroh_server_accepts_0rtt_by_default`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_iroh_server_accepts_0rtt_by_default.rs), 20 of 20 early-data sends accepted and read.

iroh sets `max_early_data_size = u32::MAX` on every server TLS config ([`tls.rs:123-125`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/tls.rs#L123-L125)). A server that keeps the default `on_accepting` ([`protocol.rs:248-256`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L248-L256)), or uses a plain `accept()` loop, reads replayable early data from a resumed client in every run. It also has no way to tell: `RecvStream::is_0rtt()` is true only when the server itself called `into_0rtt` ([`recv_stream.rs:302-314`](https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/recv_stream.rs#L302-L314)), so it reads false in 20 of 20 runs here. `handshake_data` and `ConnectionStats` do not report resumption either.

**Impact:** our payment messages are not replay-safe, and the only guard left is on the client side, which a server cannot enforce.

**Workaround:** we removed 0-RTT from every client path and keep a source-scan test that fails if any code calls `into_0rtt`.

**In deCDN:** [`node/tests/no_early_data_emitters.rs`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/tests/no_early_data_emitters.rs)

**Ask:** a per-endpoint or per-ALPN switch that sets `max_early_data_size = 0`, and a "resumed" or "early data received" flag a server can read without opting into 0-RTT.

### 9. `Router::shutdown` waits forever on a stuck handler

**iroh · Behavior · Test:** [`behavior_router_shutdown_unbounded`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_router_shutdown_unbounded.rs), still pending after 3 s; the endpoint never closes.

`Router::shutdown` ([`protocol.rs:435-452`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L435-L452)) awaits every handler's `shutdown` with `join_all` ([`404-407`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L404-L407)) and only then closes the endpoint. One handler whose `shutdown` never resolves hangs process exit, and the trait docs ([`278-287`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L278-L287)) state no bound.

**Workaround:** `tokio::time::timeout` around `router.shutdown()`, and nothing afterwards awaits `Endpoint::closed`.

**In deCDN:** [`node/src/runtime/mod.rs:2524`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/runtime/mod.rs#L2524)

**Ask:** a `Router::shutdown_with_timeout`, or close the endpoint even when a handler overruns a deadline.

### 10. `status()` hides a partial blob's size

**iroh-blobs · Behavior · Test:** [`behavior_status_partial_size_unknown`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_status_partial_size_unknown.rs), on `MemStore` and `FsStore`.

With the first 16 chunks of a 256 KiB blob imported, `status()` returns `Partial { size: None }`, while the `observe()` bitfield already has `size() == 262144` and `validated_size() == None`. `status` reports only a validated size ([`mem.rs:358-380`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L358-L380), [`fs/meta.rs:325`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs/meta.rs#L325)), so two calls on the same entry disagree about its size. In our production nodes, each `observe()` on an idle partial also reloads the entry, and its idle shutdown fsyncs and rewrites the bitfield. We have no test for that cost.

**Impact:** our advertise path used `status()` and our serve path used the bitfield, and they disagreed about which blobs we could serve.

**Workaround:** we size partials from the bitfield everywhere and cache each result for 30 s.

**In deCDN:** [`cache/src/engine.rs:879`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L879)

**Ask:** have `BlobStatus::Partial` carry the claimed size with a validated flag, and add a cheap present-ranges query that does not load the entry.

### 11. `await_completion` waits indefinitely on a partial blob

**iroh-blobs · Behavior · Test:** [`behavior_await_completion_hangs_on_partial`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_await_completion_hangs_on_partial.rs), pending after 3 s; resolves once the rest arrives.

`ObserveProgress::await_completion()` ([`api/blobs.rs:772-780`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L772-L780)) waits for completion, so on a partial blob that nothing is filling it never returns. The name reads like "the current state", and code that wants a snapshot hangs.

**Workaround:** for a snapshot we await `observe(hash)` once, since its first item is the current bitfield.

**In deCDN:** [`cache/src/serve_store.rs:20`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/serve_store.rs#L20)

**Ask:** document that `await_completion` waits for the blob to complete, and add an explicit snapshot call such as `observe(hash).current()`.

### 12. Exporting an absent range fails like corruption

**iroh-blobs · Behavior · Test:** [`behavior_export_bao_absent_range_error_kind`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_export_bao_absent_range_error_kind.rs), on `MemStore` and `FsStore`.

With chunk groups `0..16` and `32..48` imported, `export_bao` on the hole `16..32` yields the size, 4 parents, then `Error(LeafHashMismatch(16))`. On `48..64`, past the written data, it ends with `Error(Io(UnexpectedEof))`. See [`mem.rs:569-596`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L569-L596) and [`fs.rs:1198-1215`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/fs.rs#L1198-L1215). Both errors read as a damaged store or a short read, not as "this range is not here". `export_ranges` is clearer: it returns "missing range: RangeSet{16..32}".

**Workaround:** we check the bitfield before every export.

**Ask:** a typed "range not present" error from `export_bao`, raised before any bytes go out, as `export_ranges` already does.

### 13. `MemStore::observe` on an unknown hash creates an entry

**iroh-blobs · Behavior · Test:** [`behavior_mem_observe_unknown_hash_creates_entry`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_mem_observe_unknown_hash_creates_entry.rs), `MemStore` only.

`MemStore`'s `observe` calls `get_or_create_entry` ([`mem.rs:195-202`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L195-L202), [`428-438`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mem.rs#L428-L438)). After one `observe` on a hash the store has never seen, `status` changes from `NotFound` to `Partial { size: None }`, `list()` includes the hash, and `export_bao` fails with `LeafHashMismatch(0)` in place of not-found. `FsStore` leaves the hash `NotFound`. A read-only call changes state, and the two stores disagree.

**Workaround:** we call `status()` before `observe()`.

**Ask:** make `observe` side-effect free on `MemStore`, matching `FsStore`.

### 14. Awaiting `add_bytes` creates a persistent tag

**iroh-blobs · Behavior · Test:** [`behavior_add_bytes_creates_named_tag`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_add_bytes_creates_named_tag.rs).

`add_bytes(..).await` goes through `IntoFuture` and `with_tag()` ([`api/blobs.rs:677-726`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L677-L726)) and leaves a named tag such as `auto-2026-09-30T12:49:58.795Z`. `add_bytes(..).temp_tag()` and `add_stream(..).temp_tag()` leave no tag. The blob then never becomes eligible for GC, and we found this through leaked tags in a cache that was meant to evict. There is also no way to find a blob's tags by hash, so dropping them means listing every tag.

**Workaround:** we always take `temp_tag()` and set our own named tags, and our evict path scans the full tag list.

**In deCDN:** [`cache/src/engine.rs:2760`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L2760)

**Ask:** document the tagging default on `add_bytes`, and add `tags().list_for_hash(hash)` or `delete_for_hash(hash)`.

### 15. Dropping an unfinished `RecvStream` stops the peer

**noq · Behavior · Test:** [`behavior_recv_stream_drop_sends_stop_sending`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_recv_stream_drop_sends_stop_sending.rs), peer sees `stopped() = Ok(Some(0))`.

`RecvStream`'s `Drop` calls `stop(0)` when the stream has not reached FIN ([`recv_stream.rs:613-640`](https://github.com/n0-computer/noq/blob/c1f411562e6078852749b8bcf1190523096a107f/noq/src/recv_stream.rs#L613-L640)), and the peer's next write fails with `Stopped(0)`. This is QUIC-correct, but it is easy to trigger by accident: our server dropped the request stream after reading the last message it needed, and the client's final voucher write failed at the end of a fully paid transfer.

**Workaround:** the server reads to FIN under a byte and time bound before it drops a stream, and the client, when a write fails, briefly reads for a final message.

**In deCDN:** [`node/src/handlers/client/mod.rs:2388`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/handlers/client/mod.rs#L2388), [`client/src/lib.rs:3446`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/client/src/lib.rs#L3446)

**Ask:** document the implicit `STOP_SENDING(0)` on drop, and consider a `RecvStream::drain_to_fin(limit)` helper.

### 16. With only an IPv4 port set, the IPv6 port is random

**iroh · Behavior · Test:** [`behavior_ipv6_port_ephemeral_when_only_v4_set`](https://github.com/decdn/iroh-tests/blob/main/tests/behavior_ipv6_port_ephemeral_when_only_v4_set.rs), `bound_sockets() = [127.0.0.1:60328, [::]:53827]`.

The builder always adds the default IPv4 and IPv6 transports ([`endpoint.rs:193-199`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint.rs#L193-L199)). The default IPv6 transport is `[::]:0` and optional ([`socket/transports.rs:136-151`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/socket/transports.rs#L136-L151)), and a user address replaces only the default of its own family ([`197-220`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/socket/transports.rs#L197-L220)). Operators who set one port got a new IPv6 port on every restart, so they could not publish a stable IPv6 address or open a firewall rule for it.

**Workaround:** we bind `[::]:<port>` explicitly as optional, then check `bound_sockets()` and warn when no IPv6 socket came up.

**In deCDN:** [`node/src/runtime/mod.rs:2988`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/runtime/mod.rs#L2988)

**Ask:** document this on `bind_addr`, or reuse the IPv4 port for the default IPv6 socket when one is set.

## API and build requests

### 17. Public delete, on-demand GC and a GC shutdown

**iroh-blobs · API**

[`gc_run_once`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/gc.rs#L179) is `pub` inside a private module ([`store/mod.rs:11`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/mod.rs#L11)), and [`Blobs::delete`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L166) is `pub(crate)`. The only GC trigger is `Options::gc`, with an interval fixed at load. The GC task holds its own `Store` clone, so the store outlives every user handle, and a sweep reports nothing about what it freed.

**Impact:** a cache evicts on demand. Our evictor ticks every second, but disk space comes back only on the next 300 s sweep, and an operator command to free space now cannot be built.

**Workaround:** an in-memory evicted set blocks serving at once, and we drop tags and wait for the sweep. The protect callback holds a `Weak` to the store so it stops after our engine drops.

**In deCDN:** [`cache/src/engine.rs:461-464`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L461-L464), [`1064`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L1064)

**Ask:** make `Blobs::delete` public (or add delete-if-unprotected), export `gc_run_once`, return an abort handle for the GC task, and report freed hashes and bytes per sweep. We described our use case on [iroh-blobs#235](https://github.com/n0-computer/iroh-blobs/issues/235).

### 18. A reader for a partial blob's outboard

**iroh-blobs · API**

iroh-blobs holds a partial blob's outboard inside the store actor and exposes no reader for it. `import_bao` validates parent nodes and hands none of them back.

**Impact:** our core path serves a range of a large blob to one client while the blob is still arriving for another. That needs the proof nodes for ranges already present.

**Workaround:** we decode with `bao_tree::ResponseDecoder` ourselves, copy every parent node into our own per-hash outboard, then feed leaves to `import_bao`. The serve path encodes from that copy. Every blob's proof tree lives twice.

**In deCDN:** [`cache/src/fill_session.rs`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/fill_session.rs#L13)

**Ask:** a public `Outboard` or `AsyncSliceReader` over a partial entry, for example `Blobs::outboard(hash)`, or an import variant that yields the parent nodes it validates.

### 19. A codec crate without the store

**iroh-blobs / bao-tree · API**

Our CLI and client SDK speak the iroh-blobs wire format, but iroh-blobs brings redb, irpc and the fs-store, which dominate their binary size. So a small crate of ours re-implements on bao-tree what iroh-blobs already has: `IROH_BLOCK_SIZE` (with a lock-step test against upstream), a `Hash` type that serializes byte for byte like `iroh_blobs::Hash`, the pre-order outboard encoder, verified range encode and verify, and encoded-size math. We also depend on `iroh-io` directly, because bao-tree re-exports `AsyncSliceReader` and `AsyncSliceWriter` but not `AsyncStreamReader`.

**In deCDN:** [`crates/bao-range`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/bao-range/src/lib.rs)

**Ask:** a store-free crate (for example `iroh-blobs-proto`) with `Hash`, `BlobFormat`, the block size, range helpers and the bao wire codec, or an iroh-blobs feature set that builds only those. Re-export `AsyncStreamReader` from bao-tree.

### 20. Re-export the congestion controller configs

**iroh · API**

iroh re-exports the `ControllerFactory` trait ([`endpoint.rs:107`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint.rs#L107)) but no controller config. iroh's own example for `congestion_controller_factory` imports `noq_proto` directly ([`endpoint/quic.rs:411-417`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint/quic.rs#L411-L417)). To select BBR3, every user adds a direct `noq-proto` dependency that must resolve to exactly iroh's version, or the trait objects do not match. BBR matters for bulk transfer ([iroh#4286](https://github.com/n0-computer/iroh/issues/4286)).

**In deCDN:** [`Cargo.toml:79-88`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/Cargo.toml#L79-L88), [`node/src/runtime/mod.rs:281`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/runtime/mod.rs#L281)

**Ask:** re-export `noq_proto::congestion::{Bbr3Config, CubicConfig, NewRenoConfig}` from `iroh::endpoint`.

### 21. Forget a byte range

**iroh-blobs · API**

iroh-blobs can drop a whole blob through GC but not part of one. A cache under a disk quota wants to keep the hot head of a large file and release its cold tail.

**Workaround:** none. Our partial-eviction design waits on this.

**Ask:** `Blobs::forget_ranges(hash, ChunkRanges)`, which clears the range in the bitfield and outboard and hole-punches the data file in one step.

### 22. A concurrency cap on `Router`

**iroh · API**

`Router` spawns one task per accepted connection with no cap. [`Router::incoming_filter`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L478) refuses connections before the handshake, which covers per-source rules, but it keeps no count of live connections, so a global cap still needs a semaphore in every handler.

**Workaround:** a semaphore plus a per-IP rate limiter at the top of every `ProtocolHandler::accept`.

**In deCDN:** [`node/src/dispatch.rs`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/dispatch.rs)

**Ask:** `Router::max_concurrent_connections`, answered with `Reject` when full, before the handshake.

### 23. `Router` logs every handler error at WARN, without its chain

**iroh · API**

When `ProtocolHandler::accept` returns `Err`, `Router` logs it with `warn!("... {err}")` ([`protocol.rs:665`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L665)): at WARN, with no rate limit, and with `{err}` rather than the `{err:#}` used for accept failures a few lines above ([`640`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/protocol.rs#L640)), so the source chain is not printed. Under a connection flood, refusing load by returning `Err` multiplies log volume.

**Workaround:** handlers close with an application error code, return `Ok(())` and log on their own, rate-limited.

**In deCDN:** [`node/src/handlers/probe.rs:288`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/handlers/probe.rs#L288)

**Ask:** print the full chain, and make the level configurable or rate-limit the line.

### 24. `Sync` bounds that need only `Send`

**iroh-blobs / iroh-io · API**

- The GC [`ProtectCb`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/store/gc.rs#L169) future must be `Send + Sync`, but the store's own `list` and `status` futures are only `Send`. We spawn a task and bridge through a oneshot purely to satisfy the bound.
- [`add_stream`](https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/blobs.rs#L273-L276) takes a `Send + Sync` stream, which forces `Sync` onto our public origin stream type.
- `iroh_io::AsyncStreamReader` methods carry no `Send` bound, so a generic decode loop over it can never be proven `Send`. That pushed our fill path onto a thread with its own runtime, which is how we hit item 1.

**In deCDN:** [`cache/src/engine.rs:1073-1078`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/engine.rs#L1073-L1078), [`cache/src/origin/mod.rs:65`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/cache/src/origin/mod.rs#L65)

**Ask:** relax `ProtectCb` and `add_stream` to `Send`, and offer a `Send` variant of `AsyncStreamReader`.

### 25. Transport limits per ALPN

**iroh · API**

`QuicTransportConfig` is set once per endpoint ([`endpoint.rs:680`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint.rs#L680)). We serve three ALPNs: a probe takes one stream, our DHT a few, paid delivery up to 100. The QUIC layer can only enforce the loosest limit.

**Workaround:** `max_concurrent_bidi_streams = 100` at the transport, with tighter caps inside each handler.

**In deCDN:** [`node/src/runtime/mod.rs:133`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/runtime/mod.rs#L133)

**Ask:** per-ALPN overrides for at least the stream limits.

### 26. A signal for "direct path ready"

**iroh · API**

We pick providers by RTT. On a cold dial the first exchange runs over the relay, so timing it read 200-330 ms against about 30 ms on the direct path that followed.

**Workaround:** we time only the request exchange, then watch [`paths_stream()`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint/connection.rs#L1197) for up to 500 ms for a selected IP path and use its `rtt()`.

**In deCDN:** [`client/src/probe.rs:88-100`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/client/src/probe.rs#L88-L100)

**Ask:** a future that resolves when a direct path is selected or known to be impossible. Related: [iroh#4201](https://github.com/n0-computer/iroh/issues/4201).

### 27. `presets::Minimal` sets no relay mode

**iroh · Docs**

[`presets::Minimal`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint/presets.rs#L59-L79) sets only the crypto provider. We moved from `N0` to `Minimal` to run our own address lookup, and relays went away with n0 discovery without a word.

**Workaround:** we set `RelayMode::Default` explicitly with `Minimal`, and read [`home_relay_status()`](https://github.com/n0-computer/iroh/blob/0072d7d84b233f9e7185eb676f049beaf557ac03/iroh/src/endpoint.rs#L1408) after bind to warn when no relay connects.

**In deCDN:** [`node/src/runtime/mod.rs:2916-2932`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/node/src/runtime/mod.rs#L2916-L2932), [`client/src/endpoint.rs:130-158`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/crates/client/src/endpoint.rs#L130-L158); the relay-status check is in [decdn#2250](https://github.com/decdn/decdn/pull/2250)

**Ask:** say on each preset what it sets and leaves unset.

### 28. `genawaiter` brings an unmaintained crate

**bao-tree / iroh-blobs · Build**

`genawaiter` pulls in `proc-macro-error`, which is flagged unmaintained ([RUSTSEC-2024-0370](https://rustsec.org/advisories/RUSTSEC-2024-0370)), through bao-tree and iroh-blobs. Every downstream `cargo deny` config carries an ignore for it.

**In deCDN:** [`deny.toml:19`](https://github.com/decdn/decdn/blob/ef84c76aadb6e258c4aa019e06eb10b34f216282/deny.toml#L19)

**Ask:** replace `genawaiter` in bao-tree and iroh-blobs.

## Resolved or withdrawn since our first draft

We first wrote this list against iroh 1.0.0. Moving to 1.3.0 and writing the tests settled several items, and we dropped them:

- **Fixed upstream:** net-report spans no longer open at WARN (iroh 1.0.1, [#4375](https://github.com/n0-computer/iroh/issues/4375)). iroh 1.3 ships stable `ed25519-dalek` 3.0.0 and `curve25519-dalek` 5.0.0.
- **Already in iroh, and now in our code:** `Endpoint::home_relay_status()` with `RelayStatus::last_error` and `auth_denied_reason` replaced our TCP probe of relay hosts. `Router::incoming_filter` covers pre-handshake refusal (item 22 narrows to the cap). `AcceptError::from_boxed` keeps an error chain; we had flattened ours to a string.
- **Not reproduced by a test:** `RecvStream::is_0rtt()` is documented as true only when the server opted into 0-RTT, and it read true in 20 of 20 runs once we did. An empty-range bao encoding does not validate against any root in release builds. `export_bao` on an absent range errors rather than returning zeros (item 12). `observe()` on an unknown hash reports not complete (item 13 covers what it does do).

## Patterns across these items

Five themes account for most of the list.

1. **Errors become hangs or silent success.** A source error hangs `add_stream` (3), a setup error hangs `FsStore::load` (2), a shutdown turns an export into `Ok` with 0 bytes (4), and a dropped runtime hangs `Endpoint::close` (1). Every hang we found sits on an error or teardown path, where tests rarely look.
2. **Hidden runtime affinity.** Connection drivers, the GC task and the store actor run on whichever runtime was current when they started (1, 2, 17, 24). Item 1 was our most expensive production bug.
3. **Partial blobs are second class.** Sizing, observing, exporting, outboard access and range removal (10, 11, 12, 13, 18, 21) assume a blob is either absent or complete. Serving while filling is the core of a CDN, and most of our cache code works around this.
4. **Codec and store in one crate.** Light clients that only verify iroh-blobs ranges re-implement the codec (19).
5. **Server defaults suit demos more than public services.** 0-RTT on for every ALPN, no connection cap, unbounded shutdown and unthrottled WARN lines (8, 9, 22, 23).

Thank you for iroh. The core transport, hole punching and verified streaming held up well at every stage, and 1.0 made the upgrade path smooth.
