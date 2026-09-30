//! Empty `ChunkRanges` panic the async encoder and both decoders in debug builds.
//!
//! Crate: bao-tree 0.16.1 (feature `tokio_fsm`).
//!
//! Upstream source at the 0.16.1 release commit:
//! - `PreOrderPartialChunkIterRef::next` asserts non-empty ranges:
//!   <https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/iter.rs#L547>
//!   `PreOrderPartialChunkIterRef::new` pushes the root with the caller's ranges
//!   unchanged, so an empty `ChunkRanges` reaches that assert on the first `next()`:
//!   <https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/iter.rs#L513-L516>
//! - The sync encoder short-circuits on empty ranges before it builds the iterator:
//!   <https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/io/sync.rs#L484-L486>
//! - The async encoder has no such check and iterates directly:
//!   <https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/io/fsm.rs#L586-L599>
//! - Both decoders build the same iterator via `ResponseIterRef::new` with no check:
//!   <https://github.com/n0-computer/bao-tree/blob/2be9abd144783455606424424c29bd3a57f926f8/src/io/sync.rs#L321-L339>
//!
//! What the tests assert:
//! - `sync_encode_empty_ranges_is_empty`: the sync encoder accepts empty ranges and
//!   writes nothing. Sanity check. Passes.
//! - `fsm_encode_empty_ranges_matches_sync`: the async `encode_ranges_validated` with
//!   empty ranges returns `Ok` and writes the same bytes as the sync encoder. Fails:
//!   the task panics with `assertion failed: !ranges.is_empty()` at `src/iter.rs:547`.
//! - `sync_decode_of_sync_empty_encoding_succeeds`: the sync decoder with empty ranges
//!   accepts the (empty) output of the sync encoder with the correct root. Fails: it
//!   panics at the same assert.
//!
//! Each test runs for a 0-byte blob and a 3 chunk group blob (block size
//! `BlockSize::from_chunk_log(4)`, 48 KiB).
//!
//! Observed at 0.16.1 in debug builds: the fsm encoder, the fsm decoder and the sync
//! decoder all panic on empty ranges. In release builds (assert compiled out) the
//! sync and fsm encoders disagree: for the 48 KiB blob the sync encoder writes 0
//! bytes while the fsm encoder writes the 64-byte root parent pair. The decoders in
//! release expect that root proof, so they reject the sync encoder's empty output
//! with `ParentNotFound(TreeNode(31))`: the sync encoder and decoder do not round-trip
//! empty ranges.

use std::panic::{AssertUnwindSafe, catch_unwind};

use bao_tree::io::outboard::{EmptyOutboard, PostOrderMemOutboard};
use bao_tree::{BlockSize, ChunkRanges};
use bytes::Bytes;

const BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);
/// 0 bytes, and 3 chunk groups of 16 KiB.
const LENGTHS: [usize; 2] = [0, 3 * 16 * 1024];

fn blob(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic>".to_string())
}

fn sync_encode_empty(data: &[u8], ob: &PostOrderMemOutboard) -> Vec<u8> {
    let mut out = Vec::new();
    bao_tree::io::sync::encode_ranges_validated(data, ob, &ChunkRanges::empty(), &mut out)
        .expect("sync encode with empty ranges");
    out
}

#[test]
fn sync_encode_empty_ranges_is_empty() {
    for len in LENGTHS {
        let data = blob(len);
        let ob = PostOrderMemOutboard::create(&data, BLOCK_SIZE);
        let out = sync_encode_empty(&data, &ob);
        assert!(
            out.is_empty(),
            "len={len}: sync encoder wrote {} bytes",
            out.len()
        );
    }
}

#[tokio::test]
async fn fsm_encode_empty_ranges_matches_sync() {
    let mut failures = Vec::new();
    for len in LENGTHS {
        let data = blob(len);
        let ob = PostOrderMemOutboard::create(&data, BLOCK_SIZE);
        let expected = sync_encode_empty(&data, &ob);

        let task = tokio::spawn(async move {
            let mut out = Vec::new();
            bao_tree::io::fsm::encode_ranges_validated(
                Bytes::from(data),
                ob,
                &ChunkRanges::empty(),
                &mut out,
            )
            .await
            .map(|()| out)
        });
        match tokio::time::timeout(std::time::Duration::from_secs(10), task).await {
            Err(_) => failures.push(format!("len={len}: fsm encode timed out")),
            Ok(Err(join)) if join.is_panic() => failures.push(format!(
                "len={len}: fsm encode panicked: {}",
                panic_message(&*join.into_panic())
            )),
            Ok(Err(join)) => failures.push(format!("len={len}: fsm encode task failed: {join}")),
            Ok(Ok(Err(e))) => failures.push(format!("len={len}: fsm encode returned {e:?}")),
            Ok(Ok(Ok(out))) if out != expected => failures.push(format!(
                "len={len}: fsm encode wrote {} bytes, sync wrote {}",
                out.len(),
                expected.len()
            )),
            Ok(Ok(Ok(_))) => {}
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn sync_decode_of_sync_empty_encoding_succeeds() {
    let mut failures = Vec::new();
    for len in LENGTHS {
        let data = blob(len);
        let ob = PostOrderMemOutboard::create(&data, BLOCK_SIZE);
        let encoded = sync_encode_empty(&data, &ob);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut target = Vec::<u8>::new();
            let eob = EmptyOutboard {
                tree: ob.tree,
                root: ob.root,
            };
            bao_tree::io::sync::decode_ranges(&encoded[..], &ChunkRanges::empty(), &mut target, eob)
        }));
        match result {
            Err(payload) => failures.push(format!(
                "len={len}: sync decode panicked: {}",
                panic_message(&*payload)
            )),
            Ok(Err(e)) => failures.push(format!("len={len}: sync decode returned {e:?}")),
            Ok(Ok(())) => {}
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
