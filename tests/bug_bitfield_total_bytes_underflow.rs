//! `Bitfield::total_bytes` underflows on a range that starts past `size`.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source: <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/api/proto/bitfield.rs#L141-L153>
//!
//! `total_bytes` clamps each range end to `self.size` but leaves the start
//! unclamped, then computes `end - start` with unchecked `u64` subtraction.
//! A range whose start lies past `size` gives `start > end`. In a debug build
//! this panics with "attempt to subtract with overflow"; in a release build it
//! wraps to a value near `u64::MAX`.
//!
//! `Bitfield::new` is public (`iroh_blobs::api::proto::Bitfield`, re-exported
//! as `iroh_blobs::api::blobs::Bitfield`) and keeps such ranges as given
//! (bitfield.rs L78-L91): it only canonicalizes a complete or validated
//! bitfield. The `Deserialize` impl (L50-L70) also goes through `new`, so a
//! peer or client can produce the same value.
//!
//! The test builds `Bitfield::new(chunks 10..11, size 100)` and asserts
//! `total_bytes()` returns 0 without a panic, since no byte of chunk 10 lies
//! below byte 100.
//!
//! Observed at the pinned version: in a debug build `total_bytes()` panics with
//! "attempt to subtract with overflow" at bitfield.rs L150; in a release build
//! (`cargo test --release`) it returns 18446744073709541476 (`100 - 10240`
//! wrapped).
//!
//! The test reaches the bitfield through `Bitfield::new`. It does not show a
//! store `observe()` producing one.

use std::panic::{AssertUnwindSafe, catch_unwind};

use bao_tree::{ChunkNum, ChunkRanges};
use iroh_blobs::api::proto::Bitfield;

#[test]
fn total_bytes_is_zero_for_range_past_size() {
    let bitfield = Bitfield::new(ChunkRanges::from(ChunkNum(10)..ChunkNum(11)), 100);
    assert_eq!(bitfield.size(), 100);
    assert!(!bitfield.is_complete());

    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let res = catch_unwind(AssertUnwindSafe(|| bitfield.total_bytes()));
    std::panic::set_hook(prev_hook);

    match res {
        Ok(total) => {
            println!("total_bytes() = {total}");
            assert_eq!(
                total, 0,
                "Bitfield::total_bytes wraps on a range that starts past size"
            );
        }
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_default();
            println!("total_bytes() panicked: {msg}");
            panic!("Bitfield::total_bytes panics on a range that starts past size: {msg}");
        }
    }
}
