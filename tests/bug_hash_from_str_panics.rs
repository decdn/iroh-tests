//! `Hash::from_str` panics on wrong-length input instead of returning `Err`.
//!
//! Crate: iroh-blobs 0.103.0.
//!
//! Source: <https://github.com/n0-computer/iroh-blobs/blob/e82cbdcbdac9a78033174aad55e3199b2cf4c0dc/src/hash.rs#L151-L172>
//!
//! `from_str` decodes into a fixed `[u8; 32]` buffer with
//! `data_encoding::{HEXLOWER, BASE32_NOPAD}::decode_mut`. `decode_mut` panics
//! when the output length does not equal `decode_len(input.len())`, and
//! `from_str` never checks `decode_len` first. Every input whose decoded length
//! is not exactly 32 bytes reaches that panic.
//!
//! The test parses a set of malformed strings and asserts each one returns
//! `Err` without a panic.
//!
//! Observed at the pinned version (data-encoding 2.11.1): all seven malformed
//! inputs panic with `assertion left == right failed` inside `decode_mut`
//! (for example 53 chars: `left: Ok(32), right: Ok(33)`). Only a length of
//! exactly 64 takes the hex branch, so a 63- or 65-char hex string goes to the
//! base32 decoder and panics there. The 52-char base32 and 64-char hex
//! controls parse.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::str::FromStr;

use iroh_blobs::Hash;

#[test]
fn hash_from_str_returns_err_on_wrong_length() {
    // 52 base32 chars encode 32 bytes; "a" is the zero symbol, so this parses.
    let valid_b32 = "a".repeat(52);
    assert!(
        Hash::from_str(&valid_b32).is_ok(),
        "52-char base32 control parses"
    );
    let valid_hex = Hash::new(b"x").to_hex().to_string();
    assert_eq!(valid_hex.len(), 64);
    assert!(
        Hash::from_str(&valid_hex).is_ok(),
        "64-char hex control parses"
    );

    let inputs: Vec<(&str, String)> = vec![
        ("empty", String::new()),
        ("3 chars", "abc".to_string()),
        ("4 chars", "aaaa".to_string()),
        ("51-char base32", valid_b32[..51].to_string()),
        ("53-char base32", format!("{valid_b32}a")),
        ("63-char hex", valid_hex[..63].to_string()),
        ("65-char hex", format!("{valid_hex}0")),
    ];

    // Silence the default hook so the output shows one line per input.
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let mut panicked = Vec::new();
    for (label, s) in &inputs {
        let res = catch_unwind(AssertUnwindSafe(|| Hash::from_str(s).map(|_| ())));
        match res {
            Ok(Ok(())) => println!("{label} ({} chars): Ok (unexpected)", s.len()),
            Ok(Err(e)) => println!("{label} ({} chars): Err({e})", s.len()),
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
                    .unwrap_or_default();
                println!("{label} ({} chars): PANIC: {msg}", s.len());
                panicked.push(*label);
            }
        }
    }
    std::panic::set_hook(prev_hook);

    assert!(
        panicked.is_empty(),
        "Hash::from_str panics instead of returning Err on wrong-length input: {panicked:?}"
    );
}
