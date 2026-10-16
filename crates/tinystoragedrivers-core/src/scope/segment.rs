//! Segment encoding: the legal set, the length limit and the hashed form.

use data_encoding::BASE32_NOPAD;
use sha2::{Digest, Sha256};

use super::MAX_SEGMENT_LEN;
use crate::error::{Result, StorageError};

/// The marker that starts a hashed segment.
const HASH_MARKER: char = '~';
/// Characters of base32 kept from the digest.
const HASH_CHARS: usize = 26;

fn is_legal_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@')
}

fn is_plain(segment: &str) -> bool {
    !segment.is_empty() && segment.len() <= MAX_SEGMENT_LEN && segment.bytes().all(is_legal_byte)
}

fn is_hashed(segment: &str) -> bool {
    segment.strip_prefix(HASH_MARKER).is_some_and(|rest| {
        rest.len() == HASH_CHARS && rest.bytes().all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7'))
    })
}

/// Whether `segment` is a stored segment: plain, or in the hashed form.
pub(super) fn is_valid(segment: &str) -> bool {
    is_plain(segment) || is_hashed(segment)
}

/// The stored form of the raw `id`: kept as is when legal, hashed otherwise.
///
/// Fails only for an empty `id`.
pub(super) fn encode(id: &str) -> Result<String> {
    if id.is_empty() {
        return Err(StorageError::invalid_input("scope id must not be empty"));
    }
    if is_plain(id) {
        return Ok(id.to_owned());
    }
    let digest = Sha256::digest(id.as_bytes());
    let mut text = BASE32_NOPAD.encode(&digest).to_ascii_lowercase();
    text.truncate(HASH_CHARS);
    Ok(format!("{HASH_MARKER}{text}"))
}
