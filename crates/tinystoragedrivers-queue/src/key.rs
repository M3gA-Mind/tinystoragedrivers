//! Queue record keys compatible with the legacy core encoding.

#[cfg(feature = "document")]
use sha2::{Digest, Sha256};
#[cfg(feature = "document")]
use tinystoragedrivers_types::{Key, MAX_ID_LEN, Result};

/// Encode key parts exactly as the previous queue implementation did.
#[cfg(feature = "document")]
pub(crate) fn of(parts: &[&str]) -> Result<Key> {
    let mut encoded = String::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            encoded.push('/');
        }
        for (char_index, ch) in part.chars().enumerate() {
            match ch {
                '%' => encoded.push_str("%25"),
                '/' => encoded.push_str("%2F"),
                '\0' => encoded.push_str("%00"),
                '~' if index == 0 && char_index == 0 => encoded.push_str("%7E"),
                _ => encoded.push(ch),
            }
        }
    }
    if encoded.len() > MAX_ID_LEN {
        encoded = format!(
            "~{}",
            data_encoding::BASE32_NOPAD
                .encode(&Sha256::digest(encoded.as_bytes()))
                .to_ascii_lowercase()
        );
    }
    Key::new(encoded)
}

/// Return a fresh sortable id.
pub(crate) fn ulid() -> String {
    ulid::Ulid::new().to_string()
}

#[cfg(all(test, feature = "document"))]
#[path = "key_tests.rs"]
mod tests;
