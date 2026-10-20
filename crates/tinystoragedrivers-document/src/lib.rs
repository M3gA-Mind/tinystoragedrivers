//! Document and append-only stream primitive engines.
//!
//! These engines implement the standalone storage ports without host or
//! legacy-engine dependencies. Each adapter binds both ports to one scope.

#[cfg(feature = "file")]
mod file;
#[cfg(feature = "file")]
pub use file::documents::FileDocuments;
#[cfg(feature = "file")]
pub use file::streams::FileStreams;
#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "sqlite")]
pub mod sqlite;

mod stream;

use std::sync::Arc;

/// Clock shared by local engines, in Unix epoch milliseconds.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[cfg(feature = "memory")]
fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn validate_stream(stream: &str) -> tinystoragedrivers_types::Result<()> {
    if stream.is_empty() || stream.len() > 512 || stream.as_bytes().contains(&0) {
        return Err(tinystoragedrivers_types::StorageError::invalid_input(
            "stream name must contain 1..=512 non-NUL bytes",
        ));
    }
    Ok(())
}

fn sealed_blob_key(stream: &str) -> tinystoragedrivers_types::Result<String> {
    use data_encoding::BASE32_NOPAD;
    use sha2::{Digest, Sha256};
    use tinystoragedrivers_types::{Key, MAX_KEY_LEN};
    validate_stream(stream)?;
    let mut escaped = String::new();
    for (index, ch) in stream.chars().enumerate() {
        match ch {
            '%' => escaped.push_str("%25"),
            '/' => escaped.push_str("%2F"),
            '\0' => escaped.push_str("%00"),
            '~' if index == 0 => escaped.push_str("%7E"),
            other => escaped.push(other),
        }
    }
    if escaped == "." {
        escaped = "%2E".to_owned();
    }
    if escaped == ".." {
        escaped = "%2E.".to_owned();
    }
    if escaped.len() > tinystoragedrivers_types::MAX_ID_LEN {
        let digest = Sha256::digest(escaped.as_bytes());
        escaped = format!("~{}", BASE32_NOPAD.encode(&digest).to_ascii_lowercase());
    }
    let key = format!("_tsd/sealed/{escaped}");
    if key.len() > MAX_KEY_LEN {
        return Err(tinystoragedrivers_types::StorageError::invalid_input(
            "sealed stream key exceeds maximum length",
        ));
    }
    Key::new(key.clone())?;
    Ok(key)
}
