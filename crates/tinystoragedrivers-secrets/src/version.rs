//! Opaque secret revisions for persistent engines.

use sha2::{Digest, Sha256};
use tinystoragedrivers_types::SecretVersion;

/// Build a stable revision from non-plaintext identity and revision parts.
pub(crate) fn version_from_parts(parts: &[&[u8]]) -> SecretVersion {
    let mut digest = Sha256::new();
    digest.update(b"tinystoragedrivers-secrets/version/v1\0");
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    SecretVersion::from_bytes(digest.finalize().into())
}
