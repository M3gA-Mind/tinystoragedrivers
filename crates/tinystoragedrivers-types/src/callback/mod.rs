//! Data-only exact-instance callback routing; never storage authority.
use serde::{Deserialize, Serialize};

/// The finite native callback profile, versioned independently of storage data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopedStorageProfile {
    /// Documents, native JSON streams and secrets; no blobs or watches.
    DocumentsStreamsSecretsV1,
}

/// Routing to an already admitted, exact module-instance callback binding.
///
/// No scope, backend, fence, generation or schema grant can be carried here.
/// The transport validates the unique destination and object path before use.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedStorageRoute {
    /// Protocol version; only 1 is supported.
    pub version: u32,
    /// Unique bus destination; well-known names are forbidden.
    pub destination: String,
    /// Root beneath which Documents, Streams and Secrets are served.
    pub object_root: String,
    /// The supported finite port profile.
    pub profile: ScopedStorageProfile,
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
