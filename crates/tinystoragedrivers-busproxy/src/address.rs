//! A request-scoped address for a set of storage ports on one host connection.

use tinybus::{BusName, ObjectPath};

use crate::transport::Object;

/// Prefix reserved for invocation-scoped storage endpoints.
pub const INVOCATION_OBJECT_ROOT: &str = "/ai/tinyhumans/tinystorage/Invocation";

/// Identifies one storage endpoint registered on an already-connected host.
///
/// The host name must be the broker-assigned unique name of the host
/// connection. Each invocation gets one root directly below
/// [`INVOCATION_OBJECT_ROOT`]; the six port objects and watch signal path are
/// derived beneath it. This permits concurrent endpoints on the same
/// connection without replacing one another's `TinyBus` object registrations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageAddress {
    host: BusName,
    object_root: ObjectPath,
    documents_path: ObjectPath,
    streams_path: ObjectPath,
    blobs_path: ObjectPath,
    secrets_path: ObjectPath,
    queues_path: ObjectPath,
    kv_path: ObjectPath,
    watch_path: ObjectPath,
}

impl StorageAddress {
    /// Validate an invocation-scoped address.
    ///
    /// `object_root` must contain exactly one path component below the
    /// invocation prefix. Callers should generate that component rather than
    /// accepting it as an authority selector from untrusted module input.
    ///
    /// # Errors
    /// Returns an error if the host is not unique, the root is outside the
    /// invocation namespace, or a derived port path is invalid or too long.
    pub fn try_new(host: BusName, object_root: ObjectPath) -> tinybus::Result<Self> {
        let prefix = format!("{INVOCATION_OBJECT_ROOT}/");
        let Some(component) = object_root.as_str().strip_prefix(&prefix) else {
            return Err(tinybus::Error::failed(
                "storage address root is outside the invocation namespace",
            ));
        };
        if !host.is_unique() || component.is_empty() || component.contains('/') {
            return Err(tinybus::Error::failed(
                "storage address requires a unique host and one invocation component",
            ));
        }
        let port_path = |object: Object| {
            ObjectPath::new(format!(
                "{}/{name}",
                object_root.as_str(),
                name = object.as_str()
            ))
        };
        let documents_path = port_path(Object::Documents)?;
        let streams_path = port_path(Object::Streams)?;
        let blobs_path = port_path(Object::Blobs)?;
        let secrets_path = port_path(Object::Secrets)?;
        let queues_path = port_path(Object::Queues)?;
        let kv_path = port_path(Object::Kv)?;
        let watch_path = ObjectPath::new(format!("{}/Watch", object_root.as_str()))?;
        Ok(Self {
            host,
            object_root,
            documents_path,
            streams_path,
            blobs_path,
            secrets_path,
            queues_path,
            kv_path,
            watch_path,
        })
    }

    /// The broker-assigned host connection name.
    #[must_use]
    pub fn host(&self) -> &BusName {
        &self.host
    }

    /// This invocation's root path.
    #[must_use]
    pub fn object_root(&self) -> &ObjectPath {
        &self.object_root
    }

    /// The object path for one storage port.
    #[must_use]
    pub fn port_path(&self, object: Object) -> &ObjectPath {
        match object {
            Object::Documents => &self.documents_path,
            Object::Streams => &self.streams_path,
            Object::Blobs => &self.blobs_path,
            Object::Secrets => &self.secrets_path,
            Object::Queues => &self.queues_path,
            Object::Kv => &self.kv_path,
        }
    }

    /// The path on which this endpoint emits watch invalidations.
    #[must_use]
    pub fn watch_path(&self) -> &ObjectPath {
        &self.watch_path
    }
}

#[cfg(test)]
#[path = "address_tests.rs"]
mod tests;
