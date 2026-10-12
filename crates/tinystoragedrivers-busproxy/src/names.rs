//! Where the storage objects live on the bus, and how errors are named.
use tinybus::{BusName, ObjectPath};
use tinystoragedrivers_bus::STORAGE_INTERFACE;
use tinystoragedrivers_client::Object;

/// The well-known name the host owns, and the interface its objects speak.
pub const HOST_NAME: &str = STORAGE_INTERFACE;
/// Where the four objects are served: `<root>/<Object>`.
pub const OBJECT_ROOT: &str = "/ai/tinyhumans/tinystorage";
/// Error name for a call the host refused before reaching a backend.
pub const ERROR_DENIED: &str = "ai.tinyhumans.tinystorage.Error.Denied";
/// Error name for a backend failure; the message is the
/// `snake_case` `StorageKind`.
pub const ERROR_STORAGE: &str = "ai.tinyhumans.tinystorage.Error.Storage";
/// Signal member that carries a payload-free `WatchChanged`.
pub const WATCH_MEMBER: &str = "WatchChanged";

/// The object path serving `object`.
///
/// # Errors
/// Never in practice: the root and object names are fixed valid names.
pub fn object_path(object: Object) -> tinybus::Result<ObjectPath> {
    ObjectPath::new(format!("{OBJECT_ROOT}/{}", object.as_str()))
}

/// The path the host emits `recipient`'s watch signals on.
///
/// Watch ids are minted per caller binding (`w1`, `w2`, ...), so two callers'
/// signals cannot share an address: each caller subscribes to the path derived
/// from its own unique bus name, and only the host's unique name is accepted
/// as the sender. A unique name `:1.17` becomes `_1_17`, which is injective
/// for names of that shape.
///
/// # Errors
/// A recipient that is not a unique bus name.
pub fn watch_path(recipient: &BusName) -> tinybus::Result<ObjectPath> {
    if !recipient.is_unique() {
        return Err(tinybus::Error::failed(
            "watch signals address a unique bus name",
        ));
    }
    let element: String = recipient
        .as_str()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    ObjectPath::new(format!("{OBJECT_ROOT}/Watch/{element}"))
}
