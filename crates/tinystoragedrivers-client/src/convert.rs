//! Mechanical conversions between native port types and bus wire types.
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tinystoragedrivers_bus::{StorageFailure, StorageKind, Version, Versioned, WriteOp};
use tinystoragedrivers_core::{
    Capabilities, Capability, ErrorKind, StorageError, Versioned as NativeVersioned,
    WriteOp as NativeWriteOp,
};

/// Capabilities a host keeps to itself: they are not exposed to bus callers.
/// `StreamSeal` is here until the bus contract carries the seal call (schema
/// v2 task 10); advertising it would promise a call the wire cannot make.
pub(crate) const HOST_ONLY: [Capability; 6] = [
    Capability::StreamSeal,
    Capability::Fencing,
    Capability::CrossDatabaseFencing,
    Capability::ScopePurge,
    Capability::AtomicScopePurge,
    Capability::ExistingSchema,
];

/// Convert between types that share one serde shape (filters, sorts, specs,
/// preconditions, write results, read checks, queries).
pub(crate) fn via<T: Serialize + ?Sized, U: DeserializeOwned>(
    value: &T,
) -> Result<U, StorageError> {
    let wire = serde_json::to_value(value)?;
    Ok(serde_json::from_value(wire)?)
}

pub(crate) fn failure_of(error: &StorageError) -> StorageFailure {
    let kind = match error.kind() {
        ErrorKind::NotFound => StorageKind::NotFound,
        ErrorKind::Conflict => StorageKind::Conflict,
        ErrorKind::AlreadyExists => StorageKind::AlreadyExists,
        ErrorKind::Unsupported(_) => StorageKind::Unsupported,
        ErrorKind::InvalidInput => StorageKind::InvalidInput,
        ErrorKind::Serialization => StorageKind::Serialization,
        ErrorKind::Crypto => StorageKind::Crypto,
        ErrorKind::Unavailable => StorageKind::Unavailable,
        ErrorKind::Fenced => StorageKind::Fenced,
        ErrorKind::ScopeGenerationRejected => StorageKind::ScopeGenerationRejected,
        _ => StorageKind::Backend,
    };
    StorageFailure { kind }
}

/// Rebuild a native error. The wire carries only a category, so an
/// `Unsupported` failure is attributed to the capability the client's call
/// needs (`unsupported_as`).
pub(crate) fn error_of(
    failure: StorageFailure,
    unsupported_as: Option<Capability>,
) -> StorageError {
    let message = "the storage host refused the operation";
    match failure.kind {
        StorageKind::NotFound => StorageError::not_found(message),
        StorageKind::Conflict => StorageError::conflict(message),
        StorageKind::AlreadyExists => StorageError::already_exists(message),
        StorageKind::Unsupported => match unsupported_as {
            Some(capability) => StorageError::unsupported(capability, message),
            None => StorageError::backend("the storage host does not support the operation"),
        },
        StorageKind::InvalidInput => StorageError::invalid_input(message),
        StorageKind::Serialization => StorageError::serialization(message),
        StorageKind::Crypto => StorageError::crypto(message),
        StorageKind::Unavailable => StorageError::unavailable(message),
        StorageKind::Backend => StorageError::backend(message),
        StorageKind::Fenced => StorageError::fenced(message),
        StorageKind::ScopeGenerationRejected => StorageError::scope_generation_rejected(message),
    }
}

pub(crate) fn versioned_to_wire(found: NativeVersioned<Value>) -> Versioned {
    Versioned {
        id: found.id,
        version: Version(found.version.0),
        doc: found.doc,
    }
}

pub(crate) fn versioned_from_wire(found: Versioned) -> NativeVersioned<Value> {
    NativeVersioned {
        id: found.id,
        version: tinystoragedrivers_core::Version(found.version.0),
        doc: found.doc,
    }
}

pub(crate) fn op_to_wire(op: NativeWriteOp) -> Result<WriteOp, StorageError> {
    Ok(match op {
        NativeWriteOp::Put {
            collection,
            id,
            doc,
            precondition,
        } => WriteOp::Put {
            collection,
            id,
            doc,
            precondition: via(&precondition)?,
        },
        NativeWriteOp::Delete {
            collection,
            id,
            precondition,
        } => WriteOp::Delete {
            collection,
            id,
            precondition: via(&precondition)?,
        },
    })
}

pub(crate) fn op_from_wire(op: WriteOp) -> Result<NativeWriteOp, StorageError> {
    Ok(match op {
        WriteOp::Put {
            collection,
            id,
            doc,
            precondition,
        } => NativeWriteOp::Put {
            collection,
            id,
            doc,
            precondition: via(&precondition)?,
        },
        WriteOp::Delete {
            collection,
            id,
            precondition,
        } => NativeWriteOp::Delete {
            collection,
            id,
            precondition: via(&precondition)?,
        },
    })
}

/// Names of the capabilities a bus caller may see, in a stable order.
pub(crate) fn capability_names(capabilities: Capabilities) -> Vec<String> {
    capabilities
        .iter()
        .filter(|capability| !HOST_ONLY.contains(capability))
        .map(|capability| capability.to_string())
        .collect()
}

/// Parse capability names; unknown names (a newer server) are ignored and
/// host-only ones are never granted.
pub(crate) fn capabilities_of(names: &[String]) -> Capabilities {
    Capabilities::all()
        .iter()
        .filter(|capability| !HOST_ONLY.contains(capability))
        .filter(|capability| names.iter().any(|name| *name == capability.to_string()))
        .fold(Capabilities::none(), Capabilities::with)
}
