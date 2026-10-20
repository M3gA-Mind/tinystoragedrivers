//! Mechanical conversions between native port types and bus wire types.
pub(crate) mod queues;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tinystoragedrivers_bus::{StorageFailure, StorageKind, Versioned, WriteOp};
use tinystoragedrivers_types::{Capabilities, Capability, ErrorKind, StorageError};

/// Capabilities bus callers never see.
///
/// Fencing and the existing-schema open are the host's own
/// lifecycle controls. Two more are host-only by the storage-bus trust model,
/// even though the wire could carry them: `Maintenance` (`maintain`,
/// `checkpoint`) lets the caller trigger vacuum, WAL checkpoints and
/// seal/retention sweeps across the whole database, and `ScopeEnumeration`
/// (`scopes`) lists the other tenants. A module is bound to one scope and
/// must neither learn that others exist nor spend the host's I/O budget, so
/// the contract has no selector for either and the host runs them natively.
///
/// `SharedAcrossProcesses` and `CrossProcessCas` describe how a host opens its
/// own backends and coordinates across clients, which a module bound to one
/// grant cannot do.
///
/// `RecordMetadata` (contract version 4: `Meta` on every read, `schema` and
/// `expires_ms` on `Put`) and `StreamSeal` (`Streams.Seal`) act only inside
/// the module's own grant, so they are exposed.
pub(crate) const HOST_ONLY: [Capability; 2] = [
    Capability::SharedAcrossProcesses,
    Capability::CrossProcessCas,
];

/// Convert between types that share one serde shape (filters, sorts, specs,
/// preconditions, write results, read checks, queries).
pub(crate) fn via<T: Serialize + ?Sized, U: DeserializeOwned>(
    value: &T,
) -> std::result::Result<U, StorageError> {
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
        StorageKind::Backend | StorageKind::Fenced => StorageError::backend(message),
    }
}

pub(crate) fn versioned_to_wire<T>(found: Versioned<T>) -> Versioned<T> {
    found
}

pub(crate) fn versioned_from_wire<T>(found: Versioned<T>) -> Versioned<T> {
    found
}

pub(crate) fn op_to_wire(op: WriteOp) -> WriteOp {
    op
}

pub(crate) fn op_from_wire(op: WriteOp) -> WriteOp {
    op
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

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
