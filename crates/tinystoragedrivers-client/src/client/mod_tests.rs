//! Client behavior that conformance does not pin: binding and error mapping.
use std::sync::Arc;

use tinystoragedrivers_core::{Capability, ErrorKind, MemoryStorage, Scope, StorageBackend};

use super::*;
use crate::{Admission, Loopback, StorageServer};

async fn bus() -> BusStorage {
    let backend = MemoryStorage::new();
    let server = StorageServer::new(backend.for_scope(&Scope::local()).unwrap());
    BusStorage::connect(
        Arc::new(Loopback::new(Arc::new(server), Admission::Private)),
        Scope::local(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_bus_caller_is_bound_to_its_scope_and_cannot_pick_databases_or_fences() {
    let bus = bus().await;
    assert_eq!(bus.driver(), "bus");
    assert!(bus.for_scope(&Scope::local()).is_ok());
    let other = Scope::new("bob").unwrap();
    assert_eq!(
        bus.for_scope(&other).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        bus.database("other").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    for host_only in [
        Capability::Fencing,
        Capability::ScopePurge,
        Capability::ExistingSchema,
    ] {
        assert!(!bus.capabilities().contains(host_only));
    }
    let fence = tinystoragedrivers_core::Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    assert!(bus.for_scope_fenced(&Scope::local(), &fence).is_err());
}

#[tokio::test]
async fn unsupported_refusals_name_the_capability_the_call_needs() {
    let bus = bus().await;
    let storage = bus.for_scope(&Scope::local()).unwrap();
    let expected = Capability::PresignedUrls;
    let error = storage
        .blobs()
        .presign_get("k", std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(expected));
}
