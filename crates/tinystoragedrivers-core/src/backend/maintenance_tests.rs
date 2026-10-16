//! Maintenance values and the refusals a driver without the abilities gives.

use std::sync::Arc;

use super::*;
use crate::{Capabilities, ErrorKind, MemoryStorage, Result, Scope, ScopedStorage, StorageBackend};

#[derive(Debug)]
struct Plain;

impl StorageBackend for Plain {
    fn driver(&self) -> &'static str {
        "plain"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        MemoryStorage::new().for_scope(scope)
    }
    fn database(&self, _: &str) -> Result<Arc<dyn StorageBackend>> {
        Ok(Arc::new(Plain))
    }
}

#[test]
fn a_budget_is_unlimited_unless_bounded() {
    assert_eq!(MaintenanceBudget::default().max_reclaim_bytes, None);
    assert_eq!(MaintenanceBudget::unlimited(), MaintenanceBudget::default());
    assert_eq!(
        MaintenanceBudget::reclaim_up_to(4096).max_reclaim_bytes,
        Some(4096)
    );
}

#[test]
fn a_report_starts_empty() {
    assert_eq!(MaintenanceReport::default().reclaimed_bytes, 0);
    assert_eq!(MaintenanceReport::new(7).reclaimed_bytes, 7);
}

#[tokio::test]
async fn a_backend_without_the_abilities_refuses_them() {
    let maintain = Plain.maintain(MaintenanceBudget::default()).await;
    assert_eq!(
        maintain.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Maintenance)
    );
    let checkpoint = Plain.checkpoint(Checkpoint::Truncate).await;
    assert_eq!(
        checkpoint.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Maintenance)
    );
    let scopes = Plain.scopes("").await;
    assert_eq!(
        scopes.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopeEnumeration)
    );
}
