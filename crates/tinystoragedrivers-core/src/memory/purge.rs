//! Atomic scope erase and native admission revocation.
use super::{Arc, Clock, MemoryDb, MemoryFence, Result, Scope, StorageError};
use crate::{PurgeReceipt, PurgeRequest, PurgeStatus, ScopeGeneration, ScopePurge, async_trait};

pub(super) struct MemoryPurge {
    pub(super) db: Arc<MemoryDb>,
    pub(super) scope: Scope,
    pub(super) generation: ScopeGeneration,
    pub(super) clock: Clock,
    pub(super) fence: Option<Arc<MemoryFence>>,
}

impl std::fmt::Debug for MemoryPurge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryPurge")
            .field("scope", &self.scope)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ScopePurge for MemoryPurge {
    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        if self
            .fence
            .as_ref()
            .is_some_and(|binding| binding.fence.scope() == &self.scope)
        {
            return Err(StorageError::invalid_input(
                "purge guard must be outside the erased scope",
            ));
        }
        let mut state = self.db.lock()?;
        let now = if self.fence.is_some() {
            (self.clock)()
        } else {
            0
        };
        self.db.guard(&state, self.fence.as_deref(), now)?;
        if let Some((generation, operation)) = state.admissions.get(self.scope.as_str())
            && operation == &request.operation_id
        {
            return Ok(PurgeReceipt {
                operation_id: operation.clone(),
                generation: *generation,
                status: PurgeStatus::Complete,
            });
        }
        state.check_generation(self.scope.as_str(), self.generation)?;
        let generation = ScopeGeneration(
            self.generation
                .0
                .checked_add(1)
                .ok_or_else(|| StorageError::backend("scope generation space is exhausted"))?,
        );
        let retain = |key: &super::state::ScopedKey| key.0 != self.scope.as_str();
        state.docs.retain(|key, _| retain(key));
        state.tombstones.retain(|key, _| retain(key));
        state.streams.retain(|key, _| retain(key));
        state.blobs.retain(|key, _| retain(key));
        state.admissions.insert(
            self.scope.as_str().to_owned(),
            (generation, request.operation_id.clone()),
        );
        Ok(PurgeReceipt {
            operation_id: request.operation_id.clone(),
            generation,
            status: PurgeStatus::Complete,
        })
    }
}
