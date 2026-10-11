//! Persistent scope generations and atomic all-port erasure.
use crate::{
    connection::Db,
    error::during,
    fence::{self, Fencing},
    sql::{Tables, ident},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::Arc;
use tinystoragedrivers_core::{
    PurgeReceipt, PurgeRequest, PurgeStatus, Result, Scope, ScopeGeneration, ScopePurge,
    StorageError, async_trait,
};

fn admission(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
) -> Result<Option<(ScopeGeneration, String)>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            &format!(
                "SELECT generation, operation FROM {} WHERE scope = ?1",
                ident(&tables.admissions)
            ),
            [scope],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(during("read scope admission"))?;
    row.map(|(generation, operation)| {
        Ok((
            ScopeGeneration(
                u64::try_from(generation)
                    .map_err(|_| StorageError::serialization("invalid scope generation"))?,
            ),
            operation,
        ))
    })
    .transpose()
}

/// Must run inside the same immediate transaction as every mutation.
pub(crate) fn check(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    expected: ScopeGeneration,
) -> Result<()> {
    let current = admission(conn, tables, scope)?
        .map_or(ScopeGeneration::default(), |(generation, _)| generation);
    if current != expected {
        return Err(StorageError::fenced(
            "scope writer generation has been revoked",
        ));
    }
    Ok(())
}

pub(crate) struct SqlitePurge {
    pub(crate) db: Arc<Db>,
    pub(crate) tables: Arc<Tables>,
    pub(crate) scope: Scope,
    pub(crate) generation: ScopeGeneration,
    pub(crate) fencing: Option<Arc<Fencing>>,
}
impl std::fmt::Debug for SqlitePurge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePurge")
            .field("scope", &self.scope)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
#[async_trait]
impl ScopePurge for SqlitePurge {
    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        if self
            .fencing
            .as_ref()
            .is_some_and(|binding| binding.guard_scope() == &self.scope)
        {
            return Err(StorageError::invalid_input(
                "purge guard must be outside the erased scope",
            ));
        }
        let tables = Arc::clone(&self.tables);
        let scope = self.scope.clone();
        let expected = self.generation;
        let fencing = self.fencing.clone();
        let request = request.clone();
        self.db.run(move |conn| fence::write(conn, fencing.as_deref(), &tables, |tx| {
            if let Some((generation, operation)) = admission(tx, &tables, scope.as_str())?
                && operation == request.operation_id {
                    return Ok(PurgeReceipt { operation_id: operation, generation, status: PurgeStatus::Complete });
            }
            check(tx, &tables, scope.as_str(), expected)?;
            let generation = expected.0.checked_add(1).and_then(|next| i64::try_from(next).ok())
                .ok_or_else(|| StorageError::backend("scope generation space is exhausted"))?;
            for table in [&tables.docs, &tables.tombstones, &tables.fts, &tables.streams, &tables.stream_meta, &tables.blobs] {
                tx.execute(&format!("DELETE FROM {} WHERE scope = ?1", ident(table)), [scope.as_str()])
                    .map_err(during("purge scope data"))?;
            }
            tx.execute(&format!("INSERT INTO {} (scope, generation, operation) VALUES (?1, ?2, ?3)
                ON CONFLICT(scope) DO UPDATE SET generation=excluded.generation, operation=excluded.operation", ident(&tables.admissions)),
                params![scope.as_str(), generation, request.operation_id]).map_err(during("issue scope generation"))?;
            Ok(PurgeReceipt { operation_id: request.operation_id.clone(), generation: ScopeGeneration(u64::try_from(generation).unwrap_or(0)), status: PurgeStatus::Complete })
        })).await
    }
}
