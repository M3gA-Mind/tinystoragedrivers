//! Durable admission and staged, quiescent GridFS reset.
use crate::{backend::Shared, errors};
use mongodb::{
    ClientSession,
    bson::{Document, doc},
};
use tinystoragedrivers_core::{Result, Scope, ScopeGeneration, StorageError};

pub(crate) const ADMISSIONS: &str = "_tsd_validated_batch";
pub(crate) fn scope_key(scope: &Scope) -> String {
    format!("scope:{}", scope.as_str())
}

fn verify(document: Option<&Document>, expected: ScopeGeneration) -> Result<()> {
    let current = match &document {
        Some(record) if record.contains_key("generation") => record
            .get_i64("generation")
            .ok()
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| StorageError::serialization("malformed scope generation"))?,
        _ => 0,
    };
    let pending = document
        .as_ref()
        .filter(|record| record.contains_key("pending"))
        .map(|record| {
            record
                .get_bool("pending")
                .map_err(|_| StorageError::serialization("malformed reset admission"))
        })
        .transpose()?
        .unwrap_or(false);
    if pending || current != expected.0 {
        return Err(StorageError::fenced(
            "scope reset is pending or this handle generation has retired",
        ));
    }
    Ok(())
}

/// Transactional mutations check after touching the same scope admission row.
pub(crate) async fn check(
    shared: &Shared,
    scope: &Scope,
    expected: ScopeGeneration,
    session: Option<&mut ClientSession>,
) -> Result<()> {
    let collection = shared.raw(ADMISSIONS);
    let action = collection.find_one(doc! {"_id": scope_key(scope)});
    let document = match session {
        Some(session) => action.session(session).await,
        None => action.await,
    }
    .map_err(errors::failed("read scope admission"))?;
    verify(document.as_ref(), expected)
}

/// Read admission before and after a port operation. Primary reads ensure a
/// monotonic generation change cannot expose replacement data.
macro_rules! admitted {
    ($this:expr, $body:expr) => {{
        $crate::purge::check(&$this.shared, &$this.scope, $this.generation, None).await?;
        let result = async { $body }.await;
        $crate::purge::check(&$this.shared, &$this.scope, $this.generation, None).await?;
        result
    }};
}
pub(crate) use admitted;

use crate::{
    fence::in_write,
    naming::{BLOBS, META},
};
use futures_util::TryStreamExt;
use std::sync::Arc;
use tinystoragedrivers_core::{
    Capability, PurgeMode, PurgeReceipt, PurgeRequest, PurgeStage, PurgeStatus, ScopePurge,
    async_trait,
};

const MANIFEST: &str = "_tsd_purge_blobs";
const RECEIPTS: &str = "_tsd_purge_receipts";

#[derive(Debug)]
pub(crate) struct MongoPurge {
    pub(crate) shared: Arc<Shared>,
    pub(crate) scope: Scope,
    pub(crate) generation: ScopeGeneration,
    pub(crate) fenced: bool,
    #[cfg(test)]
    pub(crate) fail_after_chunks: bool,
}

fn decode(record: &Document) -> Result<PurgeReceipt> {
    let bad = || StorageError::serialization("malformed purge receipt");
    let generation = record
        .get_i64("generation")
        .ok()
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(bad)?;
    let stage = match record.get_str("stage").map_err(|_| bad())? {
        "records" => PurgeStage::Records,
        "blob_manifest" => PurgeStage::BlobManifest,
        "blobs" => PurgeStage::Blobs,
        "complete" => PurgeStage::Complete,
        _ => return Err(bad()),
    };
    let pending = record.get_bool("pending").map_err(|_| bad())?;
    if pending == (stage == PurgeStage::Complete) {
        return Err(bad());
    }
    Ok(PurgeReceipt {
        operation_id: record.get_str("operation").map_err(|_| bad())?.to_owned(),
        generation: ScopeGeneration(generation),
        status: if pending {
            PurgeStatus::Pending
        } else {
            PurgeStatus::Complete
        },
        stage,
    })
}

fn owns_records(prefix: &str, physical: &str) -> bool {
    let Some(name) = physical.strip_prefix(prefix) else {
        return false;
    };
    !name.contains(':')
        && ![META, ADMISSIONS, MANIFEST, RECEIPTS].contains(&name)
        && !name.starts_with(&format!("{BLOBS}."))
        && !name.starts_with("system.")
}

impl MongoPurge {
    async fn archive_complete(&self, record: &Document, session: &mut ClientSession) -> Result<()> {
        let receipt = decode(record)?;
        if receipt.status != PurgeStatus::Complete {
            return Err(StorageError::conflict("cannot archive an incomplete reset"));
        }
        let mut fields = record.clone();
        fields.remove("_id");
        self.shared
            .raw(RECEIPTS)
            .update_one(
                doc! {"_id": {"s": self.scope.as_str(), "o": &receipt.operation_id}},
                doc! {"$setOnInsert": fields},
            )
            .upsert(true)
            .session(session)
            .await
            .map_err(errors::failed("persist completed reset receipt"))?;
        Ok(())
    }

    async fn begin(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        self.shared.prepare_validation(Some(&self.scope)).await?;
        // Initialize the internal collection before a transaction could need
        // to create it. This is trusted native reset metadata, never tenant data.
        let initialized = self
            .shared
            .raw(RECEIPTS)
            .update_one(
                doc! {"_id": "metadata"},
                doc! {"$setOnInsert": {"version": 1_i64}},
            )
            .upsert(true)
            .await;
        if let Err(error) = initialized {
            let error = errors::map(error, "initialize reset receipt metadata");
            if error.kind() != tinystoragedrivers_core::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        in_write!(&self.shared, None, session => {
            self.shared.hold_validation(Some(&self.scope), &mut *session).await?;
            if let Some(record) = self.shared.raw(RECEIPTS).find_one(
                doc! {"_id": {"s": self.scope.as_str(), "o": &request.operation_id}}
            ).session(&mut *session).await.map_err(errors::failed("read completed reset receipt"))? {
                return decode(&record);
            }
            let metadata = self.shared.raw(ADMISSIONS);
            let record = metadata.find_one(doc! {"_id": scope_key(&self.scope)})
                .session(&mut *session).await.map_err(errors::failed("read reset authority"))?
                .ok_or_else(|| StorageError::unavailable("scope admission is missing"))?;
            if record.get_str("operation").ok() == Some(request.operation_id.as_str()) {
                return decode(&record);
            }
            if record.get_bool("pending").unwrap_or(false) {
                return Err(StorageError::conflict("another scope reset remains pending"));
            }
            verify(Some(&record), self.generation)?;
            // Backfill the latest completed receipt from an earlier driver
            // before replacing its admission record with the next operation.
            if record.contains_key("operation") {
                self.archive_complete(&record, &mut *session).await?;
            }
            let next = self.generation.0.checked_add(1)
                .and_then(|v| i64::try_from(v).ok())
                .ok_or_else(|| StorageError::backend("scope generation space is exhausted"))?;
            metadata.update_one(doc! {"_id": scope_key(&self.scope)}, doc! {"$set": {
                "generation": next, "operation": &request.operation_id,
                "pending": true, "stage": "records",
            }}).session(session).await.map_err(errors::failed("close reset admission"))?;
            Ok(PurgeReceipt { operation_id: request.operation_id.clone(),
                generation: ScopeGeneration(u64::try_from(next).map_err(|_| StorageError::serialization("invalid issued generation"))?), status: PurgeStatus::Pending,
                stage: PurgeStage::Records })
        })
    }

    async fn pending(&self, operation: &str, session: &mut ClientSession) -> Result<PurgeReceipt> {
        self.shared
            .hold_validation(Some(&self.scope), &mut *session)
            .await?;
        let record = self
            .shared
            .raw(ADMISSIONS)
            .find_one(doc! {"_id": scope_key(&self.scope)})
            .session(session)
            .await
            .map_err(errors::failed("read pending reset"))?
            .ok_or_else(|| StorageError::unavailable("reset authority is missing"))?;
        let receipt = decode(&record)?;
        if receipt.operation_id != operation || receipt.status != PurgeStatus::Pending {
            return Err(StorageError::conflict(
                "reset operation is no longer pending",
            ));
        }
        Ok(receipt)
    }

    async fn advance(
        &self,
        operation: &str,
        stage: &str,
        session: &mut ClientSession,
    ) -> Result<()> {
        let updated = self
            .shared
            .raw(ADMISSIONS)
            .update_one(
                doc! {"_id": scope_key(&self.scope), "operation": operation, "pending": true},
                doc! {"$set": {"stage": stage, "pending": stage != "complete"}},
            )
            .session(&mut *session)
            .await
            .map_err(errors::failed("advance reset stage"))?;
        if updated.matched_count != 1 {
            return Err(StorageError::conflict(
                "reset operation is no longer pending",
            ));
        }
        if stage == "complete" {
            let completed = self
                .shared
                .raw(ADMISSIONS)
                .find_one(doc! {"_id": scope_key(&self.scope)})
                .session(&mut *session)
                .await
                .map_err(errors::failed("read completed reset authority"))?
                .ok_or_else(|| StorageError::unavailable("reset authority is missing"))?;
            self.archive_complete(&completed, session).await?;
        }
        Ok(())
    }

    async fn records(&self, operation: &str) -> Result<()> {
        let names = self
            .shared
            .db
            .list_collection_names()
            .await
            .map_err(errors::failed("enumerate reset collections"))?;
        in_write!(&self.shared, None, session => {
            let receipt = self.pending(operation, &mut *session).await?;
            if receipt.stage != PurgeStage::Records { return Ok(()); }
            for name in &names {
                if owns_records(&self.shared.prefix, name) {
                    self.shared.db.collection::<Document>(name).delete_many(doc! {"_scope": self.scope.as_str()})
                        .session(&mut *session).await.map_err(errors::failed("erase scoped records"))?;
                }
            }
            self.advance(operation, "blob_manifest", session).await
        })
    }

    /// Keep one durable manifest record per file, avoiding BSON document limits.
    async fn plan_blobs(&self, operation: &str) -> Result<()> {
        let mut files = self
            .shared
            .raw(&format!("{BLOBS}.files"))
            .find(doc! {"metadata._scope": self.scope.as_str()})
            .await
            .map_err(errors::failed("enumerate reset blobs"))?;
        let manifest = self.shared.raw(MANIFEST);
        while let Some(file) = files
            .try_next()
            .await
            .map_err(errors::failed("enumerate reset blobs"))?
        {
            let id = file
                .get("_id")
                .ok_or_else(|| StorageError::serialization("blob has no identity"))?;
            manifest.update_one(
                doc! {"_id": {"s": self.scope.as_str(), "o": operation, "f": id}},
                doc! {"$setOnInsert": {"scope": self.scope.as_str(), "operation": operation, "file_id": id}},
            ).upsert(true).await.map_err(errors::failed("persist reset blob manifest"))?;
        }
        in_write!(&self.shared, None, session => {
            let receipt = self.pending(operation, &mut *session).await?;
            if receipt.stage == PurgeStage::BlobManifest {
                self.advance(operation, "blobs", session).await?;
            }
            Ok(())
        })
    }

    /// Chunks precede file deletion: on any failure the manifest retains ownership.
    async fn erase_blobs(&self, operation: &str) -> Result<()> {
        let manifest = self.shared.raw(MANIFEST);
        let mut entries = manifest
            .find(doc! {"scope": self.scope.as_str(), "operation": operation})
            .await
            .map_err(errors::failed("read reset blob manifest"))?;
        while let Some(entry) = entries
            .try_next()
            .await
            .map_err(errors::failed("read reset blob manifest"))?
        {
            let id = entry
                .get("file_id")
                .ok_or_else(|| StorageError::serialization("manifest has no blob identity"))?;
            self.shared
                .raw(&format!("{BLOBS}.chunks"))
                .delete_many(doc! {"files_id": id})
                .await
                .map_err(errors::failed("erase reset blob chunks"))?;
            #[cfg(test)]
            if self.fail_after_chunks {
                return Err(StorageError::backend(
                    "injected failure after durable chunk erasure",
                ));
            }

            self.shared
                .raw(&format!("{BLOBS}.files"))
                .delete_one(doc! {"_id": id, "metadata._scope": self.scope.as_str()})
                .await
                .map_err(errors::failed("erase reset blob file"))?;
            manifest
                .delete_one(doc! {"_id": entry.get("_id").cloned().unwrap_or_default()})
                .await
                .map_err(errors::failed("finish reset blob manifest entry"))?;
        }
        if manifest
            .count_documents(doc! {"scope": self.scope.as_str(), "operation": operation})
            .await
            .map_err(errors::failed("verify reset blob manifest"))?
            != 0
        {
            return Err(StorageError::unavailable(
                "reset blob manifest is not empty; resume",
            ));
        }
        if self
            .shared
            .raw(&format!("{BLOBS}.files"))
            .count_documents(doc! {"metadata._scope": self.scope.as_str()})
            .await
            .map_err(errors::failed("verify reset blobs"))?
            != 0
        {
            in_write!(&self.shared, None, session => {
                self.pending(operation, &mut *session).await?;
                self.advance(operation, "blob_manifest", session).await
            })?;
            return Err(StorageError::unavailable(
                "blob writers were not drained; reset remains pending and must replan",
            ));
        }
        in_write!(&self.shared, None, session => {
            let receipt = self.pending(operation, &mut *session).await?;
            if receipt.stage != PurgeStage::Blobs {
                return Err(StorageError::conflict("reset blob stage changed"));
            }
            self.advance(operation, "complete", session).await
        })
    }
}

#[async_trait]
impl ScopePurge for MongoPurge {
    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "public transactions are required for reset admission",
            ));
        }
        if request.mode == PurgeMode::Atomic {
            return Err(StorageError::unsupported(
                Capability::AtomicScopePurge,
                "GridFS cannot join an atomic all-port purge",
            ));
        }
        if self.fenced {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "GridFS purge cannot hold a native fence across all stages",
            ));
        }
        let receipt = self.begin(request).await?;
        if receipt.status == PurgeStatus::Complete {
            return Ok(receipt);
        }
        let operation = request.operation_id.as_str();
        if receipt.stage == PurgeStage::Records {
            self.records(operation).await?;
        }
        let receipt = self
            .status(operation)
            .await?
            .ok_or_else(|| StorageError::unavailable("reset authority is missing"))?;
        if receipt.stage == PurgeStage::BlobManifest {
            self.plan_blobs(operation).await?;
        }
        if receipt.status == PurgeStatus::Complete {
            return Ok(receipt);
        }
        self.erase_blobs(operation).await?;
        self.status(operation)
            .await?
            .ok_or_else(|| StorageError::unavailable("reset authority is missing"))
    }
    async fn status(&self, operation: &str) -> Result<Option<PurgeReceipt>> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "public reset capabilities are disabled",
            ));
        }
        if self.fenced {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "fenced GridFS reset is unsupported",
            ));
        }
        if let Some(record) = self
            .shared
            .raw(RECEIPTS)
            .find_one(doc! {"_id": {"s": self.scope.as_str(), "o": operation}})
            .await
            .map_err(errors::failed("read completed reset receipt"))?
        {
            return decode(&record).map(Some);
        }
        self.shared
            .raw(ADMISSIONS)
            .find_one(doc! {"_id": scope_key(&self.scope), "operation": operation})
            .await
            .map_err(errors::failed("read reset receipt"))?
            .as_ref()
            .map(decode)
            .transpose()
    }
}

#[cfg(test)]
#[path = "purge_tests.rs"]
mod tests;
