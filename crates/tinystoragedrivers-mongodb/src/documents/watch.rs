//! Native scoped change streams with payload-free notifications.
//!
//! Only operation type and the opaque native token leave the server. Scope
//! filtering uses the native composite document key, including delete events.
use std::fmt;
use std::sync::Arc;

use futures_util::StreamExt;
use mongodb::bson::{Document, doc};
use mongodb::change_stream::{ChangeStream, event::ResumeToken};
use mongodb::options::ChangeStreamOptions;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{
    Capability, DocumentStore, DocumentWatch, Filter, Result, StorageError, WatchCursor,
    WatchNotice, WatchOptions, WatchReset, validate_collection,
};

use super::MongoDocuments;
use crate::errors;

/// Cursor contents never implement Debug or contain user document values.
#[derive(Serialize, Deserialize)]
struct CursorWire {
    binding: [u8; 32],
    token: ResumeToken,
}

fn binding(docs: &MongoDocuments, collection: &str, filter: &Filter) -> Result<[u8; 32]> {
    let guard = docs.fence.as_ref().map(|guard| {
        (
            guard.shared.db.name(),
            &guard.shared.prefix,
            guard.scope().as_str(),
            guard.collection(),
            guard.id(),
            guard.filter(),
        )
    });
    let encoded = serde_json::to_vec(&(
        docs.shared.watch_origin.to_hex(),
        docs.shared.db.name(),
        &docs.shared.prefix,
        docs.scope.as_str(),
        docs.generation,
        collection,
        filter,
        guard,
    ))?;
    Ok(Sha256::digest(encoded).into())
}

fn decode(cursor: &WatchCursor, expected: [u8; 32]) -> Result<ResumeToken> {
    cursor.validate()?;
    let decoded: CursorWire = serde_json::from_slice(cursor.as_bytes())
        .map_err(|_| StorageError::invalid_input("invalid native watch cursor"))?;
    if decoded.binding != expected {
        return Err(StorageError::invalid_input(
            "watch cursor belongs to another binding",
        ));
    }
    Ok(decoded.token)
}

fn encode(binding: [u8; 32], token: ResumeToken) -> Result<WatchCursor> {
    WatchCursor::new(serde_json::to_vec(&CursorWire { binding, token })?)
}

fn resume_lost(error: &mongodb::error::Error) -> bool {
    match error.kind.as_ref() {
        mongodb::error::ErrorKind::Command(command) => {
            matches!(command.code, 136 | 260 | 280 | 286)
        }
        _ => false,
    }
}

/// Include every scoped mutation, even when its previous value left a filter.
/// Destructive namespace events have no documentKey and invalidate all scopes.
fn pipeline(scope: &str) -> Vec<Document> {
    vec![
        doc! {"$match": {"$or": [
            {"documentKey._id.s": scope},
            {"operationType": {"$in": ["drop", "rename", "dropDatabase", "invalidate"]}}
        ]}},
        doc! {"$project": {"_id": 1, "operationType": 1}},
    ]
}

async fn admitted(docs: &MongoDocuments) -> Result<()> {
    crate::purge::check(&docs.shared, &docs.scope, docs.generation, None).await?;
    if docs.fence.is_some() {
        docs.atomic_batch_checked(vec![], vec![]).await?;
    }
    Ok(())
}

impl MongoDocuments {
    pub(super) async fn open_watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> Result<Box<dyn DocumentWatch>> {
        validate_collection(collection)?;
        filter.validate()?;
        options.validate()?;
        if !self.shared.physical_transactions {
            return Err(StorageError::unsupported(
                Capability::Watch,
                "native watch requires a replica set or sharded deployment",
            ));
        }
        let binding = binding(self, collection, filter)?;
        let resume_after = options
            .after
            .as_ref()
            .map(|cursor| decode(cursor, binding))
            .transpose()?;
        admitted(self).await?;
        if self.shared.existing {
            self.shared.spec(collection).await?;
        }
        let mut native_options = ChangeStreamOptions::default();
        native_options.resume_after = resume_after;
        native_options.max_await_time = Some(options.interval);
        native_options.batch_size = Some(1);
        let opened = self
            .shared
            .raw(collection)
            .watch()
            .pipeline(pipeline(self.scope.as_str()))
            .with_options(native_options)
            .await;
        let docs = MongoDocuments::new(
            Arc::clone(&self.shared),
            self.scope.clone(),
            self.fence.clone(),
        )
        .with_generation(self.generation);
        let (stream, pending) = match opened {
            Ok(stream) => (Some(stream.with_type::<Document>()), None),
            Err(error) if resume_lost(&error) => (
                None,
                Some(WatchNotice::ResetRequired {
                    reason: WatchReset::ResumeLost,
                    source: Some(errors::map(error, "resume document watch")),
                }),
            ),
            Err(error) => return Err(errors::map(error, "open document watch")),
        };
        admitted(&docs).await?;
        let cursor = stream
            .as_ref()
            .and_then(ChangeStream::resume_token)
            .map(|token| encode(binding, token))
            .transpose()?;
        Ok(Box::new(MongoWatch {
            docs,
            stream,
            delivery: PendingDelivery {
                pending: pending.map(Ok),
            },
            cursor,
            binding,
            interval: options.interval,
            terminal: false,
        }))
    }
}

struct MongoWatch {
    docs: MongoDocuments,
    stream: Option<ChangeStream<Document>>,
    delivery: PendingDelivery,
    cursor: Option<WatchCursor>,
    binding: [u8; 32],
    interval: std::time::Duration,
    terminal: bool,
}

impl fmt::Debug for MongoWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoWatch")
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

/// Keep a consumed native event across a canceled admission check.
#[derive(Default)]
struct PendingDelivery {
    pending: Option<Result<WatchNotice>>,
}

impl PendingDelivery {
    async fn take_after(
        &mut self,
        admitted: impl std::future::Future<Output = Result<()>>,
    ) -> Result<Option<WatchNotice>> {
        admitted.await?;
        self.pending.take().transpose()
    }
}

impl MongoWatch {
    async fn deliver(&mut self) -> Result<Option<WatchNotice>> {
        let notice = self.delivery.take_after(admitted(&self.docs)).await?;
        if matches!(notice, Some(WatchNotice::ResetRequired { .. })) {
            self.terminal = true;
            self.stream = None;
        }
        Ok(notice)
    }
}

#[tinystoragedrivers_core::async_trait]
impl DocumentWatch for MongoWatch {
    async fn next(&mut self) -> Result<Option<WatchNotice>> {
        if self.terminal {
            return Ok(None);
        }
        if self.delivery.pending.is_some() {
            return self.deliver().await;
        }
        admitted(&self.docs).await?;
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| StorageError::backend("missing native watch cursor"))?;
        // StreamExt retains a pending getMore inside ChangeStream when timeout
        // cancels its borrow. Never access resume_token during that getMore.
        let event = tokio::time::timeout(self.interval, stream.next()).await;
        let result = match event {
            Err(_) => Ok(WatchNotice::Invalidated {
                cursor: self.cursor.clone(),
            }),
            Ok(None) => Ok(WatchNotice::ResetRequired {
                reason: WatchReset::Invalidated,
                source: None,
            }),
            Ok(Some(Err(error))) if resume_lost(&error) => Ok(WatchNotice::ResetRequired {
                reason: WatchReset::ResumeLost,
                source: Some(errors::map(error, "resume document watch")),
            }),
            Ok(Some(Err(error))) => Err(errors::map(error, "read document watch")),
            Ok(Some(Ok(event))) => {
                if matches!(
                    event.get_str("operationType"),
                    Ok("drop" | "rename" | "dropDatabase" | "invalidate")
                ) {
                    Ok(WatchNotice::ResetRequired {
                        reason: WatchReset::Invalidated,
                        source: None,
                    })
                } else {
                    self.cursor = self
                        .stream
                        .as_ref()
                        .and_then(ChangeStream::resume_token)
                        .map(|token| encode(self.binding, token))
                        .transpose()?;
                    Ok(WatchNotice::Invalidated {
                        cursor: self.cursor.clone(),
                    })
                }
            }
        };
        self.delivery.pending = Some(result);
        self.deliver().await
    }
}

#[cfg(test)]
#[path = "watch_tests.rs"]
mod tests;
