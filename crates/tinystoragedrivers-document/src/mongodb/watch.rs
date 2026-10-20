//! Scope-bound MongoDB change-stream invalidation watches.

use std::fmt;

use futures_util::StreamExt;
use mongodb::bson::{Document, doc};
use mongodb::change_stream::{ChangeStream, event::ResumeToken};
use mongodb::options::ChangeStreamOptions;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tinystoragedrivers_api::{DocumentWatch, async_trait};
use tinystoragedrivers_types::{
    Capability, Filter, Result, StorageError, WatchCursor, WatchNotice, WatchOptions, WatchReset,
    validate_collection,
};

use super::documents::{MongoDocuments, mongo_error};

#[derive(Serialize, Deserialize)]
struct CursorWire {
    binding: [u8; 32],
    token: ResumeToken,
}

fn binding(docs: &MongoDocuments, collection: &str, filter: &Filter) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(&(
        docs.origin.to_hex(),
        docs.database.name(),
        docs.scope.as_str(),
        collection,
        filter,
    ))?;
    Ok(Sha256::digest(bytes).into())
}

fn decode(cursor: &WatchCursor, expected: [u8; 32]) -> Result<ResumeToken> {
    cursor.validate()?;
    let wire: CursorWire = serde_json::from_slice(cursor.as_bytes())
        .map_err(|_| StorageError::invalid_input("invalid native watch cursor"))?;
    if wire.binding != expected {
        return Err(StorageError::invalid_input(
            "watch cursor belongs to another database, scope, collection, or filter",
        ));
    }
    Ok(wire.token)
}

fn encode(binding: [u8; 32], token: ResumeToken) -> Result<WatchCursor> {
    WatchCursor::new(serde_json::to_vec(&CursorWire { binding, token })?)
}

fn resume_lost(error: &mongodb::error::Error) -> bool {
    matches!(error.kind.as_ref(), mongodb::error::ErrorKind::Command(command) if matches!(command.code, 136 | 260 | 280 | 286))
}

fn pipeline(scope: &str) -> Vec<Document> {
    vec![
        doc! {"$match": {"$or": [
            {"documentKey._id.s": scope},
            {"operationType": {"$in": ["drop", "rename", "dropDatabase", "invalidate"]}}
        ]}},
        doc! {"$project": {"_id": 1, "operationType": 1}},
    ]
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
        if !self.transactions {
            return Err(StorageError::unsupported(
                Capability::Watch,
                "native document watch requires a replica set or sharded deployment",
            ));
        }
        let binding = binding(self, collection, filter)?;
        let resume_after = options
            .after
            .as_ref()
            .map(|cursor| decode(cursor, binding))
            .transpose()?;
        let mut native_options = ChangeStreamOptions::default();
        native_options.resume_after = resume_after;
        native_options.max_await_time = Some(options.interval);
        native_options.batch_size = Some(1);
        let opened = self
            .database
            .collection::<Document>(collection)
            .watch()
            .pipeline(pipeline(self.scope.as_str()))
            .with_options(native_options)
            .await;
        let (stream, pending) = match opened {
            Ok(stream) => (Some(stream.with_type::<Document>()), None),
            Err(error) if resume_lost(&error) => (
                None,
                Some(WatchNotice::ResetRequired {
                    reason: WatchReset::ResumeLost,
                    source: Some(mongo_error("resume a document watch", error)),
                }),
            ),
            Err(error) => return Err(mongo_error("open a document watch", error)),
        };
        let cursor = stream
            .as_ref()
            .and_then(ChangeStream::resume_token)
            .map(|token| encode(binding, token))
            .transpose()?;
        Ok(Box::new(MongoWatch {
            stream,
            pending,
            cursor,
            binding,
            interval: options.interval,
            terminal: false,
        }))
    }
}

struct MongoWatch {
    stream: Option<ChangeStream<Document>>,
    pending: Option<WatchNotice>,
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

#[async_trait]
impl DocumentWatch for MongoWatch {
    async fn next(&mut self) -> Result<Option<WatchNotice>> {
        if self.terminal {
            return Ok(None);
        }
        if let Some(notice) = self.pending.take() {
            if matches!(notice, WatchNotice::ResetRequired { .. }) {
                self.terminal = true;
                self.stream = None;
            }
            return Ok(Some(notice));
        }
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| StorageError::backend("missing native MongoDB watch cursor"))?;
        let event = tokio::time::timeout(self.interval, stream.next()).await;
        let notice = match event {
            Err(_) => WatchNotice::Invalidated {
                cursor: self.cursor.clone(),
            },
            Ok(None) => WatchNotice::ResetRequired {
                reason: WatchReset::Invalidated,
                source: None,
            },
            Ok(Some(Err(error))) if resume_lost(&error) => WatchNotice::ResetRequired {
                reason: WatchReset::ResumeLost,
                source: Some(mongo_error("resume a document watch", error)),
            },
            Ok(Some(Err(error))) => return Err(mongo_error("read a document watch", error)),
            Ok(Some(Ok(event))) => {
                if matches!(
                    event.get_str("operationType"),
                    Ok("drop" | "rename" | "dropDatabase" | "invalidate")
                ) {
                    WatchNotice::ResetRequired {
                        reason: WatchReset::Invalidated,
                        source: None,
                    }
                } else {
                    self.cursor = self
                        .stream
                        .as_ref()
                        .and_then(ChangeStream::resume_token)
                        .map(|token| encode(self.binding, token))
                        .transpose()?;
                    WatchNotice::Invalidated {
                        cursor: self.cursor.clone(),
                    }
                }
            }
        };
        if matches!(notice, WatchNotice::ResetRequired { .. }) {
            self.terminal = true;
            self.stream = None;
        }
        Ok(Some(notice))
    }
}
