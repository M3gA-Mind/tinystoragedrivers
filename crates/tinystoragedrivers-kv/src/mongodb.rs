//! MongoDB KV adapter using one native collection and single-document CAS.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::bson::{Bson, doc};
use mongodb::{Client, Collection, IndexModel};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_connect::MongoConnection;
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Meta, Page, Precondition, Result, Scope, StorageError,
    Version, Versioned,
};

use crate::common::{
    check_precondition, cursor_key, duration_millis, make_cursor, next_version, validate_key,
    validate_limit, validate_prefix, version_to_i64,
};

const COLLECTION: &str = "_tsd_kv";
const WRITE_ATTEMPTS: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    #[serde(rename = "_id")]
    id: String,
    scope: String,
    key: String,
    version: i64,
    #[serde(default)]
    expires_ms: Option<i64>,
    deleted: bool,
    value_json: String,
}

impl Record {
    fn live(&self, now: i64) -> bool {
        !self.deleted && self.expires_ms.is_none_or(|expiry| expiry > now)
    }

    fn version(&self) -> Result<Version> {
        u64::try_from(self.version)
            .map(Version)
            .map_err(|_| StorageError::backend("mongodb stored an invalid KV version"))
    }

    fn value(&self) -> Result<Value> {
        Ok(serde_json::from_str(&self.value_json)?)
    }
}

/// MongoDB key/value engine sharing the configured database connection.
#[derive(Clone)]
pub struct MongoKv {
    client: Client,
    collection: Collection<Record>,
    scope: String,
    clock: tinystoragedrivers_connect::Clock,
    condition_guard: Option<tinystoragedrivers_document::mongodb::MongoDocuments>,
}

/// Factory for creating MongoDB KV handles bound to individual scopes.
///
/// The KV collection has a database-wide unique index on `(scope, key)`, so
/// the index is prepared once before scoped handles are created.
#[derive(Clone)]
pub struct MongoKvFactory {
    client: Client,
    collection: Collection<Record>,
    clock: tinystoragedrivers_connect::Clock,
    connection: MongoConnection,
}

impl std::fmt::Debug for MongoKvFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoKvFactory").finish_non_exhaustive()
    }
}

impl MongoKvFactory {
    /// Prepare the native KV collection and its cross-scope unique index.
    ///
    /// # Errors
    /// Returns an error if MongoDB cannot create the KV index.
    pub async fn new(connection: MongoConnection) -> Result<Self> {
        let collection = connection.database().collection::<Record>(COLLECTION);
        collection
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"scope": 1, "key": 1})
                    .options(
                        mongodb::options::IndexOptions::builder()
                            .unique(true)
                            .name("scope_key".to_owned())
                            .build(),
                    )
                    .build(),
            )
            .await
            .map_err(|error| mongo_error("create the KV index", error))?;
        Ok(Self {
            client: connection.client(),
            collection,
            clock: connection.clock(),
            connection,
        })
    }

    /// Bind the prepared collection to a scope without further I/O.
    #[must_use]
    pub fn for_scope(&self, scope: &Scope) -> MongoKv {
        MongoKv {
            client: self.client.clone(),
            collection: self.collection.clone(),
            scope: scope.as_str().to_owned(),
            clock: Arc::clone(&self.clock),
            condition_guard: None,
        }
    }

    /// Bind a scope's KV handle to a host condition checked in the same
    /// transaction as each KV mutation.
    ///
    /// # Errors
    /// Returns `Unsupported(ConditionalWrites)` when the deployment does not
    /// support transactions.
    pub fn conditioned_for_scope(
        &self,
        scope: &Scope,
        condition: tinystoragedrivers_types::WriteCondition,
    ) -> Result<MongoKv> {
        let guard = tinystoragedrivers_document::mongodb::MongoDocuments::conditioned(
            self.connection.clone(),
            scope.clone(),
            condition,
        )?;
        let mut kv = self.for_scope(scope);
        kv.condition_guard = Some(guard);
        Ok(kv)
    }
}

impl std::fmt::Debug for MongoKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoKv")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl MongoKv {
    /// Create and index the native KV collection on the shared database.
    ///
    /// # Errors
    /// Returns an error if MongoDB cannot create the KV index.
    pub async fn new(connection: MongoConnection, scope: Scope) -> Result<Self> {
        Ok(MongoKvFactory::new(connection).await?.for_scope(&scope))
    }

    fn now_ms(&self) -> i64 {
        i64::try_from((self.clock)()).unwrap_or(i64::MAX)
    }

    async fn read(&self, key: &str) -> Result<Option<Record>> {
        self.collection
            .find_one(doc! {"_id": record_id(&self.scope, key)})
            .await
            .map_err(|error| mongo_error("read a KV value", error))
    }

    async fn write(
        &self,
        key: &str,
        value: Value,
        expected: Precondition,
        expires_ms: Option<i64>,
    ) -> Result<Version> {
        if self.condition_guard.is_some() {
            return self
                .write_conditioned(key, value, expected, expires_ms)
                .await;
        }
        validate_key(key)?;
        let now = self.now_ms();
        let value_json = serde_json::to_string(&value)?;
        for _ in 0..WRITE_ATTEMPTS {
            let current = self.read(key).await?;
            let live = current.as_ref().filter(|record| record.live(now));
            check_precondition(expected, live.map(Record::version).transpose()?)?;
            let version = version_to_i64(next_version(
                current.as_ref().map(Record::version).transpose()?,
            )?)?;
            let next = Record {
                id: record_id(&self.scope, key),
                scope: self.scope.clone(),
                key: key.to_owned(),
                version,
                expires_ms,
                deleted: false,
                value_json: value_json.clone(),
            };
            let outcome = match current {
                None => self.collection.insert_one(next).await.map(|_| true),
                Some(old) => self
                    .collection
                    .replace_one(doc! {"_id": &old.id, "version": old.version}, next)
                    .await
                    .map(|result| result.matched_count == 1),
            };
            match outcome {
                Ok(true) => {
                    return u64::try_from(version)
                        .map(Version)
                        .map_err(|_| StorageError::backend("key version space is exhausted"));
                }
                Ok(false) => {}
                Err(error) if duplicate_key(&error) => {}
                Err(error) => return Err(mongo_error("write a KV value", error)),
            }
        }
        Err(StorageError::unavailable(
            "KV value kept changing concurrently; retry",
        ))
    }

    async fn write_conditioned(
        &self,
        key: &str,
        value: Value,
        expected: Precondition,
        expires_ms: Option<i64>,
    ) -> Result<Version> {
        validate_key(key)?;
        let guard = self
            .condition_guard
            .as_ref()
            .ok_or_else(|| StorageError::backend("conditioned KV write lost its guard handle"))?;
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|error| mongo_error("start a conditioned KV write", error))?;
        session
            .start_transaction()
            .await
            .map_err(|error| mongo_error("start a conditioned KV write", error))?;
        let result = async {
            guard.check_condition(&mut session).await?;
            let now = self.now_ms();
            let filter = doc! {"_id": record_id(&self.scope, key)};
            let current = self
                .collection
                .find_one(filter.clone())
                .session(&mut session)
                .await
                .map_err(|error| mongo_error("read a conditioned KV value", error))?;
            let live = current.as_ref().filter(|record| record.live(now));
            check_precondition(expected, live.map(Record::version).transpose()?)?;
            let version = version_to_i64(next_version(
                current.as_ref().map(Record::version).transpose()?,
            )?)?;
            let next = Record {
                id: record_id(&self.scope, key),
                scope: self.scope.clone(),
                key: key.to_owned(),
                version,
                expires_ms,
                deleted: false,
                value_json: serde_json::to_string(&value)?,
            };
            match current {
                None => {
                    self.collection
                        .insert_one(next)
                        .session(&mut session)
                        .await
                        .map_err(transaction_error)?;
                }
                Some(old) => {
                    let replaced = self
                        .collection
                        .replace_one(doc! {"_id": &old.id, "version": old.version}, next)
                        .session(&mut session)
                        .await
                        .map_err(transaction_error)?;
                    if replaced.matched_count != 1 {
                        return Err(StorageError::conflict("conditioned KV value changed"));
                    }
                }
            }
            u64::try_from(version)
                .map(Version)
                .map_err(|_| StorageError::backend("key version space is exhausted"))
        }
        .await;
        match result {
            Ok(version) => {
                session
                    .commit_transaction()
                    .await
                    .map_err(transaction_error)?;
                Ok(version)
            }
            Err(error) => {
                let _ = session.abort_transaction().await;
                Err(error)
            }
        }
    }

    async fn delete_conditioned(&self, key: &str, expected: Precondition) -> Result<bool> {
        validate_key(key)?;
        let guard = self
            .condition_guard
            .as_ref()
            .ok_or_else(|| StorageError::backend("conditioned KV delete lost its guard handle"))?;
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|error| mongo_error("start a conditioned KV delete", error))?;
        session
            .start_transaction()
            .await
            .map_err(|error| mongo_error("start a conditioned KV delete", error))?;
        let result = async {
            guard.check_condition(&mut session).await?;
            let now = self.now_ms();
            let filter = doc! {"_id": record_id(&self.scope, key)};
            let Some(current) = self
                .collection
                .find_one(filter.clone())
                .session(&mut session)
                .await
                .map_err(|error| mongo_error("read a conditioned KV value", error))?
            else {
                check_precondition(expected, None)?;
                return Ok(false);
            };
            let live = current.live(now);
            check_precondition(expected, live.then(|| current.version()).transpose()?)?;
            if !live {
                return Ok(false);
            }
            let next_version = version_to_i64(next_version(Some(current.version()?))?)?;
            let deleted = Record {
                version: next_version,
                expires_ms: None,
                deleted: true,
                value_json: String::new(),
                ..current.clone()
            };
            let replaced = self
                .collection
                .replace_one(
                    doc! {"_id": &current.id, "version": current.version},
                    deleted,
                )
                .session(&mut session)
                .await
                .map_err(transaction_error)?;
            if replaced.matched_count != 1 {
                return Err(StorageError::conflict("conditioned KV value changed"));
            }
            Ok(true)
        }
        .await;
        match result {
            Ok(removed) => {
                session
                    .commit_transaction()
                    .await
                    .map_err(transaction_error)?;
                Ok(removed)
            }
            Err(error) => {
                let _ = session.abort_transaction().await;
                Err(error)
            }
        }
    }
}

#[async_trait]
impl KvStore for MongoKv {
    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none()
            .with(Capability::Kv)
            .with(Capability::Ttl)
            .with(Capability::SharedAcrossProcesses)
            .with(Capability::CrossProcessCas);
        if self.condition_guard.is_some() {
            capabilities.with(Capability::ConditionalWrites)
        } else {
            capabilities
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        validate_key(key)?;
        let now = self.now_ms();
        self.read(key)
            .await?
            .filter(|record| record.live(now))
            .map(|record| {
                let version = record.version()?;
                let doc = record.value()?;
                Ok(Versioned {
                    id: record.key,
                    version,
                    doc,
                    meta: Meta::unknown(),
                })
            })
            .transpose()
    }

    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.write(key, value, Precondition::None, None).await
    }

    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.write(key, value, expected, None).await
    }

    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        if self.condition_guard.is_some() {
            return self.delete_conditioned(key, expected).await;
        }
        validate_key(key)?;
        let now = self.now_ms();
        for _ in 0..WRITE_ATTEMPTS {
            let Some(current) = self.read(key).await? else {
                check_precondition(expected, None)?;
                return Ok(false);
            };
            let live = current.live(now);
            check_precondition(expected, live.then(|| current.version()).transpose()?)?;
            if !live {
                return Ok(false);
            }
            let next_version = version_to_i64(next_version(Some(current.version()?))?)?;
            let result = self
                .collection
                .replace_one(
                    doc! {"_id": &current.id, "version": current.version},
                    Record {
                        id: current.id,
                        scope: current.scope,
                        key: current.key,
                        version: next_version,
                        expires_ms: None,
                        deleted: true,
                        value_json: "null".to_owned(),
                    },
                )
                .await
                .map_err(|error| mongo_error("delete a KV value", error))?;
            if result.matched_count == 1 {
                return Ok(true);
            }
        }
        Err(StorageError::unavailable(
            "KV value kept changing concurrently; retry",
        ))
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        let after = cursor
            .as_ref()
            .map(|cursor| cursor_key(cursor, prefix))
            .transpose()?;
        let now = self.now_ms();
        let mut filter = doc! {
            "scope": &self.scope,
            "key": {"$regex": format!("^{}", regex_literal(prefix))},
            "deleted": false,
            "$or": [
                {"expires_ms": {"$exists": false}},
                {"expires_ms": Bson::Null},
                {"expires_ms": {"$gt": now}},
            ],
        };
        if let Some(after) = after {
            filter.insert(
                "key",
                doc! {"$regex": format!("^{}", regex_literal(prefix)), "$gt": after},
            );
        }
        let mut cursor = self
            .collection
            .find(filter)
            .sort(doc! {"key": 1})
            .limit(i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX))
            .await
            .map_err(|error| mongo_error("list KV values", error))?;
        let mut items = Vec::new();
        while let Some(record) = cursor
            .try_next()
            .await
            .map_err(|error| mongo_error("read a KV page", error))?
        {
            let doc = record.value()?;
            items.push(Versioned {
                version: record.version()?,
                id: record.key,
                doc,
                meta: Meta::unknown(),
            });
        }
        let has_more = items.len() > limit;
        if has_more {
            items.pop();
        }
        let next = if has_more {
            items
                .last()
                .map(|item| make_cursor(&item.id, prefix))
                .transpose()?
        } else {
            None
        };
        Ok(Page { items, next })
    }

    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        let now = self.now_ms();
        let ttl = i64::try_from(duration_millis(ttl)).unwrap_or(i64::MAX);
        self.write(
            key,
            value,
            Precondition::None,
            Some(now.saturating_add(ttl)),
        )
        .await
    }
}

fn record_id(scope: &str, key: &str) -> String {
    format!("{}:{scope}{key}", scope.len())
}

fn regex_literal(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        if matches!(
            ch,
            '\\' | '.'
                | '^'
                | '$'
                | '|'
                | '?'
                | '*'
                | '+'
                | '('
                | ')'
                | '['
                | ']'
                | '{'
                | '}'
                | '-'
        ) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

fn duplicate_key(error: &mongodb::error::Error) -> bool {
    matches!(
        &*error.kind,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(write_error))
            if write_error.code == 11000
    )
}

fn mongo_error(
    action: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> StorageError {
    StorageError::unavailable(format!("MongoDB could not {action}")).with_source(error)
}

fn transaction_error(error: mongodb::error::Error) -> StorageError {
    use mongodb::error::{ErrorKind as MongoErrorKind, WriteFailure};

    let code = match error.kind.as_ref() {
        MongoErrorKind::Command(command) => Some(command.code),
        MongoErrorKind::Write(WriteFailure::WriteError(write)) => Some(write.code),
        _ => None,
    };
    if !error.contains_label("UnknownTransactionCommitResult")
        && (code == Some(112) || error.contains_label("TransientTransactionError"))
    {
        StorageError::conflict("conditioned MongoDB KV transaction conflicted").with_source(error)
    } else {
        mongo_error("commit a conditioned KV transaction", error)
    }
}

#[cfg(test)]
#[path = "mongodb_tests.rs"]
mod tests;
