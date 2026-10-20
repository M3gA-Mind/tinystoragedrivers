use std::cmp::Ordering;

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::ReplaceOptions;
use mongodb::{Collection, Database};
use serde_json::{Map, Number, Value};
use tinystoragedrivers_api::{DocumentStore, async_trait};
use tinystoragedrivers_types::{
    Capabilities, Capability, CollectionSpec, Cursor, CursorPosition, Filter, Meta, Page,
    Precondition, Query, Result, Scope, SearchHit, Sort, StorageError, Version, Versioned,
    WriteOptions, sort_documents_with_meta, validate_collection, validate_doc, validate_id,
};

use tinystoragedrivers_connect::MongoConnection;

const SCOPE: &str = "_scope";
const KEY: &str = "_key";
const VERSION: &str = "_v";
const BODY: &str = "d";
const SCHEMA: &str = "_schema";
const CREATED: &str = "_c";
const UPDATED: &str = "_u";
const EXPIRES: &str = "_x";
const DELETED: &str = "_del";

#[derive(Debug, Clone)]
pub struct MongoDocuments {
    database: Database,
    scope: Scope,
    clock: crate::Clock,
}

impl MongoDocuments {
    /// Bind the v2 document port to a shared Mongo connection and opaque scope.
    pub fn new(connection: MongoConnection, scope: Scope) -> Self {
        Self {
            database: connection.database(),
            scope,
            clock: connection.clock(),
        }
    }

    fn collection(&self, collection: &str) -> Collection<Document> {
        self.database.collection(collection)
    }

    fn scoped_id(&self, id: &str) -> Document {
        doc! {"s": self.scope.as_str(), "k": id}
    }

    async fn rows(&self, collection: &str) -> Result<Vec<Versioned<Value>>> {
        let mut cursor = self
            .collection(collection)
            .find(doc! {SCOPE: self.scope.as_str()})
            .await
            .map_err(|error| mongo_error("read documents", error))?;
        let mut rows = Vec::new();
        while let Some(record) = cursor
            .try_next()
            .await
            .map_err(|error| mongo_error("read documents", error))?
        {
            if let Some(row) = decode(&record)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }
}

#[async_trait]
impl DocumentStore for MongoDocuments {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Documents)
            .with(Capability::Ttl)
            .with(Capability::RecordMetadata)
            .with(Capability::CollectionEnumeration)
            .with(Capability::CollectionValidation)
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let value = serde_json::to_value(spec)
            .map_err(|error| StorageError::serialization(error.to_string()))?;
        let record = doc! {"_id": &spec.name, "spec": json_to_bson(&value)?};
        self.database
            .collection::<Document>("_tsd_meta")
            .replace_one(doc! {"_id": &spec.name}, record)
            .upsert(true)
            .await
            .map_err(|error| mongo_error("store a collection declaration", error))?;
        Ok(())
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        required.validate()?;
        let found = self
            .database
            .collection::<Document>("_tsd_meta")
            .find_one(doc! {"_id": &required.name})
            .await
            .map_err(|error| mongo_error("read a collection declaration", error))?
            .ok_or_else(|| StorageError::not_found("collection has no persisted declaration"))?;
        let value = found
            .get("spec")
            .ok_or_else(|| StorageError::serialization("malformed collection declaration"))?;
        let stored: CollectionSpec = serde_json::from_value(bson_to_json(value)?)
            .map_err(|error| StorageError::serialization(error.to_string()))?;
        if stored.merge(required)? != stored {
            return Err(StorageError::invalid_input(
                "stored declaration lacks required features",
            ));
        }
        Ok(())
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        let Some(record) = self
            .collection(collection)
            .find_one(doc! {"_id": self.scoped_id(id), DELETED: {"$ne": true}})
            .await
            .map_err(|error| mongo_error("read a document", error))?
        else {
            return Ok(None);
        };
        let row = decode(&record)?;
        Ok(row.filter(|found| !expired(found, (self.clock)())))
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.put_with(collection, id, value, precondition, WriteOptions::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&value)?;
        let table = self.collection(collection);
        let filter = doc! {"_id": self.scoped_id(id)};
        let old = table
            .find_one(filter.clone())
            .await
            .map_err(|error| mongo_error("read before writing a document", error))?;
        let current = old
            .as_ref()
            .and_then(|record| decode(record).ok().flatten())
            .filter(|row| !expired(row, (self.clock)()));
        check(precondition, current.as_ref().map(|row| row.version))?;
        let version = match old
            .as_ref()
            .and_then(|row| row.get_i64(VERSION).ok())
            .and_then(|v| u64::try_from(v).ok())
        {
            Some(v) => Version(v)
                .next()
                .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
            None => Version::FIRST,
        };
        let now = i64::try_from((self.clock)()).unwrap_or(i64::MAX);
        let meta = Meta {
            schema: options.schema,
            created_ms: options
                .created_ms
                .unwrap_or_else(|| current.as_ref().map_or(now, |row| row.meta.created_ms)),
            updated_ms: current
                .as_ref()
                .map_or(now, |row| now.max(row.meta.updated_ms)),
            expires_ms: options.expires_ms,
        };
        let record = encode(&self.scope, id, version, &value, &meta)?;
        let expected = old.as_ref().and_then(|row| row.get_i64(VERSION).ok());
        let mut cas = filter;
        if let Some(expected) = expected {
            cas.insert(VERSION, expected);
        }
        let result = table
            .replace_one(cas, record)
            .upsert(old.is_none())
            .await
            .map_err(|error| mongo_error("write a document", error))?;
        if result.matched_count == 0 && result.upserted_id.is_none() {
            return Err(StorageError::conflict("document changed during write"));
        }
        Ok(version)
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        let table = self.collection(collection);
        let filter = doc! {"_id": self.scoped_id(id)};
        let Some(old) = table
            .find_one(filter.clone())
            .await
            .map_err(|error| mongo_error("read before deleting a document", error))?
        else {
            check(precondition, None)?;
            return Ok(false);
        };
        let live = decode(&old)?.filter(|row| !expired(row, (self.clock)()));
        check(precondition, live.as_ref().map(|row| row.version))?;
        let Some(version) = old.get_i64(VERSION).ok() else {
            return Err(StorageError::serialization(
                "stored document has no version",
            ));
        };
        table
            .update_one(
                doc! {"_id": self.scoped_id(id), VERSION: version},
                doc! {"$set": {BODY: {}, DELETED: true}},
            )
            .await
            .map_err(|error| mongo_error("delete a document", error))?;
        Ok(live.is_some())
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        validate_collection(collection)?;
        query.validate()?;
        let mut rows = self.rows(collection).await?;
        let now = (self.clock)();
        rows.retain(|row| {
            !expired(row, now) && query.filter.matches_with(&row.id, &row.doc, &row.meta)
        });
        sort_documents_with_meta(&mut rows, &query.sort, |row| (&row.id, &row.doc, &row.meta));
        let fingerprint = fingerprint(collection, &self.scope, query);
        let after = if let Some(cursor) = &query.cursor {
            let position = cursor.decode_keyset("mongo", fingerprint)?;
            position.validate(&query.sort)?;
            Some(position)
        } else {
            None
        };
        if let Some(after) = after {
            rows.retain(|row| {
                CursorPosition::for_record(&row.id, &row.doc, &row.meta, &query.sort)
                    .compare(&after, &query.sort)
                    == Ordering::Greater
            });
        }
        let limit = query.limit.unwrap_or(rows.len());
        let more = rows.len() > limit;
        rows.truncate(limit);
        let next = if more {
            rows.last()
                .map(|row| {
                    Cursor::keyset(
                        "mongo",
                        fingerprint,
                        &CursorPosition::for_record(&row.id, &row.doc, &row.meta, &query.sort),
                    )
                })
                .transpose()?
        } else {
            None
        };
        Ok(Page { items: rows, next })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        Ok(self
            .rows(collection)
            .await?
            .into_iter()
            .filter(|row| {
                !expired(row, (self.clock)()) && filter.matches_with(&row.id, &row.doc, &row.meta)
            })
            .count() as u64)
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let rows = self.rows(collection).await?;
        let mut count = 0;
        for row in rows.into_iter().filter(|row| {
            !expired(row, (self.clock)()) && filter.matches_with(&row.id, &row.doc, &row.meta)
        }) {
            count += u64::from(
                self.delete(collection, &row.id, Precondition::Version(row.version))
                    .await?,
            );
        }
        Ok(count)
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        validate_doc(patch)?;
        for _ in 0..64 {
            let query = Query {
                filter: filter.clone(),
                sort: sort.to_vec(),
                limit: Some(1),
                cursor: None,
            };
            let Some(mut first) = self
                .query(collection, &query)
                .await?
                .items
                .into_iter()
                .next()
            else {
                return Ok(None);
            };
            tinystoragedrivers_types::value::merge_patch(&mut first.doc, patch);
            let options = WriteOptions {
                schema: first.meta.schema,
                expires_ms: first.meta.expires_ms,
                created_ms: Some(first.meta.created_ms),
            };
            match self
                .put_with(
                    collection,
                    &first.id,
                    first.doc.clone(),
                    Precondition::Version(first.version),
                    options,
                )
                .await
            {
                Ok(version) => {
                    first.version = version;
                    return Ok(Some(first));
                }
                Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StorageError::unavailable(
            "MongoDB claim retry limit reached",
        ))
    }

    async fn search(&self, _: &str, _: &str, _: usize) -> Result<Vec<SearchHit>> {
        Err(StorageError::unsupported(
            Capability::FullText,
            "MongoDB full-text search is unsupported by this adapter",
        ))
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        self.collection(collection)
            .delete_many(doc! {SCOPE: self.scope.as_str()})
            .await
            .map_err(|error| mongo_error("drop scoped documents", error))?;
        Ok(())
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let mut names = self
            .database
            .list_collection_names()
            .await
            .map_err(|error| mongo_error("list collections", error))?;
        names.retain(|name| name.starts_with(prefix) && !name.starts_with("_tsd_"));
        names.sort();
        Ok(names)
    }
}

fn expired(row: &Versioned<Value>, now: u64) -> bool {
    row.meta
        .expires_ms
        .is_some_and(|expires| expires <= i64::try_from(now).unwrap_or(i64::MAX))
}

fn check(precondition: Precondition, found: Option<Version>) -> Result<()> {
    match (precondition, found) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(_)) => Err(StorageError::conflict("document already exists")),
        (Precondition::Version(expected), Some(found)) if expected == found => Ok(()),
        (Precondition::Version(_), _) => Err(StorageError::conflict("document version changed")),
    }
}

fn encode(
    scope: &Scope,
    id: &str,
    version: Version,
    body: &Value,
    meta: &Meta,
) -> Result<Document> {
    let body = json_to_bson(body)?;
    let Bson::Document(body) = body else {
        return Err(StorageError::invalid_input(
            "document body must be an object",
        ));
    };
    let mut record = doc! {"_id": {"s": scope.as_str(), "k": id}, SCOPE: scope.as_str(), KEY: id, VERSION: i64::try_from(version.0).map_err(|_| StorageError::backend("document version space is exhausted"))?, BODY: body, SCHEMA: i64::from(meta.schema), CREATED: meta.created_ms, UPDATED: meta.updated_ms};
    if let Some(expires) = meta.expires_ms {
        record.insert(EXPIRES, expires);
    }
    Ok(record)
}

fn decode(record: &Document) -> Result<Option<Versioned<Value>>> {
    if record.get_bool(DELETED).unwrap_or(false) {
        return Ok(None);
    }
    let id = record
        .get_str(KEY)
        .map_err(|_| StorageError::serialization("stored document has no key"))?
        .to_owned();
    let version = record
        .get_i64(VERSION)
        .ok()
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| StorageError::serialization("stored document has invalid version"))?;
    let body = record
        .get_document(BODY)
        .map_err(|_| StorageError::serialization("stored document has no body"))?;
    let meta = Meta {
        schema: record
            .get_i64(SCHEMA)
            .ok()
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0),
        created_ms: record.get_i64(CREATED).unwrap_or(0),
        updated_ms: record.get_i64(UPDATED).unwrap_or(0),
        expires_ms: record.get_i64(EXPIRES).ok(),
    };
    Ok(Some(Versioned {
        id,
        version: Version(version),
        doc: bson_to_json(&Bson::Document(body.clone()))?,
        meta,
    }))
}

pub(super) fn json_to_bson(value: &Value) -> Result<Bson> {
    Ok(match value {
        Value::Null => Bson::Null,
        Value::Bool(value) => Bson::Boolean(*value),
        Value::String(value) => Bson::String(value.clone()),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Bson::Int64(value)
            } else if let Some(value) = number.as_u64() {
                Bson::Int64(i64::try_from(value).map_err(|_| {
                    StorageError::serialization("MongoDB cannot store integers above i64::MAX")
                })?)
            } else {
                Bson::Double(number.as_f64().unwrap_or_default())
            }
        }
        Value::Array(values) => {
            Bson::Array(values.iter().map(json_to_bson).collect::<Result<_>>()?)
        }
        Value::Object(values) => Bson::Document(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_to_bson(value)?)))
                .collect::<Result<_>>()?,
        ),
    })
}

pub(super) fn bson_to_json(value: &Bson) -> Result<Value> {
    Ok(match value {
        Bson::Null => Value::Null,
        Bson::Boolean(value) => Value::Bool(*value),
        Bson::String(value) => Value::String(value.clone()),
        Bson::Int32(value) => Value::from(*value),
        Bson::Int64(value) => Value::from(*value),
        Bson::Double(value) => Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| StorageError::serialization("MongoDB stored a non-finite number"))?,
        Bson::Array(values) => {
            Value::Array(values.iter().map(bson_to_json).collect::<Result<_>>()?)
        }
        Bson::Document(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), bson_to_json(value)?)))
                .collect::<Result<Map<_, _>>>()?,
        ),
        _ => {
            return Err(StorageError::serialization(
                "MongoDB stored a non-JSON value",
            ));
        }
    })
}

fn fingerprint(collection: &str, scope: &Scope, query: &Query) -> u64 {
    collection
        .as_bytes()
        .iter()
        .chain(scope.as_str().as_bytes())
        .chain(serde_json::to_vec(&query.filter).unwrap_or_default().iter())
        .fold(0xcbf29ce484222325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
}

fn mongo_error(operation: &'static str, error: mongodb::error::Error) -> StorageError {
    StorageError::unavailable(format!("MongoDB failed to {operation}")).with_source(error)
}
