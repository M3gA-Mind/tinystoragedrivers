//! The document rules applied to files, run under the database lock.
//!
//! These mirror the memory driver's `DbState` one for one: expired documents
//! stay on disk (so versions keep rising across expiry) but read as absent.
//!
//! A delete (and `delete_where`, and `drop_collection`) does not remove the
//! file: it replaces it with a tombstone, `{"id", "version"}` without a `doc`,
//! in one atomic rename. A recreated id therefore continues from its last
//! version, and a compare-and-swap prepared before the deletion fails, which
//! is what the memory driver's tombstone map provides.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, Filter, Meta, Precondition, Result, Scope, Sort, StorageError, Version,
    Versioned, WriteOptions, sort_documents_with_meta, validate_collection, validate_doc,
    validate_id, value,
};

use crate::encode::{decode, dir_components, file_stem};
use crate::fsio::{files_with_suffix, io_error, read_json, write_json};
use crate::storage::Db;

/// One document file: a live document, or a tombstone (no `doc`) that keeps
/// a deleted id's last version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Record {
    id: String,
    version: Version,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    doc: Option<Value>,
    /// The record metadata, kept in the envelope beside the body. A file
    /// written before it existed reads as [`Meta::unknown`].
    #[serde(default, skip_serializing_if = "is_unknown")]
    meta: Meta,
}

fn is_unknown(meta: &Meta) -> bool {
    *meta == Meta::unknown()
}

impl Record {
    /// The stored document, unless it is a tombstone or has expired.
    fn live(self, spec: &CollectionSpec, now_ms: u64) -> Option<StoredDoc> {
        let doc = self.doc?;
        let lapsed = expired(spec, &doc, now_ms)
            || self
                .meta
                .is_expired(i64::try_from(now_ms).unwrap_or(i64::MAX));
        (!lapsed).then_some(StoredDoc {
            id: self.id,
            version: self.version,
            doc,
            meta: self.meta,
        })
    }
}

/// A live document.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct StoredDoc {
    pub(super) id: String,
    pub(super) version: Version,
    pub(super) doc: Value,
    pub(super) meta: Meta,
}

impl StoredDoc {
    fn versioned(self) -> Versioned<Value> {
        Versioned {
            id: self.id,
            version: self.version,
            doc: self.doc,
            meta: self.meta,
        }
    }
}

/// The error for two names that hash to one file name. Unreachable outside a
/// deliberate 128-bit FNV collision, and refused rather than overwriting the
/// other document.
pub(super) fn collision() -> StorageError {
    StorageError::backend("file storage name hash collision; refusing to overwrite another entry")
}

/// One scope's documents in one database.
#[derive(Debug)]
pub(super) struct Docs<'a> {
    pub(super) db: &'a Db,
    pub(super) scope: &'a Scope,
    pub(super) now: u64,
}

impl Docs<'_> {
    fn collection_dir(&self, collection: &str) -> PathBuf {
        self.db
            .scope_dir(self.scope)
            .join("docs")
            .join(dir_components(collection))
    }

    fn doc_path(&self, collection: &str, id: &str) -> PathBuf {
        self.collection_dir(collection)
            .join(format!("{}.json", file_stem(id)))
    }

    fn spec_path(&self, collection: &str) -> PathBuf {
        self.db
            .specs_dir()
            .join(format!("{}.json", file_stem(collection)))
    }

    /// The collection's declaration, or the default for an undeclared one.
    pub(super) fn spec(&self, collection: &str) -> Result<CollectionSpec> {
        match read_json::<CollectionSpec>(&self.spec_path(collection))? {
            Some(spec) if spec.name == collection => Ok(spec),
            Some(_) => Err(collision()),
            None => Ok(CollectionSpec::new(collection)),
        }
    }

    pub(super) fn validate_spec(&self, required: &CollectionSpec) -> Result<()> {
        let stored = read_json::<CollectionSpec>(&self.spec_path(&required.name))?
            .ok_or_else(|| StorageError::not_found("collection has no persisted declaration"))?;
        if stored.name != required.name {
            return Err(collision());
        }
        stored.validate()?;
        if stored.merge(required)? != stored {
            return Err(StorageError::invalid_input(
                "stored declaration lacks required features",
            ));
        }
        Ok(())
    }

    /// Merge `spec` into the stored declaration.
    pub(super) fn ensure(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let path = self.spec_path(&spec.name);
        let (stored, merged) = match read_json::<CollectionSpec>(&path)? {
            Some(existing) if existing.name == spec.name => {
                let merged = existing.merge(spec)?;
                (existing, merged)
            }
            Some(_) => return Err(collision()),
            None => (CollectionSpec::new(&spec.name), spec.clone()),
        };
        self.check_existing_unique(&stored, &merged)?;
        write_json(&path, &merged)
    }

    /// The stored file for `id`, expired or not. `Err` when the file belongs
    /// to a different id (a hash collision).
    fn stored(&self, collection: &str, id: &str) -> Result<Option<Record>> {
        match read_json::<Record>(&self.doc_path(collection, id))? {
            Some(stored) if stored.id == id => Ok(Some(stored)),
            Some(_) => Err(collision()),
            None => Ok(None),
        }
    }

    /// Every live (non-expired) document of a collection, in file order.
    pub(super) fn live(&self, spec: &CollectionSpec) -> Result<Vec<StoredDoc>> {
        load_live(&self.collection_dir(&spec.name), spec, self.now)
    }

    /// Read one live document.
    pub(super) fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.spec(collection)?;
        Ok(self
            .stored(collection, id)?
            .and_then(|record| record.live(&spec, self.now))
            .map(StoredDoc::versioned))
    }

    /// Write one document under `precondition`.
    pub(super) fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&doc)?;
        let spec = self.spec(collection)?;
        let stored = self.stored(collection, id)?;
        let current = stored
            .clone()
            .and_then(|record| record.live(&spec, self.now));
        check(precondition, current.as_ref().map(|stored| stored.version))?;
        if spec.indexes.iter().any(|index| index.unique) {
            unique_clash(&spec, &self.live(&spec)?, id, &doc)?;
        }
        // Versions keep rising across expiry and deletion (tombstones), so a
        // stale CAS on a re-created document still fails.
        let version = match &stored {
            None => Version::FIRST,
            Some(stored) => stored
                .version
                .next()
                .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
        };
        // A live document keeps its creation time; one that was never there,
        // was deleted or has expired is created now. The update time never
        // runs backwards (the memory driver's rule).
        let now = i64::try_from(self.now).unwrap_or(i64::MAX);
        let meta = Meta {
            schema: options.schema,
            created_ms: options
                .created_ms
                .unwrap_or_else(|| current.as_ref().map_or(now, |found| found.meta.created_ms)),
            updated_ms: current
                .as_ref()
                .map_or(now, |found| now.max(found.meta.updated_ms)),
            expires_ms: options.expires_ms,
        };
        let record = Record {
            id: id.to_owned(),
            version,
            doc: Some(doc),
            meta,
        };
        write_json(&self.doc_path(collection, id), &record)?;
        Ok(version)
    }

    /// Remove one document under `precondition`; report whether a live one
    /// went.
    pub(super) fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        let spec = self.spec(collection)?;
        let stored = self.stored(collection, id)?;
        let live = stored
            .clone()
            .and_then(|record| record.live(&spec, self.now))
            .map(|stored| stored.version);
        check(precondition, live)?;
        let removed = match stored {
            Some(record) => self.bury(collection, record)?,
            None => false,
        };
        Ok(removed && live.is_some())
    }

    /// Replace a document file with its tombstone; report whether it held a
    /// document (live or expired).
    fn bury(&self, collection: &str, record: Record) -> Result<bool> {
        if record.doc.is_none() {
            return Ok(false);
        }
        let tombstone = Record {
            doc: None,
            meta: Meta::unknown(),
            ..record
        };
        write_json(&self.doc_path(collection, &tombstone.id), &tombstone)?;
        Ok(true)
    }

    /// The live documents matching `filter`, in `sort` order.
    pub(super) fn matching(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
    ) -> Result<Vec<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        let spec = self.spec(collection)?;
        let mut found: Vec<Versioned<Value>> = self
            .live(&spec)?
            .into_iter()
            .filter(|stored| filter.matches_with(&stored.id, &stored.doc, &stored.meta))
            .map(StoredDoc::versioned)
            .collect();
        sort_documents_with_meta(&mut found, sort, |item| {
            (item.id.as_str(), &item.doc, &item.meta)
        });
        Ok(found)
    }

    /// Remove the live documents matching `filter`, returning how many.
    pub(super) fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let doomed = self.matching(collection, filter, &[])?;
        for doc in doomed.iter().cloned() {
            self.bury(
                collection,
                Record {
                    id: doc.id,
                    version: doc.version,
                    doc: Some(doc.doc),
                    meta: doc.meta,
                },
            )?;
        }
        Ok(doomed.len() as u64)
    }

    /// Replace every document of the collection in this scope, live or
    /// expired, with its tombstone.
    pub(super) fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        for path in files_with_suffix(&self.collection_dir(collection), ".json")? {
            if let Some(record) = read_json::<Record>(&path)? {
                self.bury(collection, record)?;
            }
        }
        Ok(())
    }

    /// Collection names are encoded reversibly in scope-local directory
    /// components. Only directories holding document records are reported.
    pub(super) fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let root = self.db.scope_dir(self.scope).join("docs");
        let mut encoded = String::new();
        let mut found = BTreeSet::new();
        collect_collection_dirs(&root, &mut encoded, prefix, &mut found)?;
        Ok(found.into_iter().collect())
    }

    /// Refuse a declaration whose unique indexes the stored documents of
    /// this collection already violate, in any scope. Liveness follows the
    /// declaration already stored, as in the memory driver.
    fn check_existing_unique(
        &self,
        stored: &CollectionSpec,
        merged: &CollectionSpec,
    ) -> Result<()> {
        if !merged.indexes.iter().any(|index| index.unique) {
            return Ok(());
        }
        let components = dir_components(&merged.name);
        for scope_dir in scope_dirs(&self.db.scopes_dir())? {
            let docs = load_live(&scope_dir.join("docs").join(&components), stored, self.now)?;
            for doc in &docs {
                unique_clash(merged, &docs, &doc.id, &doc.doc)?;
            }
        }
        Ok(())
    }
}

fn collect_collection_dirs(
    dir: &Path,
    encoded: &mut String,
    prefix: &str,
    found: &mut BTreeSet<String>,
) -> Result<()> {
    if !files_with_suffix(dir, ".json")?.is_empty()
        && let Some(name) = decode(encoded)
        && name.starts_with(prefix)
    {
        found.insert(name);
    }

    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("list collection directories")(error)),
    };
    let mut children = Vec::new();
    for entry in entries {
        let entry = entry.map_err(io_error("list collection directories"))?;
        let file_type = entry
            .file_type()
            .map_err(io_error("inspect a collection directory"))?;
        let Some(component) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let first_component = encoded.is_empty();
        if file_type.is_dir()
            && if first_component {
                !component.starts_with('+')
            } else {
                component.starts_with('+')
            }
        {
            children.push((entry.path(), component));
        }
    }
    children.sort_by(|a, b| a.1.cmp(&b.1));
    for (child, component) in children {
        let old_len = encoded.len();
        if encoded.is_empty() {
            encoded.push_str(&component);
        } else {
            encoded.push_str(&component[1..]);
        }
        collect_collection_dirs(&child, encoded, prefix, found)?;
        encoded.truncate(old_len);
    }
    Ok(())
}

/// Whether `doc` has passed its collection's expiry time.
fn expired(spec: &CollectionSpec, doc: &Value, now_ms: u64) -> bool {
    spec.ttl_field
        .as_deref()
        .and_then(|field| value::lookup(doc, field))
        .and_then(Value::as_f64)
        .is_some_and(|expires| {
            // Epoch milliseconds fit an f64 exactly until the year 287396.
            #[allow(clippy::cast_precision_loss)]
            let now = now_ms as f64;
            expires <= now
        })
}

/// The live documents in one collection directory.
fn load_live(dir: &Path, spec: &CollectionSpec, now_ms: u64) -> Result<Vec<StoredDoc>> {
    let mut out = Vec::new();
    for path in files_with_suffix(dir, ".json")? {
        if let Some(stored) = read_json::<Record>(&path)?.and_then(|r| r.live(spec, now_ms)) {
            out.push(stored);
        }
    }
    Ok(out)
}

/// Every scope directory under `scopes`: a directory holding `docs`, found by
/// walking the first-level chunks and their `+` continuations.
fn scope_dirs(scopes: &Path) -> Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let mut pending = subdirs(scopes, |_| true)?;
    while let Some(dir) = pending.pop() {
        if dir.join("docs").is_dir() {
            found.push(dir.clone());
        }
        pending.extend(subdirs(&dir, |name| name.starts_with('+'))?);
    }
    Ok(found)
}

/// The subdirectories of `dir` whose names pass `keep`; none when `dir` is
/// missing.
fn subdirs(dir: &Path, keep: impl Fn(&str) -> bool) -> Result<Vec<PathBuf>> {
    let read_error = io_error("list a directory");
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(read_error(error)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(&read_error)?;
        let is_dir = entry.file_type().map_err(&read_error)?.is_dir();
        if is_dir && entry.file_name().to_str().is_some_and(&keep) {
            out.push(entry.path());
        }
    }
    Ok(out)
}

/// Fail with `AlreadyExists` when `doc` (stored as `id`) would share a unique
/// index value with any other document in `others`.
fn unique_clash(spec: &CollectionSpec, others: &[StoredDoc], id: &str, doc: &Value) -> Result<()> {
    for index in spec.indexes.iter().filter(|index| index.unique) {
        let Some(key) = index_key(&index.fields, doc) else {
            continue;
        };
        let clash = others.iter().any(|other| {
            other.id != id
                && index_key(&index.fields, &other.doc)
                    .is_some_and(|theirs| theirs.iter().zip(&key).all(|(a, b)| value::equal(a, b)))
        });
        if clash {
            return Err(StorageError::already_exists(format!(
                "unique index `{}` already holds this value",
                index.name
            )));
        }
    }
    Ok(())
}

/// The values of `fields` in `doc`, or `None` when any is missing.
fn index_key(fields: &[String], doc: &Value) -> Option<Vec<Value>> {
    fields
        .iter()
        .map(|field| value::lookup(doc, field).cloned())
        .collect()
}

/// Enforce a precondition against the live document's version, if any.
pub(super) fn check(precondition: Precondition, current: Option<Version>) -> Result<()> {
    match (precondition, current) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(found)) => Err(StorageError::conflict(format!(
            "document already exists at version {}",
            found.0
        ))),
        (Precondition::Version(expected), Some(found)) if found == expected => Ok(()),
        (Precondition::Version(expected), found) => Err(StorageError::conflict(format!(
            "document expected at version {}, found {}",
            expected.0,
            found.map_or_else(|| "nothing".to_owned(), |f| f.0.to_string())
        ))),
    }
}
