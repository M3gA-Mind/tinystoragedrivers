//! The declaration types: [`Owner`], [`Schema`], [`Item`], [`ScopeClass`],
//! [`Step`] and the records a step reads and writes.

use std::fmt;
use std::sync::Arc;

use serde_json::Value;

use crate::document::{CollectionSpec, Meta};
use crate::error::{Result, StorageError};
use crate::stream::SealPolicy;

/// The owner reserved for the crate's own shared collections.
const RESERVED_OWNER: &str = "tsd";

/// The crate or domain that defines a record type: lowercase
/// `[a-z][a-z0-9_]{1,23}`. Every name an owner declares starts with its owner
/// (`<owner>.` for collections and streams, `<owner>/` for blob and secret
/// prefixes).
///
/// `tsd` is reserved for the shared primitives and is refused by
/// [`Owner::new`].
///
/// ```
/// use tinystoragedrivers_core::schema::Owner;
///
/// assert_eq!(Owner::new("cron").unwrap().as_str(), "cron");
/// assert!(Owner::new("tsd").is_err());
/// assert!(Owner::new("Cron").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Owner(String);

impl Owner {
    /// Validate an owner name.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the
    /// name is outside `[a-z][a-z0-9_]{1,23}` or is the reserved `tsd`.
    pub fn new(name: &str) -> Result<Self> {
        let bytes = name.as_bytes();
        let ok = (2..=24).contains(&bytes.len())
            && bytes.first().is_some_and(u8::is_ascii_lowercase)
            && bytes
                .iter()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_');
        if !ok {
            return Err(StorageError::invalid_input(format!(
                "owner `{name}` must match [a-z][a-z0-9_]{{1,23}}"
            )));
        }
        if name == RESERVED_OWNER {
            return Err(StorageError::invalid_input(format!(
                "owner `{RESERVED_OWNER}` is reserved for the storage crate's own collections"
            )));
        }
        Ok(Self(name.to_owned()))
    }

    /// The reserved `tsd` owner. Not public: only the crate's own shared
    /// collections (`tsd.migrations`, and later `tsd.leases` and friends) use
    /// it.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "first used by the shared primitives (leases, kv, counters)"
        )
    )]
    pub(crate) fn tsd() -> Self {
        Self(RESERVED_OWNER.to_owned())
    }

    /// The owner as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which scope an item lives in. The registry records it so a host can
/// derive grants and pick the right [`Scope`](crate::Scope) for each item; the
/// storage drivers do not look at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeClass {
    /// Shared across the tenant (`u:<id>` or `local`).
    Tenant,
    /// One agent inside the tenant (`<tenant>/agent:<id>`).
    Agent,
    /// One module inside the tenant (`<tenant>/module:<id>`).
    Module,
    /// The tenant-less `cluster` scope.
    Cluster,
}

/// One thing an owner stores.
#[derive(Debug, Clone)]
pub enum Item {
    /// A document collection, named `<owner>.<entity>`, at body `version`
    /// (starting at 1).
    Collection {
        /// The collection name.
        name: String,
        /// The declared body version.
        version: u32,
        /// Indexes, search and so on, declared when the registry is prepared.
        spec: CollectionSpec,
        /// Where the collection lives.
        scope: ScopeClass,
    },
    /// A stream family, declared by its base name `<owner>.<stream>`. Entries
    /// are appended to `<base>` or `<base>/<key>` at use time.
    Stream {
        /// The base stream name.
        name: String,
        /// Where the streams live.
        scope: ScopeClass,
        /// When the owner seals these streams and how long sealed ones are
        /// kept.
        seal: SealPolicy,
    },
    /// A blob key prefix `<owner>/…`.
    Blobs {
        /// The key prefix.
        prefix: String,
        /// Where the blobs live.
        scope: ScopeClass,
    },
    /// A secret key prefix `<owner>/…`.
    Secrets {
        /// The key prefix.
        prefix: String,
        /// Where the secrets live.
        scope: ScopeClass,
    },
}

impl Item {
    /// The name (collection or stream) or prefix (blobs or secrets) the item
    /// claims.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Collection { name, .. } | Self::Stream { name, .. } => name,
            Self::Blobs { prefix, .. } | Self::Secrets { prefix, .. } => prefix,
        }
    }

    /// The scope class of the item.
    #[must_use]
    pub fn scope(&self) -> ScopeClass {
        match self {
            Self::Collection { scope, .. }
            | Self::Stream { scope, .. }
            | Self::Blobs { scope, .. }
            | Self::Secrets { scope, .. } => *scope,
        }
    }
}

/// A document handed to a [`Step`], as stored in its source collection.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceRecord {
    /// The document id.
    pub id: String,
    /// The document body.
    pub body: Value,
    /// The driver's metadata for it; `meta.created_ms` is what a step passes to
    /// [`TargetRecord::created_ms`] to keep history.
    pub meta: Meta,
}

/// A document a [`Step`] wants written.
///
/// It is written to `collection` (an item of the same owner) with the schema
/// version implied by the step's `to` (or by the declared version of a
/// different item), and with `Precondition::Absent`, so a record that already
/// exists is kept. Its expiry defaults to the source record's when the output
/// rewrites that record under its own id ([`Expiry::Carry`]).
#[derive(Debug, Clone, PartialEq)]
pub struct TargetRecord {
    /// Destination collection.
    pub collection: String,
    /// Destination id.
    pub id: String,
    /// The new body.
    pub body: Value,
    /// Creation time to record (Unix epoch ms), usually the source's. `None`
    /// uses the driver clock (or keeps the time of an in-place rewrite).
    pub created_ms: Option<i64>,
    /// What to record as the expiry. See [`Expiry`].
    pub expires: Expiry,
}

/// The expiry a [`TargetRecord`] is written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Expiry {
    /// The expiry of the source record with the same id, when the output is
    /// that record rewritten (an in-place step, or a step that keeps ids);
    /// none for a record with a new id, which the step must date itself.
    #[default]
    Carry,
    /// The record never expires, whatever the source did.
    Never,
    /// The record reads as absent from this Unix epoch millisecond.
    At(i64),
}

impl TargetRecord {
    /// A record for `collection` with no creation time of its own.
    #[must_use]
    pub fn new(collection: impl Into<String>, id: impl Into<String>, body: Value) -> Self {
        Self {
            collection: collection.into(),
            id: id.into(),
            body,
            created_ms: None,
            expires: Expiry::Carry,
        }
    }

    /// Keep `created_ms` as the creation time (migration only).
    #[must_use]
    pub fn created_ms(mut self, created_ms: i64) -> Self {
        self.created_ms = Some(created_ms);
        self
    }

    /// Record `expires_ms` as the expiry (`Some(ms)`), or none (`None`),
    /// instead of carrying the source record's. A step moving a legacy TTL
    /// body field into metadata passes it here; one that renames a record
    /// passes `record.meta.expires_ms` to keep the old deadline.
    #[must_use]
    pub fn expires_ms(mut self, expires_ms: Option<i64>) -> Self {
        self.expires = expires_ms.map_or(Expiry::Never, Expiry::At);
        self
    }
}

/// The transformation of a document [`Step`]: a pure function of its batch, so
/// a re-run produces the same output.
pub type StepFn = Arc<dyn Fn(Vec<SourceRecord>) -> Result<Vec<TargetRecord>> + Send + Sync>;

/// The transformation of a stream [`Step`]: `(source stream name, its entries
/// in order)` to `Some((target stream name, new entries))`, or `None` to drop
/// the stream. Pure, like [`StepFn`]: a resumed run calls it again and must
/// get the same entries.
pub type StreamStepFn =
    Arc<dyn Fn(String, Vec<Value>) -> Result<Option<(String, Vec<Value>)>> + Send + Sync>;

/// What a blob [`Step`] writes: `(target key, content type, bytes)`.
pub type BlobOutput = (String, Option<String>, Vec<u8>);

/// The transformation of a blob [`Step`]: `(source key, content type, bytes)`
/// to `Some((target key, content type, bytes))`, or `None` to drop the blob.
/// Pure, like [`StepFn`].
pub type BlobStepFn =
    Arc<dyn Fn(String, Option<String>, Vec<u8>) -> Result<Option<BlobOutput>> + Send + Sync>;

/// What a [`Step`] does, and so which kind of [`Item`] it can target.
#[derive(Clone)]
pub enum StepBody {
    /// Rewrite the documents of a [`Item::Collection`].
    Documents(StepFn),
    /// Move the legacy streams under a prefix into an [`Item::Stream`].
    Streams(StreamStepFn),
    /// Move the legacy blobs under a prefix into an [`Item::Blobs`].
    Blobs(BlobStepFn),
}

impl StepBody {
    fn kind(&self) -> &'static str {
        match self {
            Self::Documents(_) => "documents",
            Self::Streams(_) => "streams",
            Self::Blobs(_) => "blobs",
        }
    }
}

impl fmt::Debug for StepBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StepBody::{}", self.kind())
    }
}

/// Brings one item forward from version `from` to `to`.
///
/// - [`Step::new`] rewrites the records of a **collection**. By default the
///   source is the collection itself (an in-place rewrite of the records
///   still below version `to`). [`Step::source`] names another collection
///   instead, normally a legacy v1 name; its rows are deleted once the step
///   completes.
/// - [`Step::stream`] moves **streams**: every stream whose name starts with
///   the [`Step::source_prefix`] is read whole, mapped, written under its new
///   name inside the item, and deleted.
/// - [`Step::blobs`] moves **blobs** the same way, one blob at a time.
///
/// Stream and blob versions are labels for ordering the steps of an item
/// (they start at 1 and must not skip); unlike a collection, the item has no
/// declared version to end at.
#[derive(Clone)]
pub struct Step {
    /// Identifies the step in `tsd.migrations`; unique within its item.
    pub id: &'static str,
    /// The item the step brings to version `to`.
    pub item: &'static str,
    /// The version it reads.
    pub from: u32,
    /// The version it writes.
    pub to: u32,
    /// A legacy collection to read instead of `item` (document steps), or the
    /// legacy stream name / blob key prefix to migrate (stream and blob
    /// steps, where it is required).
    pub source: Option<&'static str>,
    /// The transformation.
    pub run: StepBody,
}

impl Step {
    /// A document step over collection `item` from version `from` to `to`.
    #[must_use]
    pub fn new(
        id: &'static str,
        item: &'static str,
        from: u32,
        to: u32,
        run: impl Fn(Vec<SourceRecord>) -> Result<Vec<TargetRecord>> + Send + Sync + 'static,
    ) -> Self {
        Self::with_body(id, item, from, to, StepBody::Documents(Arc::new(run)))
    }

    /// A stream step into the stream family `item`. Set the legacy streams
    /// with [`Step::source_prefix`].
    ///
    /// The target name `run` returns must be `item` itself or start with
    /// `item/`. Migrated streams start at offset 0, whatever the offsets of
    /// their source were: a source that had been truncated keeps only the
    /// entries `read_window` can still see, renumbered from 0, because a
    /// stream's base cannot be set from outside the driver. Owners that keep
    /// offsets as external references must translate them with the source's
    /// base. A sealed source produces a sealed target.
    #[must_use]
    pub fn stream(
        id: &'static str,
        item: &'static str,
        from: u32,
        to: u32,
        run: impl Fn(String, Vec<Value>) -> Result<Option<(String, Vec<Value>)>> + Send + Sync + 'static,
    ) -> Self {
        Self::with_body(id, item, from, to, StepBody::Streams(Arc::new(run)))
    }

    /// A blob step into the prefix `item`. Set the legacy blobs with
    /// [`Step::source_prefix`].
    ///
    /// The target key `run` returns must start with `item`. An existing
    /// target is kept (nothing is overwritten).
    #[must_use]
    pub fn blobs(
        id: &'static str,
        item: &'static str,
        from: u32,
        to: u32,
        run: impl Fn(String, Option<String>, Vec<u8>) -> Result<Option<BlobOutput>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::with_body(id, item, from, to, StepBody::Blobs(Arc::new(run)))
    }

    fn with_body(id: &'static str, item: &'static str, from: u32, to: u32, run: StepBody) -> Self {
        Self {
            id,
            item,
            from,
            to,
            source: None,
            run,
        }
    }

    /// Read `name` (a legacy collection) instead of the item itself.
    #[must_use]
    pub fn source(mut self, name: &'static str) -> Self {
        self.source = Some(name);
        self
    }

    /// The legacy stream name prefix or blob key prefix a stream or blob step
    /// migrates. It must not overlap the item's own name, so the runner never
    /// reads its own output.
    #[must_use]
    pub fn source_prefix(self, prefix: &'static str) -> Self {
        self.source(prefix)
    }

    /// The collection a document step reads.
    pub(super) fn source_collection(&self) -> &'static str {
        self.source.unwrap_or(self.item)
    }
}

impl fmt::Debug for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Step")
            .field("id", &self.id)
            .field("kind", &self.run.kind())
            .field("item", &self.item)
            .field("from", &self.from)
            .field("to", &self.to)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

/// What one owner stores and how older versions are brought forward. Each
/// owning crate exports `pub fn storage_schema() -> Schema`.
///
/// Built with chained calls; nothing is validated until
/// [`Registry::compose`](super::Registry::compose).
#[derive(Debug, Clone)]
pub struct Schema {
    /// The owner every name must start with.
    pub owner: Owner,
    /// Everything the owner stores.
    pub items: Vec<Item>,
    /// The migration steps, in the order they run.
    pub steps: Vec<Step>,
}

impl Schema {
    /// An empty schema for `owner`.
    #[must_use]
    pub fn new(owner: Owner) -> Self {
        Self {
            owner,
            items: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// Declare collection `name` at `version`, with no indexes.
    #[must_use]
    pub fn collection(self, name: &str, version: u32, scope: ScopeClass) -> Self {
        self.collection_with(version, CollectionSpec::new(name), scope)
    }

    /// Declare a collection from a full spec (indexes, search, ttl); the item
    /// takes the spec's name.
    #[must_use]
    pub fn collection_with(
        mut self,
        version: u32,
        spec: CollectionSpec,
        scope: ScopeClass,
    ) -> Self {
        self.items.push(Item::Collection {
            name: spec.name.clone(),
            version,
            spec,
            scope,
        });
        self
    }

    /// Declare stream family `name`.
    #[must_use]
    pub fn stream(self, name: &str, scope: ScopeClass) -> Self {
        self.stream_with(name, scope, SealPolicy::default())
    }

    /// Declare a stream family with its seal policy.
    #[must_use]
    pub fn stream_with(mut self, name: &str, scope: ScopeClass, seal: SealPolicy) -> Self {
        self.items.push(Item::Stream {
            name: name.to_owned(),
            scope,
            seal,
        });
        self
    }

    /// Declare a blob prefix.
    #[must_use]
    pub fn blobs(mut self, prefix: &str, scope: ScopeClass) -> Self {
        self.items.push(Item::Blobs {
            prefix: prefix.to_owned(),
            scope,
        });
        self
    }

    /// Declare a secret prefix.
    #[must_use]
    pub fn secrets(mut self, prefix: &str, scope: ScopeClass) -> Self {
        self.items.push(Item::Secrets {
            prefix: prefix.to_owned(),
            scope,
        });
        self
    }

    /// Add a migration step.
    #[must_use]
    pub fn step(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
