//! [`Registry`]: composition of owner schemas, backend preparation and the
//! readiness gate.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::backend::StorageBackend;
use crate::blob::MAX_BLOB_KEY_LEN;
use crate::document::{MAX_COLLECTION_LEN, validate_collection};
use crate::error::{Result, StorageError};
use crate::primitives;
use crate::scope::Scope;
use crate::stream::{SEALED_BLOB_PREFIX, validate_stream};

use super::migrations::migrations_spec;
use super::types::{Item, Owner, Schema, Step, StepBody};

/// The composed declarations of every owner in a process.
///
/// Built once by the host with [`Registry::compose`]. The registry itself is
/// immutable apart from the per-scope readiness sets [`Registry::check_ready`]
/// and [`Registry::check_ready_in`] read, which
/// [`Registry::migrate`](Registry::migrate) keeps current.
///
/// ```
/// use tinystoragedrivers_core::schema::{Owner, Registry, Schema, ScopeClass};
///
/// let cron = Schema::new(Owner::new("cron").unwrap())
///     .collection("cron.jobs", 1, ScopeClass::Tenant);
/// let registry = Registry::compose([cron]).unwrap();
/// assert_eq!(registry.collection_version("cron.jobs"), Some(1));
/// registry.check_ready("cron.jobs").unwrap();
///
/// let stray = Schema::new(Owner::new("cron").unwrap())
///     .collection("flows.runs", 1, ScopeClass::Tenant);
/// assert!(Registry::compose([stray]).is_err());
/// ```
#[derive(Debug)]
pub struct Registry {
    pub(super) schemas: Vec<Schema>,
    pub(super) collections: HashMap<String, CollectionInfo>,
    /// Every declared name or prefix, for [`Registry::check_ready`].
    declared: HashSet<String>,
    /// Items that have at least one step.
    stepped: HashSet<String>,
    /// For each scope [`Registry::migrate`] has run over, the items whose
    /// steps have not all finished there.
    pending: Mutex<HashMap<Scope, HashSet<String>>>,
}

#[derive(Debug, Clone)]
pub(super) struct CollectionInfo {
    pub(super) version: u32,
}

impl Registry {
    /// Validate and combine `schemas`.
    ///
    /// Nothing touches a backend. Items that have steps start out
    /// *pending*: [`Registry::check_ready`] and [`Registry::check_ready_in`]
    /// refuse them until [`Registry::migrate`](Registry::migrate) has seen
    /// every step finished.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput), naming the
    /// owner and item at fault, for: a malformed owner, collection, stream or
    /// prefix; a name outside its owner's prefix; a name or owner declared
    /// twice; a collection version of 0; a step for an item that is not declared by
    /// its owner or is of another kind than the step (collection, stream or
    /// blob prefix); a stream or blob step with no source prefix, or one that
    /// overlaps its item (or, for blobs, the sealed-stream blobs); a step that does not go up; a step id used
    /// twice; and a step chain that does not start at version 1, has a gap,
    /// or does not end at the declared version (stream and blob items have no
    /// declared version, so their chains only have to start at 1 and not skip).
    pub fn compose(schemas: impl IntoIterator<Item = Schema>) -> Result<Self> {
        let schemas: Vec<Schema> = schemas.into_iter().collect();
        let mut owners = HashSet::new();
        for schema in &schemas {
            if !owners.insert(schema.owner.as_str()) {
                return Err(StorageError::invalid_input(format!(
                    "owner `{}` is declared twice",
                    schema.owner
                )));
            }
        }
        let mut claims: HashMap<&str, &Owner> = HashMap::new();
        let mut collections = HashMap::new();
        let mut declared = HashSet::new();
        let mut stepped = HashSet::new();
        for schema in &schemas {
            for item in &schema.items {
                check_item(&schema.owner, item, &owners)?;
                if let Some(first) = claims.insert(item.name(), &schema.owner) {
                    return Err(StorageError::invalid_input(format!(
                        "`{}` is claimed twice: by owner `{first}` and by owner `{}`",
                        item.name(),
                        schema.owner
                    )));
                }
                declared.insert(item.name().to_owned());
                if let Item::Collection { name, version, .. } = item {
                    collections.insert(name.clone(), CollectionInfo { version: *version });
                }
            }
            check_steps(schema)?;
            stepped.extend(schema.steps.iter().map(|step| step.item.to_owned()));
        }
        Ok(Self {
            schemas,
            collections,
            declared,
            stepped,
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// The owners, in the order they were composed.
    pub fn owners(&self) -> impl Iterator<Item = &str> {
        self.schemas.iter().map(|schema| schema.owner.as_str())
    }

    /// The declared version of collection `name`, or `None` when no owner
    /// declares it.
    #[must_use]
    pub fn collection_version(&self, name: &str) -> Option<u32> {
        self.collections.get(name).map(|info| info.version)
    }

    /// Declare every collection in `backend`, including the internal
    /// `tsd.migrations` and the shared primitives' `tsd.leases`, `tsd.kv`
    /// and `tsd.counters` ([`primitives::collection_specs`]). Safe to call again: `ensure_collection` merges into
    /// what is declared.
    ///
    /// **Trusted bootstrap, host only.** Owners never call this; it is the one
    /// place declarations are made. Drivers declare a collection for the whole
    /// backend, so the handle's scope (`local`) is only a way in.
    ///
    /// # Errors
    ///
    /// Whatever the driver's `ensure_collection` returns, for instance
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) on an
    /// existing-schema backend.
    pub async fn prepare(&self, backend: &dyn StorageBackend) -> Result<()> {
        let storage = backend.for_scope(&Scope::local())?;
        storage
            .documents()
            .ensure_collection(&migrations_spec())
            .await?;
        for spec in primitives::collection_specs() {
            storage.documents().ensure_collection(&spec).await?;
        }
        for schema in &self.schemas {
            for item in &schema.items {
                if let Item::Collection { spec, .. } = item {
                    storage.documents().ensure_collection(spec).await?;
                }
            }
        }
        Ok(())
    }

    /// Whether `item` may be written in `scope` now. Typed repositories and
    /// hosts call it before a write; drivers do not enforce it.
    ///
    /// Migration progress lives in each scope's own `tsd.migrations`, so
    /// readiness is per scope: an item with steps is open in `scope` only
    /// after [`Registry::migrate`](Registry::migrate) has finished every one of
    /// its steps over that scope. A scope `migrate` has not run over is not
    /// ready. Use this for any item whose data is split across scopes.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unavailable`](crate::ErrorKind::Unavailable) with the
    /// message `migrating <owner>.<item>` while steps are pending in `scope`;
    /// [`ErrorKind::NotFound`](crate::ErrorKind::NotFound) when no owner
    /// declares `item`.
    pub fn check_ready_in(&self, scope: &Scope, item: &str) -> Result<()> {
        self.check_declared(item)?;
        if !self.stepped.contains(item) {
            return Ok(());
        }
        let ready = self
            .pending()
            .get(scope)
            .is_some_and(|pending| !pending.contains(item));
        if ready { Ok(()) } else { Err(migrating(item)) }
    }

    /// Whether `item` may be written now, without naming a scope: for a
    /// process that migrates one scope, or a host that has no use for the
    /// distinction.
    ///
    /// An item with steps is open once `migrate` has run over at least one
    /// scope and no scope it has run over still has steps pending for it.
    /// The registry cannot know scopes it has not been asked to migrate, so
    /// this does **not** vouch for them; a host with several scopes calls
    /// [`Registry::check_ready_in`] instead.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unavailable`](crate::ErrorKind::Unavailable) with the
    /// message `migrating <owner>.<item>` while steps are pending (or before
    /// any scope was migrated); [`ErrorKind::NotFound`](crate::ErrorKind::NotFound)
    /// when no owner declares `item`.
    pub fn check_ready(&self, item: &str) -> Result<()> {
        self.check_declared(item)?;
        if !self.stepped.contains(item) {
            return Ok(());
        }
        let pending = self.pending();
        if pending.is_empty() || pending.values().any(|items| items.contains(item)) {
            return Err(migrating(item));
        }
        Ok(())
    }

    fn check_declared(&self, item: &str) -> Result<()> {
        if self.declared.contains(item) {
            Ok(())
        } else {
            Err(StorageError::not_found(format!(
                "unknown storage item `{item}`"
            )))
        }
    }

    fn pending(&self) -> MutexGuard<'_, HashMap<Scope, HashSet<String>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record the items `scope` still has steps pending for (the runner,
    /// after reading that scope's `tsd.migrations`). Other scopes are not
    /// touched.
    pub(super) fn set_pending(&self, scope: &Scope, items: HashSet<String>) {
        self.pending().insert(scope.clone(), items);
    }

    /// Open `item` for writes in `scope`.
    pub(super) fn mark_ready(&self, scope: &Scope, item: &str) {
        if let Some(items) = self.pending().get_mut(scope) {
            items.remove(item);
        }
    }

    /// Every step with its owner, in run order.
    pub(super) fn steps(&self) -> impl Iterator<Item = (&Owner, &Step)> {
        self.schemas
            .iter()
            .flat_map(|schema| schema.steps.iter().map(|step| (&schema.owner, step)))
    }
}

fn migrating(item: &str) -> StorageError {
    StorageError::unavailable(format!("migrating {item}"))
}

fn check_item(owner: &Owner, item: &Item, owners: &HashSet<&str>) -> Result<()> {
    match item {
        Item::Collection {
            name,
            version,
            spec,
            ..
        } => {
            check_dotted(owner, name, "collection", owners)?;
            validate_collection(name).map_err(|e| with_context(&e, name))?;
            if spec.name != *name {
                return Err(StorageError::invalid_input(format!(
                    "collection `{name}` of owner `{owner}` carries a spec for `{}`",
                    spec.name
                )));
            }
            spec.validate().map_err(|e| with_context(&e, name))?;
            if *version == 0 {
                return Err(StorageError::invalid_input(format!(
                    "collection `{name}` of owner `{owner}` is declared at version 0; versions start at 1"
                )));
            }
        }
        Item::Stream { name, .. } => {
            check_dotted(owner, name, "stream", owners)?;
            if name.len() > MAX_COLLECTION_LEN {
                return Err(StorageError::invalid_input(format!(
                    "stream `{name}` of owner `{owner}` is longer than {MAX_COLLECTION_LEN} bytes"
                )));
            }
        }
        Item::Blobs { prefix, .. } => check_prefix(owner, prefix, "blob prefix", owners)?,
        Item::Secrets { prefix, .. } => check_prefix(owner, prefix, "secret prefix", owners)?,
    }
    Ok(())
}

fn with_context(error: &StorageError, name: &str) -> StorageError {
    StorageError::invalid_input(format!("`{name}`: {}", error.message()))
}

/// `<owner>.<entity>` with a lowercase `snake_case` entity.
fn check_dotted(owner: &Owner, name: &str, what: &str, owners: &HashSet<&str>) -> Result<()> {
    let Some(entity) = name
        .strip_prefix(owner.as_str())
        .and_then(|r| r.strip_prefix('.'))
    else {
        return Err(outside_prefix(owner, name, what, '.', owners));
    };
    let mut bytes = entity.bytes();
    let ok = bytes.next().is_some_and(|b| b.is_ascii_lowercase())
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if ok {
        Ok(())
    } else {
        Err(StorageError::invalid_input(format!(
            "{what} `{name}` of owner `{owner}`: the entity after `{owner}.` must be lowercase snake case ([a-z][a-z0-9_]*)"
        )))
    }
}

/// `<owner>/<path>` with plain, non-empty segments (a trailing `/` is fine).
fn check_prefix(owner: &Owner, prefix: &str, what: &str, owners: &HashSet<&str>) -> Result<()> {
    let Some(rest) = prefix
        .strip_prefix(owner.as_str())
        .and_then(|r| r.strip_prefix('/'))
    else {
        return Err(outside_prefix(owner, prefix, what, '/', owners));
    };
    let mut segments: Vec<&str> = rest.split('/').collect();
    if segments.last() == Some(&"") {
        segments.pop();
    }
    let ok = !segments.is_empty()
        && prefix.len() <= MAX_BLOB_KEY_LEN
        && !prefix.contains(['\0', '\\'])
        && segments
            .iter()
            .all(|segment| !segment.is_empty() && *segment != "." && *segment != "..");
    if ok {
        Ok(())
    } else {
        Err(StorageError::invalid_input(format!(
            "{what} `{prefix}` of owner `{owner}` must be `{owner}/` followed by plain path segments"
        )))
    }
}

fn outside_prefix(
    owner: &Owner,
    name: &str,
    what: &str,
    separator: char,
    owners: &HashSet<&str>,
) -> StorageError {
    let belongs = name
        .split(separator)
        .next()
        .filter(|head| *head != owner.as_str() && owners.contains(head));
    let note = belongs.map_or_else(String::new, |other| {
        format!("; it belongs to owner `{other}`")
    });
    StorageError::invalid_input(format!(
        "{what} `{name}` declared by owner `{owner}` must start with `{owner}{separator}`{note}"
    ))
}

fn check_steps(schema: &Schema) -> Result<()> {
    let owner = &schema.owner;
    let mut chains: HashMap<&str, Vec<&Step>> = HashMap::new();
    for step in &schema.steps {
        let Some(item) = schema.items.iter().find(|item| item.name() == step.item) else {
            return Err(StorageError::invalid_input(format!(
                "step `{}` targets `{}`, which is not declared by owner `{owner}`",
                step.id, step.item
            )));
        };
        check_step_kind(step, item)?;
        if step.to <= step.from {
            return Err(StorageError::invalid_input(format!(
                "step `{}` of `{}` must go up, but runs from version {} to {}",
                step.id, step.item, step.from, step.to
            )));
        }
        check_source(step, item)?;
        let chain = chains.entry(step.item).or_default();
        if chain.iter().any(|earlier| earlier.id == step.id) {
            return Err(StorageError::invalid_input(format!(
                "step id `{}` is used twice for `{}`",
                step.id, step.item
            )));
        }
        chain.push(step);
    }
    for item in &schema.items {
        let chain = chains.get(item.name()).map_or(&[][..], Vec::as_slice);
        match item {
            Item::Collection { name, version, .. } => check_chain(name, *version, chain)?,
            Item::Stream { name, .. } => check_unversioned_chain(name, chain)?,
            Item::Blobs { prefix, .. } => check_unversioned_chain(prefix, chain)?,
            Item::Secrets { .. } => {}
        }
    }
    Ok(())
}

/// A step runs against exactly the kind of item its body is written for.
fn check_step_kind(step: &Step, item: &Item) -> Result<()> {
    let (matches, wanted) = match (&step.run, item) {
        (StepBody::Documents(_), Item::Collection { .. }) => (true, "collection"),
        (StepBody::Streams(_), Item::Stream { .. }) => (true, "stream"),
        (StepBody::Blobs(_), Item::Blobs { .. }) => (true, "blob prefix"),
        (StepBody::Documents(_), _) => (false, "collection"),
        (StepBody::Streams(_), _) => (false, "stream"),
        (StepBody::Blobs(_), _) => (false, "blob prefix"),
    };
    if matches {
        Ok(())
    } else {
        Err(StorageError::invalid_input(format!(
            "step `{}` targets `{}`, which is not a {wanted} declared by its owner; collection steps rewrite collections, stream steps move streams and blob steps move blob prefixes",
            step.id, step.item
        )))
    }
}

/// The legacy source of a step: a collection name, or for stream and blob
/// steps a required prefix that stays clear of the item and, for blobs, of
/// the sealed-stream blobs.
fn check_source(step: &Step, item: &Item) -> Result<()> {
    let fail = |why: String| {
        StorageError::invalid_input(format!(
            "step `{}` of `{}` has source `{}`: {why}",
            step.id,
            step.item,
            step.source.unwrap_or_default()
        ))
    };
    match (&step.run, step.source) {
        (StepBody::Documents(_), Some(source)) => {
            validate_collection(source).map_err(|e| fail(e.message().to_owned()))
        }
        (StepBody::Documents(_), None) => Ok(()),
        (_, None) => Err(StorageError::invalid_input(format!(
            "step `{}` of `{}` needs a legacy source prefix: set it with `source_prefix`",
            step.id, step.item
        ))),
        (body, Some(source)) => {
            let blobs = matches!(body, StepBody::Blobs(_));
            let valid = if blobs {
                !source.is_empty() && source.len() <= MAX_BLOB_KEY_LEN && !source.contains('\0')
            } else {
                validate_stream(source).is_ok()
            };
            if !valid {
                return Err(fail("not a valid prefix".to_owned()));
            }
            let target = item.name();
            if source.starts_with(target) || target.starts_with(source) {
                return Err(fail(format!(
                    "it overlaps the item `{target}`, so the step would read its own output"
                )));
            }
            if blobs
                && (source.starts_with(SEALED_BLOB_PREFIX)
                    || SEALED_BLOB_PREFIX.starts_with(source))
            {
                return Err(fail(format!(
                    "it overlaps `{SEALED_BLOB_PREFIX}`, which holds sealed streams"
                )));
            }
            Ok(())
        }
    }
}

/// Stream and blob items declare no version: the chain only has to start at 1
/// and not skip.
fn check_unversioned_chain(name: &str, chain: &[&Step]) -> Result<()> {
    let Some(first) = chain.first() else {
        return Ok(());
    };
    if first.from != 1 {
        return Err(StorageError::invalid_input(format!(
            "steps for `{name}` must start at version 1, but `{}` starts at {}",
            first.id, first.from
        )));
    }
    for pair in chain.windows(2) {
        if let [earlier, later] = pair
            && later.from != earlier.to
        {
            return Err(StorageError::invalid_input(format!(
                "step gap for `{name}`: `{}` starts at {} after `{}` ended at {}",
                later.id, later.from, earlier.id, earlier.to
            )));
        }
    }
    Ok(())
}

fn check_chain(name: &str, version: u32, chain: &[&Step]) -> Result<()> {
    let (Some(first), Some(last)) = (chain.first(), chain.last()) else {
        return if version == 1 {
            Ok(())
        } else {
            Err(StorageError::invalid_input(format!(
                "`{name}` is declared at version {version} but has no steps from version 1"
            )))
        };
    };
    if first.from != 1 {
        return Err(StorageError::invalid_input(format!(
            "steps for `{name}` must start at version 1, but `{}` starts at {}",
            first.id, first.from
        )));
    }
    for pair in chain.windows(2) {
        if let [earlier, later] = pair
            && later.from != earlier.to
        {
            return Err(StorageError::invalid_input(format!(
                "step gap for `{name}`: `{}` starts at {} after `{}` ended at {}",
                later.id, later.from, earlier.id, earlier.to
            )));
        }
    }
    if last.to != version {
        return Err(StorageError::invalid_input(format!(
            "the step chain for `{name}` ends at {} but version {version} is declared",
            last.to
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "compose_tests.rs"]
mod tests;
