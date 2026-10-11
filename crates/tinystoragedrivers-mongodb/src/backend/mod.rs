//! [`MongoStorage`]: one MongoDB database shared by every tenant.
//!
//! The backend owns the client and, per named database (a collection
//! prefix), a [`Shared`] holding the declared collection specs and which
//! collections already carry the driver's indexes. Port handles are cheap
//! views over a `Shared` bound to one scope.

pub(crate) mod specs;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use mongodb::bson::{Document, doc};
use mongodb::options::ClientOptions;
use mongodb::{Client, ClientSession, Collection, Database};
use tinystoragedrivers_core::{
    Capabilities, Capability, Clock, Fence, GuardDatabase, Result, Scope, ScopedStorage,
    StorageBackend, StorageError, validate_collection, validate_database,
};

use crate::blobs::MongoBlobs;
use crate::documents::MongoDocuments;
use crate::errors;
use crate::fence::Fencing;
use crate::naming::{database_prefix, validate_mongo_database};
use crate::scoped::ScopedCollection;
use crate::streams::MongoStreams;

pub(crate) use specs::SpecCache;

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Whether a `hello` reply describes a deployment that runs multi-document
/// transactions: a replica set member or a `mongos` router, with sessions.
pub(crate) fn supports_transactions(hello: &Document) -> bool {
    let sessions = hello.contains_key("logicalSessionTimeoutMinutes");
    let replica_set = hello.get_str("setName").is_ok();
    let router = hello.get_str("msg").is_ok_and(|msg| msg == "isdbgrid");
    sessions && (replica_set || router)
}

/// What every handle of one named database shares.
pub(crate) struct Shared {
    pub(crate) client: Client,
    pub(crate) watch_origin: mongodb::bson::oid::ObjectId,
    pub(crate) db: Database,
    /// Prepended to every Mongo collection name: empty for the root database.
    pub(crate) prefix: String,
    pub(crate) clock: Clock,
    pub(crate) transactions: bool,
    /// Detected server support, retained when public capabilities are disabled.
    pub(crate) physical_transactions: bool,
    validation_ready: AtomicBool,
    validation_scopes: Mutex<BTreeSet<String>>,
    pub(crate) specs: SpecCache,
}

impl Shared {
    fn new(
        client: Client,
        db: Database,
        prefix: String,
        clock: Clock,
        transactions: bool,
        physical_transactions: bool,
    ) -> Self {
        Self {
            watch_origin: mongodb::bson::oid::ObjectId::new(),
            client,
            db,
            prefix,
            clock,
            transactions,
            physical_transactions,
            validation_ready: AtomicBool::new(false),
            validation_scopes: Mutex::new(BTreeSet::new()),
            specs: SpecCache::default(),
        }
    }

    async fn prepare_validation_registry(&self) -> Result<()> {
        if self.validation_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let outcome = self
            .raw("_tsd_validated_batch")
            .update_one(
                doc! {"_id": "documents"},
                doc! {"$setOnInsert": {"epoch": 0_i64}},
            )
            .upsert(true)
            .await;
        if let Err(error) = outcome {
            let error = errors::map(error, "initialize document validation metadata");
            if error.kind() != tinystoragedrivers_core::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        self.validation_ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Register a scope against collection-wide schema mutations. Existing
    /// scopes take no registration lock on their ordinary mutation paths.
    pub(crate) async fn prepare_validation(&self, scope: Option<&Scope>) -> Result<()> {
        self.prepare_validation_registry().await?;
        let Some(scope) = scope else {
            return Ok(());
        };
        let key = format!("scope:{}", scope.as_str());
        if self
            .validation_scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&key)
        {
            return Ok(());
        }
        let metadata = self.raw("_tsd_validated_batch");
        if metadata
            .find_one(doc! {"_id": &key})
            .await
            .map_err(errors::failed("read validation scope"))?
            .is_none()
        {
            crate::fence::in_write!(self, None, session => {
                metadata.update_one(doc! {"_id": "documents"}, doc! {"$inc": {"epoch": 1_i64}})
                    .session(&mut *session).await.map_err(errors::failed("register validation scope"))?;
                metadata.update_one(doc! {"_id": &key}, doc! {"$setOnInsert": {"epoch": 0_i64, "scope": scope.as_str()}})
                    .upsert(true).session(&mut *session).await.map_err(errors::failed("initialize validation scope"))?;
                Ok(())
            })?;
        }
        self.validation_scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key);
        Ok(())
    }

    /// Ordinary mutations lock only their tenant. Collection-wide schema
    /// changes lock registration and every admitted tenant in one transaction.
    pub(crate) async fn hold_validation(
        &self,
        scope: Option<&Scope>,
        session: &mut ClientSession,
    ) -> Result<()> {
        let metadata = self.raw("_tsd_validated_batch");
        if let Some(scope) = scope {
            let key = format!("scope:{}", scope.as_str());
            let held = metadata
                .update_one(doc! {"_id": key}, doc! {"$inc": {"epoch": 1_i64}})
                .session(session)
                .await
                .map_err(errors::failed("hold document validation metadata"))?;
            if held.matched_count != 1 {
                return Err(StorageError::unavailable(
                    "document validation metadata is missing; retry",
                ));
            }
        } else {
            metadata
                .update_one(doc! {"_id": "documents"}, doc! {"$inc": {"epoch": 1_i64}})
                .session(&mut *session)
                .await
                .map_err(errors::failed("hold validation registration"))?;
            metadata
                .update_many(
                    doc! {"scope": {"$exists": true}},
                    doc! {"$inc": {"epoch": 1_i64}},
                )
                .session(session)
                .await
                .map_err(errors::failed("hold all document validation scopes"))?;
        }
        Ok(())
    }

    /// Milliseconds since the Unix epoch, from the configured clock.
    pub(crate) fn now(&self) -> u64 {
        (self.clock)()
    }

    /// The raw Mongo collection behind a driver or port collection name.
    /// Only collection-wide bookkeeping (specs, indexes) uses this directly.
    pub(crate) fn raw(&self, name: &str) -> Collection<Document> {
        self.db.collection(&format!("{}{name}", self.prefix))
    }

    /// The scoped handle to a port or driver collection.
    pub(crate) fn scoped(&self, name: &str, scope: &Scope) -> ScopedCollection {
        ScopedCollection::new(self.raw(name), scope.clone())
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("prefix", &self.prefix)
            .field("transactions", &self.transactions)
            .finish_non_exhaustive()
    }
}

/// A [`StorageBackend`] on MongoDB.
///
/// Every scope shares the database; each record carries a `_scope` field that
/// every query, write and index includes. [`StorageBackend::database`] maps a
/// name to the collection prefix `<name>:`.
///
/// ```no_run
/// # async fn demo() -> tinystoragedrivers_core::Result<()> {
/// use tinystoragedrivers_core::{Scope, StorageBackend};
/// use tinystoragedrivers_mongodb::MongoStorage;
///
/// let storage = MongoStorage::connect("mongodb://db.internal/openhuman", "openhuman").await?;
/// let alice = storage.database("approvals")?.for_scope(&Scope::new("alice")?)?;
/// assert_eq!(alice.driver(), "mongodb");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MongoStorage {
    pub(crate) shared: Arc<Shared>,
    pub(crate) databases: Arc<Mutex<BTreeMap<String, Arc<Shared>>>>,
}

impl MongoStorage {
    /// Connect to `uri` and use the database `database`.
    ///
    /// Connecting runs `hello` once, so an unreachable server fails here, and
    /// its reply decides whether the backend reports
    /// [`Capability::Transactions`] (replica sets and sharded clusters only).
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](tinystoragedrivers_core::ErrorKind::InvalidInput)
    /// for a malformed URI or database name,
    /// [`ErrorKind::Unavailable`](tinystoragedrivers_core::ErrorKind::Unavailable)
    /// when no server answers. Messages never include the URI.
    pub async fn connect(uri: &str, database: &str) -> Result<Self> {
        validate_mongo_database(database)?;
        let mut options = ClientOptions::parse(uri).await.map_err(|error| {
            let mapped = errors::map(error, "parse the connection string");
            if mapped.is_retryable() {
                mapped
            } else {
                StorageError::invalid_input("the MongoDB connection string is malformed")
            }
        })?;
        if options.app_name.is_none() {
            options.app_name = Some("tinystoragedrivers".to_owned());
        }
        let client =
            Client::with_options(options).map_err(errors::failed("configure the client"))?;
        let hello = client
            .database("admin")
            .run_command(doc! {"hello": 1})
            .await
            .map_err(errors::failed("reach the server"))?;
        // Admission and payload reads must observe the primary.
        let db = client.database_with_options(
            database,
            mongodb::options::DatabaseOptions::builder()
                .selection_criteria(mongodb::options::SelectionCriteria::ReadPreference(
                    mongodb::options::ReadPreference::Primary,
                ))
                .build(),
        );
        let shared = Shared::new(
            client,
            db,
            String::new(),
            Arc::new(system_clock),
            supports_transactions(&hello),
            supports_transactions(&hello),
        );
        Ok(Self {
            shared: Arc::new(shared),
            databases: Arc::default(),
        })
    }

    /// Read time from `clock` (milliseconds since the Unix epoch) instead of
    /// the system clock, so document expiry can be driven deterministically.
    #[must_use]
    pub fn with_clock(self, clock: Clock) -> Self {
        self.rebuild(clock, self.shared.transactions)
    }

    /// Disable public transactional capabilities, as if the deployment were a
    /// standalone server: [`Capability::Transactions`] is not reported and
    /// [`atomic_batch`](tinystoragedrivers_core::DocumentStore::atomic_batch)
    /// returns `Unsupported`. Internal document and stream writes still use native
    /// metadata transactions when the server supports them, so these handles
    /// cannot bypass another handle's read validation. A true standalone
    /// deployment keeps its existing single-document writes.
    #[must_use]
    pub fn without_transactions(self) -> Self {
        let clock = Arc::clone(&self.shared.clock);
        self.rebuild(clock, false)
    }

    fn rebuild(&self, clock: Clock, transactions: bool) -> Self {
        let shared = Shared::new(
            self.shared.client.clone(),
            self.shared.db.clone(),
            self.shared.prefix.clone(),
            clock,
            transactions,
            self.shared.physical_transactions,
        );
        Self {
            shared: Arc::new(shared),
            databases: Arc::default(),
        }
    }

    /// Remove the documents of `collection` in `scope` whose expiry time has
    /// passed. Reads already treat them as absent; this reclaims the space.
    /// Their versions are remembered, so a recreated id continues from them.
    ///
    /// # Errors
    ///
    /// An invalid collection name, or a backend error.
    pub async fn sweep_expired(&self, scope: &Scope, collection: &str) -> Result<u64> {
        self.sweep_expired_generation(
            scope,
            tinystoragedrivers_core::ScopeGeneration::default(),
            collection,
        )
        .await
    }

    /// Reclaim expired documents through an explicitly issued scope generation.
    ///
    /// # Errors
    /// Invalid collection, retired generation, pending reset or backend failure.
    pub async fn sweep_expired_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
        collection: &str,
    ) -> Result<u64> {
        crate::purge::check(&self.shared, scope, generation, None).await?;
        validate_collection(collection)?;
        let docs = MongoDocuments::new(Arc::clone(&self.shared), scope.clone(), None);
        if !self.shared.physical_transactions {
            return docs.sweep(collection, None).await;
        }
        self.shared.prepare_validation(Some(scope)).await?;
        crate::fence::in_write!(&self.shared, None, session => {
            self.shared.hold_validation(Some(scope), &mut *session).await?;
            crate::purge::check(&self.shared, scope, generation, Some(&mut *session)).await?;
            self.shared.spec_in_session(collection, &mut *session).await?;
            docs.sweep(collection, Some(session)).await
        })
    }
}

impl MongoStorage {
    fn handles(&self, scope: &Scope, fence: Option<&Arc<Fencing>>) -> ScopedStorage {
        self.handles_generation(
            scope,
            fence,
            tinystoragedrivers_core::ScopeGeneration::default(),
        )
    }

    fn handles_generation(
        &self,
        scope: &Scope,
        fence: Option<&Arc<Fencing>>,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(
                MongoDocuments::new(Arc::clone(&self.shared), scope.clone(), fence.cloned())
                    .with_generation(generation),
            ),
            Arc::new(
                MongoStreams::new(Arc::clone(&self.shared), scope.clone(), fence.cloned())
                    .with_generation(generation),
            ),
            Arc::new(
                MongoBlobs::new(Arc::clone(&self.shared), scope.clone(), fence.is_some())
                    .with_generation(generation),
            ),
        )
        .with_purge(Arc::new(crate::purge::MongoPurge {
            shared: Arc::clone(&self.shared),
            scope: scope.clone(),
            generation,
            fenced: fence.is_some(),
            #[cfg(test)]
            fail_after_chunks: false,
        }))
    }
}

impl fmt::Debug for MongoStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoStorage")
            .field("shared", &self.shared)
            .finish_non_exhaustive()
    }
}

impl StorageBackend for MongoStorage {
    fn driver(&self) -> &'static str {
        "mongodb"
    }

    fn capabilities(&self) -> Capabilities {
        let base = Capabilities::none()
            .with(Capability::FullText)
            .with(Capability::Ttl)
            .with(Capability::CollectionEnumeration);
        let base = if self.shared.physical_transactions {
            base.with(Capability::Watch)
        } else {
            base
        };
        if self.shared.transactions {
            base.with(Capability::Transactions)
                .with(Capability::ValidatedBatch)
                .with(Capability::ConditionalStreamAppend)
                .with(Capability::Fencing)
                .with(Capability::CrossDatabaseFencing)
                .with(Capability::ScopePurge)
        } else {
            base
        }
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Result<ScopedStorage> {
        if !self.shared.physical_transactions && generation.0 != 0 {
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "standalone cannot bind reset generations",
            ));
        }
        Ok(self.handles_generation(scope, None, generation))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::Fencing,
                "fencing needs transactions, which this MongoDB deployment lacks",
            ));
        }
        fence.validate()?;
        Ok(self.handles(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
                shared: Arc::clone(&self.shared),
            })),
        ))
    }

    fn for_scope_fenced_in_database(
        &self,
        scope: &Scope,
        fence: &Fence,
        locator: &GuardDatabase,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        locator.validate()?;
        if *locator == GuardDatabase::Current {
            return self.for_scope_fenced(scope, fence);
        }
        if !self.shared.transactions {
            return Err(StorageError::unsupported(
                Capability::CrossDatabaseFencing,
                "cross-database fencing needs transactions",
            ));
        }
        let guard = match locator {
            GuardDatabase::Default => Arc::new(Shared::new(
                self.shared.client.clone(),
                self.shared.db.clone(),
                String::new(),
                Arc::clone(&self.shared.clock),
                true,
                self.shared.physical_transactions,
            )),
            GuardDatabase::Named(name) => self.named(name)?,
            GuardDatabase::Current => unreachable!(),
        };
        Ok(self.handles(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
                shared: guard,
            })),
        ))
    }

    fn for_scope_fenced_in_database_generation(
        &self,
        scope: &Scope,
        generation: tinystoragedrivers_core::ScopeGeneration,
        fence: &Fence,
        locator: &GuardDatabase,
    ) -> Result<ScopedStorage> {
        // Validate and resolve the guard using the existing native binding contract.
        self.for_scope_fenced_in_database(scope, fence, locator)?;
        let shared = match locator {
            GuardDatabase::Current => Arc::clone(&self.shared),
            GuardDatabase::Default => Arc::new(Shared::new(
                self.shared.client.clone(),
                self.shared.db.clone(),
                String::new(),
                Arc::clone(&self.shared.clock),
                true,
                self.shared.physical_transactions,
            )),
            GuardDatabase::Named(name) => self.named(name)?,
        };
        Ok(self.handles_generation(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
                shared,
            })),
            generation,
        ))
    }

    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        Ok(Arc::new(Self {
            shared: self.named(name)?,
            databases: Arc::clone(&self.databases),
        }))
    }
}

impl MongoStorage {
    /// Typed fixture access to the same logical namespace as `database(name)`.
    /// Kept test-only so fixture isolation does not expand the production API.
    #[cfg(test)]
    pub(crate) fn fixture_named(&self, name: &str) -> Result<Self> {
        Ok(Self {
            shared: self.named(name)?,
            databases: Arc::clone(&self.databases),
        })
    }

    pub(crate) fn named(&self, name: &str) -> Result<Arc<Shared>> {
        validate_database(name)?;
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| StorageError::backend("mongodb database registry lock poisoned"))?;
        Ok(Arc::clone(databases.entry(name.to_owned()).or_insert_with(
            || {
                Arc::new(Shared::new(
                    self.shared.client.clone(),
                    self.shared.db.clone(),
                    database_prefix(name),
                    Arc::clone(&self.shared.clock),
                    self.shared.transactions,
                    self.shared.physical_transactions,
                ))
            },
        )))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
