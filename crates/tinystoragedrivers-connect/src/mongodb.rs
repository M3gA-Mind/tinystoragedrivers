//! Shared MongoDB client and primary-read database handles.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mongodb::bson::{Document, doc};
use mongodb::options::{ClientOptions, DatabaseOptions, ReadPreference, SelectionCriteria};
use mongodb::{Client, Database};
use tinystoragedrivers_types::{Result, StorageError};

use crate::Clock;

/// MongoDB connection requirements applied when connecting.
#[derive(Debug, Clone, Copy, Default)]
pub struct MongoConnectionOptions {
    /// Require a deployment that supports multi-document transactions.
    pub require_transactions: bool,
}

/// A client and database shared by primitive adapters for one MongoDB URL.
#[derive(Clone)]
pub struct MongoConnection {
    client: Client,
    database: Database,
    clock: Clock,
    transactions: bool,
    options: MongoConnectionOptions,
}

impl std::fmt::Debug for MongoConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoConnection")
            .field("database", &self.database.name())
            .field("transactions", &self.transactions)
            .finish_non_exhaustive()
    }
}

impl MongoConnection {
    /// Connect to a MongoDB URI and select `database` with primary reads.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a malformed URI or database name, and an
    /// unavailable/backend error if the server cannot be reached.
    pub async fn connect(uri: &str, database: &str) -> Result<Self> {
        Self::connect_with_options(uri, database, MongoConnectionOptions::default()).await
    }

    /// Connect while applying explicit transaction requirements.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a malformed URI or database name, and an
    /// unavailable/backend error if the server cannot be reached or does not
    /// meet the requested transaction requirement.
    pub async fn connect_with_options(
        uri: &str,
        database: &str,
        options: MongoConnectionOptions,
    ) -> Result<Self> {
        validate_database(database)?;
        let mut client_options = ClientOptions::parse(uri).await.map_err(|error| {
            let _ = error;
            StorageError::invalid_input("the MongoDB connection string is malformed")
        })?;
        if client_options.app_name.is_none() {
            client_options.app_name = Some("tinystoragedrivers".to_owned());
        }
        let client = Client::with_options(client_options)
            .map_err(|error| map_error("configure the client", error))?;
        let hello = client
            .database("admin")
            .run_command(doc! {"hello": 1})
            .await
            .map_err(|error| map_error("reach the server", error))?;
        let transactions = supports_transactions(&hello);
        if options.require_transactions && !transactions {
            return Err(StorageError::invalid_input(
                "the MongoDB deployment does not support transactions",
            ));
        }
        let database = client.database_with_options(
            database,
            DatabaseOptions::builder()
                .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
                .build(),
        );
        Ok(Self {
            client,
            database,
            clock: Arc::new(system_clock),
            transactions,
            options,
        })
    }

    /// Return a clone of the shared MongoDB client.
    #[must_use]
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Return a clone of the selected database handle.
    #[must_use]
    pub fn database(&self) -> Database {
        self.database.clone()
    }

    /// Whether the connected deployment supports multi-document transactions.
    #[must_use]
    pub const fn transactions_available(&self) -> bool {
        self.transactions
    }

    /// Return the connection's shared clock.
    #[must_use]
    pub fn clock(&self) -> Clock {
        Arc::clone(&self.clock)
    }

    /// Return a connection clone with a deterministic clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Return the requirements used when the connection was opened.
    #[must_use]
    pub const fn options(&self) -> MongoConnectionOptions {
        self.options
    }
}

fn supports_transactions(hello: &Document) -> bool {
    let sessions = hello.contains_key("logicalSessionTimeoutMinutes");
    let replica_set = hello.get_str("setName").is_ok();
    let router = hello.get_str("msg").is_ok_and(|msg| msg == "isdbgrid");
    sessions && (replica_set || router)
}

fn validate_database(database: &str) -> Result<()> {
    if database.is_empty()
        || database.len() > 63
        || database.chars().any(|character| {
            character.is_whitespace() || matches!(character, '/' | '\\' | '.' | '"' | '$')
        })
    {
        return Err(StorageError::invalid_input("invalid MongoDB database name"));
    }
    Ok(())
}

fn map_error(operation: &'static str, error: impl std::error::Error + Send + Sync + 'static) -> StorageError {
    StorageError::unavailable(format!("MongoDB failed to {operation}")).with_source(error)
}

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}
