//! SQLite KV adapter over the shared database connection.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_connect::sqlite::{SqliteConnection, rusqlite};
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Meta, Page, Precondition, Result, Scope, StorageError,
    Version, Versioned,
};

use crate::common::{
    check_precondition, cursor_key, duration_millis, from_sqlite_version, next_version,
    validate_key, validate_limit, validate_prefix, version_to_i64,
};

const TABLE: &str = "_tsd_kv";

#[derive(Debug)]
struct Record {
    version: Version,
    expires_ms: Option<i64>,
    value: Option<Value>,
}

/// SQLite key/value engine sharing a connection with the other local engines.
#[derive(Debug, Clone)]
pub struct SqliteKv {
    connection: SqliteConnection,
    scope: String,
}

impl SqliteKv {
    /// Create the native KV table on `connection` and bind this adapter to
    /// `scope`.
    ///
    /// # Errors
    /// Returns an SQLite schema or connection error.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(connection: SqliteConnection, scope: Scope) -> Result<Self> {
        connection.run_now(|conn| {
            conn.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {TABLE} (
                    scope TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    expires_ms INTEGER,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    value TEXT,
                    PRIMARY KEY (scope, key)
                ) WITHOUT ROWID"
            ))
            .map_err(tinystoragedrivers_connect::sqlite::error::during(
                "create KV table",
            ))
        })?;
        Ok(Self {
            connection,
            scope: scope.as_str().to_owned(),
        })
    }

    fn now_ms(&self) -> i64 {
        i64::try_from((self.connection.clock())()).unwrap_or(i64::MAX)
    }
}

#[async_trait]
impl KvStore for SqliteKv {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Kv)
            .with(Capability::Ttl)
            .with(Capability::SharedAcrossProcesses)
            .with(Capability::CrossProcessCas)
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        validate_key(key)?;
        let connection = self.connection.clone();
        let scope = self.scope.clone();
        let key = key.to_owned();
        let now = self.now_ms();
        connection
            .run(move |conn| {
                let record = read_record(conn, &scope, &key)?;
                Ok(record.filter(|record| record.live(now)).and_then(|record| {
                    record.value.map(|value| Versioned {
                        id: key,
                        version: record.version,
                        doc: value,
                        meta: Meta::unknown(),
                    })
                }))
            })
            .await
    }

    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.write(key, value, Precondition::None, None).await
    }

    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.write(key, value, expected, None).await
    }

    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        validate_key(key)?;
        let connection = self.connection.clone();
        let scope = self.scope.clone();
        let key = key.to_owned();
        let now = self.now_ms();
        connection
            .run(move |conn| {
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(tinystoragedrivers_connect::sqlite::error::during("begin KV delete"))?;
                let old = read_record(&tx, &scope, &key)?;
                let current = old.as_ref().filter(|record| record.live(now)).map(|r| r.version);
                check_precondition(expected, current)?;
                let Some(current) = current else {
                    return Ok(false);
                };
                let version = version_to_i64(next_version(Some(current))?)?;
                tx.execute(
                    &format!("UPDATE {TABLE} SET version=?3, expires_ms=NULL, deleted=1, value=NULL WHERE scope=?1 AND key=?2"),
                    rusqlite::params![scope, key, version],
                )
                .map_err(tinystoragedrivers_connect::sqlite::error::during("delete a KV value"))?;
                tx.commit()
                    .map_err(tinystoragedrivers_connect::sqlite::error::during("commit KV delete"))?;
                Ok(true)
            })
            .await
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
        let connection = self.connection.clone();
        let scope = self.scope.clone();
        let prefix = prefix.to_owned();
        let now = self.now_ms();
        connection
            .run(move |conn| {
                let mut statement = conn
                    .prepare(&format!("SELECT key, version, expires_ms, deleted, value FROM {TABLE} WHERE scope=?1 AND key>=?2 ORDER BY key"))
                    .map_err(tinystoragedrivers_connect::sqlite::error::during("prepare KV listing"))?;
                let rows = statement
                    .query_map(rusqlite::params![scope, prefix], |row| {
                        let key: String = row.get(0)?;
                        let version: i64 = row.get(1)?;
                        let expires_ms: Option<i64> = row.get(2)?;
                        let deleted: bool = row.get(3)?;
                        let value: Option<String> = row.get(4)?;
                        Ok((key, version, expires_ms, deleted, value))
                    })
                    .map_err(tinystoragedrivers_connect::sqlite::error::during("list KV values"))?;
                let mut found = Vec::new();
                for row in rows {
                    let (key, version, expires_ms, deleted, value) = row
                        .map_err(tinystoragedrivers_connect::sqlite::error::during("read a KV value"))?;
                    if !key.starts_with(&prefix)
                        || after.as_ref().is_some_and(|last| key <= *last)
                        || deleted
                        || expires_ms.is_some_and(|expires| expires <= now)
                    {
                        continue;
                    }
                    let value = value.ok_or_else(|| StorageError::backend("sqlite KV value is missing"))?;
                    found.push(Versioned {
                        id: key,
                        version: from_sqlite_version(version)?,
                        doc: serde_json::from_str(&value)?,
                        meta: Meta::unknown(),
                    });
                    if found.len() > limit {
                        break;
                    }
                }
                let has_more = found.len() > limit;
                if has_more {
                    found.pop();
                }
                let next = if has_more {
                    found.last().map(|item| crate::common::make_cursor(&item.id, &prefix)).transpose()?
                } else {
                    None
                };
                Ok(Page { items: found, next })
            })
            .await
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

impl SqliteKv {
    async fn write(
        &self,
        key: &str,
        value: Value,
        expected: Precondition,
        expires_ms: Option<i64>,
    ) -> Result<Version> {
        validate_key(key)?;
        let encoded = serde_json::to_string(&value)?;
        let connection = self.connection.clone();
        let scope = self.scope.clone();
        let key = key.to_owned();
        let now = self.now_ms();
        connection
            .run(move |conn| {
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(tinystoragedrivers_connect::sqlite::error::during("begin KV write"))?;
                let old = read_record(&tx, &scope, &key)?;
                let current = old.as_ref().filter(|record| record.live(now)).map(|r| r.version);
                check_precondition(expected, current)?;
                let version = version_to_i64(next_version(old.map(|record| record.version))?)?;
                tx.execute(
                    &format!("INSERT INTO {TABLE}(scope,key,version,expires_ms,deleted,value) VALUES (?1,?2,?3,?4,0,?5) ON CONFLICT(scope,key) DO UPDATE SET version=excluded.version, expires_ms=excluded.expires_ms, deleted=0, value=excluded.value"),
                    rusqlite::params![scope, key, version, expires_ms, encoded],
                )
                .map_err(tinystoragedrivers_connect::sqlite::error::during("write a KV value"))?;
                tx.commit()
                    .map_err(tinystoragedrivers_connect::sqlite::error::during("commit KV write"))?;
                Ok(Version(u64::try_from(version).map_err(|_| {
                    StorageError::backend("key version space is exhausted")
                })?))
            })
            .await
    }
}

impl Record {
    fn live(&self, now: i64) -> bool {
        self.value.is_some() && self.expires_ms.is_none_or(|expires_ms| expires_ms > now)
    }
}

fn read_record(conn: &rusqlite::Connection, scope: &str, key: &str) -> Result<Option<Record>> {
    use rusqlite::OptionalExtension;
    conn.query_row(
        &format!(
            "SELECT version, expires_ms, deleted, value FROM {TABLE} WHERE scope=?1 AND key=?2"
        ),
        rusqlite::params![scope, key],
        |row| {
            let version: i64 = row.get(0)?;
            let expires_ms: Option<i64> = row.get(1)?;
            let deleted: bool = row.get(2)?;
            let value: Option<String> = row.get(3)?;
            Ok((version, expires_ms, deleted, value))
        },
    )
    .optional()
    .map_err(tinystoragedrivers_connect::sqlite::error::during(
        "read a KV value",
    ))?
    .map(|(version, expires_ms, deleted, value)| {
        let version = from_sqlite_version(version)?;
        let value = if deleted {
            None
        } else {
            value
                .map(|value| serde_json::from_str(&value))
                .transpose()?
        };
        Ok(Record {
            version,
            expires_ms,
            value,
        })
    })
    .transpose()
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
