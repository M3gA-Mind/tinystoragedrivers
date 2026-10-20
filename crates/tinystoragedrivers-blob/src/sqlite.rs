//! SQLite blob adapter over the shared connection and schema.

use std::{ops::Range, time::Duration};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tinystoragedrivers_api::BlobStore;
use tinystoragedrivers_connect::{
    Clock,
    sqlite::{
        SqliteConnection,
        rusqlite::{OptionalExtension, params},
    },
};
use tinystoragedrivers_types::{
    Blob, BlobListEntry, BlobListPage, BlobMeta, Capabilities, Capability,
    MAX_BLOB_LIST_PAGE_ITEMS, Result, Scope, StorageError,
};

use crate::key::{MAX_KEY_LEN, clamp_range, validate_key, validate_prefix, validate_sealed_key};

/// A scope-bound blob handle over a shared SQLite database.
#[derive(Debug, Clone)]
pub struct SqliteBlobStore {
    connection: SqliteConnection,
    scope: Scope,
}

impl SqliteBlobStore {
    /// Bind a shared database connection to one scope.
    #[must_use]
    pub fn new(connection: SqliteConnection, scope: Scope) -> Self {
        Self { connection, scope }
    }

    fn now(clock: &Clock) -> u64 {
        clock()
    }
}

#[async_trait]
impl BlobStore for SqliteBlobStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Blobs)
            .with(Capability::BlobTtl)
            .with(Capability::BlobListing)
    }
    fn max_key_len(&self) -> usize {
        MAX_KEY_LEN
    }
    fn supports_sealed_blobs(&self) -> bool {
        true
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, false).await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, Some(ttl), false).await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_key(key)?;
        let scope = self.scope.as_str().to_owned();
        let key = key.to_owned();
        let table = self.connection.tables().blobs().to_owned();
        let now = Self::now(&self.connection.clock());
        self.connection.run(move |connection| {
            let sql = format!("SELECT content_type, size, expires_ms, bytes FROM \"{table}\" WHERE scope=?1 AND key=?2");
            let row = connection.query_row(&sql, params![scope, key], |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<i64>>(2)?, row.get::<_, Vec<u8>>(3)?))).optional().map_err(sql_error)?;
            let Some((content_type, size, expires, bytes)) = row else { return Ok(None) };
            if expires.is_some_and(|expires| u64::try_from(expires).unwrap_or(0) <= now) { return Ok(None); }
            Ok(Some(Blob { meta: BlobMeta { key, len: u64::try_from(size).unwrap_or(bytes.len() as u64), content_type }, bytes }))
        }).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        clamp_range(0, range.start, range.end)?;
        let Some(blob) = self.get(key).await? else {
            return Ok(None);
        };
        Ok(Some(
            blob.bytes[clamp_range(blob.bytes.len(), range.start, range.end)?].to_vec(),
        ))
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_key(key)?;
        let scope = self.scope.as_str().to_owned();
        let key = key.to_owned();
        let table = self.connection.tables().blobs().to_owned();
        let now = Self::now(&self.connection.clock());
        self.connection.run(move |connection| {
            let sql = format!("SELECT content_type, size, expires_ms FROM \"{table}\" WHERE scope=?1 AND key=?2");
            let row = connection.query_row(&sql, params![scope, key], |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<i64>>(2)?))).optional().map_err(sql_error)?;
            Ok(row.and_then(|(content_type, size, expires)| (expires.is_none_or(|expires| u64::try_from(expires).unwrap_or(0) > now)).then(|| BlobMeta { key, len: u64::try_from(size).unwrap_or(0), content_type })))
        }).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        validate_key(key)?;
        let scope = self.scope.as_str().to_owned();
        let key = key.to_owned();
        let blobs = self.connection.tables().blobs().to_owned();
        let written = self.connection.tables().blob_written().to_owned();
        let now = Self::now(&self.connection.clock());
        self.connection
            .run(move |connection| {
                let transaction = connection.transaction().map_err(sql_error)?;
                let query = format!("SELECT expires_ms FROM \"{blobs}\" WHERE scope=?1 AND key=?2");
                let expires: Option<Option<i64>> = transaction
                    .query_row(&query, params![scope, key], |row| row.get(0))
                    .optional()
                    .map_err(sql_error)?;
                let delete = format!("DELETE FROM \"{blobs}\" WHERE scope=?1 AND key=?2");
                transaction
                    .execute(&delete, params![scope, key])
                    .map_err(sql_error)?;
                let delete_age = format!("DELETE FROM \"{written}\" WHERE scope=?1 AND key=?2");
                transaction
                    .execute(&delete_age, params![scope, key])
                    .map_err(sql_error)?;
                transaction.commit().map_err(sql_error)?;
                Ok(expires
                    .flatten()
                    .is_none_or(|expires| u64::try_from(expires).unwrap_or(0) > now)
                    && expires.is_some())
            })
            .await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_prefix(prefix, None)?;
        let scope = self.scope.as_str().to_owned();
        let prefix = prefix.to_owned();
        let table = self.connection.tables().blobs().to_owned();
        let now = Self::now(&self.connection.clock());
        self.connection.run(move |connection| {
            let sql = format!("SELECT key, content_type, size FROM \"{table}\" WHERE scope=?1 AND substr(key,1,length(?2))=?2 AND substr(key,1,12)!='_tsd/sealed/' AND (expires_ms IS NULL OR expires_ms>?3) ORDER BY key COLLATE BINARY");
            let mut statement = connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement.query_map(params![scope, prefix, to_i64(now)], |row| Ok(BlobMeta { key: row.get(0)?, content_type: row.get(1)?, len: u64::try_from(row.get::<_, i64>(2)?).unwrap_or(0) })).map_err(sql_error)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(sql_error)
        }).await
    }

    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, true).await
    }
    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        validate_sealed_key(key)?;
        let scope = self.scope.as_str().to_owned();
        let key = key.to_owned();
        let table = self.connection.tables().blobs().to_owned();
        let now = Self::now(&self.connection.clock());
        self.connection.run(move |connection| {
            let sql = format!("SELECT content_type,size,expires_ms,bytes FROM \"{table}\" WHERE scope=?1 AND key=?2");
            let row = connection.query_row(&sql, params![scope, key], |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?, row.get::<_, Option<i64>>(2)?, row.get::<_, Vec<u8>>(3)?))).optional().map_err(sql_error)?;
            let Some((content_type,size,expires,bytes)) = row else { return Ok(None) };
            if expires.is_some_and(|expires| u64::try_from(expires).unwrap_or(0) <= now) { return Ok(None); }
            Ok(Some(Blob { meta: BlobMeta { key, len: u64::try_from(size).unwrap_or(bytes.len() as u64), content_type }, bytes }))
        }).await
    }
    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_sealed_key(key)?;
        let scope = self.scope.as_str().to_owned();
        let key = key.to_owned();
        let table = self.connection.tables().blobs().to_owned();
        let now = Self::now(&self.connection.clock());
        self.connection
            .run(move |connection| {
                let sql = format!(
                    "SELECT content_type,size,expires_ms FROM \"{table}\" WHERE scope=?1 AND key=?2"
                );
                let row = connection
                    .query_row(&sql, params![scope, key], |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                        ))
                    })
                    .optional()
                    .map_err(sql_error)?;
                Ok(row.and_then(|(content_type, size, expires)| {
                    expires
                        .is_none_or(|expires| u64::try_from(expires).unwrap_or(0) > now)
                        .then(|| BlobMeta {
                            key,
                            len: u64::try_from(size).unwrap_or(0),
                            content_type,
                        })
                }))
            })
            .await
    }
    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        validate_sealed_key(key)?;
        let scope = self.scope.as_str().to_owned();
        let key = key.to_owned();
        let blobs = self.connection.tables().blobs().to_owned();
        let written = self.connection.tables().blob_written().to_owned();
        let now = Self::now(&self.connection.clock());
        self.connection
            .run(move |connection| {
                let tx = connection.transaction().map_err(sql_error)?;
                let q = format!("SELECT expires_ms FROM \"{blobs}\" WHERE scope=?1 AND key=?2");
                let expires: Option<Option<i64>> = tx
                    .query_row(&q, params![scope, key], |row| row.get(0))
                    .optional()
                    .map_err(sql_error)?;
                let d = format!("DELETE FROM \"{blobs}\" WHERE scope=?1 AND key=?2");
                tx.execute(&d, params![scope, key]).map_err(sql_error)?;
                let a = format!("DELETE FROM \"{written}\" WHERE scope=?1 AND key=?2");
                tx.execute(&a, params![scope, key]).map_err(sql_error)?;
                tx.commit().map_err(sql_error)?;
                Ok(expires
                    .flatten()
                    .is_none_or(|e| u64::try_from(e).unwrap_or(0) > now)
                    && expires.is_some())
            })
            .await
    }
    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_sealed_key(prefix)?;
        let scope = self.scope.as_str().to_owned();
        let prefix = prefix.to_owned();
        let table = self.connection.tables().blobs().to_owned();
        let now = to_i64(Self::now(&self.connection.clock()));
        self.connection.run(move |connection| {
            let sql=format!("SELECT key,content_type,size FROM \"{table}\" WHERE scope=?1 AND substr(key,1,length(?2))=?2 AND (expires_ms IS NULL OR expires_ms>?3) ORDER BY key COLLATE BINARY");
            let mut st=connection.prepare(&sql).map_err(sql_error)?;
            let rows=st.query_map(params![scope,prefix,now],|row|Ok(BlobMeta{key:row.get(0)?,content_type:row.get(1)?,len:u64::try_from(row.get::<_,i64>(2)?).unwrap_or(0)})).map_err(sql_error)?;
            rows.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)
        }).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobListPage> {
        if limit == 0 || limit > MAX_BLOB_LIST_PAGE_ITEMS {
            return Err(StorageError::invalid_input(
                "invalid blob listing page size",
            ));
        }
        validate_prefix(prefix, after)?;
        let scope = self.scope.as_str().to_owned();
        let prefix = prefix.to_owned();
        let blobs = self.connection.tables().blobs().to_owned();
        let written = self.connection.tables().blob_written().to_owned();
        let after = after.map(str::to_owned);
        self.connection.run(move |connection| {
            let sql = format!("SELECT b.key,b.content_type,b.size,w.written_ms FROM \"{blobs}\" b LEFT JOIN \"{written}\" w ON w.scope=b.scope AND w.key=b.key WHERE b.scope=?1 AND substr(b.key,1,length(?2))=?2 AND substr(b.key,1,12)!='_tsd/sealed/' AND (?3 IS NULL OR b.key COLLATE BINARY > ?3 COLLATE BINARY) ORDER BY b.key COLLATE BINARY LIMIT ?4");
            let mut statement = connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement.query_map(params![scope, prefix, after, i64::try_from(limit + 1).unwrap_or(i64::MAX)], |row| {
                Ok(BlobListEntry { meta: BlobMeta { key: row.get(0)?, content_type: row.get(1)?, len: u64::try_from(row.get::<_, i64>(2)?).unwrap_or(0) }, written_at_ms: row.get::<_, Option<i64>>(3)?.and_then(|age| u64::try_from(age).ok()) })
            }).map_err(sql_error)?;
            let mut items = rows.collect::<std::result::Result<Vec<_>, _>>().map_err(sql_error)?;
            let more = items.len() > limit;
            items.truncate(limit);
            let next_after = if more { items.last().map(|item| item.meta.key.clone()) } else { None };
            Ok(BlobListPage { items, next_after })
        }).await
    }
    async fn sweep_expired(&self) -> Result<u64> {
        let scope = self.scope.as_str().to_owned();
        let blobs = self.connection.tables().blobs().to_owned();
        let written = self.connection.tables().blob_written().to_owned();
        let now = to_i64(Self::now(&self.connection.clock()));
        self.connection.run(move |connection| {
            let transaction = connection.transaction().map_err(sql_error)?;
            let select = format!("SELECT key FROM \"{blobs}\" WHERE scope=?1 AND expires_ms IS NOT NULL AND expires_ms<=?2");
            let keys = {
                let mut statement = transaction.prepare(&select).map_err(sql_error)?;
                statement.query_map(params![scope, now], |row| row.get::<_, String>(0)).map_err(sql_error)?
                    .collect::<std::result::Result<Vec<_>, _>>().map_err(sql_error)?
            };
            let remove = format!("DELETE FROM \"{blobs}\" WHERE scope=?1 AND expires_ms IS NOT NULL AND expires_ms<=?2");
            transaction.execute(&remove, params![scope, now]).map_err(sql_error)?;
            let remove_age = format!("DELETE FROM \"{written}\" WHERE scope=?1 AND key=?2");
            for key in &keys { transaction.execute(&remove_age, params![scope, key]).map_err(sql_error)?; }
            transaction.commit().map_err(sql_error)?;
            Ok(keys.len() as u64)
        }).await
    }
}

impl SqliteBlobStore {
    async fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
        allow_sealed: bool,
    ) -> Result<BlobMeta> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let scope = self.scope.as_str().to_owned();
        let key = key.to_owned();
        let content_type = content_type.map(str::to_owned);
        let blobs = self.connection.tables().blobs().to_owned();
        let written = self.connection.tables().blob_written().to_owned();
        let now = Self::now(&self.connection.clock());
        let expires =
            ttl.map(|ttl| now.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)));
        let hash = Sha256::digest(&bytes).to_vec();
        let len = bytes.len() as u64;
        self.connection.run(move |connection| {
            let transaction = connection.transaction().map_err(sql_error)?;
            let put = format!("INSERT OR REPLACE INTO \"{blobs}\" (scope,key,content_type,size,sha256,expires_ms,bytes) VALUES (?1,?2,?3,?4,?5,?6,?7)");
            transaction.execute(&put, params![scope, key, content_type, i64::try_from(len).unwrap_or(i64::MAX), hash, expires.map(to_i64), bytes]).map_err(sql_error)?;
            let age = format!("INSERT OR REPLACE INTO \"{written}\" (scope,key,written_ms) VALUES (?1,?2,?3)");
            transaction.execute(&age, params![scope, key, to_i64(now)]).map_err(sql_error)?;
            transaction.commit().map_err(sql_error)?;
            Ok(BlobMeta { key, len, content_type })
        }).await
    }
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn sql_error(error: tinystoragedrivers_connect::sqlite::rusqlite::Error) -> StorageError {
    StorageError::backend("SQLite blob operation failed").with_source(error)
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
