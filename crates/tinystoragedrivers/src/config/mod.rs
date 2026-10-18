//! Choosing and opening a backend from a URL.
//!
//! A host reads one string from its configuration (`memory`,
//! `sqlite:/var/lib/app`, `mongodb://db.internal/openhuman`) and hands it to
//! [`StorageConfig::parse`] then [`open`]. A URL for a driver this build did
//! not compile in is rejected with the Cargo feature to enable, so a
//! misconfigured deployment fails at boot with an actionable message.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use tinystoragedrivers_core::{MemoryStorage, Result, StorageBackend, StorageError};

/// A parsed storage location.
///
/// ```
/// use tinystoragedrivers::StorageConfig;
///
/// assert_eq!(StorageConfig::parse("memory")?, StorageConfig::Memory);
/// let mongo = StorageConfig::parse("mongodb://app:hunter2@db.internal/openhuman")?;
/// assert_eq!(mongo.to_string(), "mongodb://app:***@db.internal/openhuman");
/// # Ok::<(), tinystoragedrivers::StorageError>(())
/// ```
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageConfig {
    /// Process memory; nothing survives a restart.
    Memory,
    /// SQLite. A path ending in `.db` or `.sqlite` is one database file; any
    /// other path is a directory holding the one file `storage.db`.
    Sqlite {
        /// File or directory.
        path: PathBuf,
    },
    /// `MongoDB`, one database shared by every scope.
    MongoDb {
        /// The connection string, credentials included.
        uri: String,
        /// The database name taken from the URI path.
        database: String,
    },
    /// JSON and JSONL files under a directory.
    File {
        /// Root directory.
        dir: PathBuf,
    },
}

impl StorageConfig {
    /// Parse a storage URL.
    ///
    /// Accepted forms: `memory`; `sqlite:<path>`; a bare path (absolute, or
    /// starting with `./` or `../`), read as SQLite;
    /// `mongodb://…/<database>` and `mongodb+srv://…/<database>`;
    /// `file:<dir>`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// empty or unrecognized URL, an empty path, or a `MongoDB` URI without a
    /// database name.
    pub fn parse(url: &str) -> Result<Self> {
        let url = url.trim();
        if url.is_empty() {
            return Err(StorageError::invalid_input("storage URL is empty"));
        }
        if url == "memory" || url == "memory:" {
            return Ok(Self::Memory);
        }
        if url.starts_with("mongodb://") || url.starts_with("mongodb+srv://") {
            return Self::parse_mongo(url);
        }
        if let Some(path) = url.strip_prefix("sqlite:") {
            return Ok(Self::Sqlite {
                path: non_empty_path(path.strip_prefix("//").unwrap_or(path), "sqlite")?,
            });
        }
        if let Some(dir) = url.strip_prefix("file:") {
            return Ok(Self::File {
                dir: non_empty_path(dir.strip_prefix("//").unwrap_or(dir), "file")?,
            });
        }
        if url.starts_with('/') || url.starts_with("./") || url.starts_with("../") {
            return Ok(Self::Sqlite {
                path: PathBuf::from(url),
            });
        }
        Err(StorageError::invalid_input(format!(
            "unrecognized storage URL `{}`; expected memory, sqlite:<path>, mongodb://…/<db> or file:<dir>",
            redact_url(url)
        )))
    }

    fn parse_mongo(url: &str) -> Result<Self> {
        let path = split_uri(url, Boundary::BeforeQuery).map_or("", |parts| parts.path);
        let database = path.split('/').next().unwrap_or_default();
        if database.is_empty() {
            return Err(StorageError::invalid_input(format!(
                "MongoDB URL `{}` names no database; add one as the path, e.g. mongodb://host/openhuman",
                redact_url(url)
            )));
        }
        Ok(Self::MongoDb {
            uri: url.to_owned(),
            database: database.to_owned(),
        })
    }

    /// The driver this configuration selects.
    #[must_use]
    pub fn driver(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Sqlite { .. } => "sqlite",
            Self::MongoDb { .. } => "mongodb",
            Self::File { .. } => "file",
        }
    }
}

fn non_empty_path(path: &str, driver: &str) -> Result<PathBuf> {
    if path.is_empty() {
        Err(StorageError::invalid_input(format!(
            "{driver} storage URL has no path"
        )))
    } else {
        Ok(PathBuf::from(path))
    }
}

/// The pieces of `scheme://[userinfo@]hosts[/path][?query]`.
struct UriParts<'a> {
    scheme: &'a str,
    userinfo: Option<&'a str>,
    hosts: &'a str,
    path: &'a str,
    query: Option<&'a str>,
}

/// Split a connection URI without trusting the credentials to be
/// percent-encoded: the userinfo ends at the *last* `@` (before the query, or
/// anywhere, per `boundary`), so a stray `/` or `@` inside a password cannot
/// move the boundary.
fn split_uri(url: &str, boundary: Boundary) -> Option<UriParts<'_>> {
    let (scheme, rest) = url.split_once("://")?;
    let at = match boundary {
        Boundary::BeforeQuery => rest.split('?').next().and_then(|head| head.rfind('@')),
        Boundary::Anywhere => rest.rfind('@'),
    };
    let (userinfo, tail) = match at {
        Some(at) => (rest.get(..at), rest.get(at + 1..).unwrap_or_default()),
        None => (None, rest),
    };
    let (before_query, query) = match tail.split_once('?') {
        Some((head, query)) => (head, Some(query)),
        None => (tail, None),
    };
    let (hosts, path) = before_query.split_once('/').unwrap_or((before_query, ""));
    Some(UriParts {
        scheme,
        userinfo,
        hosts,
        path,
        query,
    })
}

/// Where the userinfo of a connection URI may end.
#[derive(Clone, Copy)]
enum Boundary {
    /// At the last `@` before the query: the parse of a well-formed URI,
    /// where an `@` in a query value (`appName=a@b`) stays in the query.
    BeforeQuery,
    /// At the last `@` anywhere: for redaction, so an unencoded `?`, `/` or
    /// `@` in a password can only make more text hidden, never less.
    Anywhere,
}

/// Query options whose values are credentials (AWS session tokens ride in
/// `authMechanismProperties`).
const SECRET_OPTIONS: [&str; 3] = [
    "authmechanismproperties",
    "tlscertificatekeyfilepassword",
    "password",
];

/// Hide credentials in a storage connection URI before displaying it.
///
/// The password and credential-valued query options are replaced with `***`.
/// Malformed URI input is returned unchanged because no safe credential
/// boundary can be inferred.
#[must_use]
pub fn redact_url(url: &str) -> String {
    let sanitized = redact_secret_options(url);
    let Some(parts) = split_uri(&sanitized, Boundary::Anywhere) else {
        return sanitized;
    };
    let mut out = format!("{}://", parts.scheme);
    if let Some(userinfo) = parts.userinfo {
        let user = userinfo.split_once(':').map_or(userinfo, |(user, _)| user);
        out.push_str(user);
        if userinfo.contains(':') {
            out.push_str(":***");
        }
        out.push('@');
    }
    out.push_str(parts.hosts);
    if !parts.path.is_empty() || parts.query.is_some() {
        out.push('/');
        out.push_str(parts.path);
    }
    if let Some(query) = parts.query {
        out.push('?');
        let options: Vec<String> = query
            .split('&')
            .map(|option| match option.split_once('=') {
                Some((key, _)) if SECRET_OPTIONS.contains(&key.to_ascii_lowercase().as_str()) => {
                    format!("{key}=***")
                }
                _ => option.to_owned(),
            })
            .collect();
        out.push_str(&options.join("&"));
    }
    out
}

/// Mask secret query options before parsing userinfo: an `@` in a query value
/// must never hide a later credential option from the parser.
fn redact_secret_options(url: &str) -> String {
    let mut output = String::with_capacity(url.len());
    let mut remaining = url;
    loop {
        let Some(question) = remaining.find('?') else {
            output.push_str(remaining);
            return output;
        };
        let (prefix, query) = remaining.split_at(question + 1);
        output.push_str(&prefix[..prefix.len() - 1]);
        output.push('?');
        let next_query = query.find('?').unwrap_or(query.len());
        let (options, suffix) = query.split_at(next_query);
        for (index, option) in options.split('&').enumerate() {
            if index > 0 {
                output.push('&');
            }
            match option.split_once('=') {
                Some((key, _)) if SECRET_OPTIONS.contains(&key.to_ascii_lowercase().as_str()) => {
                    output.push_str(key);
                    output.push_str("=***");
                }
                _ => output.push_str(option),
            }
        }
        remaining = suffix;
    }
}

impl fmt::Display for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("memory"),
            Self::Sqlite { path } => write!(f, "sqlite:{}", path.display()),
            Self::MongoDb { uri, .. } => f.write_str(&redact_url(uri)),
            Self::File { dir } => write!(f, "file:{}", dir.display()),
        }
    }
}

impl fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageConfig({self})")
    }
}

/// Open the backend `config` names.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) naming the
/// Cargo feature to enable when the driver is not compiled in; otherwise the
/// driver's own connection error.
#[allow(
    clippy::unused_async,
    reason = "the network drivers this dispatches to connect asynchronously"
)]
pub async fn open(config: &StorageConfig) -> Result<Arc<dyn StorageBackend>> {
    match config {
        StorageConfig::Memory => Ok(Arc::new(MemoryStorage::new())),
        #[cfg(feature = "file")]
        StorageConfig::File { dir } => Ok(Arc::new(tinystoragedrivers_file::FileStorage::open(
            dir.clone(),
        )?)),
        #[cfg(feature = "sqlite")]
        StorageConfig::Sqlite { path } => Ok(Arc::new(
            tinystoragedrivers_sqlite::SqliteStorage::open(path)?,
        )),
        #[cfg(feature = "mongodb")]
        StorageConfig::MongoDb { uri, database } => Ok(Arc::new(
            tinystoragedrivers_mongodb::MongoStorage::connect(uri, database).await?,
        )),
        #[allow(
            unreachable_patterns,
            reason = "reachable unless every driver feature is enabled"
        )]
        other => Err(StorageError::invalid_input(format!(
            "storage URL `{other}` needs the `{}` driver, which is not available in this build of tinystoragedrivers",
            other.driver()
        ))),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
