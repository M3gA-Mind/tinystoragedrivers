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
        let path = split_uri(url).map_or("", |parts| parts.path);
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
    path: &'a str,
}

/// Split a connection URI without trusting the credentials to be
/// percent-encoded: the userinfo ends at the last `@` before the query, so a
/// stray `/` or `@` inside a password cannot move the boundary.
fn split_uri(url: &str) -> Option<UriParts<'_>> {
    let (_, rest) = url.split_once("://")?;
    let at = rest.split('?').next().and_then(|head| head.rfind('@'));
    let tail = match at {
        Some(at) => rest.get(at + 1..).unwrap_or_default(),
        None => rest,
    };
    let before_query = tail.split('?').next().unwrap_or(tail);
    let (_, path) = before_query.split_once('/').unwrap_or((before_query, ""));
    Some(UriParts { path })
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
    let Some((scheme, rest)) = url.split_once("://") else {
        return redact_unqualified_query(url);
    };
    let (without_fragment, fragment) = split_fragment(rest);
    let (before_query, query) = match without_fragment.split_once('?') {
        Some((before, query)) => (before, Some(query)),
        None => (without_fragment, None),
    };
    let (authority, path) = match before_query.find('/') {
        Some(slash) => (&before_query[..slash], &before_query[slash..]),
        None => (before_query, ""),
    };
    let (userinfo, host) = authority
        .rsplit_once('@')
        .map_or((None, authority), |(userinfo, host)| (Some(userinfo), host));

    // If the authority cannot be separated safely (for example, an unescaped
    // '?' in userinfo makes a nonnumeric port), hide the whole URI. Never let
    // query or path delimiters retroactively create a userinfo boundary.
    if !valid_authority(host) {
        return format!("{scheme}://***");
    }

    let mut output = format!("{scheme}://");
    if let Some(userinfo) = userinfo {
        output.push_str(&redact_userinfo(userinfo));
        output.push('@');
    }
    output.push_str(host);
    if path.is_empty() && query.is_some() {
        output.push('/');
    } else {
        output.push_str(path);
    }
    if let Some(query) = query {
        output.push('?');
        output.push_str(&redact_query_options(query));
    }
    output.push_str(fragment);
    output
}

/// Return a URI fragment, including its marker, without interpreting any of
/// its contents as authority or query data.
fn split_fragment(value: &str) -> (&str, &str) {
    value
        .find('#')
        .map_or((value, ""), |marker| (&value[..marker], &value[marker..]))
}

/// A host authority may contain a hostname and optional numeric port, or a
/// bracketed IPv6 address with an optional numeric port.
fn valid_authority(authority: &str) -> bool {
    !authority.is_empty() && authority.split(',').all(valid_host_port)
}

fn valid_host_port(authority: &str) -> bool {
    if let Some(address) = authority.strip_prefix('[') {
        let Some(end) = address.find(']') else {
            return false;
        };
        let host = &address[..end];
        let port = &address[end + 1..];
        return !host.is_empty()
            && (port.is_empty()
                || port.strip_prefix(':').is_some_and(|port| {
                    !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
                }));
    }
    let host = match authority.rsplit_once(':') {
        Some((host, port)) => {
            if host.contains(':') || port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
                return false;
            }
            host
        }
        None => authority,
    };
    !host.is_empty()
        && !host
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || b"@/?#&=".contains(&byte))
}

fn redact_userinfo(userinfo: &str) -> String {
    match userinfo.split_once(':') {
        Some((username, _)) => format!("{username}:***"),
        None => userinfo.to_owned(),
    }
}

/// Mask credential-valued query options. `?` and `@` are ordinary bytes in a
/// query value, while `&` starts the next option and `#` ended the query above.
fn redact_query_options(query: &str) -> String {
    query
        .split('&')
        .map(|option| match option.split_once('=') {
            Some((key, _)) if SECRET_OPTIONS.contains(&key.to_ascii_lowercase().as_str()) => {
                format!("{key}=***")
            }
            _ => option.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn redact_unqualified_query(url: &str) -> String {
    let (without_fragment, fragment) = split_fragment(url);
    match without_fragment.split_once('?') {
        Some((prefix, query)) => {
            format!("{prefix}?{}{fragment}", redact_query_options(query))
        }
        None => url.to_owned(),
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
