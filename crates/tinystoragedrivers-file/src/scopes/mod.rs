//! Listing the scopes that hold records, by walking `scopes/`.
//!
//! A scope's directory name is its [`encode`](crate::encode::encode)d text
//! split into 200-byte chunks: the first chunk is a directory under `scopes/`
//! and every later chunk is a `+`-prefixed directory inside the previous one.
//! A directory holds the scope's data when it has a `docs`, `streams` or
//! `blobs` child that contains at least one file. Those three names never
//! start with `+`, so they cannot be mistaken for a continuation chunk even
//! when a scope is itself called `docs`. Entries that do not decode (a stray
//! directory someone created by hand) are skipped.

use std::fs;
use std::path::Path;

use tinystoragedrivers_core::{Result, Scope};

use crate::encode::decode;
use crate::fsio::io_error;
use crate::storage::Db;

/// The directories under a scope directory that hold its records.
const DATA_DIRS: [&str; 3] = ["docs", "streams", "blobs"];

/// The scopes with at least one stored file whose text starts with `prefix`,
/// in byte order.
pub(crate) fn list(db: &Db, prefix: &str) -> Result<Vec<Scope>> {
    let mut encoded = Vec::new();
    for (name, path) in child_dirs(&db.scopes_dir())? {
        walk(&path, name, &mut encoded)?;
    }
    let mut scopes = encoded
        .iter()
        .filter_map(|name| decode(name))
        .filter(|text| text.starts_with(prefix))
        .filter_map(|text| Scope::new(text).ok())
        .collect::<Vec<_>>();
    scopes.sort_by(|a, b| a.as_str().as_bytes().cmp(b.as_str().as_bytes()));
    scopes.dedup();
    Ok(scopes)
}

/// Record `encoded` when `dir` holds data, then follow its continuation chunks.
fn walk(dir: &Path, encoded: String, found: &mut Vec<String>) -> Result<()> {
    let mut holds_data = false;
    for (name, path) in child_dirs(dir)? {
        if let Some(chunk) = name.strip_prefix('+') {
            walk(&path, format!("{encoded}{chunk}"), found)?;
        } else if DATA_DIRS.contains(&name.as_str()) && has_file(&path)? {
            holds_data = true;
        }
    }
    if holds_data {
        found.push(encoded);
    }
    Ok(())
}

/// The visible subdirectories of `dir` (not symlinks); a missing one has none.
fn child_dirs(dir: &Path) -> Result<Vec<(String, std::path::PathBuf)>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("list the scopes")(error)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(io_error("list the scopes"))?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let is_dir = entry
            .file_type()
            .map_err(io_error("list the scopes"))?
            .is_dir();
        if is_dir && !name.starts_with('.') {
            out.push((name, entry.path()));
        }
    }
    Ok(out)
}

/// Whether `dir` or anything below it is a visible regular file.
fn has_file(dir: &Path) -> Result<bool> {
    for entry in fs::read_dir(dir).map_err(io_error("list the scopes"))? {
        let entry = entry.map_err(io_error("list the scopes"))?;
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let kind = entry.file_type().map_err(io_error("list the scopes"))?;
        if kind.is_file() || (kind.is_dir() && has_file(&entry.path())?) {
            return Ok(true);
        }
    }
    Ok(false)
}
