//! Listing the scopes that hold records.

use std::collections::BTreeSet;

use mongodb::bson::{Bson, doc};
use tinystoragedrivers_core::{RESERVED_PREFIX, Result, Scope, StorageError, validate_collection};

use crate::backend::Shared;
use crate::errors;
use crate::naming::{BLOBS, STREAM_HEADS, STREAMS, prefix_regex};

/// The scopes with a document (a tombstone included), a stream or a blob
/// whose text starts with `prefix`, in byte order.
///
/// Each collection answers `distinct(_scope)` restricted by an anchored,
/// escaped regular expression, so `.`, `*` and the like in a prefix are
/// literal, and a scope held by several collections appears once.
pub(crate) async fn list(shared: &Shared, prefix: &str) -> Result<Vec<Scope>> {
    let names = shared
        .db
        .list_collection_names()
        .await
        .map_err(errors::failed("list scopes"))?;
    let mut sources = vec![
        ("_scope", STREAMS.to_owned()),
        ("_scope", STREAM_HEADS.to_owned()),
        ("metadata._scope", format!("{BLOBS}.files")),
    ];
    for name in names {
        let name = name.as_str();
        if !name.starts_with(RESERVED_PREFIX) && validate_collection(name).is_ok() {
            sources.push(("_scope", name.to_owned()));
        }
    }
    let mut found = BTreeSet::new();
    for (field, name) in sources {
        let scopes = shared
            .raw(&name)
            .distinct(field, doc! {field: prefix_regex(prefix)})
            .await
            .map_err(errors::failed("list scopes"))?;
        for scope in scopes {
            if let Bson::String(text) = scope {
                found.insert(text);
            }
        }
    }
    found
        .into_iter()
        .map(|text| {
            Scope::new(text).map_err(|error| {
                StorageError::backend("a stored scope is not a valid scope").with_source(error)
            })
        })
        .collect()
}
