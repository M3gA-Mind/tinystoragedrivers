//! Record metadata checks: `schema`, `created_ms`, `updated_ms` and
//! `expires_ms`, and the reserved filter and sort paths that address them.
//!
//! The checks never assert exact timestamps, because a driver reads its own
//! clock. They assert order instead: a pause of a few milliseconds separates
//! the writes whose order is compared.

use std::time::Duration;

use serde_json::json;

use super::{empty, fails, has, ok, unique};
use crate::backend::ScopedStorage;
use crate::capabilities::Capability;
use crate::document::{CollectionSpec, DocumentStoreExt, Meta, Precondition, Query, WriteOptions};
use crate::error::ErrorKind;
use crate::filter::{CREATED_FIELD, EXPIRES_FIELD, Filter, SCHEMA_FIELD, Sort, UPDATED_FIELD};

/// Long enough that two writes never share a millisecond on any driver clock.
const PAUSE: Duration = Duration::from_millis(20);

/// An expiry that has long passed: epoch millisecond `1`.
const LONG_AGO_MS: i64 = 1;

/// An expiry that never arrives in practice: the year 2100.
const FAR_FUTURE_MS: i64 = 4_102_444_800_000;

pub(super) async fn run(storage: &ScopedStorage) {
    if !has(storage, Capability::RecordMetadata) {
        declared_options_are_unsupported(storage).await;
        return;
    }
    schema_round_trips(storage).await;
    created_survives_updates(storage).await;
    recreation_starts_a_new_creation_time(storage).await;
    metadata_filters_and_sorts(storage).await;
    expiry_hides_records(storage).await;
}

async fn put_with(storage: &ScopedStorage, coll: &str, id: &str, options: WriteOptions) -> Meta {
    let docs = storage.documents();
    ok(
        docs.put_with(coll, id, json!({"n": 1}), Precondition::None, options)
            .await,
        "put_with",
    );
    meta_of(storage, coll, id).await
}

async fn meta_of(storage: &ScopedStorage, coll: &str, id: &str) -> Meta {
    ok(storage.documents().get(coll, id).await, "get")
        .unwrap_or_else(|| panic!("`{coll}/{id}` should exist"))
        .meta
}

async fn ids(storage: &ScopedStorage, coll: &str, query: &Query) -> Vec<String> {
    let found = ok(storage.documents().query_all(coll, query).await, "query");
    found.into_iter().map(|item| item.id).collect()
}

/// Without the capability, declaring a schema or expiry is refused rather
/// than silently dropped.
async fn declared_options_are_unsupported(storage: &ScopedStorage) {
    let coll = unique("meta_unsupported");
    for options in [
        WriteOptions::new().schema(1),
        WriteOptions::new().expires_ms(FAR_FUTURE_MS),
    ] {
        fails(
            storage
                .documents()
                .put_with(&coll, "a", json!({}), Precondition::None, options)
                .await,
            ErrorKind::Unsupported(Capability::RecordMetadata),
            "put_with declared options without RecordMetadata",
        );
    }
    ok(
        storage
            .documents()
            .put_with(
                &coll,
                "a",
                json!({}),
                Precondition::None,
                WriteOptions::default(),
            )
            .await,
        "put_with default options is a plain put",
    );
}

/// The declared schema reads back from `get` and `query`, `0` means
/// undeclared, and a write replaces it.
async fn schema_round_trips(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("meta_schema");
    let written = put_with(storage, &coll, "a", WriteOptions::new().schema(7)).await;
    assert_eq!(written.schema, 7, "get returns the declared schema");
    let page = ok(docs.query(&coll, &Query::all()).await, "query");
    assert_eq!(
        page.items.first().map(|item| item.meta),
        Some(written),
        "query returns the same metadata as get"
    );
    assert_eq!(
        ids(storage, &coll, &Query::filter(Filter::eq(SCHEMA_FIELD, 7))).await,
        ["a"],
        "_schema filters on the declared schema"
    );
    empty(
        &ids(storage, &coll, &Query::filter(Filter::eq(SCHEMA_FIELD, 8))).await,
        "_schema of another version",
    );

    ok(
        docs.put(&coll, "a", json!({"n": 2}), Precondition::None)
            .await,
        "plain put",
    );
    assert_eq!(
        meta_of(storage, &coll, "a").await.schema,
        0,
        "a write that declares nothing leaves the schema undeclared"
    );
}

/// An update keeps `created_ms` and advances `updated_ms`.
async fn created_survives_updates(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("meta_created");
    let first = put_with(storage, &coll, "a", WriteOptions::new()).await;
    assert!(first.created_ms > 0, "created_ms is set on first write");
    assert!(
        first.updated_ms >= first.created_ms,
        "updated_ms is not before created_ms"
    );
    tokio::time::sleep(PAUSE).await;
    ok(
        docs.put(&coll, "a", json!({"n": 2}), Precondition::None)
            .await,
        "update",
    );
    let second = meta_of(storage, &coll, "a").await;
    assert_eq!(
        second.created_ms, first.created_ms,
        "an update keeps created_ms"
    );
    assert!(
        second.updated_ms > first.updated_ms,
        "an update advances updated_ms ({} then {})",
        first.updated_ms,
        second.updated_ms
    );
}

/// Deleting an id and writing it again starts a new creation time.
async fn recreation_starts_a_new_creation_time(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("meta_recreate");
    let first = put_with(storage, &coll, "a", WriteOptions::new()).await;
    ok(docs.delete(&coll, "a", Precondition::None).await, "delete");
    tokio::time::sleep(PAUSE).await;
    let second = put_with(storage, &coll, "a", WriteOptions::new()).await;
    assert!(
        second.created_ms > first.created_ms,
        "a re-created id gets a new created_ms ({} then {})",
        first.created_ms,
        second.created_ms
    );
}

/// `_created_ms` and `_updated_ms` filter by range and sort.
async fn metadata_filters_and_sorts(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("meta_paths");
    let mut created = Vec::new();
    for id in ["a", "b", "c"] {
        created.push(
            put_with(storage, &coll, id, WriteOptions::new())
                .await
                .created_ms,
        );
        tokio::time::sleep(PAUSE).await;
    }
    assert!(
        created.windows(2).all(|pair| pair[0] < pair[1]),
        "writes apart in time get increasing created_ms: {created:?}"
    );

    let from_b =
        Query::filter(Filter::gte(CREATED_FIELD, created[1])).sort(Sort::asc(CREATED_FIELD));
    assert_eq!(
        ids(storage, &coll, &from_b).await,
        ["b", "c"],
        "_created_ms range"
    );
    let before_c = Query::filter(Filter::lt(CREATED_FIELD, created[2]));
    assert_eq!(
        ids(storage, &coll, &before_c).await,
        ["a", "b"],
        "_created_ms upper bound"
    );
    let newest = Query::all().sort(Sort::desc(CREATED_FIELD));
    assert_eq!(
        ids(storage, &coll, &newest).await,
        ["c", "b", "a"],
        "_created_ms sort"
    );
    assert_eq!(
        ok(
            docs.count(&coll, &Filter::eq(CREATED_FIELD, created[0]))
                .await,
            "count"
        ),
        1,
        "_created_ms equality"
    );

    // Touching `a` last makes it the most recently updated.
    ok(
        docs.put(&coll, "a", json!({"n": 2}), Precondition::None)
            .await,
        "touch",
    );
    let by_update = Query::all().sort(Sort::desc(UPDATED_FIELD));
    assert_eq!(
        ids(storage, &coll, &by_update).await,
        ["a", "c", "b"],
        "_updated_ms sort"
    );
    let paged = ok(
        docs.query(&coll, &Query::all().sort(Sort::asc(CREATED_FIELD)).limit(2))
            .await,
        "first page",
    );
    let next = paged.next.expect("a second page exists");
    let rest = ok(
        docs.query(
            &coll,
            &Query::all()
                .sort(Sort::asc(CREATED_FIELD))
                .limit(2)
                .after(next),
        )
        .await,
        "second page",
    );
    assert_eq!(
        rest.items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["c"],
        "paging resumes after a metadata sort key"
    );
}

/// A record past `expires_ms` reads as absent from every read.
async fn expiry_hides_records(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("meta_expiry");
    if has(storage, Capability::FullText) {
        ok(
            docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["title"]))
                .await,
            "ensure searchable",
        );
    }
    let write = |id: &'static str, expires: i64| {
        docs.put_with(
            &coll,
            id,
            json!({"title": "harbor", "state": "queued"}),
            Precondition::None,
            WriteOptions::new().expires_ms(expires),
        )
    };
    ok(write("live", FAR_FUTURE_MS).await, "write live");
    ok(write("dead", LONG_AGO_MS).await, "write expired");

    assert_eq!(
        meta_of(storage, &coll, "live").await.expires_ms,
        Some(FAR_FUTURE_MS),
        "expires_ms reads back"
    );
    assert!(
        ok(docs.get(&coll, "dead").await, "get expired").is_none(),
        "get hides an expired record"
    );
    assert_eq!(ids(storage, &coll, &Query::all()).await, ["live"], "query");
    assert_eq!(ok(docs.count(&coll, &Filter::All).await, "count"), 1);
    assert_eq!(
        ids(
            storage,
            &coll,
            &Query::filter(Filter::exists(EXPIRES_FIELD, true))
        )
        .await,
        ["live"],
        "_expires_ms is addressable"
    );
    assert_eq!(
        ids(
            storage,
            &coll,
            &Query::filter(Filter::gt(EXPIRES_FIELD, FAR_FUTURE_MS - 1))
        )
        .await,
        ["live"],
        "_expires_ms ranges"
    );
    if has(storage, Capability::FullText) {
        let hits = ok(docs.search(&coll, "harbor", 10).await, "search");
        assert_eq!(
            hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
            ["live"],
            "search skips an expired record"
        );
    }
    // An expired record is not there to be claimed or to clash with an insert.
    let claimed = ok(
        docs.claim(
            &coll,
            &Filter::eq("state", "queued"),
            &[Sort::asc("_id")],
            &json!({"state": "taken"}),
        )
        .await,
        "claim",
    );
    assert_eq!(
        claimed.map(|item| item.id),
        Some("live".to_owned()),
        "claim skips an expired record"
    );
    ok(
        docs.put(&coll, "dead", json!({}), Precondition::Absent)
            .await,
        "insert over an expired record",
    );
    assert!(
        ok(docs.get(&coll, "dead").await, "get rewritten").is_some(),
        "the rewritten record no longer expires"
    );
}
