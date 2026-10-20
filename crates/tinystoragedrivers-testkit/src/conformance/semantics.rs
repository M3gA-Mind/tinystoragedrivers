//! Search and expiry semantics beyond bare capability presence: ranking
//! order, indexing of documents that predate a search declaration, index
//! upkeep on update and delete, and expiry as seen by claims and queries.

use serde_json::{Value, json};

use super::{empty, has, ok, unique};
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::{Capability, CollectionSpec, Filter, Precondition, Query, Sort};

/// Check full-text ranking, index upkeep, and TTL views when supported.
pub async fn run(docs: &dyn DocumentStore) {
    if has(docs.capabilities(), Capability::FullText) {
        ranking(docs).await;
        late_declaration(docs).await;
        index_upkeep(docs).await;
        if has(docs.capabilities(), Capability::Ttl) {
            search_skips_expired(docs).await;
        }
    }
    if has(docs.capabilities(), Capability::Ttl) {
        expiry_in_claims_and_queries(docs).await;
    }
}

async fn put(docs: &dyn DocumentStore, coll: &str, id: &str, doc: Value) {
    ok(docs.put(coll, id, doc, Precondition::None).await, "put");
}

/// Best first: scores never increase down the list, equal scores order by id,
/// a document matching more of the query ranks ahead of one matching less,
/// and `limit` keeps the best prefix of the full ranking.
async fn ranking(docs: &dyn DocumentStore) {
    let coll = unique("rank");
    ok(
        docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["title"]))
            .await,
        "ensure searchable",
    );
    put(docs, &coll, "d1", json!({"title": "cat dog"})).await;
    put(docs, &coll, "d2", json!({"title": "cat"})).await;
    put(docs, &coll, "d3", json!({"title": "dog"})).await;
    put(docs, &coll, "d4", json!({"title": "bird"})).await;

    let full = ok(docs.search(&coll, "cat dog", 10).await, "search");
    let ids: Vec<_> = full.iter().map(|hit| hit.id.as_str()).collect();
    assert_eq!(ids.first(), Some(&"d1"), "matching both terms ranks first");
    assert_eq!(
        full.len(),
        3,
        "non-matching documents are not hits: {ids:?}"
    );
    for pair in full.windows(2) {
        assert!(
            pair[0].score >= pair[1].score,
            "scores must not increase down the list: {full:?}"
        );
        if pair[0].score.total_cmp(&pair[1].score).is_eq() {
            assert!(
                pair[0].id < pair[1].id,
                "equal scores order by id: {full:?}"
            );
        }
    }
    let top = ok(docs.search(&coll, "cat dog", 2).await, "limited");
    let prefix: Vec<_> = full.iter().take(2).map(|hit| hit.id.clone()).collect();
    let top: Vec<_> = top.into_iter().map(|hit| hit.id).collect();
    assert_eq!(top, prefix, "limit keeps the best prefix");
    empty(
        &ok(docs.search(&coll, "cat dog", 0).await, "limit zero"),
        "limit zero",
    );
}

/// Documents written before a field was declared searchable are found once it
/// is, and a later declaration adds fields without dropping earlier ones.
async fn late_declaration(docs: &dyn DocumentStore) {
    let coll = unique("reindex");
    put(
        docs,
        &coll,
        "old",
        json!({"title": "harbor", "body": "lighthouse"}),
    )
    .await;
    ok(
        docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["title"]))
            .await,
        "declare title",
    );
    let hits = ok(docs.search(&coll, "harbor", 10).await, "pre-existing");
    assert_eq!(
        hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        ["old"],
        "a document predating the declaration is indexed"
    );
    empty(
        &ok(docs.search(&coll, "lighthouse", 10).await, "undeclared"),
        "body is not declared yet",
    );
    ok(
        docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["body"]))
            .await,
        "declare body",
    );
    put(
        docs,
        &coll,
        "new",
        json!({"title": "dock", "body": "harbor"}),
    )
    .await;
    let mut ids: Vec<_> = ok(docs.search(&coll, "harbor", 10).await, "both fields")
        .into_iter()
        .map(|hit| hit.id)
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        ["new", "old"],
        "title stays searchable after body joins"
    );
    let body: Vec<_> = ok(docs.search(&coll, "lighthouse", 10).await, "body")
        .into_iter()
        .map(|hit| hit.id)
        .collect();
    assert_eq!(body, ["old"], "an old document's new field is indexed");
}

/// The index follows the document: an update replaces its terms and a delete
/// removes them.
async fn index_upkeep(docs: &dyn DocumentStore) {
    let coll = unique("upkeep");
    ok(
        docs.ensure_collection(&CollectionSpec::new(&coll).searchable(["text"]))
            .await,
        "ensure searchable",
    );
    put(docs, &coll, "n", json!({"text": "alpha"})).await;
    put(docs, &coll, "n", json!({"text": "omega"})).await;
    empty(
        &ok(docs.search(&coll, "alpha", 10).await, "stale term"),
        "an updated document no longer matches its old text",
    );
    assert_eq!(
        ok(docs.search(&coll, "omega", 10).await, "new term").len(),
        1
    );
    ok(docs.delete(&coll, "n", Precondition::None).await, "delete");
    empty(
        &ok(docs.search(&coll, "omega", 10).await, "deleted term"),
        "a deleted document no longer matches",
    );
}

async fn search_skips_expired(docs: &dyn DocumentStore) {
    let coll = unique("search_ttl");
    ok(
        docs.ensure_collection(&CollectionSpec::new(&coll).ttl("exp").searchable(["text"]))
            .await,
        "ensure ttl and search",
    );
    put(docs, &coll, "gone", json!({"text": "ember", "exp": 1})).await;
    put(
        docs,
        &coll,
        "live",
        json!({"text": "ember", "exp": 32_503_680_000_000_u64}),
    )
    .await;
    let ids: Vec<_> = ok(docs.search(&coll, "ember", 10).await, "search")
        .into_iter()
        .map(|hit| hit.id)
        .collect();
    assert_eq!(ids, ["live"], "expired documents are not search hits");
}

/// A cache entry past its expiry is invisible to claims and queries, and
/// rewriting it with a later expiry keeps it alive.
async fn expiry_in_claims_and_queries(docs: &dyn DocumentStore) {
    let coll = unique("ttl_views");
    ok(
        docs.ensure_collection(&CollectionSpec::new(&coll).ttl("exp"))
            .await,
        "ensure ttl",
    );
    let far = 32_503_680_000_000_u64;
    put(docs, &coll, "a", json!({"state": "q", "n": 1, "exp": 1})).await;
    put(docs, &coll, "b", json!({"state": "q", "n": 2, "exp": far})).await;
    let eligible = Filter::eq("state", "q");
    let claimed = ok(
        docs.claim(&coll, &eligible, &[Sort::asc("n")], &json!({"state": "r"}))
            .await,
        "claim",
    )
    .expect("the live task is claimable");
    assert_eq!(claimed.id, "b", "an expired task is never claimed");
    assert!(
        ok(
            docs.claim(&coll, &eligible, &[Sort::asc("n")], &json!({"state": "r"}))
                .await,
            "claim exhausted",
        )
        .is_none()
    );
    let all = ok(docs.query(&coll, &Query::all()).await, "query").items;
    assert_eq!(
        all.iter().map(|doc| doc.id.as_str()).collect::<Vec<_>>(),
        ["b"],
        "queries skip expired documents"
    );
    let version = claimed.version;
    ok(
        docs.put(
            &coll,
            "b",
            json!({"state": "r", "n": 2, "exp": far + 1}),
            Precondition::Version(version),
        )
        .await,
        "extend expiry",
    );
    assert!(ok(docs.get(&coll, "b").await, "extended").is_some());
}
