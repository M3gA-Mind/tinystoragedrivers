//! Composition refusals and the readiness gate.

use super::*;
use crate::schema::ScopeClass;
use crate::{CollectionSpec, ErrorKind, IndexSpec, MemoryStorage, Precondition, StorageBackend};
use serde_json::json;

fn owner(name: &str) -> Owner {
    Owner::new(name).unwrap()
}

fn noop(id: &'static str, item: &'static str, from: u32, to: u32) -> Step {
    Step::new(id, item, from, to, |_| Ok(Vec::new()))
}

fn refused(schemas: Vec<Schema>) -> String {
    let error = Registry::compose(schemas).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    error.message().to_owned()
}

#[test]
fn composes_valid_schemas_from_several_owners() {
    let cron = Schema::new(owner("cron"))
        .collection("cron.jobs", 2, ScopeClass::Tenant)
        .step(noop("a", "cron.jobs", 1, 2))
        .stream("cron.audit", ScopeClass::Tenant)
        .blobs("cron/files/", ScopeClass::Tenant)
        .secrets("cron/keys", ScopeClass::Tenant);
    let flows = Schema::new(owner("flows")).collection("flows.runs", 1, ScopeClass::Agent);
    let registry = Registry::compose([cron, flows]).unwrap();
    assert_eq!(registry.owners().collect::<Vec<_>>(), ["cron", "flows"]);
    assert_eq!(registry.collection_version("cron.jobs"), Some(2));
    assert_eq!(registry.collection_version("flows.runs"), Some(1));
    assert_eq!(registry.collection_version("flows.nope"), None);
}

#[test]
fn refuses_a_duplicate_owner() {
    let message = refused(vec![Schema::new(owner("cron")), Schema::new(owner("cron"))]);
    assert!(
        message.contains("owner `cron` is declared twice"),
        "{message}"
    );
}

#[test]
fn refuses_a_collection_name_outside_the_owner_prefix_and_names_the_claimant() {
    let message = refused(vec![
        Schema::new(owner("cron")).collection("flows.jobs", 1, ScopeClass::Tenant),
        Schema::new(owner("flows")),
    ]);
    assert!(message.contains("`flows.jobs`"), "{message}");
    assert!(message.contains("owner `cron`"), "{message}");
    assert!(message.contains("owner `flows`"), "{message}");
    let message = refused(vec![Schema::new(owner("cron")).collection(
        "jobs",
        1,
        ScopeClass::Tenant,
    )]);
    assert!(message.contains("must start with `cron.`"), "{message}");
}

#[test]
fn refuses_malformed_entities_and_names() {
    for bad in [
        "cron.",
        "cron.Jobs",
        "cron.jobs-x",
        "cron.1x",
        "cron.a.b",
        "cron.a/b",
    ] {
        let message = refused(vec![Schema::new(owner("cron")).collection(
            bad,
            1,
            ScopeClass::Tenant,
        )]);
        assert!(message.contains(bad), "{bad}: {message}");
        let message = refused(vec![
            Schema::new(owner("cron")).stream(bad, ScopeClass::Tenant),
        ]);
        assert!(message.contains(bad), "{bad}: {message}");
    }
    let long = format!("cron.{}", "a".repeat(200));
    refused(vec![Schema::new(owner("cron")).collection(
        &long,
        1,
        ScopeClass::Tenant,
    )]);
}

#[test]
fn refuses_a_name_claimed_twice_even_across_item_kinds() {
    let message = refused(vec![
        Schema::new(owner("cron"))
            .collection("cron.jobs", 1, ScopeClass::Tenant)
            .stream("cron.jobs", ScopeClass::Tenant),
    ]);
    assert!(
        message.contains("`cron.jobs` is claimed twice"),
        "{message}"
    );
    let message = refused(vec![
        Schema::new(owner("cron"))
            .blobs("cron/x", ScopeClass::Tenant)
            .secrets("cron/x", ScopeClass::Tenant),
    ]);
    assert!(message.contains("`cron/x` is claimed twice"), "{message}");
}

#[test]
fn refuses_bad_blob_and_secret_prefixes() {
    for bad in [
        "cron",
        "cron/",
        "flows/x",
        "cron//x",
        "cron/../x",
        "cron/./x",
        "cron/x\0",
        "cron\\x",
    ] {
        refused(vec![
            Schema::new(owner("cron")).blobs(bad, ScopeClass::Tenant),
        ]);
        refused(vec![
            Schema::new(owner("cron")).secrets(bad, ScopeClass::Tenant),
        ]);
    }
}

#[test]
fn refuses_version_zero_and_a_spec_for_another_name() {
    let message = refused(vec![Schema::new(owner("cron")).collection(
        "cron.jobs",
        0,
        ScopeClass::Tenant,
    )]);
    assert!(message.contains("versions start at 1"), "{message}");
    let mut schema = Schema::new(owner("cron"));
    schema.items.push(Item::Collection {
        name: "cron.jobs".into(),
        version: 1,
        spec: CollectionSpec::new("cron.other"),
        scope: ScopeClass::Tenant,
    });
    let message = refused(vec![schema]);
    assert!(message.contains("cron.other"), "{message}");
    // The spec's own validation runs too.
    refused(vec![Schema::new(owner("cron")).collection_with(
        1,
        CollectionSpec::new("cron.jobs").ttl(""),
        ScopeClass::Tenant,
    )]);
}

fn chained(steps: Vec<Step>, version: u32) -> Schema {
    steps.into_iter().fold(
        Schema::new(owner("cron")).collection("cron.jobs", version, ScopeClass::Tenant),
        Schema::step,
    )
}

#[test]
fn refuses_a_step_gap() {
    let message = refused(vec![chained(
        vec![noop("a", "cron.jobs", 1, 2), noop("c", "cron.jobs", 3, 4)],
        4,
    )]);
    assert!(message.contains("gap"), "{message}");
    assert!(message.contains("cron.jobs"), "{message}");
}

#[test]
fn refuses_a_chain_that_does_not_start_at_one() {
    let message = refused(vec![chained(vec![noop("b", "cron.jobs", 2, 3)], 3)]);
    assert!(message.contains("start at version 1"), "{message}");
}

#[test]
fn refuses_a_declared_version_above_the_chain_without_steps() {
    let message = refused(vec![chained(Vec::new(), 2)]);
    assert!(message.contains("no steps"), "{message}");
}

#[test]
fn refuses_a_chain_that_misses_or_overshoots_the_declared_version() {
    let short = refused(vec![chained(vec![noop("a", "cron.jobs", 1, 2)], 3)]);
    assert!(short.contains("ends at 2"), "{short}");
    let over = refused(vec![chained(vec![noop("a", "cron.jobs", 1, 3)], 2)]);
    assert!(over.contains("ends at 3"), "{over}");
}

#[test]
fn refuses_descending_or_empty_version_steps() {
    for (from, to) in [(2, 1), (2, 2)] {
        let message = refused(vec![chained(vec![noop("a", "cron.jobs", from, to)], 2)]);
        assert!(message.contains("must go up"), "{message}");
    }
}

#[test]
fn refuses_steps_for_unknown_items_and_duplicate_step_ids() {
    let message = refused(vec![Schema::new(owner("cron")).step(noop(
        "a",
        "cron.ghost",
        1,
        2,
    ))]);
    assert!(message.contains("cron.ghost"), "{message}");
    let message = refused(vec![chained(
        vec![noop("a", "cron.jobs", 1, 2), noop("a", "cron.jobs", 2, 3)],
        3,
    )]);
    assert!(message.contains("step id `a`"), "{message}");
    let message = refused(vec![
        Schema::new(owner("cron"))
            .stream("cron.audit", ScopeClass::Tenant)
            .step(noop("a", "cron.audit", 1, 2)),
    ]);
    assert!(message.contains("collection"), "{message}");
}

#[test]
fn refuses_a_legacy_source_that_is_not_a_collection_name() {
    let message = refused(vec![
        Schema::new(owner("cron"))
            .collection("cron.jobs", 2, ScopeClass::Tenant)
            .step(noop("a", "cron.jobs", 1, 2).source("bad name")),
    ]);
    assert!(message.contains("bad name"), "{message}");
}

#[tokio::test]
async fn prepare_declares_every_collection_spec() {
    let registry = Registry::compose([Schema::new(owner("cron")).collection_with(
        1,
        CollectionSpec::new("cron.jobs").index(IndexSpec::new("by_key", ["key"]).unique()),
        ScopeClass::Tenant,
    )])
    .unwrap();
    let backend = MemoryStorage::new();
    registry.prepare(&backend).await.unwrap();
    registry.prepare(&backend).await.unwrap();
    let storage = backend.for_scope(&crate::Scope::local()).unwrap();
    let docs = storage.documents();
    docs.put("cron.jobs", "a", json!({"key": 1}), Precondition::Absent)
        .await
        .unwrap();
    let error = docs
        .put("cron.jobs", "b", json!({"key": 1}), Precondition::Absent)
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::AlreadyExists,
        "the unique index is declared"
    );
    // The internal progress collection is declared too.
    crate::schema::Migrations::new(&storage)
        .mark_done("cron", "x")
        .await
        .unwrap();
}

#[test]
fn check_ready_gates_collections_with_unfinished_steps() {
    let registry = Registry::compose([Schema::new(owner("cron"))
        .collection("cron.jobs", 2, ScopeClass::Tenant)
        .collection("cron.runs", 1, ScopeClass::Tenant)
        .stream("cron.audit", ScopeClass::Tenant)
        .step(noop("a", "cron.jobs", 1, 2))])
    .unwrap();
    let error = registry.check_ready("cron.jobs").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.message(), "migrating cron.jobs");
    registry.check_ready("cron.runs").unwrap();
    registry.check_ready("cron.audit").unwrap();
    assert_eq!(
        registry.check_ready("cron.ghost").unwrap_err().kind(),
        ErrorKind::NotFound
    );
    let scope = Scope::local();
    registry.set_pending(&scope, ["cron.jobs".to_owned()].into());
    assert!(registry.check_ready_in(&scope, "cron.jobs").is_err());
    registry.mark_ready(&scope, "cron.jobs");
    registry.check_ready_in(&scope, "cron.jobs").unwrap();
    registry.check_ready("cron.jobs").unwrap();
}

fn stream_step(id: &'static str, item: &'static str, from: u32, to: u32) -> Step {
    Step::stream(id, item, from, to, |_, entries| {
        Ok(Some(("x".into(), entries)))
    })
}

fn blob_step(id: &'static str, item: &'static str, from: u32, to: u32) -> Step {
    Step::blobs(id, item, from, to, |key, ct, bytes| {
        Ok(Some((key, ct, bytes)))
    })
}

fn stream_schema(steps: Vec<Step>) -> Schema {
    steps.into_iter().fold(
        Schema::new(owner("cron"))
            .stream("cron.audit", ScopeClass::Tenant)
            .blobs("cron/files/", ScopeClass::Tenant),
        Schema::step,
    )
}

#[test]
fn accepts_stream_and_blob_steps_and_gates_their_items() {
    let registry = Registry::compose([stream_schema(vec![
        stream_step("a", "cron.audit", 1, 2).source_prefix("audit:"),
        stream_step("b", "cron.audit", 2, 3).source_prefix("audit2:"),
        blob_step("c", "cron/files/", 1, 2).source_prefix("files/"),
    ])])
    .unwrap();
    assert_eq!(
        registry.check_ready("cron.audit").unwrap_err().message(),
        "migrating cron.audit"
    );
    assert_eq!(
        registry.check_ready("cron/files/").unwrap_err().kind(),
        ErrorKind::Unavailable
    );
}

#[test]
fn refuses_stream_and_blob_steps_without_a_source_prefix() {
    for step in [
        stream_step("a", "cron.audit", 1, 2),
        blob_step("a", "cron/files/", 1, 2),
    ] {
        let message = refused(vec![stream_schema(vec![step])]);
        assert!(message.contains("source prefix"), "{message}");
    }
}

#[test]
fn refuses_a_source_prefix_that_overlaps_the_item() {
    for step in [
        stream_step("a", "cron.audit", 1, 2).source_prefix("cron.audit/old/"),
        stream_step("a", "cron.audit", 1, 2).source_prefix("cron."),
        blob_step("a", "cron/files/", 1, 2).source_prefix("cron/"),
        blob_step("a", "cron/files/", 1, 2).source_prefix("cron/files/old/"),
    ] {
        let message = refused(vec![stream_schema(vec![step])]);
        assert!(message.contains("overlaps"), "{message}");
    }
}

#[test]
fn refuses_a_blob_source_prefix_over_sealed_streams() {
    for prefix in ["_tsd/", "_tsd/sealed/x"] {
        let message = refused(vec![stream_schema(vec![
            blob_step("a", "cron/files/", 1, 2).source_prefix(prefix),
        ])]);
        assert!(message.contains("sealed"), "{message}");
    }
}

#[test]
fn refuses_steps_of_the_wrong_kind_for_their_item() {
    for step in [
        stream_step("a", "cron/files/", 1, 2).source_prefix("old/"),
        blob_step("a", "cron.audit", 1, 2).source_prefix("old/"),
        noop("a", "cron.audit", 1, 2),
        noop("a", "cron/files/", 1, 2),
    ] {
        let message = refused(vec![stream_schema(vec![step])]);
        assert!(message.contains("not a"), "{message}");
    }
}

#[test]
fn validates_stream_and_blob_chains_without_a_declared_version() {
    let message = refused(vec![stream_schema(vec![
        stream_step("a", "cron.audit", 2, 3).source_prefix("audit:"),
    ])]);
    assert!(message.contains("start at version 1"), "{message}");
    let message = refused(vec![stream_schema(vec![
        blob_step("a", "cron/files/", 1, 2).source_prefix("a/"),
        blob_step("b", "cron/files/", 3, 4).source_prefix("b/"),
    ])]);
    assert!(message.contains("gap"), "{message}");
    let message = refused(vec![stream_schema(vec![
        stream_step("a", "cron.audit", 2, 1).source_prefix("audit:"),
    ])]);
    assert!(message.contains("must go up"), "{message}");
    let message = refused(vec![stream_schema(vec![
        stream_step("a", "cron.audit", 1, 2).source_prefix("audit:"),
        stream_step("a", "cron.audit", 2, 3).source_prefix("audit2:"),
    ])]);
    assert!(message.contains("step id `a`"), "{message}");
}
