//! The migration runner over the memory driver: batching, resume after an
//! injected failure, idempotence and `Migrations`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};

use super::*;
use crate::schema::{Schema, ScopeClass, TargetRecord};
use crate::{
    DocumentStoreExt, ErrorKind, Filter, MemoryStorage, Precondition, Query, Scope, ScopedStorage,
    StorageBackend, WriteOptions,
};

const TOTAL: usize = 250;
const BATCH: usize = 50;

fn owner() -> Owner {
    Owner::new("shop").unwrap()
}

fn opts() -> MigrateOptions {
    MigrateOptions::new().batch_size(BATCH)
}

fn scoped() -> ScopedStorage {
    MemoryStorage::with_clock(Arc::new(|| 5_000))
        .for_scope(&Scope::local())
        .unwrap()
}

/// Step one: legacy `orders_v1` rows become `n-<id>` in `shop.orders`, keeping
/// their creation time. Fails once on its `fail_at`-th call when asked to.
fn legacy_step(calls: Arc<AtomicUsize>, fail_at: Option<usize>) -> Step {
    let failed = Arc::new(AtomicUsize::new(0));
    Step::new("import", "shop.orders", 1, 2, move |batch| {
        let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
        if fail_at == Some(call) && failed.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(crate::StorageError::backend("injected failure"));
        }
        Ok(batch
            .into_iter()
            .map(|record| {
                let mut body = record.body;
                body["step"] = json!(2);
                TargetRecord::new("shop.orders", format!("n-{}", record.id), body)
                    .created_ms(record.meta.created_ms)
            })
            .collect())
    })
    .source("orders_v1")
}

/// Step two rewrites `shop.orders` in place.
fn in_place_step(calls: Arc<AtomicUsize>) -> Step {
    Step::new("flatten", "shop.orders", 2, 3, move |batch| {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok(batch
            .into_iter()
            .map(|record| {
                let mut body = record.body;
                body["step"] = json!(3);
                TargetRecord::new("shop.orders", record.id, body)
            })
            .collect())
    })
}

fn registry(first: Step, second: Option<Step>) -> Registry {
    let mut schema = Schema::new(owner())
        .collection(
            "shop.orders",
            if second.is_some() { 3 } else { 2 },
            ScopeClass::Tenant,
        )
        .step(first);
    if let Some(second) = second {
        schema = schema.step(second);
    }
    Registry::compose([schema]).unwrap()
}

async fn seed_legacy(storage: &ScopedStorage) {
    for n in 0..TOTAL {
        storage
            .documents()
            .put_with(
                "orders_v1",
                &format!("o{n:03}"),
                json!({"n": n}),
                Precondition::Absent,
                WriteOptions::new()
                    .schema(1)
                    .created_ms(1_000 + i64::try_from(n).unwrap()),
            )
            .await
            .unwrap();
    }
}

async fn snapshot(storage: &ScopedStorage, collection: &str) -> Vec<(String, Value, u32, i64)> {
    storage
        .documents()
        .query_all(collection, &Query::all())
        .await
        .unwrap()
        .into_iter()
        .map(|found| {
            (
                found.id,
                found.doc,
                found.meta.schema,
                found.meta.created_ms,
            )
        })
        .collect()
}

async fn progress(storage: &ScopedStorage, step: &str) -> Value {
    storage
        .documents()
        .get("tsd.migrations", &format!("shop/shop.orders/{step}"))
        .await
        .unwrap()
        .unwrap()
        .doc
}

#[tokio::test]
async fn migrates_a_two_step_chain_in_batches() {
    let storage = scoped();
    seed_legacy(&storage).await;
    let (first, second) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let registry = registry(
        legacy_step(Arc::clone(&first), None),
        Some(in_place_step(Arc::clone(&second))),
    );
    assert!(registry.check_ready("shop.orders").is_err());

    let report = registry.migrate(&storage, &opts()).await.unwrap();

    assert_eq!(first.load(Ordering::SeqCst), TOTAL / BATCH);
    assert_eq!(second.load(Ordering::SeqCst), TOTAL / BATCH);
    assert_eq!(report.already_done, 0);
    let ran: Vec<_> = report
        .ran
        .iter()
        .map(|step| {
            (
                step.owner.as_str(),
                step.item.as_str(),
                step.step.as_str(),
                step.written,
            )
        })
        .collect();
    assert_eq!(
        ran,
        [
            ("shop", "shop.orders", "import", 250),
            ("shop", "shop.orders", "flatten", 250)
        ]
    );
    let migrated = snapshot(&storage, "shop.orders").await;
    assert_eq!(migrated.len(), TOTAL);
    assert_eq!(migrated[0].0, "n-o000");
    assert_eq!(migrated[0].1, json!({"n": 0, "step": 3}));
    for (n, (_, _, schema, created)) in migrated.iter().enumerate() {
        assert_eq!(*schema, 3);
        assert_eq!(
            *created,
            1_000 + i64::try_from(n).unwrap(),
            "creation time survives both steps"
        );
    }
    // The legacy source is gone once its step completed; the in-place
    // collection is never emptied.
    assert_eq!(snapshot(&storage, "orders_v1").await.len(), 0);
    registry.check_ready("shop.orders").unwrap();
    assert_eq!(progress(&storage, "import").await["done"], json!(true));
}

#[tokio::test]
async fn resumes_after_a_failure_with_exactly_the_uninterrupted_result() {
    let reference = scoped();
    seed_legacy(&reference).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let clean = registry(legacy_step(Arc::clone(&calls), None), None);
    clean.migrate(&reference, &opts()).await.unwrap();

    let storage = scoped();
    seed_legacy(&storage).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let flaky = registry(legacy_step(Arc::clone(&calls), Some(3)), None);
    let error = flaky.migrate(&storage, &opts()).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    // Two batches landed and were recorded; the source is untouched and the
    // collection stays closed.
    let recorded = progress(&storage, "import").await;
    assert_eq!(recorded["written"], json!(100));
    assert_eq!(recorded["done"], json!(false));
    assert!(recorded["cursor"].is_string());
    assert_eq!(snapshot(&storage, "orders_v1").await.len(), TOTAL);
    assert_eq!(snapshot(&storage, "shop.orders").await.len(), 100);
    assert_eq!(
        flaky.check_ready("shop.orders").unwrap_err().message(),
        "migrating shop.orders"
    );

    let report = flaky.migrate(&storage, &opts()).await.unwrap();
    // The failed call plus the three batches left.
    assert_eq!(calls.load(Ordering::SeqCst), 3 + 3);
    assert_eq!(report.ran[0].written, 250);
    assert_eq!(
        snapshot(&storage, "shop.orders").await,
        snapshot(&reference, "shop.orders").await
    );
    assert_eq!(snapshot(&storage, "shop.orders").await.len(), TOTAL);
    assert_eq!(snapshot(&storage, "orders_v1").await.len(), 0);
    flaky.check_ready("shop.orders").unwrap();
}

#[tokio::test]
async fn a_finished_migration_is_a_no_op_when_run_again() {
    let storage = scoped();
    seed_legacy(&storage).await;
    let (first, second) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let registry = registry(
        legacy_step(Arc::clone(&first), None),
        Some(in_place_step(Arc::clone(&second))),
    );
    registry.migrate(&storage, &opts()).await.unwrap();
    let before = snapshot(&storage, "shop.orders").await;

    // A fresh registry (a new process) learns the state from storage.
    let again = fresh(&first, &second);
    assert!(again.check_ready("shop.orders").is_err());
    let report = again.migrate(&storage, &opts()).await.unwrap();

    assert_eq!(report.ran.len(), 0);
    assert_eq!(report.already_done, 2);
    assert_eq!(first.load(Ordering::SeqCst), TOTAL / BATCH);
    assert_eq!(second.load(Ordering::SeqCst), TOTAL / BATCH);
    assert_eq!(snapshot(&storage, "shop.orders").await, before);
    again.check_ready("shop.orders").unwrap();
}

fn fresh(first: &Arc<AtomicUsize>, second: &Arc<AtomicUsize>) -> Registry {
    registry(
        legacy_step(Arc::clone(first), None),
        Some(in_place_step(Arc::clone(second))),
    )
}

#[tokio::test]
async fn an_output_that_already_exists_counts_as_written_and_is_kept() {
    let storage = scoped();
    seed_legacy(&storage).await;
    storage
        .documents()
        .put(
            "shop.orders",
            "n-o000",
            json!({"pre": true}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let registry = registry(legacy_step(Arc::new(AtomicUsize::new(0)), None), None);
    let report = registry.migrate(&storage, &opts()).await.unwrap();
    assert_eq!(report.ran[0].written, 250);
    let kept = storage
        .documents()
        .get("shop.orders", "n-o000")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(kept.doc, json!({"pre": true}));
    assert_eq!(snapshot(&storage, "shop.orders").await.len(), TOTAL);
}

#[tokio::test]
async fn refuses_outputs_for_foreign_or_undeclared_collections() {
    for target in ["other.things", "shop.ghost"] {
        let storage = scoped();
        seed_legacy(&storage).await;
        let step = Step::new("import", "shop.orders", 1, 2, move |batch| {
            Ok(batch
                .into_iter()
                .map(|record| TargetRecord::new(target, record.id, record.body))
                .collect())
        })
        .source("orders_v1");
        let error = registry(step, None)
            .migrate(&storage, &opts())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{target}");
        assert!(error.message().contains(target), "{target}");
        assert_eq!(snapshot(&storage, "orders_v1").await.len(), TOTAL);
    }
}

#[tokio::test]
async fn rejects_a_zero_batch_size() {
    let storage = scoped();
    let registry = registry(legacy_step(Arc::new(AtomicUsize::new(0)), None), None);
    let error = registry
        .migrate(&storage, &MigrateOptions::new().batch_size(0))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(MigrateOptions::default().batch_size, 100);
}

#[tokio::test]
async fn migrating_an_empty_source_completes_the_step() {
    let storage = scoped();
    let registry = registry(legacy_step(Arc::new(AtomicUsize::new(0)), None), None);
    let report = registry.migrate(&storage, &opts()).await.unwrap();
    assert_eq!(report.ran.len(), 1);
    assert_eq!(report.ran[0].written, 0);
    registry.check_ready("shop.orders").unwrap();
}

#[tokio::test]
async fn in_place_steps_only_touch_records_below_the_target_version() {
    let storage = scoped();
    for (id, schema) in [("a", 1), ("b", 2), ("c", 0)] {
        storage
            .documents()
            .put_with(
                "shop.orders",
                id,
                json!({"id": id}),
                Precondition::Absent,
                WriteOptions::new().schema(schema),
            )
            .await
            .unwrap();
    }
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&seen);
    let step = Step::new("bump", "shop.orders", 1, 2, move |batch| {
        counter.fetch_add(batch.len(), Ordering::SeqCst);
        Ok(batch
            .into_iter()
            .map(|record| TargetRecord::new("shop.orders", record.id, record.body))
            .collect())
    });
    registry(step, None)
        .migrate(&storage, &opts())
        .await
        .unwrap();
    assert_eq!(
        seen.load(Ordering::SeqCst),
        2,
        "the v2 record is left alone"
    );
    let schemas: Vec<u32> = snapshot(&storage, "shop.orders")
        .await
        .iter()
        .map(|r| r.2)
        .collect();
    assert_eq!(schemas, [2, 2, 2]);
    assert_eq!(
        storage
            .documents()
            .count("shop.orders", &Filter::eq("_schema", 2))
            .await
            .unwrap(),
        3
    );
}

#[tokio::test]
async fn migrations_records_importer_completion() {
    let storage = scoped();
    let migrations = Migrations::new(&storage);
    assert!(!migrations.done("threads", "import_v1").await.unwrap());
    migrations.mark_done("threads", "import_v1").await.unwrap();
    migrations.mark_done("threads", "import_v1").await.unwrap();
    assert!(migrations.done("threads", "import_v1").await.unwrap());
    assert!(!migrations.done("threads", "other").await.unwrap());
    assert!(!migrations.done("cron", "import_v1").await.unwrap());
    let stored = storage
        .documents()
        .get("tsd.migrations", "threads/import_v1")
        .await
        .unwrap()
        .unwrap()
        .doc;
    assert_eq!(
        stored,
        json!({"owner": "threads", "step": "import_v1", "from": 0, "to": 0,
               "cursor": null, "written": 0, "done": true})
    );
}
