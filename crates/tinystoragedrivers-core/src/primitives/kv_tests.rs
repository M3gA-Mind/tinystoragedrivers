use serde_json::json;

use super::*;
use crate::{MemoryStorage, Scope, StorageBackend};

fn docs() -> Arc<dyn DocumentStore> {
    Arc::clone(
        MemoryStorage::new()
            .for_scope(&Scope::local())
            .unwrap()
            .documents(),
    )
}

#[tokio::test]
async fn put_get_delete_round_trip_any_json_shape() {
    let kv = Kv::ns(&docs(), "cron", "state").unwrap();
    assert_eq!(kv.get::<u32>("n").await.unwrap(), None);
    kv.put("n", &7_u32).await.unwrap();
    kv.put("s", &"text").await.unwrap();
    kv.put("o", &json!({"a": [1, 2]})).await.unwrap();
    assert_eq!(kv.get::<u32>("n").await.unwrap(), Some(7));
    assert_eq!(
        kv.get::<String>("s").await.unwrap().as_deref(),
        Some("text")
    );
    assert_eq!(
        kv.get::<serde_json::Value>("o").await.unwrap(),
        Some(json!({"a": [1, 2]}))
    );
    assert!(kv.delete("n").await.unwrap());
    assert!(!kv.delete("n").await.unwrap());
    assert_eq!(kv.get::<u32>("n").await.unwrap(), None);
}

#[tokio::test]
async fn owners_and_namespaces_do_not_see_each_other() {
    let docs = docs();
    let a = Kv::ns(&docs, "cron", "one").unwrap();
    let b = Kv::ns(&docs, "cron", "two").unwrap();
    let c = Kv::ns(&docs, "flows", "one").unwrap();
    a.put("k", &1).await.unwrap();
    assert_eq!(b.get::<i32>("k").await.unwrap(), None);
    assert_eq!(c.get::<i32>("k").await.unwrap(), None);
    assert_eq!(b.list::<i32>("").await.unwrap().len(), 0);
    assert_eq!(c.list::<i32>("").await.unwrap().len(), 0);
}

#[tokio::test]
async fn list_filters_by_prefix_in_key_order_across_pages() {
    let kv = Kv::ns(&docs(), "cron", "state").unwrap();
    for i in (0..450).rev() {
        kv.put(&format!("job/{i:04}"), &i).await.unwrap();
    }
    kv.put("jobs", &-1).await.unwrap();
    kv.put("a", &-2).await.unwrap();
    kv.put("z", &-3).await.unwrap();
    let listed = kv.list::<i32>("job/").await.unwrap();
    assert_eq!(listed.len(), 450);
    assert_eq!(listed[0], ("job/0000".to_owned(), 0));
    assert_eq!(listed[449], ("job/0449".to_owned(), 449));
    assert_eq!(kv.list::<i32>("").await.unwrap().len(), 453);
    assert_eq!(kv.list::<i32>("jobs").await.unwrap().len(), 1);
    assert_eq!(kv.list::<i32>("nope").await.unwrap().len(), 0);
}

#[tokio::test]
async fn keys_with_separators_do_not_collide() {
    let kv = Kv::ns(&docs(), "cron", "state").unwrap();
    kv.put("a/b", &1).await.unwrap();
    kv.put("a%2Fb", &2).await.unwrap();
    assert_eq!(kv.get::<i32>("a/b").await.unwrap(), Some(1));
    assert_eq!(kv.get::<i32>("a%2Fb").await.unwrap(), Some(2));
}

#[test]
fn an_empty_owner_or_namespace_is_refused() {
    let docs = docs();
    assert!(Kv::ns(&docs, "", "x").is_err());
    assert!(Kv::ns(&docs, "x", "").is_err());
}

#[tokio::test]
async fn a_value_that_is_not_a_t_is_a_serialization_error() {
    let kv = Kv::ns(&docs(), "cron", "state").unwrap();
    kv.put("s", &"text").await.unwrap();
    let error = kv.get::<u32>("s").await.unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Serialization);
}
