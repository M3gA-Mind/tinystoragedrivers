//! Optional watch admission and payload-free conservative delivery.
use super::{fails, has, ok, unique};
use crate::{
    Capability, ErrorKind, Filter, Precondition, ScopedStorage, WatchCursor, WatchNotice,
    WatchOptions,
};
use serde_json::json;

pub(super) async fn run(storage: &ScopedStorage) {
    let collection = unique("watch");
    let options = WatchOptions {
        interval: std::time::Duration::from_millis(1),
        ..WatchOptions::default()
    };
    let docs = storage.documents();
    if !has(storage, Capability::Watch) {
        fails(
            docs.watch(&collection, &Filter::All, &options).await,
            ErrorKind::Unsupported(Capability::Watch),
            "native watch is optional",
        );
        return;
    }
    let mut watch = ok(
        docs.watch(&collection, &Filter::eq("state", "queued"), &options)
            .await,
        "subscribe before reading",
    );
    assert!(
        ok(
            docs.get(&collection, "one").await,
            "initial authoritative read"
        )
        .is_none()
    );
    ok(
        docs.put(
            &collection,
            "one",
            json!({"state":"queued"}),
            Precondition::Absent,
        )
        .await,
        "insert watched document",
    );
    assert!(matches!(
        ok(watch.next().await, "insertion invalidates"),
        Some(WatchNotice::Invalidated { .. })
    ));
    ok(
        docs.put(
            &collection,
            "one",
            json!({"state":"done"}),
            Precondition::None,
        )
        .await,
        "leave watched filter",
    );
    assert!(matches!(
        ok(watch.next().await, "filter exit invalidates"),
        Some(WatchNotice::Invalidated { .. })
    ));
    ok(
        docs.delete(&collection, "one", Precondition::None).await,
        "delete watched document",
    );
    assert!(matches!(
        ok(watch.next().await, "deletion invalidates"),
        Some(WatchNotice::Invalidated { .. })
    ));
    fails(
        docs.watch(
            &collection,
            &Filter::All,
            &WatchOptions {
                after: Some(WatchCursor::new(vec![1]).unwrap()),
                ..options
            },
        )
        .await,
        ErrorKind::InvalidInput,
        "invalid native watch cursor",
    );
    drop(watch);
    ok(
        docs.drop_collection(&collection).await,
        "clean watched collection",
    );
}
