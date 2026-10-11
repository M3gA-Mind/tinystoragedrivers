//! Stream port checks.

use serde_json::json;

use super::{empty, fails, ok, unique};
use crate::backend::ScopedStorage;
use crate::error::ErrorKind;

pub(super) async fn run(storage: &ScopedStorage) {
    conditional_append(storage).await;
    let streams = storage.streams();
    let name = unique("stream");

    assert_eq!(ok(streams.len(&name).await, "len of a missing stream"), 0);
    empty(
        &ok(streams.read_window(&name, 0, 10).await, "read missing"),
        "expected nothing",
    );
    assert_eq!(
        ok(streams.append_batch(&name, vec![]).await, "empty batch"),
        0
    );

    assert_eq!(
        ok(streams.append(&name, json!({"i": 0})).await, "append"),
        0
    );
    assert_eq!(
        ok(
            streams
                .append_batch(
                    &name,
                    vec![json!({"i": 1}), json!({"i": 2}), json!({"i": 3})]
                )
                .await,
            "append batch"
        ),
        1,
        "a batch starts at the next offset"
    );
    assert_eq!(ok(streams.len(&name).await, "len"), 4);
    assert_eq!(
        ok(streams.append_batch(&name, vec![]).await, "empty batch"),
        4
    );

    let window = ok(streams.read_window(&name, 1, 2).await, "window");
    let offsets: Vec<_> = window.iter().map(|e| e.offset).collect();
    assert_eq!(offsets, [1, 2]);
    assert_eq!(window[1].value, json!({"i": 2}));
    empty(
        &ok(streams.read_window(&name, 9, 2).await, "past the end"),
        "expected nothing",
    );

    assert_eq!(ok(streams.truncate_before(&name, 2).await, "truncate"), 2);
    assert_eq!(
        ok(
            streams.truncate_before(&name, 1).await,
            "truncate below base"
        ),
        0
    );
    assert_eq!(ok(streams.len(&name).await, "len keeps counting"), 4);
    let rest: Vec<_> = ok(streams.read_window(&name, 0, 10).await, "after truncate")
        .into_iter()
        .map(|e| e.offset)
        .collect();
    assert_eq!(rest, [2, 3], "truncated offsets are skipped");
    assert_eq!(
        ok(
            streams.append(&name, json!({"i": 4})).await,
            "append after truncate"
        ),
        4
    );
    assert_eq!(
        ok(
            streams.truncate_before(&name, 99).await,
            "truncate past the end"
        ),
        3
    );
    assert_eq!(ok(streams.len(&name).await, "len after full truncate"), 5);
    assert_eq!(
        ok(
            streams.truncate_before(&unique("none"), 3).await,
            "truncate missing"
        ),
        0
    );

    let sibling = format!("{name}/child");
    ok(streams.append(&sibling, json!(1)).await, "sibling");
    let listed = ok(streams.streams(&name).await, "list");
    assert_eq!(
        listed,
        [name.clone(), sibling.clone()],
        "listing is sorted and prefixed"
    );

    assert!(ok(streams.delete_stream(&sibling).await, "delete"));
    assert!(!ok(streams.delete_stream(&sibling).await, "delete missing"));
    assert_eq!(ok(streams.len(&sibling).await, "len after delete"), 0);

    fails(
        streams.append("", json!(1)).await,
        ErrorKind::InvalidInput,
        "empty stream name",
    );
}

async fn conditional_append(storage: &ScopedStorage) {
    let streams = storage.streams();
    let name = unique("conditional");
    if !super::has(storage, crate::Capability::ConditionalStreamAppend) {
        fails(
            streams.append_batch_if_offset(&name, 0, vec![]).await,
            ErrorKind::Unsupported(crate::Capability::ConditionalStreamAppend),
            "conditional append unsupported",
        );
        return;
    }
    assert_eq!(
        ok(
            streams.append_batch_if_offset(&name, 0, vec![]).await,
            "empty conditional"
        ),
        0
    );
    assert_eq!(
        ok(
            streams
                .append_batch_if_offset(&name, 0, vec![json!(0), json!(1)])
                .await,
            "conditional batch"
        ),
        0
    );
    for values in [vec![], vec![json!(9)]] {
        fails(
            streams.append_batch_if_offset(&name, 0, values).await,
            ErrorKind::Conflict,
            "stale offset",
        );
    }
    ok(
        streams.truncate_before(&name, 2).await,
        "truncate conditional",
    );
    assert_eq!(ok(streams.len(&name).await, "persistent offset"), 2);
    assert_eq!(
        ok(
            streams.append_batch_if_offset(&name, 2, vec![]).await,
            "empty after truncate"
        ),
        2
    );
    let (a, b) = tokio::join!(
        streams.append_batch_if_offset(&name, 2, vec![json!(2)]),
        streams.append_batch_if_offset(&name, 2, vec![json!(3)])
    );
    assert_eq!(
        usize::from(a.is_ok()) + usize::from(b.is_ok()),
        1,
        "one conditional winner"
    );
    assert_eq!(
        a.err()
            .or_else(|| b.err())
            .expect("one conditional append must conflict")
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(ok(streams.len(&name).await, "winner offset"), 3);
}
