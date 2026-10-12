//! Run-ledger checks: the claim, compare-and-swap, release and retention
//! patterns a task or run ledger builds from the document port alone.
//!
//! A ledger worker claims the next eligible task, finishes it with a
//! version-guarded write, releases its claims on shutdown, and a janitor prunes
//! finished tasks past retention. None of this needs a port beyond
//! [`DocumentStore`](crate::DocumentStore); this suite pins that every driver
//! gives these patterns the same answers.

use serde_json::json;

use super::{empty, fails, ok, unique};
use crate::backend::ScopedStorage;
use crate::document::{Precondition, Query};
use crate::error::ErrorKind;
use crate::filter::{Filter, Sort};

#[allow(
    clippy::too_many_lines,
    reason = "one walk through a task's life: enqueue, claim, complete, release, prune"
)]
pub(super) async fn run(storage: &ScopedStorage) {
    let docs = storage.documents();
    let coll = unique("ledger");
    for (id, priority, seq) in [("t1", 2, 1), ("t2", 1, 2), ("t3", 1, 3), ("t4", 3, 4)] {
        ok(
            docs.put(
                &coll,
                id,
                json!({"state": "queued", "priority": priority, "seq": seq}),
                Precondition::Absent,
            )
            .await,
            "enqueue",
        );
    }
    let eligible = Filter::eq("state", "queued");
    let order = [Sort::asc("priority"), Sort::asc("seq")];
    let mine = json!({"state": "running", "owner": "worker-a"});
    let theirs = json!({"state": "running", "owner": "worker-b"});

    // Claims follow priority, then arrival, and never hand a task out twice.
    let first = ok(docs.claim(&coll, &eligible, &order, &mine).await, "claim 1")
        .expect("a task is claimable");
    let second = ok(
        docs.claim(&coll, &eligible, &order, &theirs).await,
        "claim 2",
    )
    .expect("a second task is claimable");
    assert_eq!(
        (first.id.as_str(), second.id.as_str()),
        ("t2", "t3"),
        "ties on priority fall to the next sort key"
    );

    // Completion is a compare-and-swap on the claimed version: the owner wins,
    // a holder of the stale version loses and learns it.
    let done = json!({"state": "done", "priority": 1, "seq": 2, "finished_at": 100});
    ok(
        docs.put(
            &coll,
            "t2",
            done.clone(),
            Precondition::Version(first.version),
        )
        .await,
        "complete with the claimed version",
    );
    fails(
        docs.put(&coll, "t2", done, Precondition::Version(first.version))
            .await,
        ErrorKind::Conflict,
        "complete twice with a stale version",
    );
    fails(
        docs.delete(&coll, "t2", Precondition::Version(first.version))
            .await,
        ErrorKind::Conflict,
        "delete with a stale version",
    );
    fails(
        docs.put(&coll, "t3", json!({}), Precondition::Absent).await,
        ErrorKind::Conflict,
        "enqueue over an existing task",
    );

    // Shutdown releases only this worker's running tasks back to the queue.
    let owned = Filter::eq("state", "running").and(Filter::eq("owner", "worker-b"));
    let release = json!({"state": "queued", "owner": null});
    let released = ok(
        docs.claim(&coll, &owned, &[], &release).await,
        "release a claim",
    )
    .expect("worker-b holds a task");
    assert_eq!(released.id, "t3");
    assert!(
        released.doc.get("owner").is_none(),
        "a null in the patch removes the field"
    );
    assert!(
        ok(
            docs.claim(&coll, &owned, &[], &release).await,
            "release done"
        )
        .is_none(),
        "nothing left to release"
    );
    let queued = ok(
        docs.query(&coll, &Query::filter(eligible.clone())).await,
        "requeued",
    );
    let queued: Vec<_> = queued.items.iter().map(|doc| doc.id.as_str()).collect();
    assert_eq!(queued, ["t1", "t3", "t4"], "the released task is claimable");

    // The released task is claimable again, and the claim stays the owner's.
    let again = ok(
        docs.claim(&coll, &eligible, &order, &theirs).await,
        "reclaim",
    )
    .expect("released task comes back");
    assert_eq!(again.id, "t3");
    assert_eq!(again.doc["owner"], "worker-b");

    // Retention prunes finished tasks older than the cutoff, and only those.
    ok(
        docs.put(
            &coll,
            "t5",
            json!({"state": "done", "finished_at": 900}),
            Precondition::None,
        )
        .await,
        "recent finished task",
    );
    let stale = Filter::eq("state", "done").and(Filter::lt("finished_at", 500));
    assert_eq!(ok(docs.delete_where(&coll, &stale).await, "prune"), 1);
    assert_eq!(ok(docs.delete_where(&coll, &stale).await, "prune again"), 0);
    assert!(ok(docs.get(&coll, "t2").await, "pruned").is_none());
    assert!(ok(docs.get(&coll, "t5").await, "kept").is_some());
    empty(
        &ok(
            docs.query(&coll, &Query::filter(stale)).await,
            "nothing stale left",
        )
        .items,
        "stale tasks",
    );
}
