//! Stream sealing checks: reads are unchanged, appends are refused, sealing
//! is idempotent and a deleted stream can start over.
//!
//! Where the sealed blob lives is each driver's own business (a composite
//! backend keeps it beside the records, not in the blob backend), so these
//! checks never look for it; the driver tests do.

use serde_json::{Value, json};
use std::sync::Arc;

use super::{empty, fails, has, ok, unique};
use tinystoragedrivers_api::StreamStore;
use tinystoragedrivers_types::{Capability, ErrorKind, SealOutcome, StreamEntry};

const ENTRIES: u64 = 1000;
const PAGE: usize = 64;

/// Check sealing behavior and races for a stream engine with a stable handle.
pub async fn run(streams: &Arc<dyn StreamStore>) {
    if !has(streams.capabilities(), Capability::StreamSeal) {
        fails(
            streams.seal(&unique("seal_unsupported")).await,
            ErrorKind::Unsupported(Capability::StreamSeal),
            "seal on a driver without StreamSeal",
        );
        return;
    }
    missing_and_invalid(streams.as_ref()).await;
    reads_survive_sealing(streams.as_ref()).await;
    appends_are_refused(streams.as_ref()).await;
    truncated_streams_seal(streams.as_ref()).await;
    delete_removes_the_blob(streams.as_ref()).await;
    sealing_loses_no_concurrent_append(streams).await;
    sealing_never_resurrects_truncated_entries(streams).await;
    delete_sealed_stream_removes_only_the_inspected_incarnation(streams.as_ref()).await;
    info_records_times(streams.as_ref()).await;
}

/// Every entry of `stream`, read in pages of `PAGE`.
async fn paged(streams: &dyn StreamStore, stream: &str) -> Vec<StreamEntry> {
    let mut out = Vec::new();
    let mut from = 0;
    loop {
        let page = ok(streams.read_window(stream, from, PAGE).await, "page");
        let Some(last) = page.last() else {
            return out;
        };
        from = last.offset + 1;
        out.extend(page);
    }
}

fn values(count: u64) -> Vec<Value> {
    (0..count)
        .map(|i| json!({"i": i, "pad": "x".repeat(20)}))
        .collect()
}

async fn missing_and_invalid(streams: &dyn StreamStore) {
    fails(
        streams.seal(&unique("seal_missing")).await,
        ErrorKind::NotFound,
        "seal a stream that does not exist",
    );
    fails(
        streams.seal("").await,
        ErrorKind::InvalidInput,
        "seal an empty name",
    );
    assert_eq!(
        ok(
            streams.info(&unique("info_missing")).await,
            "info of a missing stream"
        ),
        None,
        "a stream nothing was appended to has no info"
    );
    fails(
        streams.info("").await,
        ErrorKind::InvalidInput,
        "info of an empty name",
    );
}

async fn reads_survive_sealing(streams: &dyn StreamStore) {
    let name = unique("seal/stream:1");
    ok(
        streams.append_batch(&name, values(ENTRIES)).await,
        "fill the stream",
    );
    let before = paged(streams, &name).await;
    assert_eq!(
        before.len() as u64,
        ENTRIES,
        "all entries read before sealing"
    );
    let whole_before = ok(
        streams.read_window(&name, 0, usize::MAX).await,
        "whole window",
    );
    let middle_before = ok(streams.read_window(&name, 333, 10).await, "middle");

    let outcome = ok(streams.seal(&name).await, "seal");
    assert_eq!((outcome.base, outcome.len), (0, ENTRIES), "seal outcome");
    assert!(outcome.blob_bytes > 0, "the outcome sizes the blob");

    assert_eq!(paged(streams, &name).await, before, "paged reads");
    assert_eq!(
        ok(streams.read_window(&name, 0, usize::MAX).await, "whole"),
        whole_before,
        "whole window after sealing"
    );
    assert_eq!(
        ok(streams.read_window(&name, 333, 10).await, "middle"),
        middle_before,
        "mid-stream window after sealing"
    );
    empty(
        &ok(streams.read_window(&name, ENTRIES, 10).await, "at the end"),
        "reading at the length",
    );
    empty(
        &ok(streams.read_window(&name, 5000, 10).await, "past the end"),
        "reading past the length",
    );
    assert_eq!(ok(streams.len(&name).await, "len"), ENTRIES, "length");
    assert!(
        ok(streams.streams(&name).await, "list").contains(&name),
        "a sealed stream is still listed"
    );

    let again = ok(streams.seal(&name).await, "seal again");
    assert_eq!(again, outcome, "sealing twice returns the same outcome");
    assert_eq!(paged(streams, &name).await, before, "after re-seal");
}

/// An append racing a seal either lands in the sealed stream or is refused;
/// it is never acknowledged and then lost with the entry rows.
async fn sealing_loses_no_concurrent_append(streams: &Arc<dyn StreamStore>) {
    const ATTEMPTS: u64 = 40;
    for round in 0..4 {
        let name = unique(&format!("seal_race{round}"));
        ok(streams.append_batch(&name, values(5)).await, "fill");
        let appender = |id: u64| {
            let (streams, name) = (streams.clone(), name.clone());
            async move {
                let mut acknowledged = Vec::new();
                for attempt in 0..ATTEMPTS {
                    let value = json!({"appender": id, "attempt": attempt});
                    match streams.append(&name, value.clone()).await {
                        Ok(_) => acknowledged.push(value),
                        Err(error) if error.kind() == ErrorKind::Conflict => break,
                        Err(error) => panic!("append during a seal: {error}"),
                    }
                }
                acknowledged
            }
        };
        let sealer = async {
            tokio::task::yield_now().await;
            // A driver that cannot lock appends out retries a bounded number of
            // times and may report a `Conflict` under sustained contention;
            // sealing again then succeeds, once the appenders have stopped.
            let mut last = None;
            for _ in 0..200 {
                match streams.seal(&name).await {
                    Ok(outcome) => return outcome,
                    Err(error) if error.kind() == ErrorKind::Conflict => last = Some(error),
                    Err(error) => panic!("seal during appends: {error}"),
                }
            }
            panic!("seal during appends kept conflicting: {last:?}")
        };
        let (a, b, c, outcome) = tokio::join!(appender(0), appender(1), appender(2), sealer);
        let stored: Vec<Value> = paged(streams.as_ref(), &name)
            .await
            .into_iter()
            .map(|entry| entry.value)
            .collect();
        let acknowledged: Vec<Value> = [a, b, c].concat();
        assert_eq!(
            stored.len() as u64,
            outcome.len - outcome.base,
            "the sealed stream holds what the seal reported"
        );
        for value in &acknowledged {
            assert!(
                stored.contains(value),
                "an acknowledged append {value} was lost by the seal"
            );
        }
        assert_eq!(
            stored.len(),
            5 + acknowledged.len(),
            "no entry beyond the acknowledged appends"
        );
    }
}

/// A truncation racing a seal is ordered one way or the other: the seal holds
/// the truncated stream, or the truncation is refused because the stream was
/// sealed first. An acknowledged truncation is never undone by the seal.
async fn sealing_never_resurrects_truncated_entries(streams: &Arc<dyn StreamStore>) {
    const CUTS: [u64; 3] = [1, 3, 5];
    for round in 0..8 {
        let name = unique(&format!("seal_trunc_race{round}"));
        ok(streams.append_batch(&name, values(6)).await, "fill");
        let truncator = async {
            let mut floor = 0;
            for cut in CUTS {
                tokio::task::yield_now().await;
                match streams.truncate_before(&name, cut).await {
                    Ok(_) => floor = cut,
                    Err(error) if error.kind() == ErrorKind::Conflict => break,
                    Err(error) => panic!("truncate during a seal: {error}"),
                }
            }
            floor
        };
        let sealer = async {
            for _ in 0..200 {
                match streams.seal(&name).await {
                    Ok(outcome) => return outcome,
                    Err(error) if error.kind() == ErrorKind::Conflict => {}
                    Err(error) => panic!("seal during a truncation: {error}"),
                }
            }
            panic!("seal during a truncation kept conflicting")
        };
        let (floor, _outcome) = tokio::join!(truncator, sealer);
        let offsets: Vec<u64> = paged(streams.as_ref(), &name)
            .await
            .into_iter()
            .map(|entry| entry.offset)
            .collect();
        assert!(
            offsets.iter().all(|offset| *offset >= floor),
            "an acknowledged truncation to {floor} was undone by the seal (round {round}): {offsets:?}"
        );
        assert_eq!(
            offsets.last().copied(),
            Some(5),
            "the tail survives (round {round})"
        );
    }
}

/// The blob a seal writes is not a caller's blob: it is not listed.
async fn appends_are_refused(streams: &dyn StreamStore) {
    let name = unique("seal_append");
    ok(streams.append_batch(&name, values(3)).await, "fill");
    ok(streams.seal(&name).await, "seal");
    fails(
        streams.append(&name, json!(1)).await,
        ErrorKind::Conflict,
        "append to a sealed stream",
    );
    fails(
        streams.append_batch(&name, values(2)).await,
        ErrorKind::Conflict,
        "append a batch to a sealed stream",
    );
    fails(
        streams.append_batch(&name, vec![]).await,
        ErrorKind::Conflict,
        "append an empty batch to a sealed stream",
    );
    if has(streams.capabilities(), Capability::ConditionalStreamAppend) {
        for expected in [3, 0] {
            fails(
                streams
                    .append_batch_if_offset(&name, expected, values(1))
                    .await,
                ErrorKind::Conflict,
                "conditional append to a sealed stream",
            );
        }
    }
    fails(
        streams.truncate_before(&name, 1).await,
        ErrorKind::Conflict,
        "truncate a sealed stream",
    );
    assert_eq!(
        ok(streams.len(&name).await, "len"),
        3,
        "refused writes change nothing"
    );
    assert_eq!(
        ok(streams.read_window(&name, 0, 10).await, "read").len(),
        3,
        "refused writes keep the entries"
    );
}

async fn truncated_streams_seal(streams: &dyn StreamStore) {
    let partial = unique("seal_partial");
    ok(streams.append_batch(&partial, values(5)).await, "fill");
    ok(streams.truncate_before(&partial, 3).await, "truncate");
    let before = ok(streams.read_window(&partial, 0, 10).await, "read");
    let outcome = ok(streams.seal(&partial).await, "seal a truncated stream");
    assert_eq!(
        outcome,
        SealOutcome::new(3, 5, outcome.blob_bytes),
        "outcome"
    );
    assert_eq!(outcome.retained(), 2);
    assert_eq!(
        ok(streams.read_window(&partial, 0, 10).await, "read sealed"),
        before,
        "offsets below the base stay skipped"
    );

    let drained = unique("seal_drained");
    ok(streams.append_batch(&drained, values(3)).await, "fill");
    ok(streams.truncate_before(&drained, 99).await, "truncate all");
    let outcome = ok(streams.seal(&drained).await, "seal a drained stream");
    assert_eq!((outcome.base, outcome.len, outcome.retained()), (3, 3, 0));
    assert_eq!(
        ok(streams.len(&drained).await, "len"),
        3,
        "len keeps counting"
    );
    empty(
        &ok(streams.read_window(&drained, 0, 10).await, "read"),
        "a drained sealed stream",
    );
    fails(
        streams.append(&drained, json!(1)).await,
        ErrorKind::Conflict,
        "append to a drained sealed stream",
    );
}

async fn delete_removes_the_blob(streams: &dyn StreamStore) {
    let name = unique("seal_delete");
    ok(streams.append_batch(&name, values(4)).await, "fill");
    ok(streams.seal(&name).await, "seal");
    assert!(ok(streams.delete_stream(&name).await, "delete"), "existed");
    assert_eq!(ok(streams.len(&name).await, "len"), 0, "gone");
    empty(
        &ok(streams.read_window(&name, 0, 10).await, "read"),
        "a deleted sealed stream",
    );
    assert!(!ok(streams.delete_stream(&name).await, "delete again"));
    assert_eq!(
        ok(streams.append(&name, json!("fresh")).await, "recreate"),
        0,
        "a deleted sealed stream starts over unsealed"
    );
    assert_eq!(ok(streams.read_window(&name, 0, 10).await, "read").len(), 1);
}

/// `delete_sealed_stream` removes a stream only while it is the sealed
/// incarnation that was inspected, so a stream recreated since survives.
async fn delete_sealed_stream_removes_only_the_inspected_incarnation(streams: &dyn StreamStore) {
    let name = unique("seal_cond_delete");
    assert!(
        !ok(
            streams.delete_sealed_stream(&name, 1).await,
            "conditional delete of a missing stream"
        ),
        "a missing stream is not deleted"
    );
    fails(
        streams.delete_sealed_stream("", 1).await,
        ErrorKind::InvalidInput,
        "conditional delete of an empty name",
    );

    ok(streams.append_batch(&name, values(3)).await, "fill");
    assert!(
        !ok(
            streams.delete_sealed_stream(&name, 1).await,
            "conditional delete of an open stream"
        ),
        "an unsealed stream is not deleted"
    );
    assert_eq!(ok(streams.len(&name).await, "len"), 3, "open stream kept");

    ok(streams.seal(&name).await, "seal");
    let first = ok(streams.info(&name).await, "info")
        .and_then(|info| info.sealed_ms)
        .expect("sealed time");
    assert!(
        !ok(
            streams.delete_sealed_stream(&name, first + 1).await,
            "conditional delete at another instant"
        ),
        "a different sealed instant does not match"
    );
    assert_eq!(ok(streams.len(&name).await, "len"), 3, "sealed stream kept");

    // Another actor deletes and recreates the stream after it was inspected.
    assert!(ok(streams.delete_stream(&name).await, "delete"));
    ok(streams.append(&name, json!("fresh")).await, "recreate");
    assert!(
        !ok(
            streams.delete_sealed_stream(&name, first).await,
            "conditional delete of a recreated stream"
        ),
        "the new incarnation is not the one inspected"
    );
    assert_eq!(ok(streams.len(&name).await, "len"), 1, "fresh stream kept");

    // Recreated and sealed again later: still not the inspected instant.
    clock_past(streams, first).await;
    ok(streams.seal(&name).await, "seal again");
    let second = ok(streams.info(&name).await, "info")
        .and_then(|info| info.sealed_ms)
        .expect("sealed time");
    assert!(second > first, "a later seal records a later instant");
    assert!(
        !ok(
            streams.delete_sealed_stream(&name, first).await,
            "conditional delete with the old instant"
        ),
        "the old instant does not match the new incarnation"
    );
    assert!(
        ok(
            streams.delete_sealed_stream(&name, second).await,
            "conditional delete at the sealed instant"
        ),
        "the matching incarnation is deleted"
    );
    assert_eq!(ok(streams.info(&name).await, "info"), None, "stream gone");
    assert_eq!(
        ok(
            streams.append(&name, json!("again")).await,
            "reuse the name"
        ),
        0,
        "a deleted sealed stream starts over unsealed"
    );
}

/// Wait until the driver's own clock reads later than `stamp_ms`, observed
/// through the `last_append_ms` of probe appends to a scratch stream, so the
/// next timestamp is strictly later on a clock of any resolution.
async fn clock_past(streams: &dyn StreamStore, stamp_ms: i64) {
    let probe = unique("seal_tick");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        ok(streams.append(&probe, json!("tick")).await, "clock probe");
        let seen = ok(streams.info(&probe).await, "info").and_then(|info| info.last_append_ms);
        if seen.is_some_and(|ms| ms > stamp_ms) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the driver clock never passed {stamp_ms}: it must advance"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

/// `info` reports extent, the time of the last append and the time of the seal.
async fn info_records_times(streams: &dyn StreamStore) {
    let name = unique("seal_info");
    ok(streams.append_batch(&name, values(3)).await, "fill");
    let first = ok(streams.info(&name).await, "info").expect("info of a stream");
    assert_eq!((first.base, first.len), (0, 3), "extent");
    assert_eq!(first.sealed_ms, None, "an open stream is not sealed");
    let first_append = first.last_append_ms.expect("an append is timestamped");
    assert!(first_append > 1_500_000_000_000, "epoch milliseconds");

    clock_past(streams, first_append).await;
    ok(streams.append(&name, json!("later")).await, "append");
    let second = ok(streams.info(&name).await, "info").expect("info");
    assert_eq!(second.len, 4, "length advances");
    assert!(
        second.last_append_ms.expect("timestamped") > first_append,
        "last_append_ms advances on every append"
    );

    ok(
        streams.truncate_before(&name, 2).await,
        "truncate keeps times",
    );
    let truncated = ok(streams.info(&name).await, "info").expect("info");
    assert_eq!((truncated.base, truncated.len), (2, 4), "truncated extent");
    assert_eq!(truncated.last_append_ms, second.last_append_ms);

    clock_past(streams, second.last_append_ms.expect("timestamped")).await;
    let outcome = ok(streams.seal(&name).await, "seal");
    let sealed = ok(streams.info(&name).await, "info").expect("info");
    let sealed_ms = sealed.sealed_ms.expect("seal records its time");
    assert!(sealed_ms > second.last_append_ms.expect("timestamped"));
    assert_eq!((sealed.base, sealed.len), (outcome.base, outcome.len));
    assert_eq!(
        sealed.last_append_ms, second.last_append_ms,
        "seal is not an append"
    );

    ok(streams.seal(&name).await, "seal again");
    let again = ok(streams.info(&name).await, "info").expect("info");
    assert_eq!(
        again.sealed_ms,
        Some(sealed_ms),
        "sealing twice keeps the first time"
    );

    ok(streams.delete_stream(&name).await, "delete");
    assert_eq!(ok(streams.info(&name).await, "info"), None, "deleted");
    ok(streams.append(&name, json!(1)).await, "recreate");
    let fresh = ok(streams.info(&name).await, "info").expect("info");
    assert_eq!(fresh.sealed_ms, None, "a recreated stream is open");
}
