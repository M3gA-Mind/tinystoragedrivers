//! The blob step runner: each legacy blob is read, mapped by the step,
//! written under its new key and only then deleted.
//!
//! The stores are their own progress. A blob moves in two writes (put the
//! target, delete the source), so a crash between them leaves both; the next
//! run lists the source again, finds the target present and keeps it (a
//! target is never overwritten), then deletes the source. `tsd.migrations`
//! gets the step's running count after every blob.

use crate::error::{Result, StorageError};

use super::migrations::Progress;
use super::runner::Context;
use super::types::BlobStepFn;

pub(super) async fn run(
    context: &Context<'_>,
    run: &BlobStepFn,
    progress: Option<Progress>,
) -> Result<u64> {
    let step = context.step;
    let prefix = step.source.unwrap_or_default();
    let blobs = context.storage.blobs();
    let mut progress = progress.unwrap_or_else(|| Progress {
        owner: context.owner.to_string(),
        step: step.id.to_owned(),
        from: step.from,
        to: step.to,
        cursor: None,
        written: 0,
        done: false,
    });
    for listed in blobs.list(prefix).await? {
        // Listed a moment ago and gone now: another runner moved it.
        let Some(blob) = blobs.get(&listed.key).await? else {
            continue;
        };
        if let Some((target, content_type, bytes)) =
            run(listed.key.clone(), blob.meta.content_type, blob.bytes)?
        {
            check_target(context, &target)?;
            if blobs.head(&target).await?.is_none() {
                blobs.put(&target, bytes, content_type.as_deref()).await?;
            }
            progress.written += 1;
        }
        blobs.delete(&listed.key).await?;
        context.migrations.save(context.key, &progress).await?;
    }
    progress.done = true;
    context.migrations.save(context.key, &progress).await?;
    Ok(progress.written)
}

/// The target key must lie under the item's prefix.
fn check_target(context: &Context<'_>, target: &str) -> Result<()> {
    let item = context.step.item;
    if !target.starts_with(item) {
        return Err(StorageError::invalid_input(format!(
            "step `{}` of `{item}` mapped a blob to `{target}`, which is not under `{item}`",
            context.step.id
        )));
    }
    crate::blob::validate_user_blob_key(target)
}

#[cfg(test)]
#[path = "runner_blobs_tests.rs"]
mod tests;
