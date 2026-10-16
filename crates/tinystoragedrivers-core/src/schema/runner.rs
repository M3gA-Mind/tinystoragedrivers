//! [`Registry::migrate`]: the resumable step runner.

use std::collections::{HashMap, HashSet};

use crate::backend::ScopedStorage;
use crate::document::{
    CollectionSpec, Cursor, DocumentStore, Precondition, Query, Version, WriteOptions,
};
use crate::error::{ErrorKind, Result, StorageError};
use crate::filter::{Filter, ID_FIELD, SCHEMA_FIELD, Sort};

use super::compose::Registry;
use super::migrations::{Migrations, Progress, migrations_spec, step_key};
use super::types::{Owner, SourceRecord, Step, TargetRecord};

/// How [`Registry::migrate`] runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrateOptions {
    /// Records per batch: the page size read from the source, and the unit of
    /// progress that survives a crash.
    pub batch_size: usize,
}

impl MigrateOptions {
    /// 100 records per batch.
    #[must_use]
    pub const fn new() -> Self {
        Self { batch_size: 100 }
    }

    /// Set the batch size (must be at least 1).
    #[must_use]
    pub const fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl Default for MigrateOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// A step that ran in this call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepReport {
    /// The owner of the step.
    pub owner: String,
    /// The collection the step brought forward.
    pub item: String,
    /// The step id.
    pub step: String,
    /// Records written in total, including batches finished by an earlier
    /// interrupted call.
    pub written: u64,
}

/// What [`Registry::migrate`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Steps that ran to completion in this call, in order.
    pub ran: Vec<StepReport>,
    /// Steps skipped because `tsd.migrations` already had them done.
    pub already_done: usize,
}

impl Registry {
    /// Run every pending step over `storage`'s scope, in schema order and step
    /// order, then open the migrated collections for writes.
    ///
    /// For each step the runner pages the source (the item itself, filtered to
    /// records below version `to`, or the legacy collection named by
    /// [`Step::source`]) by id with a keyset cursor, `batch_size` records at a
    /// time. It writes every output with `Precondition::Absent` and the
    /// schema version `to`; an output that already exists counts as written,
    /// which makes a re-run idempotent. An in-place rewrite of a record under
    /// the same id replaces it instead, guarded by the version it read. After
    /// each batch it stores `{cursor, written}` in `tsd.migrations`. Only when
    /// the source is exhausted does it delete a legacy source's rows and mark
    /// the step done. A crash or error anywhere leaves the progress of the
    /// last finished batch; calling `migrate` again resumes there and yields
    /// the same final state as an uninterrupted run, because steps are pure.
    ///
    /// Run one `migrate` per scope at a time (hold a lease): two concurrent
    /// runners are safe for the data (writes are idempotent) but waste work.
    /// The target collections must have been declared with
    /// [`Registry::prepare`]; the progress collection and a legacy source are
    /// declared here.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`] for a zero batch size or a step that emits
    /// a record for a collection its owner does not declare; whatever the step
    /// function or the document store returns otherwise.
    pub async fn migrate(
        &self,
        storage: &ScopedStorage,
        options: &MigrateOptions,
    ) -> Result<MigrationReport> {
        if options.batch_size == 0 {
            return Err(StorageError::invalid_input(
                "migration batch size must be at least 1",
            ));
        }
        let docs = storage.documents();
        docs.ensure_collection(&migrations_spec()).await?;
        let migrations = Migrations::new(storage);

        let mut plan = Vec::new();
        let mut remaining: HashMap<&str, usize> = HashMap::new();
        for (owner, step) in self.steps() {
            let key = step_key(owner.as_str(), step.item, step.id)?;
            let progress = migrations.load(&key).await?;
            if !progress.as_ref().is_some_and(|found| found.done) {
                *remaining.entry(step.item).or_default() += 1;
            }
            plan.push((owner, step, key, progress));
        }
        self.set_pending(
            remaining
                .keys()
                .map(|item| (*item).to_owned())
                .collect::<HashSet<_>>(),
        );

        let mut report = MigrationReport::default();
        for (owner, step, key, progress) in plan {
            if progress.as_ref().is_some_and(|found| found.done) {
                report.already_done += 1;
                continue;
            }
            let runner = StepRun {
                registry: self,
                owner,
                step,
                docs,
                batch_size: options.batch_size,
            };
            let written = runner.run(&migrations, &key, progress).await?;
            report.ran.push(StepReport {
                owner: owner.to_string(),
                item: step.item.to_owned(),
                step: step.id.to_owned(),
                written,
            });
            if let Some(left) = remaining.get_mut(step.item) {
                *left -= 1;
                if *left == 0 {
                    self.mark_ready(step.item);
                }
            }
        }
        Ok(report)
    }
}

struct StepRun<'a> {
    registry: &'a Registry,
    owner: &'a Owner,
    step: &'a Step,
    docs: &'a std::sync::Arc<dyn DocumentStore>,
    batch_size: usize,
}

impl StepRun<'_> {
    fn in_place(&self) -> bool {
        self.step.source_collection() == self.step.item
    }

    async fn run(
        &self,
        migrations: &Migrations,
        key: &crate::Key,
        progress: Option<Progress>,
    ) -> Result<u64> {
        let source = self.step.source_collection();
        if !self.in_place() {
            self.docs
                .ensure_collection(&CollectionSpec::new(source))
                .await?;
        }
        let mut progress = progress.unwrap_or_else(|| Progress {
            owner: self.owner.to_string(),
            step: self.step.id.to_owned(),
            from: self.step.from,
            to: self.step.to,
            cursor: None,
            written: 0,
            done: false,
        });
        // An in-place rewrite must not pick its own output up again, so it
        // reads only records still below the target version.
        let filter = if self.in_place() {
            Filter::lt(SCHEMA_FIELD, self.step.to)
        } else {
            Filter::default()
        };
        loop {
            let mut query = Query::filter(filter.clone())
                .sort(Sort::asc(ID_FIELD))
                .limit(self.batch_size);
            if let Some(cursor) = &progress.cursor {
                query = query.after(Cursor(cursor.clone()));
            }
            let page = self.docs.query(source, &query).await?;
            if !page.items.is_empty() {
                progress.written += self.write_batch(page.items).await?;
                progress.cursor = page.next.as_ref().map(|cursor| cursor.0.clone());
                migrations.save(key, &progress).await?;
            }
            if page.next.is_none() {
                break;
            }
        }
        // The source goes only now, with every batch written; a crash before
        // `done` is recorded re-runs against an emptier source and finishes.
        if !self.in_place() {
            self.docs.delete_where(source, &Filter::default()).await?;
        }
        progress.cursor = None;
        progress.done = true;
        migrations.save(key, &progress).await?;
        Ok(progress.written)
    }

    async fn write_batch(&self, items: Vec<crate::Versioned<serde_json::Value>>) -> Result<u64> {
        let versions: HashMap<String, Version> = items
            .iter()
            .map(|item| (item.id.clone(), item.version))
            .collect();
        let records = items
            .into_iter()
            .map(|item| SourceRecord {
                id: item.id,
                body: item.doc,
                meta: item.meta,
            })
            .collect();
        let outputs = (self.step.run)(records)?;
        // Validate every output before writing any, so a bad step leaves the
        // batch untouched.
        let schemas = outputs
            .iter()
            .map(|output| self.target_schema(output))
            .collect::<Result<Vec<_>>>()?;
        let count = outputs.len() as u64;
        for (output, schema) in outputs.into_iter().zip(schemas) {
            let mut write = WriteOptions::new().schema(schema);
            if let Some(created) = output.created_ms {
                write = write.created_ms(created);
            }
            let precondition = if self.in_place() && output.collection == self.step.item {
                versions
                    .get(&output.id)
                    .map_or(Precondition::Absent, |version| {
                        Precondition::Version(*version)
                    })
            } else {
                Precondition::Absent
            };
            match self
                .docs
                .put_with(
                    &output.collection,
                    &output.id,
                    output.body,
                    precondition,
                    write,
                )
                .await
            {
                // Written by an earlier, interrupted run of this step.
                Err(error)
                    if matches!(error.kind(), ErrorKind::AlreadyExists | ErrorKind::Conflict) => {}
                other => {
                    other?;
                }
            }
        }
        Ok(count)
    }

    /// The schema version stamped on an output, after checking it targets a
    /// collection of the step's owner.
    fn target_schema(&self, output: &TargetRecord) -> Result<u32> {
        let owned = output
            .collection
            .strip_prefix(self.owner.as_str())
            .is_some_and(|rest| rest.starts_with('.'));
        let info = self
            .registry
            .collections
            .get(&output.collection)
            .filter(|_| owned)
            .ok_or_else(|| {
                StorageError::invalid_input(format!(
                    "step `{}` of `{}` emitted a record for `{}`, which is not a collection declared by owner `{}`",
                    self.step.id, self.step.item, output.collection, self.owner
                ))
            })?;
        if self.in_place() && output.collection == self.step.source_collection() {
            return Ok(self.step.to);
        }
        Ok(if output.collection == self.step.item {
            self.step.to
        } else {
            info.version
        })
    }
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;
