//! Reads: paged queries, counts, the documents a bulk removal targets, and
//! full-text search.
//!
//! Every read evaluates the caller's filter AND the expiry rule. When the
//! translation is exact and every sort value is a scalar, MongoDB filters,
//! sorts and pages (`$skip`/`$limit`, offset paging like the memory driver).
//! Otherwise the server returns a superset, which is filtered, sorted and
//! paged in Rust with the reference semantics, still scoped server-side.

use mongodb::bson::{Bson, Document, doc, to_bson};
use mongodb::options::FindOptions;
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, CursorPosition, Filter, Result, SearchHit, Sort, StorageError, Version,
    Versioned, sort_documents_with_meta, value,
};

use super::MongoDocuments;
use super::stored::{Stored, live_filter, visible};
use crate::errors;
use crate::naming::{BODY, KEY};
use crate::scoped::ScopedCollection;
use crate::translate::{self, Translated};

/// One page of matches and whether more follow.
pub(super) type Matches = (Vec<Versioned<Value>>, bool);

fn decode_all(docs: &[Document]) -> Result<Vec<Stored>> {
    docs.iter().map(Stored::decode).collect()
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

impl MongoDocuments {
    /// `filter` AND the expiry rule, and its translation.
    fn effective(&self, spec: &CollectionSpec, filter: &Filter) -> (Filter, Translated) {
        let effective = filter.clone().and(live_filter(spec, self.shared.now()));
        let mut translated = translate::filter(&effective);
        translated.query = visible(translated.query);
        (effective, translated)
    }

    /// Every stored document the server query may match, narrowed in Rust to
    /// those `effective` matches.
    async fn candidates(
        &self,
        handle: &ScopedCollection,
        effective: &Filter,
        translated: &Translated,
    ) -> Result<Vec<Stored>> {
        let docs = handle
            .find(translated.query.clone(), FindOptions::default())
            .await
            .map_err(errors::failed("read documents"))?;
        Ok(decode_all(&docs)?
            .into_iter()
            .filter(|stored| !stored.deleted && effective.matches_with(&stored.key, &stored.body, &stored.meta))
            .collect())
    }

    /// The page of documents matching `filter` in `sort` order after `position`.
    pub(super) async fn page(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        position: Option<CursorPosition>,
        limit: Option<usize>,
    ) -> Result<Matches> {
        let spec = self.shared.spec(collection).await?;
        let handle = self.collection(collection);
        let (effective, translated) = self.effective(&spec, filter);
        if translated.exact
            && let Some(plan) = translate::sort_plan(sort)
        {
            let cursor_needs_reference_sort = position
                .as_ref()
                .is_some_and(|position| !cursor_is_scalar_bson_position(position));
            let complex = if sort.is_empty() {
                false
            } else {
                let found = handle
                    .aggregate(
                        translated.query.clone(),
                        vec![
                            doc! {"$addFields": plan.add_fields.clone()},
                            doc! {"$match": plan.complex.clone()},
                            doc! {"$limit": 1},
                            doc! {"$project": {"_id": 1}},
                        ],
                    )
                    .await
                    .map_err(errors::failed("inspect sort values"))?;
                cursor_needs_reference_sort || !found.is_empty()
            };
            if !complex {
                let mut stages = Vec::new();
                if !plan.add_fields.is_empty() {
                    stages.push(doc! {"$addFields": plan.add_fields});
                }
                if let Some(position) = &position {
                    stages.push(doc! {"$match": {"$expr": Self::seek_expression(sort, position)?}});
                }
                stages.push(doc! {"$sort": plan.sort});
                if let Some(limit) = limit {
                    stages.push(doc! {"$limit": to_i64(limit as u64).saturating_add(1)});
                }
                if !plan.unset.is_empty() {
                    stages.push(doc! {"$unset": plan.unset});
                }
                let docs = handle
                    .aggregate(translated.query, stages)
                    .await
                    .map_err(errors::failed("query documents"))?;
                let mut items: Vec<Versioned<Value>> = decode_all(&docs)?
                    .into_iter()
                    .map(Stored::into_versioned)
                    .collect();
                let more = limit.is_some_and(|limit| items.len() > limit);
                if let Some(limit) = limit {
                    items.truncate(limit);
                }
                return Ok((items, more));
            }
        }
        let mut found: Vec<Versioned<Value>> = self
            .candidates(&handle, &effective, &translated)
            .await?
            .into_iter()
            .map(Stored::into_versioned)
            .collect();
        sort_documents_with_meta(&mut found, sort, |item| {
            (item.id.as_str(), &item.doc, &item.meta)
        });
        let start = position.as_ref().map_or(0, |cursor| {
            found.partition_point(|item| {
                CursorPosition::for_record(&item.id, &item.doc, &item.meta, sort)
                    .compare(cursor, sort)
                    != std::cmp::Ordering::Greater
            })
        });
        let end = limit.map_or(found.len(), |limit| {
            start.saturating_add(limit).min(found.len())
        });
        let more = end < found.len();
        Ok((found.drain(start..end).collect(), more))
    }

    /// Build a lexicographic seek predicate over the computed sort rank/value
    /// fields. This path is used only after the sort plan confirms every
    /// matching value is scalar, so BSON value comparison agrees with core
    /// ordering within each type.
    pub(super) fn seek_expression(sort: &[Sort], position: &CursorPosition) -> Result<Bson> {
        let mut alternatives = Vec::with_capacity(sort.len() + 1);
        let mut prefix = Vec::with_capacity(sort.len() * 2);
        for (index, key) in sort.iter().enumerate() {
            let value = position
                .values
                .get(index)
                .unwrap_or(&serde_json::Value::Null);
            let rank = scalar_rank(value);
            let rank_ref = format!("$_tsd_r{index}");
            let value_ref = format!("$_tsd_k{index}");
            let operator = match key.direction {
                tinystoragedrivers_core::Direction::Asc => "$gt",
                tinystoragedrivers_core::Direction::Desc => "$lt",
            };
            let bson_value = to_bson(value).map_err(|error| {
                StorageError::serialization(error.to_string()).with_source(error)
            })?;
            let literal_value = doc! {"$literal": bson_value.clone()};
            let mut rank_advance = Document::new();
            rank_advance.insert(
                operator,
                Bson::Array(vec![Bson::String(rank_ref.clone()), Bson::Int32(rank)]),
            );
            let mut value_advance = Document::new();
            value_advance.insert(
                operator,
                Bson::Array(vec![
                    Bson::String(value_ref.clone()),
                    Bson::Document(literal_value.clone()),
                ]),
            );
            let advances = doc! {"$or": [
                rank_advance,
                {"$and": [
                    {"$eq": [&rank_ref, rank]},
                    value_advance,
                ]},
            ]};
            let mut clause = prefix.clone();
            clause.push(advances);
            alternatives.push(Bson::Document(doc! {"$and": clause}));
            prefix.push(doc! {"$eq": [&rank_ref, rank]});
            prefix.push(doc! {"$eq": [&value_ref, literal_value]});
        }
        let mut tied = prefix;
        tied.push(doc! {"$gt": ["$_key", {"$literal": position.id.as_str()}]});
        alternatives.push(Bson::Document(doc! {"$and": tied}));
        Ok(Bson::Document(doc! {"$or": alternatives}))
    }

    /// How many live documents match `filter`.
    pub(super) async fn count_matching(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let spec = self.shared.spec(collection).await?;
        let handle = self.collection(collection);
        let (effective, translated) = self.effective(&spec, filter);
        if translated.exact {
            return handle
                .count(translated.query)
                .await
                .map_err(errors::failed("count documents"));
        }
        Ok(self
            .candidates(&handle, &effective, &translated)
            .await?
            .len() as u64)
    }

    /// The `(key, version)` of every stored document `filter` matches, live
    /// or expired: the caller decides whether expiry applies.
    pub(super) async fn matching_pairs(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> Result<Vec<(String, Version)>> {
        let handle = self.collection(collection);
        let mut translated = translate::filter(filter);
        translated.query = visible(translated.query);
        Ok(self
            .candidates(&handle, filter, &translated)
            .await?
            .into_iter()
            .map(|stored| (stored.key, stored.version))
            .collect())
    }

    /// The live documents matching `filter`, as removal targets.
    pub(super) async fn live_pairs(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> Result<Vec<(String, Version)>> {
        let spec = self.shared.spec(collection).await?;
        let effective = filter.clone().and(live_filter(&spec, self.shared.now()));
        self.matching_pairs(collection, &effective).await
    }

    /// Full-text search over the declared fields, best first.
    pub(super) async fn search_hits(
        &self,
        collection: &str,
        text: &str,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        let spec = self.shared.spec(collection).await?;
        let Some(search) = &spec.search else {
            return Err(StorageError::invalid_input(
                "this collection declares no search fields",
            ));
        };
        if search.fields.is_empty() {
            // Declared but empty: nothing is searchable, as on the memory
            // driver, and no text index exists to ask.
            return Ok(Vec::new());
        }
        // Tokens only: the reference tokenizer's alphanumeric runs, so no
        // `-negation` or `"phrase"` operator reaches `$text`.
        let tokens = value::tokens(&Value::String(text.to_owned()));
        if tokens.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let handle = self.collection(collection);
        let (effective, translated) = self.effective(&spec, &Filter::All);
        let mut query = doc! {"$text": {"$search": tokens.join(" ")}};
        if !translated.query.is_empty() {
            query = doc! {"$and": [query, translated.query]};
        }
        let mut options = FindOptions::builder()
            .projection(doc! {"score": {"$meta": "textScore"}, KEY: 1, BODY: 1, "_v": 1})
            .sort(doc! {"score": {"$meta": "textScore"}, KEY: 1})
            .build();
        if translated.exact {
            options.limit = Some(to_i64(limit as u64));
        }
        let docs = handle
            .find(query, options)
            .await
            .map_err(errors::failed("search documents"))?;
        let mut hits = Vec::new();
        for doc in &docs {
            let stored = Stored::decode(doc)?;
            if effective.matches_with(&stored.key, &stored.body, &stored.meta) {
                hits.push(SearchHit {
                    id: stored.key,
                    score: doc.get_f64("score").unwrap_or_default(),
                });
            }
        }
        hits.truncate(limit);
        Ok(hits)
    }
}

/// Whether every cursor sort value can use Mongo's scalar BSON seek ordering.
/// The current rows may no longer contain the complex or unrepresentable
/// value that produced the cursor (for example, after that row was deleted).
fn cursor_is_scalar_bson_position(position: &CursorPosition) -> bool {
    position.values.iter().all(|value| {
        !matches!(
            value,
            serde_json::Value::Array(_) | serde_json::Value::Object(_)
        ) && to_bson(value).is_ok()
    })
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

fn scalar_rank(value: &serde_json::Value) -> i32 {
    match value {
        serde_json::Value::Null => 0,
        serde_json::Value::Bool(_) => 1,
        serde_json::Value::Number(_) => 2,
        serde_json::Value::String(_) => 3,
        serde_json::Value::Array(_) => 4,
        serde_json::Value::Object(_) => 5,
    }
}
