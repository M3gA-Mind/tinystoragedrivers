# Uniform schema (v2)

Status: Draft. Owner: TinyStorageDrivers. Host integration: OpenHuman.
Implementation order: [`../plans/schema-v2.md`](../plans/schema-v2.md).

## Problem

The ports in [`storage-ports.md`](storage-ports.md) fix how records are stored.
They say nothing about what the records look like, and every crate that
adopted them invented its own conventions. A survey of OpenHuman and the
modules it hosts (2026-10, tinystoragedrivers v0.11.0) found the following.

**One tenant addressed by five scope grammars.**
- Domain stores use `profile:<id>`, `profile-sha256:<hex>` or `local`.
- Sessions use `<profile>~<agent>`, or `default` when there is no agent.
- Conversations use `profile:<id>` and drop the agent.
- Modules use `module:<id>@<base>`, `<profile>~module:<id>` or `module-sha256:<hex>`.
- Leases use `cluster`.
- Legacy importers write to `local`, where a profile-scoped reader never looks.
- A hashed session scope (`sha256:<hex>`) falls outside the profile fence, which guards the prefix `<profile>~`.

**Four hash fallbacks and six key schemes.** Fallbacks are `sha256:`, `profile-sha256:`, `module-sha256:` and `h:<hex>`. Keys are built as:
- `<len>:<ns>/<key>`
- `<len>:<prefix><name>`
- `<len>:p/…`
- sha256 of length-prefixed parts
- in-body `scope_key`/`workspace` fields
- collection-name prefixes

Stream names use `/`, `:` and `.` as separators interchangeably.

**Five time encodings, often for the same instant.** Seconds, ms, µs, ns and RFC 3339 strings all appear. Examples of one instant stored twice or three times: `next_run_ms` + `next_run_ns`, `ingested_ms` + `ingested_ns` + `ingested_at`. Several indexes are built on RFC 3339 strings.

**Mixed casing and wrappers.**
- snake_case and camelCase bodies coexist, sometimes in one record (`created_at` next to `updatedAt`).
- Values are wrapped as `{"value": …}`, `{"json": "<string>"}` or `record: {…}`.
- Journal rows repeat `run_id`, `root_run_id` and `event_id` in every entry.

**Collection names follow no rule.** Some have a domain prefix and some do not (`approvals`, `artifacts`, `leases`, `config`). Siblings disagree: `channels_*` and `channel_remote_*`, `wf_*` and `flows_*`. One name is kebab case (`composio-user-scopes`) and one family carries a version suffix (`conversation_*_v1`).

**Primitives implemented again and again.**
- Leases and claims (7 shapes): `leases`, `cron_occurrence_claims`, an embedded `occurrence_lease`, `<prefix>_leases`, `wf_locks`, `#claim` documents, `reserved_at`.
- Import markers (10 collections), and the marker `version` is never read.
- Key-value stores (5).
- Counters (3).
- Graph checkpointers (2).
- CAS retry loops (budgets of 16, 32 and 64).

**Growth nobody bounds.** Finished run journals are kept forever as one row per event; a desktop profile reached 3.6M rows and 1.4 GB. Deleted rows never return space, because SQLite never vacuums.

The consequence: data cannot be moved between backends or users without per-domain code, a tenant cannot be listed or purged by prefix, and every new store re-decides the same questions.

## Goals

- One addressing scheme: scope, collection, id, stream and blob key.
- One body convention: bare snake_case JSON, epoch-ms times.
- Record metadata (created, updated, expiry, schema version) kept by tsd, not by each body.
- A schema registry: every owner declares its collections, and tsd validates, prepares and migrates them.
- Shared primitives for leases, migrations, key-value, counters and CAS updates, so owners stop reimplementing them.
- Sealing for finished streams, and space reclamation on SQLite.
- A user's SQLite file is a complete, portable unit: it loads into a shared MongoDB with no rewrite.
- A deterministic, resumable migration from every v1 shape listed in the [catalogue](#catalogue).

## Non-goals

- Typed repositories. They stay with the owning crates; this spec fixes their naming and storage shape only.
- Cross-backend replication or live dual-writes.
- The host's on-disk layout and the legacy-file importers (OpenHuman owns those). This spec only requires that a host can place one database file per user.
- Changing RPC or UI wire formats. Only the stored form changes.

## Proposed behavior

### 1. Scopes

```text
scope     = tenant [ "/" principal ]  |  "cluster"
tenant    = "local"  |  "u:" segment
principal = ( "agent:" | "module:" ) segment
segment   = 1*( ALPHA / DIGIT / "-" / "_" / "." / "@" )   ; at most 96 bytes
          | "~" 26( base32-lower )                         ; hashed form
```

- **`tenant` is the isolation and fence boundary.** `local` is the desktop operator; `u:<id>` is a user or profile. Every scope that begins `<tenant>` or `<tenant>/` belongs to that tenant.
- **`principal` narrows a record to one agent or one module inside the tenant.** No principal means the record is shared across the tenant.
- **`cluster` is the only scope with no tenant.** It holds the profile registry and process-placement leases on shared backends.
- **Hashing.** A segment that is too long or holds characters outside the set is replaced by `~` plus the first 26 characters of the lowercase base32 SHA-256 of the raw segment. Only the segment is hashed, never the whole scope, so a hashed agent still sits under its tenant and its fence.
- **API.** `Scope::tenant(id)`, `Scope::local()`, `Scope::cluster()`, `scope.agent(id)`, `scope.module(id)`, `scope.tenant_scope()` and `Scope::parse(text) -> Result<Parsed>`. `Scope::new` continues to accept any valid v1 string, so v1 data stays readable during migration. It is deprecated for new writes.
- **Tenant purge.** A tenant purge covers the tenant scope and every principal under it. `StorageBackend::scopes(prefix)` (capability `ScopeEnumeration`) lists them, and the host drives the purge (see [`scope-purge.md`](scope-purge.md)).

### 2. Names

| Thing | Form | Example |
|---|---|---|
| Collection | `<owner>.<entity>` | `harness.run_status`, `cron.jobs` |
| Stream | `<owner>.<stream>` or `<owner>.<stream>/<key>` | `mcp.audit`, `harness.journal/run.0006…` |
| Blob key | `<owner>/<path>` or `cas/<sha256-hex>` | `juice/ccr/<token>` |
| Document id | `Key::of([parts])` or a ULID | `job.7f…/2026-10-16T10:00Z` |

- **`owner`** is the crate or domain that defines the record type: lowercase, `[a-z][a-z0-9_]{1,23}`.
- **`entity`** is a lowercase snake_case plural noun.
- **No version suffixes.** The version lives in the [registry](#4-schema-registry).
- **The owner `tsd` is reserved** for the shared primitives (§5). The prefix `_tsd` stays reserved for driver internals.
- **`Key::of(parts)`** joins parts with `/`, escaping `%`, `/` and NUL as `%25`, `%2F` and `%00`. If the result exceeds 512 bytes it becomes `~` plus the base32 SHA-256 of the escaped key. `Key::split` reverses an unhashed key.
- **The length-prefixed and `h:` schemes are retired.**
- **New entity ids are ULIDs** (`Key::ulid()`): 26 characters, sortable by creation time. That makes `_ns` tie-break fields unnecessary.

### 3. Bodies and record metadata

**Bodies**
- A document body is a JSON object holding the record's own fields at top level. No `value`, `json` or `record` wrapper, and no JSON encoded inside a string. Bytes are stored as a blob and referenced by key.
- Field names are snake_case. A type whose RPC form is camelCase uses a separate storage DTO, or `#[serde(rename_all = "snake_case")]` when the type is storage-only.
- A time is an `i64` count of Unix epoch milliseconds, in a field named `<what>_ms`, for example `next_run_ms`. One field per instant. Durations are `<what>_ms` too. Storage never holds seconds, µs, ns or formatted dates.
- Stream entries do not repeat their stream key. A journal entry is `{kind, ts_ms, …}`: its run id is the stream name and its offset is the sequence number.

**Metadata.** tsd keeps four fields per document, outside the body:

| Field | Meaning | Set by |
|---|---|---|
| `schema` | body version the writer declared (§4) | the handle, from the registry |
| `created_ms` | first write | the driver |
| `updated_ms` | last write | the driver |
| `expires_ms` | optional expiry; past it, the record reads as absent | the writer |

- **Reading.** `Document` gains `meta: Meta { schema, created_ms, updated_ms, expires_ms }`. Filters and sorts address the fields as `_schema`, `_created_ms`, `_updated_ms` and `_expires_ms`, alongside `_id`.
- **Precedence over bodies.** A body keeps its own time field only when it means something other than creation, update or expiry (`next_run_ms`, `decided_ms`).
- **Expiry.** `expires_ms` replaces `CollectionSpec::ttl(field)`. A v1 TTL field is honoured until its collection migrates, and the migration step moves its value into `expires_ms`.
- **Who sets the times.** `created_ms` and `updated_ms` come from the driver clock, never from the caller. A migration step can supply `created_ms` when it rewrites a v1 record, so history keeps its original times.

### 4. Schema registry

```rust
pub struct Schema { owner: Owner, items: Vec<Item>, steps: Vec<Step> }
pub enum Item {
    Collection { name, version: u32, indexes, search, scope: ScopeClass },
    Stream     { name, scope: ScopeClass, seal: SealPolicy },
    Blobs      { prefix, scope: ScopeClass },
    Secrets    { prefix, scope: ScopeClass },
}
pub enum ScopeClass { Tenant, Agent, Module, Cluster }
pub struct Step { id: &'static str, from: u32, to: u32, run: StepFn }
```

**What every owner provides.** Each owning crate exports `pub fn storage_schema() -> Schema`.

**Composition.** The host builds `Registry::compose([schemas])`. Composition refuses:
- a name outside its owner's prefix
- a name claimed twice
- a version that goes down
- a step gap

Composition then runs `ensure_collection` once for each item, from trusted bootstrap. This is the only place declarations are made.

**Migration steps**
- A `Step` rewrites one collection, stream or prefix from version `from` to version `to`. It receives a batch of v1 records (body, meta, scope, id) and returns the replacement records with their new ids and names. It may also emit records for other items of the same owner.
- The runner pages through the source with a keyset cursor and writes each output batch with `Precondition::Absent`. It stores progress `{step, cursor, written}` in `tsd.migrations` after every batch, and deletes the source only when the step completes. A crash resumes from the cursor.
- Steps are pure functions of their input, so a re-run produces the same output.
- A collection whose recorded version is below the declared one is not opened for writes until its steps finish (`ErrorKind::Unavailable`, message `migrating <owner>.<item>`). Reads of an unmigrated collection go to the v1 source.
- Legacy-file importers are steps too: steps with no source item, whose completion is recorded in `tsd.migrations`. This replaces every `*_import` marker collection.

**Module grants are the module's schema.** A host grants a loadable module exactly the items in its `Schema`, scoped to `ScopeClass::Module`, so grant tables are generated rather than hand-written.

### 5. Shared primitives (owner `tsd`)

| Collection | API | Body |
|---|---|---|
| `tsd.leases` | `Leases::acquire(key, holder, ttl) / renew / release / current` | `{holder, epoch, expires_ms}`; `epoch` doubles as the write fence ([`write-fencing.md`](write-fencing.md)) |
| `tsd.migrations` | used by the registry runner; `Migrations::done(owner, step)` for importers | `{owner, step, from, to, cursor, written, done}` |
| `tsd.kv` | `Kv::ns(owner, ns).get/put/delete/list(prefix)` | the value itself; id `Key::of([owner, ns, key])` |
| `tsd.counters` | `Counters::next(owner, name) -> u64` (atomic) | `{value}` |
| `tsd.secrets` | `SecretStore` over documents (exists as `secrets`) | `{ciphertext}` |
| `tsd.secret_keys` | KMS envelope (exists as `secret_keys`) | `{format, key_id, wrapped}` |

Two shared helpers are not collections:
- **`Repo<T>`** is a typed collection handle: `get`, `put`, `insert`, `update(id, f)` and `query`. `update` runs one compare-and-swap loop with a budget of 32 and jittered backoff. Every owner uses it instead of its own loop.
- **The graph checkpointer** exists once, in tinyagents-graph (`graph.checkpoints`, `graph.threads`, `graph.writes`). Each user passes a namespace field instead of copying the checkpointer.

### 6. Streams: sealing and retention

- **Sealing.** `StreamStore::seal(name)` closes a stream to appends. It writes the entries as one zstd-compressed JSON array blob under `_tsd/sealed/<stream>`, records that in the stream head, and deletes the entry rows.
  - Reads (`read`, `read_window`, `len`) behave exactly as before and return the same offsets and values.
  - Appending to a sealed stream is `ErrorKind::Conflict`.
  - Sealing is idempotent and survives a crash at any point: the blob is written before the head changes, and rows are deleted only after.
- **Seal policy.** A `SealPolicy` on a `Stream` item states when the owner seals:
  - `OnFinish`: the owner calls `seal`
  - `Idle(ms)`: tsd seals streams untouched for that long, during maintenance
  - `Never`

  It also states a retention period: `delete_stream` after the stream has been sealed for N ms.
- **Maintenance.** `Maintenance::run(budget)` (capability `Maintenance`) applies seal and retention policies and reclaims space:
  - SQLite runs `PRAGMA incremental_vacuum` with `auto_vacuum=INCREMENTAL`, set on creation and on the v2 physical migration.
  - MongoDB and File return `Unsupported`, which callers treat as nothing to do.
- **`checkpoint(Truncate)`** flushes the SQLite WAL. Call it before moving a database file.

### 7. Physical layout

**SQLite**: one database file.

```text
docs(scope, coll, id, version, schema, created_ms, updated_ms, expires_ms,
     deleted, body)                     PK (scope, coll, id)
streams(scope, name, seq, body)         PK (scope, name, seq)
stream_heads(scope, name, base, len, sealed)
blobs(scope, key, content_type, size, sha256, expires_ms, bytes)
fts                                     FTS5, searchable collections only
meta(key, value)                        registry snapshot, versions, layout
```

- `deleted` replaces `_tsd_tombstones`: a deleted id keeps its row with an empty body, so versions continue across deletion as the ports require.
- `body` is UTF-8 JSON text. A body over 4 KiB is stored zstd-compressed, with a one-byte marker; drivers decode it transparently.
- Index and filter evaluation over compressed bodies uses SQLite generated columns, declared per index from the registry.
- Named databases (`StorageBackend::database`) are removed: scope plus owner prefix give the separation they were meant for, and no caller uses them.

**MongoDB**:
- The same scopes, names and metadata.
- Document fields are `_scope`, `_key`, `_v`, `_schema`, `_c`, `_u`, `_x` (expires) and `_del`, with the body under `d`.
- TTL indexes use `_x`.

**File driver**: `scopes/<scope>/{docs/<coll>/<id>.json, streams/<name>.jsonl, blobs/<key>}`, with metadata in a `.meta.json` sidecar. It exists for tests and inspection, not production.

**Physical migration.** On open, the SQLite driver upgrades a v0.x file (`_tsd_*` tables):
- it renames tables in place
- it folds tombstones into `deleted`
- it adds the metadata columns, backfilling `created_ms`/`updated_ms` from the body when a recognised v1 field exists, else from the open time
- it sets `auto_vacuum`

The upgrade runs as one transaction, records `meta.layout = 2` and is not reversible. A backup copy is the host's responsibility.

## Invariants

1. Every record's scope parses under §1. Tenants never nest, and no tenant's scope is a prefix of another tenant's: `u:a` and `u:ab` are distinct because the tenant ends at `/`.
2. No two owners declare the same name. Every name starts with its owner's prefix.
3. Every stored time is epoch ms in a field named `_ms`, or tsd metadata.
4. A migration step is deterministic and resumable. Running it twice changes nothing.
5. Sealing never changes what a reader observes: offsets, values and lengths stay the same.
6. A v2 database copied to another backend of the same major version reads identically.

## Acceptance criteria

- The conformance suite gains `schema` cases, and they pass on memory, SQLite, File and MongoDB:
  - scope grammar and hashing
  - `Key::of` and its reversal
  - metadata on write and read, including `_created_ms` filters and sorts
  - `expires_ms`
  - registry composition refusals
  - step resume after an injected failure
  - leases, kv and counters under contention
  - seal and read equality
  - retention
- The SQLite physical migration passes against a v0.11.0 fixture database with every table populated.
- `Maintenance::run` reduces a SQLite file's size after a bulk delete.
- Every collection in the [catalogue](#catalogue) has a v2 name, an owner and a migration step in its owning repository.
- OpenHuman's `scripts/ci/check-storage-schema.mjs` passes. It checks names, single ownership, casing and the `_ms` rule, and generates `docs/storage-schema.md`.

## Open questions

1. Whether a body over 4 KiB should be stored as SQLite JSONB (3.45+) instead of zstd text. JSONB keeps `json_extract` cheap; zstd saves more space. The current default is zstd plus generated columns for indexed fields.
2. Whether `harness.journal` retention (default 30 days) belongs in the owner's `SealPolicy` or in host configuration. The proposal is owner default, host override.

## Catalogue

The table maps every v1 name to its v2 name. Each owner implements the step for its rows, in its own repository.

**tsd and OpenHuman host stores**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `secrets`, `secret_keys` | `tsd.secrets`, `tsd.secret_keys` | Tenant | rename only |
| `leases` (cluster) | `tsd.leases` | Cluster | `expires_at_ms` becomes `expires_ms` |
| `storage_import`, `threads_import`, `*_host_import`, `session_import_markers`, `*_state_import`, `subagent_sessions_import` | `tsd.migrations` | Tenant | one entry per completed importer |
| `storage_agents` | `agents.registry` | Tenant | |
| `profiles` | `profiles.registry` | Cluster | `created_at` (seconds) moves to metadata |
| `config` | `config.documents` | Tenant | `toml_enc` stays (an encrypted value) |
| `approvals`, `approval_flow_trust` | `approvals.requests`, `approvals.flow_trust` | Tenant | RFC 3339 becomes `_ms`; uuid becomes ULID for new rows only |
| `paired_devices` | `devices.paired` | Tenant | |
| `integration_notifications`, `notification_dedup`, `notification_settings`, `core_notifications` | `notifications.integration`, `.dedup`, `.settings`, `.core` | Tenant | drop `received_at`; keep `received_ms` |
| `task_sources`, `ingested_tasks` | `task_sources.sources`, `.ingested` | Tenant | drop `_ns`/`_at` copies |
| `composio_state` | `tsd.kv` ns `composio` | Tenant | unwrap `value` |
| `artifacts`, `artifact_args` | `artifacts.records`, `.args` | Tenant | unwrap `args` |
| `subagent_sessions` | `subagents.sessions` | Agent | `updatedAt` becomes `updated_ms` metadata |
| `cost_records` | `cost.records` | Tenant | `ts_ms` kept |
| `tinymemes_documents` | `memes.state` | Tenant | parse the `json` string into the body |
| `threads_transcript_keys` | `threads.transcript_keys` | Tenant | |
| `cron_occurrence_claims` | `tsd.leases` | Tenant | key `Key::of(["cron", job, occurrence_ms])` |
| `delegation_graph_*` | `graph.*` ns `delegation` | Agent | |

**tinyagents**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `session_kv` | `tsd.kv` (owner `session`, ns as today) | Agent | unwrap `value`; drop `<len>:` keys |
| stream `16:session_journal/<run>` | stream `harness.journal/<run>` | Agent | drop repeated `run_id`, `root_run_id` and `event_id`; seal finished runs |
| `harness_run_status` | `harness.run_status` | Agent | unwrap `record`; id is the run id |
| `harness_embedding_cache` | `harness.embedding_cache` | Agent | |
| `attachment_stash_index`, blobs `attachments/…` | `harness.attachment_stash`, blobs `harness/attachments/…` | Agent | |
| blobs `tool-results/…`, `outputs/…`, `workspace/…`, `generated-media/…` | `harness/tool_results/…` and so on | Agent | |
| `session_transcripts`, `session_transcript_entries` | `session.transcripts`, `session.transcript_entries` | Agent | `created_at` and `reserved_at` move to metadata and `tsd.leases` |
| `session_turn_states` | `session.turn_states` | Agent | |
| `run_ledger_*` | `ledger.*` (`workflow_runs`, `telemetry`, `agent_runs`, `teams`, `members`, `tool_effects`, `event_outbox`, `event_heads`, `tasks`, `entry_heads`, `entries`, `entry_labels`, `entry_indexes`) | Agent | unwrap `record`; camelCase becomes snake_case; drop `*Sql` copies |
| `run_ledger_agent_imports` | `tsd.migrations` | Agent | |
| stream `run_events:<sha>` | `ledger.events/<run>` | Agent | |
| `session_history_{sessions,messages,tool_calls,fts}` | `history.{sessions,messages,tool_calls,search}` | Agent | µs becomes ms |
| `session_history_counters` | `tsd.counters` | Agent | |
| `conversation_threads_v1`, `conversation_appends_v1`, streams `conversation_messages_v1:<sha>:<inc>` | `conversations.threads`, `.appends`, streams `conversations.messages/<thread>/<inc>` | Tenant | |
| `<prefix>_{checkpoints,threads,writes,leases}` | `graph.{checkpoints,threads,writes}` plus `tsd.leases` | Agent | prefix becomes a `ns` field |
| `orchestration_tasks`, `orchestration_task_history`, `orchestration_completions` | `tasks.records`, `tasks.history`, `tasks.completions` | Agent | `updatedAtMs` moves to metadata |
| `harness_store`, `harness_namespaced_store`, `harness_response_cache`, `graph_task_cache` | `tsd.kv` (owner `harness` or `graph`) | Agent | |

**tinyflows**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `flows_{definitions,revisions,runs,suggestions,drafts}` | `flows.{definitions,revisions,runs,suggestions,drafts}` | Tenant | drop `*_ns`; keep `_ms`; ULIDs give ordering |
| `flows_kv`, `flows_state` | `tsd.kv` (owner `flows`) | Tenant | |
| `flows_graph_*` | `graph.*` ns `flows` | Tenant | |
| `cron_jobs`, `cron_runs` | `cron.jobs`, `cron.runs` | Tenant | drop `next_run_ns`; embedded `occurrence_lease` moves to `tsd.leases` |
| `cron_counters`, `adaptive_counters` | `tsd.counters` | Tenant | |
| `adaptive_*` | `flows.adaptive_*` | Tenant | `scope_key` becomes a key part |
| `wf_*` | `flows.*` | Tenant | unused library store, mapped to the same collections |

**Modules**

| v1 name | v2 name | Scope class | Notes |
|---|---|---|---|
| `mcp_servers`, `mcp_server_names`, `mcp_registry_cache`, `mcp_tool_cache`, `mcp_registry_index`, `mcp_registry_index_state`; stream `mcp_audit` | `mcp.servers`, `.server_names`, `.registry_cache`, `.tool_cache`, `.registry_index`, `.registry_index_state`; stream `mcp.audit` | Module | `installed_at`, `cached_at`, `synced_at` and `claimed_at` (ms) become `_ms` or metadata |
| `channels_delivery_*`, `channel_remote_*`, `whatsapp_*` | `channels.delivery_*`, `channels.remote_*`, `channels.whatsapp_*` | Module | |
| `composio-user-scopes`, streams `triggers/<date>` | `connectors.user_scopes`, streams `connectors.triggers/<date>` | Module | unwrap `value` |
| blobs `tinyjuice_ccr/<token>` | blobs `juice/ccr/<token>` | Module | |
| `x402_budget`, stream `x402.payments` | `wallet.budgets`, stream `wallet.payments` | Module | |
| `tinybox_boxes`, `tinybox_jail_index` | `box.boxes`, `box.jail_index` | Module | |
| tinyskills bundle blobs | `skills/bundles/…` | Tenant | |
