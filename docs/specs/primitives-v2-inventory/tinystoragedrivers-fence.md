# tinystoragedrivers-fence public item destinations

Source symbols below include public definitions, associated methods, public fields, and re-export declarations. Line numbers refer to the audited epic source revision `ae2fc08`. A name repeated through a re-export is intentionally repeated: both public paths are API.

| Source | Public item(s) | v2 destination |
| --- | --- | --- |
| `src/backend.rs` | `for_scope_fenced (L16)` | Deleted; lease/fencing policy belongs to the consumer that owns the lease. |
| `src/document.rs` | `DocumentLeases (L24)`, `new (L54)`, `with_endpoint (L86)` | Deleted; lease/fencing policy belongs to the consumer that owns the lease. |
| `src/lease.rs` | `LeaseRecord (L7)`, `LeaseError (L11)`, `LeaseGrant (L25)` and its fields, `LeaseStore (L42)` methods `acquire`, `renew`, `release`, `inspect`, `holder`, `driver_fence`, `is_live (L73)`, `retry_after_ms (L82)` | Deleted with reverted fence crate (#24); lease state and fencing policy are owned by the concrete lease consumer (tinyagents session reservation and OpenHuman host), not a generic TSD crate. |
| `src/lib.rs` | `use backend::for_scope_fenced (L11)`, `use document::DocumentLeases (L12)`, `use lease::{LeaseError, LeaseGrant, LeaseRecord, LeaseStore, is_live, retry_after_ms} (L13)`, `use local::LocalLeases (L19)` | Deleted; lease/fencing policy belongs to the consumer that owns the lease. |
| `src/local.rs` | `LocalLeases (L38)`, `new (L52)`, `with_endpoint (L71)` | Deleted; lease/fencing policy belongs to the consumer that owns the lease. |
