# tinystoragedrivers-tinybus public item destinations

Source symbols below include public definitions, associated methods, public fields, and re-export declarations. Line numbers refer to the audited epic source revision `ae2fc08`. A name repeated through a re-export is intentionally repeated: both public paths are API.

| Source | Public item(s) | v2 destination |
| --- | --- | --- |
| `src/lib.rs` | `endpoint (L31)`, `serve_collection (L50)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
