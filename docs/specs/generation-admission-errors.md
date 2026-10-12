# Scope generation refusals

Status: implemented and locally verified candidate; not released or consumed
by OpenHuman. Owner: TinyStorageDrivers' native admission and error contracts.

`ErrorKind::ScopeGenerationRejected` distinguishes native scope-reset admission
from loss of a lease. Memory and SQLite return it when the handle's captured
generation differs from current authority. MongoDB also returns it while a
quiescent reset is pending. A rejected generation does not establish that an
independent native lease has expired or changed.

The error is not retryable through the same handle. A trusted owner must finish
the reset and explicitly bind its issued generation. Constructors do not
discover current authority or revive an old handle. Malformed or missing
authority with contradictory reset history continues to return `Serialization`.
An actual native lease guard refusal remains `Fenced`.

The document bus exposes the content-free `scope_generation_rejected` kind.
The adapter maps the native category exactly, preserves `fenced` for native
lease refusal, and keeps unknown future native kinds mapped to `backend`.
Neither backend messages nor source chains cross that ABI. Older bus clients
that cannot decode the new kind must be updated alongside the adapter; the
addition does not promise backward decoding by older exhaustive wire enums.

## Evidence

The baseline is canonical `d2f85b2ac188d504b8398bdb70943c48bf2dfa51`.
The regressions use existing APIs to assert that a retired generation must not
claim lease loss, so their initial failure is behavioral rather than a missing
enum compilation failure.

- Memory: successful build followed by the wrong-`Fenced` assertion failure.
- SQLite and the captured native document-service dispatch: both failed the
  wrong-`Fenced` assertion.
- Native MongoDB and its production pending/reset admission validator: both
  failed the wrong-`Fenced` assertion; four existing controls passed.
- The first Mongo fixture attempted unsupported fenced GridFS purge. Its
  output is retained and excluded from behavioral evidence. The corrected
  fixture uses supported quiescent reset through the trusted owner's actual
  unfenced native port, retains the original fenced document/stream handles,
  and preserves unsupported fenced blob-write behavior.
- The corrected implementation passes the four targeted native/adapter cases,
  with successful issued-generation use under the same lease and a genuine
  lease takeover still classified as `Fenced`.

Native Mongo uses a local replica set with a distinct test database and
process-qualified scopes. The adapter unit fixture exercises native refusal
mapping; it is not additional proof of compiled-module callback admission.
The five owning packages subsequently passed 353 unit/integration tests and
19 doctests, including native Mongo pending-reset and watch admission. The
first full run exposed one remaining retired-generation watch expectation of
`Fenced`; only that generation-specific expectation was updated before the
fresh complete run. Genuine watch lease-refusal expectations are unchanged.
The first all-feature run also exposed the existing secret-store cached-DEK
retired-generation expectation of `Fenced`. Its two expectations now name the
generation category; native secret refusal and cryptographic behavior are
unchanged. Strict Clippy initially rejected the new Mongo test's length; an
assertion-preserving helper extraction addressed it.

Final owner gates passed:

- Formatting, full workspace build, and strict Clippy, all targets and features.
- All features: 682 unit/integration tests and 29 doctests; default: 641 tests
  and 28 doctests. No failures or ignored tests in these runs.
- Rustdoc with warnings denied, and locked Rust 1.88 checks for all workspace
  targets/features plus the excluded compiled-proof workspace.
- Four existing actual compiled document-callback controls. These preserve
  callback admission and native lease refusal; the new generation wire kind
  is pinned by native service dispatch and serde tests, not a new cdylib case.
- The canonical per-file coverage gate, including its native publication,
  concurrent key, and isolated Secret Service probes: 90 reported source
  files all exceed 90%, with 13,505/13,937 covered lines (96.90%). The
  instrumented suite passed 682 tests. Error contract coverage is 70/70;
  Memory admission 239/240; SQLite purge 116/122; Mongo purge/admission
  258/284; document adapter 130/133.

Durable local outputs are kept in `.validation/generation-*.log`, with the
coverage export in `.validation/generation-coverage.json`. Initial failure
outputs are retained separately from final acceptance logs.
No OpenHuman consumption, release, or host reset acceptance is claimed.
