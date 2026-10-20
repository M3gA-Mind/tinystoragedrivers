# Contributing

Thanks for contributing. The best changes here are small, explicit, tested, and
easy to review. [`AGENTS.md`](AGENTS.md) holds the full repository guidelines —
this document is the short path through them.

## Development Setup

Install a stable Rust toolchain with Rust 2024 support (see `rust-version` in
`Cargo.toml` for the minimum supported version), then run the four checks CI
runs:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
cargo test --all-features
```

CI also requires at least 90% line coverage in every source file. The coverage
suite includes the compiled storage-callback module, so build and exercise that
fixture first, then pass its artifact path to the gate. After installing
`cargo-llvm-cov`, run:

```sh
bash scripts/test-storage-callback.sh
proof_target="$(cargo metadata --manifest-path proof-workspaces/storage-callback/Cargo.toml --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
case "$(uname -s)" in
  Darwin) proof_library="libstorage_callback_module_proof.dylib" ;;
  MINGW*|MSYS*|CYGWIN*) proof_library="storage_callback_module_proof.dll" ;;
  *) proof_library="libstorage_callback_module_proof.so" ;;
esac
TSD_STORAGE_CALLBACK_MODULE="$proof_target/debug/$proof_library" \
  .github/scripts/check-file-coverage.sh 90 target/coverage.json
```

## Making A Change

1. Branch from `main` — never commit directly to it. If you use the `worktree`
   helper, work inside `worktrees/<slug>`.
2. Put each feature area in its own module directory: `mod.rs` for the module
   root and public surface, `types.rs` for substantial types, `mod_tests.rs` for
   module-local unit tests. Integration tests belong in `tests/`.
3. Add a specific variant to the crate error type rather than encoding new
   failure context into a message string.
4. Add or update tests with every behavior change, covering the failure paths.
5. Document public items, including `# Errors` and `# Panics` sections.
6. Update `README.md` and `docs/` in the same commit when behavior, the public
   API, or usage changes.

Do not add blanket `#[allow(...)]` attributes, mark tests `#[ignore]`, or relax
lints to get a green run. Fix the cause, or raise the blocker in the pull
request.

## Adding A Dependency

Check whether the standard library or an existing dependency already covers the
need. If not, add it with a caret range, only the features you need, gated
behind a Cargo feature when optional, and with a comment saying why it exists.
Run `cargo deny check all` if you have `cargo-deny` installed; CI runs it
regardless.

## Pull Request Checklist

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo build --all-targets --all-features`
- [ ] `cargo test --all-features`
- [ ] `.github/scripts/check-file-coverage.sh 90 target/coverage.json`
- [ ] tests added or updated for behavior changes
- [ ] documentation updated for public API, architecture, or usage changes
- [ ] the pull request is focused on one logical change

Open pull requests ready for review rather than as drafts, unless the work
genuinely must not merge yet — say in the body what has to happen first.

## Commit Style

Use concise, imperative subjects scoped to one logical change:

```text
Add retry policy to the client
Document the error taxonomy
```

Avoid mixing formatting, refactors, and behavior changes unless they are
inseparable. Never commit secrets; `.env` is git-ignored and new variables are
documented in `.env.example` with placeholder values.

## Issue Triage

Good issues include the version or commit, the toolchain, the relevant module or
API, a minimal reproduction, the expected and actual behavior, and the commands
run. Feature requests should explain the workflow they unlock and the public API
shape they imply.

## Security

Do not report vulnerabilities through public issues. Follow the process in
[SECURITY.md](SECURITY.md).
