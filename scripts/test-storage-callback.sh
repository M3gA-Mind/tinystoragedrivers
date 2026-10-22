#!/usr/bin/env bash
# Build and admit a real TinyBus module that calls all six storage ports.
set -euo pipefail
cd -- "$(dirname -- "$0")/.."
proof_manifest="$PWD/proof-workspaces/storage-callback/Cargo.toml"
cargo build --locked --manifest-path "$proof_manifest" -p storage-callback-module-proof
proof_target="$(cargo metadata --manifest-path "$proof_manifest" --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
case "$(uname -s)" in
    Darwin) proof_library="libstorage_callback_module_proof.dylib" ;;
    MINGW*|MSYS*|CYGWIN*) proof_library="storage_callback_module_proof.dll" ;;
    *) proof_library="libstorage_callback_module_proof.so" ;;
esac
proof_artifact="$proof_target/debug/$proof_library"
test -f "$proof_artifact"
if [[ -n "${GITHUB_ENV:-}" ]]; then
    printf 'TSD_STORAGE_CALLBACK_MODULE=%s\n' "$proof_artifact" >>"$GITHUB_ENV"
fi
TSD_STORAGE_CALLBACK_MODULE="$proof_artifact" cargo test --locked --manifest-path "$proof_manifest" -p storage-callback-proof
