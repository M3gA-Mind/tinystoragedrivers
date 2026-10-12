#!/usr/bin/env bash
# Actual SDK cdylib, hashed and admitted before confidential CRUD dispatch.
set -euo pipefail
cd -- "$(dirname -- "$0")/.."
proof_manifest="$PWD/proof-workspaces/document-callback/Cargo.toml"
cargo build --manifest-path "$proof_manifest" -p document-callback-module-proof
proof_target="$(cargo metadata --manifest-path "$proof_manifest" --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
case "$(uname -s)" in
    Darwin) proof_library="libdocument_callback_module_proof.dylib" ;;
    MINGW*|MSYS*|CYGWIN*) proof_library="document_callback_module_proof.dll" ;;
    *) proof_library="libdocument_callback_module_proof.so" ;;
esac
proof_artifact="$proof_target/debug/$proof_library"
test -f "$proof_artifact"
TSD_DOCUMENT_MODULE="$proof_artifact" cargo test --manifest-path "$proof_manifest" -p document-callback-proof
