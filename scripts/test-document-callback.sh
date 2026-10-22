#!/usr/bin/env bash
# Actual SDK cdylib, hashed and admitted before confidential CRUD dispatch.
set -euo pipefail
cd -- "$(dirname -- "$0")/.."
proof_manifest="$PWD/proof-workspaces/document-callback/Cargo.toml"
cargo build --locked --manifest-path "$proof_manifest" -p document-callback-module-proof -p document-callback-peer-proof
proof_target="$(cargo metadata --manifest-path "$proof_manifest" --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
case "$(uname -s)" in
    Darwin) proof_library="libdocument_callback_module_proof.dylib" ;;
    MINGW*|MSYS*|CYGWIN*) proof_library="document_callback_module_proof.dll" ;;
    *) proof_library="libdocument_callback_module_proof.so" ;;
esac
proof_artifact="$proof_target/debug/$proof_library"
replacement_artifact="${proof_artifact/document_callback_module_proof/document_callback_peer_proof}"
test -f "$proof_artifact"
test -f "$replacement_artifact"
TSD_DOCUMENT_MODULE="$proof_artifact" TSD_DOCUMENT_REPLACEMENT_MODULE="$replacement_artifact" cargo test --locked --manifest-path "$proof_manifest" -p document-callback-proof
