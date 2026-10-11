#!/usr/bin/env bash
# Stop only the process started by start-minio.sh in this checkout.
set -euo pipefail
name="${1:-tsd-minio}"
[[ "$name" =~ ^[a-z0-9_-]+$ ]] || exit 2
fixture="$PWD/target/$name"
[[ -f "$fixture/server.pid" ]] || exit 0
pid="$(cat "$fixture/server.pid")"
[[ "$pid" =~ ^[0-9]+$ ]] || exit 2
if [[ "$(readlink "/proc/$pid/exe" || true)" == "$fixture/usr/local/bin/minio" ]]; then
  kill "$pid"
fi
