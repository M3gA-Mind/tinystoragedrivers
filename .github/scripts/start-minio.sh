#!/usr/bin/env bash
# Linux/amd64 local MinIO fixture from a pinned, verified upstream release.
set -euo pipefail
port="${1:-19000}"
name="${2:-tsd-minio}"
[[ "$name" =~ ^[a-z0-9_-]+$ ]] || exit 2
[[ "$(uname -sm)" == "Linux x86_64" ]] || exit 2
fixture="$PWD/target/$name"
asset="minio_20250422221226.0.0_amd64.deb"
mkdir -p "$fixture"
if [[ ! -f "$fixture/$asset" ]]; then
  curl -fsSL "https://github.com/minio/minio/releases/download/RELEASE.2025-04-22T22-12-26Z/$asset" -o "$fixture/$asset"
fi
# SHA256 published with the upstream release; pinned here to verify the download.
echo "d8898351831761e083e7ba6b74ea7ec478b9a6680794fe92e5e838145341c9c5  $fixture/$asset" | sha256sum --check --status
dpkg-deb -x "$fixture/$asset" "$fixture"
mkdir -p "$fixture/data"
nohup env MINIO_ROOT_USER=tsd-test-access MINIO_ROOT_PASSWORD=tsd-test-secret \
  "$fixture/usr/local/bin/minio" server --address "127.0.0.1:$port" \
  --console-address 127.0.0.1:0 "$fixture/data" >"$fixture/server.log" 2>&1 &
echo "$!" >"$fixture/server.pid"
for attempt in {1..60}; do
  if curl -fsS "http://127.0.0.1:$port/minio/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "http://127.0.0.1:$port/minio/health/ready" >/dev/null
if ! curl -fsS --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "tsd-test-access:tsd-test-secret" -I \
  "http://127.0.0.1:$port/tsd-test" >/dev/null 2>&1; then
  curl -fsS --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "tsd-test-access:tsd-test-secret" -X PUT \
    "http://127.0.0.1:$port/tsd-test" >/dev/null
fi
