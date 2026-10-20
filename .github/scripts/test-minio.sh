#!/usr/bin/env bash
# Keep service startup, tests and cleanup in one process lifetime.
set -euo pipefail
port="${1:-19000}"
name="${2:-tsd-minio}"
trap '.github/scripts/stop-minio.sh "$name"' EXIT
.github/scripts/start-minio.sh "$port" "$name"
export TSD_S3_URL="s3://tsd-test/ci?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A$port"
export AWS_ACCESS_KEY_ID=tsd-test-access
export AWS_SECRET_ACCESS_KEY=tsd-test-secret
cargo test --locked -p tinystoragedrivers-s3 --test live_minio
cargo test --locked -p tinystoragedrivers-blob --no-default-features --features s3
