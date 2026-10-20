#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

cargo check --locked -p tinystoragedrivers --no-default-features

for feature in file sqlite mongodb s3 rabbitmq keyring kms-aws kms-gcp; do
  printf 'Checking facade feature: %s\n' "$feature"
  cargo check --locked -p tinystoragedrivers --no-default-features \
    --features "$feature"
done
