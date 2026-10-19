#!/usr/bin/env bash
# Start RabbitMQ for the RabbitMQ queue backend's live tests and wait until it
# accepts AMQP connections.
#
# Usage: start-rabbitmq.sh [port] [image]
set -euo pipefail

port="${1:-5672}"
image="${2:-rabbitmq:4-management}"
name="tsd-rabbitmq"

# Remove the container if startup fails, so a retry on the same runner can
# reuse the name; on success it stays up for the tests.
cleanup() {
  if [[ $? -ne 0 ]]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker run -d --rm --name "$name" -p "127.0.0.1:${port}:5672" "$image" >/dev/null

for _ in $(seq 1 90); do
  if docker exec "$name" rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1; then
    echo "rabbitmq is ready on 127.0.0.1:${port}"
    exit 0
  fi
  sleep 1
done
echo "rabbitmq did not become ready" >&2
docker logs "$name" >&2 || true
exit 1
