#!/usr/bin/env sh
# Creates the migration topology on the Pub/Sub emulator (same names as Terraform modules/pubsub,
# no schemas). Idempotent: existing resources return 409 and are ignored.
#   PUBSUB_EMULATOR_HOST=localhost:8085 PUBSUB_PROJECT=tb-local ./local/pubsub-init.sh
set -eu
HOST="${PUBSUB_EMULATOR_HOST:-localhost:8085}"
PROJECT="${PUBSUB_PROJECT:-tb-local}"
BASE="http://${HOST}/v1/projects/${PROJECT}"

put() { # path json
  code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT -H 'Content-Type: application/json' "${BASE}/$1" -d "$2")
  case "$code" in
    200|409) echo "  $1 -> $code" ;;
    *) echo "  $1 -> $code (FAILED)"; exit 1 ;;
  esac
}

echo "Waiting for emulator at ${HOST} ..."
for i in $(seq 1 60); do
  if curl -s "http://${HOST}/" >/dev/null 2>&1; then break; fi
  sleep 1
done

echo "Creating topics in project ${PROJECT}"
for t in orders-v1 inventory-v1 shipments-v1 events-dlq migration-control; do
  put "topics/${t}" '{}'
done

echo "Creating subscriptions"
put "subscriptions/orders-inventory-service" \
  "{\"topic\":\"projects/${PROJECT}/topics/orders-v1\",\"enableMessageOrdering\":true,\"ackDeadlineSeconds\":60}"
put "subscriptions/orders-dataflow" \
  "{\"topic\":\"projects/${PROJECT}/topics/orders-v1\",\"enableMessageOrdering\":true}"
# The real subscription carries filter attributes.source != "TIBCO_EMS_BRIDGE" (loop guard);
# the emulator's filter support varies by version, the bridge enforces the same guard in code.
put "subscriptions/orders-to-legacy-mq" \
  "{\"topic\":\"projects/${PROJECT}/topics/orders-v1\",\"enableMessageOrdering\":true,\"ackDeadlineSeconds\":30}"
put "subscriptions/orders-bq-archive" \
  "{\"topic\":\"projects/${PROJECT}/topics/orders-v1\"}"
put "subscriptions/events-dlq-monitor" \
  "{\"topic\":\"projects/${PROJECT}/topics/events-dlq\"}"
put "subscriptions/migration-control-jms-to-pubsub" \
  "{\"topic\":\"projects/${PROJECT}/topics/migration-control\"}"
put "subscriptions/migration-control-pubsub-to-jms" \
  "{\"topic\":\"projects/${PROJECT}/topics/migration-control\"}"
echo "Pub/Sub emulator topology ready."
