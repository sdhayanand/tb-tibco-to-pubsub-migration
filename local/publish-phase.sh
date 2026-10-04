#!/usr/bin/env sh
# Broadcast a migration phase to both bridges through the emulator's migration-control topic.
#   ./local/publish-phase.sh DUAL_RUN
set -eu
PHASE="${1:?usage: publish-phase.sh LEGACY_ONLY|SHADOW|DUAL_RUN|PUBSUB_PRIMARY|CUTOVER}"
HOST="${PUBSUB_EMULATOR_HOST:-localhost:8085}"
PROJECT="${PUBSUB_PROJECT:-tb-local}"
DATA=$(printf '%s' "$PHASE" | base64)
curl -s -X POST -H 'Content-Type: application/json' \
  "http://${HOST}/v1/projects/${PROJECT}/topics/migration-control:publish" \
  -d "{\"messages\":[{\"data\":\"${DATA}\",\"attributes\":{\"phase\":\"${PHASE}\"}}]}"
echo
