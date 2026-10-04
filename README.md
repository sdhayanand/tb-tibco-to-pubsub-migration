# tb-tibco-to-pubsub-migration

Live migration of the Tailored Brands Order-to-Delivery messaging backbone from **TIBCO EMS /
TIBCO BusinessWorks / IBM MQ** to **Google Cloud Pub/Sub** — the two bridges that make the old
and the new world coexist, the reconciler that proves they agree, and the documentation that an
integration team needs to run the cutover.

```
   TIBCO EMS  TB.ORDERS.OUT ──▶ jms-to-pubsub-bridge ──▶ Pub/Sub orders-v1 ──▶ inventory-service, Dataflow, BigQuery
                                   (XML → canonical JSON,            │
                                    ordering key = storeId)          ▼  subscription orders-to-legacy-mq
   IBM MQ     ERP.ORDERS.IN ◀── pubsub-to-jms-bridge ◀───────────────┘  (filter source != TIBCO_EMS_BRIDGE)
                                   (canonical JSON → XML)
   reconciler: EMS audit queue / export  ⨝  BigQuery otd.order_events  ──▶ otd.migration_reconciliation
```

| Module | What | Runs on | Port |
|---|---|---|---|
| `migration-common` | `JmsProviderConfig` (`JMS_PROVIDER=artemis\|ibmmq\|ems` → `jakarta.jms.ConnectionFactory`), `LegacyXmlMapper` (legacy order XML ⇄ `OrderEvent` JSON), `HeaderMapper` (JMS headers ⇄ Pub/Sub attributes), `MigrationPhase` + `MigrationPhaseSource` (+ `migration-control` subscriber), `PubSubClients` (emulator-aware), `BridgeMetrics` | library | — |
| `jms-to-pubsub-bridge` | Spring `DefaultMessageListenerContainer` on `JMS_SOURCE` (queue, or durable topic subscription), transacted; publish ordered to `orders-v1`, wait for the ack, then commit; poison → `events-dlq`; phase-aware; `/actuator/health` + `/actuator/bridge` | GKE `otd`, 1 replica, `Recreate` | 8090 |
| `pubsub-to-jms-bridge` | Streaming-pull `Subscriber` on `orders-to-legacy-mq`; canonical → legacy XML; transacted JMS send to `ERP.ORDERS.IN` (IBM MQ / Artemis / EMS) with properties mapped back; loop guard; phase-aware | GKE `otd`, 1 replica, `Recreate` | 8091 |
| `reconciler` | CLI / Cloud Run Job / CronJob: window `--from --to`, legacy side via `QueueBrowser` on `TB.ORDERS.AUDIT` or `--legacyFile`, Pub/Sub side via BigQuery `otd.order_events` or `--pubsubFile`; join on `orderId` → `matched / onlyLegacy / onlyPubsub / payloadMismatch`; JSON + Markdown report; row into `otd.migration_reconciliation` | Cloud Run Job / k8s CronJob | — |

Docs (the part the interview is about):

* [`docs/CONCEPT-MAPPING.md`](docs/CONCEPT-MAPPING.md) — EMS / BW / MQ concept → Pub/Sub & GCP equivalent, with the gotchas.
* [`docs/RUNBOOK.md`](docs/RUNBOOK.md) — phases 0–4: pre-checks, commands, BigQuery verification queries, go/no-go, rollback, comms template.
* [`docs/MIGRATION-STRATEGY.md`](docs/MIGRATION-STRATEGY.md) — strangler-fig rationale, inventory of 12 destinations with target designs and waves, risk register, test strategy, metrics/SLOs.
* Platform context: `tb-platform-infra/docs/ARCHITECTURE.md` (§3 contract, §4 topology, §7 phases).

## How this maps to the job posting

| Posting | Here |
|---|---|
| "Migrate TIBCO EMS / BW integrations to GCP (Pub/Sub, Dataflow, Cloud Run)" | The whole repo; `CONCEPT-MAPPING.md` is the translation table, the bridges are the strangler-fig seam |
| "IBM MQ, JMS" | `JmsProviderConfig` targets EMS, IBM MQ (`com.ibm.mq.jakarta.client`, `WMQ_CM_CLIENT`) and Artemis through one `jakarta.jms` API; `IbmMqBridgeIT` runs against the real MQ image |
| "Design for ordering, idempotency, exactly-once" | ordering key = `storeId`, single consumer + `Recreate`, `legacyMessageId`/`pubsubMessageId` dedup keys, transacted JMS + publish-then-commit, exactly-once subscription downstream |
| "Operational excellence / cutover planning" | `RUNBOOK.md`, phase switch via env + `migration-control` topic, actuator `bridge.state`, Prometheus metrics, incident playbooks |
| "Data reconciliation / BigQuery" | `reconciler` + `otd.migration_reconciliation`, SQL in the runbook |
| "CI/CD, containers, Kubernetes" | GitHub Actions (unit + Testcontainers ITs + MQ job), Jib to GHCR/Artifact Registry, kustomize overlays, WIF deploy |

## Running locally

Prerequisites: Docker, JDK 21, Maven 3.9 (CI has Maven Central; the authoring workspace does not — see `CONVENTIONS.md`).

### Everything in docker-compose

```bash
docker compose up -d --build                       # artemis + pubsub emulator (+ topology) + both bridges
./local/send-legacy-order.sh ORD-1 0412            # legacy XML → TB.ORDERS.OUT
curl -s localhost:8090/actuator/bridge | jq        # {"bridge.state":"RUNNING","phase":"SHADOW",...}
curl -s localhost:8090/actuator/health | jq .components.bridge
# the canonical event is now on orders-v1; the Pub/Sub→MQ bridge (DUAL_RUN in compose) sends it to ERP.ORDERS.IN
# — except that it came from TIBCO_EMS_BRIDGE, so the loop guard drops it: look for "Loop guard" in
docker compose logs pubsub-to-jms-bridge | tail
# publish a canonical event yourself (base64 of the JSON) to see the forward path:
DATA=$(jq -c . jms-to-pubsub-bridge/../migration-common/src/test/resources/sample-canonical-event.json 2>/dev/null || echo '{"eventId":"e-1","eventType":"ORDER_CREATED","eventTime":"2026-10-03T22:14:05Z","schemaVersion":"1","source":"ORDER_INTAKE_API","correlationId":"c-1","order":{"orderId":"ORD-API-1","orderType":"RETAIL","channel":"STORE","storeId":"0412","orderedAt":"2026-10-03T22:14:00Z","currency":"USD","totalAmount":99.00,"lines":[{"lineNumber":1,"sku":"MW-TIE-RED","quantity":1,"unitPrice":99.00,"fulfillmentType":"STORE_PICKUP"}]}}')
curl -s -X POST localhost:8085/v1/projects/tb-local/topics/orders-v1:publish -H 'content-type: application/json' \
  -d "{\"messages\":[{\"data\":\"$(printf '%s' "$DATA" | base64 -w0)\",\"attributes\":{\"source\":\"ORDER_INTAKE_API\",\"storeId\":\"0412\",\"eventType\":\"ORDER_CREATED\",\"schemaVersion\":\"1\"},\"orderingKey\":\"0412\"}]}"
# → Artemis console http://localhost:8161 (artemis/artemis): queue ERP.ORDERS.IN has one XML message
./local/publish-phase.sh LEGACY_ONLY               # both bridges flip to PAUSED without a restart
docker compose --profile mq up -d ibm-mq           # optional: real IBM MQ on 1414 (app/passw0rd, DEV.QUEUE.1)
```

### From Maven, against the emulator

```bash
mvn -q test                                        # unit tests (seconds, no Docker)
mvn -B verify                                      # + Testcontainers ITs: Artemis + Pub/Sub emulator (Docker)
RUN_MQ_IT=true mvn -B -pl pubsub-to-jms-bridge -am verify -Dit.test=IbmMqBridgeIT \
  -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false   # IBM MQ IT (~1 GB image)

# run one bridge against docker-compose's artemis + emulator
JMS_URL=tcp://localhost:61616 JMS_USER=artemis JMS_PASSWORD=artemis PUBSUB_EMULATOR_HOST=localhost:8085 \
MIGRATION_PHASE=SHADOW mvn -pl jms-to-pubsub-bridge spring-boot:run

# reconciler on files, no infrastructure at all
mvn -q -pl reconciler -am package -DskipTests
java -jar reconciler/target/reconciler-*.jar --from=2026-10-03 --to=2026-10-04 --phase=SHADOW \
  --legacyFile=reconciler/src/test/resources/legacy-orders.csv \
  --pubsubFile=reconciler/src/test/resources/pubsub-events.jsonl --out=/tmp/recon
cat /tmp/recon.md
```

### TIBCO EMS (real one)

`tibjms.jar` is licensed software and not on Maven Central. The code only touches it via reflection
(`com.tibco.tibjms.TibjmsConnectionFactory`), so builds never need it. To run against EMS:

```bash
mvn install:install-file -Dfile=/opt/tibco/ems/10.3/lib/tibjms.jar -DgroupId=com.tibco -DartifactId=tibjms -Dversion=10.3.0 -Dpackaging=jar
mvn -Pems -pl jms-to-pubsub-bridge jib:build -Dimage=…         # image now contains the EMS client
JMS_PROVIDER=ems JMS_URL=tcp://ems-a:7222,tcp://ems-b:7222 JMS_USER=bridge JMS_PASSWORD=… MIGRATION_PHASE=SHADOW …
```
EMS 10.x ships a Jakarta-JMS build of the client; the fault-tolerant URL pair and `setReconnAttemptCount` are honoured.

## Configuration (env vars)

| Variable | jms-to-pubsub | pubsub-to-jms | Default |
|---|---|---|---|
| `JMS_PROVIDER` | ✓ | ✓ | `artemis` (`ibmmq`, `ems`) |
| `JMS_URL` `JMS_USER` `JMS_PASSWORD` | ✓ | ✓ | `tcp://localhost:61616` |
| `MQ_QMGR` `MQ_CHANNEL` `MQ_HOST` `MQ_PORT` | ✓ | ✓ | `QM1` `DEV.APP.SVRCONN` `localhost` `1414` (ibmmq only) |
| `JMS_SOURCE` `JMS_SOURCE_TYPE` `JMS_CLIENT_ID` `JMS_DURABLE_NAME` `JMS_SELECTOR` | ✓ | | `TB.ORDERS.OUT` `queue` |
| `BRIDGE_CONCURRENCY` | ✓ | | `1` (keep for ordering) |
| `TARGET_TOPIC` `DLQ_TOPIC` `DEFAULT_EVENT_TYPE` | ✓ | (`DLQ_TOPIC`) | `orders-v1` `events-dlq` `ORDER_CREATED` |
| `SOURCE_SUBSCRIPTION` `JMS_DESTINATION` `JMS_DESTINATION_TYPE` | | ✓ | `orders-to-legacy-mq` `ERP.ORDERS.IN` `queue` |
| `PUBSUB_PROJECT` `PUBSUB_EMULATOR_HOST` | ✓ | ✓ | `tb-local`, unset |
| `MIGRATION_PHASE` | ✓ | ✓ | `LEGACY_ONLY` |
| `MIGRATION_CONTROL_SUBSCRIPTION` | ✓ | ✓ | empty = env only; use one subscription **per bridge** |
| `SPRING_PROFILES_ACTIVE=gcp` | ✓ | ✓ | JSON logs with `correlationId`/`orderId`/`eventId` MDC |

## CI/CD

* `.github/workflows/ci.yml` — `mvn verify` (unit + Artemis/emulator ITs) on every push/PR; on `main`
  additionally the `mq-integration` job (`RUN_MQ_IT=true -Dit.test=IbmMqBridgeIT` against
  `icr.io/ibm-messaging/mq:latest`) and Jib pushes of `tb-jms-to-pubsub-bridge`, `tb-pubsub-to-jms-bridge`,
  `tb-reconciler` to GHCR.
* `.github/workflows/deploy-gcp.yml` — guarded by `vars.GCP_PROJECT_ID != ''`; Workload Identity
  Federation auth, Jib to Artifact Registry, `gcloud run jobs replace` for the reconciler,
  `kubectl apply -k <bridge>/deploy/k8s/overlays/gcp` into namespace `otd`.
* Images: `ghcr.io/sdhayanand/tb-<module>` (default `image` property in the root pom) and
  `us-central1-docker.pkg.dev/$PROJECT/tb-otd/<module>`.

## Verifying it works (production-ish)

```bash
kubectl -n otd port-forward deploy/jms-to-pubsub-bridge 8090:8090 &
curl -s localhost:8090/actuator/bridge
# {"bridge.state":"RUNNING","phase":"SHADOW","phaseOrigin":"pubsub:migration-control-jms-to-pubsub:123",
#  "containerActive":true,"activeConsumers":1,"lastMessageAt":"2026-10-03T22:14:05Z","lastMessageAgeSeconds":3.2,"lagMillis":180}
curl -s localhost:8090/actuator/health
# {"status":"UP","components":{"bridge":{"status":"UP","details":{"state":"RUNNING","phase":"SHADOW","jmsConnection":"UP",
#   "jmsProvider":"TIBCO Enterprise Message Service 10.3.0", ...}}, ...}}
curl -s localhost:8090/actuator/prometheus | grep bridge_messages_bridged_total
# bridge_messages_bridged_total{application="jms-to-pubsub-bridge",bridge="jms-to-pubsub",} 41873.0
gcloud pubsub subscriptions pull orders-bq-archive --limit=1 --format=json | jq '.[0].message.attributes'
# {"source":"TIBCO_EMS_BRIDGE","eventType":"ORDER_CREATED","storeId":"0412","legacyMessageId":"ID:EMS-SERVER.1A2B3C","schemaVersion":"1",...}
bq query --use_legacy_sql=false 'SELECT * FROM otd.migration_reconciliation ORDER BY run_time DESC LIMIT 7'
```

## Design decisions worth knowing

* **At-least-once, not exactly-once, at the seam.** The JMS session is transacted; the bridge
  publishes, waits for the Pub/Sub message id, then commits. A crash between the two yields a
  duplicate, never a loss. Duplicates carry the same `legacyMessageId` (and, on the way back, the same
  `pubsubMessageId`), which is what consumers de-duplicate on. XA across EMS and Pub/Sub is impossible
  and would be slower than the duplicate window is wide.
* **One consumer, `Recreate`, concurrency 1.** EMS queues are FIFO; Pub/Sub ordering is per key.
  The only way to carry queue order into ordering keys is a single in-order consumer. Scaling is per
  queue (one bridge deployment per destination), not per queue consumer.
* **Poison goes to the DLQ and is acked.** A message that cannot be parsed will never parse; keeping it
  in the queue blocks everything behind it. The DLQ message carries `dlqReason`, `dlqStage`, the original
  attributes and payload (truncated at 1 MB).
* **Loop guard twice.** The `orders-to-legacy-mq` subscription filter is the real guard; the in-code
  check protects against someone recreating the subscription without the filter (and makes the emulator test possible).
* **Phase is data, not deployment.** Flipping a phase is `kubectl set env` + a Pub/Sub message, not a
  release; rollback is the same command with the previous value.
* **Reconciliation is asymmetric on purpose.** Legacy has no replay and no archive → browse the audit
  queue; Pub/Sub has no browse → query BigQuery. That asymmetry is the migration in one sentence.

## What to say in the interview

1. **"We strangled EMS at the edge, not in the middle."** Two stateless bridges translate once; every
   new consumer speaks the canonical contract and never sees TIBCO XML. Provenance attributes
   (`source`, `legacyMessageId`, `pubsubMessageId`) made loops impossible and every message traceable
   across the seam.
2. **"Queue ≠ topic: a Pub/Sub subscription is a cursor, not a buffer."** Durable subscribers became one
   subscription each, selectors became attribute-only filters fixed at creation, priority became separate
   topics, browse became BigQuery, replay became `seek`. The concept-mapping table is the thing I'd hand a
   BW developer on day one.
3. **"Ordering is a key, and a key is a business invariant."** `storeId` for the order lifecycle,
   `sku:location` for stock, `groupId` for rentals — never two keys on one topic; and the 1 MB/s per-key
   ceiling plus `resumePublish` after a failure are the two things people forget.
4. **"XA is gone; outbox + idempotent consumers + exactly-once subscriptions replace it."** The bridge is
   at-least-once by design (transacted JMS, publish, wait for ack, commit); dedup keys are carried in both
   directions; `inventory-service` uses an exactly-once subscription and an inbox table.
5. **"A phase switch, five positions, each with a lossless rollback."** `LEGACY_ONLY → SHADOW → DUAL_RUN →
   PUBSUB_PRIMARY → CUTOVER`, driven by env + a control topic, visible as `bridge.state` on the actuator.
   Seven days of shadow with reconciler diff = 0 was the only exit criterion I accepted.
6. **"Measure parity, don't trust it."** The reconciler joins the EMS audit queue (browse) with BigQuery on
   `orderId`, compares total and line count, writes a row per run; the alert is on `only_legacy + only_pubsub + mismatched > 0`.
7. **"The JMS layer is provider-agnostic — and we proved it in CI with IBM MQ."** `jakarta.jms` only;
   Artemis as the EMS stand-in in every build, the real IBM MQ image in the `mq-integration` job, EMS via
   reflection behind `-Pems` so the licensed jar never enters git. Ten days from "we also have MQ" to a green test.
8. **"BW processes don't map to one GCP service."** Stateless request/response → Cloud Run; stream
   transforms → Dataflow; connector-heavy low-code → Application Integration; sagas → Workflows; XSLT →
   typed mappers under unit test. The hard part of a BW migration is the transactions and the
   request/reply habits, not the JMS calls.
