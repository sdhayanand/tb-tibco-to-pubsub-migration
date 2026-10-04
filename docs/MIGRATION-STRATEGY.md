# Migration strategy — TIBCO EMS / BusinessWorks / IBM MQ → Cloud Pub/Sub

> Why strangler-fig, what we migrate in which wave, how we know it works, what can go wrong.
> Read with `CONCEPT-MAPPING.md` (the "how do I translate X") and `RUNBOOK.md` (the "what do I type").

## 1. Approach: strangler fig, not big bang

A big-bang switch of the order backbone is not an option for a retailer with ~1 200 stores, tuxedo
rentals tied to wedding dates and an ERP close every month: one bad evening is a quarter of
lost revenue and a lot of unhappy wedding parties. We therefore grow the new system **around** the
old one and let the old one wither destination by destination:

1. **Façade first.** New producers (`order-intake-api` behind Apigee) and new consumers
   (`inventory-service`, Dataflow → BigQuery) are built against the canonical contract
   (`OrderEvent`, ARCHITECTURE §3) — never against EMS. The contract is the strangler's trunk.
2. **Bridges at the edge.** Two small, boring, stateless services translate at the boundary:
   `jms-to-pubsub-bridge` (legacy XML on EMS → canonical JSON on `orders-v1`) and
   `pubsub-to-jms-bridge` (canonical → legacy XML on IBM MQ for the ERP). They carry provenance
   (`source`, `legacyMessageId`, `pubsubMessageId`) so every message can be traced across the seam
   and so loops are impossible (`source != TIBCO_EMS_BRIDGE` filter + in-code guard).
3. **One global switch with five positions** (`MIGRATION_PHASE`, ARCHITECTURE §7) that every
   component understands. Phases are *additive*: each one turns on one more thing and leaves a
   lossless rollback to the previous one. The switch is an env var (durable) and a Pub/Sub control
   topic (fast).
4. **Measure, don't trust.** A reconciler joins both sides on `orderId` every day and writes the
   result to BigQuery; "diff = 0 for N days" is the only exit criterion that matters.
5. **Decommission last.** EMS/MQ stay up — idle — through Phase 3 and a one-week grace after
   Phase 4. Licences are the cheapest insurance in the project.

Alternatives considered and rejected:

| Option | Why not |
|---|---|
| Lift-and-shift EMS to GCE VMs ("EMS on GCP") | Keeps the licence, the FT pair, the single-region SPOF and every BW process; moves the problem, doesn't solve it. Acceptable only as a *first hop* if the data-centre lease forced it — it did not. |
| Big-bang cutover over a weekend | No shadow period → no evidence; rollback = restore from backups; the month-end ERP close would be the first real test. |
| Replace EMS with Kafka / Confluent on GCP | Closer to EMS semantics (partitions ≈ ordered queues) but another cluster to run, another team to staff; Pub/Sub is managed, regional, IAM-native and already the platform's event bus. Pub/Sub Lite / Kafka-compatible APIs were not needed. |
| Keep EMS forever behind an adapter | Every new consumer would still need the legacy XML and BW expertise; the goal is to *retire* the TIBCO skill dependency. |

## 2. Inventory of EMS / MQ destinations and target design

Twelve destinations (a realistic subset of the Tailored Brands estate) with producer, consumer,
volume, ordering need, chosen Pub/Sub design and wave. Volumes are weekday averages; peak = Saturday
afternoon and the two weeks before prom/wedding season (≈ 4×).

| # | Destination (type) | Producer → Consumer(s) | Avg / peak msgs | Ordering need | Size | Target design on Pub/Sub | Wave |
|---|---|---|---|---|---|---|---|
| 1 | `TB.ORDERS.OUT` (queue) | Store POS → BW `OrderOut` → OMS (SOAP) ; **audit copy** to #12 | 40 k/day, 150/min peak | Per store (create before update) | 4 KB XML | Topic `orders-v1` (proto schema), ordering key `storeId`; subs `orders-inventory-service` (exactly-once), `orders-dataflow`, `orders-bq-archive`, `orders-to-legacy-mq` (filtered) | **W1** (this repo) |
| 2 | `TB.ORDERS.CANCEL` (queue) | POS / Customer-service app → BW → OMS + ERP | 2 k/day | Must follow the create of the same order | 1 KB | Same topic `orders-v1`, `eventType=ORDER_CANCELLED` (ordering key `storeId` keeps create→cancel order); bridge deployment #2 with `JMS_SOURCE=TB.ORDERS.CANCEL`, `DEFAULT_EVENT_TYPE=ORDER_CANCELLED` | W1 |
| 3 | `TB.INVENTORY.ADJ` (topic, 3 durable subscribers) | WMS + store cycle counts → inventory-service, BI, planning | 120 k/day, 600/min | Per SKU+location | 500 B | Topic `inventory-v1`, ordering key `sku:locationId`; one subscription per former durable subscriber; bridge with `JMS_SOURCE_TYPE=topic` + durable name until WMS publishes natively | W2 |
| 4 | `TB.TAILORING.WO` (queue) | OMS → tailor-shop work-order app (alterations, custom measurements) | 15 k/day | Per order | 20 KB (+ up to 30 MB CAD/measurement PDFs) | Topic `tailoring-workorders-v1`, key `orderId`; **claim-check**: attachments to GCS `gs://tb-otd-tailoring/{orderId}/…`, message carries URI + MD5 | W2 |
| 5 | `TB.RENTAL.EVENTS` (topic) | Rental app → OMS, notification, group-linking service | 8 k/day, 10× in prom season | Per rental group (wedding party) | 2 KB | Topic `rentals-v1`, ordering key `groupId`; retention 31 d (events reference dates months ahead → replay must be possible) | W2 |
| 6 | `TB.PRICE.UPDATES` (topic, last-value semantics) | Pricing → all POS, e-com, OMS | 50 k/day bursts (nightly) | Per SKU, **last value wins** | 300 B | Topic `prices-v1` for the change feed **plus** a Firestore/Memorystore "current price" store written by one consumer — Pub/Sub has no retained messages; POS reads the store at startup | W3 |
| 7 | `TB.CUSTOMER.UPD` (queue) | CRM → OMS, marketing | 20 k/day | Per customer | 3 KB | Topic `customers-v1`, key `customerId`; PII → CMEK + VPC-SC perimeter, 7 d retention, no BigQuery raw archive (only hashed ids) | W3 |
| 8 | `ERP.ORDERS.IN` (IBM MQ local queue) | BW → ERP / finance (non-JMS MQ reader, MQSTR) | 42 k/day | Per order | 4 KB | Fed by `pubsub-to-jms-bridge` from `orders-to-legacy-mq` (`targetClient=1`, persistent, `JMSCorrelationID=correlationId`) until the ERP consumes `orders-v1` directly (its own subscription, W4) | **W1** (bridge) / W4 (native) |
| 9 | `ERP.INVOICE.OUT` (IBM MQ) | ERP → BW → OMS, e-com (invoice ready) | 30 k/day nightly batch | None | 2 KB | ERP publishes to `invoices-v1` via a second `jms-to-pubsub-bridge` deployment (`JMS_PROVIDER=ibmmq`, `JMS_SOURCE=ERP.INVOICE.OUT`) — the bridge is provider-agnostic | W3 |
| 10 | `WMS.SHIP.CONFIRM` (queue) | WMS → BW → OMS + carrier notification | 25 k/day | Per shipment | 1 KB | Replaced by `shipment-webhook` (Cloud Run) → `shipments-v1` push to `notification-service`; WMS gets an HTTPS endpoint instead of a queue | W2 |
| 11 | `POS.EOD.SUMMARY` (queue) | 1 200 stores nightly → BW → finance | 1.2 k/day in a 2 h window | None | 50 KB | Topic `store-eod-v1` → BigQuery subscription directly (no consumer code); dedup by `storeId+businessDate` in BigQuery | W3 |
| 12 | `TB.ORDERS.AUDIT` (queue, TTL 7 d, never consumed) | BW copies every #1/#2 message | = #1 + #2 | — | 4 KB | **Not migrated** — it exists for the reconciler's `QueueBrowser`; its Pub/Sub counterpart is the BigQuery subscription `orders-bq-archive`. Exported to GCS at cutover for retention. | W1 (used), W4 (archived) |

Waves: **W1** orders slice (this repo + `order-intake-api` + `inventory-service`), **W2** fulfilment
(inventory, tailoring, rental, shipping), **W3** master data & finance feeds, **W4** ERP native +
decommission. Each wave repeats phases 0–4 of the runbook for its destinations; the
`MIGRATION_PHASE` switch is per wave (`MIGRATION_PHASE` env on the components of that wave).

Design rules distilled from the table:

* **Queue with one consumer → topic + one subscription**; queue with competing consumers → same, N clients.
* **Topic with durable subscribers → one subscription per former subscriber** (never share).
* **Ordering key = the business invariant** (`storeId` for the order lifecycle, `sku:location` for stock, `groupId` for rentals). Never `orderId` *and* `storeId` on the same topic.
* **Last-value topics need a store**, not a topic.
* **Big payloads → claim-check**; **PII → CMEK + VPC-SC + short retention**.
* **No new consumer ever reads legacy XML.** Translation happens once, in the bridge.

## 3. Risk register

| # | Risk | Likelihood | Impact | Mitigation | Owner |
|---|---|---|---|---|---|
| R1 | **Message loop** (legacy order → Pub/Sub → MQ → ERP twice) | Med | High (duplicate invoices) | Subscription filter `attributes.source != "TIBCO_EMS_BRIDGE"` **and** in-code guard; IT `loopGuardSwallowsEventsBridgedFromEms`; ERP dedup on `pubsubMessageId` | Integration |
| R2 | **Duplicates** from at-least-once (bridge rollback after Pub/Sub ack; Pub/Sub redelivery) | High (by design) | Med | `legacyMessageId`/`eventId` idempotency keys, inbox table in consumers, exactly-once subscription for inventory; reconciler collapses dups | Integration |
| R3 | **Ordering broken** (two bridge pods, concurrency > 1, publisher in two regions) | Med | High (cancel before create) | `replicas: 1`, `Recreate`, `BRIDGE_CONCURRENCY=1`, single region; ordering IT asserts key; alert on `bridge_running > 1` | Platform |
| R4 | **Legacy XML variants** BW emits that the hand-written mapper rejects | High early | Med | DLQ with `dlqReason`, 7-day SHADOW on real traffic, mapper tolerant to namespace prefixes/date formats, sampled byte-compare in DUAL_RUN | Integration |
| R5 | **Pub/Sub 10 MB limit** hit by tailoring attachments | Certain for #4 | Med | Claim-check to GCS; bridge rejects > 9 MB to DLQ with reason | Integration |
| R6 | **Network path GKE ↔ on-prem EMS/MQ** (VPN flaps, firewall) | Med | Med | DMLC back-off reconnect, EMS persists, Pub/Sub retains 7 d; `bridge_messages_failures_total{reason="connection"}` alert; Interconnect preferred over VPN for W2+ | Network |
| R7 | **Schema rejection** on `orders-v1` (proto schema stricter than JSON) | Med | Med | `NON_NULL` serialisation, contract tests in CI against the schema, DLQ | Integration |
| R8 | **Reconciler false positives** (clock skew, window edges, late events) | Med | Low | Window on event time not ingestion time, 15-min grace (`--to` = midnight − 15 min for same-day runs), duplicates collapsed, tolerance 0.005 on amounts | Data |
| R9 | **ERP month-end close** during DUAL_RUN | Med | High | Freeze window; DUAL_RUN must include one close before go; finance sign-off is a go/no-go gate | Finance |
| R10 | **Skill gap** (team knows BW, not Pub/Sub/Dataflow) | High | Med | This repo's docs, pairing, study guide in `tb-platform-infra`, run the local compose end-to-end in onboarding | Eng mgmt |
| R11 | **Cost surprise** (unbounded backlog, BigQuery raw archive) | Low | Low | Budget alerts, retention 7 d, `orders_raw` partition expiry 90 d | FinOps |
| R12 | **TIBCO EMS client licensing / jar distribution** | Low | Low | `-Pems` profile with locally installed `tibjms.jar`, never in git; reflection so builds don't need it | Platform |
| R13 | **Pub/Sub regional outage** | Low | High | Outbox keeps API orders; bridge leaves messages in EMS (rollback); multi-region topics not needed for RPO 0 but documented as an option (publish to two regional topics) | Platform |

## 4. Testing strategy

| Level | What | Where |
|---|---|---|
| Unit | XML ⇄ canonical round trip (incl. namespace prefixes, legacy date formats, XXE), header mapping both ways, phase table, option parsing, reconciliation join (matched / onlyLegacy / onlyPubsub / payloadMismatch, duplicates, tolerance), report rendering | `migration-common`, `reconciler` (`mvn test`, seconds) |
| Integration (Testcontainers) | Artemis (EMS stand-in) + Pub/Sub emulator: legacy XML on `TB.ORDERS.OUT` → message on `orders-v1` with attributes + ordering key; poison → `events-dlq`; canonical event → XML on `ERP.ORDERS.IN` with JMS properties; loop guard | `*IT.java`, failsafe in `mvn verify`, CI on every push |
| Integration, provider-specific | Same against the **IBM MQ** developer image (`IbmMqBridgeIT`, opt-in `RUN_MQ_IT=true`) — proves the JMS layer is provider-agnostic; EMS itself cannot run in CI (licence) → tested in the TB test environment with `-Pems` | CI `mq-integration` job on `main` |
| Contract | Published JSON validated against the Pub/Sub proto schema (`tb-platform-infra`); Postman/Newman suites in `tb-orchestration` | CI of those repos |
| End-to-end (local) | `docker-compose.yml`: Artemis + emulator + both bridges (+ IBM MQ with `--profile mq`); `local/send-legacy-order.sh` → check `/actuator/bridge`, emulator pull, Artemis `ERP.ORDERS.IN` | developer laptop / onboarding |
| Shadow on production traffic | Phase 1 — the real test: 7 days, reconciler diff = 0, DLQ empty | production |
| Load | JMeter plan in `tb-orchestration` replays a Saturday peak (150/min) ×4 through the EMS simulator; assert bridge p95 < 500 ms, no redeliveries, ordering per store intact (sequence numbers in the test payload) | pre-prod |
| Chaos | Kill the bridge pod mid-batch (expect redelivery + dedup, no loss); stop Artemis for 2 min (expect back-off reconnect); nack storm on MQ (expect DLQ after 5 attempts, replay) | pre-prod game day before Phase 2 |
| Rollback drills | Execute each runbook rollback once in pre-prod and time it (< 2 min target) | before each phase |

## 5. Metrics and SLOs

**Bridge metrics** (Micrometer → `/actuator/prometheus` → Managed Prometheus → Cloud Monitoring):

| Metric | Meaning | Alert |
|---|---|---|
| `bridge_messages_bridged_total{bridge}` | forwarded OK | rate = 0 during store hours while `bridge_running = 1` → warn |
| `bridge_messages_failures_total{bridge,reason}` | publish / send / connection failures (retried) | > 10/5 min → page |
| `bridge_messages_dlq_total{bridge}` | poison messages | > 0 → page during SHADOW/DUAL_RUN |
| `bridge_messages_duplicates_total{bridge}` | redeliveries seen | > 1 % of bridged → warn (at-least-once working too hard) |
| `bridge_messages_skipped_total{bridge}` | loop-guard skips | > 0 on `pubsub-to-jms` means the subscription filter is missing → page |
| `bridge_publish_seconds{quantile}` | forward latency | p95 > 500 ms (5 min) → warn |
| `bridge_lag_seconds` | age of the last forwarded message | > 30 s → warn |
| `bridge_last_message_age_seconds` | silence | > 900 s in store hours → warn |
| `bridge_running` | 1 consuming / 0 paused | ≠ expected for phase → page; sum > 1 per bridge → page (ordering) |

**Pub/Sub metrics** (Cloud Monitoring): `subscription/num_undelivered_messages`,
`subscription/oldest_unacked_message_age` (> 300 s → page), `subscription/dead_letter_message_count`
(> 0 → page), `topic/send_request_count` by response code (non-OK > 1 % → warn),
`subscription/expired_ack_deadlines_count` (consumer too slow).

**Reconciler**: `otd.migration_reconciliation` row per run; scheduled query alert when
`only_legacy + only_pubsub + mismatched > 0`.

**SLOs for the migrated orders slice** (measured in BigQuery / Cloud Monitoring, 30-day window):

| SLO | Target |
|---|---|
| Order availability on `orders-v1` (API accepted → event published) | 99.9 % within 5 s |
| Bridge forward latency EMS → Pub/Sub (p95) | < 500 ms |
| Order → ERP (via Pub/Sub → MQ bridge) p95 | < 5 s |
| Reconciliation parity | diff = 0 daily (hard gate for phase exit) |
| Message loss | 0 (at-least-once + DLQ; every DLQ message replayed within 1 business day) |
| Duplicate orders reaching ERP | 0 (dedup on `pubsubMessageId`) |

## 6. What "done" looks like

* All twelve destinations in the inventory are either migrated (topic/subscription/consumer live,
  legacy destination idle for 30 days) or explicitly retired.
* No BW process starts from a JMS destination; BW engines shut down; EMS FT pair and MQ queue
  manager decommissioned; licences returned.
* `jms-to-pubsub-bridge` / `pubsub-to-jms-bridge` deployments deleted; the code stays in this repo
  (the next acquisition will have an EMS too).
* Runbook, concept mapping and the reconciliation history in BigQuery are the audit trail.
