# Cutover runbook — TIBCO EMS / IBM MQ → Cloud Pub/Sub (phases 0 → 4)

> Operational companion to `ARCHITECTURE.md` §7 and `MIGRATION-STRATEGY.md`. One section per
> phase, each with **pre-checks → commands → verification → go/no-go → rollback**. Commands are
> written for the `otd` namespace on GKE Autopilot cluster `tb-otd-autopilot` (us-central1) and
> assume `gcloud` and `kubectl` are authenticated. Replace `$PROJECT` with the GCP project id.

## 0. How a phase change actually propagates

```
                  ┌──────────── authoritative (survives restarts) ─────────────┐
  operator ──▶ kubectl set env deploy/<bridge> MIGRATION_PHASE=<P>  (Recreate rollout, ~30 s)
           ──▶ gcloud pubsub topics publish migration-control --attribute phase=<P>
                  └──── fast path: every bridge with MIGRATION_CONTROL_SUBSCRIPTION flips in < 1 s,
                        no restart; a restarted pod falls back to the env value ────┘
```

* **Always do both**, env first (persistence), then the topic (speed) — or accept the rollout latency and skip the topic.
* Each bridge deployment has its **own** control subscription (`migration-control-jms-to-pubsub`,
  `migration-control-pubsub-to-jms`); a shared subscription would deliver the phase to only one of them.
* `GET /actuator/bridge` on every bridge shows `bridge.state` (`RUNNING`/`PAUSED`), `phase`, `phaseOrigin`.
* Phase → bridge state table (`MigrationPhase.java`):

| Phase | jms-to-pubsub-bridge (EMS → Pub/Sub) | pubsub-to-jms-bridge (Pub/Sub → MQ) | order-intake-api |
|---|---|---|---|
| 0 `LEGACY_ONLY` | PAUSED | PAUSED | not yet taking traffic |
| 1 `SHADOW` | RUNNING | PAUSED | shadow (no side effects) |
| 2 `DUAL_RUN` | RUNNING | RUNNING | dual-write (Pub/Sub primary + EMS) |
| 3 `PUBSUB_PRIMARY` | RUNNING (drain only — EMS source idle) | RUNNING | Pub/Sub only |
| 4 `CUTOVER` | PAUSED | PAUSED | Pub/Sub only |

Helper variables used below:

```bash
export PROJECT=$(gcloud config get-value project)
export NS=otd
alias phase-set='f(){ kubectl -n $NS set env deploy/jms-to-pubsub-bridge deploy/pubsub-to-jms-bridge MIGRATION_PHASE=$1 && \
  gcloud pubsub topics publish migration-control --attribute=phase=$1 --message=$1 && \
  kubectl -n $NS rollout status deploy/jms-to-pubsub-bridge && kubectl -n $NS rollout status deploy/pubsub-to-jms-bridge; }; f'
alias bridge-state='for d in jms-to-pubsub-bridge pubsub-to-jms-bridge; do echo "== $d"; kubectl -n $NS exec deploy/$d -- curl -s localhost:${d/jms-to-pubsub-bridge/8090}; done'
```

(`bridge-state` is easier as `kubectl -n otd port-forward deploy/jms-to-pubsub-bridge 8090:8090` then
`curl -s localhost:8090/actuator/bridge | jq`.)

---

## Phase 0 → `LEGACY_ONLY` (baseline)

**Goal:** everything deployed, nothing bridged, baseline numbers captured.

### Pre-checks
- [ ] Terraform applied: topics `orders-v1`, `events-dlq`, `migration-control`; subscriptions `orders-to-legacy-mq` (filter `attributes.source != "TIBCO_EMS_BRIDGE"`, ordering), `events-dlq-monitor`, `migration-control-jms-to-pubsub`, `migration-control-pubsub-to-jms`, `orders-bq-archive`.
- [ ] BigQuery tables `otd.order_events`, `otd.migration_reconciliation` exist.
- [ ] Secrets in namespace `otd`: `otd-jms` (`url`, `username`, `password` for EMS — used by jms-to-pubsub-bridge) and `otd-mq` (same keys for IBM MQ — used by pubsub-to-jms-bridge); EMS user `bridge` has `receive` on `TB.ORDERS.OUT`, `browse` on `TB.ORDERS.AUDIT`; MQ user `app` can `PUT` on `ERP.ORDERS.IN`. (Both are created by the `cluster-config` job of tb-platform-infra.)
- [ ] Workload Identity: KSA `jms-to-pubsub-bridge`/`pubsub-to-jms-bridge` → GSA `tb-migration-bridge` with `pubsub.publisher` (orders-v1, events-dlq) and `pubsub.subscriber` (orders-to-legacy-mq, migration-control-*).
- [ ] Network: GKE → EMS 7222 and MQ 1414 reachable (VPN/Interconnect firewall rule), `nc -vz` from a debug pod.
- [ ] TIBCO BW writes a copy of every outbound order to `TB.ORDERS.AUDIT` (TTL 7 d) — the reconciler's legacy side.

### Commands
```bash
kubectl apply -k jms-to-pubsub-bridge/deploy/k8s/overlays/gcp
kubectl apply -k pubsub-to-jms-bridge/deploy/k8s/overlays/gcp
phase-set LEGACY_ONLY
```

### Verification
```bash
curl -s localhost:8090/actuator/bridge   # {"bridge.state":"PAUSED","phase":"LEGACY_ONLY",...}
curl -s localhost:8090/actuator/health   # status UP, bridge.jmsConnection UP (connection works even while paused)
curl -s localhost:8091/actuator/bridge   # {"bridge.state":"PAUSED",...}
```
Baseline (run daily for a week, keep the numbers):
```sql
-- legacy volume per day/store from the OMS extract already in BigQuery (tb-order-events-dataflow daily job)
SELECT DATE(ordered_at) d, store_id, COUNT(*) orders, SUM(total_amount) revenue
FROM `otd.legacy_oms_orders` WHERE extract_date >= CURRENT_DATE() - 7
GROUP BY d, store_id ORDER BY d, store_id;
```

### Go / no-go for Phase 1
- Both bridges `UP` + `PAUSED`, JMS connection UP, 0 restarts in 24 h.
- Baseline captured (orders/day, peak orders/min, p95 EMS→OMS latency from BW stats).

### Rollback
Nothing to roll back; `kubectl delete -k …` removes the bridges.

---

## Phase 1 → `SHADOW`

**Goal:** EMS → Pub/Sub bridge on; new consumers (inventory-service in shadow mode, Dataflow → BigQuery) run on real traffic without side effects. **Legacy is still the system of record.**

### Pre-checks
- [ ] `inventory-service` deployed with `MIGRATION_PHASE=SHADOW` (consumes `orders-inventory-service`, writes nothing externally, logs only).
- [ ] Dataflow `order-events-streaming` running, `orders-dataflow` subscription backlog ≈ 0.
- [ ] Alert policies enabled: `events-dlq` backlog > 0 (5 min), `orders-to-legacy-mq`/`orders-inventory-service` `oldest_unacked_message_age` > 300 s, bridge `bridge_last_message_age_seconds` > 900 during store hours.
- [ ] Change ticket / comms sent (template §6).

### Commands
```bash
phase-set SHADOW
# watch the first messages
kubectl -n $NS logs deploy/jms-to-pubsub-bridge -f | grep "Bridged JMS"
```

### Verification
```bash
curl -s localhost:8090/actuator/bridge      # bridge.state RUNNING, lastMessageAgeSeconds small, lagMillis < 2000
curl -s localhost:8090/actuator/prometheus | grep -E 'bridge_messages_(bridged|failures|dlq)_total'
gcloud pubsub subscriptions describe orders-to-legacy-mq --format='value(filter)'   # filter present
gcloud monitoring metrics list --filter='metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages"' >/dev/null
```
```sql
-- (a) bridged events are landing with the right provenance
SELECT source, event_type, COUNT(*) n, MIN(event_time) first_seen, MAX(event_time) last_seen
FROM `otd.order_events` WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY source, event_type;

-- (b) every bridged event carries its JMS id (dedup key) and exactly one event per legacy message
SELECT COUNT(*) events, COUNT(DISTINCT legacy_message_id) legacy_ids, COUNTIF(legacy_message_id IS NULL) missing_ids
FROM `otd.order_events` WHERE source = 'TIBCO_EMS_BRIDGE' AND event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);

-- (c) DLQ must be empty
SELECT dlq_stage, dlq_reason, COUNT(*) FROM `otd.dead_letter`
WHERE ingestion_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) GROUP BY 1, 2;
```
Daily reconciliation (Cloud Run Job or CronJob; see `reconciler/README`):
```bash
gcloud run jobs execute tb-reconciler --region us-central1 --wait \
  --args="--from=$(date -u -d 'yesterday 00:00' +%FT%TZ),--to=$(date -u -d 'today 00:00' +%FT%TZ),--phase=SHADOW,--legacyQueue=TB.ORDERS.AUDIT,--bigQueryDataset=otd,--failOnDiff=true"
```
```sql
SELECT run_time, phase, matched, only_legacy, only_pubsub, mismatched
FROM `otd.migration_reconciliation` ORDER BY run_time DESC LIMIT 14;
```

### Go / no-go for Phase 2 (hold SHADOW ≥ 7 days)
- `only_legacy + only_pubsub + mismatched = 0` for **7 consecutive daily runs** (ARCHITECTURE §7 "reconciler diff = 0").
- `bridge_messages_dlq_total` = 0 over the window (or every DLQ message root-caused and fixed in the mapper).
- Bridge p95 lag (`bridge_publish_seconds`) < 500 ms; `oldest_unacked_message_age` on `orders-inventory-service` < 60 s at peak.
- inventory-service shadow decisions match OMS for a sampled day (manual spot check of 50 orders).
- No bridge restarts caused by JMS/Pub/Sub errors (`bridge_messages_failures_total{reason="connection"}` flat).

### Rollback (SHADOW → LEGACY_ONLY)
```bash
phase-set LEGACY_ONLY        # bridge pauses; EMS keeps the messages (TB.ORDERS.OUT is still consumed by OMS as before)
```
Nothing downstream was authoritative, so rollback is instant and lossless. If the bridge **stays** up
but produces wrong events: pause it, `gcloud pubsub subscriptions seek orders-inventory-service --time=<now>` is *not*
needed in shadow (consumers have no side effects) — just fix and resume; the EMS queue was never emptied
by us (the bridge is an additional consumer of the audit copy, or the queue is shared by OMS — see
MIGRATION-STRATEGY §2 for the destination-by-destination answer).

---

## Phase 2 → `DUAL_RUN`

**Goal:** new API dual-writes (Pub/Sub primary + EMS); legacy stores still enter via EMS → bridge; Pub/Sub → MQ bridge keeps the ERP fed; **new consumers become authoritative**, legacy consumers read-only.

### Pre-checks
- [ ] `order-intake-api` with `MIGRATION_PHASE=DUAL_RUN` and `LEGACY_JMS_URL` set (dual-write to EMS via its own outbox).
- [ ] OMS/ERP teams confirmed the ERP accepts XML from `ERP.ORDERS.IN` sent by the bridge (JMSCorrelationID, `storeId`/`eventType` properties, `targetClient=1` if the ERP reader is non-JMS).
- [ ] Loop guard confirmed: `orders-to-legacy-mq` has filter `attributes.source != "TIBCO_EMS_BRIDGE"` **and** the bridge's in-code guard is on (`bridge.loop-guard-source=TIBCO_EMS_BRIDGE`). Without this, a legacy order would come back to the ERP twice.
- [ ] Legacy consumers of `TB.ORDERS.OUT` switched to **read-only** mode (OMS no longer reserves inventory; `inventory-service` does).
- [ ] Reconciler can run in both directions (phase 1 numbers clean for 7 days).
- [ ] Freeze window agreed (no BW or EMS config changes during DUAL_RUN).

### Commands
```bash
phase-set DUAL_RUN
kubectl -n $NS logs deploy/pubsub-to-jms-bridge -f | grep -E "Bridged Pub/Sub|Loop guard|Poison"
```

### Verification
```bash
curl -s localhost:8091/actuator/bridge      # RUNNING, DUAL_RUN
curl -s localhost:8091/actuator/prometheus | grep -E 'bridge_messages_(bridged|skipped|dlq|failures)_total'
# ERP side: the erp-mq-consumer simulator (or MQ Explorer) shows new XML on ERP.ORDERS.IN
curl -s http://erp-mq-consumer.legacy:8087/received | jq '.[0]'
gcloud pubsub subscriptions describe orders-to-legacy-mq --format='value(deadLetterPolicy, filter)'
```
```sql
-- (a) dual-write parity: every API order appears once on Pub/Sub (source ORDER_INTAKE_API) and once in the legacy audit (reconciler)
-- (b) loop check: nothing with source TIBCO_EMS_BRIDGE may have reached the ERP → bridge counter must stay 0 except skips
-- (c) end-to-end latency store → ERP
SELECT APPROX_QUANTILES(TIMESTAMP_DIFF(event_time, orders.ordered_at, MILLISECOND), 100)[OFFSET(95)] p95_ms
FROM `otd.order_events` e JOIN `otd.order_events` orders USING (order_id)
WHERE e.event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```
Reconciler daily with `--phase=DUAL_RUN` (legacy side now also includes the API's dual-written copies).

### Go / no-go for Phase 3 (hold DUAL_RUN ≥ 14 days incl. a weekend peak and a month-end close)
- Reconciler diff = 0 for the whole window; `bridge_messages_dlq_total` on both bridges = 0.
- Latency SLO met: p95 order → Pub/Sub consumer < 2 s; p95 order → ERP (via bridge) < 5 s, both at peak.
- ERP team signs off on volume parity (`ERP.ORDERS.IN` depth, no backouts) and on content (sampled 100 orders byte-compared with BW output).
- Store rollout plan for Phase 3 (legacy POS → new API) complete, waves scheduled.

### Rollback (DUAL_RUN → SHADOW)
```bash
phase-set SHADOW                               # Pub/Sub→MQ bridge pauses; API stops dual-writing... careful: see below
kubectl -n $NS set env deploy/order-intake-api MIGRATION_PHASE=SHADOW
```
Consequence: the ERP stops receiving API orders through the bridge — in SHADOW the API must not take
production traffic, so **also** flip Apigee to route `/v1/orders` back to the legacy SOAP endpoint
(`apigee … --target legacy`) and switch legacy consumers back to authoritative. Orders that were
already in `orders-to-legacy-mq` but not sent are safe: the subscription retains them (7 d); when the
bridge resumes they are delivered (ordered, deduplicated by `pubsubMessageId` on the ERP side).
Partial rollback option: keep DUAL_RUN but pause only the Pub/Sub→MQ bridge
(`curl -X POST localhost:8091/actuator/bridge -H 'content-type: application/json' -d '{"phase":"SHADOW"}'`) — instance-local, resets on restart.

---

## Phase 3 → `PUBSUB_PRIMARY`

**Goal:** all producers (stores, e-com) on Pub/Sub; EMS has no new input; only Pub/Sub → MQ remains because the ERP is not migrated yet.

### Pre-checks
- [ ] All store waves on the new API (Apigee analytics: 0 calls on legacy SOAP target for 24 h, EMS `TB.ORDERS.OUT` inbound rate 0 via `tibemsadmin show queue TB.ORDERS.OUT`).
- [ ] BW process `OrderOut` stopped (or left running — it has nothing to do).
- [ ] `order-intake-api` `MIGRATION_PHASE=PUBSUB_PRIMARY` (dual-write off).

### Commands
```bash
phase-set PUBSUB_PRIMARY
# the EMS→Pub/Sub bridge stays RUNNING as a drain: any straggler from a store that missed the wave still arrives
```

### Verification
```bash
curl -s localhost:8090/actuator/bridge | jq .lastMessageAgeSeconds   # grows and grows: EMS is idle
tibemsadmin -server tcp://ems:7222 -user admin -script - <<< "show queue TB.ORDERS.OUT"   # pending 0, in rate 0
curl -s localhost:8091/actuator/bridge                                # RUNNING, feeding ERP
```
```sql
SELECT source, COUNT(*) FROM `otd.order_events`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) GROUP BY source;   -- TIBCO_EMS_BRIDGE → 0
```

### Go / no-go for Phase 4
- 0 events with `source = TIBCO_EMS_BRIDGE` for 7 days.
- ERP migrated to consume Pub/Sub directly (its own subscription on `orders-v1`) **or** ERP decommissioned; `orders-to-legacy-mq` backlog 0.
- Reconciler clean (legacy side = empty).

### Rollback (PUBSUB_PRIMARY → DUAL_RUN)
```bash
phase-set DUAL_RUN
kubectl -n $NS set env deploy/order-intake-api MIGRATION_PHASE=DUAL_RUN   # dual-write to EMS again
```
Stores stay on the new API (there is no reason to send them back); legacy consumers get their EMS feed from the API's dual-write.

---

## Phase 4 → `CUTOVER`

**Goal:** EMS / MQ decommissioned.

### Pre-checks
- [ ] ERP on Pub/Sub; `orders-to-legacy-mq` **deleted** (or left paused one more week — it costs retention only).
- [ ] EMS audit queue exported to GCS (`TB.ORDERS.AUDIT` → `gs://tb-otd-migration-archive/ems-audit/…`) for the record retention policy (7 years for orders).
- [ ] Hawk rules / EMS alerts disabled so nobody gets paged for an idle broker.

### Commands
```bash
phase-set CUTOVER                       # both bridges PAUSED
kubectl -n $NS scale deploy/jms-to-pubsub-bridge deploy/pubsub-to-jms-bridge --replicas=0
# a week later:
kubectl delete -k jms-to-pubsub-bridge/deploy/k8s/overlays/gcp
kubectl delete -k pubsub-to-jms-bridge/deploy/k8s/overlays/gcp
gcloud pubsub subscriptions delete orders-to-legacy-mq migration-control-jms-to-pubsub migration-control-pubsub-to-jms
# tibemsd / MQ queue manager shutdown by the middleware team per their decommission checklist
```

### Verification
- `otd.order_events` volume unchanged day-over-day (only `source` changed); no `events-dlq` growth.
- Cost: Pub/Sub + GKE bridge pods removed from the bill; EMS/MQ licences returned.

### Rollback (CUTOVER → PUBSUB_PRIMARY)
Possible **only while EMS/MQ are still running** (the one-week grace): `phase-set PUBSUB_PRIMARY`, scale the
Pub/Sub→MQ bridge to 1, recreate `orders-to-legacy-mq` with `seek --time=<cutover time>` so the ERP
receives everything since the cutover. After decommissioning there is no rollback — this is why Phase 3 is held long.

---

## 5. Incident playbooks

| Symptom | Likely cause | Action |
|---|---|---|
| `bridge.state=PAUSED` but phase says RUNNING | Pod restarted and `MIGRATION_PHASE` env is stale (control message was not persisted) | `phase-set <P>` (env + topic) |
| `bridge_messages_failures_total{reason="publish"}` climbing, JMS redeliveries | Pub/Sub unreachable / IAM / schema rejection | Check `kubectl logs`, `gcloud pubsub topics publish orders-v1 --message='{}'` with the GSA; messages are safe in EMS (rolled back, redelivered with back-off) |
| `events-dlq` growing with `dlqStage=jms-to-pubsub-bridge` | BW emitted an XML variant the mapper rejects | Read `dlqReason`, fix `LegacyXmlMapper`, redeploy, replay from the DLQ (`gcloud pubsub subscriptions pull events-dlq-monitor --auto-ack` → republish to `orders-v1` with `source=REPLAY`) |
| Pub/Sub→MQ bridge `nack` storms, `delivery_attempt` rising | MQ down / queue full (`MQRC_Q_FULL`) | MQ team; backlog waits in `orders-to-legacy-mq` (7 d retention). After 5 attempts messages go to the DLQ — replay after MQ is back |
| Duplicate orders in ERP | Bridge committed Pub/Sub ack after an MQ send whose commit raced a pod kill | ERP dedups on `pubsubMessageId` JMS property (documented contract); verify with `SELECT pubsub_message_id, COUNT(*) … HAVING COUNT(*) > 1` on the ERP staging table |
| Ordering violation reported by inventory | Two bridge replicas (HPA added by mistake) or `BRIDGE_CONCURRENCY > 1` | Enforce `replicas: 1`, `Recreate`, concurrency 1 — the deployment manifest pins all three |
| `oldest_unacked_message_age` alert on `orders-inventory-service` | Consumer slow/down, not the bridge | Standard consumer runbook in `tb-integration-services` |

## 6. Communication template

```
Subject: [OTD migration] Phase <N> <NAME> — <date> <time> UTC

What changes: <one line, e.g. "EMS→Pub/Sub bridge turned on in shadow mode; legacy remains system of record">
Impact: none expected for stores / e-com / ERP. <or: ERP now receives orders through the Pub/Sub→MQ bridge>
Window: <start> – <end> UTC, change ticket <CHG-…>
Go/no-go met: reconciler diff = 0 for <n> days (link to BigQuery saved query), DLQ empty, latency p95 <x> ms
Rollback: `phase-set <previous>` (< 2 min), decision owner: <name>, on-call: <pager>
Dashboards: <Cloud Monitoring dashboard "OTD migration">, <Grafana bridge_* panel>
Next phase earliest: <date>, criteria in RUNBOOK.md §Phase <N+1>
```

## 7. Checklist cheat sheet (print this)

```
[ ] env + topic both updated          [ ] /actuator/bridge on both bridges shows expected state
[ ] DLQ empty                         [ ] reconciler clean (N days)       [ ] latency SLO
[ ] alerts enabled & quiet            [ ] comms sent                      [ ] rollback command ready
```
