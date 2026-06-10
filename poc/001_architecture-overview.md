# Kafka → Snowflake Streaming Pipeline — Architecture Overview
**VetSource · Data Modernization POC · Approach A (Managed Kafka Connect)**

---

## 1. Infrastructure at a Glance

<img src="kafka-to-snowflake-approach-a.svg" alt="PII Tokenization and Kafka → Snowflake Ingestion Flow" width="8000">

---

## 2. End-to-End Message Flow

| Step | What Happens | Technology |
|------|-------------|------------|
| **① Publish** | `producer.py` serializes an `OrderEvent` as Avro (Confluent wire format: magic byte `0x00` + 4-byte schema ID + binary payload). For PII fields (`customer_id`, `customer_name`, `email`, etc.) Cloud DLP deterministic encryption runs **before** publish — tokens enter Kafka, never raw PII. | GCE VM → GCP Managed Kafka |
| **② Poll** | Kafka Connect GCS Sink Connector polls `raw-events` using the same SASL_SSL / OAUTHBEARER token auth as the producer. Credentials are pulled from Secret Manager at connector start — not baked into config. | Managed Kafka Connect |
| **③ Decode** | The connector contacts Confluent Schema Registry (using `schema-registry-api-key` / `schema-registry-api-secret` from Secret Manager) to resolve the schema ID and deserialize the Avro payload into a typed record. Schema mismatch fails fast here — bad records never reach GCS. | Confluent Schema Registry (external) |
| **④ Flush to GCS** | The connector batches decoded records and flushes a JSON file to `kafka-poc-gcs-landing` every **60 seconds** (configurable). File naming includes topic, partition, and offset — making every file individually replayable. | Cloud Storage |
| **⑤ Notify** | When a file is finalized in the GCS bucket, an `OBJECT_FINALIZE` event fires automatically to the Pub/Sub topic `kafka-poc-gcs-notify`. No polling loop — event-driven latency. | Cloud Pub/Sub |
| **⑥ Auto-Ingest trigger** | Snowpipe's `AUTO_INGEST = TRUE` pipe subscribes to the Pub/Sub subscription. It receives the file-ready notification and queues the file for ingestion within seconds. | Snowpipe (Snowflake) |
| **⑦ COPY INTO** | Snowpipe executes `COPY INTO POC_DB.KAFKA_INGEST.ORDER_EVENTS` from the GCS stage, mapping JSON fields to table columns. `INGESTED_AT` is stamped by the pipeline. The GCS file remains as an immutable audit record. | Snowflake |

**Total end-to-end latency (steady state):** ~60–90 seconds (dominated by the 60s GCS flush window + Pub/Sub/Snowpipe queue time of ~5–30s). Flush interval is tunable — lower values reduce latency at the cost of more small files.

---

## 3. Why This Architecture

### Low Latency with High Confidence

- **Event capture, not state snapshots.** Every state transition (`PENDING → PROCESSING → PAYMENT_FAILED → CANCELLED`) is a discrete, ordered Kafka message. An intermediate state that occurs and resolves within the flush window is still captured in the file — unlike a polling-based extractor that would only see the final state.
- **Schema enforcement at the wire.** Confluent Schema Registry validates every message at step ③ before it touches GCS. A schema-breaking change in the producer is caught immediately — no silent schema drift accumulating in Snowflake.
- **Offset-based file naming.** Each GCS file encodes topic + partition + start/end offsets. If a downstream failure forces a replay, the exact offset range to re-consume is derivable from the filename — no guesswork.
- **Secret Manager at runtime.** No credentials in code or connector config files. Credential rotation in Secret Manager is immediately picked up on next connector restart — no redeployment.

### Maximum Uptime

| Component | Uptime Mechanism |
|-----------|-----------------|
| GCP Managed Kafka | Google-managed, 3-broker cluster with automatic failover. Kafka's ISR (in-sync replicas) ensures no message loss during a broker restart. |
| Managed Kafka Connect | Fully managed by GCP — no connector process to babysit. Google handles restarts, upgrades, and scaling. |
| Cloud Storage | 99.999999999% durability, 99.99% availability SLA. GCS is the decoupling layer — a Snowpipe outage does not require Kafka replay; files wait in GCS until Snowpipe recovers. |
| Cloud Pub/Sub | Serverless, Google-managed. Pub/Sub retains unacknowledged notifications for up to 7 days — surviving extended Snowpipe downtime without message loss. |
| Snowpipe | Serverless Snowflake service. Auto-scales to file queue depth. A warehouse outage does not block Snowpipe — it queues files and processes them when the warehouse resumes. |
| Snowflake Warehouse (`POC_WH`) | Auto-suspends after 1 minute idle, auto-resumes on load — zero cost during quiet periods, instant scale-out when Snowpipe sends work. |

**Key design insight:** GCS acts as a **durable replay buffer** between Kafka and Snowflake. Kafka's 7-day retention window is the primary recovery boundary; GCS files can be retained indefinitely and re-ingested via manual `COPY INTO` if needed.

### Dead Letter Queue (DLQ)

**Current status (Phase 1 POC):** No DLQ is wired. Schema validation at step ③ is the failure gate — records that fail deserialization are dropped with a connector error log.

**Phase 2 design:** A `raw-events-dlq` Kafka topic will receive any message the GCS Sink Connector cannot process (deserialization failure, schema mismatch, malformed payload). A separate monitoring consumer reads the DLQ topic, alerts on non-zero lag, and surfaces the raw bytes for forensic inspection — without blocking the main `raw-events` pipeline.

```
raw-events  ──(normal)──▶  GCS Sink Connector  ──▶  GCS  ──▶  Snowflake
                 │
                 └──(error)──▶  raw-events-dlq  ──▶  Alert / forensic consumer
```

This keeps the happy path unblocked while ensuring no event is silently discarded.

---

## 4. Alignment with Workstreams

### Workstream 1 — NRT Ingestion Architecture

This pipeline **is** Workstream 1. Its deliverables:

- Sub-2-minute end-to-end latency from event publish to Snowflake row availability (60s flush + ~30s Snowpipe queue).
- No per-row SaaS cost (Fivetran MAR model ruled out at VetSource's event volume — 9,000+ practices × multiple event types per order = tens of millions of events/month).
- VPC-private Kafka topology: GCP Managed Kafka is PSC-only with no public endpoint. Kafka Connect runs inside the same VPC — no data transits the public internet between producer and GCS.
- Multi-consumer fan-out capability: additional Kafka Connect sink connectors can be attached to `raw-events` for pharmacy fulfillment, inventory, PIMS Rx writeback, and client notification — all reading the same topic independently, with no coupling between consumers.
- GCS as immutable audit log: every ingested file is retained indefinitely, providing a complete, replayable history beyond Kafka's 7-day window. Critical for compliance and data lineage.

### Workstream 2 — Data Security & Privacy

Workstream 2 mandates that **raw PII never enters the pipeline**. This architecture enforces that constraint structurally:

```
Source DB (raw PII)
       │
       ▼  producer.py
   Cloud DLP
   Deterministic encryption (AES-SIV, key in Cloud KMS)
       │
       ▼
   Tokenized OrderEvent  ──▶  Kafka  ──▶  GCS  ──▶  Snowflake
```

**Fields tokenized before Kafka publish:** `customer_id`, `customer_name`, `email`, `phone`, `shipping_address`
**Fields passed through untouched:** `order_id`, `product_id`, `amount`, `currency`, `status`, `timestamp`

Every downstream system — Kafka broker, GCS file, Snowflake table — holds tokens only. There is no stage in the pipeline where raw PII could be intercepted.

**Why this rules out Fivetran for this workstream:** Fivetran is a managed SaaS connector with no injection point between extraction and load. The only tokenization option with Fivetran is a post-load Snowflake transformation — at which point raw PII has already transited Fivetran's infrastructure. The "at the source" guarantee is structurally impossible with Fivetran.

**De-tokenization (controlled access, not pipeline):** Authorized access to original PII uses one of two patterns without touching the pipeline:
1. **Row-level lookup** — Snowflake External Function → Cloud DLP `reidentifyContent` API (~50ms/batch). For individual record investigation.
2. **Batch de-tokenization** — approval-gated Cloud DLP GCS job against exported Snowflake data. Produces an ephemeral, time-limited restricted table. Supports tens of millions of rows within minutes to hours. Every step logged in Cloud Audit Logs and Snowflake Access History.

---

*Scope: Workstream 1 (NRT Ingestion) + Workstream 2 (Data Security & Privacy) · VetSource Data Modernization POC*
