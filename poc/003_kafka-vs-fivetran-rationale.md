# Why Kafka Pipeline Over Fivetran — Architectural Rationale
**VetSource · Data Modernization Engagement · Workstreams 1 & 2**

---

## The Question

> VetSource already uses Fivetran for RDBMS → Snowflake ingestion. Why build a complex Kafka / Kafka Connect / GCS / Snowpipe pipeline? Why not schedule Fivetran to run every 5 minutes — including against a Kafka topic?

---

## Primary Constraint: Workstream 2 Eliminates Fivetran

Workstream 2 mandates **producer-side tokenization** — PII is de-identified via Google Cloud DLP *before the message is published to Kafka*. This is not a feature preference; it is an architectural constraint.

```
PII at source  →  Cloud DLP (producer.py)  →  Tokenized event  →  Kafka  →  GCS  →  Snowflake
```

Raw PII never enters the pipeline. Every downstream system — Kafka, GCS, Snowflake — receives tokens only.

**Fivetran cannot satisfy this requirement.** It is a managed SaaS connector with no injection point between extraction and load. The only available tokenization points with Fivetran are post-load transformations in Snowflake — at which point raw PII has already transited Fivetran's infrastructure and landed in the warehouse. The "at the source" guarantee is structurally broken.

> **Workstream 2 alone is sufficient to rule out Fivetran as the primary transport.**

---

## Supporting Architectural Reasons

### 1 · Events vs. State
Fivetran — at any sync frequency — captures a **snapshot of current DB state**. If an order transitions `PENDING → PROCESSING → PAYMENT_FAILED → CANCELLED` within a 5-minute window, Fivetran records only `CANCELLED`. Kafka captures every state transition as a discrete, ordered, durable event. For VetSource's prescription fulfillment and compliance workflows, intermediate states are operationally critical and cannot be lost.

### 2 · VPC-Private Kafka
The Managed Kafka cluster is **PSC-only with no public endpoint**. Fivetran SaaS operates outside GCP and has no network path to the cluster without either opening it to the internet (security violation) or deploying Fivetran's private networking agent (significant operational overhead). Kafka Connect runs inside the VPC natively — no exposure required.

### 3 · Multi-Consumer Fan-Out
A single Kafka topic must simultaneously serve Snowflake, pharmacy fulfillment, inventory management, client notification, and PIMS Rx writeback. Fivetran is point-to-point: one source, one destination. Kafka Connect supports multiple independent sink connectors against the same topic. Removing Kafka Connect does not simplify the architecture — it removes the fan-out layer without a replacement.

### 4 · Cost at VetSource's Event Volume
Fivetran pricing is **per Monthly Active Row (MAR)**. At 9,000+ practices generating prescription, AutoShip, fulfillment, and engagement events, MAR costs scale to tens of millions of rows per month — a significant recurring expense. Kafka Connect + GCS + Snowpipe is pure infrastructure cost with no per-row charge, and is substantially more economical at this volume.

### 5 · GCS as a Durable Replay Buffer
GCS is not merely a transit hop. It provides: (a) decoupling between Kafka and Snowpipe so a Snowpipe outage does not require Kafka replay, (b) indefinite file retention beyond Kafka's 7-day window, and (c) an immutable audit record of every ingested payload — important for compliance and data lineage.

### 6 · Data Residency
With Kafka Connect → GCS → Snowpipe, event data remains entirely within GCP (`us-central1`) until it reaches Snowflake. Fivetran would route VetSource's prescription and pet health event data through third-party SaaS infrastructure — a data residency and contractual concern for a Mars Petcare subsidiary.

---

## Decision Summary

| Criterion | Fivetran (any configuration) | Kafka / Connect / GCS / Snowpipe |
|---|:---:|:---:|
| Producer-side PII tokenization (WS-2) | ✗ | ✓ |
| Captures intermediate event states | ✗ | ✓ |
| Operates within VPC-private Kafka topology | ✗ | ✓ |
| Multi-consumer fan-out | ✗ | ✓ |
| Cost-efficient at tens of millions of events/month | ✗ | ✓ |
| Data remains within GCP boundary | ✗ | ✓ |
| Suitable for structured RDBMS → Snowflake replication | ✓ | Not required |

---

## Conclusion

Fivetran remains the right tool for VetSource's existing use case: structured RDBMS data (product catalog, customer master, historical tables) replicated into Snowflake on a schedule. It should continue to serve that function.

For the streaming pipeline, Fivetran is not a simpler alternative — it is an architecturally incompatible one. The Kafka pipeline is the **only design that satisfies Workstream 2's producer-side tokenization requirement**. Once that constraint is accepted, Kafka Connect, GCS, and Snowpipe are the natural, cost-effective, VPC-native continuation of that same pipeline — not additional complexity, but a coherent end-to-end design.

---

*Document scope: Workstream 1 (NRT Ingestion Architecture) + Workstream 2 (Data Security & Privacy) · VetSource Data Modernization POC*
