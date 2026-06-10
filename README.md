# kafka-dlp-streamshield

A production-grade Kafka data-pipeline with field-level DLP tokenization, Avro Schema Registry enforcement, and automated stream delivery to Snowflake — built entirely on GCP Managed Services

## What's in this repo

| Folder | Purpose |
|---|---|
| [`poc/`](poc/) | Architecture decision records and step-by-step GCP setup guides used during the proof-of-concept |
| [`terraform/`](terraform/) | Terraform templates to reproduce the full GCP environment in any new project |
| [`streamshield/`](streamshield/) | Python SDK that packages the POC patterns into an installable library |

## Pipeline at a glance

```
Application (StreamShield SDK)
  │  Avro + Confluent wire format, SASL_SSL
  ▼
GCP Managed Kafka  ──  GCP Managed Schema Registry
  │
  │  Managed Kafka Connect — GCS Sink Connector
  ▼
GCS  (Avro → JSON lines)
  │  Snowpipe auto-ingest
  ▼
Snowflake
```

## Quick start

**Reproduce the infrastructure** — see [`terraform/APPLY-GUIDE.md`](terraform/APPLY-GUIDE.md).  
**Use the SDK** — see [`streamshield/README.md`](streamshield/README.md).  
**Understand the design decisions** — see [`poc/001_architecture-overview.md`](poc/001_architecture-overview.md).

## Key technology choices

- **Schema Registry:** GCP Managed (not Confluent Cloud) — single-cloud, OAuth via existing service account
- **Stream delivery:** Managed Kafka Connect GCS Sink (not Dataflow) — no custom pipeline code, Snowpipe latency <60 s
- **DLP tokenization:** field-level, batched at 100 records/call, policy embedded in Avro schema metadata
- **Offset commit:** always manual — committed only after successful downstream write
