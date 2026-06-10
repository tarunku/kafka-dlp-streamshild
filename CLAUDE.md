# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Repository Layout

```
kafka-dlp-streamshild/              ← git root
├── CLAUDE.md                       ← this file
├── poc/                            ← architecture decisions + GCP setup guides (reference only)
│   ├── 001_architecture-overview.md
│   ├── 002_detokenization-strategy.md
│   ├── 003_kafka-vs-fivetran-rationale.md
│   ├── 013_dlp-tokenization-poc.md
│   └── setup-guides/               ← step-by-step GCP infra setup (01–12)
│       ├── 05a-kafka-schema-registry.md   ← CHOSEN: GCP Managed Schema Registry
│       ├── 11a-dataflow-pipeline.md       ← CHOSEN: Kafka → GCS via Managed Kafka Connect
│       ├── scripts/                ← baseline POC scripts (no DLP)
│       └── scripts-dlp/            ← DLP POC scripts (reference; replaced by SDK)
├── terraform/                      ← IaC to reproduce the environment in a new GCP project
│   ├── APPLY-GUIDE.md              ← how to run Terraform end-to-end
│   └── *.tf                        ← VPC, IAM, Kafka, GCS, Pub/Sub, Secrets, VM
└── streamshield/                   ← Python SDK — the production deliverable
    ├── CLAUDE.md                   ← full SDK context (read before any SDK code work)
    ├── pyproject.toml
    ├── streamshield/               ← installable Python package
    ├── examples/
    └── tests/
```

---

## How the Three Folders Relate

1. **`poc/`** — documents every architectural decision and the manual GCP setup that was carried out for the initial proof-of-concept in `vetsource-496203`. Two guides are the canonical choices:
   - **Schema Registry:** `poc/setup-guides/05a-kafka-schema-registry.md` — GCP Managed Schema Registry (native OAuth, no Confluent Cloud account needed)
   - **Stream delivery:** `poc/setup-guides/11a-dataflow-pipeline.md` — Kafka → GCS via Managed Kafka Connect GCS Sink → Snowpipe auto-ingest into Snowflake. No custom pipeline code or Dataflow job.

2. **`terraform/`** — Terraform templates that recreate the full POC environment (VPC, IAM service accounts, Managed Kafka cluster + Schema Registry + Connect cluster, GCE VM, GCS bucket, Pub/Sub topic, Secret Manager shells) in **any new GCP project**. See `terraform/APPLY-GUIDE.md` for full apply instructions. Key variables: `project_id`, `region`, `zone`, `laptop_ip_cidr`. Terraform creates secret containers but not values — fill those in after apply.

3. **`streamshield/`** — the Python SDK that packages everything proven in the POC into an installable library application teams can consume without touching Kafka internals, DLP, or Schema Registry directly. Read `streamshield/CLAUDE.md` in full before making any SDK changes.

---

## SDK Commands

All commands run from `streamshield/`:

```bash
cd streamshield

# Install (dev)
python3 -m venv venv && source venv/bin/activate
pip install -e ".[dev]"

# Unit tests (no GCP needed, ~1s)
python3 -m pytest tests/unit/ -v

# Unit tests with coverage
python3 -m pytest tests/unit/ --cov=streamshield --cov-report=term-missing

# Integration tests (requires GCE VM with vm-producer-sa ADC)
python3 -m pytest tests/integration/ -v

# Lint
ruff check streamshield/

# Type check
mypy streamshield/
```

Run unit tests before every commit. Integration tests require the `vetsource-496203` GCE VM.

---

## Terraform Commands

Run from `terraform/`. Requires Project Owner on the target GCP project; authenticate with `gcloud auth application-default login` first.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in project_id + laptop_ip_cidr
terraform init
terraform plan
terraform apply   # ~20–25 min — Kafka cluster is the slow step
terraform destroy # always run when done — Kafka cluster ~$1.50–2/hr
```

---

## End-to-End Pipeline Architecture

```
GCE VM (poc-dev-vm / StreamShield SDK)
  │  Avro + Confluent wire format, SASL_SSL
  ▼
GCP Managed Kafka  ──  GCP Managed Schema Registry (05a)
  │
  │  Managed Kafka Connect — GCS Sink Connector (AvroConverter → JSON) (11a)
  ▼
GCS  kafka-poc-gcs-landing/
  │  OBJECT_FINALIZE → Pub/Sub → Snowpipe auto-ingest
  ▼
Snowflake  POC_DB.KAFKA_INGEST.ORDER_EVENTS
```

Key design choices locked in:
- **Schema Registry:** GCP Managed (not Confluent Cloud) — single-cloud, OAuth via existing service account, 8 secrets instead of 10
- **Stream delivery:** Managed Kafka Connect GCS Sink (not Dataflow) — no custom code, no job to operate, Snowpipe latency <60 s
- **DLP tokenization:** field-level, batched (100 records/DLP call), policy embedded in Avro schema metadata
- **Offset commit:** always manual, only after successful downstream write (`enable.auto.commit=false`)
