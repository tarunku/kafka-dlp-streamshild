# StreamShield

Production-grade Python SDK for publishing and consuming Kafka messages with automatic GCP DLP tokenization, Avro Schema Registry enforcement, and safe offset management.

**Version:** 0.1.0 | **Python:** 3.11+ | **Platform:** GCP Managed Kafka

---

## What it does

StreamShield wraps Confluent Kafka so your application never touches DLP, Schema Registry, or offset commit logic directly. You call `producer.send()` with plaintext records — the SDK tokenizes sensitive fields, validates the schema, serializes to Avro, and produces to Kafka. On the consumer side, `consumer.process()` deserializes, optionally detokenizes, calls your handler, and commits the offset only after your handler succeeds.

| Concern | Handled automatically |
|---|---|
| Sensitive field tokenization | DLP `deidentifyContent` (batch, up to 100 records/call) |
| Schema validation | Avro schema checked before any I/O |
| Serialization | Confluent wire format (magic byte + schema_id + Avro) |
| Offset management | Commit-after-process — never loses a message on downstream failure |
| Dead Letter Queue | Failed messages routed to `{topic}.dlq`, consumer always advances |
| GCP auth | ADC token refresh 5 minutes before expiry |

---

## Prerequisites

- GCE VM with a service account attached (Application Default Credentials)
- GCP Secret Manager secrets: `kafka-bootstrap-servers`, `schema-registry-url`
- Kafka topic created and schema registered (see [Setup](#setup))
- Python 3.11+

Required IAM roles:

| Operation | Roles |
|---|---|
| Produce | `roles/dlp.user`, `roles/cloudkms.cryptoKeyEncrypter`, `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |
| Consume with detokenization | `roles/dlp.user`, `roles/cloudkms.cryptoKeyDecrypter`, `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |
| Consume without detokenization | `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |

---

## Installation

```bash
python3 -m venv venv
source venv/bin/activate

pip install -e .

# Optional: OpenTelemetry metrics export
pip install -e ".[metrics]"
```

Verify:
```bash
python3 -c "import streamshield; print('StreamShield', streamshield.__version__)"
```

---

## Setup

### 1. Register the schema (one-time)

```bash
python3 examples/register_schema.py
```

This registers the Avro schema under `prescription-events-value` in the Schema Registry. Skip if already done from the POC.

### 2. Create the Kafka topic (one-time)

```python
from streamshield import TopicAdmin, SDKConfig, GCPConfig

admin = TopicAdmin(SDKConfig(gcp=GCPConfig(project_id="vetsource-496203")))
admin.create_topic("prescription-events", partitions=3)
admin.create_dlq_topic("prescription-events")   # creates prescription-events.dlq
```

---

## Quickstart

### Produce a message

```python
from streamshield import KafkaProducer, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))

with KafkaProducer(config) as producer:
    producer.send(
        topic = "prescription-events",
        key   = "RX-001",
        value = {
            "order_id":           "RX-001",
            "owner_name":         "Alice Smith",       # tokenized automatically
            "owner_email":        "alice@example.com", # tokenized automatically
            "owner_phone":        "+1-555-0100",       # hashed (irreversible)
            "owner_payment_card": "4111111111111111",  # FPE tokenized
            "pet_name":           "Biscuit",
            "medication":         "Carprofen 25mg",
            "quantity":           30,
            "order_date":         "2026-06-07",
            "is_refill":          False,
        }
    )
# flush() is called automatically when the context manager exits
```

### Consume messages

```python
from streamshield import KafkaConsumer, ConsumedMessage, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))

def handle(msg: ConsumedMessage) -> None:
    print(msg.value["owner_name"])   # plaintext if detokenize=True
    # offset commits only after this function returns without raising

with KafkaConsumer(config, group_id="my-group") as consumer:
    consumer.process(
        handler        = handle,
        topics         = ["prescription-events"],
        detokenize     = True,    # requires cryptoKeyDecrypter role
        idle_timeout_s = 30.0,
    )
```

### Produce a batch (100x fewer DLP calls)

```python
with KafkaProducer(config) as producer:
    producer.send_batch(
        topic     = "prescription-events",
        records   = [record_1, record_2, ..., record_100],
        key_field = "order_id",
    )
```

### Async producer / consumer

```python
import asyncio
from streamshield import AsyncKafkaProducer, AsyncKafkaConsumer

async def produce():
    async with AsyncKafkaProducer(config) as producer:
        await producer.send("prescription-events", key="RX-001", value=record)

async def consume():
    async def handle(msg: ConsumedMessage) -> None:
        await db.insert(msg.value)

    async with AsyncKafkaConsumer(config, group_id="my-group") as consumer:
        await consumer.process(handle, ["prescription-events"], detokenize=True)
```

---

## Error handling

All exceptions inherit from `StreamShieldError` and carry a `safe_context` dict that is always safe to log (no PII, no key material).

```python
from streamshield import (
    StreamShieldError,
    SchemaValidationError,
    TokenizationError,
    DeliveryFailedError,
)

try:
    with KafkaProducer(config) as producer:
        producer.send("prescription-events", value=record)
except SchemaValidationError as exc:
    # Record rejected before any I/O — safe to log
    logger.error("Schema error: %s context=%s", exc, exc.safe_context)
except TokenizationError as exc:
    # DLP call failed
    logger.error("DLP error: %s context=%s", exc, exc.safe_context)
except StreamShieldError as exc:
    logger.error("SDK error: %s: %s", type(exc).__name__, exc)
```

---

## Configuration

The minimal config only requires `project_id`:

```python
config = SDKConfig(gcp=GCPConfig(project_id="my-project"))
```

Load from a YAML file:

```python
config = SDKConfig.from_yaml("/etc/streamshield/config.yaml")
```

Load from environment variables:

```bash
export STREAMSHIELD_GCP_PROJECT_ID=my-project
export STREAMSHIELD_DLP_BATCH_SIZE=50
```

```python
config = SDKConfig.from_env()
```

See [docs/sdk-reference.md](docs/sdk-reference.md) for the full configuration reference.

---

## Observability

```python
import logging
from streamshield import configure_json_logging, configure_logging_metrics

# Structured JSON logs through Python logging (lands in any attached FileHandler)
configure_json_logging(level=logging.INFO)

# OTel metrics routed through Python logging — appear in log file
configure_logging_metrics(export_interval_ms=10_000)
```

Available metrics: `streamshield_messages_produced_total`, `streamshield_dlp_calls_total`, `streamshield_dlp_call_duration_seconds`, `streamshield_schema_cache_hits_total`, and more. See [docs/sdk-reference.md](docs/sdk-reference.md#observability).

---

## Running the examples

```bash
source venv/bin/activate

python3 examples/prescription_producer.py     # produce 5 tokenized orders
python3 examples/tokenized_consumer.py        # consume — tokens as-is
python3 examples/detokenized_consumer.py      # consume — restore plaintext
```

---

## Running tests

```bash
# Unit tests — no GCP credentials needed (~1 second)
python3 -m pytest tests/unit/ -v

# Integration tests — requires GCE VM with vm-producer-sa
python3 -m pytest tests/integration/ -v

# With coverage
python3 -m pytest tests/unit/ --cov=streamshield --cov-report=term-missing
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `PermissionDenied` from DLP | Missing `cryptoKeyDecrypter` or `dlp.user` | Add IAM binding to service account |
| `SchemaNotFoundError` on send | Schema not registered | Run `python3 examples/register_schema.py` |
| `InvalidConfigError: HTTPS` | `schema_registry_url` uses `http://` | Use `https://` URL |
| `owner_phone` not detokenized | `CryptoHashConfig` is irreversible by design | Correct behavior — `token.reversible=false` in schema |
| Consumer exits immediately | `idle_timeout_s` elapsed with no messages | Increase `idle_timeout_s` or verify topic has messages |
| `SASL authentication failed` | ADC token expired | SDK refreshes automatically — verify ADC is valid with `gcloud auth application-default print-access-token` |

---

For the complete API reference, configuration options, and exception hierarchy see [docs/sdk-reference.md](docs/sdk-reference.md).
