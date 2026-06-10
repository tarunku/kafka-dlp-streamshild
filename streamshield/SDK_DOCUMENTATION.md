# StreamShield Python SDK Documentation

**Version:** 0.1.0  
**GCP Project:** vetsource-496203  
**Python:** 3.11+

---

## Table of Contents

1. [Overview](#1-overview)
2. [Installation](#2-installation)
3. [Quick Start](#3-quick-start)
4. [Configuration](#4-configuration)
5. [KafkaProducer](#5-kafkaproducer)
6. [KafkaConsumer](#6-kafkaconsumer)
7. [SchemaAdmin](#7-schemaadmin)
8. [TopicAdmin](#8-topicadmin)
9. [Data Models](#9-data-models)
10. [Exception Handling](#10-exception-handling)
11. [Dead Letter Queue](#11-dead-letter-queue)
12. [Authentication](#12-authentication)
13. [End-to-End Examples](#13-end-to-end-examples)

---

## 1. Overview

StreamShield is a production-grade Python SDK for publishing and consuming Kafka messages with:

- **Google Cloud DLP tokenization** — sensitive fields are tokenized before reaching the broker and optionally de-tokenized on consumption
- **Avro Schema Registry enforcement** — every message is validated against a registered schema before serialization
- **Commit-after-process offset management** — offsets commit only after the application handler returns successfully, preventing data loss
- **Dead Letter Queue routing** — failed messages are preserved in a DLQ topic instead of being dropped or looping infinitely

Application teams import from `streamshield` only. Sub-module paths are internal and may change.

```python
from streamshield import (
    KafkaProducer, KafkaConsumer,
    SchemaAdmin, TopicAdmin,
    SDKConfig, GCPConfig,
)
```

---

## 2. Installation

```bash
# From source (development)
git clone <repo>
cd streamshield
python3 -m venv venv && source venv/bin/activate
pip install -e ".[dev]"

# Verify
python3 -c "import streamshield; print(streamshield.__version__)"
# 0.1.0
```

**Dependencies installed automatically:**

| Package | Purpose |
|---|---|
| `confluent-kafka` | Kafka producer / consumer / admin client |
| `fastavro` | Avro schema parsing and serialization |
| `google-cloud-dlp` | Cloud DLP tokenization API |
| `google-cloud-secret-manager` | Secret Manager for bootstrap / schema URL |
| `google-cloud-kms` | KMS key operations (via DLP) |
| `google-auth` | Application Default Credentials |
| `requests` | Schema Registry REST calls |

Optional: `pip install streamshield[metrics]` adds OpenTelemetry instrumentation.

---

## 3. Quick Start

### Produce a message

```python
from streamshield import KafkaProducer, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))

with KafkaProducer(config) as producer:
    producer.send(
        topic="prescription-events",
        key="RX-001",
        value={
            "order_id":           "RX-001",
            "owner_name":         "Sarah Mitchell",   # tokenized by DLP
            "owner_email":        "sarah@example.com", # tokenized by DLP
            "owner_payment_card": "4111111111111111",  # tokenized by DLP (PCI-DSS)
            "medication":         "Carprofen 25mg",
            "quantity":           30,
            "order_date":         "2026-06-01",
            "is_refill":          False,
        },
    )
# flush() is called automatically on context manager exit
```

### Consume messages

```python
from streamshield import KafkaConsumer, ConsumedMessage, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))

def handle(msg: ConsumedMessage) -> None:
    print(msg.value)   # offset commits ONLY after this returns

with KafkaConsumer(config, group_id="my-app") as consumer:
    consumer.process(
        handler=handle,
        topics=["prescription-events"],
        detokenize=True,      # restore plaintext via Cloud DLP
        idle_timeout_s=30.0,
    )
```

---

## 4. Configuration

All configuration flows through a single `SDKConfig` object passed to every SDK component. There are no loose constructor parameters.

### 4.1 SDKConfig

```python
from streamshield import SDKConfig, GCPConfig, ProducerConfig, ConsumerConfig
from streamshield import DLPConfig, DLQConfig, SchemaConfig, CompatibilityMode

config = SDKConfig(
    gcp      = GCPConfig(...),
    producer = ProducerConfig(...),
    consumer = ConsumerConfig(...),
    dlp      = DLPConfig(...),
    dlq      = DLQConfig(...),
    schema   = SchemaConfig(...),
)
```

#### Loading from a YAML file

```python
config = SDKConfig.from_yaml("/etc/streamshield/config.yaml")
```

#### Loading from environment variables

```python
config = SDKConfig.from_env()
```

| Environment variable | Config field |
|---|---|
| `STREAMSHIELD_GCP_PROJECT_ID` | `gcp.project_id` |
| `STREAMSHIELD_GCP_USE_SECRET_MANAGER` | `gcp.use_secret_manager` |
| `STREAMSHIELD_GCP_BOOTSTRAP_SERVERS` | `gcp.bootstrap_servers` |
| `STREAMSHIELD_GCP_SCHEMA_REGISTRY_URL` | `gcp.schema_registry_url` |
| `STREAMSHIELD_DLP_ENABLED` | `dlp.enabled` |
| `STREAMSHIELD_DLP_BATCH_SIZE` | `dlp.batch_size` |

#### Safe config logging

```python
import logging
logging.info("Starting with config: %s", config.to_safe_dict())
# Masks any key containing: dek, key, secret, token, wrapped, password
```

---

### 4.2 GCPConfig

```python
GCPConfig(
    project_id="vetsource-496203",          # REQUIRED

    # --- Secret Manager mode (default, for VM / production) ---
    use_secret_manager=True,
    bootstrap_servers_secret="kafka-bootstrap-servers",
    schema_registry_url_secret="schema-registry-url",

    # --- Direct mode (for laptop / local dev) ---
    # use_secret_manager=False,
    # bootstrap_servers="bootstrap.poc-kafka-cluster...cloud.goog:9092",
    # schema_registry_url="https://managedkafka.googleapis.com/...",

    dlp_location="global",                  # Cloud DLP API region
    token_refresh_buffer_s=300,             # refresh ADC token 5 min before expiry
    secrets_refresh_interval_s=None,        # set to enable live secret rotation
)
```

---

### 4.3 ProducerConfig

```python
ProducerConfig(
    enable_idempotence=True,    # prevents duplicate messages on retry — always on
    acks="all",                 # wait for all ISR replicas; "1" = leader only
    retries=5,
    retry_backoff_ms=500,
    linger_ms=5,                # batch wait time in milliseconds
    batch_size_bytes=65536,     # 64 KB per batch
    compression_type="snappy",  # "none" | "gzip" | "snappy" | "lz4" | "zstd"
    request_timeout_ms=30000,
    delivery_timeout_ms=120000,
    validate_topic_on_send=True,
)
```

---

### 4.4 ConsumerConfig

```python
ConsumerConfig(
    auto_offset_reset="earliest",   # "earliest" | "latest"
    max_poll_records=500,
    session_timeout_ms=30000,
    heartbeat_interval_ms=3000,
    max_poll_interval_ms=300000,    # must be >= session_timeout_ms
    idle_timeout_s=30.0,            # process() exits after this many seconds with no messages
)
```

> **Note:** `enable.auto.commit` is hardcoded to `False` and is not exposed in `ConsumerConfig`. The SDK manages commits explicitly.

---

### 4.5 DLPConfig

```python
DLPConfig(
    enabled=True,
    batch_size=100,             # records per DLP API call (max 5000; 100 is safe default)
    context_field="order_id",  # CryptoDeterministicConfig context; overridable per schema
    max_retries=3,
    retry_backoff_ms=500,
)
```

---

### 4.6 DLQConfig

```python
DLQConfig(
    enabled=True,
    topic_suffix=".dlq",        # DLQ topic = source topic + suffix
    max_retries=3,
    raise_on_dlq_failure=True,  # raise DLQPublishError if DLQ is also unavailable
    auto_create_topic=True,     # create the DLQ topic on first routing call
)
```

---

### 4.7 SchemaConfig

```python
SchemaConfig(
    serialization_format=SerializationFormat.AVRO,
    auto_register=False,                                    # never register schemas automatically in production
    default_compatibility_mode=CompatibilityMode.BACKWARD,
    subject_name_strategy="TopicNameStrategy",              # "{topic}-value"
    cache_capacity=1000,
)
```

#### CompatibilityMode values

| Value | Meaning |
|---|---|
| `BACKWARD` | New schema can read data written by the previous schema |
| `FORWARD` | Previous schema can read data written by the new schema |
| `FULL` | Both BACKWARD and FORWARD |
| `BACKWARD_TRANSITIVE` | BACKWARD against all historical versions |
| `FORWARD_TRANSITIVE` | FORWARD against all historical versions |
| `FULL_TRANSITIVE` | FULL against all historical versions |
| `NONE` | No checks (use with caution) |

---

## 5. KafkaProducer

### 5.1 Sync — `KafkaProducer`

```python
from streamshield import KafkaProducer

with KafkaProducer(config) as producer:
    ...
```

The context manager calls `flush()` automatically on exit. Use `close()` explicitly if not using a context manager.

#### send()

```python
meta = producer.send(
    topic          = "prescription-events",
    value          = record_dict,          # plaintext; sensitive fields tokenized automatically
    key            = "RX-001",             # optional Kafka message key
    schema_version = None,                 # None = latest; int = pin to specific version
    headers        = {"source": "api"},    # optional Kafka headers
    on_delivery    = my_callback,          # optional callback(err, msg)
)
# Returns MessageMetadata(topic, partition=-1, offset=-1)
# Partition and offset are populated after flush()
```

**Send pipeline:**
1. Fetch schema from Schema Registry (cached after first fetch)
2. Validate record against Avro schema (pre-flight, no I/O — fails fast)
3. Tokenize sensitive fields via Cloud DLP
4. Serialize to Confluent Avro wire format (`0x00` + 4-byte `schema_id` + Avro bytes)
5. Produce to Kafka (non-blocking)

#### send_batch()

```python
records = [record1, record2, ..., record100]

results = producer.send_batch(
    topic        = "prescription-events",
    records      = records,
    key_field    = "order_id",    # field name to use as Kafka key; None = no key
    schema_version = None,
)
# Returns List[MessageMetadata]
```

`send_batch()` calls Cloud DLP **once** for the entire list (up to `DLPConfig.batch_size` records per API call). This is ~100× cheaper than calling `send()` in a loop for bulk workloads.

#### flush()

```python
producer.flush(timeout=30.0)
# Blocks until all in-flight messages are acknowledged
# Raises DeliveryFailedError if any delivery failed
```

#### close()

```python
producer.close()   # flush() + close; idempotent
```

---

### 5.2 Async — `AsyncKafkaProducer`

```python
from streamshield import AsyncKafkaProducer

async with AsyncKafkaProducer(config) as producer:
    await producer.send("prescription-events", value=record)
    await producer.send_batch("prescription-events", records, key_field="order_id")
    await producer.flush()
```

All methods delegate to the sync producer via `asyncio.to_thread()`. To avoid blocking the event loop during initialisation:

```python
producer = await AsyncKafkaProducer.create(config)
```

---

## 6. KafkaConsumer

### 6.1 Sync — `KafkaConsumer`

```python
from streamshield import KafkaConsumer

with KafkaConsumer(config, group_id="my-group") as consumer:
    ...
```

#### subscribe()

```python
consumer.subscribe(
    topics    = ["prescription-events"],
    on_assign = lambda c, partitions: print("Assigned", partitions),  # optional
    on_revoke = lambda c, partitions: print("Revoking", partitions),  # optional
)
```

#### poll()

```python
msg = consumer.poll(timeout=1.0, detokenize=False)
# Returns ConsumedMessage or None
# Caller must call commit() after processing
if msg:
    process(msg.value)
    consumer.commit(msg)
```

#### commit()

```python
consumer.commit(message=msg, asynchronous=False)
# asynchronous=True commits in the background (no error feedback)
```

#### process() — managed poll loop

```python
def handle(msg: ConsumedMessage) -> None:
    write_to_snowflake(msg.value)
    # Offset commits ONLY after this returns without raising

consumer.process(
    handler       = handle,
    topics        = ["prescription-events"],
    detokenize    = True,         # call DLP reidentifyContent before delivering to handler
    max_messages  = 1000,         # stop after N messages; None = run forever
    idle_timeout_s = 30.0,        # stop after N seconds with no messages
)
```

**process() offset contract:**

| Event | Offset committed? |
|---|---|
| Handler returns normally | Yes — after handler returns |
| Handler raises any exception | Yes — after routing to DLQ |
| Deserialization fails | Yes — after routing to DLQ |
| DLP detokenization fails | Yes — after routing to DLQ |
| DLQ itself fails (raise_on_dlq_failure=True) | No — `DLQPublishError` raised |

---

### 6.2 Async — `AsyncKafkaConsumer`

```python
from streamshield import AsyncKafkaConsumer

async def handle(msg: ConsumedMessage) -> None:
    await db.insert(msg.value)

async with AsyncKafkaConsumer(config, group_id="my-group") as consumer:
    await consumer.process(
        handler        = handle,     # can be a coroutine function
        topics         = ["prescription-events"],
        detokenize     = True,
        idle_timeout_s = 30.0,
    )
```

If `handler` is a coroutine function, it is `await`-ed directly. Otherwise it runs in a thread pool.

---

## 7. SchemaAdmin

Used by infrastructure / ops teams to register and manage schemas. Application code does not call `SchemaAdmin` during normal produce/consume operations.

```python
from streamshield import SchemaAdmin, SDKConfig, GCPConfig, CompatibilityMode

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))
admin  = SchemaAdmin(config)
```

#### register()

```python
from streamshield import SchemaAdmin, CompatibilityMode

sv = admin.register(
    subject           = "prescription-events-value",
    schema_definition = schema_dict,              # Avro schema as dict or JSON string
    compatibility_mode = CompatibilityMode.BACKWARD,  # applied before registration
)
# Returns SchemaVersion(schema_id, subject, version, schema)
print(f"Registered schema_id={sv.schema_id} version={sv.version}")
```

#### get_latest()

```python
sv = admin.get_latest("prescription-events-value")
# Returns SchemaVersion
```

#### get_by_id()

```python
defn = admin.get_by_id(2)
# Returns SchemaDefinition(schema_id, schema, schema_type)
```

#### check_compatibility()

```python
result = admin.check_compatibility("prescription-events-value", new_schema_dict)
# Returns CompatibilityResult(is_compatible, messages)
if not result.is_compatible:
    print(result.messages)
```

#### set_compatibility()

```python
admin.set_compatibility("prescription-events-value", CompatibilityMode.FULL)
```

#### list_subjects() / list_versions()

```python
subjects = admin.list_subjects()          # List[str]
versions = admin.list_versions("prescription-events-value")  # List[int]
```

#### delete_version()

```python
admin.delete_version("prescription-events-value", version="latest")
```

---

## 8. TopicAdmin

```python
from streamshield import TopicAdmin

admin = TopicAdmin(config)
```

#### create_topic()

```python
result = admin.create_topic(
    name               = "prescription-events",
    partitions         = 3,
    replication_factor = 3,
    config             = {"retention.ms": "604800000"},  # optional
)
# Returns TopicCreationResult(name, partitions, replication_factor, created)
# created=False if the topic already existed (idempotent — does not raise)
```

#### create_dlq_topic()

```python
result = admin.create_dlq_topic("prescription-events")
# Creates "prescription-events.dlq" with the same partition count
# DLQ topic has retention.ms=604800000 (7 days) set automatically
```

#### topic_exists()

```python
admin.topic_exists("prescription-events")   # bool
```

#### describe_topic()

```python
meta = admin.describe_topic("prescription-events")
# Returns TopicMetadata(name, partitions, replication_factor, config)
print(meta.partitions)  # 3
```

#### delete_topic()

```python
admin.delete_topic("prescription-events", confirm=True)
# confirm=True is required — raises ValueError without it
# Raises TopicNotFoundError if the topic does not exist
```

#### list_topics()

```python
topics = admin.list_topics()   # List[str], excludes internal __ topics
```

---

## 9. Data Models

All models are plain dataclasses importable from `streamshield`.

### ConsumedMessage

Delivered to the handler in `consumer.process()` and returned by `consumer.poll()`.

```python
@dataclass
class ConsumedMessage:
    topic:      str
    partition:  int
    offset:     int
    timestamp:  int           # epoch milliseconds
    key:        bytes | None
    value:      dict          # deserialized (and optionally de-tokenized) record
    raw_schema: dict          # full Avro schema dict — includes all token.* metadata
    schema_id:  int           # schema ID from the Confluent wire-format header
    headers:    dict[str, bytes]
```

#### Inspecting tokenized fields

```python
from streamshield.dlp.policy import get_tokenized_fields, get_reversible_fields

tokenized = get_tokenized_fields(msg.raw_schema)
# Returns list of field dicts that have logicalType="tokenized"

reversible = get_reversible_fields(msg.raw_schema)
# Returns only the fields where token.reversible != "false"
```

### MessageMetadata

Returned by `producer.send()`.

```python
@dataclass
class MessageMetadata:
    topic:     str
    partition: int   # -1 before flush()
    offset:    int   # -1 before flush()
    timestamp: int
    key:       bytes | None
```

### SchemaVersion

Returned by `SchemaAdmin.register()`, `get_latest()`, `get_version()`.

```python
@dataclass
class SchemaVersion:
    schema_id: int
    subject:   str
    version:   int
    schema:    dict   # raw Avro schema dict
```

### SchemaDefinition

Returned by `SchemaAdmin.get_by_id()`.

```python
@dataclass
class SchemaDefinition:
    schema_id:   int
    schema:      dict
    schema_type: str   # "AVRO"
```

### CompatibilityResult

Returned by `SchemaAdmin.check_compatibility()`.

```python
@dataclass
class CompatibilityResult:
    is_compatible: bool
    messages:      list[str]   # human-readable reasons if incompatible
```

### TopicMetadata

Returned by `TopicAdmin.describe_topic()`.

```python
@dataclass
class TopicMetadata:
    name:               str
    partitions:         int
    replication_factor: int
    config:             dict[str, str]
```

### TopicCreationResult

Returned by `TopicAdmin.create_topic()` and `create_dlq_topic()`.

```python
@dataclass
class TopicCreationResult:
    name:               str
    partitions:         int
    replication_factor: int
    created:            bool   # False if the topic already existed
```

---

## 10. Exception Handling

All SDK exceptions inherit from `StreamShieldError`. Every exception carries a `.safe_context` dict that is always safe to log — it never contains record values, PII, or key material.

### Exception hierarchy

```
StreamShieldError
├── ConfigurationError
│   ├── MissingConfigError        — required field absent (e.g. project_id)
│   └── InvalidConfigError        — bad value (e.g. http:// URL, batch_size=0)
├── AuthenticationError
│   └── TokenRefreshError         — ADC OAuth2 refresh failed
├── SchemaError
│   ├── SchemaNotFoundError       — no schema for subject or schema_id
│   ├── SchemaRegistrationError   — POST to Schema Registry failed
│   ├── SchemaValidationError     — record fails Avro schema (before any I/O)
│   └── SchemaCompatibilityError  — new schema breaks compatibility mode
│       └── .messages             — list of human-readable reasons
├── SerializationError
│   ├── SerializationFailedError  — fastavro write error
│   └── DeserializationFailedError — fastavro read or bad magic byte
├── DLPError
│   ├── TokenizationError         — deidentifyContent failed
│   └── DetokenizationError       — reidentifyContent failed
├── TopicError
│   ├── TopicNotFoundError
│   └── TopicCreationError
├── ProducerError
│   ├── DeliveryFailedError       — broker ack failure (raised from flush())
│   └── MessageTooLargeError
└── ConsumerError
    ├── OffsetCommitError
    └── DLQPublishError           — DLQ topic also unavailable; message unrecoverable
```

### Handling exceptions

```python
from streamshield import (
    StreamShieldError,
    SchemaValidationError,
    TokenizationError,
    DeliveryFailedError,
    DLQPublishError,
)

try:
    with KafkaProducer(config) as producer:
        producer.send("prescription-events", value=record)
except SchemaValidationError as exc:
    # Record was rejected before any I/O — safe to log full context
    logger.error("Schema validation failed: %s context=%s", exc, exc.safe_context)
except TokenizationError as exc:
    logger.error("DLP tokenization failed: %s context=%s", exc, exc.safe_context)
except DeliveryFailedError as exc:
    logger.error("Kafka delivery failed: %s context=%s", exc, exc.safe_context)
except StreamShieldError as exc:
    # Catch-all for any SDK error
    logger.error("StreamShield error [%s]: %s", type(exc).__name__, exc.safe_context)
```

### safe_context examples

```python
SchemaValidationError.safe_context  # {"subject": "events-value", "schema_id": 2}
TokenizationError.safe_context      # {"topic": "events", "schema_id": 2, "operation": "tokenize"}
DeliveryFailedError.safe_context    # {"failure_count": 1, "first_error": "..."}
DLQPublishError.safe_context        # {"source_topic": "events", "source_offset": 42, "dlq_topic": "events.dlq"}
```

---

## 11. Dead Letter Queue

When any of the following occur in `consumer.process()`, the message is routed to the DLQ topic instead of crashing the consumer:

| Failure type | Reason code | What caused it |
|---|---|---|
| `DeserializationFailedError` | `deserialization` | Bad magic byte, corrupt Avro bytes |
| `DetokenizationError` | `dlp` | Cloud DLP `reidentifyContent` failed |
| Any `Exception` from handler | `business` | Application-level failure |

### DLQ message format

DLQ messages are JSON (not Avro — the schema may be unavailable when deserialization fails).

```json
{
  "source_topic":     "prescription-events",
  "source_partition": 0,
  "source_offset":    42,
  "source_timestamp": 1748795123000,
  "source_key":       "RX-001",
  "failure_reason":   "business",
  "error_type":       "ValueError",
  "error_message":    "downstream database unavailable",
  "routed_at":        1748795124000,
  "streamshield_version": "0.1.0"
}
```

### DLQ topic naming

```
source_topic + DLQConfig.topic_suffix
"prescription-events" + ".dlq"  →  "prescription-events.dlq"
```

### Consuming the DLQ

```python
def handle_dlq(msg: ConsumedMessage) -> None:
    import json
    record = json.loads(msg.value)   # DLQ messages are raw JSON dicts
    print(f"Failed message from {record['source_topic']} "
          f"offset {record['source_offset']}: {record['error_message']}")

with KafkaConsumer(config, group_id="dlq-inspector") as consumer:
    consumer.process(
        handler        = handle_dlq,
        topics         = ["prescription-events.dlq"],
        detokenize     = False,    # DLQ messages are already raw bytes — no schema
        idle_timeout_s = 10.0,
    )
```

---

## 12. Authentication

The SDK uses **Application Default Credentials (ADC)**. No credentials are hardcoded.

### How ADC is resolved (in order)

1. `GOOGLE_APPLICATION_CREDENTIALS` env var pointing to a service account key file
2. Service account attached to the GCE VM (used in production)
3. `gcloud auth application-default login` (used on a laptop)

### Token refresh

`GCPAuth` tracks token expiry and refreshes the token proactively when it is within `token_refresh_buffer_s` (default: 5 minutes) of expiry. This is called automatically before every Schema Registry HTTP request and before every Kafka produce/consume call.

This fixes a known POC bug where long-running producers would hit `SASL authentication failed` after ~1 hour.

### Setting up ADC on a laptop

```bash
# Option A — your Google account (requires IAM roles on the project)
gcloud auth application-default login

# Option B — service account key file
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

### Required IAM roles

| Component | Required roles |
|---|---|
| `KafkaProducer` | `roles/dlp.user`, `roles/cloudkms.cryptoKeyEncrypter`, `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |
| `KafkaConsumer` (detokenize=True) | `roles/dlp.user`, `roles/cloudkms.cryptoKeyDecrypter`, `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |
| `KafkaConsumer` (detokenize=False) | `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |
| `SchemaAdmin` | `roles/managedkafka.schemaRegistryEditor`, `roles/secretmanager.secretAccessor` |
| `TopicAdmin` | `roles/managedkafka.admin`, `roles/secretmanager.secretAccessor` |

---

## 13. End-to-End Examples

### Register schema (one-time setup)

```python
from streamshield import SchemaAdmin, SDKConfig, GCPConfig, CompatibilityMode
from streamshield.auth.gcp import GCPAuth

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203", use_secret_manager=True))
auth   = GCPAuth(project_id="vetsource-496203")

# Load KMS keys and wrapped DEKs from Secret Manager
pii_kms_key     = auth.get_secret("dlp-kms-pii-key-name")
pci_kms_key     = auth.get_secret("dlp-kms-pci-key-name")
pii_wrapped_dek = auth.get_secret("dlp-pii-wrapped-dek")
pci_wrapped_dek = auth.get_secret("dlp-pci-wrapped-dek")

# Build schema with embedded DLP metadata (see examples/schemas/prescription_order.py)
schema = build_prescription_schema(pii_kms_key, pii_wrapped_dek, pci_kms_key, pci_wrapped_dek)

admin = SchemaAdmin(config)
sv = admin.register(
    subject            = "prescription-events-value",
    schema_definition  = schema,
    compatibility_mode = CompatibilityMode.BACKWARD,
)
print(f"Registered schema_id={sv.schema_id} version={sv.version}")
```

### Produce tokenized messages

```python
import time, uuid, random
from streamshield import KafkaProducer, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203", use_secret_manager=True))

records = [
    {
        "order_id":           f"RX-{uuid.uuid4().hex[:8].upper()}",
        "owner_name":         "Sarah Mitchell",
        "owner_email":        "sarah@example.com",
        "owner_phone":        "+1-555-0142",
        "owner_payment_card": "4111111111111111",
        "pet_name":           "Biscuit",
        "medication":         "Carprofen 25mg",
        "quantity":           30,
        "order_date":         time.strftime("%Y-%m-%d"),
        "is_refill":          False,
    }
    for _ in range(5)
]

with KafkaProducer(config) as producer:
    results = producer.send_batch(
        topic     = "prescription-events",
        records   = records,
        key_field = "order_id",
    )
print(f"Queued {len(results)} messages")
```

### Consume without DLP (tokens as-is)

```python
from streamshield import KafkaConsumer, ConsumedMessage, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203", use_secret_manager=True))

def handle(msg: ConsumedMessage) -> None:
    for field, value in msg.value.items():
        print(f"  {field}: {value}")

with KafkaConsumer(config, group_id="tokenized-reader") as consumer:
    consumer.process(
        handler        = handle,
        topics         = ["prescription-events"],
        detokenize     = False,    # no DLP — tokens shown as-is
        idle_timeout_s = 30.0,
    )
```

### Consume with DLP (restore plaintext)

```python
from streamshield import KafkaConsumer, ConsumedMessage, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203", use_secret_manager=True))

def handle(msg: ConsumedMessage) -> None:
    # Reversible tokens have been replaced with plaintext
    # Irreversible fields (e.g. owner_phone hashed with CryptoHashConfig) remain as hashes
    print(msg.value["owner_name"])   # "Sarah Mitchell" (restored)
    print(msg.value["owner_phone"])  # "VETSOURCE_PII_TOKEN(...)" (irreversible)

with KafkaConsumer(config, group_id="detokenized-reader") as consumer:
    consumer.process(
        handler        = handle,
        topics         = ["prescription-events"],
        detokenize     = True,     # calls DLP reidentifyContent
        idle_timeout_s = 30.0,
    )
```

### Async producer and consumer

```python
import asyncio
from streamshield import AsyncKafkaProducer, AsyncKafkaConsumer, ConsumedMessage
from streamshield import SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203", use_secret_manager=True))

async def produce():
    async with AsyncKafkaProducer(config) as producer:
        await producer.send("prescription-events", key="RX-001", value=record)

async def consume():
    async def handle(msg: ConsumedMessage) -> None:
        await asyncio.sleep(0)   # any async work here
        print(msg.value)

    async with AsyncKafkaConsumer(config, group_id="async-reader") as consumer:
        await consumer.process(handle, ["prescription-events"], detokenize=True)

asyncio.run(produce())
asyncio.run(consume())
```

### Topic and schema setup (ops script)

```python
from streamshield import TopicAdmin, SchemaAdmin, SDKConfig, GCPConfig, CompatibilityMode

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203", use_secret_manager=True))

# Create topics
topic_admin = TopicAdmin(config)
topic_admin.create_topic("prescription-events", partitions=3)
topic_admin.create_dlq_topic("prescription-events")     # → prescription-events.dlq

print(topic_admin.list_topics())
print(topic_admin.describe_topic("prescription-events"))

# Register schema
schema_admin = SchemaAdmin(config)
schema_admin.set_compatibility("prescription-events-value", CompatibilityMode.BACKWARD)
sv = schema_admin.register("prescription-events-value", schema_dict)
print(f"Schema registered: id={sv.schema_id} version={sv.version}")
```

---

*StreamShield SDK v0.1.0 — GCP project: vetsource-496203*
