# CLAUDE.md — StreamShield SDK

This file gives Claude Code complete context for working in this repository. Read it in full before making any changes.

---

## What This Project Is

**StreamShield** is a production-grade Python SDK (v0.1.0) for publishing and consuming Kafka messages with Google Cloud DLP tokenization, Avro Schema Registry enforcement, commit-after-process offset management, and Dead Letter Queue routing.

It was built to replace the POC at `../kafka-dlp/` with a clean, installable library that application teams can consume without touching Kafka internals, DLP configuration, or Schema Registry mechanics.

**GCP project:** `vetsource-496203`  
**Kafka topic (primary):** `prescription-events` (3 partitions)  
**Schema Registry subject:** `prescription-events-value`  
**VM service account:** `vm-producer-sa@vetsource-496203.iam.gserviceaccount.com`

---

## Project Status

| Item | State |
|---|---|
| Version | 0.1.0 |
| All phases | Complete (12/12) |
| Unit tests | 50 — all pass |
| Integration tests | 13 — all pass (against vetsource-496203) |
| Verified against real GCP | Yes — producer, tokenized consumer, detokenized consumer all run end-to-end |
| Next milestone | Groovy SDK implementing the same public interface |

---

## Repository Layout

```
streamshield/                         ← this directory (git root)
│
├── CLAUDE.md                         ← this file
├── README.md                         ← user-facing setup and usage guide
├── pyproject.toml                    ← build config, pinned deps, tool config
├── implementation-plan.md            ← full architecture document (19 sections)
├── progress-tracker.md               ← phase-by-phase roadmap and decisions log
│
├── streamshield/                     ← installable Python package
│   ├── __init__.py                   ← all public API re-exports
│   ├── config.py                     ← SDKConfig, GCPConfig, ProducerConfig, ...
│   ├── errors/exceptions.py          ← 18-class typed exception hierarchy
│   ├── auth/gcp.py                   ← GCPAuth: ADC + token refresh + Secret Manager
│   ├── schema/
│   │   ├── models.py                 ← ConsumedMessage, MessageMetadata, SchemaVersion, ...
│   │   ├── registry.py               ← SchemaRegistryClient (internal) + SchemaAdmin (public)
│   │   ├── serializer.py             ← AvroSerializer (Confluent wire format)
│   │   └── deserializer.py           ← AvroDeserializer (reads schema_id from header)
│   ├── dlp/
│   │   ├── policy.py                 ← get_tokenized_fields(), get_reversible_fields(), ...
│   │   ├── tokenizer.py              ← DLPTokenizer: tokenize(), tokenize_batch()
│   │   └── detokenizer.py            ← DLPDetokenizer: detokenize(), detokenize_batch()
│   ├── topic/admin.py                ← TopicAdmin: create, describe, delete, create_dlq_topic
│   ├── producer/producer.py          ← KafkaProducer (sync) + AsyncKafkaProducer
│   ├── consumer/
│   │   ├── consumer.py               ← KafkaConsumer (sync) + AsyncKafkaConsumer
│   │   └── dlq.py                    ← DLQRouter: routes failures to {topic}.dlq
│   └── observability/
│       ├── logging.py                ← named loggers; optional JSON formatter
│       └── metrics.py                ← 11 OTel metrics; no-op if OTel not installed
│
├── examples/
│   ├── schemas/prescription_order.py ← build_prescription_schema() (domain-specific)
│   ├── register_schema.py            ← one-time: register schema with DLP metadata
│   ├── prescription_producer.py      ← produce 5 tokenized orders (replaces POC producer.py)
│   ├── tokenized_consumer.py         ← consume without DLP access (tokens as-is)
│   └── detokenized_consumer.py       ← consume with DLP access (restore plaintext)
│
└── tests/
    ├── unit/                         ← 50 tests — no GCP needed; mocks all I/O
    │   ├── test_config.py            ← SDKConfig validation, YAML/env loading, safe_dict
    │   ├── test_dlp.py               ← policy helpers, tokenizer/detokenizer with mock DLP
    │   └── test_serializer.py        ← Avro round-trip, wire format, error cases
    ├── integration/                  ← 13 tests — requires GCE VM with vm-producer-sa
    │   ├── conftest.py               ← integration_config fixture (vetsource-496203)
    │   ├── test_schema_registry_integration.py
    │   ├── test_producer_integration.py
    │   └── test_consumer_integration.py
    └── fixtures/schemas/prescription_order.json
```

---

## Architecture (5-Layer Model)

```
┌──────────────────────────────────────────────────────────┐
│                   Application Code                        │
│   producer.send(topic, key, value)                        │
│   consumer.process(handler, topics)                       │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│                 Public API Layer                           │
│  KafkaProducer   KafkaConsumer   SchemaAdmin   TopicAdmin │
│  AsyncKafkaProducer   AsyncKafkaConsumer                  │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│               Orchestration Layer                          │
│  SchemaRegistryClient  AvroSerializer   DLPTokenizer      │
│  AvroDeserializer      DLPDetokenizer   DLQRouter         │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│              Infrastructure Layer                          │
│  GCPAuth (ADC + token refresh)  SecretManagerClient       │
│  confluent_kafka.Producer / Consumer / AdminClient        │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│               Configuration Layer                          │
│  SDKConfig   GCPConfig   ProducerConfig   ConsumerConfig  │
│  DLPConfig   DLQConfig   SchemaConfig                     │
└──────────────────────────────────────────────────────────┘
```

**Rule:** Application code only imports from `streamshield` (top-level package). Sub-modules are internal. The producer and consumer must never be instantiated directly from sub-modules by application teams.

---

## Key Design Decisions (Do Not Reverse Without Good Reason)

### 1. `enable.auto.commit` is always `False`
`KafkaConsumer` hardcodes `enable.auto.commit=False` in the confluent_kafka config. It is not a user-configurable parameter. Offsets commit **only after** the application handler returns successfully. This prevents data loss when downstream sinks (Snowflake, BigQuery, etc.) fail mid-write.

`ConsumerConfig` deliberately has no `enable_auto_commit` field.

### 2. DLP batching (100 records per API call)
The POC called DLP once per record. `DLPTokenizer.tokenize_batch()` sends up to `DLPConfig.batch_size` (default: 100) records in a single `deidentify_content` table request. For bulk workloads, this is ~100x cheaper in both latency and API quota usage.

The DLP `table` response rows are matched back to input records by index — result order is guaranteed to match input order.

### 3. Schema-embedded crypto metadata
All tokenization policy (KMS key names, wrapped DEKs, per-field methods, reversibility) lives in the Avro schema registered in the Schema Registry. The `token.*` schema properties are:

| Schema property | Content |
|---|---|
| `token.kms-key` | KMS key resource name — PII domain |
| `token.wrapped-dek` | Base64 KMS-wrapped AES-256 key — PII |
| `token.pci-kms-key` | KMS key resource name — PCI-DSS domain |
| `token.pci-wrapped-dek` | Base64 KMS-wrapped AES-256 key — PCI-DSS |
| `token.surrogate-info-type` | DLP surrogate prefix for PII tokens |
| `token.pci-surrogate-info-type` | DLP surrogate prefix for PCI-DSS tokens |
| `token.context-field` | (optional) overrides `DLPConfig.context_field` per schema |

Per-field: `logicalType="tokenized"`, `token.method`, `token.sensitivity`, `token.reversible`.

`DLPTokenizer` and `DLPDetokenizer` read all of this at runtime — zero field names are hardcoded in the SDK.

### 4. `fastavro.parse_schema()` must be called before read/write
The `token.*` custom properties and `logicalType="tokenized"` are schema metadata, not Avro types. fastavro raises `UnknownType` if you call `schemaless_writer`/`schemaless_reader` on the raw dict. `SchemaRegistryClient._cache_schema()` always calls `fastavro.parse_schema()` and caches both the raw dict and the parsed schema object. Never pass the raw dict directly to fastavro.

### 5. Confluent wire format
Every serialized Kafka message value begins with a 5-byte header:
```
Byte 0:    0x00  (magic byte — identifies Confluent Avro format)
Bytes 1-4: schema_id as 4-byte big-endian unsigned int
Bytes 5+:  Avro-encoded payload (no Avro container file)
```
`AvroSerializer.serialize()` writes this. `AvroDeserializer.deserialize()` reads it. Never write raw Avro without this header — consumers will fail to look up the schema.

### 6. Token refresh in long-running processes (fixes POC bug)
The POC fetched one ADC token at startup. `GCPAuth` tracks expiry and calls `ensure_fresh_token()` before every Schema Registry HTTP call and before every Kafka produce/consume. The token is refreshed proactively when it is within `token_refresh_buffer_s` (default: 300 seconds / 5 minutes) of expiry. The Kafka SASL config is rebuilt via `build_kafka_config()` after each refresh.

### 7. `confluent_kafka.Consumer.subscribe()` does not accept `None` callbacks
Pass `on_assign`/`on_revoke` only when they are non-None. Passing `None` raises `TypeError: on_assign expects a callable`. `KafkaConsumer.subscribe()` uses `**subscribe_kwargs` to conditionally pass these.

### 8. DLQ topic is `{source_topic}.dlq` (configurable via `DLQConfig.topic_suffix`)
DLQ messages are JSON-encoded `DLQRecord` dicts (not Avro — schema may be unavailable when deserialization fails). The DLQ producer is lazy-initialised on the first routing call. After routing, the source offset is committed so the consumer always advances past failed messages.

### 9. HTTPS enforced for Schema Registry
`SDKConfig.validate()` raises `InvalidConfigError` for `http://` schema registry URLs. Schemas carry KMS key material — plaintext transport is not allowed.

### 10. No auto topic creation
Topics are never auto-created by `producer.send()` or `consumer.subscribe()`. They must be pre-created by `TopicAdmin.create_topic()` or Terraform. `TopicAdmin.delete_topic()` requires `confirm=True` — without it, `ValueError` is raised immediately without touching the broker.

---

## GCP Resources

| Resource | Name / Value |
|---|---|
| GCP project | `vetsource-496203` |
| KMS key ring | `vetsource-dlp` (location: `global`) |
| KMS key — PII | `pii-dek-kek` |
| KMS key — PCI-DSS | `pci-dek-kek` |
| Kafka bootstrap | `bootstrap.poc-kafka-cluster.us-central1.managedkafka.vetsource-496203.cloud.goog:9092` |
| Kafka topic | `prescription-events` (3 partitions) |
| DLQ topic | `prescription-events.dlq` |
| Schema Registry subject | `prescription-events-value` (version 1, schema_id 2) |
| VM service account | `vm-producer-sa@vetsource-496203.iam.gserviceaccount.com` |

### Secret Manager secrets (project: vetsource-496203)

| Secret name | Content |
|---|---|
| `kafka-bootstrap-servers` | Kafka broker address |
| `schema-registry-url` | Schema Registry HTTPS URL |
| `dlp-kms-pii-key-name` | Full KMS key resource name — PII |
| `dlp-kms-pci-key-name` | Full KMS key resource name — PCI-DSS |
| `dlp-pii-wrapped-dek` | Base64-encoded KMS-wrapped AES-256 key — PII |
| `dlp-pci-wrapped-dek` | Base64-encoded KMS-wrapped AES-256 key — PCI-DSS |

These secrets are created by `../kafka-dlp/generate_wrapped_dek.py`. **Do not regenerate them unless you are rotating keys** — regenerating requires schema re-registration and re-tokenization of all existing Kafka data.

---

## IAM Requirements

| Service account | Required roles |
|---|---|
| Producer | `roles/dlp.user`, `roles/cloudkms.cryptoKeyEncrypter` (both KMS keys), `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |
| Detokenizing consumer | `roles/dlp.user`, `roles/cloudkms.cryptoKeyDecrypter` (both KMS keys), `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` |
| Tokenized consumer | `roles/secretmanager.secretAccessor`, `roles/managedkafka.client` (no KMS needed) |
| Schema admin | `roles/managedkafka.schemaRegistryEditor`, `roles/secretmanager.secretAccessor` |

In production, split the producer and consumer into separate service accounts (one with Encrypter only, one with Decrypter only).

---

## Setup

```bash
cd /home/tarunkumar_fusionleap_io/kafka-poc/streamshield

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install SDK and dev tools
pip install -e ".[dev]"

# Verify
python3 -c "import streamshield; print('StreamShield', streamshield.__version__)"
```

---

## Running Tests

```bash
source venv/bin/activate

# Unit tests only (no GCP needed — fast, ~1s)
python3 -m pytest tests/unit/ -v

# Integration tests (requires GCE VM with vm-producer-sa ADC)
python3 -m pytest tests/integration/ -v

# Full suite
python3 -m pytest tests/ -v

# With coverage
python3 -m pytest tests/unit/ --cov=streamshield --cov-report=term-missing
```

**Expected results:** 50 unit + 13 integration = 63 total, all pass.

---

## Running the Examples

```bash
source venv/bin/activate

# One-time: register schema (skip if already registered from POC)
python3 examples/register_schema.py

# Produce 5 tokenized prescription orders
python3 examples/prescription_producer.py

# Consume without DLP (see tokens as-is)
python3 examples/tokenized_consumer.py

# Consume with DLP (restore plaintext — requires cryptoKeyDecrypter)
python3 examples/detokenized_consumer.py
```

---

## Public API Quick Reference

### `KafkaProducer` (sync) / `AsyncKafkaProducer` (async)

```python
from streamshield import KafkaProducer, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))

# Sync
with KafkaProducer(config) as p:
    meta = p.send("prescription-events", key="RX-001", value=record)
    results = p.send_batch("prescription-events", records, key_field="order_id")
    p.flush()

# Async
async with AsyncKafkaProducer(config) as p:
    await p.send("prescription-events", key="RX-001", value=record)
```

### `KafkaConsumer` (sync) / `AsyncKafkaConsumer` (async)

```python
from streamshield import KafkaConsumer, ConsumedMessage

def handle(msg: ConsumedMessage) -> None:
    # Offset commits ONLY after this returns without raising
    write_to_snowflake(msg.value)

with KafkaConsumer(config, group_id="my-group") as c:
    c.subscribe(["prescription-events"])
    msg = c.poll(detokenize=True)   # single message
    c.commit(msg)

    # Or managed loop:
    c.process(
        handler=handle,
        topics=["prescription-events"],
        detokenize=True,
        idle_timeout_s=30.0,
    )

# Async with coroutine handler:
async def handle(msg: ConsumedMessage) -> None:
    await db.insert(msg.value)

async with AsyncKafkaConsumer(config, "my-group") as c:
    await c.process(handle, ["prescription-events"], detokenize=True)
```

### `SchemaAdmin`

```python
from streamshield import SchemaAdmin, CompatibilityMode

admin = SchemaAdmin(config)
sv    = admin.register("prescription-events-value", schema_dict, CompatibilityMode.BACKWARD)
sv    = admin.get_latest("prescription-events-value")
defn  = admin.get_by_id(2)
rc    = admin.check_compatibility("prescription-events-value", new_schema)
admin.set_compatibility("prescription-events-value", CompatibilityMode.BACKWARD)
```

### `TopicAdmin`

```python
from streamshield import TopicAdmin

admin = TopicAdmin(config)
admin.create_topic("prescription-events", partitions=3)
admin.create_dlq_topic("prescription-events")   # → prescription-events.dlq
admin.topic_exists("prescription-events")
admin.describe_topic("prescription-events")
admin.delete_topic("prescription-events", confirm=True)  # confirm required
admin.list_topics()
```

### Exception handling

```python
from streamshield import (
    StreamShieldError,      # catch-all
    SchemaValidationError,  # record rejected before any I/O
    TokenizationError,      # DLP deidentify failed
    DetokenizationError,    # DLP reidentify failed
    DeliveryFailedError,    # Kafka broker ack failure
    DLQPublishError,        # DLQ topic also unavailable
)

try:
    with KafkaProducer(config) as p:
        p.send("my-topic", value=record)
except SchemaValidationError as e:
    print(e.safe_context)  # always safe to log — no PII values
except StreamShieldError as e:
    print(type(e).__name__, e)
```

---

## Configuration Reference

```python
from streamshield import (
    SDKConfig, GCPConfig, ProducerConfig, ConsumerConfig,
    DLPConfig, DLQConfig, SchemaConfig, CompatibilityMode
)

config = SDKConfig(
    gcp=GCPConfig(
        project_id="vetsource-496203",   # REQUIRED
        dlp_location="global",
        use_secret_manager=True,
        bootstrap_servers_secret="kafka-bootstrap-servers",
        schema_registry_url_secret="schema-registry-url",
        # Direct values bypass Secret Manager:
        # bootstrap_servers="host:9092",
        # schema_registry_url="https://...",
        token_refresh_buffer_s=300,      # refresh ADC 5min before expiry
    ),
    producer=ProducerConfig(
        enable_idempotence=True,  # always on — prevents duplicates
        acks="all",
        linger_ms=5,
        compression_type="snappy",
    ),
    consumer=ConsumerConfig(
        auto_offset_reset="earliest",
        max_poll_records=500,
        idle_timeout_s=30.0,
        # NOTE: enable.auto.commit is always False — not configurable
    ),
    dlp=DLPConfig(
        enabled=True,
        batch_size=100,          # records per DLP API call (default: 100)
        context_field="order_id",  # crypto context field; schema can override
        max_retries=3,
    ),
    dlq=DLQConfig(
        enabled=True,
        topic_suffix=".dlq",
        raise_on_dlq_failure=True,
        auto_create_topic=True,
    ),
    schema=SchemaConfig(
        auto_register=False,     # require explicit schema registration
        default_compatibility_mode=CompatibilityMode.BACKWARD,
        subject_name_strategy="TopicNameStrategy",  # → "{topic}-value"
    ),
)

# From YAML file:
config = SDKConfig.from_yaml("/etc/streamshield/config.yaml")

# From env vars (prefix: STREAMSHIELD_):
# STREAMSHIELD_GCP_PROJECT_ID=vetsource-496203
config = SDKConfig.from_env()
```

---

## Exception Hierarchy

```
StreamShieldError
├── ConfigurationError
│   ├── MissingConfigError       — required field absent
│   └── InvalidConfigError       — bad value (http:// URL, batch_size=0, etc.)
├── AuthenticationError
│   └── TokenRefreshError        — ADC refresh failed
├── SchemaError
│   ├── SchemaNotFoundError      — subject/schema_id not in registry
│   ├── SchemaRegistrationError  — POST to registry failed
│   ├── SchemaValidationError    — record fails schema (raised before any I/O)
│   └── SchemaCompatibilityError — new schema breaks compatibility mode
├── SerializationError
│   ├── SerializationFailedError     — fastavro write error
│   └── DeserializationFailedError   — fastavro read or bad magic byte
├── DLPError
│   ├── TokenizationError        — deidentifyContent failed
│   └── DetokenizationError      — reidentifyContent failed
├── TopicError
│   ├── TopicNotFoundError
│   └── TopicCreationError
├── ProducerError
│   ├── DeliveryFailedError      — broker ack failure (raised from flush())
│   └── MessageTooLargeError
└── ConsumerError
    ├── OffsetCommitError
    └── DLQPublishError          — DLQ topic also unavailable; message unrecoverable
```

All exceptions carry `.safe_context: dict` — always safe to log (no PII, no key material, no record values).

---

## Key Files for Groovy SDK Reference

When building the Groovy SDK, these Python files are the reference implementations:

| Python file | Groovy equivalent task |
|---|---|
| `auth/gcp.py` | ADC token fetch via Google Auth Java library + Secret Manager client |
| `dlp/tokenizer.py` | `DlpServiceClient.deidentifyContent()` — batch rows in DLP Table |
| `dlp/detokenizer.py` | `DlpServiceClient.reidentifyContent()` — same batch approach |
| `dlp/policy.py` | Avro schema field introspection (`logicalType`, `token.*` properties) |
| `schema/registry.py` | Confluent Schema Registry REST client (Java CachedSchemaRegistryClient or custom) |
| `schema/serializer.py` | Confluent wire format: `0x00` + 4-byte schema_id + Avro bytes |
| `schema/deserializer.py` | Extract schema_id from header; deserialize Avro |
| `producer/producer.py` | KafkaProducer wrapping confluent_kafka → Kafka Producer Java client |
| `consumer/consumer.py` | KafkaConsumer with `enable.auto.commit=false`; manual offset commit |
| `consumer/dlq.py` | DLQ routing: JSON payload; retry on produce failure |

Key Java/Groovy libraries:
- `io.confluent:kafka-avro-serializer` — Confluent Avro SerDe
- `com.google.cloud:google-cloud-dlp` — Cloud DLP Java client
- `com.google.cloud:google-cloud-secretmanager` — Secret Manager Java client
- `org.apache.avro:avro` — Avro schema parsing
- `org.apache.kafka:kafka-clients` — Kafka producer/consumer

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `PermissionDenied` from DLP | Missing `cryptoKeyDecrypter` or `dlp.user` | Add IAM binding to SA |
| `SchemaNotFoundError` on send | Schema not registered | Run `python3 examples/register_schema.py` |
| `TypeError: on_assign expects a callable` | Passing `None` to `consumer.subscribe()` | Fixed in v0.1.0 — pass callbacks only when non-None |
| `InvalidConfigError: HTTPS` | `schema_registry_url` uses `http://` | Use `https://` URL |
| `SASL authentication failed` | ADC token expired (old code) | SDK refreshes automatically; verify ADC is valid |
| `fastavro.write.UnknownType` | Raw schema dict passed to fastavro directly | Always call `fastavro.parse_schema()` first |
| `owner_phone` not de-tokenized | Expected — `CryptoHashConfig` is irreversible | `token.reversible=false` in schema; this is correct |
| Consumer reads wrong record in tests | Topic has old messages; consumer reads from `earliest` | Assert format (not exact value) in integration tests |
| Integration tests need `max_messages` | Tests will run forever without a stop condition | Always pass `max_messages` or `idle_timeout_s` |

---

## Coding Standards for This Repo

- **No `print()` in SDK code.** All output through named loggers (`streamshield.producer`, `streamshield.consumer`, etc.). Examples may use `print()`.
- **Comments explain WHY, not WHAT.** The code is self-documenting via names. Comments exist for non-obvious invariants (e.g., why fastavro needs parse_schema, why the context_col must be in the table headers even though it is not transformed).
- **No hardcoded GCP resource names in the SDK.** `vetsource-496203`, `prescription-events`, etc. belong only in examples and integration tests.
- **Exception messages are always safe to log.** They contain field names, schema IDs, topic names, and error codes — never record values or key material.
- **`SDKConfig.to_safe_dict()` before any config logging.** Never log raw config — it could contain wrapped DEK values.
- **All new public methods need a docstring with Args, Returns, and Raises.**
- **Run `pytest tests/unit/` before committing.** Integration tests require the GCE VM.

---

## What Is NOT in This SDK

These concerns are intentionally out of scope:

- **DEK generation** — `../kafka-dlp/generate_wrapped_dek.py` is the ops runbook. StreamShield does not manage key lifecycle.
- **Schema definition** — `examples/schemas/prescription_order.py` is domain-specific code. The SDK provides infrastructure; schemas are the application's concern.
- **ACL management** — Kafka ACLs are Terraform/ops territory.
- **Protobuf** — Avro only in v1.0. Protobuf is deferred to v1.1.
- **Multi-cloud** — GCP only. AWS MSK and Confluent Cloud are deferred.
- **Async-native Kafka client** — `AsyncKafkaProducer`/`AsyncKafkaConsumer` use `asyncio.to_thread()` over the sync confluent_kafka client. A true async Kafka client (e.g. aiokafka) is deferred.

---

## Related Files

| Path | Description |
|---|---|
| `../kafka-dlp/` | Original POC — do not modify; reference only |
| `../kafka-dlp/generate_wrapped_dek.py` | One-time DEK generation (ops runbook) |
| `../kafka-dlp/CLAUDE.md` | POC context document |
| `streamshield/implementation-plan.md` | Full architecture document — read before major changes |
| `streamshield/progress-tracker.md` | Phase log, decision log, open questions |
