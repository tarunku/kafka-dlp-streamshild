# StreamShield SDK

Production-grade Python SDK for publishing and consuming Kafka messages with GCP DLP tokenization, Avro Schema Registry enforcement, commit-after-process offset management, and Dead Letter Queue routing.

**Version:** 0.1.0 | **Python:** 3.11+ | **Platform:** GCP Managed Kafka

---

## What StreamShield Does

StreamShield hides Kafka complexity behind a clean API. Application teams call `producer.send()` or `consumer.process()` — the SDK handles everything else:

| Concern | What the SDK does automatically |
|---|---|
| Authentication | ADC token rotation via GCPAuth — no static credentials |
| DLP Tokenization | Tokenizes sensitive fields before produce; de-tokenizes on consume |
| Schema enforcement | Validates records against Avro schema before any I/O |
| Serialization | Confluent wire format (magic byte + schema_id + Avro) |
| Offset management | Commits offset ONLY after handler succeeds |
| Dead Letter Queue | Routes failures to `{topic}.dlq` automatically |
| Batching | Up to 100 records per DLP API call (100x fewer API calls than POC) |

---

## Architecture

```
Application Code
       │
┌──────▼─────────────────────────────────────┐
│          Public API Layer                   │
│  KafkaProducer    KafkaConsumer             │
│  SchemaAdmin      TopicAdmin                │
└──────┬──────────────────────────────────────┘
       │
┌──────▼─────────────────────────────────────┐
│        Orchestration Layer                  │
│  SchemaRegistryClient  DLPTokenizer         │
│  AvroSerializer        DLPDetokenizer       │
│  AvroDeserializer      DLQRouter            │
└──────┬──────────────────────────────────────┘
       │
┌──────▼─────────────────────────────────────┐
│        Infrastructure Layer                 │
│  GCPAuth (ADC + token refresh)              │
│  SecretManagerClient                        │
│  confluent_kafka (Producer / Consumer)      │
└─────────────────────────────────────────────┘
```

**Data flow (producer):**
```
Plaintext record
    → validate against Avro schema
    → DLP deidentifyContent (batch tokenization)
    → Avro serialize (Confluent wire format)
    → Kafka broker
```

**Data flow (consumer):**
```
Kafka broker
    → Avro deserialize (extract schema_id from header)
    → DLP reidentifyContent (if detokenize=True)
    → application handler(ConsumedMessage)
    → commit offset (only after handler succeeds)
```

---

## Project Structure

```
streamshield/
├── streamshield/           ← installable Python package
│   ├── config.py           ← SDKConfig — single config object
│   ├── errors/             ← typed exception hierarchy
│   ├── auth/               ← GCPAuth — ADC + Secret Manager
│   ├── schema/             ← Schema Registry client, Avro serializer/deserializer
│   ├── dlp/                ← DLP tokenizer, detokenizer, policy helpers
│   ├── topic/              ← TopicAdmin — create/manage Kafka topics
│   ├── producer/           ← KafkaProducer + AsyncKafkaProducer
│   ├── consumer/           ← KafkaConsumer + AsyncKafkaConsumer + DLQRouter
│   └── observability/      ← structured logging + OTel metrics
├── examples/               ← ready-to-run scripts (Vetsource prescription domain)
│   ├── schemas/prescription_order.py
│   ├── register_schema.py
│   ├── prescription_producer.py
│   ├── tokenized_consumer.py
│   └── detokenized_consumer.py
└── tests/
    ├── unit/               ← no GCP credentials needed
    └── integration/        ← runs against vetsource-496203
```

---

## Environment Setup

### 1. GCP VM and Service Account

All scripts are designed to run on a GCE VM with the `vm-producer-sa` service account attached. Application Default Credentials (ADC) are used — no static credential files.

```bash
# Verify ADC is working on the VM
gcloud auth application-default print-access-token
```

The `vm-producer-sa` requires these IAM roles:

| Operation | Required Role |
|---|---|
| Produce (tokenize) | `roles/dlp.user`, `roles/cloudkms.cryptoKeyEncrypter` |
| Consume (de-tokenize) | `roles/dlp.user`, `roles/cloudkms.cryptoKeyDecrypter` |
| Schema Registry | `roles/managedkafka.schemaRegistryEditor` |
| Secret Manager | `roles/secretmanager.secretAccessor` |

For production, split into a producer SA (encrypter only) and a consumer SA (decrypter only).

### 2. Python Environment

```bash
cd /home/tarunkumar_fusionleap_io/kafka-poc/streamshield

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install StreamShield and all dependencies
pip install -e .
pip install -e ".[metrics]"
pip install -e ".[streamlit]"
# Verify imports
python3 -c "import streamshield; print(f'StreamShield {streamshield.__version__} OK')"
python3 -c "from google.cloud import dlp_v2, kms, secretmanager; print('GCP SDK OK')"
python3 -c "import confluent_kafka, fastavro; print('Kafka + Avro OK')"
```

### 3. GCP Secret Manager Secrets

The following secrets must exist in the `vetsource-496203` project:

| Secret Name | Content |
|---|---|
| `kafka-bootstrap-servers` | Kafka bootstrap address |
| `schema-registry-url` | Schema Registry HTTPS URL |
| `dlp-kms-pii-key-name` | KMS key resource name — PII domain |
| `dlp-kms-pci-key-name` | KMS key resource name — PCI-DSS domain |
| `dlp-pii-wrapped-dek` | Base64-encoded KMS-wrapped AES-256 key (PII) |
| `dlp-pci-wrapped-dek` | Base64-encoded KMS-wrapped AES-256 key (PCI-DSS) |

These were created by `kafka-dlp/generate_wrapped_dek.py` in the POC. If they already exist, no regeneration is needed.

### 4. Register the Schema (one-time)

The schema must be registered before the producer can run. If the POC's `register_schema.py` has already registered it, skip this step.

```bash
# Register the PrescriptionOrder schema with embedded DLP metadata
python3 examples/register_schema.py
```

Expected output:
```
Loading KMS keys and wrapped DEKs from Secret Manager...
  PII KMS key: projects/vetsource-496203/...
Registering schema under subject 'prescription-events-value'...
Schema registered — ID: 3, version: 1
```

---

## Running the Examples

### Producer — Tokenize and publish 5 orders

```bash
python3 examples/prescription_producer.py
streamlit run examples/streamlit_producer.py   # port 8501
```

What happens:
1. Loads bootstrap servers and schema registry URL from Secret Manager.
2. Fetches the registered `PrescriptionOrder` schema from the Schema Registry.
3. For each order: validates → DLP tokenizes sensitive fields → Avro serializes → publishes to Kafka.
4. Flushes and waits for all broker acknowledgments before exiting.

Sample output:
```
Producing 5 prescription orders to 'prescription-events'...
────────────────────────────────────────────────────────────
Message 1: RX-3F7A1C2E
  Plaintext  owner_name  : Sarah Mitchell
  Plaintext  owner_email : sarah@example.com
  Plaintext  card        : 4111111111111111
  Queued: topic=prescription-events

All 5 messages delivered.
```

### Tokenized Consumer — Read tokens without DLP access

```bash
python3 examples/tokenized_consumer.py
streamlit run examples/streamlit_consumer.py   # port 8502
```

Simulates a downstream subscriber without Cloud KMS access. Sensitive fields appear as opaque tokens.

```
🔒  owner_name             : VETSOURCE_PII_TOKEN(14):aB3xKp…  [reversible token]
🔐  owner_phone            : VETSOURCE_PII_TOKEN(12):kJ9sUe…  [irreversible hash]
🔒  owner_payment_card     : 5412753489210033  [FPE token — format preserved]
```

### Detokenized Consumer — Restore plaintext via DLP

```bash
python3 examples/detokenized_consumer.py
```

Requires `roles/cloudkms.cryptoKeyDecrypter` on both KMS domain keys. Calls DLP `reidentifyContent` to reverse tokens.

```
  owner_name              Sarah Mitchell                    [de-tokenized — PII]
  owner_email             sarah@example.com                 [de-tokenized — PII]
  owner_phone             VETSOURCE_PII_TOKEN(12):kJ9s...   [hash — irreversible]
  owner_payment_card      4111111111111111                  [de-tokenized — PCI-DSS]
```

---

## SDK Usage

### Producer (sync)

```python
from streamshield import KafkaProducer, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))

with KafkaProducer(config) as producer:
    producer.send(
        topic = "prescription-events",
        key   = "RX-001",
        value = {
            "order_id":           "RX-001",
            "owner_name":         "Alice Smith",    # ← tokenized automatically
            "owner_payment_card": "4111111111111111",  # ← FPE tokenized
            "medication":         "Carprofen 25mg",
            "quantity":           30,
            "order_date":         "2026-06-01",
            "is_refill":          False,
            "owner_email":        "alice@example.com",
            "owner_phone":        "+1-555-0100",
            "pet_name":           "Biscuit",
        }
    )
    # flush() called automatically on context manager exit
```

### Batch producer (100x fewer DLP calls)

```python
records = [make_prescription_order() for _ in range(100)]

with KafkaProducer(config) as producer:
    producer.send_batch(
        topic     = "prescription-events",
        records   = records,
        key_field = "order_id",   # field to use as Kafka message key
    )
```

### Consumer (sync)

```python
from streamshield import KafkaConsumer, ConsumedMessage

def handle(msg: ConsumedMessage) -> None:
    # msg.value contains the record — detokenized if detokenize=True
    # Offset commits ONLY after this function returns without raising
    write_to_snowflake(msg.value)

with KafkaConsumer(config, group_id="snowflake-loader") as consumer:
    consumer.process(
        handler      = handle,
        topics       = ["prescription-events"],
        detokenize   = True,    # calls DLP reidentifyContent
        idle_timeout_s = 60.0,
    )
```

### Async producer

```python
import asyncio
from streamshield import AsyncKafkaProducer, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="vetsource-496203"))

async def main():
    async with AsyncKafkaProducer(config) as producer:
        await producer.send("prescription-events", key="RX-001", value=record)

asyncio.run(main())
```

### Async consumer

```python
from streamshield import AsyncKafkaConsumer, ConsumedMessage

async def handle(msg: ConsumedMessage) -> None:
    await db.insert(msg.value)

async def main():
    async with AsyncKafkaConsumer(config, group_id="async-loader") as consumer:
        await consumer.process(
            handler    = handle,
            topics     = ["prescription-events"],
            detokenize = True,
        )

asyncio.run(main())
```

### Schema Admin

```python
from streamshield import SchemaAdmin, CompatibilityMode

admin = SchemaAdmin(config)

# Register a schema
sv = admin.register(
    subject           = "prescription-events-value",
    schema_definition = my_schema_dict,
    compatibility_mode = CompatibilityMode.BACKWARD,
)
print(f"Registered schema ID: {sv.schema_id}")

# Check compatibility before registering
result = admin.check_compatibility("prescription-events-value", new_schema)
if not result.is_compatible:
    print("Incompatible:", result.messages)
```

### Topic Admin

```python
from streamshield import TopicAdmin

admin = TopicAdmin(config)

# Create topic and its DLQ
admin.create_topic("prescription-events", partitions=3, replication_factor=3)
admin.create_dlq_topic("prescription-events")  # creates prescription-events.dlq

# List topics
print(admin.list_topics())
```

---

## Configuration Reference

The minimal config only requires `project_id`:

```python
config = SDKConfig(gcp=GCPConfig(project_id="my-project"))
```

Full config with all options:

```python
config = SDKConfig(
    gcp=GCPConfig(
        project_id="vetsource-496203",
        dlp_location="global",
        use_secret_manager=True,
        bootstrap_servers_secret="kafka-bootstrap-servers",
        schema_registry_url_secret="schema-registry-url",
        # Or set directly to bypass Secret Manager:
        # bootstrap_servers="broker1:9092",
        # schema_registry_url="https://registry.example.com",
        token_refresh_buffer_s=300,  # refresh ADC token 5 min before expiry
    ),
    producer=ProducerConfig(
        enable_idempotence=True,  # default — prevents duplicate messages
        acks="all",               # wait for all ISR replicas
        linger_ms=5,
        compression_type="snappy",
    ),
    consumer=ConsumerConfig(
        auto_offset_reset="earliest",
        max_poll_records=500,
        idle_timeout_s=30.0,
    ),
    dlp=DLPConfig(
        enabled=True,
        batch_size=100,        # records per DLP API call
        context_field="order_id",
        max_retries=3,
    ),
    dlq=DLQConfig(
        enabled=True,
        topic_suffix=".dlq",
        raise_on_dlq_failure=True,
    ),
    schema=SchemaConfig(
        auto_register=False,    # require explicit schema registration
        default_compatibility_mode=CompatibilityMode.BACKWARD,
        subject_name_strategy="TopicNameStrategy",
    ),
)
```

From YAML:
```bash
python3 -c "from streamshield import SDKConfig; c = SDKConfig.from_yaml('config.yaml'); c.validate()"
```

From environment variables:
```bash
export STREAMSHIELD_GCP_PROJECT_ID=vetsource-496203
export STREAMSHIELD_DLP_BATCH_SIZE=50
python3 -c "from streamshield import SDKConfig; c = SDKConfig.from_env(); print(c.gcp.project_id)"
```

---

## Running Tests

### Unit tests (no GCP credentials required)

```bash
cd /home/tarunkumar_fusionleap_io/kafka-poc/streamshield
source venv/bin/activate
pip install -e ".[dev]"

pytest tests/unit/ -v --cov=streamshield --cov-report=term-missing
```

### Integration tests (requires GCP access)

Must run on the GCE VM with `vm-producer-sa` attached.

```bash
pytest tests/integration/ -v
```

---

## Error Handling

All SDK exceptions inherit from `StreamShieldError` and carry a `safe_context` dict that is always safe to log:

```python
from streamshield import KafkaProducer, StreamShieldError, SchemaValidationError, TokenizationError

try:
    with KafkaProducer(config) as producer:
        producer.send("my-topic", value=record)
except SchemaValidationError as exc:
    # Record rejected before any I/O — no DLP or Kafka call was made
    print(f"Schema error: {exc} | context: {exc.safe_context}")
except TokenizationError as exc:
    # DLP call failed
    print(f"DLP error: {exc} | context: {exc.safe_context}")
except StreamShieldError as exc:
    # Catch-all for any SDK error
    print(f"SDK error: {type(exc).__name__}: {exc}")
```

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **`enable.auto.commit` is always False** | Prevents data loss when downstream sinks (Snowflake, BigQuery) fail mid-write |
| **DLP batching (100 records/call)** | Up to 100x reduction in DLP API calls compared to the POC |
| **Schema-embedded crypto metadata** | Consumers need only the schema to call DLP — no side-channel config |
| **ADC token proactive refresh** | Prevents SASL auth failures in long-running consumers |
| **DLQ before offset commit** | Failed messages are preserved in DLQ; consumer always moves forward |
| **No auto topic creation** | Prevents misconfigured topics; all topics are created explicitly |
| **HTTPS enforced for Schema Registry** | Schemas carry KMS key references — no plaintext transport |

---

## GCP Resources (Vetsource-496203)

| Resource | Name |
|---|---|
| GCP Project | `vetsource-496203` |
| KMS Key Ring | `vetsource-dlp` (location: global) |
| KMS Key — PII | `pii-dek-kek` |
| KMS Key — PCI-DSS | `pci-dek-kek` |
| Kafka Topic | `prescription-events` (3 partitions) |
| DLQ Topic | `prescription-events.dlq` |
| Schema Registry Subject | `prescription-events-value` |
| VM Service Account | `vm-producer-sa@vetsource-496203.iam.gserviceaccount.com` |

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `PermissionDenied` from DLP | SA missing `roles/cloudkms.cryptoKeyDecrypter` | Add IAM binding |
| `SchemaNotFoundError` | Schema not registered | Run `python3 examples/register_schema.py` |
| `SASL authentication failed` | ADC token expired (in POC without token refresh) | StreamShield refreshes automatically; check ADC is valid |
| `InvalidConfigError: HTTPS` | `schema_registry_url` uses http:// | Use https:// URL |
| `owner_phone` not de-tokenized | Expected — `CryptoHashConfig` is irreversible | This is correct behavior |
| `fastavro.write.UnknownType` | `fastavro.parse_schema()` not called on raw schema | Always pass the parsed schema to serializer/deserializer |
| Consumer stops immediately | `idle_timeout_s` reached with no messages | Increase `idle_timeout_s` or check topic has messages |

---

*StreamShield v0.1.0 — Next milestone: Groovy SDK implementing the same interface*
