# StreamShield SDK Reference

**Version:** 0.1.0 | **Python:** 3.11+

This document is the complete technical reference for the StreamShield SDK. For installation and a 5-minute quickstart, see the [README](../README.md).

---

## Table of Contents

1. [Configuration](#configuration)
2. [KafkaProducer](#kafkaproducer)
3. [KafkaConsumer](#kafkaconsumer)
4. [AsyncKafkaProducer](#asynckafkaproducer)
5. [AsyncKafkaConsumer](#asynckafkaconsumer)
6. [SchemaAdmin](#schemaadmin)
7. [TopicAdmin](#topicadmin)
8. [Data Models](#data-models)
9. [Exception Hierarchy](#exception-hierarchy)
10. [Observability](#observability)

---

## Configuration

All SDK components accept a single `SDKConfig` object. Create one at application startup and reuse it.

```python
from streamshield import (
    SDKConfig, GCPConfig, ProducerConfig, ConsumerConfig,
    DLPConfig, DLQConfig, SchemaConfig, CompatibilityMode
)
```

### SDKConfig

Top-level configuration object. All nested configs have safe defaults — most applications only need to set `GCPConfig.project_id`.

```python
config = SDKConfig(
    gcp      = GCPConfig(...),
    producer = ProducerConfig(...),
    consumer = ConsumerConfig(...),
    dlp      = DLPConfig(...),
    dlq      = DLQConfig(...),
    schema   = SchemaConfig(...),
)
```

#### `SDKConfig.from_yaml(path: str) -> SDKConfig`

Load configuration from a YAML file. Keys not present in the file fall back to dataclass defaults. Calls `validate()` automatically.

```yaml
# streamshield-config.yaml
gcp:
  project_id: my-project
  use_secret_manager: true
dlp:
  batch_size: 100
```

```python
config = SDKConfig.from_yaml("/etc/streamshield/config.yaml")
```

#### `SDKConfig.from_env() -> SDKConfig`

Load configuration from environment variables with the `STREAMSHIELD_` prefix. Calls `validate()` automatically.

| Environment variable | Config field | Type |
|---|---|---|
| `STREAMSHIELD_GCP_PROJECT_ID` | `gcp.project_id` | str |
| `STREAMSHIELD_GCP_USE_SECRET_MANAGER` | `gcp.use_secret_manager` | bool |
| `STREAMSHIELD_GCP_BOOTSTRAP_SERVERS` | `gcp.bootstrap_servers` | str |
| `STREAMSHIELD_GCP_SCHEMA_REGISTRY_URL` | `gcp.schema_registry_url` | str |
| `STREAMSHIELD_GCP_TOKEN_REFRESH_BUFFER_S` | `gcp.token_refresh_buffer_s` | int |
| `STREAMSHIELD_DLP_ENABLED` | `dlp.enabled` | bool |
| `STREAMSHIELD_DLP_BATCH_SIZE` | `dlp.batch_size` | int |
| `STREAMSHIELD_DLP_CONTEXT_FIELD` | `dlp.context_field` | str |

#### `SDKConfig.validate() -> None`

Validates the configuration and raises immediately if invalid. Called automatically by `from_yaml()` and `from_env()`. Call it manually when constructing `SDKConfig` directly:

```python
config = SDKConfig(gcp=GCPConfig(project_id="my-project"))
config.validate()
```

Raises `MissingConfigError` if `project_id` is empty. Raises `InvalidConfigError` for:
- `schema_registry_url` using `http://` instead of `https://`
- `dlp.batch_size` outside the range 1–5000
- `consumer.max_poll_interval_ms` less than `session_timeout_ms`
- `producer.acks` not in `("0", "1", "all", "-1")`
- `schema.subject_name_strategy` not a recognized value

#### `SDKConfig.to_safe_dict() -> dict`

Returns the configuration as a dict with all sensitive values masked (`***`). Safe to include in log output. Fields masked: anything whose key contains `dek`, `key`, `secret`, `token`, `wrapped`, or `password`.

---

### GCPConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `project_id` | `str` | `""` | **Required.** GCP project ID. |
| `dlp_location` | `str` | `"global"` | Cloud DLP API location. |
| `use_secret_manager` | `bool` | `True` | Fetch bootstrap servers and schema registry URL from Secret Manager. |
| `bootstrap_servers_secret` | `str` | `"kafka-bootstrap-servers"` | Secret Manager secret name for the Kafka bootstrap address. |
| `schema_registry_url_secret` | `str` | `"schema-registry-url"` | Secret Manager secret name for the Schema Registry HTTPS URL. |
| `bootstrap_servers` | `str \| None` | `None` | Set directly to bypass Secret Manager. |
| `schema_registry_url` | `str \| None` | `None` | Set directly to bypass Secret Manager. Must start with `https://`. |
| `token_refresh_buffer_s` | `int` | `300` | Proactively refresh the ADC token this many seconds before expiry. Prevents SASL auth failures in long-running processes. |
| `secrets_refresh_interval_s` | `int \| None` | `None` | Re-fetch Secret Manager secrets on this interval. `None` = cache forever. Set this when secrets are rotated without a process restart. |

---

### ProducerConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `enable_idempotence` | `bool` | `True` | Prevents duplicate messages during broker retries. Always leave enabled in production. |
| `acks` | `str` | `"all"` | Broker acknowledgment requirement. `"all"` waits for all in-sync replicas. |
| `retries` | `int` | `5` | Number of broker-level retries before `DeliveryFailedError`. |
| `retry_backoff_ms` | `int` | `500` | Wait between broker retries (ms). |
| `linger_ms` | `int` | `5` | Wait up to this many ms to batch messages before sending. |
| `batch_size_bytes` | `int` | `65536` | Maximum bytes per Kafka batch (64 KB). |
| `compression_type` | `str` | `"snappy"` | Message compression. Options: `"none"`, `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"`. |
| `request_timeout_ms` | `int` | `30000` | Timeout per produce request (ms). |
| `delivery_timeout_ms` | `int` | `120000` | Total delivery timeout including retries (ms). |
| `validate_topic_on_send` | `bool` | `True` | Check that the topic exists before the first `send()`. Raises `TopicNotFoundError` early. |

---

### ConsumerConfig

> `enable.auto.commit` is intentionally absent from this config. StreamShield always sets it to `False` and manages offset commits explicitly after your handler succeeds. This is not configurable.

| Field | Type | Default | Description |
|---|---|---|---|
| `auto_offset_reset` | `str` | `"earliest"` | Where to start reading when no committed offset exists. `"earliest"` reads from the oldest available message. |
| `max_poll_records` | `int` | `500` | Maximum records returned per internal `poll()` call. |
| `session_timeout_ms` | `int` | `30000` | Consumer is declared dead if no heartbeat within this window (ms). |
| `heartbeat_interval_ms` | `int` | `3000` | Heartbeat frequency (ms). Must be less than `session_timeout_ms / 3`. |
| `max_poll_interval_ms` | `int` | `300000` | Maximum time between `poll()` calls before a rebalance is triggered (ms). Increase if your handler takes longer than 5 minutes. |
| `idle_timeout_s` | `float` | `30.0` | Exit the `process()` loop after this many seconds with no messages. `0` runs forever. |

---

### DLPConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `True` | Set to `False` to skip DLP entirely. Records pass through unchanged. Useful for non-sensitive schemas. |
| `batch_size` | `int` | `100` | Records per DLP `deidentifyContent` / `reidentifyContent` API call. Max 5000. |
| `context_field` | `str` | `"order_id"` | Field used as the CryptoDeterministicConfig context. Ties each token to a specific record so the same value tokenizes differently across records. Overridden by the schema-level `token.context-field` annotation if present. |
| `max_retries` | `int` | `3` | Retries on transient DLP errors (`UNAVAILABLE`, `RESOURCE_EXHAUSTED`). Does not retry `PERMISSION_DENIED` or `INVALID_ARGUMENT`. |
| `retry_backoff_ms` | `int` | `500` | Initial backoff for DLP retries. Doubles on each attempt (exponential). |

---

### DLQConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `True` | Route failed messages to the DLQ instead of crashing the consumer. |
| `topic_suffix` | `str` | `".dlq"` | DLQ topic name = source topic name + this suffix. |
| `max_retries` | `int` | `3` | Attempts to publish to the DLQ before raising `DLQPublishError`. |
| `raise_on_dlq_failure` | `bool` | `True` | Raise `DLQPublishError` if the DLQ itself is unavailable. Set to `False` to log and continue (accepts potential message loss as a last resort). |
| `auto_create_topic` | `bool` | `True` | Automatically create the DLQ topic on first route if it does not exist. |

---

### SchemaConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `auto_register` | `bool` | `False` | Automatically register the schema on the first `send()` if none exists. Disabled by default — schemas must be pre-registered by an operator. |
| `default_compatibility_mode` | `CompatibilityMode` | `BACKWARD` | Compatibility mode applied when registering a new schema version. |
| `subject_name_strategy` | `str` | `"TopicNameStrategy"` | How to derive the Schema Registry subject name from the topic. See below. |
| `cache_capacity` | `int` | `1000` | Maximum number of schemas to keep in the in-process cache. |

**Subject name strategies:**

| Strategy | Subject format |
|---|---|
| `TopicNameStrategy` | `{topic}-value` (default) |
| `RecordNameStrategy` | `{avro_namespace}.{avro_record_name}` |
| `TopicRecordNameStrategy` | `{topic}-{avro_namespace}.{avro_record_name}` |

---

### CompatibilityMode

```python
from streamshield import CompatibilityMode

CompatibilityMode.BACKWARD            # new schema can read old data (safe to add optional fields)
CompatibilityMode.FORWARD             # old schema can read new data (safe to remove optional fields)
CompatibilityMode.FULL                # both BACKWARD and FORWARD
CompatibilityMode.BACKWARD_TRANSITIVE # BACKWARD against all historical versions
CompatibilityMode.FORWARD_TRANSITIVE  # FORWARD against all historical versions
CompatibilityMode.FULL_TRANSITIVE     # FULL against all historical versions
CompatibilityMode.NONE                # no checks (use with caution)
```

---

## KafkaProducer

Synchronous Kafka producer with integrated DLP tokenization and schema enforcement.

```python
from streamshield import KafkaProducer, SDKConfig, GCPConfig

config = SDKConfig(gcp=GCPConfig(project_id="my-project"))

with KafkaProducer(config) as producer:
    producer.send("my-topic", key="k1", value=record)
# flush() called automatically on context manager exit
```

**Lifecycle:** Use as a context manager (preferred). On exit, `flush()` is called to wait for all pending deliveries before the connection is closed. Alternatively call `close()` explicitly.

**Thread safety:** A single `KafkaProducer` instance can be shared across threads. The delivery failure list is protected by a lock.

---

### `KafkaProducer.send()`

```python
def send(
    topic:          str,
    value:          dict,
    key:            str | None = None,
    schema_version: int | None = None,
    headers:        dict[str, str] | None = None,
    on_delivery:    Callable | None = None,
) -> MessageMetadata
```

Produce one message. The pipeline runs synchronously: validate → tokenize → serialize → produce (non-blocking enqueue).

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `topic` | `str` | Kafka topic name. Topic must exist. |
| `value` | `dict` | Plaintext record. Sensitive fields (those with `logicalType="tokenized"` in the schema) are tokenized automatically. |
| `key` | `str \| None` | Kafka message key. Encoded to UTF-8. Used for partition routing. |
| `schema_version` | `int \| None` | Pin to a specific schema version number. `None` fetches the latest version. |
| `headers` | `dict[str, str] \| None` | Optional key-value headers attached to the Kafka message. |
| `on_delivery` | `Callable \| None` | Optional callback `fn(err, msg)` invoked on delivery confirmation, in addition to internal tracking. |

**Returns:** `MessageMetadata` with `topic` populated. `partition` and `offset` are `-1` until `flush()` is called.

**Raises:**

| Exception | When |
|---|---|
| `SchemaValidationError` | Record does not match the Avro schema. Raised before any DLP or Kafka I/O. |
| `SchemaNotFoundError` | No schema registered for this topic. |
| `TokenizationError` | DLP `deidentifyContent` call failed. |
| `SerializationFailedError` | fastavro could not encode the record. |
| `TopicNotFoundError` | Topic does not exist (when `validate_topic_on_send=True`). |

---

### `KafkaProducer.send_batch()`

```python
def send_batch(
    topic:          str,
    records:        list[dict],
    key_field:      str | None = None,
    schema_version: int | None = None,
) -> list[MessageMetadata]
```

Produce multiple messages using a **single DLP API call** for the entire batch. All records must conform to the same schema.

Compared to calling `send()` in a loop, `send_batch()` sends all records as one DLP table request instead of one DLP call per record — up to 100x fewer DLP API calls for a full batch.

If `len(records) > dlp.batch_size`, records are automatically split into multiple DLP calls.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `topic` | `str` | Kafka topic name. |
| `records` | `list[dict]` | Plaintext record dicts. All validated against the schema before any DLP call. |
| `key_field` | `str \| None` | Field name in each record to use as the Kafka message key. `None` = no key. |
| `schema_version` | `int \| None` | Pin to a specific schema version. `None` = latest. |

**Returns:** List of `MessageMetadata` in the same order as `records`. `partition` and `offset` are `-1` until `flush()`.

**Raises:** Same as `send()`, plus: raises `SchemaValidationError` at the first invalid record before any DLP or Kafka call.

---

### `KafkaProducer.flush()`

```python
def flush(timeout: float = 30.0) -> None
```

Block until all in-flight messages are acknowledged by the broker, then raise if any delivery failed.

**Parameters:** `timeout` — seconds to wait. Messages still in-flight after the timeout are logged as a warning but do not raise.

**Raises:** `DeliveryFailedError` if any message failed delivery since the last `flush()`.

---

### `KafkaProducer.close()`

Flush pending messages and close the producer. Idempotent — safe to call multiple times.

---

## KafkaConsumer

Synchronous Kafka consumer with integrated DLP detokenization, commit-after-process offset management, and DLQ routing.

```python
from streamshield import KafkaConsumer, ConsumedMessage

with KafkaConsumer(config, group_id="my-group") as consumer:
    consumer.process(handler=handle, topics=["my-topic"], detokenize=True)
```

**Offset management contract:**
- `enable.auto.commit` is always `False`. This is not configurable.
- Offsets are committed **only after** the handler returns successfully.
- If the handler raises, the message is routed to the DLQ and then committed so the consumer always advances.

---

### `KafkaConsumer.subscribe()`

```python
def subscribe(
    topics:    list[str],
    on_assign: Callable | None = None,
    on_revoke: Callable | None = None,
) -> None
```

Subscribe to one or more Kafka topics. Called automatically by `process()`.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `topics` | `list[str]` | Topic names to subscribe to. |
| `on_assign` | `Callable \| None` | Callback invoked when partitions are assigned after a rebalance. |
| `on_revoke` | `Callable \| None` | Callback invoked when partitions are being revoked. |

---

### `KafkaConsumer.poll()`

```python
def poll(
    timeout:    float = 1.0,
    detokenize: bool  = False,
) -> ConsumedMessage | None
```

Poll for one message. Returns `None` if no message arrives within the timeout. The caller is responsible for calling `commit()` after processing.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `timeout` | `float` | Seconds to wait for a message. |
| `detokenize` | `bool` | Call DLP `reidentifyContent` to reverse tokens before returning. Requires `cryptoKeyDecrypter` IAM role. |

**Returns:** `ConsumedMessage` or `None`.

**Raises:** `DeserializationFailedError`, `DetokenizationError`, `KafkaException`.

---

### `KafkaConsumer.commit()`

```python
def commit(
    message:     ConsumedMessage | None = None,
    asynchronous: bool = False,
) -> None
```

Commit the offset for a processed message.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `message` | `ConsumedMessage \| None` | The message to commit. `None` commits the current position. |
| `asynchronous` | `bool` | Commit in the background with no error feedback. |

**Raises:** `OffsetCommitError` on synchronous commit failure.

---

### `KafkaConsumer.process()`

```python
def process(
    handler:       Callable[[ConsumedMessage], None],
    topics:        list[str],
    detokenize:    bool          = False,
    max_messages:  int | None    = None,
    idle_timeout_s: float | None = None,
) -> None
```

Run a managed poll loop. Subscribes, polls, calls the handler, and commits offsets. Handles DLQ routing automatically on failure.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `handler` | `Callable` | Called with each `ConsumedMessage`. Must not suppress exceptions — raise on failure so the DLQ router catches it. |
| `topics` | `list[str]` | Topics to subscribe to. |
| `detokenize` | `bool` | Call DLP `reidentifyContent` before delivering to the handler. |
| `max_messages` | `int \| None` | Stop after processing this many messages. `None` runs until `idle_timeout_s`. |
| `idle_timeout_s` | `float \| None` | Stop after this many seconds with no messages. `None` uses `ConsumerConfig.idle_timeout_s`. |

**Failure handling:**
- Deserialization failure → message routed to DLQ, offset committed, loop continues.
- DLP detokenization failure → same.
- Handler raises → same.
- DLQ unavailable and `raise_on_dlq_failure=True` → raises `DLQPublishError`.

---

### `KafkaConsumer.close()`

Flush pending offsets, close the DLQ producer, and close the consumer. Idempotent.

---

## AsyncKafkaProducer

Async wrapper around `KafkaProducer`. All methods are `async` and delegate to the synchronous producer running on a thread pool.

```python
from streamshield import AsyncKafkaProducer

async with AsyncKafkaProducer(config) as producer:
    await producer.send("my-topic", key="k1", value=record)
    await producer.send_batch("my-topic", records=records, key_field="id")
    await producer.flush()
```

**Methods:** Same signatures as `KafkaProducer` with `async` prefix. See [KafkaProducer](#kafkaproducer).

**Note:** `__init__` is synchronous (GCP auth happens at construction). For fully async initialisation use `AsyncKafkaProducer.create(config)`.

```python
producer = await AsyncKafkaProducer.create(config)
```

---

## AsyncKafkaConsumer

Async wrapper around `KafkaConsumer`. Accepts both regular functions and coroutine handlers.

```python
from streamshield import AsyncKafkaConsumer

async def handle(msg: ConsumedMessage) -> None:
    await db.insert(msg.value)

async with AsyncKafkaConsumer(config, group_id="my-group") as consumer:
    await consumer.process(handle, topics=["my-topic"], detokenize=True)
```

**Methods:** `subscribe`, `poll`, `commit`, `process`, `close` — same signatures as `KafkaConsumer` with `async` prefix.

---

## SchemaAdmin

Manages schema registration and compatibility in the Confluent Schema Registry.

```python
from streamshield import SchemaAdmin, CompatibilityMode

admin = SchemaAdmin(config)
```

---

### `SchemaAdmin.register()`

```python
def register(
    subject:            str,
    schema_definition:  dict,
    compatibility_mode: CompatibilityMode | None = None,
) -> SchemaVersion
```

Register a new schema version under a subject.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `subject` | `str` | Schema Registry subject name (e.g. `"prescription-events-value"`). |
| `schema_definition` | `dict` | Raw Avro schema dict. |
| `compatibility_mode` | `CompatibilityMode \| None` | Compatibility mode to set for this subject. `None` uses the registry default. |

**Returns:** `SchemaVersion` with `schema_id`, `version`, and `subject`.

**Raises:** `SchemaRegistrationError`, `SchemaCompatibilityError`.

---

### `SchemaAdmin.get_latest()`

```python
def get_latest(subject: str) -> SchemaVersion
```

Fetch the latest registered schema version for a subject.

**Raises:** `SchemaNotFoundError` if the subject has no registered schema.

---

### `SchemaAdmin.get_by_id()`

```python
def get_by_id(schema_id: int) -> SchemaDefinition
```

Fetch a schema by its integer ID.

**Raises:** `SchemaNotFoundError` if no schema with that ID exists.

---

### `SchemaAdmin.check_compatibility()`

```python
def check_compatibility(subject: str, schema_definition: dict) -> CompatibilityResult
```

Check whether a schema is compatible with the latest registered version without registering it.

**Returns:** `CompatibilityResult` with `is_compatible: bool` and `messages: list[str]`.

---

### `SchemaAdmin.set_compatibility()`

```python
def set_compatibility(subject: str, mode: CompatibilityMode) -> None
```

Update the compatibility mode for an existing subject.

---

## TopicAdmin

Creates and manages Kafka topics.

```python
from streamshield import TopicAdmin

admin = TopicAdmin(config)
```

---

### `TopicAdmin.create_topic()`

```python
def create_topic(
    topic:              str,
    partitions:         int = 3,
    replication_factor: int = 3,
    config:             dict | None = None,
) -> TopicCreationResult
```

Create a Kafka topic.

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `topic` | `str` | Topic name. |
| `partitions` | `int` | Number of partitions. |
| `replication_factor` | `int` | Number of replicas per partition. |
| `config` | `dict \| None` | Additional broker-level topic config (e.g. `{"retention.ms": "604800000"}`). |

**Raises:** `TopicCreationError`.

---

### `TopicAdmin.create_dlq_topic()`

```python
def create_dlq_topic(source_topic: str) -> TopicCreationResult
```

Create the DLQ topic for a source topic. The DLQ topic name is `{source_topic}{DLQConfig.topic_suffix}` (default: `{source_topic}.dlq`).

---

### `TopicAdmin.topic_exists()`

```python
def topic_exists(topic: str) -> bool
```

Return `True` if the topic exists on the broker.

---

### `TopicAdmin.describe_topic()`

```python
def describe_topic(topic: str) -> TopicMetadata
```

Return partition count, replication factor, and broker-level config for a topic.

**Raises:** `TopicNotFoundError`.

---

### `TopicAdmin.delete_topic()`

```python
def delete_topic(topic: str, confirm: bool = False) -> None
```

Delete a topic. `confirm=True` is required — without it `ValueError` is raised immediately without touching the broker.

---

### `TopicAdmin.list_topics()`

```python
def list_topics() -> list[str]
```

Return all topic names visible to the authenticated service account.

---

## Data Models

### ConsumedMessage

Returned by `poll()` and passed to `process()` handlers.

```python
from streamshield import ConsumedMessage

msg.topic      # str  — Kafka topic name
msg.partition  # int  — partition number
msg.offset     # int  — message offset
msg.timestamp  # int  — broker timestamp (milliseconds since epoch)
msg.key        # bytes | None — raw message key
msg.value      # dict — deserialized record (detokenized if detokenize=True)
msg.raw_schema # dict — raw Avro schema dict with token.* metadata
msg.schema_id  # int  — schema_id from the Confluent wire format header
msg.headers    # dict[str, bytes] — Kafka message headers
```

---

### MessageMetadata

Returned by `send()` and `send_batch()`.

```python
meta.topic     # str — topic the message was produced to
meta.partition # int — partition (-1 until flush() completes)
meta.offset    # int — broker offset (-1 until flush() completes)
meta.timestamp # int — broker timestamp (-1 until flush() completes)
meta.key       # bytes | None — encoded message key
```

---

### SchemaVersion

Returned by `SchemaAdmin.register()`, `get_latest()`.

```python
sv.schema_id # int  — unique integer ID assigned by the Schema Registry
sv.subject   # str  — subject name (e.g. "prescription-events-value")
sv.version   # int  — version number within the subject (1-based)
sv.schema    # dict — raw Avro schema dict
```

---

### SchemaDefinition

Returned by `SchemaAdmin.get_by_id()`.

```python
defn.schema_id   # int
defn.schema      # dict — raw Avro schema dict
defn.schema_type # str  — always "AVRO" in this version
```

---

### CompatibilityResult

Returned by `SchemaAdmin.check_compatibility()`.

```python
result.is_compatible # bool
result.messages      # list[str] — human-readable reasons if not compatible
```

---

## Exception Hierarchy

All SDK exceptions inherit from `StreamShieldError` and carry a `safe_context: dict` attribute that is always safe to log — it never contains record values, PII, or key material.

```
StreamShieldError
├── ConfigurationError
│   ├── MissingConfigError       — required field not provided (e.g. project_id)
│   └── InvalidConfigError       — field present but invalid (e.g. http:// URL, batch_size=0)
├── AuthenticationError          — ADC or Kafka SASL auth failed
│   └── TokenRefreshError        — ADC OAuth2 token refresh failed
├── SchemaError
│   ├── SchemaNotFoundError      — no schema for this subject or schema_id
│   ├── SchemaRegistrationError  — POST to registry failed
│   ├── SchemaValidationError    — record rejected by Avro schema (before any I/O)
│   └── SchemaCompatibilityError — new schema breaks the configured compatibility mode
│                                  carries .messages: list[str] with reasons
├── SerializationError
│   ├── SerializationFailedError     — fastavro write failed
│   └── DeserializationFailedError   — bad magic byte or fastavro read failed
├── DLPError
│   ├── TokenizationError        — DLP deidentifyContent failed
│   └── DetokenizationError      — DLP reidentifyContent failed
├── TopicError
│   ├── TopicNotFoundError
│   └── TopicCreationError
├── ProducerError
│   ├── DeliveryFailedError      — broker did not acknowledge delivery (raised from flush())
│   └── MessageTooLargeError     — serialized message exceeds broker max.message.bytes
└── ConsumerError
    ├── OffsetCommitError        — offset commit to broker failed
    └── DLQPublishError          — DLQ topic also unavailable; message unrecoverable
```

**When to catch what:**

| Scenario | Exception to catch |
|---|---|
| Record does not match schema | `SchemaValidationError` |
| DLP call failed | `TokenizationError` / `DetokenizationError` |
| Broker rejected delivery | `DeliveryFailedError` (from `flush()`) |
| Schema not registered | `SchemaNotFoundError` |
| New schema breaks compatibility | `SchemaCompatibilityError` |
| Any SDK failure | `StreamShieldError` |

---

## Observability

### Logging

All SDK output goes through named Python loggers. No `print()` is used.

| Logger name | Emitting component |
|---|---|
| `streamshield.auth` | Token refresh, Secret Manager |
| `streamshield.schema` | Schema fetch, cache hits/misses, registration |
| `streamshield.dlp` | Tokenization and detokenization |
| `streamshield.producer` | Send, flush, delivery callbacks |
| `streamshield.consumer` | Poll, commit, process loop |
| `streamshield.dlq` | DLQ routing events |
| `streamshield.topic` | Topic creation and validation |
| `streamshield.metrics` | OTel metric snapshots (when using `configure_logging_metrics`) |

Configure structured JSON output:

```python
import logging
from streamshield import configure_json_logging

configure_json_logging(level=logging.INFO)
```

Add a file handler:

```python
import json, logging

class _JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {"level": record.levelname, "logger": record.name, "message": record.getMessage()}
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)

file_handler = logging.FileHandler("streamshield.log")
file_handler.setFormatter(_JsonFormatter())
logging.getLogger("streamshield").addHandler(file_handler)
```

---

### Metrics

OpenTelemetry metrics are available when `opentelemetry-sdk` is installed (`pip install 'streamshield[metrics]'`). Without the package, all metric calls are no-ops.

| Metric name | Type | Labels | Description |
|---|---|---|---|
| `streamshield_messages_produced_total` | Counter | `topic`, `status` | Messages produced. `status`: `success` or `failed`. |
| `streamshield_messages_consumed_total` | Counter | `topic`, `group_id`, `status` | Messages consumed. `status`: `success` or `dlq`. |
| `streamshield_dlp_calls_total` | Counter | `operation`, `status` | DLP API calls. `operation`: `tokenize` or `detokenize`. |
| `streamshield_dlp_call_duration_seconds` | Histogram | `operation` | DLP API call latency in seconds. |
| `streamshield_dlp_records_per_call` | Histogram | `operation` | Records sent per DLP API call. |
| `streamshield_schema_cache_hits_total` | Counter | `schema_id` | Schema Registry cache hits. |
| `streamshield_schema_cache_misses_total` | Counter | `schema_id` | Schema Registry cache misses (HTTP fetches). |
| `streamshield_token_refreshes_total` | Counter | `reason` | ADC OAuth2 token refresh events. |
| `streamshield_dlq_messages_total` | Counter | `source_topic`, `reason` | Messages routed to DLQ. |
| `streamshield_offset_commits_total` | Counter | `topic`, `group_id`, `status` | Offset commit attempts. |

Route metric snapshots through Python logging (lands in log file alongside SDK logs):

```python
from streamshield import configure_logging_metrics

configure_logging_metrics(export_interval_ms=10_000)
```

Print to stdout only (console debugging):

```python
from streamshield import configure_console_metrics

configure_console_metrics(export_interval_ms=10_000)
```

---

## Design decisions

These decisions are intentional and should not be changed without understanding the rationale.

**`enable.auto.commit` is always `False`**
Offsets are committed only after the application handler returns successfully. This prevents data loss when downstream sinks (Snowflake, BigQuery) fail mid-write. If your handler does not raise, the message is considered processed and the offset is committed.

**DLP batching**
`send_batch()` sends up to `DLPConfig.batch_size` records in a single `deidentifyContent` table request. The SDK never calls DLP once per record. For bulk workloads this is up to 100x fewer API calls.

**Schema-embedded crypto metadata**
KMS key names, wrapped DEKs, and per-field tokenization policy live in the Avro schema registered in the Schema Registry. Consumers need only the schema to call DLP — there is no side-channel configuration.

**HTTPS enforced for Schema Registry**
`InvalidConfigError` is raised for `http://` schema registry URLs. Schemas carry KMS key references — plaintext transport is not permitted.

**No auto topic creation**
Topics are never auto-created by `send()` or `subscribe()`. They must be pre-created by `TopicAdmin.create_topic()` or Terraform. This prevents misconfigured topic names from producing silently to the wrong topic.

**DLQ before offset commit**
When a message fails, the SDK routes it to the DLQ and then commits the offset. The consumer always moves forward — no poison-pill infinite loops. The original message is preserved in the DLQ for inspection and replay.
