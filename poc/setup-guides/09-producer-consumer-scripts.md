# Step 09 — Producer & Consumer Python Scripts

These scripts run on the GCE VM (`poc-dev-vm`) inside the VSCode terminal. The **producer** serializes `OrderEvent` records using Avro, registers the schema with Confluent Schema Registry, and publishes messages to the `raw-events` Kafka topic. The **consumer** reads messages from the same topic, strips the Confluent wire-format header, and decodes each Avro payload back into a Python dictionary.

All credentials are read at runtime from **Secret Manager** — no plaintext secrets in the code.

---

## 1. Project Structure

Make sure you are inside `~/kafka-poc/` with the virtual environment activated before creating any files:

```bash
cd ~/kafka-poc
source venv/bin/activate
```

You will create four files:

```
~/kafka-poc/
├── venv/               # virtual environment (already created in Step 07)
├── utils.py            # Secret Manager helper
├── schema.py           # Avro schema definition
├── producer.py         # Avro producer
└── consumer.py         # Avro consumer
```

---

## 2. `utils.py` — Secret Manager Helper

This module provides a single function that fetches the latest version of any secret from GCP Secret Manager. All other scripts import from here instead of duplicating the boilerplate.

Create the file `~/kafka-poc/utils.py` with the following content:

```python
# utils.py — Secret Manager helper
from google.cloud import secretmanager


def get_secret(project_id: str, secret_name: str) -> str:
    """
    Fetches the latest version of a secret from GCP Secret Manager.

    Args:
        project_id:  GCP project ID (e.g., "kafka-poc")
        secret_name: Name of the secret (e.g., "kafka-bootstrap-servers")

    Returns:
        The secret value as a plain string.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
```

---

## 3. `schema.py` — Avro Schema Definition

This module defines the `OrderEvent` Avro schema as a Python dictionary. Both the producer (for serialization) and the consumer (for deserialization) import this schema.

Create the file `~/kafka-poc/schema.py`:

```python
# schema.py — Avro schema for OrderEvent

ORDER_EVENT_SCHEMA = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.poc.events",
    "fields": [
        {"name": "order_id",    "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "product_id",  "type": "string"},
        {"name": "amount",      "type": "double"},
        {"name": "currency",    "type": "string"},
        {"name": "timestamp",   "type": "long"},   # epoch milliseconds
        {"name": "status",      "type": "string"}
    ]
}
```

> **Avro schema basics:** Avro is a compact binary serialization format. The schema above says every `OrderEvent` record must have exactly these 7 fields with these types. If the producer tries to send a record with a missing field or wrong type, fastavro raises an error before the message ever reaches Kafka.

---

## 4. `producer.py` — Avro Producer

This script:
1. Reads all credentials from Secret Manager
2. Registers the `OrderEvent` schema with Confluent Schema Registry (or retrieves the existing schema ID if already registered)
3. Creates a `confluent_kafka.Producer` configured for SASL/SSL authentication
4. Generates 10 sample `OrderEvent` records with realistic fake data
5. Serializes each record using the **Confluent wire format**: `0x00` magic byte + 4-byte big-endian schema ID + Avro bytes
6. Publishes each serialized message to the `raw-events` topic
7. Prints a confirmation line for each delivered message

Create the file `~/kafka-poc/producer.py`:

```python
# producer.py — Avro producer for raw-events topic
import io
import json
import random
import struct
import time
import uuid

import fastavro
import requests
from confluent_kafka import Producer

from schema import ORDER_EVENT_SCHEMA
from utils import get_secret

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ID = "kafka-poc"
TOPIC      = "raw-events"

# ── Load credentials from Secret Manager ─────────────────────────────────────

print("Loading credentials from Secret Manager...")
bootstrap_servers     = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url   = get_secret(PROJECT_ID, "schema-registry-url")
schema_registry_key   = get_secret(PROJECT_ID, "schema-registry-api-key")
schema_registry_secret = get_secret(PROJECT_ID, "schema-registry-api-secret")
print("Credentials loaded.")

# ── Register / fetch schema from Confluent Schema Registry ───────────────────

def get_or_register_schema(
    registry_url: str,
    api_key: str,
    api_secret: str,
    subject: str,
    schema: dict,
) -> int:
    """
    Registers the schema under the given subject if it does not exist yet.
    Returns the schema ID (integer) assigned by Schema Registry.
    Schema Registry de-duplicates identical schemas, so re-registering is safe.
    """
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
    payload = {"schema": json.dumps(schema)}
    response = requests.post(
        url,
        auth=(api_key, api_secret),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        json=payload,
        timeout=10,
    )
    response.raise_for_status()
    schema_id = response.json()["id"]
    print(f"Schema registered/found — ID: {schema_id}")
    return schema_id


# The "subject" naming convention for a topic's value schema is "<topic>-value"
SUBJECT   = f"{TOPIC}-value"
schema_id = get_or_register_schema(
    schema_registry_url,
    schema_registry_key,
    schema_registry_secret,
    SUBJECT,
    ORDER_EVENT_SCHEMA,
)

# Parse schema once for reuse in serialization
parsed_schema = fastavro.parse_schema(ORDER_EVENT_SCHEMA)

# ── Confluent wire-format serializer ─────────────────────────────────────────

def serialize_avro(record: dict, schema, sid: int) -> bytes:
    """
    Serializes a record to the Confluent wire format:
      Byte 0:    magic byte 0x00
      Bytes 1-4: schema ID as 4-byte big-endian integer
      Bytes 5+:  Avro schemaless binary encoding of the record

    This format is required so that Kafka consumers (including
    non-Python ones) can identify the schema ID and decode the payload.
    """
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, sid))   # magic byte + schema ID
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()

# ── Delivery report callback ──────────────────────────────────────────────────

def delivery_report(err, msg):
    """Called by the Producer once a message is acknowledged by Kafka."""
    if err is not None:
        print(f"Delivery FAILED: {err}")
    else:
        # Decode the message value just enough to extract order_id for logging
        raw = msg.value()
        buf = io.BytesIO(raw[5:])  # skip the 5-byte wire-format header
        decoded = fastavro.schemaless_reader(buf, parsed_schema)
        print(
            f"Delivered: order_id={decoded['order_id']}"
            f"  to {msg.topic()} [partition={msg.partition()}]"
            f"  offset={msg.offset()}"
        )

# ── Kafka Producer configuration ─────────────────────────────────────────────

producer_config = {
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "OAUTHBEARER",
    # GCP Managed Kafka uses Application Default Credentials via OAuth.
    # The confluent-kafka library picks up the VM's attached service account
    # automatically when sasl.oauthbearer.method is set to "oidc".
    "sasl.oauthbearer.method":        "oidc",
    "sasl.oauthbearer.client.id":     "unused",
    "sasl.oauthbearer.client.secret": "unused",
    "sasl.oauthbearer.scope":         "https://www.googleapis.com/auth/cloud-platform",
    "sasl.oauthbearer.token.endpoint.url": (
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts"
        "/default/token"
    ),
}

producer = Producer(producer_config)

# ── Sample data helpers ───────────────────────────────────────────────────────

PRODUCTS  = ["prod-001", "prod-042", "prod-099", "prod-200", "prod-317"]
CUSTOMERS = ["cust-A1", "cust-B2", "cust-C3", "cust-D4", "cust-E5"]
STATUSES  = ["CREATED", "SHIPPED", "DELIVERED"]
CURRENCIES = ["USD", "EUR", "GBP"]


def make_order_event() -> dict:
    """Generates one realistic fake OrderEvent record."""
    return {
        "order_id":    str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "product_id":  random.choice(PRODUCTS),
        "amount":      round(random.uniform(9.99, 499.99), 2),
        "currency":    random.choice(CURRENCIES),
        "timestamp":   int(time.time() * 1000),  # epoch milliseconds
        "status":      random.choice(STATUSES),
    }

# ── Produce 10 messages ───────────────────────────────────────────────────────

print(f"\nProducing 10 messages to topic '{TOPIC}'...\n")

for i in range(10):
    event = make_order_event()
    serialized = serialize_avro(event, parsed_schema, schema_id)

    producer.produce(
        topic=TOPIC,
        value=serialized,
        key=event["order_id"].encode("utf-8"),
        callback=delivery_report,
    )

    # Trigger delivery for callbacks (non-blocking — does not wait for ack)
    producer.poll(0)

    # Small delay so timestamps differ slightly across messages
    time.sleep(0.1)

# Wait for all outstanding messages to be acknowledged before exiting
print("\nFlushing producer — waiting for all acks...")
producer.flush()
print("\nAll messages delivered successfully.")
```

---

## 5. `consumer.py` — Avro Consumer

This script:
1. Reads credentials from Secret Manager
2. Creates a `confluent_kafka.Consumer` in consumer group `poc-consumer-group`
3. Subscribes to `raw-events` from the **earliest** available offset
4. Polls for messages, deserializes the Confluent wire format, and prints each decoded `OrderEvent`
5. Exits cleanly after 30 seconds of receiving no new messages, or on `Ctrl+C`

Create the file `~/kafka-poc/consumer.py`:

```python
# consumer.py — Avro consumer for raw-events topic
import io
import json
import struct
import time

import fastavro
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

from schema import ORDER_EVENT_SCHEMA
from utils import get_secret

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ID      = "kafka-poc"
TOPIC           = "raw-events"
CONSUMER_GROUP  = "poc-consumer-group"
IDLE_TIMEOUT_S  = 30   # exit after this many seconds with no new messages

# ── Load credentials from Secret Manager ─────────────────────────────────────

print("Loading credentials from Secret Manager...")
bootstrap_servers      = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url    = get_secret(PROJECT_ID, "schema-registry-url")
schema_registry_key    = get_secret(PROJECT_ID, "schema-registry-api-key")
schema_registry_secret = get_secret(PROJECT_ID, "schema-registry-api-secret")
print("Credentials loaded.\n")

# ── Schema cache (avoid repeated HTTP calls for the same schema ID) ──────────

_schema_cache: dict[int, object] = {}


def get_schema_by_id(schema_id: int) -> object:
    """
    Fetches the Avro schema for the given schema ID from Confluent Schema Registry.
    Results are cached in memory for the lifetime of this process.
    """
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]

    url = f"{schema_registry_url.rstrip('/')}/schemas/ids/{schema_id}"
    response = requests.get(
        url,
        auth=(schema_registry_key, schema_registry_secret),
        timeout=10,
    )
    response.raise_for_status()
    raw_schema = json.loads(response.json()["schema"])
    parsed = fastavro.parse_schema(raw_schema)
    _schema_cache[schema_id] = parsed
    print(f"  [schema cache] Loaded schema ID {schema_id} from registry.")
    return parsed


# ── Confluent wire-format deserializer ────────────────────────────────────────

def deserialize_avro(raw_bytes: bytes) -> dict:
    """
    Deserializes a Confluent wire-format message:
      Byte 0:    magic byte (must be 0x00)
      Bytes 1-4: schema ID as 4-byte big-endian integer
      Bytes 5+:  Avro schemaless binary payload

    Fetches the schema from Schema Registry using the embedded schema ID,
    then decodes the Avro payload into a Python dictionary.
    """
    if len(raw_bytes) < 5:
        raise ValueError(f"Message too short to be Confluent wire format: {len(raw_bytes)} bytes")

    magic, schema_id = struct.unpack(">bI", raw_bytes[:5])

    if magic != 0:
        raise ValueError(f"Unexpected magic byte: {magic!r} (expected 0x00)")

    schema = get_schema_by_id(schema_id)
    buf = io.BytesIO(raw_bytes[5:])
    return fastavro.schemaless_reader(buf, schema)


# ── Kafka Consumer configuration ─────────────────────────────────────────────

consumer_config = {
    "bootstrap.servers":  bootstrap_servers,
    "security.protocol":  "SASL_SSL",
    "sasl.mechanisms":    "OAUTHBEARER",
    "sasl.oauthbearer.method":        "oidc",
    "sasl.oauthbearer.client.id":     "unused",
    "sasl.oauthbearer.client.secret": "unused",
    "sasl.oauthbearer.scope":         "https://www.googleapis.com/auth/cloud-platform",
    "sasl.oauthbearer.token.endpoint.url": (
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts"
        "/default/token"
    ),
    "group.id":             CONSUMER_GROUP,
    "auto.offset.reset":    "earliest",   # read from the beginning if no committed offset
    "enable.auto.commit":   True,
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC])

# ── Poll loop ─────────────────────────────────────────────────────────────────

print(f"Subscribed to '{TOPIC}' as group '{CONSUMER_GROUP}'.")
print(f"Waiting for messages (will exit after {IDLE_TIMEOUT_S}s of silence)...\n")
print("-" * 60)

messages_received = 0
last_message_time = time.time()

try:
    while True:
        # poll() blocks for up to 1 second waiting for a message
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            # No message arrived within the poll timeout
            idle_for = time.time() - last_message_time
            if idle_for >= IDLE_TIMEOUT_S:
                print(f"\nNo messages for {IDLE_TIMEOUT_S}s — exiting.")
                break
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached the end of a partition — not an error, just informational
                print(f"  [info] End of partition {msg.partition()} at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
            continue

        # Successfully received a message — deserialize and print it
        last_message_time = time.time()
        messages_received += 1

        event = deserialize_avro(msg.value())

        print(f"Message #{messages_received}")
        print(f"  Topic/Partition/Offset : {msg.topic()} / {msg.partition()} / {msg.offset()}")
        for field, value in event.items():
            print(f"  {field:<15}: {value}")
        print()

except KeyboardInterrupt:
    print("\nInterrupted by user (Ctrl+C) — shutting down.")

finally:
    # Always close the consumer to commit offsets and release group membership
    consumer.close()
    print(f"Consumer closed. Total messages received: {messages_received}")
```

---

## 6. Run the Producer

In the VSCode terminal (connected to the VM), with the virtual environment active:

```bash
cd ~/kafka-poc
source venv/bin/activate
python3 producer.py
```

**Expected output:**

```
Loading credentials from Secret Manager...
Credentials loaded.
Schema registered/found — ID: 1

Producing 10 messages to topic 'raw-events'...

Delivered: order_id=3f7a1c2e-...  to raw-events [partition=1]  offset=0
Delivered: order_id=9b8d4f0a-...  to raw-events [partition=0]  offset=0
Delivered: order_id=c1e5a2d7-...  to raw-events [partition=2]  offset=0
... (10 lines total)

Flushing producer — waiting for all acks...

All messages delivered successfully.
```

> **If you see connection errors immediately:** Verify the VM's network tag is `dev-vm` and that the Managed Kafka cluster is in the same VPC (`poc-vpc`). Also confirm `vm-producer-sa` has the **Managed Kafka Client** IAM role on the cluster.

---

## 7. Run the Consumer

In the same (or a new) VSCode terminal:

```bash
python3 consumer.py
```

**Expected output:**

```
Loading credentials from Secret Manager...
Credentials loaded.

Subscribed to 'raw-events' as group 'poc-consumer-group'.
Waiting for messages (will exit after 30s of silence)...

------------------------------------------------------------
  [schema cache] Loaded schema ID 1 from registry.
Message #1
  Topic/Partition/Offset : raw-events / 1 / 0
  order_id       : 3f7a1c2e-0b1d-4c3a-8e5f-1a2b3c4d5e6f
  customer_id    : cust-B2
  product_id     : prod-042
  amount         : 149.99
  currency       : USD
  timestamp      : 1716000000000
  status         : SHIPPED

... (10 messages total)

No messages for 30s — exiting.
Consumer closed. Total messages received: 10
```

> **Why `auto_offset_reset = "earliest"`?** When a consumer group (`poc-consumer-group`) has never consumed from a topic before, there is no committed offset to resume from. Setting `earliest` tells the consumer to start from the very beginning of the topic. If you run the consumer a second time, it will resume from where it left off (after offset 9) because the offsets are now committed.

---

## 8. Troubleshooting

**Authentication error connecting to Kafka (`SASL authentication failed`)**
- Verify `vm-producer-sa` has the **Managed Kafka Client** (`roles/managedkafka.client`) IAM role. Go to **IAM & Admin** > **IAM**, search for the service account, and check its roles.
- Confirm the VM is running (not stopped) — the OAuth token endpoint is only available on live VMs.

**Secret not found (`404 NOT_FOUND` from Secret Manager)**
- Secret names are **case-sensitive**. Verify the exact names in **Security** > **Secret Manager**: `kafka-bootstrap-servers`, `schema-registry-url`, `schema-registry-api-key`, `schema-registry-api-secret`.
- Confirm the `vm-producer-sa` service account has the **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`) role.

**Schema Registry 401 Unauthorized**
- The API key or secret stored in Secret Manager is incorrect. Go to your Confluent Cloud account, regenerate the API key for Schema Registry, and update the secrets in GCP Secret Manager (create a new version with the correct value).

**Consumer reads 0 messages**
- Run the producer first and confirm it completed without errors.
- Check `auto.offset.reset` is set to `"earliest"` in the consumer config.
- If you ran the consumer before and it committed offsets, the group has already "seen" all messages. Either run the producer again to produce new messages, or delete the consumer group offset: in the Managed Kafka console, find the consumer group `poc-consumer-group` and reset its offsets to earliest.

**`ModuleNotFoundError` for any import**
- Make sure the virtual environment is activated: `source ~/kafka-poc/venv/bin/activate`. Your prompt should show `(venv)` at the start when active.

---

## 9. What's Next

The end-to-end Kafka pipeline is working: Avro-encoded messages are produced, stored in `raw-events`, and decoded by the consumer. Possible next steps for Phase 2:

- Add a **tokenization step** in the consumer that replaces PII fields before publishing to `enriched-events`
- Deploy a **dead-letter queue** consumer that routes malformed messages to `dlq-events`
- Run the producer as a **scheduled Cloud Run Job** instead of a manual script
