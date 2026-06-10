# consumer.py — Avro consumer for raw-events topic
import io
import json
import struct
import time

import fastavro
import google.auth
import google.auth.transport.requests
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

from schema import ORDER_EVENT_SCHEMA
from utils import get_secret

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ID      = "vetsource-496203"
TOPIC           = "raw-events"
CONSUMER_GROUP  = "poc-consumer-group"
IDLE_TIMEOUT_S  = 30   # exit after this many seconds with no new messages

# ── Load credentials from Secret Manager ─────────────────────────────────────

print("Loading credentials from Secret Manager...")
bootstrap_servers   = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url = get_secret(PROJECT_ID, "schema-registry-url")
print("Credentials loaded.\n")

# ── Schema cache (avoid repeated HTTP calls for the same schema ID) ──────────

_schema_cache: dict[int, object] = {}

def get_gcp_bearer_token() -> str:
    """Fetch a short-lived OAuth2 access token via google-auth."""
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def get_schema_by_id(schema_id: int) -> object:
    """
    Fetches the Avro schema for the given schema ID from Schema Registry.
    Results are cached in memory for the lifetime of this process.
    """
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]

    token = get_gcp_bearer_token()
    url = f"{schema_registry_url.rstrip('/')}/schemas/ids/{schema_id}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
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
# Google Managed Kafka validates SASL PLAIN credentials via its authenticateConnection
# API. The username must match the service account email embedded in the token.

_gcp_creds, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
_gcp_creds.refresh(google.auth.transport.requests.Request())

consumer_config = {
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     _gcp_creds.service_account_email,
    "sasl.password":     _gcp_creds.token,
    "group.id":          CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "error_cb":          lambda e: print(f"[KAFKA ERROR] {e}"),
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