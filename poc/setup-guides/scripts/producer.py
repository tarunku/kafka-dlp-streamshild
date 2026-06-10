# producer.py — Avro producer for raw-events topic
import io
import json
import random
import struct
import time
import uuid

import fastavro
import google.auth
import google.auth.transport.requests
import requests
from confluent_kafka import Producer

from schema import ORDER_EVENT_SCHEMA
from utils import get_secret

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ID = "vetsource-496203"
TOPIC      = "raw-events"

# ── Load credentials from Secret Manager ─────────────────────────────────────

print("Loading credentials from Secret Manager...")
bootstrap_servers   = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url = get_secret(PROJECT_ID, "schema-registry-url")
# bootstrap_servers     = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
# schema_registry_url   = get_secret(PROJECT_ID, "schema-registry-url")
# schema_registry_key   = get_secret(PROJECT_ID, "schema-registry-api-key")
# schema_registry_secret = get_secret(PROJECT_ID, "schema-registry-api-secret")
# schema_registry_key    = get_secret(PROJECT_ID, "schema-registry-api-key")
# schema_registry_secret = get_secret(PROJECT_ID, "schema-registry-api-secret")
print("Credentials loaded.")

# ── Register / fetch schema from Confluent Schema Registry ───────────────────
def get_gcp_bearer_token() -> str:
    """Fetch a short-lived OAuth2 access token via google-auth."""
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def get_or_register_schema(
    registry_url: str,
    subject: str,
    schema: dict,
) -> int:
    """
    Registers the schema under the given subject if it does not exist yet.
    Returns the schema ID assigned by Schema Registry.
    """
    token = get_gcp_bearer_token()
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
    payload = {"schema": json.dumps(schema), "schemaType": "AVRO"}
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.schemaregistry.v1+json",
        },
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
# Google Managed Kafka validates SASL PLAIN credentials via its authenticateConnection
# API. The username must match the service account email embedded in the token.

_gcp_creds, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
_gcp_creds.refresh(google.auth.transport.requests.Request())

producer_config = {
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     _gcp_creds.service_account_email,
    "sasl.password":     _gcp_creds.token,
    "error_cb":          lambda e: print(f"[KAFKA ERROR] {e}"),
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
        on_delivery=delivery_report,
    )

    # Trigger delivery for callbacks (non-blocking — does not wait for ack)
    producer.poll(0)

    # Small delay so timestamps differ slightly across messages
    time.sleep(0.1)

# Wait for all outstanding messages to be acknowledged before exiting
# print("\nFlushing producer — waiting for all acks...")
# producer.flush()
# print("\nAll messages delivered successfully.")

# Change the last two lines temporarily:
print("\nFlushing producer — waiting for all acks...")
producer.flush(timeout=30)   # ← add timeout=30
print("\nDone.")
