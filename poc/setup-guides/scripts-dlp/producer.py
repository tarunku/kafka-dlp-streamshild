# producer.py — tokenizing Avro producer for prescription-events topic
#
# Flow per message:
#   1. Generate a fake PrescriptionOrder with plaintext sensitive fields
#   2. Call Cloud DLP deidentifyContent — sensitive fields replaced with tokens
#   3. Serialize the tokenized record using the Confluent wire format (Avro)
#   4. Publish to Kafka
#
# The original plaintext values never leave this process.

import io
import json
import random
import struct
import time
import uuid

import fastavro
import requests
from google.cloud import dlp_v2
from confluent_kafka import Producer

from dlp_utils import tokenize_record
from schema import TOPIC, build_prescription_schema
from utils import get_gcp_bearer_token, get_secret, make_kafka_config

# ── Configuration ─────────────────────────────────────────────────────────────

PROJECT_ID     = "vetsource-496203"
SUBJECT        = f"{TOPIC}-value"
MESSAGE_COUNT  = 5

# ── Load credentials and key material ────────────────────────────────────────

print("Loading credentials from Secret Manager...")
bootstrap_servers   = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url = get_secret(PROJECT_ID, "schema-registry-url")
pii_kms_key_name    = get_secret(PROJECT_ID, "dlp-kms-pii-key-name")
pci_kms_key_name    = get_secret(PROJECT_ID, "dlp-kms-pci-key-name")
pii_wrapped_dek     = get_secret(PROJECT_ID, "dlp-pii-wrapped-dek")
pci_wrapped_dek     = get_secret(PROJECT_ID, "dlp-pci-wrapped-dek")
print("Credentials loaded.")

# ── Fetch registered schema from Schema Registry ──────────────────────────────

def fetch_schema(registry_url: str, subject: str) -> tuple[dict, int]:
    """
    Fetches the latest schema version for a subject.
    Returns (raw_schema_dict, schema_id).
    raw_schema_dict preserves all token.* metadata — required for DLP calls.
    """
    token    = get_gcp_bearer_token()
    url      = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()
    body       = response.json()
    schema_id  = body["id"]
    raw_schema = json.loads(body["schema"])
    print(f"Schema fetched from registry — subject: {subject}, ID: {schema_id}")
    return raw_schema, schema_id


raw_schema, schema_id = fetch_schema(schema_registry_url, SUBJECT)
parsed_schema = fastavro.parse_schema(raw_schema)

# ── Confluent wire-format serializer ─────────────────────────────────────────

def serialize_avro(record: dict, schema, sid: int) -> bytes:
    """
    Confluent wire format: magic byte (0x00) + 4-byte big-endian schema ID + Avro bytes.
    """
    buf = io.BytesIO()
    buf.write(struct.pack(">bI", 0, sid))
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()

# ── Delivery report callback ──────────────────────────────────────────────────

def delivery_report(err, msg):
    if err is not None:
        print(f"  Delivery FAILED: {err}")
    else:
        print(
            f"  Delivered: order_id={msg.key().decode()}"
            f"  → {msg.topic()} [partition={msg.partition()}, offset={msg.offset()}]"
        )

# ── Kafka Producer ────────────────────────────────────────────────────────────

producer = Producer(make_kafka_config(bootstrap_servers))

# ── DLP client ───────────────────────────────────────────────────────────────

dlp_client = dlp_v2.DlpServiceClient()

# ── Sample data helpers ───────────────────────────────────────────────────────

OWNER_NAMES  = ["Sarah Mitchell", "James Okafor", "Priya Sharma", "Carlos Reyes", "Emily Chen"]
OWNER_EMAILS = ["sarah@example.com", "james@example.com", "priya@example.com", "carlos@example.com", "emily@example.com"]
OWNER_PHONES = ["+1-555-0142", "+1-555-0287", "+1-555-0395", "+1-555-0411", "+1-555-0523"]
CARD_NUMBERS = ["4111111111111111", "5500005555555559", "340000000000009", "6011000000000004", "3530111333300000"]
PET_NAMES    = ["Biscuit", "Luna", "Max", "Cleo", "Buddy"]
MEDICATIONS  = ["Carprofen 25mg", "Metronidazole 250mg", "Amoxicillin 500mg", "Prednisone 5mg"]


def make_prescription_order() -> dict:
    """Generates one fake PrescriptionOrder with plaintext sensitive values."""
    idx = random.randrange(len(OWNER_NAMES))
    return {
        "order_id":           f"RX-{uuid.uuid4().hex[:8].upper()}",
        "owner_name":         OWNER_NAMES[idx],
        "owner_email":        OWNER_EMAILS[idx],
        "owner_phone":        OWNER_PHONES[idx],
        "owner_payment_card": CARD_NUMBERS[idx],
        "pet_name":           random.choice(PET_NAMES),
        "medication":         random.choice(MEDICATIONS),
        "quantity":           random.choice([15, 30, 60, 90]),
        "order_date":         time.strftime("%Y-%m-%d"),
        "is_refill":          random.choice([True, False]),
    }

# ── Produce messages ──────────────────────────────────────────────────────────

print(f"\nProducing {MESSAGE_COUNT} messages to topic '{TOPIC}'...\n")
print(f"{'─' * 60}")

for i in range(MESSAGE_COUNT):
    plaintext_record = make_prescription_order()

    print(f"\nMessage {i + 1}: order_id={plaintext_record['order_id']}")
    print(f"  Plaintext  owner_name  : {plaintext_record['owner_name']}")
    print(f"  Plaintext  owner_email : {plaintext_record['owner_email']}")
    print(f"  Plaintext  owner_phone : {plaintext_record['owner_phone']}")
    print(f"  Plaintext  card        : {plaintext_record['owner_payment_card']}")
    print(f"  Plaintext  pet_name    : {plaintext_record['pet_name']}")

    # Tokenize — plaintext values are replaced with DLP tokens in-memory
    tokenized_record = tokenize_record(
        dlp_client  = dlp_client,
        project_id  = PROJECT_ID,
        record      = plaintext_record,
        raw_schema  = raw_schema,
    )

    print(f"  Tokenized  owner_name  : {tokenized_record['owner_name'][:40]}...")
    print(f"  Tokenized  owner_phone : {tokenized_record['owner_phone'][:40]}...")
    print(f"  Tokenized  card        : {tokenized_record['owner_payment_card']}  ← format-preserved")

    serialized = serialize_avro(tokenized_record, parsed_schema, schema_id)

    producer.produce(
        topic       = TOPIC,
        value       = serialized,
        key         = tokenized_record["order_id"].encode("utf-8"),
        on_delivery = delivery_report,
    )
    producer.poll(0)
    time.sleep(0.2)

print(f"\n{'─' * 60}")
print("\nFlushing producer — waiting for all acks...")
producer.flush(timeout=30)
print("All messages delivered.")
