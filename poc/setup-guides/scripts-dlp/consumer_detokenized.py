# consumer_detokenized.py — consumes prescription-events and de-tokenizes sensitive fields
#
# Flow per message:
#   1. Deserialize Avro — all sensitive fields are still tokens
#   2. Extract tokenization metadata from the embedded schema (kms-key, wrapped-dek, methods)
#   3. Call Cloud DLP reidentifyContent — reversible tokens replaced with original values
#   4. Print the reconstructed record with original plaintext values
#
# Authorization is enforced by Cloud KMS IAM: callers without
# roles/cloudkms.cryptoKeyDecrypter on both domain keys receive PermissionDenied
# from DLP before any de-tokenization occurs.
#
# Hash-based fields (owner_phone) are left as-is — they cannot be reversed.

import io
import json
import struct
import time

import fastavro
import requests
from google.cloud import dlp_v2
from confluent_kafka import Consumer, KafkaError, KafkaException

from dlp_utils import detokenize_record, get_tokenized_fields
from schema import TOPIC
from utils import get_gcp_bearer_token, get_secret, make_kafka_config

# ── Configuration ─────────────────────────────────────────────────────────────

PROJECT_ID     = "vetsource-496203"
CONSUMER_GROUP = "dlp-detokenized-consumer-group"
IDLE_TIMEOUT_S = 30

# ── Load credentials ──────────────────────────────────────────────────────────

print("Loading credentials from Secret Manager...")
bootstrap_servers   = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url = get_secret(PROJECT_ID, "schema-registry-url")
print("Credentials loaded.\n")

# ── Schema cache ──────────────────────────────────────────────────────────────

_raw_schema_cache:    dict[int, dict]   = {}
_parsed_schema_cache: dict[int, object] = {}


def get_schemas_by_id(schema_id: int) -> tuple[dict, object]:
    """Returns (raw_schema_dict, parsed_fastavro_schema) for a given schema ID."""
    if schema_id in _parsed_schema_cache:
        return _raw_schema_cache[schema_id], _parsed_schema_cache[schema_id]

    token    = get_gcp_bearer_token()
    url      = f"{schema_registry_url.rstrip('/')}/schemas/ids/{schema_id}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()

    raw_schema    = json.loads(response.json()["schema"])
    parsed_schema = fastavro.parse_schema(raw_schema)

    _raw_schema_cache[schema_id]    = raw_schema
    _parsed_schema_cache[schema_id] = parsed_schema
    print(f"  [schema cache] Loaded schema ID {schema_id}")
    return raw_schema, parsed_schema

# ── Confluent wire-format deserializer ────────────────────────────────────────

def deserialize_avro(raw_bytes: bytes) -> tuple[dict, dict]:
    """Returns (decoded_record_with_tokens, raw_schema_dict)."""
    if len(raw_bytes) < 5:
        raise ValueError(f"Message too short: {len(raw_bytes)} bytes")

    magic, schema_id = struct.unpack(">bI", raw_bytes[:5])
    if magic != 0:
        raise ValueError(f"Unexpected magic byte: {magic!r}")

    raw_schema, parsed_schema = get_schemas_by_id(schema_id)
    buf    = io.BytesIO(raw_bytes[5:])
    record = fastavro.schemaless_reader(buf, parsed_schema)
    return record, raw_schema

# ── DLP client ────────────────────────────────────────────────────────────────

dlp_client = dlp_v2.DlpServiceClient()

# ── Kafka Consumer ────────────────────────────────────────────────────────────

consumer = Consumer(make_kafka_config(bootstrap_servers, extra={
    "group.id":           CONSUMER_GROUP,
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": True,
}))
consumer.subscribe([TOPIC])

# ── Poll loop ─────────────────────────────────────────────────────────────────

print(f"Subscribed to '{TOPIC}' as group '{CONSUMER_GROUP}'.")
print(f"De-tokenizing sensitive fields via Cloud DLP.")
print(f"Waiting for messages (exits after {IDLE_TIMEOUT_S}s of silence)...\n")
print("─" * 60)

messages_received = 0
last_message_time = time.time()

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            if time.time() - last_message_time >= IDLE_TIMEOUT_S:
                print(f"\nNo messages for {IDLE_TIMEOUT_S}s — exiting.")
                break
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"  [info] End of partition {msg.partition()} at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
            continue

        last_message_time  = time.time()
        messages_received += 1

        tokenized_record, raw_schema = deserialize_avro(msg.value())

        # De-tokenize — DLP reverses all reversible tokens; hashes are left as-is
        try:
            plain_record = detokenize_record(
                dlp_client       = dlp_client,
                project_id       = PROJECT_ID,
                tokenized_record = tokenized_record,
                raw_schema       = raw_schema,
            )
        except Exception as exc:
            print(f"\n  [DLP ERROR] De-tokenization failed: {exc}")
            print("  Printing tokenized record instead.\n")
            plain_record = tokenized_record

        # ── Build field metadata lookup for display labels ────────────────────
        tokenized_field_meta = {
            f["name"]: f for f in get_tokenized_fields(raw_schema)
        }

        print(f"\nMessage #{messages_received}")
        print(f"  Partition / Offset : {msg.partition()} / {msg.offset()}")
        print()
        print(f"  {'Field':<22}  {'Value':<40}  Note")
        print(f"  {'─' * 22}  {'─' * 40}  {'─' * 20}")

        for field_name, value in plain_record.items():
            meta = tokenized_field_meta.get(field_name)
            if meta is None:
                print(f"  {field_name:<22}  {str(value):<40}")
                continue

            reversible = meta.get(
                "token.reversible",
                raw_schema.get("token.default-reversible", "true")
            ) != "false"
            method     = meta.get("token.method", "")
            sensitivity = meta.get("token.sensitivity",
                                   raw_schema.get("token.default-sensitivity", "PII"))

            if reversible:
                note = f"[de-tokenized — {sensitivity}]"
            else:
                note = "[hash — irreversible, original unrecoverable]"

            print(f"  {field_name:<22}  {str(value):<40}  {note}")

        print()

except KeyboardInterrupt:
    print("\nInterrupted (Ctrl+C) — shutting down.")

finally:
    consumer.close()
    print(f"Consumer closed. Total messages received: {messages_received}")
