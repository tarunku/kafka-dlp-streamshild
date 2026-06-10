# consumer_tokenized.py — consumes prescription-events and prints tokens as-is
#
# This consumer intentionally does NOT de-tokenize. It demonstrates what any
# subscriber without Cloud KMS cryptoKeyDecrypter sees: opaque tokens where
# sensitive values should be. No DLP or KMS calls are made.

import io
import json
import struct
import time

import fastavro
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException

from dlp_utils import get_tokenized_fields, is_tokenized_value
from schema import SURROGATE_INFO_TYPE_PII, SURROGATE_INFO_TYPE_PCI, TOPIC
from utils import get_gcp_bearer_token, get_secret, make_kafka_config

# ── Configuration ─────────────────────────────────────────────────────────────

PROJECT_ID      = "vetsource-496203"
CONSUMER_GROUP  = "dlp-tokenized-consumer-group"
IDLE_TIMEOUT_S  = 30

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
    """Returns (decoded_record, raw_schema_dict)."""
    if len(raw_bytes) < 5:
        raise ValueError(f"Message too short: {len(raw_bytes)} bytes")

    magic, schema_id = struct.unpack(">bI", raw_bytes[:5])
    if magic != 0:
        raise ValueError(f"Unexpected magic byte: {magic!r}")

    raw_schema, parsed_schema = get_schemas_by_id(schema_id)
    buf    = io.BytesIO(raw_bytes[5:])
    record = fastavro.schemaless_reader(buf, parsed_schema)
    return record, raw_schema

# ── Kafka Consumer ────────────────────────────────────────────────────────────

consumer = Consumer(make_kafka_config(bootstrap_servers, extra={
    "group.id":           CONSUMER_GROUP,
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": True,
}))
consumer.subscribe([TOPIC])

# ── Poll loop ─────────────────────────────────────────────────────────────────

print(f"Subscribed to '{TOPIC}' as group '{CONSUMER_GROUP}'.")
print(f"Printing TOKENIZED data as-is (no de-tokenization).")
print(f"Waiting for messages (exits after {IDLE_TIMEOUT_S}s of silence)...\n")
print("─" * 60)

messages_received = 0
last_message_time = time.time()

LOCK_ICON   = "🔒"
HASH_ICON   = "🔐"
PLAIN_ICON  = "  "

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

        record, raw_schema = deserialize_avro(msg.value())
        tokenized_field_meta = {
            f["name"]: f for f in get_tokenized_fields(raw_schema)
        }

        print(f"\nMessage #{messages_received}")
        print(f"  Partition / Offset : {msg.partition()} / {msg.offset()}")
        print()

        for field_name, value in record.items():
            meta = tokenized_field_meta.get(field_name)
            if meta is None:
                # Non-sensitive field — print plaintext value
                print(f"  {PLAIN_ICON}  {field_name:<22}: {value}")
                continue

            reversible = meta.get("token.reversible",
                                  raw_schema.get("token.default-reversible", "true")) != "false"

            if is_tokenized_value(str(value), SURROGATE_INFO_TYPE_PII) or \
               is_tokenized_value(str(value), SURROGATE_INFO_TYPE_PCI):
                icon  = LOCK_ICON if reversible else HASH_ICON
                label = "[reversible token]" if reversible else "[irreversible hash]"
                print(f"  {icon}  {field_name:<22}: {str(value)[:40]}…  {label}")
            else:
                # FPE token — no visible prefix, looks like plaintext
                method = meta.get("token.method", "")
                if "Ffx" in method or "Fpe" in method:
                    print(f"  {LOCK_ICON}  {field_name:<22}: {value}  [FPE token — format preserved]")
                else:
                    print(f"  {LOCK_ICON}  {field_name:<22}: {value}  [tokenized]")

        print()

except KeyboardInterrupt:
    print("\nInterrupted (Ctrl+C) — shutting down.")

finally:
    consumer.close()
    print(f"Consumer closed. Total messages received: {messages_received}")
