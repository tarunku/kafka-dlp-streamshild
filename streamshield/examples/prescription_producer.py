"""
StreamShield Example: Prescription order producer with DLP tokenization.

Produces 5 prescription orders to the 'prescription-events' topic.
Sensitive fields (owner_name, owner_email, owner_phone, owner_payment_card,
pet_name) are tokenized via Cloud DLP before they reach the Kafka broker.

This replaces the POC's producer.py with the StreamShield SDK's clean API.

Prerequisites:
  - GCE VM with vm-producer-sa attached (ADC available), OR
  - GOOGLE_APPLICATION_CREDENTIALS set to a service account key file
  - Kafka topic 'prescription-events' created (3 partitions)
  - Schema registered: python3 examples/register_schema.py

Run:
    cd kafka-poc/streamshield
    pip install -e .
    python3 examples/prescription_producer.py
"""

import json
import logging
import random
import time
import uuid

from streamshield import GCPConfig, KafkaProducer, SDKConfig, configure_json_logging, configure_logging_metrics

# ── Logging setup ──────────────────────────────────────────────────────────────
configure_json_logging(level=logging.INFO)

# Add a file handler with the same JSON format
class _JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {"level": record.levelname, "logger": record.name, "message": record.getMessage()}
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)

file_handler = logging.FileHandler("streamshield.log")
file_handler.setFormatter(_JsonFormatter())
logging.getLogger("streamshield").addHandler(file_handler)

# ── Metrics setup ──────────────────────────────────────────────────────────────
# Routes OTel metrics through the Python logger — snapshots land in streamshield.log.
# Requires: pip install 'streamshield[metrics]'
configure_logging_metrics(export_interval_ms=10_000)

# ── Configuration ──────────────────────────────────────────────────────────────
# All secrets (bootstrap servers, schema registry URL, DLP keys) are loaded
# automatically from GCP Secret Manager using the vm-producer-sa service account.
config = SDKConfig(
    gcp=GCPConfig(
        project_id="terraform-testing-498903",
        use_secret_manager=True,
        bootstrap_servers_secret="kafka-bootstrap-servers",
        schema_registry_url_secret="schema-registry-url",
    )
)

TOPIC         = "prescription-events"
MESSAGE_COUNT = 5

# ── Sample data (same as POC) ─────────────────────────────────────────────────
OWNER_NAMES  = ["Sarah Mitchell", "James Okafor", "Priya Sharma", "Carlos Reyes", "Emily Chen"]
OWNER_EMAILS = ["sarah@example.com", "james@example.com", "priya@example.com", "carlos@example.com", "emily@example.com"]
OWNER_PHONES = ["+1-555-0142", "+1-555-0287", "+1-555-0395", "+1-555-0411", "+1-555-0523"]
CARD_NUMBERS = ["4111111111111111", "5500005555555559", "340000000000009", "6011000000000004", "3530111333300000"]
PET_NAMES    = ["Biscuit", "Luna", "Max", "Cleo", "Buddy"]
MEDICATIONS  = ["Carprofen 25mg", "Metronidazole 250mg", "Amoxicillin 500mg", "Prednisone 5mg"]


def make_prescription_order() -> dict:
    """Generate one fake prescription order with plaintext sensitive values."""
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


# ── Produce messages ───────────────────────────────────────────────────────────
print(f"\nProducing {MESSAGE_COUNT} prescription orders to '{TOPIC}'...")
print("─" * 60)

# The context manager automatically calls flush() on exit,
# ensuring all messages are acknowledged before the script ends.
with KafkaProducer(config) as producer:
    for i in range(MESSAGE_COUNT):
        order = make_prescription_order()

        print(f"\nMessage {i + 1}: {order['order_id']}")
        print(f"  Plaintext  owner_name  : {order['owner_name']}")
        print(f"  Plaintext  owner_email : {order['owner_email']}")
        print(f"  Plaintext  card        : {order['owner_payment_card']}")

        # send() handles: schema fetch → DLP tokenization → Avro serialize → Kafka produce
        # The plaintext values never leave this process.
        meta = producer.send(
            topic          = TOPIC,
            key            = order["order_id"],
            value          = order,
            schema_version = 1,   # pins to version 1; hits registry once, then _by_version cache
        )

        print(f"  Queued: topic={meta.topic}")
        time.sleep(0.1)  # small delay between messages

# flush() called automatically by context manager __exit__
print(f"\n{'─' * 60}")
print(f"All {MESSAGE_COUNT} messages delivered.")
