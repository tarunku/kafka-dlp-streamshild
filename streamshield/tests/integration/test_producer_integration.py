"""
Integration test: KafkaProducer against the real GCP Kafka cluster.

Produces 5 prescription orders with DLP tokenization and verifies delivery.
Mirrors the POC's producer.py but uses the SDK's clean API.
"""

import random
import time
import uuid

import pytest

from streamshield import KafkaProducer, MessageMetadata
from tests.integration.conftest import INTEGRATION_TOPIC

# Sample data — same as the POC
OWNER_NAMES  = ["Sarah Mitchell", "James Okafor", "Priya Sharma", "Carlos Reyes", "Emily Chen"]
OWNER_EMAILS = ["sarah@example.com", "james@example.com", "priya@example.com", "carlos@example.com", "emily@example.com"]
OWNER_PHONES = ["+1-555-0142", "+1-555-0287", "+1-555-0395", "+1-555-0411", "+1-555-0523"]
CARD_NUMBERS = ["4111111111111111", "5500005555555559", "340000000000009", "6011000000000004", "3530111333300000"]
PET_NAMES    = ["Biscuit", "Luna", "Max", "Cleo", "Buddy"]
MEDICATIONS  = ["Carprofen 25mg", "Metronidazole 250mg", "Amoxicillin 500mg", "Prednisone 5mg"]


def make_prescription_order() -> dict:
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


class TestKafkaProducerIntegration:
    def test_send_single_prescription_order(self, integration_config):
        """Send one order and verify it is delivered without error."""
        record = make_prescription_order()

        with KafkaProducer(integration_config) as producer:
            meta = producer.send(
                topic = INTEGRATION_TOPIC,
                key   = record["order_id"],
                value = record,
            )

        # MessageMetadata is populated after flush (called on context exit)
        assert meta.topic == INTEGRATION_TOPIC
        print(f"\nDelivered: {record['order_id']} → partition={meta.partition} offset={meta.offset}")

    def test_send_batch_5_orders(self, integration_config):
        """Send 5 orders using send_batch (single DLP call)."""
        records = [make_prescription_order() for _ in range(5)]

        with KafkaProducer(integration_config) as producer:
            results = producer.send_batch(
                topic     = INTEGRATION_TOPIC,
                records   = records,
                key_field = "order_id",
            )

        assert len(results) == 5
        for meta in results:
            assert meta.topic == INTEGRATION_TOPIC
            print(f"  Delivered → partition={meta.partition} offset={meta.offset}")

    def test_tokenization_happens_before_kafka(self, integration_config):
        """
        Verify that sensitive fields are tokenized: the raw_schema returned by a consumer
        should show logicalType='tokenized' fields and the values in Kafka are tokens.

        This test just verifies the producer runs without error — full end-to-end
        tokenization verification is in test_consumer_integration.py.
        """
        record = make_prescription_order()
        plaintext_name = record["owner_name"]

        with KafkaProducer(integration_config) as producer:
            meta = producer.send(
                topic = INTEGRATION_TOPIC,
                key   = record["order_id"],
                value = record,
            )

        # The plaintext name should NOT appear in Kafka (it was tokenized)
        # We verify this by consuming the message in the consumer integration test
        print(f"\nProduced order with plaintext name '{plaintext_name}' — should be tokenized in Kafka")
        assert meta.topic == INTEGRATION_TOPIC
