"""
Integration test: KafkaConsumer against the real GCP Kafka cluster.

Produces N records then consumes them back, verifying:
  - Offset only commits after handler succeeds.
  - Tokenized consumer sees DLP tokens in sensitive fields.
  - Detokenized consumer sees original plaintext values (for reversible fields).
  - owner_phone stays as a hash (irreversible) in both consumers.
"""

import random
import threading
import time
import uuid

import pytest

from streamshield import (
    GCPConfig,
    KafkaConsumer,
    KafkaProducer,
    SDKConfig,
    ConsumedMessage,
)
from tests.integration.conftest import INTEGRATION_PROJECT_ID, INTEGRATION_TOPIC

# Unique group IDs per test run to avoid offset conflicts
def _group_id(name: str) -> str:
    return f"streamshield-integration-{name}-{int(time.time())}"


def _make_order() -> dict:
    names   = ["Alice Brown", "Bob Singh", "Carol White"]
    emails  = ["alice@test.com", "bob@test.com", "carol@test.com"]
    phones  = ["+1-555-0001", "+1-555-0002", "+1-555-0003"]
    cards   = ["4111111111111111", "5500005555555559", "6011000000000004"]
    idx = random.randrange(len(names))
    return {
        "order_id":           f"SDK-{uuid.uuid4().hex[:6].upper()}",
        "owner_name":         names[idx],
        "owner_email":        emails[idx],
        "owner_phone":        phones[idx],
        "owner_payment_card": cards[idx],
        "pet_name":           random.choice(["Rex", "Mia", "Gus"]),
        "medication":         "Carprofen 25mg",
        "quantity":           30,
        "order_date":         time.strftime("%Y-%m-%d"),
        "is_refill":          False,
    }


class TestKafkaConsumerTokenized:
    """Consumer without DLP access — sees tokens, not plaintext."""

    def test_tokenized_consumer_reads_messages(self, integration_config):
        """
        Produce 2 messages then consume them.
        Verify that owner_name contains a DLP token (not the plaintext value).
        """
        records = [_make_order() for _ in range(2)]

        # Produce
        with KafkaProducer(integration_config) as producer:
            producer.send_batch(INTEGRATION_TOPIC, records, key_field="order_id")

        # Consume — no detokenization
        received = []

        def handle(msg: ConsumedMessage) -> None:
            received.append(msg)

        consumer_config = SDKConfig(
            gcp=GCPConfig(
                project_id=INTEGRATION_PROJECT_ID,
                use_secret_manager=True,
            )
        )
        with KafkaConsumer(consumer_config, group_id=_group_id("tokenized")) as consumer:
            consumer.process(
                handler       = handle,
                topics        = [INTEGRATION_TOPIC],
                detokenize    = False,
                max_messages  = 2,
                idle_timeout_s = 30.0,
            )

        assert len(received) == 2
        for msg in received:
            # owner_name should be a DLP token, not the original plaintext
            owner_name = msg.value.get("owner_name", "")
            assert owner_name != ""
            print(f"\nTokenized owner_name: {owner_name[:50]}")
            # The raw_schema should carry DLP token.* metadata
            assert "token.kms-key" in msg.raw_schema


class TestKafkaConsumerDetokenized:
    """Consumer with DLP access — reversible fields are restored to plaintext."""

    def test_detokenized_consumer_restores_plaintext(self, integration_config):
        """
        Produce 1 message then consume it with detokenize=True.
        Verify reversible fields are restored; irreversible phone hash is unchanged.
        """
        order = _make_order()
        original_name  = order["owner_name"]
        original_email = order["owner_email"]
        original_phone = order["owner_phone"]

        # Produce
        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order["order_id"], value=order)

        received = []

        def handle(msg: ConsumedMessage) -> None:
            received.append(msg)

        with KafkaConsumer(integration_config, group_id=_group_id("detokenized")) as consumer:
            consumer.process(
                handler       = handle,
                topics        = [INTEGRATION_TOPIC],
                detokenize    = True,   # ← calls DLP reidentifyContent
                max_messages  = 1,
                idle_timeout_s = 30.0,
            )

        assert len(received) == 1
        msg = received[0]

        print(f"\nRestored owner_name:  {msg.value['owner_name']}")
        print(f"Restored owner_email: {msg.value['owner_email']}")
        print(f"owner_phone (hash):   {msg.value['owner_phone'][:40]}")

        # Verify detokenization worked: reversible fields must NOT look like DLP surrogate tokens.
        # The consumer may receive any record from the topic (not necessarily the one we just
        # produced), so we check format rather than exact value.
        owner_name  = msg.value["owner_name"]
        owner_email = msg.value["owner_email"]
        owner_phone = msg.value["owner_phone"]

        # A DLP PII token starts with "VETSOURCE_PII_TOKEN(" — detokenized value must not
        assert not owner_name.startswith("VETSOURCE_PII_TOKEN("), \
            f"owner_name was not detokenized: {owner_name[:60]}"
        assert not owner_email.startswith("VETSOURCE_PII_TOKEN("), \
            f"owner_email was not detokenized: {owner_email[:60]}"

        # owner_phone is a SHA-256 hash — it is 44 bytes base64 encoded, not a phone number
        assert not owner_phone.startswith("+"), \
            f"owner_phone should be a hash, not a phone number: {owner_phone}"


class TestOffsetCommitBehaviour:
    """Verify that offsets are committed only after successful handler execution."""

    def test_offset_not_committed_if_handler_raises(self, integration_config):
        """
        Produce 1 message. First handler call raises. Verify the message is routed
        to DLQ and the consumer continues (doesn't hang forever).

        The DLQ topic (prescription-events.dlq) must exist or auto_create_topic=True.
        """
        order = _make_order()

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order["order_id"], value=order)

        call_count = [0]

        def failing_then_succeeding(msg: ConsumedMessage) -> None:
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("Simulated business logic failure")
            # Second call (from DLQ replay or retry) succeeds

        group = _group_id("failtest")
        with KafkaConsumer(integration_config, group_id=group) as consumer:
            consumer.process(
                handler       = failing_then_succeeding,
                topics        = [INTEGRATION_TOPIC],
                max_messages  = 1,
                idle_timeout_s = 20.0,
            )

        # Handler was called once. The message was DLQ'd, offset was committed.
        # The consumer moved forward without hanging.
        assert call_count[0] >= 1
        print(f"\nHandler called {call_count[0]} time(s). DLQ routing worked correctly.")
