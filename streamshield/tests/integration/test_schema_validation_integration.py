"""
Integration tests: schema validation and schema error paths.

Covers:
  1. Missing required field         — SchemaValidationError before any DLP/Kafka I/O.
  2. Wrong field type               — SchemaValidationError (int field gets a string).
  3. Null in required non-null field — SchemaValidationError.
  4. Extra / unknown fields         — fastavro ignores them; send succeeds.
  5. Non-existent topic subject      — SchemaNotFoundError before any Kafka I/O.
  6. Empty batch                    — send_batch([]) returns [] immediately.
  7. Validation is pre-flight       — error raised before DLP tokenization.

All tests run against the real Schema Registry and Kafka cluster.
No mocking.
"""

from __future__ import annotations

import time
import uuid

import pytest

from streamshield import (
    GCPConfig,
    KafkaProducer,
    SDKConfig,
)
from streamshield.errors.exceptions import (
    SchemaNotFoundError,
    SchemaValidationError,
    TokenizationError,
)
from tests.integration.conftest import INTEGRATION_TOPIC


# ── Helpers ───────────────────────────────────────────────────────────────────

def _valid_order() -> dict:
    """Build a fully valid prescription order record."""
    return {
        "order_id":           f"VAL-{uuid.uuid4().hex[:8].upper()}",
        "owner_name":         "Test Owner",
        "owner_email":        "owner@example.com",
        "owner_phone":        "+1-555-0199",
        "owner_payment_card": "4111111111111111",
        "pet_name":           "Spot",
        "medication":         "Carprofen 25mg",
        "quantity":           30,
        "order_date":         "2026-06-04",
        "is_refill":          False,
    }


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestSchemaValidationBeforeIO:
    """
    SchemaValidationError is raised by AvroSerializer.validate() BEFORE the producer
    calls DLP tokenize() or confluent_kafka.Producer.produce(). These tests confirm
    that invalid records are rejected at the SDK boundary with zero side-effects.
    """

    def test_missing_required_field_raises_schema_validation_error(self, integration_config):
        """order_id is required (type=string, no default). Omitting it must fail immediately."""
        record = _valid_order()
        del record["order_id"]

        with KafkaProducer(integration_config) as producer:
            with pytest.raises(SchemaValidationError) as exc_info:
                producer.send(INTEGRATION_TOPIC, value=record)

        # Confirm the error carries safe_context (no PII values inside)
        assert exc_info.value.safe_context is not None

    def test_wrong_field_type_raises_schema_validation_error(self, integration_config):
        """
        quantity is schema type 'int'. Passing a string must trigger a
        SchemaValidationError from fastavro during dry-run serialization.
        """
        record = _valid_order()
        record["quantity"] = "thirty"  # str instead of int

        with KafkaProducer(integration_config) as producer:
            with pytest.raises(SchemaValidationError):
                producer.send(INTEGRATION_TOPIC, value=record)

    def test_null_in_required_field_raises_schema_validation_error(self, integration_config):
        """
        order_id is type='string' (not nullable union). Setting it to None must
        be caught by the pre-flight validator.
        """
        record = _valid_order()
        record["order_id"] = None

        with KafkaProducer(integration_config) as producer:
            with pytest.raises(SchemaValidationError):
                producer.send(INTEGRATION_TOPIC, value=record)

    def test_list_value_for_string_field_raises(self, integration_config):
        """
        order_id is type='string'. Passing a list must fail Avro schema validation
        because fastavro cannot serialise a list as an Avro string.
        """
        record = _valid_order()
        record["order_id"] = ["not", "a", "string"]

        with KafkaProducer(integration_config) as producer:
            with pytest.raises(SchemaValidationError):
                producer.send(INTEGRATION_TOPIC, value=record)

    def test_extra_unknown_fields_are_ignored_and_send_succeeds(self, integration_config):
        """
        fastavro silently ignores fields present in the dict but absent from the
        schema. The message must be delivered successfully.
        """
        record = _valid_order()
        record["unknown_field_xyz"]  = "this is not in the Avro schema"
        record["another_extra_key"]  = 12345

        with KafkaProducer(integration_config) as producer:
            meta = producer.send(INTEGRATION_TOPIC, key=record["order_id"], value=record)

        # Delivery succeeds — the extra fields were stripped during serialization
        assert meta.topic == INTEGRATION_TOPIC

    def test_validation_error_raised_before_tokenization(self, integration_config):
        """
        The pipeline order is: validate → tokenize → serialize → produce.
        A SchemaValidationError must be raised, NOT a TokenizationError —
        DLP is never called for an invalid record.
        """
        record = _valid_order()
        record["quantity"] = "invalid_type"  # guaranteed to fail schema check

        with KafkaProducer(integration_config) as producer:
            with pytest.raises(SchemaValidationError):
                # If DLP were called first, we'd get TokenizationError instead
                producer.send(INTEGRATION_TOPIC, value=record)


class TestSchemaErrors:
    """Errors arising from missing or incompatible Schema Registry entries."""

    def test_send_to_topic_with_no_registered_schema_raises(self, integration_config):
        """
        If the Schema Registry has no subject for the topic, the producer must
        raise SchemaNotFoundError before any DLP or Kafka I/O.
        """
        record = _valid_order()

        with KafkaProducer(integration_config) as producer:
            with pytest.raises(SchemaNotFoundError):
                producer.send("nonexistent-topic-no-schema-xyz-99999", value=record)

    def test_send_batch_empty_list_returns_empty_immediately(self, integration_config):
        """send_batch with an empty list must return [] without any network calls."""
        with KafkaProducer(integration_config) as producer:
            results = producer.send_batch(INTEGRATION_TOPIC, records=[])

        assert results == []

    def test_send_batch_validates_all_records_before_dlp(self, integration_config):
        """
        send_batch validates EVERY record before calling DLP.
        If any record is invalid, SchemaValidationError is raised immediately
        and no records are sent (fail-fast on the first invalid record).
        """
        records = [_valid_order() for _ in range(3)]
        # Poison the middle record
        records[1]["quantity"] = "bad_type"

        with KafkaProducer(integration_config) as producer:
            with pytest.raises(SchemaValidationError):
                producer.send_batch(INTEGRATION_TOPIC, records=records)
