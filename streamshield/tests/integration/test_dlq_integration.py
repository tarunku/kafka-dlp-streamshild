"""
Integration tests: Dead Letter Queue (DLQ) routing.

Covers every path that routes a message to the DLQ:
  1. Business logic failure  — handler raises an exception.
  2. Deserialization failure — raw bytes that are too short.
  3. Wrong magic byte        — Confluent wire format violated.
  4. Corrupted Avro body     — valid header, garbage payload.
  5. Unknown schema_id       — schema not in registry.
  6. Multiple failures       — all route independently.
  7. DLQ message format      — every required field present and correct.
  8. DLQ values are JSON     — raw consumer can parse DLQ payloads.

All tests run against the real GCP environment (terraform-testing-498903).
No mocking.

DLQ strategy:
  - A raw consumer is positioned at the END of the DLQ topic BEFORE each test
    produces anything.  Only messages produced during the test are visible.
  - Each test uses a unique key so DLQ records can be identified unambiguously
    even if concurrent test runs produce other DLQ messages.
"""

from __future__ import annotations

import json
import struct
import time
import uuid
from typing import Callable

import pytest
from confluent_kafka import Consumer as ConfluentConsumer, KafkaError

from streamshield import (
    ConsumedMessage,
    GCPConfig,
    KafkaConsumer,
    KafkaProducer,
    SDKConfig,
)
from streamshield.consumer.dlq import DLQRouter
from tests.integration.conftest import (
    DLQ_TOPIC,
    INTEGRATION_PROJECT_ID,
    INTEGRATION_TOPIC,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _group_id(label: str) -> str:
    return f"streamshield-dlq-test-{label}-{int(time.time())}"


def _make_order(order_id: str | None = None) -> dict:
    oid = order_id or f"DLQ-{uuid.uuid4().hex[:8].upper()}"
    return {
        "order_id":           oid,
        "owner_name":         "Test User",
        "owner_email":        "test@example.com",
        "owner_phone":        "+1-555-0100",
        "owner_payment_card": "4111111111111111",
        "pet_name":           "Buddy",
        "medication":         "Carprofen 25mg",
        "quantity":           30,
        "order_date":         "2026-06-04",
        "is_refill":          False,
    }


def _latest_dlq_consumer(raw_consumer_factory, group_id: str) -> ConfluentConsumer:
    """
    Create a raw consumer on the DLQ topic starting from the current end.
    Blocks until partition assignment is confirmed so the starting offset is
    locked in before any DLQ messages are produced by the test.
    """
    consumer = raw_consumer_factory(group_id, auto_offset_reset="latest")
    consumer.subscribe([DLQ_TOPIC])
    deadline = time.time() + 20.0
    while time.time() < deadline:
        consumer.poll(timeout=1.0)
        if consumer.assignment():
            break
    return consumer


def _poll_dlq_for_key(
    consumer: ConfluentConsumer,
    source_key: str,
    timeout_s: float = 30.0,
) -> dict | None:
    """
    Poll the DLQ consumer until a record with source_key == source_key is found.
    Returns the parsed DLQ record dict, or None if not found within timeout_s.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            continue
        try:
            data = json.loads(msg.value().decode("utf-8"))
            if data.get("source_key") == source_key:
                return data
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
    return None


def _selective_fail_handler(target_key: str) -> Callable[[ConsumedMessage], None]:
    """Handler that raises only when msg.key matches target_key; passes all others."""
    def handler(msg: ConsumedMessage) -> None:
        key = msg.key.decode("utf-8", errors="replace") if msg.key else ""
        if key == target_key:
            raise RuntimeError(f"Intentional test failure for key={target_key}")
    return handler


def _run_source_consumer(integration_config: SDKConfig, group_label: str, handler: Callable, idle_timeout_s: float = 10.0) -> None:
    """Run the SDK consumer with earliest offset reset and a short idle timeout."""
    config = SDKConfig(
        gcp=integration_config.gcp,
        dlp=integration_config.dlp,
        dlq=integration_config.dlq,
        schema=integration_config.schema,
        consumer=integration_config.consumer.__class__(
            auto_offset_reset="earliest",
            idle_timeout_s=idle_timeout_s,
        ),
    )
    with KafkaConsumer(config, group_id=_group_id(group_label)) as consumer:
        consumer.process(
            handler=handler,
            topics=[INTEGRATION_TOPIC],
            detokenize=False,
            idle_timeout_s=idle_timeout_s,
        )


# ── Test classes ──────────────────────────────────────────────────────────────

class TestDLQBusinessFailure:
    """Handler exceptions route to DLQ with reason='business'."""

    def test_business_failure_routes_to_dlq(self, integration_config, raw_consumer_factory):
        """A handler that raises must route the message to DLQ."""
        unique_key = f"BIZ-{uuid.uuid4().hex[:8].upper()}"
        order = _make_order(unique_key)

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("biz-fail-reader"))

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=unique_key, value=order)

        _run_source_consumer(integration_config, "biz-fail-src", _selective_fail_handler(unique_key))

        dlq_record = _poll_dlq_for_key(dlq_consumer, unique_key)
        assert dlq_record is not None, f"DLQ message not found for source_key={unique_key}"
        assert dlq_record["failure_reason"] == DLQRouter.REASON_BUSINESS

    def test_dlq_message_has_all_required_fields(self, integration_config, raw_consumer_factory):
        """
        DLQ record must contain every field defined in DLQRouter.route():
        source_topic, source_partition, source_offset, source_timestamp,
        source_key, failure_reason, error_type, error_message, routed_at,
        streamshield_version.
        """
        unique_key = f"FORMAT-{uuid.uuid4().hex[:8].upper()}"
        order = _make_order(unique_key)

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("format-reader"))

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=unique_key, value=order)

        _run_source_consumer(integration_config, "format-src", _selective_fail_handler(unique_key))

        rec = _poll_dlq_for_key(dlq_consumer, unique_key)
        assert rec is not None, f"DLQ message not found for source_key={unique_key}"

        assert rec["source_topic"]     == INTEGRATION_TOPIC
        assert rec["source_key"]       == unique_key
        assert rec["failure_reason"]   == DLQRouter.REASON_BUSINESS
        assert rec["error_type"]       == "RuntimeError"
        assert "test failure" in rec["error_message"].lower() or "intentional" in rec["error_message"].lower()
        assert isinstance(rec["source_partition"], int) and rec["source_partition"] >= 0
        assert isinstance(rec["source_offset"],    int) and rec["source_offset"]    >= 0
        assert isinstance(rec["source_timestamp"], int) and rec["source_timestamp"] > 0
        assert isinstance(rec["routed_at"],        int) and rec["routed_at"]        > 0
        assert rec["streamshield_version"] == "0.1.0"

    def test_error_type_reflects_exception_class(self, integration_config, raw_consumer_factory):
        """error_type in the DLQ record must be the exception class name."""
        unique_key = f"ETYPE-{uuid.uuid4().hex[:8].upper()}"
        order = _make_order(unique_key)

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("etype-reader"))

        def handler(msg: ConsumedMessage) -> None:
            key = msg.key.decode("utf-8", errors="replace") if msg.key else ""
            if key == unique_key:
                raise ValueError("custom error type test")

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=unique_key, value=order)

        _run_source_consumer(integration_config, "etype-src", handler)

        rec = _poll_dlq_for_key(dlq_consumer, unique_key)
        assert rec is not None, f"DLQ message not found for source_key={unique_key}"
        assert rec["error_type"] == "ValueError"

    def test_multiple_failures_all_routed_to_dlq(self, integration_config, raw_consumer_factory):
        """Three separate messages each fail; each must produce an independent DLQ record."""
        keys = [f"MULTI-{uuid.uuid4().hex[:6].upper()}" for _ in range(3)]

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("multi-reader"))

        with KafkaProducer(integration_config) as producer:
            for k in keys:
                producer.send(INTEGRATION_TOPIC, key=k, value=_make_order(k))

        # Handler fails for all three keys
        fail_keys = set(keys)
        def handler(msg: ConsumedMessage) -> None:
            key = msg.key.decode("utf-8", errors="replace") if msg.key else ""
            if key in fail_keys:
                raise RuntimeError(f"Failure for {key}")

        _run_source_consumer(integration_config, "multi-src", handler)

        found = {}
        deadline = time.time() + 40.0
        while time.time() < deadline and len(found) < len(keys):
            msg = dlq_consumer.poll(timeout=2.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                if data.get("source_key") in fail_keys:
                    found[data["source_key"]] = data
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

        assert len(found) == 3, f"Expected 3 DLQ records, got {len(found)}: {list(found.keys())}"
        for k in keys:
            assert found[k]["failure_reason"] == DLQRouter.REASON_BUSINESS


class TestDLQDeserializationFailure:
    """Invalid wire-format bytes route to DLQ with reason='deserialization'."""

    def test_short_payload_routes_to_dlq(self, integration_config, raw_kafka_producer, raw_consumer_factory):
        """
        A message shorter than 5 bytes cannot contain a valid Confluent header.
        DeserializationFailedError must be raised and the message DLQ'd.
        """
        unique_key = f"SHORT-{uuid.uuid4().hex[:6].upper()}"

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("short-reader"))

        # 4 bytes — one byte short of the minimum 5-byte Confluent header
        raw_kafka_producer.produce(
            topic=INTEGRATION_TOPIC,
            key=unique_key.encode("utf-8"),
            value=b"\x00\x01\x02\x03",
        )
        raw_kafka_producer.flush(timeout=15.0)

        _run_source_consumer(integration_config, "short-src", lambda msg: None)

        rec = _poll_dlq_for_key(dlq_consumer, unique_key)
        assert rec is not None, f"DLQ record not found for short payload key={unique_key}"
        assert rec["failure_reason"] == DLQRouter.REASON_DESERIALIZATION
        assert rec["source_topic"]   == INTEGRATION_TOPIC

    def test_wrong_magic_byte_routes_to_dlq(self, integration_config, raw_kafka_producer, raw_consumer_factory):
        """
        Magic byte 0x01 is not the Confluent magic (0x00).
        Must raise DeserializationFailedError and route to DLQ.
        """
        unique_key = f"MAGIC-{uuid.uuid4().hex[:6].upper()}"

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("magic-reader"))

        # magic=0x01, schema_id=2 (big-endian), then padding
        bad_bytes = struct.pack(">bI", 1, 2) + b"\x00" * 8
        raw_kafka_producer.produce(
            topic=INTEGRATION_TOPIC,
            key=unique_key.encode("utf-8"),
            value=bad_bytes,
        )
        raw_kafka_producer.flush(timeout=15.0)

        _run_source_consumer(integration_config, "magic-src", lambda msg: None)

        rec = _poll_dlq_for_key(dlq_consumer, unique_key)
        assert rec is not None, f"DLQ record not found for wrong magic byte key={unique_key}"
        assert rec["failure_reason"] == DLQRouter.REASON_DESERIALIZATION

    def test_corrupted_avro_body_routes_to_dlq(self, integration_config, raw_kafka_producer, raw_consumer_factory):
        """
        Valid Confluent header (magic=0x00, schema_id=2) followed by garbage
        that fastavro cannot decode.  Must route to DLQ.
        """
        unique_key = f"CORRUPT-{uuid.uuid4().hex[:6].upper()}"

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("corrupt-reader"))

        # Correct header for schema_id=2, then intentionally corrupt Avro body
        valid_header   = struct.pack(">bI", 0, 2)
        corrupted_body = b"\xde\xad\xbe\xef\xff\xfe\xfd\x00\x01\x23\x45\x67"
        raw_kafka_producer.produce(
            topic=INTEGRATION_TOPIC,
            key=unique_key.encode("utf-8"),
            value=valid_header + corrupted_body,
        )
        raw_kafka_producer.flush(timeout=15.0)

        _run_source_consumer(integration_config, "corrupt-src", lambda msg: None)

        rec = _poll_dlq_for_key(dlq_consumer, unique_key)
        assert rec is not None, f"DLQ record not found for corrupted body key={unique_key}"
        assert rec["failure_reason"] == DLQRouter.REASON_DESERIALIZATION

    def test_unknown_schema_id_routes_to_dlq(self, integration_config, raw_kafka_producer, raw_consumer_factory):
        """
        A message with a valid header but schema_id that doesn't exist in the
        Schema Registry causes a SchemaNotFoundError wrapped as DeserializationFailedError.
        """
        unique_key = f"UNKNOWNID-{uuid.uuid4().hex[:6].upper()}"

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("unknownid-reader"))

        # schema_id=99999 does not exist in the registry
        bad_bytes = struct.pack(">bI", 0, 99999) + b"\x00" * 20
        raw_kafka_producer.produce(
            topic=INTEGRATION_TOPIC,
            key=unique_key.encode("utf-8"),
            value=bad_bytes,
        )
        raw_kafka_producer.flush(timeout=15.0)

        _run_source_consumer(integration_config, "unknownid-src", lambda msg: None)

        rec = _poll_dlq_for_key(dlq_consumer, unique_key)
        assert rec is not None, f"DLQ record not found for unknown schema_id key={unique_key}"
        assert rec["failure_reason"] == DLQRouter.REASON_DESERIALIZATION


class TestDLQMessageContent:
    """Structural and content correctness of DLQ messages read as raw JSON."""

    def test_dlq_messages_are_valid_json(self, integration_config, raw_consumer_factory):
        """
        Raw-consume the most recent DLQ messages and verify every one can be
        parsed as JSON.  Produces a fresh failing message to guarantee at least
        one DLQ record is present from this test run.
        """
        unique_key = f"JSON-{uuid.uuid4().hex[:8].upper()}"
        order = _make_order(unique_key)

        dlq_consumer = _latest_dlq_consumer(raw_consumer_factory, _group_id("json-reader"))

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=unique_key, value=order)

        _run_source_consumer(integration_config, "json-src", _selective_fail_handler(unique_key))

        # Collect up to 10 DLQ messages and verify all are valid JSON
        collected = []
        deadline = time.time() + 30.0
        while time.time() < deadline:
            msg = dlq_consumer.poll(timeout=2.0)
            if msg is None or msg.error():
                if any(d.get("source_key") == unique_key for d in collected):
                    break
                continue
            try:
                data = json.loads(msg.value().decode("utf-8"))
                collected.append(data)
                if data.get("source_key") == unique_key:
                    break
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                pytest.fail(f"DLQ message is not valid JSON: {exc}")

        assert any(d.get("source_key") == unique_key for d in collected), \
            "Our test DLQ message was not found"
        # Every collected DLQ record must have the mandatory keys
        required_keys = {
            "source_topic", "source_partition", "source_offset",
            "failure_reason", "error_type", "routed_at", "streamshield_version",
        }
        for record in collected:
            missing = required_keys - record.keys()
            assert not missing, f"DLQ record missing keys: {missing}  record={record}"
