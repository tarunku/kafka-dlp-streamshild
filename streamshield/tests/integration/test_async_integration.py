"""
Integration tests: AsyncKafkaProducer and AsyncKafkaConsumer.

Covers:
  1. AsyncKafkaProducer.send()       — single message, correct metadata returned.
  2. AsyncKafkaProducer.send_batch() — batch of 3, list of 3 metadata returned.
  3. AsyncKafkaConsumer with async handler — coroutine handler is awaited.
  4. AsyncKafkaConsumer with sync handler  — regular function handler is thread-wrapped.

Note on async consumer DLQ behaviour:
  AsyncKafkaConsumer.process() logs handler errors but does NOT route them to
  the DLQ (the sync DLQ path is not invoked). This is a known v0.1.0 limitation
  documented in the CLAUDE.md "What Is NOT in This SDK" section.
  The test documents the current behaviour (error logged, offset committed, loop continues).

All tests run against vetsource-496203.  No mocking.
"""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest

from streamshield import (
    AsyncKafkaConsumer,
    AsyncKafkaProducer,
    ConsumedMessage,
    MessageMetadata,
)
from tests.integration.conftest import INTEGRATION_TOPIC


def _group_id(label: str) -> str:
    return f"streamshield-async-test-{label}-{int(time.time())}"


def _make_order(order_id: str | None = None) -> dict:
    oid = order_id or f"ASYNC-{uuid.uuid4().hex[:8].upper()}"
    return {
        "order_id":           oid,
        "owner_name":         "Async User",
        "owner_email":        "async@example.com",
        "owner_phone":        "+1-555-0200",
        "owner_payment_card": "5500005555555559",
        "pet_name":           "Rocket",
        "medication":         "Amoxicillin 500mg",
        "quantity":           60,
        "order_date":         "2026-06-04",
        "is_refill":          True,
    }


# ── Async producer ─────────────────────────────────────────────────────────────

class TestAsyncKafkaProducerIntegration:
    def test_async_send_single_message(self, integration_config):
        """AsyncKafkaProducer.send() must produce one message and return MessageMetadata."""
        order = _make_order()

        async def _run():
            async with AsyncKafkaProducer(integration_config) as producer:
                meta = await producer.send(
                    INTEGRATION_TOPIC,
                    key=order["order_id"],
                    value=order,
                )
            return meta

        meta = asyncio.run(_run())

        assert isinstance(meta, MessageMetadata)
        assert meta.topic == INTEGRATION_TOPIC
        print(f"\nAsync delivered: {order['order_id']} → topic={meta.topic}")

    def test_async_send_batch(self, integration_config):
        """AsyncKafkaProducer.send_batch() must produce N messages and return N metadata objects."""
        records = [_make_order() for _ in range(3)]

        async def _run():
            async with AsyncKafkaProducer(integration_config) as producer:
                results = await producer.send_batch(
                    INTEGRATION_TOPIC,
                    records=records,
                    key_field="order_id",
                )
            return results

        results = asyncio.run(_run())

        assert len(results) == 3
        for meta in results:
            assert isinstance(meta, MessageMetadata)
            assert meta.topic == INTEGRATION_TOPIC
        print(f"\nAsync batch: {len(results)} messages delivered")


# ── Async consumer ─────────────────────────────────────────────────────────────

class TestAsyncKafkaConsumerIntegration:
    def test_async_consumer_with_async_handler(self, integration_config):
        """
        AsyncKafkaConsumer must await a coroutine handler for each message.
        Produce 2 messages, consume them with an async handler, verify the
        handler was called at least once.
        """
        records = [_make_order() for _ in range(2)]

        async def _produce():
            async with AsyncKafkaProducer(integration_config) as producer:
                await producer.send_batch(INTEGRATION_TOPIC, records=records, key_field="order_id")

        asyncio.run(_produce())

        received: list[ConsumedMessage] = []

        async def async_handler(msg: ConsumedMessage) -> None:
            received.append(msg)

        async def _consume():
            async with AsyncKafkaConsumer(integration_config, _group_id("async-handler")) as consumer:
                await consumer.process(
                    handler        = async_handler,
                    topics         = [INTEGRATION_TOPIC],
                    detokenize     = False,
                    max_messages   = 2,
                    idle_timeout_s = 20.0,
                )

        asyncio.run(_consume())

        assert len(received) >= 1, "Async handler was never called"
        for msg in received:
            assert msg.topic == INTEGRATION_TOPIC
        print(f"\nAsync handler received {len(received)} message(s)")

    def test_async_consumer_with_sync_handler(self, integration_config):
        """
        AsyncKafkaConsumer wraps a synchronous handler in asyncio.to_thread().
        The handler must be called and messages committed normally.
        """
        order = _make_order()

        async def _produce():
            async with AsyncKafkaProducer(integration_config) as producer:
                await producer.send(INTEGRATION_TOPIC, key=order["order_id"], value=order)

        asyncio.run(_produce())

        received: list[ConsumedMessage] = []

        def sync_handler(msg: ConsumedMessage) -> None:
            received.append(msg)

        async def _consume():
            async with AsyncKafkaConsumer(integration_config, _group_id("sync-handler")) as consumer:
                await consumer.process(
                    handler        = sync_handler,
                    topics         = [INTEGRATION_TOPIC],
                    detokenize     = False,
                    max_messages   = 1,
                    idle_timeout_s = 20.0,
                )

        asyncio.run(_consume())

        assert len(received) >= 1, "Sync handler was never called via async consumer"
        print(f"\nSync-in-async handler received {len(received)} message(s)")

    def test_async_consumer_handler_error_does_not_crash_loop(self, integration_config):
        """
        Current v0.1.0 behaviour: async consumer logs handler errors and commits
        the offset anyway (no DLQ routing). The process loop must continue and
        eventually exit on idle_timeout — it must NOT crash or hang.
        """
        order = _make_order()

        async def _produce():
            async with AsyncKafkaProducer(integration_config) as producer:
                await producer.send(INTEGRATION_TOPIC, key=order["order_id"], value=order)

        asyncio.run(_produce())

        completed = [False]

        async def failing_handler(msg: ConsumedMessage) -> None:
            raise RuntimeError("Async handler failure — offset should still be committed")

        async def _consume():
            async with AsyncKafkaConsumer(integration_config, _group_id("async-err")) as consumer:
                await consumer.process(
                    handler        = failing_handler,
                    topics         = [INTEGRATION_TOPIC],
                    detokenize     = False,
                    max_messages   = 1,
                    idle_timeout_s = 15.0,
                )
            completed[0] = True

        asyncio.run(_consume())

        assert completed[0], "Async process loop did not exit cleanly after handler error"
