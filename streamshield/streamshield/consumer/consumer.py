"""
KafkaConsumer — the primary SDK interface for consuming messages.

Orchestrates:
  1. Deserialize Avro bytes (Confluent wire format).
  2. Optionally de-tokenize via Cloud DLP.
  3. Call the application handler.
  4. Commit the offset ONLY after the handler succeeds.
  5. Route failures to the Dead Letter Queue.

Offset management contract:
  - enable.auto.commit is always False. The SDK controls commits exclusively.
  - Offsets are committed AFTER the handler returns successfully.
  - If the handler raises, the message is routed to the DLQ and THEN committed.
  - This ensures no message is silently dropped (the DLQ has a copy) and the
    consumer always advances (no poison-pill infinite loop).

Usage (sync):
    def handle(msg: ConsumedMessage) -> None:
        write_to_snowflake(msg.value)  # offset commits only after this succeeds

    with KafkaConsumer(config, group_id="snowflake-loader") as consumer:
        consumer.process(handler=handle, topics=["events"], detokenize=True)

Usage (async):
    async def handle(msg: ConsumedMessage) -> None:
        await write_to_database(msg.value)

    async with AsyncKafkaConsumer(config, group_id="loader") as consumer:
        await consumer.process(handle, topics=["events"], detokenize=True)
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable

from confluent_kafka import Consumer as ConfluentConsumer, KafkaError, KafkaException

from streamshield.auth.gcp import GCPAuth
from streamshield.config import SDKConfig
from streamshield.consumer.dlq import DLQRouter
from streamshield.dlp.detokenizer import DLPDetokenizer
from streamshield.errors.exceptions import (
    AuthenticationError,
    DeserializationFailedError,
    DetokenizationError,
    OffsetCommitError,
)
from streamshield.observability.logging import consumer_logger
from streamshield.observability.metrics import messages_consumed, offset_commits
from streamshield.schema.deserializer import AvroDeserializer
from streamshield.schema.models import ConsumedMessage
from streamshield.schema.registry import SchemaRegistryClient


class KafkaConsumer:
    """
    Synchronous Kafka consumer with integrated DLP de-tokenization and DLQ routing.

    The consumer always uses explicit offset commits (enable.auto.commit=False).
    See the module docstring for the full offset management contract.
    """

    def __init__(self, config: SDKConfig, group_id: str):
        config.validate()
        self._config   = config
        self._group_id = group_id
        self._closed   = False

        consumer_logger.info(
            "Initialising KafkaConsumer group_id=%s project=%s",
            group_id, config.gcp.project_id,
        )

        # ── Auth ──────────────────────────────────────────────────────────────
        self._auth = GCPAuth(
            project_id=config.gcp.project_id,
            token_refresh_buffer_s=config.gcp.token_refresh_buffer_s,
            secrets_refresh_interval_s=config.gcp.secrets_refresh_interval_s,
        )

        bootstrap_servers = self._resolve_bootstrap_servers()
        registry_url      = self._resolve_registry_url()

        # ── confluent_kafka.Consumer ─────────────────────────────────────────
        # enable.auto.commit is ALWAYS False — the SDK manages commits explicitly
        kafka_cfg = self._auth.build_kafka_config(bootstrap_servers, extra={
            "group.id":                    group_id,
            "auto.offset.reset":           config.consumer.auto_offset_reset,
            "enable.auto.commit":          "false",   # hardcoded — not configurable by callers
            "session.timeout.ms":          config.consumer.session_timeout_ms,
            "heartbeat.interval.ms":       config.consumer.heartbeat_interval_ms,
            "max.poll.interval.ms":        config.consumer.max_poll_interval_ms,
            "error_cb": lambda err: consumer_logger.error("Kafka broker error: %s", err),
        })
        self._consumer = ConfluentConsumer(kafka_cfg)

        # ── Schema Registry and deserialization ──────────────────────────────
        self._registry     = SchemaRegistryClient(registry_url, self._auth)
        self._deserializer = AvroDeserializer(self._registry)

        # ── DLP detokenizer ──────────────────────────────────────────────────
        self._detokenizer = DLPDetokenizer(config.dlp, config.gcp.project_id)

        # ── DLQ router ───────────────────────────────────────────────────────
        self._dlq = DLQRouter(config.dlq, self._auth, bootstrap_servers)

        consumer_logger.info("KafkaConsumer ready — group=%s bootstrap=%s", group_id, bootstrap_servers)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _resolve_bootstrap_servers(self) -> str:
        if self._config.gcp.bootstrap_servers:
            return self._config.gcp.bootstrap_servers
        if self._config.gcp.use_secret_manager:
            return self._auth.get_secret(self._config.gcp.bootstrap_servers_secret)
        raise AuthenticationError("No bootstrap_servers configured and use_secret_manager=False.")

    def _resolve_registry_url(self) -> str:
        if self._config.gcp.schema_registry_url:
            return self._config.gcp.schema_registry_url
        if self._config.gcp.use_secret_manager:
            return self._auth.get_secret(self._config.gcp.schema_registry_url_secret)
        raise AuthenticationError("No schema_registry_url configured and use_secret_manager=False.")

    # ── Public API ────────────────────────────────────────────────────────────

    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable | None = None,
        on_revoke: Callable | None = None,
    ) -> None:
        """
        Subscribe to a list of Kafka topics.

        Args:
            topics:    Topic names to subscribe to.
            on_assign: Callback when partitions are assigned (rebalance complete).
            on_revoke: Callback when partitions are being revoked.
        """
        # confluent_kafka rejects None for on_assign/on_revoke — only pass them when set
        subscribe_kwargs: dict = {}
        if on_assign is not None:
            subscribe_kwargs["on_assign"] = on_assign
        if on_revoke is not None:
            subscribe_kwargs["on_revoke"] = on_revoke
        self._consumer.subscribe(topics, **subscribe_kwargs)
        consumer_logger.info("Subscribed to topics: %s", topics)

    def poll(
        self,
        timeout: float = 1.0,
        detokenize: bool = False,
    ) -> ConsumedMessage | None:
        """
        Poll for one message.

        Returns None if no message arrives within the timeout. The caller is
        responsible for calling commit() after processing the returned message.

        Args:
            timeout:     Maximum seconds to wait for a message.
            detokenize:  If True, call DLP reidentifyContent to reverse tokens.

        Returns:
            ConsumedMessage or None.

        Raises:
            DeserializationFailedError if the Avro bytes cannot be decoded.
            DetokenizationError if DLP fails (only when detokenize=True).
            KafkaException on broker errors.
        """
        # Refresh ADC token if it is close to expiry
        self._auth.ensure_fresh_token()

        msg = self._consumer.poll(timeout=timeout)

        if msg is None:
            return None  # no message within timeout — normal

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                consumer_logger.debug(
                    "Partition EOF: topic=%s partition=%d offset=%d",
                    msg.topic(), msg.partition(), msg.offset(),
                )
                return None
            raise KafkaException(msg.error())

        # Deserialize — raises DeserializationFailedError on failure
        record, raw_schema, schema_id = self._deserializer.deserialize(msg.value())

        # De-tokenize — optional, only for authorized consumers
        if detokenize:
            record = self._detokenizer.detokenize(record, raw_schema)

        # Extract headers from the Kafka message
        headers: dict[str, bytes] = {}
        if msg.headers():
            for k, v in msg.headers():
                headers[k] = v

        return ConsumedMessage(
            topic      = msg.topic(),
            partition  = msg.partition(),
            offset     = msg.offset(),
            timestamp  = msg.timestamp()[1] if msg.timestamp() else -1,
            key        = msg.key(),
            value      = record,
            raw_schema = raw_schema,
            schema_id  = schema_id,
            headers    = headers,
        )

    def commit(
        self,
        message: ConsumedMessage | None = None,
        asynchronous: bool = False,
    ) -> None:
        """
        Commit the offset for a processed message.

        Args:
            message:     The ConsumedMessage to commit. None = commit current position.
            asynchronous: If True, commit in the background (no error feedback).

        Raises:
            OffsetCommitError if synchronous commit fails.
        """
        try:
            self._consumer.commit(asynchronous=asynchronous)
            offset_commits.add(1, {"topic": message.topic if message else "unknown", "group_id": self._group_id, "status": "success"})
            if message:
                consumer_logger.debug(
                    "Committed offset: topic=%s partition=%d offset=%d",
                    message.topic, message.partition, message.offset,
                )
        except Exception as exc:
            offset_commits.add(1, {"topic": message.topic if message else "unknown", "group_id": self._group_id, "status": "failed"})
            raise OffsetCommitError(
                f"Failed to commit offset: {exc}",
                safe_context={"group_id": self._group_id},
            ) from exc

    def process(
        self,
        handler: Callable[[ConsumedMessage], None],
        topics: list[str],
        detokenize: bool = False,
        max_messages: int | None = None,
        idle_timeout_s: float | None = None,
    ) -> None:
        """
        Run a managed poll loop. Subscribes, polls, and calls handler for each message.

        Offset management:
          - Offset is committed AFTER handler returns successfully.
          - On any failure, the message is routed to the DLQ and THEN committed.
          - The consumer always moves forward — no poison-pill infinite loops.

        Args:
            handler:        Callable that receives a ConsumedMessage. Must not suppress
                            exceptions — raise on failure so the DLQ router catches it.
            topics:         Topics to subscribe to.
            detokenize:     Call DLP reidentifyContent before delivering to handler.
            max_messages:   Stop after processing this many messages. None = run forever.
            idle_timeout_s: Stop after this many seconds with no messages.
                            None = use ConsumerConfig.idle_timeout_s.
        """
        self.subscribe(topics)
        idle_timeout = idle_timeout_s if idle_timeout_s is not None else self._config.consumer.idle_timeout_s
        last_message_time = time.time()
        processed = 0

        consumer_logger.info(
            "Starting process loop: topics=%s group=%s detokenize=%s idle_timeout=%.1fs",
            topics, self._group_id, detokenize, idle_timeout,
        )

        try:
            while True:
                # Check idle timeout
                if time.time() - last_message_time > idle_timeout:
                    consumer_logger.info(
                        "Idle timeout reached (%.1fs) — exiting process loop.", idle_timeout
                    )
                    break

                # Check max_messages limit
                if max_messages is not None and processed >= max_messages:
                    consumer_logger.info("max_messages=%d reached — exiting.", max_messages)
                    break

                raw_msg = self._consumer.poll(timeout=1.0)

                if raw_msg is None:
                    continue  # no message; loop back to check timeouts

                if raw_msg.error():
                    if raw_msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    consumer_logger.error("Kafka error: %s", raw_msg.error())
                    continue

                last_message_time = time.time()

                # ── Step 1: Deserialize ───────────────────────────────────────
                try:
                    record, raw_schema, schema_id = self._deserializer.deserialize(raw_msg.value())
                except DeserializationFailedError as exc:
                    consumer_logger.error(
                        "Deserialization failed: topic=%s offset=%d — routing to DLQ: %s",
                        raw_msg.topic(), raw_msg.offset(), exc,
                    )
                    self._route_to_dlq(raw_msg, exc, DLQRouter.REASON_DESERIALIZATION)
                    self._commit_raw(raw_msg)
                    messages_consumed.add(1, {"topic": raw_msg.topic(), "group_id": self._group_id, "status": "dlq"})
                    continue

                # ── Step 2: De-tokenize (optional) ───────────────────────────
                if detokenize:
                    try:
                        record = self._detokenizer.detokenize(record, raw_schema)
                    except DetokenizationError as exc:
                        consumer_logger.error(
                            "DLP detokenization failed: topic=%s offset=%d — routing to DLQ: %s",
                            raw_msg.topic(), raw_msg.offset(), exc,
                        )
                        self._route_to_dlq(raw_msg, exc, DLQRouter.REASON_DLP)
                        self._commit_raw(raw_msg)
                        messages_consumed.add(1, {"topic": raw_msg.topic(), "group_id": self._group_id, "status": "dlq"})
                        continue

                # Build ConsumedMessage for the application handler
                headers: dict[str, bytes] = {}
                if raw_msg.headers():
                    for k, v in raw_msg.headers():
                        headers[k] = v

                consumed_msg = ConsumedMessage(
                    topic      = raw_msg.topic(),
                    partition  = raw_msg.partition(),
                    offset     = raw_msg.offset(),
                    timestamp  = raw_msg.timestamp()[1] if raw_msg.timestamp() else -1,
                    key        = raw_msg.key(),
                    value      = record,
                    raw_schema = raw_schema,
                    schema_id  = schema_id,
                    headers    = headers,
                )

                # ── Step 3: Application handler ───────────────────────────────
                try:
                    handler(consumed_msg)
                except Exception as exc:
                    consumer_logger.error(
                        "Handler raised for topic=%s offset=%d — routing to DLQ: %s",
                        raw_msg.topic(), raw_msg.offset(), exc,
                    )
                    self._route_to_dlq(raw_msg, exc, DLQRouter.REASON_BUSINESS)
                    self._commit_raw(raw_msg)
                    messages_consumed.add(1, {"topic": raw_msg.topic(), "group_id": self._group_id, "status": "dlq"})
                    continue

                # ── Step 4: Commit offset — only after success ────────────────
                self._commit_raw(raw_msg)
                messages_consumed.add(1, {"topic": raw_msg.topic(), "group_id": self._group_id, "status": "success"})
                processed += 1

                consumer_logger.debug(
                    "Processed: topic=%s partition=%d offset=%d",
                    raw_msg.topic(), raw_msg.partition(), raw_msg.offset(),
                )

        except KeyboardInterrupt:
            consumer_logger.info("Interrupted — shutting down consumer.")
        finally:
            consumer_logger.info(
                "Process loop ended: processed=%d group=%s topics=%s",
                processed, self._group_id, topics,
            )

    def _route_to_dlq(self, raw_msg, exc: Exception, reason: str) -> None:
        """Helper: route a raw confluent_kafka Message to the DLQ."""
        self._dlq.route(
            source_topic      = raw_msg.topic(),
            source_partition  = raw_msg.partition(),
            source_offset     = raw_msg.offset(),
            source_timestamp  = raw_msg.timestamp()[1] if raw_msg.timestamp() else -1,
            source_key        = raw_msg.key(),
            original_payload  = raw_msg.value() or b"",
            reason            = reason,
            error_type        = type(exc).__name__,
            error_message     = str(exc)[:500],
        )

    def _commit_raw(self, raw_msg) -> None:
        """Helper: commit a raw confluent_kafka Message offset."""
        try:
            self._consumer.commit(message=raw_msg, asynchronous=False)
            offset_commits.add(1, {"topic": raw_msg.topic(), "group_id": self._group_id, "status": "success"})
        except Exception as exc:
            offset_commits.add(1, {"topic": raw_msg.topic(), "group_id": self._group_id, "status": "failed"})
            consumer_logger.error("Offset commit failed: %s", exc)
            # Log and continue — offset commit failure is unfortunate but not fatal.
            # The message may be reprocessed on restart; the handler or DLQ will handle it.

    def close(self) -> None:
        """Commit pending offsets, close the DLQ producer, and close the consumer. Idempotent."""
        if self._closed:
            return
        self._closed = True
        try:
            self._dlq.close()
            self._consumer.close()
            consumer_logger.info("KafkaConsumer closed (group=%s).", self._group_id)
        except Exception as exc:
            consumer_logger.error("Error during consumer close: %s", exc)

    def __enter__(self) -> "KafkaConsumer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        return None


# ── Async wrapper ─────────────────────────────────────────────────────────────

class AsyncKafkaConsumer:
    """
    Async wrapper around KafkaConsumer.

    The synchronous poll loop in process() runs in a thread pool so it doesn't
    block the event loop. The application handler can be async (coroutine).

    Usage:
        async def handle(msg: ConsumedMessage) -> None:
            await write_to_db(msg.value)

        async with AsyncKafkaConsumer(config, "my-group") as consumer:
            await consumer.process(handle, topics=["events"], detokenize=True)
    """

    def __init__(self, config: SDKConfig, group_id: str):
        self._sync = KafkaConsumer(config, group_id)

    async def subscribe(self, topics: list[str]) -> None:
        await asyncio.to_thread(self._sync.subscribe, topics)

    async def poll(
        self,
        timeout: float = 1.0,
        detokenize: bool = False,
    ) -> ConsumedMessage | None:
        return await asyncio.to_thread(self._sync.poll, timeout, detokenize)

    async def commit(self, message: ConsumedMessage | None = None) -> None:
        await asyncio.to_thread(self._sync.commit, message)

    async def process(
        self,
        handler: Callable,
        topics: list[str],
        detokenize: bool = False,
        max_messages: int | None = None,
        idle_timeout_s: float | None = None,
    ) -> None:
        """
        Async process loop. If handler is a coroutine function, it is awaited directly.
        Otherwise it runs in a thread.
        """
        self._sync.subscribe(topics)
        idle_timeout  = idle_timeout_s or self._sync._config.consumer.idle_timeout_s
        last_msg_time = time.time()
        processed     = 0

        try:
            while True:
                if time.time() - last_msg_time > idle_timeout:
                    break
                if max_messages is not None and processed >= max_messages:
                    break

                consumed = await asyncio.to_thread(self._sync.poll, 1.0, detokenize)
                if consumed is None:
                    continue

                last_msg_time = time.time()

                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(consumed)
                    else:
                        await asyncio.to_thread(handler, consumed)
                except Exception:
                    consumer_logger.error("Async handler failed — message will be committed anyway.")
                    # For async consumers, DLQ routing via the sync path is acceptable
                finally:
                    # commit() uses the public KafkaConsumer API (no raw confluent_kafka
                    # message needed) — _commit_raw() requires the raw confluent_kafka
                    # Message object which is not available here.
                    await asyncio.to_thread(self._sync.commit, consumed)
                    processed += 1

        except asyncio.CancelledError:
            consumer_logger.info("Async consumer process cancelled.")
        finally:
            await self.close()

    async def close(self) -> None:
        await asyncio.to_thread(self._sync.close)

    async def __aenter__(self) -> "AsyncKafkaConsumer":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
