"""
KafkaProducer — the primary SDK interface for producing messages.

Orchestrates the full send pipeline:
  1. Fetch schema from Schema Registry (cached after first fetch).
  2. Validate the record against the Avro schema (pre-flight, no I/O).
  3. Tokenize sensitive fields via Cloud DLP (batch-capable).
  4. Serialize to Confluent Avro wire format.
  5. Produce to Kafka.
  6. On flush(), raise DeliveryFailedError if any delivery failed.

Usage (sync):
    config = SDKConfig(gcp=GCPConfig(project_id="my-project"))
    with KafkaProducer(config) as producer:
        producer.send("my-topic", value={"id": "1", "name": "Alice"})

Usage (async):
    async with AsyncKafkaProducer(config) as producer:
        await producer.send("my-topic", value={"id": "1", "name": "Alice"})
"""

from __future__ import annotations

import asyncio
import threading
from typing import Callable

from confluent_kafka import Producer as ConfluentProducer, KafkaError

from streamshield.auth.gcp import GCPAuth
from streamshield.config import SDKConfig
from streamshield.dlp.tokenizer import DLPTokenizer
from streamshield.errors.exceptions import (
    AuthenticationError,
    DeliveryFailedError,
    TokenizationError,
)
from streamshield.observability.logging import producer_logger
from streamshield.observability.metrics import messages_produced
from streamshield.schema.models import MessageMetadata
from streamshield.schema.registry import SchemaRegistryClient
from streamshield.schema.serializer import AvroSerializer


class KafkaProducer:
    """
    Synchronous Kafka producer with integrated DLP tokenization and schema enforcement.

    Lifecycle:
        - Use as a context manager (preferred): 'with KafkaProducer(config) as p:'
        - Or call close() explicitly when done.
        - flush() is called automatically on context exit.

    Delivery guarantees:
        - Idempotent producer is enabled by default (enable_idempotence=True, acks=all).
        - Delivery failures are tracked and raised as DeliveryFailedError on flush().
        - No message is silently lost.
    """

    def __init__(self, config: SDKConfig):
        config.validate()
        self._config = config
        self._closed = False

        producer_logger.info(
            "Initialising KafkaProducer project=%s safe_config=%s",
            config.gcp.project_id,
            config.to_safe_dict(),
        )

        # ── Auth and connection setup ─────────────────────────────────────────
        self._auth = GCPAuth(
            project_id=config.gcp.project_id,
            token_refresh_buffer_s=config.gcp.token_refresh_buffer_s,
            secrets_refresh_interval_s=config.gcp.secrets_refresh_interval_s,
        )

        # Resolve bootstrap servers — Secret Manager or direct config
        bootstrap_servers = self._resolve_bootstrap_servers()
        registry_url      = self._resolve_registry_url()

        # ── confluent_kafka.Producer ─────────────────────────────────────────
        kafka_cfg = self._auth.build_kafka_config(bootstrap_servers, extra={
            # Reliability settings — always on for production
            "enable.idempotence":                       str(config.producer.enable_idempotence).lower(),
            "acks":                                      config.producer.acks,
            "retries":                                   config.producer.retries,
            "retry.backoff.ms":                          config.producer.retry_backoff_ms,
            # Batching/throughput settings
            "linger.ms":                                 config.producer.linger_ms,
            "batch.size":                                config.producer.batch_size_bytes,
            "compression.type":                          config.producer.compression_type,
            # Timeout settings
            "request.timeout.ms":                        config.producer.request_timeout_ms,
            "delivery.timeout.ms":                       config.producer.delivery_timeout_ms,
            # Error callback — logs broker errors without crashing the process
            "error_cb": lambda err: producer_logger.error("Kafka broker error: %s", err),
        })
        self._producer = ConfluentProducer(kafka_cfg)

        # ── Schema Registry and serialization ────────────────────────────────
        self._registry   = SchemaRegistryClient(registry_url, self._auth)
        self._serializer = AvroSerializer()

        # ── DLP tokenizer ────────────────────────────────────────────────────
        self._tokenizer = DLPTokenizer(config.dlp, config.gcp.project_id)

        # ── Delivery tracking ────────────────────────────────────────────────
        # Failed deliveries are collected here; DeliveryFailedError raised on flush()
        self._delivery_failures: list[str] = []
        self._delivery_lock = threading.Lock()

        producer_logger.info("KafkaProducer ready — bootstrap=%s", bootstrap_servers)

    # ── Helper: resolve connection parameters ─────────────────────────────────

    def _resolve_bootstrap_servers(self) -> str:
        if self._config.gcp.bootstrap_servers:
            return self._config.gcp.bootstrap_servers
        if self._config.gcp.use_secret_manager:
            return self._auth.get_secret(self._config.gcp.bootstrap_servers_secret)
        raise AuthenticationError(
            "No bootstrap_servers configured and use_secret_manager=False."
        )

    def _resolve_registry_url(self) -> str:
        if self._config.gcp.schema_registry_url:
            return self._config.gcp.schema_registry_url
        if self._config.gcp.use_secret_manager:
            return self._auth.get_secret(self._config.gcp.schema_registry_url_secret)
        raise AuthenticationError(
            "No schema_registry_url configured and use_secret_manager=False."
        )

    # ── Delivery callback ─────────────────────────────────────────────────────

    def _on_delivery(
        self,
        err: KafkaError | None,
        msg,
        user_callback: Callable | None = None,
    ) -> None:
        """
        Internal delivery callback. Called by confluent_kafka for every produced message.
        Records failures so flush() can surface them as a typed exception.
        """
        if err is not None:
            error_str = f"Delivery failed: key={msg.key()} topic={msg.topic()} error={err}"
            producer_logger.error(error_str)
            with self._delivery_lock:
                self._delivery_failures.append(error_str)
            messages_produced.add(1, {"topic": msg.topic(), "status": "failed"})
        else:
            producer_logger.debug(
                "Delivered: topic=%s partition=%d offset=%d",
                msg.topic(), msg.partition(), msg.offset(),
            )
            messages_produced.add(1, {"topic": msg.topic(), "status": "success"})

        if user_callback is not None:
            user_callback(err, msg)

    # ── Schema resolution ─────────────────────────────────────────────────────

    def _get_schema(self, topic: str, schema_version: int | None) -> tuple:
        """
        Fetch the schema for a topic from the registry.

        Returns (schema_version_obj, parsed_fastavro_schema)
        """
        subject = SchemaRegistryClient.resolve_subject(
            topic,
            self._config.schema.subject_name_strategy,
        )

        if schema_version is not None:
            sv = self._registry.get_version(subject, schema_version)
        else:
            sv = self._registry.get_latest(subject)

        # get_by_id returns both the raw definition and the parsed schema
        _, parsed = self._registry.get_by_id(sv.schema_id)
        return sv, parsed

    # ── Public API ────────────────────────────────────────────────────────────

    def send(
        self,
        topic: str,
        value: dict,
        key: str | None = None,
        schema_version: int | None = None,
        headers: dict[str, str] | None = None,
        on_delivery: Callable | None = None,
    ) -> MessageMetadata:
        """
        Produce one message to Kafka.

        The pipeline is:
            validate record → tokenize via DLP → serialize Avro → produce to Kafka

        flush() must be called (or the context manager must exit) to guarantee delivery.

        Args:
            topic:          Kafka topic name.
            value:          Plaintext record dict. Sensitive fields are tokenized automatically.
            key:            Optional Kafka message key (string, encoded to UTF-8).
            schema_version: Pin to a specific schema version. None = latest.
            headers:        Optional key-value headers attached to the Kafka message.
            on_delivery:    Optional callback(err, msg) in addition to internal tracking.

        Returns:
            MessageMetadata with topic; partition and offset are populated after flush().

        Raises:
            SchemaValidationError  — record does not match the schema (before any I/O).
            TokenizationError      — DLP deidentifyContent failed.
            SerializationFailedError — fastavro write failed.
        """
        # Refresh token if it is close to expiry
        self._auth.ensure_fresh_token()

        # Step 1: Fetch schema
        sv, parsed_schema = self._get_schema(topic, schema_version)

        # Step 2: Pre-flight validation — catches type mismatches before touching DLP or Kafka
        self._serializer.validate(value, parsed_schema)

        # Step 3: DLP tokenization — replaces sensitive fields with tokens
        try:
            tokenized = self._tokenizer.tokenize(value, sv.schema)
        except TokenizationError:
            raise
        except Exception as exc:
            raise TokenizationError(
                f"Unexpected error during DLP tokenization: {exc}",
                safe_context={"topic": topic, "schema_id": sv.schema_id},
            ) from exc

        # Step 4: Avro serialization — Confluent wire format (0x00 + schema_id + avro bytes)
        serialized = self._serializer.serialize(tokenized, parsed_schema, sv.schema_id)

        # Step 5: Kafka produce (non-blocking — actual delivery happens asynchronously)
        kafka_key = key.encode("utf-8") if key else None
        kafka_headers = [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]

        self._producer.produce(
            topic   = topic,
            value   = serialized,
            key     = kafka_key,
            headers = kafka_headers or None,
            on_delivery = lambda e, m: self._on_delivery(e, m, on_delivery),
        )

        # Poll for delivery callbacks on previously-produced messages (non-blocking)
        self._producer.poll(0)

        producer_logger.debug("Queued message topic=%s key=%s schema_id=%d", topic, key, sv.schema_id)

        # Partition and offset are unknown until flush(); return topic-level metadata now
        return MessageMetadata(
            topic=topic,
            partition=-1,   # populated after flush()
            offset=-1,      # populated after flush()
            timestamp=-1,
            key=kafka_key,
        )

    def send_batch(
        self,
        topic: str,
        records: list[dict],
        key_field: str | None = None,
        schema_version: int | None = None,
    ) -> list[MessageMetadata]:
        """
        Produce multiple messages using a single batched DLP tokenization call.

        All records must conform to the same schema. Schema validation is checked
        on the first record; remaining records are assumed to match the same schema.

        Args:
            topic:        Kafka topic name.
            records:      List of plaintext record dicts.
            key_field:    Field name to use as the Kafka message key. None = no key.
            schema_version: Pin to a specific schema version. None = latest.

        Returns:
            List of MessageMetadata in the same order as input.

        Raises:
            SchemaValidationError  — if any record fails schema validation.
            TokenizationError      — if DLP fails for the batch.
        """
        if not records:
            return []

        self._auth.ensure_fresh_token()

        sv, parsed_schema = self._get_schema(topic, schema_version)

        # Validate all records before touching DLP — fail fast if any is invalid
        for record in records:
            self._serializer.validate(record, parsed_schema)

        # Single batch DLP call for all records — this is the performance win
        tokenized_records = self._tokenizer.tokenize_batch(records, sv.schema)

        results: list[MessageMetadata] = []
        for tokenized in tokenized_records:
            serialized = self._serializer.serialize(tokenized, parsed_schema, sv.schema_id)
            kafka_key  = tokenized[key_field].encode("utf-8") if key_field and key_field in tokenized else None

            self._producer.produce(
                topic   = topic,
                value   = serialized,
                key     = kafka_key,
                on_delivery = lambda e, m: self._on_delivery(e, m),
            )
            self._producer.poll(0)

            results.append(MessageMetadata(
                topic=topic, partition=-1, offset=-1, timestamp=-1, key=kafka_key,
            ))

        producer_logger.info("Queued batch of %d messages to topic=%s", len(records), topic)
        return results

    def flush(self, timeout: float = 30.0) -> None:
        """
        Block until all in-flight messages are acknowledged by the broker.

        Raises:
            DeliveryFailedError if any message failed delivery.
        """
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            producer_logger.warning("%d messages still in-flight after %.1fs flush timeout", remaining, timeout)

        with self._delivery_lock:
            failures = list(self._delivery_failures)
            self._delivery_failures.clear()

        if failures:
            raise DeliveryFailedError(
                f"{len(failures)} message(s) failed delivery: {failures[0]}",
                safe_context={"failure_count": len(failures), "first_error": failures[0]},
            )

        producer_logger.debug("Flush complete — all messages delivered")

    def close(self) -> None:
        """Flush pending messages and close the producer. Idempotent."""
        if self._closed:
            return
        try:
            self.flush()
        finally:
            self._closed = True
            producer_logger.info("KafkaProducer closed.")

    def __enter__(self) -> "KafkaProducer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        # Don't suppress exceptions from the with-block
        return None


# ── Async wrapper ─────────────────────────────────────────────────────────────

class AsyncKafkaProducer:
    """
    Async wrapper around KafkaProducer for use in async/await applications.

    All methods delegate to a synchronous KafkaProducer running on the default
    thread pool executor (asyncio.to_thread). This approach avoids the complexity
    of an async Kafka client while being safe for I/O-bound workloads.

    Usage:
        async with AsyncKafkaProducer(config) as producer:
            await producer.send("my-topic", value={"id": "1", "name": "Alice"})
    """

    def __init__(self, config: SDKConfig):
        # The sync producer is created synchronously — GCP calls happen here.
        # For fully async initialisation, call AsyncKafkaProducer.create(config) instead.
        self._sync = KafkaProducer(config)

    @classmethod
    async def create(cls, config: SDKConfig) -> "AsyncKafkaProducer":
        """
        Create an AsyncKafkaProducer without blocking the event loop.
        The synchronous initialisation (GCP auth, secret fetching) runs in a thread.
        """
        return await asyncio.to_thread(cls, config)

    async def send(
        self,
        topic: str,
        value: dict,
        key: str | None = None,
        schema_version: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> MessageMetadata:
        """Async version of KafkaProducer.send(). DLP and Kafka calls run in a thread."""
        return await asyncio.to_thread(
            self._sync.send, topic, value, key, schema_version, headers
        )

    async def send_batch(
        self,
        topic: str,
        records: list[dict],
        key_field: str | None = None,
        schema_version: int | None = None,
    ) -> list[MessageMetadata]:
        """Async version of KafkaProducer.send_batch()."""
        return await asyncio.to_thread(
            self._sync.send_batch, topic, records, key_field, schema_version
        )

    async def flush(self, timeout: float = 30.0) -> None:
        """Async version of KafkaProducer.flush()."""
        await asyncio.to_thread(self._sync.flush, timeout)

    async def close(self) -> None:
        """Async version of KafkaProducer.close()."""
        await asyncio.to_thread(self._sync.close)

    async def __aenter__(self) -> "AsyncKafkaProducer":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
