"""
Dead Letter Queue (DLQ) router.

When a message cannot be processed (deserialization failure, DLP failure, or
business logic failure), the DLQ router publishes it to a dedicated DLQ topic
instead of dropping it or crashing the consumer.

This preserves every failed record for inspection and replay. The offset is
committed after successful DLQ routing so the consumer moves forward and
does not re-process the same failing message indefinitely.

DLQ message format:
    The DLQ message value is a JSON-encoded DLQRecord. This is plain JSON —
    not Avro-serialized — because deserialization may have failed before a
    schema was available.

DLQ topic naming:
    DLQ topic = source topic + DLQConfig.topic_suffix
    Default:    "my-events" → "my-events.dlq"
"""

from __future__ import annotations

import json
import time

from confluent_kafka import Producer as ConfluentProducer

from streamshield.auth.gcp import GCPAuth
from streamshield.config import DLQConfig
from streamshield.errors.exceptions import DLQPublishError
from streamshield.observability.logging import dlq_logger
from streamshield.observability.metrics import dlq_messages


class DLQRouter:
    """
    Routes failed consumer messages to a Dead Letter Queue topic.

    A single DLQRouter is shared across all failure points in the consumer:
    deserialization, DLP, and business logic failures all call route().

    The underlying Kafka producer is lazy-initialised on the first route() call
    to avoid creating an extra producer connection for consumers that never
    encounter failures.
    """

    # Valid reason codes — used as labels on DLQ metrics
    REASON_DESERIALIZATION = "deserialization"
    REASON_DLP             = "dlp"
    REASON_BUSINESS        = "business"

    def __init__(
        self,
        dlq_config: DLQConfig,
        auth: GCPAuth,
        bootstrap_servers: str,
    ):
        self._config           = dlq_config
        self._auth             = auth
        self._bootstrap        = bootstrap_servers
        self._producer: ConfluentProducer | None = None  # lazy init

    def _get_producer(self) -> ConfluentProducer:
        """Lazy-initialise the DLQ Kafka producer on first use."""
        if self._producer is None:
            kafka_cfg = self._auth.build_kafka_config(self._bootstrap, extra={
                # DLQ messages are less latency-sensitive — no idempotence overhead needed
                "acks":           "1",
                "retries":        self._config.max_retries,
                "error_cb":       lambda err: dlq_logger.error("DLQ producer error: %s", err),
            })
            self._producer = ConfluentProducer(kafka_cfg)
            dlq_logger.info("DLQ producer initialised (lazy)")
        return self._producer

    def dlq_topic_name(self, source_topic: str) -> str:
        """Return the DLQ topic name for a given source topic."""
        return f"{source_topic}{self._config.topic_suffix}"

    def route(
        self,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        source_timestamp: int,
        source_key: bytes | None,
        original_payload: bytes,
        reason: str,
        error_type: str,
        error_message: str,
    ) -> None:
        """
        Publish a failed message to the DLQ topic.

        After route() returns, the caller should commit the source offset so the
        consumer advances past the failing message.

        Args:
            source_topic:      The topic the message was consumed from.
            source_partition:  Partition of the failing message.
            source_offset:     Offset of the failing message.
            source_timestamp:  Message timestamp (epoch ms).
            source_key:        Original Kafka message key bytes.
            original_payload:  Raw bytes as received from Kafka — preserves the full message.
            reason:            One of: 'deserialization', 'dlp', 'business'.
            error_type:        Exception class name (no values, safe to log).
            error_message:     Truncated error description (safe — no PII values).

        Raises:
            DLQPublishError if publishing to DLQ fails and raise_on_dlq_failure=True.
        """
        if not self._config.enabled:
            dlq_logger.warning(
                "DLQ disabled — dropping failed message: topic=%s offset=%d reason=%s",
                source_topic, source_offset, reason,
            )
            return

        dlq_topic = self.dlq_topic_name(source_topic)

        # Build the DLQ record — this is what consumers of the DLQ will see
        dlq_record = {
            "source_topic":     source_topic,
            "source_partition": source_partition,
            "source_offset":    source_offset,
            "source_timestamp": source_timestamp,
            "source_key":       source_key.decode("utf-8", errors="replace") if source_key else None,
            "failure_reason":   reason,
            "error_type":       error_type,
            "error_message":    error_message[:500],  # truncate to keep DLQ messages small
            "routed_at":        int(time.time() * 1000),  # epoch ms
            "streamshield_version": "0.1.0",
        }

        dlq_logger.warning(
            "Routing message to DLQ: source_topic=%s offset=%d reason=%s dlq_topic=%s",
            source_topic, source_offset, reason, dlq_topic,
        )

        producer = self._get_producer()

        publish_errors: list[str] = []

        def _on_dlq_delivery(err, msg):
            if err:
                publish_errors.append(str(err))

        # Publish the DLQ record as JSON (not Avro — schema may be unavailable)
        for attempt in range(self._config.max_retries):
            publish_errors.clear()
            producer.produce(
                topic       = dlq_topic,
                value       = json.dumps(dlq_record).encode("utf-8"),
                key         = source_key,
                on_delivery = _on_dlq_delivery,
            )
            producer.flush(timeout=10.0)

            if not publish_errors:
                dlq_messages.add(1, {"source_topic": source_topic, "reason": reason})
                dlq_logger.info(
                    "DLQ publish succeeded: dlq_topic=%s source_offset=%d",
                    dlq_topic, source_offset,
                )
                return

            dlq_logger.error(
                "DLQ publish attempt %d/%d failed: %s",
                attempt + 1, self._config.max_retries, publish_errors[0],
            )

        # All retry attempts failed
        if self._config.raise_on_dlq_failure:
            raise DLQPublishError(
                f"Failed to publish to DLQ topic '{dlq_topic}' after {self._config.max_retries} attempts. "
                f"Original message lost: source_topic={source_topic} offset={source_offset}",
                safe_context={
                    "source_topic":     source_topic,
                    "source_partition": source_partition,
                    "source_offset":    source_offset,
                    "dlq_topic":        dlq_topic,
                },
            )
        else:
            dlq_logger.error(
                "DLQ publish failed and raise_on_dlq_failure=False — message dropped: "
                "source_topic=%s offset=%d",
                source_topic, source_offset,
            )

    def close(self) -> None:
        """Flush and close the DLQ producer if it was initialised."""
        if self._producer is not None:
            self._producer.flush(timeout=5.0)
            dlq_logger.debug("DLQ producer closed.")
