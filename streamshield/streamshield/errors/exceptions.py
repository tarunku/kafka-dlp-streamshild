"""
StreamShield exception hierarchy.

All SDK exceptions inherit from StreamShieldError so callers can catch the entire
SDK surface with a single except clause, or catch specific subtypes for fine-grained
handling.

Design rules:
  - Exceptions carry a 'safe_context' dict that is always safe to log (no PII, no key
    material, no record values).
  - The original exception from the underlying library is always chained via __cause__
    so stack traces are fully preserved.
  - Error messages describe WHAT failed and WHERE. They never include record field values.
"""

from __future__ import annotations


class StreamShieldError(Exception):
    """Base class for all StreamShield SDK exceptions."""

    def __init__(self, message: str, safe_context: dict | None = None):
        super().__init__(message)
        # safe_context holds metadata (topic name, schema id, field names) that is safe
        # to include in logs. Never put record values or key material here.
        self.safe_context: dict = safe_context or {}


# ── Configuration errors ──────────────────────────────────────────────────────

class ConfigurationError(StreamShieldError):
    """Raised when the SDKConfig is invalid or incomplete."""


class MissingConfigError(ConfigurationError):
    """A required configuration field was not provided."""


class InvalidConfigError(ConfigurationError):
    """A configuration field is present but has an invalid value."""


# ── Authentication errors ─────────────────────────────────────────────────────

class AuthenticationError(StreamShieldError):
    """GCP Application Default Credentials or Kafka SASL authentication failed."""


class TokenRefreshError(AuthenticationError):
    """The ADC OAuth2 token could not be refreshed."""


# ── Schema errors ─────────────────────────────────────────────────────────────

class SchemaError(StreamShieldError):
    """Base class for Schema Registry related failures."""


class SchemaNotFoundError(SchemaError):
    """No schema registered for the given subject or schema ID."""


class SchemaRegistrationError(SchemaError):
    """The attempt to register a new schema version failed."""


class SchemaValidationError(SchemaError):
    """
    A record does not conform to the expected Avro schema.
    Raised before any Kafka or DLP I/O — the record is rejected at the SDK boundary.
    """


class SchemaCompatibilityError(SchemaError):
    """
    The new schema is incompatible with the existing version under the configured
    compatibility mode. 'messages' contains human-readable reasons.
    """

    def __init__(self, message: str, messages: list[str] | None = None, safe_context: dict | None = None):
        super().__init__(message, safe_context)
        # Human-readable list of compatibility failure reasons from the Schema Registry
        self.messages: list[str] = messages or []


# ── Serialization errors ──────────────────────────────────────────────────────

class SerializationError(StreamShieldError):
    """Base class for Avro serialization and deserialization failures."""


class SerializationFailedError(SerializationError):
    """Failed to serialize a record to Avro bytes. Includes the field name that caused the error."""


class DeserializationFailedError(SerializationError):
    """Failed to deserialize Avro bytes back to a record. Includes raw bytes length for debugging."""


# ── DLP errors ────────────────────────────────────────────────────────────────

class DLPError(StreamShieldError):
    """Base class for Cloud DLP API failures."""


class TokenizationError(DLPError):
    """
    Cloud DLP deidentifyContent failed during tokenization.
    safe_context includes: project_id, field_names, operation='tokenize'
    """


class DetokenizationError(DLPError):
    """
    Cloud DLP reidentifyContent failed during de-tokenization.
    safe_context includes: project_id, field_names, operation='detokenize'
    """


# ── Topic errors ──────────────────────────────────────────────────────────────

class TopicError(StreamShieldError):
    """Base class for Kafka topic management failures."""


class TopicNotFoundError(TopicError):
    """The requested Kafka topic does not exist."""


class TopicCreationError(TopicError):
    """Topic creation failed."""


# ── Producer errors ───────────────────────────────────────────────────────────

class ProducerError(StreamShieldError):
    """Base class for Kafka producer failures."""


class DeliveryFailedError(ProducerError):
    """
    Kafka broker did not acknowledge message delivery.
    safe_context includes: topic, partition, key (if available), error_code
    """


class MessageTooLargeError(ProducerError):
    """The serialized message exceeds the broker's max.message.bytes limit."""


# ── Consumer errors ───────────────────────────────────────────────────────────

class ConsumerError(StreamShieldError):
    """Base class for Kafka consumer failures."""


class OffsetCommitError(ConsumerError):
    """Offset commit to the broker failed."""


class DLQPublishError(ConsumerError):
    """
    Publishing to the Dead Letter Queue topic failed.
    The original record cannot be recovered. Operator intervention required.
    safe_context includes: source_topic, source_partition, source_offset, dlq_topic
    """
