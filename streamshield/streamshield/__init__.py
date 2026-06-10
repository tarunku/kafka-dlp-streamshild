"""
StreamShield — Production-grade Kafka SDK with GCP DLP tokenization.

Application teams import from 'streamshield' directly. Sub-modules are
internal and their APIs may change without notice.

Quick start:
    from streamshield import KafkaProducer, KafkaConsumer, SDKConfig, GCPConfig

    config = SDKConfig(gcp=GCPConfig(project_id="my-project"))

    with KafkaProducer(config) as producer:
        producer.send("my-topic", value={"id": "1", "name": "Alice"})
"""

# ── Configuration ─────────────────────────────────────────────────────────────
from streamshield.config import (
    CompatibilityMode,
    ConsumerConfig,
    DLPConfig,
    DLQConfig,
    GCPConfig,
    ProducerConfig,
    SchemaConfig,
    SDKConfig,
    SerializationFormat,
)

# ── Producers ─────────────────────────────────────────────────────────────────
from streamshield.producer.producer import AsyncKafkaProducer, KafkaProducer

# ── Consumers ─────────────────────────────────────────────────────────────────
from streamshield.consumer.consumer import AsyncKafkaConsumer, KafkaConsumer

# ── Admin ─────────────────────────────────────────────────────────────────────
from streamshield.schema.registry import SchemaAdmin
from streamshield.topic.admin import TopicAdmin

# ── Observability ─────────────────────────────────────────────────────────────
from streamshield.observability.logging import configure_json_logging
from streamshield.observability.metrics import configure_console_metrics, configure_logging_metrics

# ── Data models ───────────────────────────────────────────────────────────────
from streamshield.schema.models import (
    CompatibilityResult,
    ConsumedMessage,
    MessageMetadata,
    SchemaDefinition,
    SchemaVersion,
    TopicCreationResult,
    TopicMetadata,
)

# ── Exceptions ────────────────────────────────────────────────────────────────
from streamshield.errors.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ConsumerError,
    DeliveryFailedError,
    DeserializationFailedError,
    DetokenizationError,
    DLPError,
    DLQPublishError,
    InvalidConfigError,
    MessageTooLargeError,
    MissingConfigError,
    OffsetCommitError,
    ProducerError,
    SchemaCompatibilityError,
    SchemaError,
    SchemaNotFoundError,
    SchemaRegistrationError,
    SchemaValidationError,
    SerializationError,
    SerializationFailedError,
    StreamShieldError,
    TokenizationError,
    TokenRefreshError,
    TopicCreationError,
    TopicError,
    TopicNotFoundError,
)

__version__ = "0.1.0"

__all__ = [
    # Config
    "SDKConfig", "GCPConfig", "ProducerConfig", "ConsumerConfig",
    "DLPConfig", "DLQConfig", "SchemaConfig",
    "SerializationFormat", "CompatibilityMode",
    # Producers
    "KafkaProducer", "AsyncKafkaProducer",
    # Consumers
    "KafkaConsumer", "AsyncKafkaConsumer",
    # Admin
    "SchemaAdmin", "TopicAdmin",
    # Models
    "ConsumedMessage", "MessageMetadata", "SchemaVersion",
    "SchemaDefinition", "CompatibilityResult", "TopicMetadata", "TopicCreationResult",
    # Exceptions
    "StreamShieldError", "ConfigurationError", "MissingConfigError", "InvalidConfigError",
    "AuthenticationError", "TokenRefreshError",
    "SchemaError", "SchemaNotFoundError", "SchemaRegistrationError",
    "SchemaValidationError", "SchemaCompatibilityError",
    "SerializationError", "SerializationFailedError", "DeserializationFailedError",
    "DLPError", "TokenizationError", "DetokenizationError",
    "TopicError", "TopicNotFoundError", "TopicCreationError",
    "ProducerError", "DeliveryFailedError", "MessageTooLargeError",
    "ConsumerError", "OffsetCommitError", "DLQPublishError",
    # Observability
    "configure_json_logging", "configure_console_metrics", "configure_logging_metrics",
]
