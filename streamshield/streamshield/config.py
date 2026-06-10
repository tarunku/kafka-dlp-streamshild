"""
StreamShield SDK configuration.

All configuration flows through a single SDKConfig dataclass. Application teams create
one config object and pass it to every SDK component (KafkaProducer, KafkaConsumer,
SchemaAdmin, TopicAdmin). There are no loose constructor parameters.

Loading priority (highest to lowest):
  1. Values set directly in the dataclass constructors
  2. SDKConfig.from_yaml(path)
  3. SDKConfig.from_env()  — env vars with STREAMSHIELD_ prefix
  4. GCP Secret Manager  — for bootstrap_servers and schema_registry_url only

Usage:
    # Minimal — everything else uses safe defaults
    config = SDKConfig(gcp=GCPConfig(project_id="my-project"))

    # From YAML file
    config = SDKConfig.from_yaml("/etc/streamshield/config.yaml")

    # From environment variables
    config = SDKConfig.from_env()
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import yaml

from streamshield.errors.exceptions import InvalidConfigError, MissingConfigError


class SerializationFormat(Enum):
    """Wire format for Kafka message values. Only Avro is supported in v1.0."""
    AVRO = "AVRO"


class CompatibilityMode(Enum):
    """
    Schema Registry compatibility modes. Controls what schema changes are allowed
    when a new version is registered.

    BACKWARD           — New schema can read data written with the previous schema.
                         Safe to add optional fields. Cannot remove required fields.
    FORWARD            — Previous schema can read data written with the new schema.
                         Safe to remove optional fields. Cannot add required fields.
    FULL               — Both BACKWARD and FORWARD. Most restrictive.
    BACKWARD_TRANSITIVE — BACKWARD against ALL historical versions, not just the latest.
    FORWARD_TRANSITIVE  — FORWARD against ALL historical versions.
    FULL_TRANSITIVE     — FULL against ALL historical versions.
    NONE               — No compatibility checks. Use with extreme caution.
    """
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"


@dataclass
class GCPConfig:
    """
    GCP-specific configuration: project, DLP location, and how to resolve
    bootstrap servers and schema registry URL.

    If use_secret_manager=True (default), bootstrap_servers and schema_registry_url
    are fetched from GCP Secret Manager at startup using the configured secret names.
    Set bootstrap_servers and schema_registry_url directly to bypass Secret Manager
    (useful in local development or CI environments).
    """

    # GCP project ID — required
    project_id: str = ""

    # Location for Cloud DLP API calls. "global" works for all data residency tiers.
    dlp_location: str = "global"

    # When True, bootstrap_servers and schema_registry_url are loaded from Secret Manager
    use_secret_manager: bool = True

    # Secret names in GCP Secret Manager (only used when use_secret_manager=True)
    bootstrap_servers_secret: str = "kafka-bootstrap-servers"
    schema_registry_url_secret: str = "schema-registry-url"

    # Direct values — set these to skip Secret Manager entirely
    bootstrap_servers: str | None = None
    schema_registry_url: str | None = None

    # Proactively refresh the ADC/Bearer token this many seconds before it expires.
    # Default: 5 minutes. Prevents SASL auth failures in long-running consumers.
    token_refresh_buffer_s: int = 300

    # If set, re-fetch Secret Manager secrets on this interval (seconds).
    # Useful when secrets are rotated without restarting the process.
    secrets_refresh_interval_s: int | None = None


@dataclass
class ProducerConfig:
    """
    confluent_kafka Producer tuning parameters.

    Idempotence is enabled by default (enable_idempotence=True, acks='all') to prevent
    duplicate messages during retries. These settings are required for production use.
    """

    enable_idempotence: bool = True   # prevents duplicate messages on retry
    acks: str = "all"                 # wait for all ISR replicas to acknowledge
    retries: int = 5                  # broker-level retries before raising
    retry_backoff_ms: int = 500       # wait between broker retries
    linger_ms: int = 5                # wait up to 5ms to batch messages before sending
    batch_size_bytes: int = 65536     # max bytes per batch (64 KB)
    compression_type: str = "snappy"  # snappy gives good balance of speed and ratio
    request_timeout_ms: int = 30000   # 30 seconds per produce request
    delivery_timeout_ms: int = 120000 # 2 minutes total delivery timeout

    # When True, KafkaProducer checks that the topic exists before the first send().
    # Prevents misconfigured topic names from going undetected until the first message.
    validate_topic_on_send: bool = True


@dataclass
class ConsumerConfig:
    """
    confluent_kafka Consumer tuning parameters.

    NOTE: enable.auto.commit is intentionally not exposed here. StreamShield always
    sets it to False and manages offset commits explicitly after successful processing.
    This prevents data loss when downstream sinks (e.g. Snowflake) fail mid-write.
    """

    auto_offset_reset: str = "earliest"    # start from oldest message if no committed offset
    max_poll_records: int = 500            # max records returned per poll() call
    session_timeout_ms: int = 30000        # consumer considered dead after 30s without heartbeat
    heartbeat_interval_ms: int = 3000      # send heartbeat every 3s (must be < session_timeout/3)
    max_poll_interval_ms: int = 300000     # max time between poll() calls before rebalance (5 min)
    idle_timeout_s: float = 30.0           # exit process() loop after this many seconds with no messages


@dataclass
class DLQConfig:
    """
    Dead Letter Queue configuration.

    When enabled, messages that fail deserialization, DLP processing, or business logic
    are routed to a DLQ topic instead of crashing the consumer. This allows the pipeline
    to continue while failed records are preserved for inspection and replay.
    """

    enabled: bool = True
    topic_suffix: str = ".dlq"     # DLQ topic name = source topic + this suffix
    max_retries: int = 3           # attempts to publish to DLQ before raising DLQPublishError

    # When True, raise DLQPublishError if the DLQ itself is unavailable.
    # When False, log an error and move on (accept potential data loss as last resort).
    raise_on_dlq_failure: bool = True

    # When True, automatically create the DLQ topic on first route if it doesn't exist.
    auto_create_topic: bool = True


@dataclass
class DLPConfig:
    """
    Google Cloud DLP tokenization configuration.

    Batching is the most important performance setting. By default, up to 100 records
    are tokenized in a single DLP API call. The POC called DLP once per record —
    at 100 records/call this SDK is up to 100x faster for bulk workloads.
    """

    enabled: bool = True

    # Maximum records per DLP deidentify/reidentify API call.
    # DLP supports up to 5000 rows per table request; 100 is a safe default.
    batch_size: int = 100

    # Field used as the CryptoDeterministicConfig context (ties a token to a specific record).
    # The schema annotation "token.context-field" overrides this per-schema if set.
    context_field: str = "order_id"

    # Retries for transient DLP errors (UNAVAILABLE, RESOURCE_EXHAUSTED).
    max_retries: int = 3
    retry_backoff_ms: int = 500


@dataclass
class SchemaConfig:
    """
    Avro Schema Registry configuration.

    Schema registration is disabled by default (auto_register=False). Topics must have
    schemas registered by an operator before producers can send. This prevents accidental
    schema evolution in production.
    """

    serialization_format: SerializationFormat = SerializationFormat.AVRO

    # When False (default), raise SchemaNotFoundError if no schema is registered.
    # When True, register the schema on the first send() call automatically.
    auto_register: bool = False

    # Default compatibility mode applied when registering a new schema.
    default_compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD

    # Subject naming strategy:
    #   "TopicNameStrategy"       — subject = "{topic}-value"  (default, matches POC)
    #   "RecordNameStrategy"      — subject = "{avro_namespace}.{avro_record_name}"
    #   "TopicRecordNameStrategy" — subject = "{topic}-{avro_namespace}.{avro_record_name}"
    subject_name_strategy: str = "TopicNameStrategy"

    # Number of schemas to keep in the in-process cache (keyed by schema_id).
    cache_capacity: int = 1000


@dataclass
class SDKConfig:
    """
    Top-level StreamShield configuration object.

    Pass one instance of this class to every SDK component. All nested configs have
    safe defaults so most applications only need to set GCPConfig.project_id.

    Examples:
        # Minimal setup — secrets loaded from Secret Manager
        config = SDKConfig(gcp=GCPConfig(project_id="my-project"))

        # Skip Secret Manager — provide values directly (good for local dev)
        config = SDKConfig(
            gcp=GCPConfig(
                project_id="my-project",
                use_secret_manager=False,
                bootstrap_servers="localhost:9092",
                schema_registry_url="https://my-registry/",
            )
        )
    """

    gcp: GCPConfig = field(default_factory=GCPConfig)
    producer: ProducerConfig = field(default_factory=ProducerConfig)
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    dlq: DLQConfig = field(default_factory=DLQConfig)
    dlp: DLPConfig = field(default_factory=DLPConfig)
    schema: SchemaConfig = field(default_factory=SchemaConfig)

    # ── Class-level constructors ──────────────────────────────────────────────

    @classmethod
    def from_yaml(cls, path: str) -> "SDKConfig":
        """
        Load configuration from a YAML file.

        The YAML structure mirrors the dataclass hierarchy. Keys not present in the
        file fall back to dataclass defaults. See examples/streamshield-config.yaml.
        """
        with open(path) as f:
            data: dict[str, Any] = yaml.safe_load(f) or {}

        gcp_data = data.get("gcp", {})
        config = cls(
            gcp=GCPConfig(**{k: v for k, v in gcp_data.items() if hasattr(GCPConfig, k)}),
            producer=ProducerConfig(**{k: v for k, v in data.get("producer", {}).items() if hasattr(ProducerConfig, k)}),
            consumer=ConsumerConfig(**{k: v for k, v in data.get("consumer", {}).items() if hasattr(ConsumerConfig, k)}),
            dlq=DLQConfig(**{k: v for k, v in data.get("dlq", {}).items() if hasattr(DLQConfig, k)}),
            dlp=DLPConfig(**{k: v for k, v in data.get("dlp", {}).items() if hasattr(DLPConfig, k)}),
            schema=SchemaConfig(**{k: v for k, v in data.get("schema", {}).items() if hasattr(SchemaConfig, k)}),
        )
        # Convert string enum values loaded from YAML
        if isinstance(config.schema.serialization_format, str):
            config.schema.serialization_format = SerializationFormat(config.schema.serialization_format)
        if isinstance(config.schema.default_compatibility_mode, str):
            config.schema.default_compatibility_mode = CompatibilityMode(config.schema.default_compatibility_mode)
        config.validate()
        return config

    @classmethod
    def from_env(cls) -> "SDKConfig":
        """
        Load configuration from environment variables with the STREAMSHIELD_ prefix.

        Examples:
            STREAMSHIELD_GCP_PROJECT_ID=my-project
            STREAMSHIELD_GCP_USE_SECRET_MANAGER=false
            STREAMSHIELD_GCP_BOOTSTRAP_SERVERS=localhost:9092
            STREAMSHIELD_DLP_BATCH_SIZE=50
            STREAMSHIELD_DLP_ENABLED=true
        """
        def env(key: str, default: Any = None) -> Any:
            return os.environ.get(f"STREAMSHIELD_{key.upper()}", default)

        def env_bool(key: str, default: bool) -> bool:
            val = env(key)
            if val is None:
                return default
            return str(val).lower() in ("true", "1", "yes")

        def env_int(key: str, default: int) -> int:
            val = env(key)
            return int(val) if val is not None else default

        config = cls(
            gcp=GCPConfig(
                project_id=env("GCP_PROJECT_ID", ""),
                dlp_location=env("GCP_DLP_LOCATION", "global"),
                use_secret_manager=env_bool("GCP_USE_SECRET_MANAGER", True),
                bootstrap_servers_secret=env("GCP_BOOTSTRAP_SERVERS_SECRET", "kafka-bootstrap-servers"),
                schema_registry_url_secret=env("GCP_SCHEMA_REGISTRY_URL_SECRET", "schema-registry-url"),
                bootstrap_servers=env("GCP_BOOTSTRAP_SERVERS"),
                schema_registry_url=env("GCP_SCHEMA_REGISTRY_URL"),
                token_refresh_buffer_s=env_int("GCP_TOKEN_REFRESH_BUFFER_S", 300),
            ),
            dlp=DLPConfig(
                enabled=env_bool("DLP_ENABLED", True),
                batch_size=env_int("DLP_BATCH_SIZE", 100),
                context_field=env("DLP_CONTEXT_FIELD", "order_id"),
                max_retries=env_int("DLP_MAX_RETRIES", 3),
            ),
        )
        config.validate()
        return config

    # ── Validation ────────────────────────────────────────────────────────────

    def validate(self) -> None:
        """
        Validate the configuration. Raises an exception immediately so the error is
        caught at startup rather than mid-flight during message production or consumption.
        """
        if not self.gcp.project_id:
            raise MissingConfigError(
                "GCPConfig.project_id is required. "
                "Set it directly or via STREAMSHIELD_GCP_PROJECT_ID."
            )

        # Reject plaintext HTTP for Schema Registry — it carries key references in schema metadata
        if self.gcp.schema_registry_url and self.gcp.schema_registry_url.startswith("http://"):
            raise InvalidConfigError(
                "schema_registry_url must use HTTPS, not HTTP. "
                f"Got: {self.gcp.schema_registry_url!r}"
            )

        if self.dlp.batch_size < 1 or self.dlp.batch_size > 5000:
            raise InvalidConfigError(
                f"DLPConfig.batch_size must be between 1 and 5000. Got: {self.dlp.batch_size}"
            )

        if self.consumer.max_poll_interval_ms < self.consumer.session_timeout_ms:
            raise InvalidConfigError(
                "ConsumerConfig.max_poll_interval_ms must be >= session_timeout_ms. "
                f"Got: max_poll_interval_ms={self.consumer.max_poll_interval_ms}, "
                f"session_timeout_ms={self.consumer.session_timeout_ms}"
            )

        if self.producer.acks not in ("0", "1", "all", "-1"):
            raise InvalidConfigError(
                f"ProducerConfig.acks must be '0', '1', 'all', or '-1'. Got: {self.producer.acks!r}"
            )

        valid_strategies = {"TopicNameStrategy", "RecordNameStrategy", "TopicRecordNameStrategy"}
        if self.schema.subject_name_strategy not in valid_strategies:
            raise InvalidConfigError(
                f"SchemaConfig.subject_name_strategy must be one of {valid_strategies}. "
                f"Got: {self.schema.subject_name_strategy!r}"
            )

    # ── Safe logging helper ───────────────────────────────────────────────────

    def to_safe_dict(self) -> dict:
        """
        Returns the configuration as a dict with all sensitive values masked.
        Safe to include in log output. Key material is never logged.

        Fields masked: anything in the config whose key contains 'dek', 'key', 'secret',
        'token', 'wrapped', or 'password'.
        """
        _SENSITIVE_SUBSTRINGS = {"dek", "key", "secret", "token", "wrapped", "password"}

        def mask(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {
                    k: "***" if any(s in k.lower() for s in _SENSITIVE_SUBSTRINGS) else mask(v)
                    for k, v in obj.items()
                }
            if isinstance(obj, (list, tuple)):
                return [mask(item) for item in obj]
            return obj

        import dataclasses
        raw = dataclasses.asdict(self)
        return mask(raw)
