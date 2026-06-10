"""
Data models for Schema Registry and Kafka message metadata.

These are plain dataclasses — no business logic, no external dependencies.
They are the return types for SchemaAdmin and KafkaConsumer methods.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SchemaDefinition:
    """A schema retrieved by its integer ID from the Schema Registry."""
    schema_id: int
    schema: dict         # raw Avro schema dict, including any token.* metadata
    schema_type: str = "AVRO"


@dataclass
class SchemaVersion:
    """A specific versioned schema for a subject."""
    schema_id: int
    subject: str
    version: int
    schema: dict         # raw Avro schema dict


@dataclass
class CompatibilityResult:
    """Result of a compatibility check before registering a new schema version."""
    is_compatible: bool
    messages: list[str] = field(default_factory=list)  # human-readable reasons if incompatible


@dataclass
class MessageMetadata:
    """
    Delivery confirmation returned by KafkaProducer.send().
    Partition and offset are populated after the broker acknowledges the write.
    """
    topic: str
    partition: int
    offset: int
    timestamp: int       # epoch milliseconds
    key: bytes | None


@dataclass
class ConsumedMessage:
    """
    A fully-processed message returned by KafkaConsumer.poll() or passed to
    the handler in KafkaConsumer.process().

    'value' contains the deserialized record. If detokenize=True was passed to
    poll()/process(), reversible DLP tokens have been replaced with plaintext.
    'raw_schema' carries all token.* metadata so business logic can inspect
    which fields were tokenized and how.
    """
    topic: str
    partition: int
    offset: int
    timestamp: int
    key: bytes | None
    value: dict              # deserialized (and optionally detokenized) record
    raw_schema: dict         # full Avro schema dict from the Schema Registry
    schema_id: int           # schema ID embedded in the Confluent wire-format header
    headers: dict[str, bytes] = field(default_factory=dict)


@dataclass
class TopicMetadata:
    """Metadata returned by TopicAdmin.describe_topic()."""
    name: str
    partitions: int
    replication_factor: int
    config: dict[str, str] = field(default_factory=dict)


@dataclass
class TopicCreationResult:
    """Result of TopicAdmin.create_topic() or create_dlq_topic()."""
    name: str
    partitions: int
    replication_factor: int
    created: bool   # False if the topic already existed
