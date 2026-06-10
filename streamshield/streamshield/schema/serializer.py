"""
Avro serializer — Confluent wire format.

The Confluent wire format prepends a 5-byte header to every Avro message:
    Byte 0:   Magic byte — always 0x00
    Bytes 1-4: Schema ID — 4-byte big-endian unsigned integer

This header lets consumers look up the correct schema from the Schema Registry
using only the message bytes, without any out-of-band configuration.

Reference: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html
"""

from __future__ import annotations

import io
import struct

import fastavro

from streamshield.errors.exceptions import SchemaValidationError, SerializationFailedError
from streamshield.observability.logging import schema_logger


# The Confluent wire format magic byte — identifies Avro-serialized messages
MAGIC_BYTE = 0


class AvroSerializer:
    """
    Serializes Python dicts to Avro bytes using the Confluent wire format.

    The serializer pre-validates the record against the schema before writing,
    so SchemaValidationError is raised before any Kafka or DLP I/O occurs.
    """

    def validate(self, record: dict, parsed_schema: object) -> None:
        """
        Validate a record against the parsed Avro schema.

        This is a dry-run serialization — we write to a throw-away buffer and
        check for errors. Called by the producer before DLP tokenization so
        invalid records fail immediately at the application boundary.

        Raises:
            SchemaValidationError if the record does not match the schema.
        """
        try:
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, parsed_schema, record)
        except (ValueError, TypeError, Exception) as exc:
            raise SchemaValidationError(
                f"Record does not conform to schema: {exc}",
                safe_context={"error": str(exc)},
            ) from exc

    def serialize(self, record: dict, parsed_schema: object, schema_id: int) -> bytes:
        """
        Serialize a record to Confluent-format Avro bytes.

        Format: [0x00][schema_id: 4 bytes big-endian][avro payload bytes]

        Args:
            record:        The Python dict to serialize (must already be tokenized).
            parsed_schema: Result of fastavro.parse_schema() — must be called once
                           after fetching the raw schema from the registry.
            schema_id:     Integer ID assigned by the Schema Registry.

        Returns:
            Bytes ready to pass as the Kafka message value.

        Raises:
            SerializationFailedError on fastavro write errors.
        """
        try:
            buf = io.BytesIO()
            # Write the 5-byte Confluent header: magic byte + 4-byte schema ID
            buf.write(struct.pack(">bI", MAGIC_BYTE, schema_id))
            # Write the Avro payload (no Avro container — raw bytes only)
            fastavro.schemaless_writer(buf, parsed_schema, record)
            schema_logger.debug("Serialized record to %d bytes (schema_id=%d)", buf.tell(), schema_id)
            return buf.getvalue()
        except Exception as exc:
            raise SerializationFailedError(
                f"Failed to serialize record to Avro: {exc}",
                safe_context={"schema_id": schema_id, "error": str(exc)},
            ) from exc
