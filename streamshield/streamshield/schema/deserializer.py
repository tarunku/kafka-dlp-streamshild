"""
Avro deserializer — Confluent wire format.

Reads the 5-byte Confluent header, extracts the schema_id, fetches the
corresponding schema from the SchemaRegistryClient cache (or registry),
then decodes the Avro payload.

The raw_schema dict returned alongside the record carries all token.*
metadata so the DLP detokenizer can read it without additional lookups.
"""

from __future__ import annotations

import io
import struct

import fastavro

from streamshield.errors.exceptions import DeserializationFailedError
from streamshield.observability.logging import schema_logger
from streamshield.schema.registry import SchemaRegistryClient

# The Confluent magic byte — any other value means the message is not Avro
MAGIC_BYTE = 0
HEADER_SIZE = 5  # 1 magic byte + 4 schema_id bytes


class AvroDeserializer:
    """
    Deserializes Confluent-format Avro bytes back to Python dicts.

    Uses SchemaRegistryClient for schema lookups, so schemas are cached
    across many messages — only the first message with a given schema_id
    triggers a registry HTTP call.
    """

    def __init__(self, registry_client: SchemaRegistryClient):
        self._registry = registry_client

    def deserialize(self, raw_bytes: bytes) -> tuple[dict, dict, int]:
        """
        Deserialize a Confluent Avro message.

        Args:
            raw_bytes: The raw bytes from msg.value() in the consumer.

        Returns:
            (record_dict, raw_schema_dict, schema_id)
            raw_schema_dict includes all token.* metadata for DLP processing.

        Raises:
            DeserializationFailedError on malformed bytes, unexpected magic byte,
            or fastavro read errors.
        """
        if len(raw_bytes) < HEADER_SIZE:
            raise DeserializationFailedError(
                f"Message too short to be a valid Confluent Avro message. "
                f"Expected at least {HEADER_SIZE} bytes, got {len(raw_bytes)}.",
                safe_context={"bytes_received": len(raw_bytes)},
            )

        try:
            magic, schema_id = struct.unpack(">bI", raw_bytes[:HEADER_SIZE])
        except struct.error as exc:
            raise DeserializationFailedError(
                f"Failed to unpack Confluent wire-format header: {exc}",
                safe_context={"bytes_received": len(raw_bytes)},
            ) from exc

        if magic != MAGIC_BYTE:
            raise DeserializationFailedError(
                f"Unexpected magic byte: expected 0x00, got {magic!r}. "
                "Is this message from a non-Confluent producer?",
                safe_context={"magic_byte": magic, "schema_id": schema_id},
            )

        try:
            schema_defn, parsed_schema = self._registry.get_by_id(schema_id)
        except Exception as exc:
            raise DeserializationFailedError(
                f"Could not fetch schema (id={schema_id}) for deserialization: {exc}",
                safe_context={"schema_id": schema_id},
            ) from exc

        try:
            buf = io.BytesIO(raw_bytes[HEADER_SIZE:])
            record: dict = fastavro.schemaless_reader(buf, parsed_schema)
        except Exception as exc:
            raise DeserializationFailedError(
                f"fastavro failed to decode Avro payload (schema_id={schema_id}): {exc}",
                safe_context={"schema_id": schema_id, "error": str(exc)},
            ) from exc

        schema_logger.debug("Deserialized record from %d bytes (schema_id=%d)", len(raw_bytes), schema_id)
        return record, schema_defn.schema, schema_id
