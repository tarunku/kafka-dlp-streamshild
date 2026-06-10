"""Unit tests for Avro serializer and deserializer."""

import io
import struct

import fastavro
import pytest

from streamshield.errors.exceptions import DeserializationFailedError, SchemaValidationError, SerializationFailedError
from streamshield.schema.serializer import AvroSerializer, MAGIC_BYTE
from streamshield.schema.deserializer import AvroDeserializer, HEADER_SIZE


# ── Minimal test schema ───────────────────────────────────────────────────────

RAW_SCHEMA = {
    "type": "record",
    "name": "TestEvent",
    "namespace": "com.test",
    "fields": [
        {"name": "id",    "type": "string"},
        {"name": "value", "type": "int"},
        {"name": "label", "type": ["null", "string"], "default": None},
    ],
}
PARSED_SCHEMA = fastavro.parse_schema(RAW_SCHEMA)
SCHEMA_ID = 42

VALID_RECORD = {"id": "rec-1", "value": 100, "label": "hello"}


class TestAvroSerializer:
    def setup_method(self):
        self.ser = AvroSerializer()

    def test_serialize_returns_bytes(self):
        result = self.ser.serialize(VALID_RECORD, PARSED_SCHEMA, SCHEMA_ID)
        assert isinstance(result, bytes)

    def test_wire_format_magic_byte(self):
        result = self.ser.serialize(VALID_RECORD, PARSED_SCHEMA, SCHEMA_ID)
        assert result[0:1] == b"\x00"

    def test_wire_format_schema_id(self):
        result = self.ser.serialize(VALID_RECORD, PARSED_SCHEMA, SCHEMA_ID)
        _, decoded_schema_id = struct.unpack(">bI", result[:5])
        assert decoded_schema_id == SCHEMA_ID

    def test_round_trip(self):
        """Serialize then deserialize must return the original record."""
        serialized = self.ser.serialize(VALID_RECORD, PARSED_SCHEMA, SCHEMA_ID)
        # Manually deserialize (skipping header)
        buf = io.BytesIO(serialized[HEADER_SIZE:])
        decoded = fastavro.schemaless_reader(buf, PARSED_SCHEMA)
        assert decoded["id"] == VALID_RECORD["id"]
        assert decoded["value"] == VALID_RECORD["value"]

    def test_validate_raises_on_wrong_type(self):
        bad_record = {"id": "rec-1", "value": "not-an-int", "label": None}
        with pytest.raises(SchemaValidationError):
            self.ser.validate(bad_record, PARSED_SCHEMA)

    def test_validate_raises_on_missing_required_field(self):
        incomplete = {"id": "rec-1"}  # missing 'value'
        with pytest.raises(SchemaValidationError):
            self.ser.validate(incomplete, PARSED_SCHEMA)

    def test_validate_passes_for_valid_record(self):
        self.ser.validate(VALID_RECORD, PARSED_SCHEMA)  # must not raise


class TestAvroDeserializer:
    def setup_method(self):
        from unittest.mock import MagicMock
        from streamshield.schema.models import SchemaDefinition

        # Build a mock SchemaRegistryClient
        mock_registry = MagicMock()
        defn = SchemaDefinition(schema_id=SCHEMA_ID, schema=RAW_SCHEMA, schema_type="AVRO")
        mock_registry.get_by_id.return_value = (defn, PARSED_SCHEMA)

        self.ser   = AvroSerializer()
        self.deser = AvroDeserializer(mock_registry)

    def test_deserialize_returns_original_record(self):
        raw = self.ser.serialize(VALID_RECORD, PARSED_SCHEMA, SCHEMA_ID)
        record, raw_schema, schema_id = self.deser.deserialize(raw)
        assert record["id"] == VALID_RECORD["id"]
        assert schema_id == SCHEMA_ID

    def test_deserialize_returns_raw_schema_with_token_metadata(self):
        raw = self.ser.serialize(VALID_RECORD, PARSED_SCHEMA, SCHEMA_ID)
        _, raw_schema, _ = self.deser.deserialize(raw)
        assert raw_schema == RAW_SCHEMA

    def test_deserialize_raises_on_too_short_bytes(self):
        with pytest.raises(DeserializationFailedError, match="too short"):
            self.deser.deserialize(b"\x00\x00")

    def test_deserialize_raises_on_wrong_magic_byte(self):
        # Replace the magic byte with 0x01
        raw = self.ser.serialize(VALID_RECORD, PARSED_SCHEMA, SCHEMA_ID)
        bad_magic = b"\x01" + raw[1:]
        with pytest.raises(DeserializationFailedError, match="magic byte"):
            self.deser.deserialize(bad_magic)
