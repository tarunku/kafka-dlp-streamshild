"""
Integration test: Schema Registry operations against the real GCP environment.

These tests verify that SchemaAdmin can fetch, check compatibility, and list
schemas against the live Schema Registry in vetsource-496203.
"""

import pytest

from streamshield import SchemaAdmin, SchemaNotFoundError
from tests.integration.conftest import INTEGRATION_SUBJECT


class TestSchemaRegistryIntegration:
    """
    Fetches the schema registered by the POC's register_schema.py and verifies
    that it contains the expected DLP token.* metadata.
    """

    def test_get_latest_schema_returns_schema(self, integration_config):
        admin = SchemaAdmin(integration_config)
        sv = admin.get_latest(INTEGRATION_SUBJECT)

        assert sv.schema_id > 0
        assert sv.subject == INTEGRATION_SUBJECT
        assert sv.version >= 1
        assert sv.schema["name"] == "PrescriptionOrder"

    def test_schema_contains_dlp_metadata(self, integration_config):
        """The schema registered by the POC must carry all token.* properties."""
        admin = SchemaAdmin(integration_config)
        sv = admin.get_latest(INTEGRATION_SUBJECT)
        schema = sv.schema

        assert "token.kms-key" in schema
        assert "token.wrapped-dek" in schema
        assert "token.pci-kms-key" in schema
        assert "token.pci-wrapped-dek" in schema
        assert "token.surrogate-info-type" in schema

    def test_schema_has_tokenized_fields(self, integration_config):
        """Check that the expected sensitive fields are annotated as tokenized."""
        admin = SchemaAdmin(integration_config)
        sv = admin.get_latest(INTEGRATION_SUBJECT)

        tokenized_field_names = [
            f["name"] for f in sv.schema.get("fields", [])
            if f.get("logicalType") == "tokenized"
        ]
        assert "owner_name" in tokenized_field_names
        assert "owner_email" in tokenized_field_names
        assert "owner_phone" in tokenized_field_names
        assert "owner_payment_card" in tokenized_field_names
        assert "pet_name" in tokenized_field_names

    def test_get_by_id_returns_same_schema(self, integration_config):
        admin = SchemaAdmin(integration_config)
        sv = admin.get_latest(INTEGRATION_SUBJECT)
        defn = admin.get_by_id(sv.schema_id)
        assert defn.schema_id == sv.schema_id

    def test_list_subjects_includes_integration_subject(self, integration_config):
        admin = SchemaAdmin(integration_config)
        subjects = admin.list_subjects()
        assert INTEGRATION_SUBJECT in subjects

    def test_unknown_subject_raises_schema_not_found(self, integration_config):
        admin = SchemaAdmin(integration_config)
        with pytest.raises(SchemaNotFoundError):
            admin.get_latest("nonexistent-subject-xyz-999")

    def test_compatibility_check_returns_result(self, integration_config):
        """Checking compatibility should return a CompatibilityResult (not raise)."""
        admin = SchemaAdmin(integration_config)
        sv = admin.get_latest(INTEGRATION_SUBJECT)
        # The same schema must always be compatible with itself
        result = admin.check_compatibility(INTEGRATION_SUBJECT, sv.schema)
        assert result.is_compatible is True
