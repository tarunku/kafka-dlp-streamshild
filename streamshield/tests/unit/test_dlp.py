"""
Unit tests for streamshield.dlp.tokenizer, .detokenizer, and .policy.

All DLP API calls are mocked — no GCP credentials required.
"""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch, call

import pytest

from streamshield.config import DLPConfig
from streamshield.dlp.policy import (
    get_context_field,
    get_reversible_fields,
    get_tokenized_fields,
    is_tokenized_value,
)
from streamshield.dlp.tokenizer import DLPTokenizer
from streamshield.dlp.detokenizer import DLPDetokenizer
from streamshield.errors.exceptions import TokenizationError, DetokenizationError


# ── Fixtures ──────────────────────────────────────────────────────────────────

WRAPPED_DEK_B64 = base64.b64encode(b"fake-wrapped-dek-32-bytes-padding!").decode()
PCI_WRAPPED_DEK_B64 = base64.b64encode(b"fake-pci-wrapped-dek-32-bytes-pad!").decode()

SAMPLE_SCHEMA = {
    "type": "record",
    "name": "PrescriptionOrder",
    "namespace": "com.test",
    "token.kms-key":               "projects/p/locations/global/keyRings/r/cryptoKeys/pii-kek",
    "token.wrapped-dek":           WRAPPED_DEK_B64,
    "token.pci-kms-key":           "projects/p/locations/global/keyRings/r/cryptoKeys/pci-kek",
    "token.pci-wrapped-dek":       PCI_WRAPPED_DEK_B64,
    "token.surrogate-info-type":   "PII_TOKEN",
    "token.pci-surrogate-info-type": "PCI_TOKEN",
    "token.default-sensitivity":   "PII",
    "token.default-reversible":    "true",
    "fields": [
        {"name": "order_id",   "type": "string"},
        {"name": "medication", "type": "string"},
        {
            "name": "owner_name",
            "type": "string",
            "logicalType": "tokenized",
            "token.method": "CryptoDeterministicConfig",
        },
        {
            "name": "owner_email",
            "type": "string",
            "logicalType": "tokenized",
            "token.method": "CryptoDeterministicConfig",
        },
        {
            "name": "owner_phone",
            "type": "string",
            "logicalType": "tokenized",
            "token.method": "CryptoHashConfig",
            "token.reversible": "false",   # irreversible
        },
        {
            "name": "owner_payment_card",
            "type": "string",
            "logicalType": "tokenized",
            "token.method": "CryptoReplaceFfxFpeConfig",
            "token.sensitivity": "PCI-DSS",
        },
    ],
}

SAMPLE_RECORD = {
    "order_id":           "RX-001",
    "medication":         "Carprofen",
    "owner_name":         "Alice Smith",
    "owner_email":        "alice@example.com",
    "owner_phone":        "+1-555-0100",
    "owner_payment_card": "4111111111111111",
}


def _make_dlp_response(field_names: list[str], records: list[dict]) -> MagicMock:
    """Build a fake DLP API response that mirrors the input rows with 'TOK_' prefixed values."""
    rows = []
    for record in records:
        values = []
        for name in field_names:
            original = str(record.get(name, ""))
            token = f"TOK_{original}" if name != "order_id" else original
            mock_val = MagicMock()
            mock_val.string_value = token
            values.append(mock_val)
        mock_row = MagicMock()
        mock_row.values = values
        rows.append(mock_row)

    mock_response = MagicMock()
    mock_response.item.table.rows = rows
    return mock_response


# ── Policy tests ──────────────────────────────────────────────────────────────

class TestPolicy:
    def test_get_tokenized_fields_returns_annotated_fields(self):
        fields = get_tokenized_fields(SAMPLE_SCHEMA)
        names = [f["name"] for f in fields]
        assert "owner_name" in names
        assert "owner_email" in names
        assert "owner_phone" in names
        assert "owner_payment_card" in names
        # Non-tokenized fields must not appear
        assert "order_id" not in names
        assert "medication" not in names

    def test_get_reversible_fields_excludes_hash(self):
        fields = get_reversible_fields(SAMPLE_SCHEMA)
        names = [f["name"] for f in fields]
        assert "owner_name" in names
        assert "owner_payment_card" in names
        # owner_phone has token.reversible=false — must be excluded
        assert "owner_phone" not in names

    def test_get_context_field_uses_config_default(self):
        assert get_context_field(SAMPLE_SCHEMA, "order_id") == "order_id"

    def test_get_context_field_reads_schema_annotation(self):
        schema_with_annotation = {**SAMPLE_SCHEMA, "token.context-field": "transaction_id"}
        assert get_context_field(schema_with_annotation, "order_id") == "transaction_id"

    def test_is_tokenized_value_detects_pii_prefix(self):
        assert is_tokenized_value("PII_TOKEN(14):abc123", "PII_TOKEN") is True

    def test_is_tokenized_value_no_prefix_returns_false(self):
        assert is_tokenized_value("4111111111111111", "PII_TOKEN") is False

    def test_is_tokenized_value_wrong_type_returns_false(self):
        assert is_tokenized_value(12345, "PII_TOKEN") is False


# ── Tokenizer tests ───────────────────────────────────────────────────────────

class TestDLPTokenizer:
    def _make_tokenizer(self) -> DLPTokenizer:
        return DLPTokenizer(DLPConfig(batch_size=100), "test-project")

    def test_tokenize_calls_dlp_once_for_single_record(self):
        tokenizer = self._make_tokenizer()
        field_names = ["owner_name", "owner_email", "owner_phone", "owner_payment_card"]

        mock_response = _make_dlp_response(
            field_names + ["order_id"], [SAMPLE_RECORD]
        )
        # Build the expected row value list
        mock_response.item.table.rows[0].values = [
            MagicMock(string_value=f"TOK_{SAMPLE_RECORD.get(n, '')}") for n in field_names
        ]

        with patch.object(tokenizer._client, "deidentify_content", return_value=mock_response) as mock_dlp:
            result = tokenizer.tokenize(SAMPLE_RECORD, SAMPLE_SCHEMA)

        # DLP called exactly once
        assert mock_dlp.call_count == 1
        # Non-tokenized fields are preserved unchanged
        assert result["order_id"] == "RX-001"
        assert result["medication"] == "Carprofen"

    def test_tokenize_batch_100_records_makes_one_dlp_call(self):
        """100 records with batch_size=100 → exactly 1 DLP API call."""
        tokenizer = self._make_tokenizer()
        records = [dict(SAMPLE_RECORD, order_id=f"RX-{i:03d}") for i in range(100)]
        field_names = ["owner_name", "owner_email", "owner_phone", "owner_payment_card"]

        # Build response with 100 rows
        mock_rows = []
        for _ in records:
            row = MagicMock()
            row.values = [MagicMock(string_value="TOK_VALUE")] * len(field_names)
            mock_rows.append(row)

        mock_response = MagicMock()
        mock_response.item.table.rows = mock_rows

        with patch.object(tokenizer._client, "deidentify_content", return_value=mock_response) as mock_dlp:
            results = tokenizer.tokenize_batch(records, SAMPLE_SCHEMA)

        assert mock_dlp.call_count == 1
        assert len(results) == 100

    def test_tokenize_batch_250_records_makes_three_dlp_calls(self):
        """250 records with batch_size=100 → 3 DLP calls (100+100+50)."""
        tokenizer = self._make_tokenizer()
        records = [dict(SAMPLE_RECORD, order_id=f"RX-{i:03d}") for i in range(250)]
        field_names = ["owner_name", "owner_email", "owner_phone", "owner_payment_card"]

        def mock_deidentify(request):
            row_count = len(request["item"]["table"]["rows"])
            mock_rows = []
            for _ in range(row_count):
                row = MagicMock()
                row.values = [MagicMock(string_value="TOK")] * len(field_names)
                mock_rows.append(row)
            resp = MagicMock()
            resp.item.table.rows = mock_rows
            return resp

        with patch.object(tokenizer._client, "deidentify_content", side_effect=mock_deidentify) as mock_dlp:
            results = tokenizer.tokenize_batch(records, SAMPLE_SCHEMA)

        assert mock_dlp.call_count == 3
        assert len(results) == 250

    def test_tokenize_disabled_returns_records_unchanged(self):
        tokenizer = DLPTokenizer(DLPConfig(enabled=False), "test-project")
        result = tokenizer.tokenize(SAMPLE_RECORD, SAMPLE_SCHEMA)
        assert result == SAMPLE_RECORD  # no changes

    def test_tokenize_no_tokenized_fields_skips_dlp(self):
        """Schema with no logicalType=tokenized fields → DLP not called."""
        schema_no_tokens = {
            "type": "record", "name": "Plain",
            "fields": [{"name": "id", "type": "string"}],
        }
        tokenizer = self._make_tokenizer()
        with patch.object(tokenizer._client, "deidentify_content") as mock_dlp:
            result = tokenizer.tokenize({"id": "1"}, schema_no_tokens)
        mock_dlp.assert_not_called()
        assert result == {"id": "1"}

    def test_tokenize_raises_tokenization_error_on_dlp_failure(self):
        from google.api_core.exceptions import ServiceUnavailable
        tokenizer = DLPTokenizer(DLPConfig(batch_size=100, max_retries=0), "test-project")
        with patch.object(tokenizer._client, "deidentify_content", side_effect=ServiceUnavailable("down")):
            with pytest.raises(TokenizationError):
                tokenizer.tokenize(SAMPLE_RECORD, SAMPLE_SCHEMA)

    def test_result_order_matches_input_order_in_batch(self):
        """Results must be in the same order as the input records."""
        tokenizer = self._make_tokenizer()
        records = [dict(SAMPLE_RECORD, order_id=f"RX-{i}") for i in range(5)]
        field_names = ["owner_name", "owner_email", "owner_phone", "owner_payment_card"]

        def mock_deidentify(request):
            row_count = len(request["item"]["table"]["rows"])
            mock_rows = []
            for i in range(row_count):
                row = MagicMock()
                # Encode the row index into the token so we can verify order
                row.values = [MagicMock(string_value=f"TOK_{i}")] * len(field_names)
                mock_rows.append(row)
            resp = MagicMock()
            resp.item.table.rows = mock_rows
            return resp

        with patch.object(tokenizer._client, "deidentify_content", side_effect=mock_deidentify):
            results = tokenizer.tokenize_batch(records, SAMPLE_SCHEMA)

        # Each result should correspond to the row at the same index
        for i, result in enumerate(results):
            assert result["owner_name"] == f"TOK_{i}"


# ── Detokenizer tests ─────────────────────────────────────────────────────────

class TestDLPDetokenizer:
    def _make_detokenizer(self) -> DLPDetokenizer:
        return DLPDetokenizer(DLPConfig(batch_size=100), "test-project")

    def _tokenized_record(self) -> dict:
        return {
            "order_id":           "RX-001",
            "medication":         "Carprofen",
            "owner_name":         "PII_TOKEN(14):abc",
            "owner_email":        "PII_TOKEN(14):def",
            "owner_phone":        "PII_TOKEN(12):xyz",  # irreversible — will stay as-is
            "owner_payment_card": "5412753489210033",
        }

    def test_detokenize_skips_irreversible_fields(self):
        """owner_phone has token.reversible=false — DLP should not receive it."""
        detokenizer = self._make_detokenizer()
        tokenized = self._tokenized_record()
        reversible_names = ["owner_name", "owner_email", "owner_payment_card"]

        mock_rows = []
        for _ in [tokenized]:
            row = MagicMock()
            row.values = [MagicMock(string_value=f"PLAIN_{n}") for n in reversible_names]
            mock_rows.append(row)

        mock_response = MagicMock()
        mock_response.item.table.rows = mock_rows

        with patch.object(detokenizer._client, "reidentify_content", return_value=mock_response) as mock_dlp:
            result = detokenizer.detokenize(tokenized, SAMPLE_SCHEMA)

        # DLP was called
        assert mock_dlp.call_count == 1

        # Check the DLP request headers — owner_phone should NOT appear
        request = mock_dlp.call_args[1]["request"]
        header_names = [h["name"] for h in request["item"]["table"]["headers"]]
        assert "owner_phone" not in header_names

        # owner_phone stays unchanged in the result
        assert result["owner_phone"] == tokenized["owner_phone"]

    def test_detokenize_disabled_returns_record_unchanged(self):
        detokenizer = DLPDetokenizer(DLPConfig(enabled=False), "test-project")
        tokenized = self._tokenized_record()
        result = detokenizer.detokenize(tokenized, SAMPLE_SCHEMA)
        assert result == tokenized

    def test_detokenize_raises_detokenization_error_on_dlp_failure(self):
        from google.api_core.exceptions import PermissionDenied
        detokenizer = DLPDetokenizer(DLPConfig(max_retries=0), "test-project")
        with patch.object(detokenizer._client, "reidentify_content", side_effect=PermissionDenied("denied")):
            with pytest.raises(DetokenizationError):
                detokenizer.detokenize(self._tokenized_record(), SAMPLE_SCHEMA)
