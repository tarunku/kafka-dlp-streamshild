"""
DLP detokenizer — reverses tokens back to original plaintext values.

Mirrors the tokenizer but calls DLP reidentifyContent instead of deidentifyContent.

Key behavior:
  - Only REVERSIBLE fields are processed. Fields with token.reversible='false'
    (e.g. SHA-256 hashes from CryptoHashConfig) are left unchanged.
  - The same batch architecture as the tokenizer — up to config.batch_size
    records per DLP API call.
  - The inspect_config in the reidentify request tells DLP what surrogate
    token prefixes to look for (VETSOURCE_PII_TOKEN, VETSOURCE_PCI_TOKEN).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from google.cloud import dlp_v2

from streamshield.dlp.policy import get_context_field, get_reversible_fields
from streamshield.dlp.tokenizer import (
    _build_field_transformations,
    _extract_crypto_material,
    _retry_with_backoff,
)
from streamshield.errors.exceptions import DetokenizationError
from streamshield.observability.logging import dlp_logger
from streamshield.observability.metrics import dlp_call_duration, dlp_calls, dlp_records_per_call

if TYPE_CHECKING:
    from streamshield.config import DLPConfig


class DLPDetokenizer:
    """
    Reverses DLP tokens back to their original plaintext values.

    Only authorized consumers (those with roles/cloudkms.cryptoKeyDecrypter)
    can successfully detokenize. IAM enforcement happens inside DLP — the SDK
    does not implement any additional access control.

    Usage:
        detokenizer = DLPDetokenizer(dlp_config, project_id)
        plain = detokenizer.detokenize(tokenized_record, raw_schema)
        plain_list = detokenizer.detokenize_batch(tokenized_records, raw_schema)
    """

    def __init__(self, dlp_config: "DLPConfig", project_id: str):
        self._config = dlp_config
        self._project_id = project_id
        self._client = dlp_v2.DlpServiceClient()

    def detokenize(self, tokenized_record: dict, raw_schema: dict) -> dict:
        """
        Reverse tokens in a single record.

        Irreversible fields (CryptoHashConfig, token.reversible='false') are
        left unchanged — their original values cannot be recovered.

        Args:
            tokenized_record: Record with DLP tokens in sensitive fields.
            raw_schema:       Raw Avro schema dict with token.* metadata.

        Returns:
            New dict with reversible tokens replaced by original plaintext.
            Irreversible fields retain their token values.
            The input is not mutated.

        Raises:
            DetokenizationError on DLP API failure.
        """
        results = self.detokenize_batch([tokenized_record], raw_schema)
        return results[0]

    def detokenize_batch(self, tokenized_records: list[dict], raw_schema: dict) -> list[dict]:
        """
        Reverse tokens in multiple records using a single DLP API call.

        Args:
            tokenized_records: List of records with DLP tokens.
            raw_schema:        Raw Avro schema dict with token.* metadata.

        Returns:
            List of records in the same order as input, with reversible tokens
            replaced by plaintext. Order is preserved.

        Raises:
            DetokenizationError on DLP API failure.
        """
        if not self._config.enabled:
            return [dict(r) for r in tokenized_records]

        # Only reversible fields — skip fields with token.reversible='false'
        reversible_fields = get_reversible_fields(raw_schema)
        if not reversible_fields:
            # Nothing to de-tokenize (all fields are irreversible or no tokenized fields)
            return [dict(r) for r in tokenized_records]

        batch_size = self._config.batch_size
        all_results: list[dict] = []
        for i in range(0, len(tokenized_records), batch_size):
            batch = tokenized_records[i : i + batch_size]
            detokenized = self._detokenize_batch_chunk(batch, raw_schema, reversible_fields)
            all_results.extend(detokenized)

        return all_results

    def _detokenize_batch_chunk(
        self,
        records: list[dict],
        raw_schema: dict,
        reversible_fields: list[dict],
    ) -> list[dict]:
        """
        Call DLP reidentifyContent for one batch of records.
        """
        pii_wrapped, pii_key, pci_wrapped, pci_key = _extract_crypto_material(raw_schema)
        context_field = get_context_field(raw_schema, self._config.context_field)

        # Build the same field transformations as the tokenizer — DLP uses the same
        # crypto key and algorithm to reverse the operation
        transformations = _build_field_transformations(
            reversible_fields, raw_schema, context_field,
            pii_wrapped, pii_key, pci_wrapped, pci_key,
        )

        # Surrogate types tell DLP which token prefix patterns to look for
        # when scanning the field values for tokens to reverse
        surrogate_pii = raw_schema.get("token.surrogate-info-type", "PII_TOKEN")
        surrogate_pci = raw_schema.get("token.pci-surrogate-info-type", "PCI_TOKEN")

        field_names = [f["name"] for f in reversible_fields]
        all_headers = field_names + [context_field]

        rows = []
        for record in records:
            row_values = [
                {"string_value": str(record.get(col, ""))}
                for col in all_headers
            ]
            rows.append({"values": row_values})

        dlp_logger.debug(
            "Calling DLP reidentifyContent: project=%s records=%d fields=%s",
            self._project_id, len(records), field_names,
        )

        start = time.monotonic()
        try:
            response = _retry_with_backoff(
                fn=lambda: self._client.reidentify_content(
                    request={
                        "parent": f"projects/{self._project_id}/locations/global",
                        "reidentify_config": {
                            "record_transformations": {
                                "field_transformations": transformations,
                            }
                        },
                        # inspect_config tells DLP what surrogate prefixes indicate a token
                        "inspect_config": {
                            "custom_info_types": [
                                {"info_type": {"name": surrogate_pii}, "surrogate_type": {}},
                                {"info_type": {"name": surrogate_pci}, "surrogate_type": {}},
                            ]
                        },
                        "item": {
                            "table": {
                                "headers": [{"name": n} for n in all_headers],
                                "rows":    rows,
                            }
                        },
                    }
                ),
                max_retries=self._config.max_retries,
                backoff_ms=self._config.retry_backoff_ms,
            )
        except Exception as exc:
            dlp_calls.add(1, {"operation": "detokenize", "status": "failed"})
            raise DetokenizationError(
                f"DLP reidentifyContent failed: {exc}",
                safe_context={
                    "project_id": self._project_id,
                    "field_names": field_names,
                    "record_count": len(records),
                },
            ) from exc

        elapsed = time.monotonic() - start
        dlp_calls.add(1, {"operation": "detokenize", "status": "success"})
        dlp_call_duration.record(elapsed, {"operation": "detokenize"})
        dlp_records_per_call.record(len(records), {"operation": "detokenize"})
        dlp_logger.debug("DLP detokenize completed in %.3fs for %d records", elapsed, len(records))

        result_rows = response.item.table.rows
        output: list[dict] = []
        for row_idx, record in enumerate(records):
            # Start with the original record (retains non-tokenized fields and irreversible fields)
            updated = dict(record)
            # Replace reversible fields with the de-tokenized values from DLP
            for col_idx, field_name in enumerate(field_names):
                updated[field_name] = result_rows[row_idx].values[col_idx].string_value
            output.append(updated)

        return output
