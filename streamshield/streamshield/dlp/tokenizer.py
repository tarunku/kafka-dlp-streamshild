"""
DLP tokenizer — converts plaintext sensitive fields to opaque tokens.

The core logic is ported from the POC's dlp_utils.py with two key improvements:
  1. Batch support: up to DLPConfig.batch_size records per API call.
     The POC called DLP once per record — this is up to 100x faster.
  2. Configurable context field: no longer hardcoded to 'order_id'.

All tokenization policy (which fields, which algorithm, which KMS key) is read
from the Avro schema at runtime. The tokenizer has zero hardcoded field names.

How DLP tokenization works in this SDK:
  - The Avro schema carries 'token.*' metadata at both the schema level
    (KMS key names, wrapped DEKs) and per-field level (method, sensitivity).
  - The tokenizer builds a DLP deidentifyContent request with one
    FieldTransformation per tokenized field.
  - For batch calls, multiple records are sent as rows in a single DLP Table.
  - DLP returns the same table with sensitive values replaced by tokens.
  - The tokenizer extracts the tokenized values by index and returns new dicts.
"""

from __future__ import annotations

import base64
import time
from typing import TYPE_CHECKING

from google.cloud import dlp_v2
from google.api_core import exceptions as google_exceptions

from streamshield.dlp.policy import get_context_field, get_tokenized_fields
from streamshield.errors.exceptions import TokenizationError
from streamshield.observability.logging import dlp_logger
from streamshield.observability.metrics import dlp_call_duration, dlp_calls, dlp_records_per_call

if TYPE_CHECKING:
    from streamshield.config import DLPConfig


# ── Primitive transform builders (private) ────────────────────────────────────

def _make_crypto_key(wrapped_bytes: bytes, kms_key_name: str) -> dict:
    """Build the DLP CryptoKey structure for KMS-wrapped DEK envelope encryption."""
    return {
        "kms_wrapped": {
            "wrapped_key":     wrapped_bytes,
            "crypto_key_name": kms_key_name,
        }
    }


def _build_primitive_transform(
    field: dict,
    raw_schema: dict,
    pii_wrapped_bytes: bytes,
    pii_kms_key: str,
    pci_wrapped_bytes: bytes,
    pci_kms_key: str,
) -> dict | None:
    """
    Build the DLP primitive_transformation config for a single schema field.

    Reads token.method and token.sensitivity from the field metadata to
    select the correct algorithm and KMS domain.

    Returns None if the field has an unrecognised token.method (skip silently).
    """
    method = field.get("token.method")
    default_sensitivity = raw_schema.get("token.default-sensitivity", "PII")
    sensitivity = field.get("token.sensitivity", default_sensitivity)

    # PCI-DSS fields use the card encryption domain; everything else uses PII
    if sensitivity == "PCI-DSS":
        wrapped_bytes = pci_wrapped_bytes
        kms_key       = pci_kms_key
        surrogate     = raw_schema.get("token.pci-surrogate-info-type", "PCI_TOKEN")
    else:
        wrapped_bytes = pii_wrapped_bytes
        kms_key       = pii_kms_key
        surrogate     = raw_schema.get("token.surrogate-info-type")

    crypto_key = _make_crypto_key(wrapped_bytes, kms_key)

    if method == "CryptoDeterministicConfig":
        # AES-SIV: same plaintext → same token (deterministic, reversible, context-bound)
        return {
            "crypto_deterministic_config": {
                "crypto_key":          crypto_key,
                "surrogate_info_type": {"name": surrogate},
                "context":             {"name": "order_id"},  # will be overridden by caller
            }
        }

    if method == "CryptoReplaceFfxFpeConfig":
        # Format-Preserving Encryption: output looks like the input alphabet
        # (e.g. a 16-digit card number tokenizes to another 16-digit number)
        return {
            "crypto_replace_ffx_fpe_config": {
                "crypto_key":          crypto_key,
                "common_alphabet":     "NUMERIC",
                "surrogate_info_type": {"name": surrogate},
            }
        }

    if method == "CryptoHashConfig":
        # SHA-256 hash: one-way, irreversible. token.reversible=false in the schema.
        return {
            "crypto_hash_config": {
                "crypto_key": crypto_key,
            }
        }

    dlp_logger.warning(
        "Unknown token.method '%s' on field '%s' — skipping.",
        method, field.get("name"),
    )
    return None


def _build_field_transformations(
    tokenized_fields: list[dict],
    raw_schema: dict,
    context_field: str,
    pii_wrapped_bytes: bytes,
    pii_kms_key: str,
    pci_wrapped_bytes: bytes,
    pci_kms_key: str,
) -> list[dict]:
    """
    Build the list of FieldTransformation dicts for the DLP request.
    One transformation per tokenized field.
    """
    transformations = []
    for field in tokenized_fields:
        prim = _build_primitive_transform(
            field, raw_schema,
            pii_wrapped_bytes, pii_kms_key,
            pci_wrapped_bytes, pci_kms_key,
        )
        if prim is None:
            continue

        # Patch the CryptoDeterministic context field name from schema/config
        if "crypto_deterministic_config" in prim:
            prim["crypto_deterministic_config"]["context"]["name"] = context_field

        transformations.append({
            "fields":               [{"name": field["name"]}],
            "primitive_transformation": prim,
        })
    return transformations


def _extract_crypto_material(raw_schema: dict) -> tuple[bytes, str, bytes, str]:
    """
    Extract KMS key names and wrapped DEKs from the schema-level token.* properties.

    The schema carries this material so consumers never need out-of-band config
    to call DLP — everything needed is in the schema.

    Returns:
        (pii_wrapped_bytes, pii_kms_key, pci_wrapped_bytes, pci_kms_key)
    """
    pii_wrapped_bytes = base64.b64decode(raw_schema["token.wrapped-dek"])
    pci_wrapped_bytes = base64.b64decode(raw_schema["token.pci-wrapped-dek"])
    pii_kms_key       = raw_schema["token.kms-key"]
    pci_kms_key       = raw_schema["token.pci-kms-key"]
    return pii_wrapped_bytes, pii_kms_key, pci_wrapped_bytes, pci_kms_key


def _retry_with_backoff(fn, max_retries: int, backoff_ms: int):
    """
    Execute fn(), retrying on transient Google API errors.

    Retries on: UNAVAILABLE (503), RESOURCE_EXHAUSTED (429).
    Does NOT retry on: PERMISSION_DENIED, INVALID_ARGUMENT (these are non-transient).
    """
    non_retryable = (
        google_exceptions.PermissionDenied,
        google_exceptions.InvalidArgument,
        google_exceptions.NotFound,
    )
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except non_retryable:
            raise  # re-raise immediately — no point retrying permission/argument errors
        except (google_exceptions.ServiceUnavailable, google_exceptions.ResourceExhausted) as exc:
            if attempt == max_retries:
                raise
            wait_s = (backoff_ms / 1000) * (2 ** attempt)  # exponential backoff
            dlp_logger.warning(
                "DLP transient error (attempt %d/%d) — retrying in %.1fs: %s",
                attempt + 1, max_retries, wait_s, exc,
            )
            time.sleep(wait_s)
    return None  # unreachable but satisfies type checkers


# ── Public tokenizer class ────────────────────────────────────────────────────

class DLPTokenizer:
    """
    Tokenizes sensitive fields in records using Cloud DLP deidentifyContent.

    Batching:
        tokenize_batch() sends up to config.batch_size records in a single
        DLP API call by encoding them as rows in a DLP Table object.
        Single records can be tokenized with tokenize() which internally
        calls tokenize_batch() with a list of one record.

    Usage:
        tokenizer = DLPTokenizer(dlp_config, project_id)
        tokenized = tokenizer.tokenize(record, raw_schema)
        tokenized_list = tokenizer.tokenize_batch(records, raw_schema)
    """

    def __init__(self, dlp_config: "DLPConfig", project_id: str):
        self._config = dlp_config
        self._project_id = project_id
        self._client = dlp_v2.DlpServiceClient()

    def tokenize(self, record: dict, raw_schema: dict) -> dict:
        """
        Tokenize sensitive fields in a single record.

        Convenience wrapper around tokenize_batch() for single-record use cases.

        Args:
            record:     Plaintext record dict. Sensitive fields will be replaced.
            raw_schema: Raw Avro schema dict with token.* metadata.

        Returns:
            New dict with sensitive fields replaced by DLP tokens.
            The original record is not mutated.

        Raises:
            TokenizationError on DLP API failure.
        """
        results = self.tokenize_batch([record], raw_schema)
        return results[0]

    def tokenize_batch(self, records: list[dict], raw_schema: dict) -> list[dict]:
        """
        Tokenize sensitive fields in multiple records using a single DLP API call.

        Records are encoded as rows in a DLP Table object. DLP processes all rows
        in one request and returns the same table with tokens. Results are matched
        back to input records by position.

        If len(records) > config.batch_size, the records are split into multiple
        API calls automatically.

        Args:
            records:    List of plaintext record dicts.
            raw_schema: Raw Avro schema dict with token.* metadata.

        Returns:
            List of new dicts in the same order as input, with sensitive fields
            replaced by DLP tokens.

        Raises:
            TokenizationError on DLP API failure.
        """
        if not self._config.enabled:
            # DLP disabled — return records as-is (useful for non-sensitive schemas)
            return [dict(r) for r in records]

        tokenized_fields = get_tokenized_fields(raw_schema)
        if not tokenized_fields:
            # No fields annotated as tokenized — nothing to do
            return [dict(r) for r in records]

        # Split into batches to stay within DLP table row limits
        batch_size = self._config.batch_size
        all_results: list[dict] = []
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            tokenized_batch = self._tokenize_batch_chunk(batch, raw_schema, tokenized_fields)
            all_results.extend(tokenized_batch)

        return all_results

    def _tokenize_batch_chunk(
        self,
        records: list[dict],
        raw_schema: dict,
        tokenized_fields: list[dict],
    ) -> list[dict]:
        """
        Call DLP deidentifyContent for one batch (up to batch_size records).
        Returns the same number of dicts as input, with tokens in place.
        """
        pii_wrapped, pii_key, pci_wrapped, pci_key = _extract_crypto_material(raw_schema)
        context_field = get_context_field(raw_schema, self._config.context_field)

        # Build field transformations (same for every row in the batch)
        transformations = _build_field_transformations(
            tokenized_fields, raw_schema, context_field,
            pii_wrapped, pii_key, pci_wrapped, pci_key,
        )

        # The DLP request headers include all tokenized field names AND the context field.
        # The context field is not transformed but must appear in the table so DLP can
        # use it as a per-record binding for CryptoDeterministicConfig.
        field_names = [f["name"] for f in tokenized_fields]
        all_headers = field_names + [context_field]

        # Build one row per record
        rows = []
        for record in records:
            row_values = [
                {"string_value": str(record.get(col, ""))}
                for col in all_headers
            ]
            rows.append({"values": row_values})

        dlp_logger.debug(
            "Calling DLP deidentifyContent: project=%s records=%d fields=%s",
            self._project_id, len(records), field_names,
        )

        start = time.monotonic()
        try:
            response = _retry_with_backoff(
                fn=lambda: self._client.deidentify_content(
                    request={
                        "parent": f"projects/{self._project_id}/locations/global",
                        "deidentify_config": {
                            "record_transformations": {
                                "field_transformations": transformations,
                            }
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
            dlp_calls.add(1, {"operation": "tokenize", "status": "failed"})
            raise TokenizationError(
                f"DLP deidentifyContent failed: {exc}",
                safe_context={
                    "project_id": self._project_id,
                    "field_names": field_names,
                    "record_count": len(records),
                },
            ) from exc

        elapsed = time.monotonic() - start
        dlp_calls.add(1, {"operation": "tokenize", "status": "success"})
        dlp_call_duration.record(elapsed, {"operation": "tokenize"})
        dlp_records_per_call.record(len(records), {"operation": "tokenize"})
        dlp_logger.debug("DLP tokenize completed in %.3fs for %d records", elapsed, len(records))

        # Extract tokenized values from the response table — matched by row and column index
        result_rows = response.item.table.rows
        output: list[dict] = []
        for row_idx, record in enumerate(records):
            updated = dict(record)  # copy non-tokenized fields as-is
            for col_idx, field_name in enumerate(field_names):
                updated[field_name] = result_rows[row_idx].values[col_idx].string_value
            output.append(updated)

        return output
