# dlp_utils.py — DLP tokenization and de-tokenization helpers
#
# Reads all tokenization policy (which fields are sensitive, which key to use,
# which algorithm, whether reversible) from the raw Avro schema dict.
# The caller never needs to hardcode field names or crypto configs.

import base64

from google.cloud import dlp_v2

from schema import SURROGATE_INFO_TYPE_PCI


# ── Schema introspection ──────────────────────────────────────────────────────

def get_tokenized_fields(raw_schema: dict) -> list[dict]:
    """Returns all fields annotated with logicalType='tokenized'."""
    return [
        f for f in raw_schema.get("fields", [])
        if f.get("logicalType") == "tokenized"
    ]


def is_tokenized_value(value: str, surrogate_info_type: str) -> bool:
    """
    Checks whether a string value is a DLP surrogate token by prefix.
    Works for CryptoDeterministic and FPE-with-surrogate tokens.
    FPE tokens produced with common_alphabet=NUMERIC have no visible prefix —
    those fields are identified by schema metadata (logicalType='tokenized'), not value.
    """
    return isinstance(value, str) and value.startswith(f"{surrogate_info_type}(")


# ── Primitive transformation builders ────────────────────────────────────────

def _make_crypto_key(wrapped_bytes: bytes, kms_key_name: str) -> dict:
    return {
        "kms_wrapped": {
            "wrapped_key":    wrapped_bytes,
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
    method      = field.get("token.method")
    default_sens = raw_schema.get("token.default-sensitivity", "PII")
    sensitivity  = field.get("token.sensitivity", default_sens)

    if sensitivity == "PCI-DSS":
        wrapped_bytes = pci_wrapped_bytes
        kms_key       = pci_kms_key
        surrogate     = raw_schema.get("token.pci-surrogate-info-type", SURROGATE_INFO_TYPE_PCI)
    else:
        wrapped_bytes = pii_wrapped_bytes
        kms_key       = pii_kms_key
        surrogate     = raw_schema.get("token.surrogate-info-type")

    crypto_key = _make_crypto_key(wrapped_bytes, kms_key)

    if method == "CryptoDeterministicConfig":
        return {
            "crypto_deterministic_config": {
                "crypto_key":          crypto_key,
                "surrogate_info_type": {"name": surrogate},
                "context":             {"name": "order_id"},
            }
        }

    if method == "CryptoReplaceFfxFpeConfig":
        return {
            "crypto_replace_ffx_fpe_config": {
                "crypto_key":          crypto_key,
                "common_alphabet":     "NUMERIC",
                "surrogate_info_type": {"name": surrogate},
            }
        }

    if method == "CryptoHashConfig":
        return {
            "crypto_hash_config": {
                "crypto_key": crypto_key,
            }
        }

    return None


def _build_transformations(
    fields: list[dict],
    raw_schema: dict,
    pii_wrapped_bytes: bytes,
    pii_kms_key: str,
    pci_wrapped_bytes: bytes,
    pci_kms_key: str,
) -> list[dict]:
    result = []
    for field in fields:
        prim = _build_primitive_transform(
            field, raw_schema,
            pii_wrapped_bytes, pii_kms_key,
            pci_wrapped_bytes, pci_kms_key,
        )
        if prim:
            result.append({
                "fields":                    [{"name": field["name"]}],
                "primitive_transformation":  prim,
            })
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def tokenize_record(
    dlp_client: dlp_v2.DlpServiceClient,
    project_id: str,
    record: dict,
    raw_schema: dict,
) -> dict:
    """
    Calls Cloud DLP deidentifyContent to tokenize all logicalType='tokenized' fields.
    Reads crypto keys and methods from the embedded schema metadata.
    Returns a new dict — the original record is not mutated.
    """
    tokenized_fields = get_tokenized_fields(raw_schema)
    if not tokenized_fields:
        return dict(record)

    pii_wrapped_bytes = base64.b64decode(raw_schema["token.wrapped-dek"])
    pci_wrapped_bytes = base64.b64decode(raw_schema["token.pci-wrapped-dek"])
    pii_kms_key       = raw_schema["token.kms-key"]
    pci_kms_key       = raw_schema["token.pci-kms-key"]

    field_names        = [f["name"] for f in tokenized_fields]
    transformations    = _build_transformations(
        tokenized_fields, raw_schema,
        pii_wrapped_bytes, pii_kms_key,
        pci_wrapped_bytes, pci_kms_key,
    )

    response = dlp_client.deidentify_content(
        request={
            "parent": f"projects/{project_id}/locations/global",
            "deidentify_config": {
                "record_transformations": {
                    "field_transformations": transformations
                }
            },
            "item": {
                "table": {
                    "headers": [{"name": n} for n in field_names],
                    "rows": [{
                        "values": [
                            {"string_value": str(record.get(n, ""))}
                            for n in field_names
                        ]
                    }],
                }
            },
        }
    )

    result = dict(record)
    for i, name in enumerate(field_names):
        result[name] = response.item.table.rows[0].values[i].string_value
    return result


def detokenize_record(
    dlp_client: dlp_v2.DlpServiceClient,
    project_id: str,
    tokenized_record: dict,
    raw_schema: dict,
) -> dict:
    """
    Calls Cloud DLP reidentifyContent to reverse all reversible tokenized fields.
    Fields with token.reversible='false' (CryptoHashConfig) are left unchanged.
    Returns a new dict — the input is not mutated.
    """
    default_reversible = raw_schema.get("token.default-reversible", "true")
    reversible_fields = [
        f for f in get_tokenized_fields(raw_schema)
        if f.get("token.reversible", default_reversible) != "false"
    ]
    if not reversible_fields:
        return dict(tokenized_record)

    pii_wrapped_bytes = base64.b64decode(raw_schema["token.wrapped-dek"])
    pci_wrapped_bytes = base64.b64decode(raw_schema["token.pci-wrapped-dek"])
    pii_kms_key       = raw_schema["token.kms-key"]
    pci_kms_key       = raw_schema["token.pci-kms-key"]
    surrogate_pii     = raw_schema.get("token.surrogate-info-type")
    surrogate_pci     = raw_schema.get("token.pci-surrogate-info-type", SURROGATE_INFO_TYPE_PCI)

    field_names     = [f["name"] for f in reversible_fields]
    transformations = _build_transformations(
        reversible_fields, raw_schema,
        pii_wrapped_bytes, pii_kms_key,
        pci_wrapped_bytes, pci_kms_key,
    )

    response = dlp_client.reidentify_content(
        request={
            "parent": f"projects/{project_id}/locations/global",
            "reidentify_config": {
                "record_transformations": {
                    "field_transformations": transformations
                }
            },
            "inspect_config": {
                # DLP uses custom_info_types with surrogate_type to locate tokens
                # in deterministic and FPE fields during re-identification.
                "custom_info_types": [
                    {"info_type": {"name": surrogate_pii}, "surrogate_type": {}},
                    {"info_type": {"name": surrogate_pci}, "surrogate_type": {}},
                ]
            },
            "item": {
                "table": {
                    "headers": [{"name": n} for n in field_names],
                    "rows": [{
                        "values": [
                            {"string_value": str(tokenized_record.get(n, ""))}
                            for n in field_names
                        ]
                    }],
                }
            },
        }
    )

    result = dict(tokenized_record)
    for i, name in enumerate(field_names):
        result[name] = response.item.table.rows[0].values[i].string_value
    return result
