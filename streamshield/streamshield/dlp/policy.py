"""
DLP policy helpers — schema introspection.

These functions read the Avro schema's token.* metadata to determine which
fields should be tokenized, how they are tokenized, and whether their tokens
can be reversed.

The schema-driven approach (inherited from the POC) means that:
  - No field names are hardcoded in the SDK.
  - No tokenization policy is hardcoded in the SDK.
  - Adding a new sensitive field only requires updating the Avro schema.
  - Consumers with the schema have everything they need to call DLP.
"""

from __future__ import annotations


def get_tokenized_fields(raw_schema: dict) -> list[dict]:
    """
    Return all fields in the schema that carry the logicalType='tokenized' annotation.

    These are the fields that Cloud DLP should process — either tokenize on the
    producer side or de-tokenize on the consumer side.

    Args:
        raw_schema: Raw Avro schema dict (from the Schema Registry, not yet parsed).

    Returns:
        List of field dicts, each containing at minimum:
          name, logicalType, token.method, and optionally token.sensitivity,
          token.reversible, token.infotype.
    """
    return [
        field for field in raw_schema.get("fields", [])
        if field.get("logicalType") == "tokenized"
    ]


def get_reversible_fields(raw_schema: dict) -> list[dict]:
    """
    Return only the tokenized fields that can be de-tokenized (reversed).

    Fields with token.reversible='false' (e.g. SHA-256 hashes) are excluded.
    The de-tokenizer skips these automatically.
    """
    default_reversible = raw_schema.get("token.default-reversible", "true")
    return [
        field for field in get_tokenized_fields(raw_schema)
        if field.get("token.reversible", default_reversible) != "false"
    ]


def get_context_field(raw_schema: dict, config_default: str = "order_id") -> str:
    """
    Resolve the context field name used in CryptoDeterministicConfig.

    CryptoDeterministicConfig ties each token to a record identifier so the same
    plaintext value tokenizes to the same token only within a given record context.

    Resolution order (highest priority first):
      1. Schema-level annotation: token.context-field
      2. DLPConfig.context_field (passed as config_default)

    Args:
        raw_schema:     Raw Avro schema dict.
        config_default: Fallback from DLPConfig.context_field.

    Returns:
        Name of the field to use as the DLP crypto context.
    """
    return raw_schema.get("token.context-field", config_default)


def is_tokenized_value(value: str, surrogate_info_type: str) -> bool:
    """
    Check whether a string value looks like a DLP surrogate token.

    CryptoDeterministicConfig produces tokens with a visible prefix:
        VETSOURCE_PII_TOKEN(14):aB3xKp...

    CryptoReplaceFfxFpeConfig (FPE/NUMERIC) produces valid-looking numbers
    with no visible prefix — those are identified by schema metadata, not value.

    Args:
        value:               The string value to inspect.
        surrogate_info_type: The DLP surrogate type prefix (e.g. 'VETSOURCE_PII_TOKEN').

    Returns:
        True if the value starts with the expected surrogate token prefix.
    """
    return isinstance(value, str) and value.startswith(f"{surrogate_info_type}(")
