# schema.py — PrescriptionOrder Avro schema with embedded DLP tokenization metadata
#
# Schema-level token.* properties carry the KMS key references and wrapped DEKs
# so consumers can bootstrap de-tokenization from the schema alone — no external
# config lookup required beyond the Pub/Sub subscription path and GCP project ID.

TOPIC                  = "prescription-events"
SURROGATE_INFO_TYPE_PII = "VETSOURCE_PII_TOKEN"
SURROGATE_INFO_TYPE_PCI = "VETSOURCE_PCI_TOKEN"


def build_prescription_schema(
    pii_kms_key: str,
    pii_wrapped_dek: str,   # base64-encoded KMS-wrapped AES-256 key — PII domain
    pci_kms_key: str,
    pci_wrapped_dek: str,   # base64-encoded KMS-wrapped AES-256 key — PCI-DSS domain
) -> dict:
    """
    Returns the full PrescriptionOrder schema dict with tokenization metadata embedded.
    Call build_prescription_schema() once at startup with values loaded from Secret Manager,
    then register the result with Schema Registry.

    Field-level properties:
      logicalType="tokenized"  — field carries a DLP token, not a plaintext value
      token.method             — DLP transformation primitive
      token.infotype           — DLP info type used during de/re-identification
      token.sensitivity        — compliance tier; absent means PII (schema default)
      token.reversible         — "false" means hash-only; absent means reversible
    """
    return {
        "type":      "record",
        "name":      "PrescriptionOrder",
        "namespace": "com.vetsource.events",
        "doc":       (
            "Prescription order event. All DLP tokenization metadata is embedded "
            "in the schema — consumers need only this schema to invoke DLP independently."
        ),

        # ── Schema-level tokenization properties ─────────────────────────────
        # PII domain key (owner_name, owner_email, pet_name, owner_phone)
        "token.system":              "google-cloud-dlp",
        "token.surrogate-info-type": SURROGATE_INFO_TYPE_PII,
        "token.kms-key":             pii_kms_key,
        "token.wrapped-dek":         pii_wrapped_dek,
        # PCI-DSS domain key (owner_payment_card)
        "token.pci-kms-key":         pci_kms_key,
        "token.pci-wrapped-dek":     pci_wrapped_dek,
        "token.pci-surrogate-info-type": SURROGATE_INFO_TYPE_PCI,
        # Defaults that apply to all tokenized fields unless overridden
        "token.default-sensitivity": "PII",
        "token.default-reversible":  "true",

        "fields": [
            # ── Non-sensitive fields ──────────────────────────────────────────
            {"name": "order_id",   "type": "string"},
            {"name": "medication", "type": "string"},
            {"name": "quantity",   "type": "int"},
            {"name": "order_date", "type": "string"},
            {"name": "is_refill",  "type": "boolean", "default": False},

            # ── PII fields — AES-SIV deterministic encryption ─────────────────
            # Same plaintext → same token. Safe to group/deduplicate by token value.
            {
                "name":            "owner_name",
                "type":            "string",
                "logicalType":     "tokenized",
                "token.infotype":  "PERSON_NAME",
                "token.method":    "CryptoDeterministicConfig",
            },
            {
                "name":            "owner_email",
                "type":            "string",
                "default":         "",
                "logicalType":     "tokenized",
                "token.infotype":  "EMAIL_ADDRESS",
                "token.method":    "CryptoDeterministicConfig",
            },
            {
                "name":             "pet_name",
                "type":             "string",
                "logicalType":      "tokenized",
                "token.infotype":   "VETSOURCE_PET_NAME",
                "token.method":     "CryptoDeterministicConfig",
            },

            # ── PII field — SHA-256 hash (irreversible) ───────────────────────
            # Cannot be reversed under any circumstances, even with the key.
            {
                "name":             "owner_phone",
                "type":             "string",
                "default":          "",
                "logicalType":      "tokenized",
                "token.infotype":   "PHONE_NUMBER",
                "token.method":     "CryptoHashConfig",
                "token.reversible": "false",
            },

            # ── PCI-DSS field — format-preserving encryption (FPE) ────────────
            # Token is a valid-looking 16-digit number; downstream card-format
            # validators pass without any schema modification.
            {
                "name":              "owner_payment_card",
                "type":              "string",
                "default":           "",
                "logicalType":       "tokenized",
                "token.infotype":    "CREDIT_CARD_NUMBER",
                "token.method":      "CryptoReplaceFfxFpeConfig",
                "token.sensitivity": "PCI-DSS",
            },
        ],
    }
