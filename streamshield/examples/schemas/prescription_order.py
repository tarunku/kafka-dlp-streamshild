"""
Prescription order Avro schema with embedded DLP tokenization metadata.

This schema is specific to the Vetsource prescription ordering domain.
It is provided as an example — NOT part of the StreamShield SDK itself.

The schema carries all cryptographic metadata in its token.* properties:
  - KMS key names for PII and PCI-DSS domains
  - Base64-encoded KMS-wrapped AES-256 DEKs
  - Per-field tokenization method and reversibility flags

Consumers fetch this schema from the Schema Registry once and have everything
they need to call DLP — no side-channel configuration required.

One-time setup (run before using this schema):
  1. python3 ../../kafka-dlp/generate_wrapped_dek.py
  2. python3 register_schema.py  (or call SchemaAdmin.register() directly)
"""

# These are used as DLP surrogate token prefixes.
# All PII tokens will start with  VETSOURCE_PII_TOKEN(...)
# All PCI-DSS tokens start with   VETSOURCE_PCI_TOKEN(...)
SURROGATE_INFO_TYPE_PII = "VETSOURCE_PII_TOKEN"
SURROGATE_INFO_TYPE_PCI = "VETSOURCE_PCI_TOKEN"

# Schema Registry subject name for this topic
TOPIC   = "prescription-events"
SUBJECT = "prescription-events-value"


def build_prescription_schema(
    pii_kms_key: str,
    pii_wrapped_dek: str,    # base64-encoded KMS-wrapped AES-256 key — PII domain
    pci_kms_key: str,
    pci_wrapped_dek: str,    # base64-encoded KMS-wrapped AES-256 key — PCI-DSS domain
) -> dict:
    """
    Build the PrescriptionOrder Avro schema with DLP tokenization metadata embedded.

    Args:
        pii_kms_key:      Full KMS key resource name for PII fields.
        pii_wrapped_dek:  Base64-encoded wrapped AES-256 key for PII fields.
        pci_kms_key:      Full KMS key resource name for PCI-DSS fields.
        pci_wrapped_dek:  Base64-encoded wrapped AES-256 key for PCI-DSS fields.

    Returns:
        Avro schema dict ready to pass to SchemaAdmin.register().

    Example:
        from streamshield import SchemaAdmin, SDKConfig, GCPConfig, CompatibilityMode
        from streamshield.auth.gcp import GCPAuth

        config = SDKConfig(gcp=GCPConfig(project_id="terraform-testing-498903"))
        auth   = GCPAuth(project_id="terraform-testing-498903")
        pii_kms_key     = auth.get_secret("dlp-kms-pii-key-name")
        pci_kms_key     = auth.get_secret("dlp-kms-pci-key-name")
        pii_wrapped_dek = auth.get_secret("dlp-pii-wrapped-dek")
        pci_wrapped_dek = auth.get_secret("dlp-pci-wrapped-dek")

        schema = build_prescription_schema(pii_kms_key, pii_wrapped_dek, pci_kms_key, pci_wrapped_dek)
        admin  = SchemaAdmin(config)
        admin.register(SUBJECT, schema, compatibility_mode=CompatibilityMode.BACKWARD)
    """
    return {
        "type":      "record",
        "name":      "PrescriptionOrder",
        "namespace": "com.vetsource.events",
        "doc": (
            "Prescription order event. All DLP tokenization metadata is embedded "
            "in the schema — consumers need only this schema to invoke DLP independently."
        ),

        # ── Schema-level tokenization properties ────────────────────────────────
        # These carry the KMS keys and wrapped DEKs needed to call DLP.
        # Consumers read these properties at runtime — they never need out-of-band config.

        "token.system":              "google-cloud-dlp",
        "token.surrogate-info-type": SURROGATE_INFO_TYPE_PII,
        "token.kms-key":             pii_kms_key,
        "token.wrapped-dek":         pii_wrapped_dek,
        # PCI-DSS domain uses separate KMS key — enables separate audit log queries
        "token.pci-kms-key":             pci_kms_key,
        "token.pci-wrapped-dek":         pci_wrapped_dek,
        "token.pci-surrogate-info-type": SURROGATE_INFO_TYPE_PCI,
        # Defaults applied to all tokenized fields unless overridden per-field
        "token.default-sensitivity": "PII",
        "token.default-reversible":  "true",

        "fields": [
            # ── Non-sensitive fields — stored as plaintext ─────────────────────
            {"name": "order_id",   "type": "string"},
            {"name": "medication", "type": "string"},
            {"name": "quantity",   "type": "int"},
            {"name": "order_date", "type": "string"},
            {"name": "is_refill",  "type": "boolean", "default": False},

            # ── PII fields — AES-SIV deterministic encryption ─────────────────
            # CryptoDeterministicConfig: same plaintext → same token.
            # Deterministic tokens can be grouped/deduplicated without decryption.
            {
                "name":           "owner_name",
                "type":           "string",
                "logicalType":    "tokenized",
                "token.infotype": "PERSON_NAME",
                "token.method":   "CryptoDeterministicConfig",
            },
            {
                "name":           "owner_email",
                "type":           "string",
                "default":        "",
                "logicalType":    "tokenized",
                "token.infotype": "EMAIL_ADDRESS",
                "token.method":   "CryptoDeterministicConfig",
            },
            {
                "name":           "pet_name",
                "type":           "string",
                "logicalType":    "tokenized",
                "token.infotype": "VETSOURCE_PET_NAME",
                "token.method":   "CryptoDeterministicConfig",
            },

            # ── PII field — SHA-256 hash (irreversible by design) ─────────────
            # CryptoHashConfig: one-way transformation.
            # token.reversible=false means DLP will never be asked to reverse this.
            # Even an authorized consumer with KMS access cannot recover the original.
            {
                "name":             "owner_phone",
                "type":             "string",
                "default":          "",
                "logicalType":      "tokenized",
                "token.infotype":   "PHONE_NUMBER",
                "token.method":     "CryptoHashConfig",
                "token.reversible": "false",
            },

            # ── PCI-DSS field — format-preserving encryption ──────────────────
            # CryptoReplaceFfxFpeConfig: the token is a valid-looking 16-digit number.
            # Downstream card-format validators (Luhn check) pass without modification.
            # Uses the PCI-DSS KMS domain — audit logs are cleanly separated from PII.
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
