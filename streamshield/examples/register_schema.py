"""
One-time setup: register the PrescriptionOrder schema with embedded DLP metadata.

Run this ONCE after generate_wrapped_dek.py has been executed and the wrapped DEKs
are stored in Secret Manager. The schema is registered under
'prescription-events-value' with BACKWARD compatibility.

Run:
    python3 examples/register_schema.py
"""

import logging

from streamshield import CompatibilityMode, GCPConfig, SchemaAdmin, SDKConfig
from streamshield.auth.gcp import GCPAuth
from streamshield.observability.logging import configure_json_logging
from schemas.prescription_order import SUBJECT, build_prescription_schema

configure_json_logging(level=logging.INFO)

PROJECT_ID = "vetsource-496203"

# ── Load key material from Secret Manager ─────────────────────────────────────
print("Loading KMS keys and wrapped DEKs from Secret Manager...")

auth = GCPAuth(project_id=PROJECT_ID)
pii_kms_key_name    = auth.get_secret("dlp-kms-pii-key-name")
pci_kms_key_name    = auth.get_secret("dlp-kms-pci-key-name")
pii_wrapped_dek     = auth.get_secret("dlp-pii-wrapped-dek")
pci_wrapped_dek     = auth.get_secret("dlp-pci-wrapped-dek")

print(f"  PII KMS key: {pii_kms_key_name}")
print(f"  PCI KMS key: {pci_kms_key_name}")

# ── Build schema with embedded tokenization metadata ─────────────────────────
schema = build_prescription_schema(
    pii_kms_key     = pii_kms_key_name,
    pii_wrapped_dek = pii_wrapped_dek,
    pci_kms_key     = pci_kms_key_name,
    pci_wrapped_dek = pci_wrapped_dek,
)

# ── Register with Schema Registry ─────────────────────────────────────────────
config = SDKConfig(gcp=GCPConfig(project_id=PROJECT_ID, use_secret_manager=True))
admin  = SchemaAdmin(config)

print(f"\nRegistering schema under subject '{SUBJECT}'...")
sv = admin.register(
    subject           = SUBJECT,
    schema_definition = schema,
    compatibility_mode = CompatibilityMode.BACKWARD,
)

print(f"Schema registered — ID: {sv.schema_id}, version: {sv.version}")
print(f"Subject: {sv.subject}")
print("\nNext step: run prescription_producer.py to publish tokenized events.")
