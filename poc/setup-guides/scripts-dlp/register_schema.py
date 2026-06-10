# register_schema.py — register PrescriptionOrder schema with GCP Schema Registry
#
# Run this ONCE after generate_wrapped_dek.py. Loads the wrapped DEKs and KMS key
# names from Secret Manager, embeds them into the Avro schema, and registers the
# schema under the subject "prescription-events-value".
#
# After this runs successfully, all consumers can bootstrap DLP de-tokenization
# from the schema alone — they only need the subscription path and project ID.
#
# Usage:
#   python3 register_schema.py

import json

import requests

from schema import TOPIC, build_prescription_schema
from utils import get_gcp_bearer_token, get_secret

# ── Configuration ─────────────────────────────────────────────────────────────

PROJECT_ID = "vetsource-496203"

# ── Load all values from Secret Manager ──────────────────────────────────────

print("Loading credentials and key material from Secret Manager...")

schema_registry_url = get_secret(PROJECT_ID, "schema-registry-url")
pii_kms_key_name    = get_secret(PROJECT_ID, "dlp-kms-pii-key-name")
pci_kms_key_name    = get_secret(PROJECT_ID, "dlp-kms-pci-key-name")
pii_wrapped_dek     = get_secret(PROJECT_ID, "dlp-pii-wrapped-dek")
pci_wrapped_dek     = get_secret(PROJECT_ID, "dlp-pci-wrapped-dek")

print("  Loaded schema-registry-url")
print(f"  Loaded PII KMS key:  {pii_kms_key_name}")
print(f"  Loaded PCI KMS key:  {pci_kms_key_name}")
print("  Loaded wrapped DEKs (PII + PCI)")

# ── Build schema with embedded tokenization metadata ─────────────────────────

schema = build_prescription_schema(
    pii_kms_key   = pii_kms_key_name,
    pii_wrapped_dek = pii_wrapped_dek,
    pci_kms_key   = pci_kms_key_name,
    pci_wrapped_dek = pci_wrapped_dek,
)

# ── Register with GCP Kafka Schema Registry ───────────────────────────────────

SUBJECT = f"{TOPIC}-value"

print(f"\nRegistering schema under subject '{SUBJECT}'...")

token    = get_gcp_bearer_token()
url      = f"{schema_registry_url.rstrip('/')}/subjects/{SUBJECT}/versions"
payload  = {
    "schema":     json.dumps(schema),
    "schemaType": "AVRO",
}

response = requests.post(
    url,
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/vnd.schemaregistry.v1+json",
    },
    json=payload,
    timeout=15,
)
response.raise_for_status()

schema_id = response.json()["id"]
print(f"Schema registered — ID: {schema_id}")
print(f"Subject:              {SUBJECT}")
print(f"Registry:             {schema_registry_url}")

# ── Verify round-trip ─────────────────────────────────────────────────────────

print("\nVerifying round-trip fetch from registry...")

token   = get_gcp_bearer_token()
ver_url = f"{schema_registry_url.rstrip('/')}/schemas/ids/{schema_id}"
verify  = requests.get(
    ver_url,
    headers={"Authorization": f"Bearer {token}"},
    timeout=10,
)
verify.raise_for_status()

fetched_schema = json.loads(verify.json()["schema"])
tokenized_fields = [
    f["name"] for f in fetched_schema.get("fields", [])
    if f.get("logicalType") == "tokenized"
]
print(f"  Fetched schema fields: {[f['name'] for f in fetched_schema['fields']]}")
print(f"  Tokenized fields:      {tokenized_fields}")
print(f"  token.kms-key present: {'token.kms-key' in fetched_schema}")
print(f"  token.wrapped-dek present: {'token.wrapped-dek' in fetched_schema}")

print("\nSchema registration complete.")
print("Next step: run producer.py to publish tokenized prescription events.")
