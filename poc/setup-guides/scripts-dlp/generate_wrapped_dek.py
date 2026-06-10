# generate_wrapped_dek.py — one-time setup: generate and KMS-wrap DEKs
#
# Run this ONCE per domain key during initial setup. The output (base64 wrapped DEKs)
# is stored in Secret Manager and then embedded permanently in the Avro schema by
# register_schema.py. Never run this again after schemas are registered — rotating
# the wrapped DEK requires schema re-registration and re-tokenization of all existing data.
#
# Usage:
#   python3 generate_wrapped_dek.py
#
# Prerequisites (run from register_schema.py setup guide first):
#   - KMS key ring and keys created (see Section 1 of the setup guide)
#   - vm-producer-sa has roles/cloudkms.cryptoKeyEncrypterDecrypter on both keys
#   - Secret Manager secrets dlp-kms-pii-key-name and dlp-kms-pci-key-name already set

import base64
import os

from google.cloud import kms, secretmanager

from utils import get_secret

# ── Configuration ─────────────────────────────────────────────────────────────

PROJECT_ID = "vetsource-496203"

# Secret Manager secret names that will be CREATED (or new-versioned) by this script
SECRET_PII_WRAPPED_DEK = "dlp-pii-wrapped-dek"
SECRET_PCI_WRAPPED_DEK = "dlp-pci-wrapped-dek"

# ── Load KMS key names from Secret Manager ────────────────────────────────────

print("Loading KMS key names from Secret Manager...")
pii_kms_key_name = get_secret(PROJECT_ID, "dlp-kms-pii-key-name")
pci_kms_key_name = get_secret(PROJECT_ID, "dlp-kms-pci-key-name")
print(f"  PII key: {pii_kms_key_name}")
print(f"  PCI key: {pci_kms_key_name}")

# ── Generate and wrap DEKs ────────────────────────────────────────────────────

kms_client = kms.KeyManagementServiceClient()
sm_client  = secretmanager.SecretManagerServiceClient()


def generate_wrapped_dek(kms_key_name: str) -> str:
    """
    Generates a random 32-byte AES-256 key, wraps it with KMS (encrypt),
    and returns the ciphertext as a base64-encoded string.
    The plaintext key is created in memory only and never persisted anywhere.
    """
    raw_aes_key = os.urandom(32)   # 256-bit AES key — exists only in process memory

    response = kms_client.encrypt(
        request={
            "name":       kms_key_name,
            "plaintext":  raw_aes_key,
        }
    )
    # response.ciphertext is the wrapped DEK — safe to store, useless without KMS access
    return base64.b64encode(response.ciphertext).decode("utf-8")


def store_secret(secret_name: str, value: str) -> None:
    """Adds a new version to an existing Secret Manager secret."""
    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_name}"
    sm_client.add_secret_version(
        request={
            "parent":  secret_path,
            "payload": {"data": value.encode("utf-8")},
        }
    )
    print(f"  Stored new version of secret: {secret_name}")


# ── Generate PII domain wrapped DEK ──────────────────────────────────────────

print("\nGenerating PII domain wrapped DEK...")
pii_wrapped_dek = generate_wrapped_dek(pii_kms_key_name)
print(f"  PII wrapped DEK (base64, first 40 chars): {pii_wrapped_dek[:40]}...")
store_secret(SECRET_PII_WRAPPED_DEK, pii_wrapped_dek)

# ── Generate PCI-DSS domain wrapped DEK ──────────────────────────────────────

print("\nGenerating PCI-DSS domain wrapped DEK...")
pci_wrapped_dek = generate_wrapped_dek(pci_kms_key_name)
print(f"  PCI wrapped DEK (base64, first 40 chars): {pci_wrapped_dek[:40]}...")
store_secret(SECRET_PCI_WRAPPED_DEK, pci_wrapped_dek)

# ── Summary ───────────────────────────────────────────────────────────────────

print("\nDone. Wrapped DEKs stored in Secret Manager.")
print("Next step: run register_schema.py to embed these DEKs in the Avro schema.")
print("\nIMPORTANT: Do NOT run this script again unless you are rotating keys and")
print("are prepared to re-tokenize all existing data in the Kafka topic.")
