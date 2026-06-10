# ── Cloud KMS — DLP Key Encryption Keys (KEKs) ────────────────────────────────
# These keys wrap the AES-256 Data Encryption Keys (DEKs) used by Cloud DLP.
# The wrapped DEKs are stored in Secret Manager and embedded in the Avro schema.
#
# Key hierarchy:
#   KMS key (KEK)  →  wraps  →  AES-256 DEK (in memory only)
#   Avro schema carries the wrapped DEK; DLP calls KMS to unwrap at tokenize time.
#
# Two separate keys keep PII and PCI-DSS audit logs cleanly separated in Cloud KMS.
#
# WARNING: Never delete these keys after generate_wrapped_dek.py has run.
# Deletion schedules the key for destruction after 24 hours; any data tokenized
# with the corresponding DEK becomes permanently unrecoverable.

resource "google_kms_key_ring" "dlp_ring" {
  name     = "dlp-kms-ring"
  location = "global"   # matches vetsource-496203 convention; global works across all DLP regions
  project  = var.project_id

  depends_on = [google_project_service.apis]
}

resource "google_kms_crypto_key" "pii_dek_kek" {
  name     = "pii-dek-kek"
  key_ring = google_kms_key_ring.dlp_ring.id
  purpose  = "ENCRYPT_DECRYPT"

  labels = {
    domain     = "pii"
    managed-by = "terraform"
  }
}

resource "google_kms_crypto_key" "pci_dek_kek" {
  name     = "pci-dek-kek"
  key_ring = google_kms_key_ring.dlp_ring.id
  purpose  = "ENCRYPT_DECRYPT"

  labels = {
    domain     = "pci-dss"
    managed-by = "terraform"
  }
}

# ── KMS IAM — vm-producer-sa ──────────────────────────────────────────────────
# EncrypterDecrypter is required for two distinct operations:
#   Encrypter: generate_wrapped_dek.py wraps the raw AES key with KMS
#   Decrypter: DLP calls KMS (acting as the calling SA) to unwrap the DEK at
#              tokenize and detokenize time

resource "google_kms_crypto_key_iam_member" "vm_producer_pii_kms" {
  crypto_key_id = google_kms_crypto_key.pii_dek_kek.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${google_service_account.vm_producer_sa.email}"
}

resource "google_kms_crypto_key_iam_member" "vm_producer_pci_kms" {
  crypto_key_id = google_kms_crypto_key.pci_dek_kek.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:${google_service_account.vm_producer_sa.email}"
}
