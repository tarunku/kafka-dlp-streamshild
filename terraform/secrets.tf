# ── Secret Manager — Secret Containers ───────────────────────────────────────
# Terraform creates the secret "shells" here.
# Secret VALUES are NOT set in Terraform — they come from actual provisioned
# infrastructure (Kafka bootstrap address, Schema Registry URL, etc.)
# and are entered manually or via a separate secrets pipeline.
#
# To set a value after apply:
#   gcloud secrets versions add <secret-name> --data-file=- <<< "your-value"

locals {
  # Using Step 05a (GCP native Schema Registry) — 8 pipeline secrets + 4 DLP secrets.
  # dlp-kms-*-key-name shells are created here; their VALUES are auto-populated below
  # once the KMS keys exist. dlp-*-wrapped-dek shells are filled by generate_wrapped_dek.py.
  secret_names = [
    "kafka-bootstrap-servers",  # filled after Kafka cluster is Active (Step 04)
    "schema-registry-url",      # filled after Schema Registry is Active (Step 05a)
    "snowflake-account",        # filled after Snowflake setup (Step 10)
    "snowflake-user",           # DATAFLOW_USER
    "snowflake-password",       # set during CREATE USER in Step 10
    "snowflake-database",       # POC_DB
    "snowflake-schema",         # KAFKA_INGEST
    "snowflake-warehouse",      # POC_WH
    "dlp-kms-pii-key-name",     # auto-populated below from KMS resource name
    "dlp-kms-pci-key-name",     # auto-populated below from KMS resource name
    "dlp-pii-wrapped-dek",      # filled by examples/generate_wrapped_dek.py
    "dlp-pci-wrapped-dek",      # filled by examples/generate_wrapped_dek.py
  ]
}

resource "google_secret_manager_secret" "pipeline_secrets" {
  for_each  = toset(local.secret_names)
  secret_id = each.value
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    managed-by = "terraform"
  }

  depends_on = [google_project_service.apis]
}

# ── Auto-populate KMS key name secrets ────────────────────────────────────────
# The full KMS resource names are known at apply time, so we store them
# automatically. generate_wrapped_dek.py reads these to know which key to call.

resource "google_secret_manager_secret_version" "dlp_kms_pii_key_name" {
  secret      = google_secret_manager_secret.pipeline_secrets["dlp-kms-pii-key-name"].id
  secret_data = google_kms_crypto_key.pii_dek_kek.id

  depends_on = [google_kms_crypto_key.pii_dek_kek]
}

resource "google_secret_manager_secret_version" "dlp_kms_pci_key_name" {
  secret      = google_secret_manager_secret.pipeline_secrets["dlp-kms-pci-key-name"].id
  secret_data = google_kms_crypto_key.pci_dek_kek.id

  depends_on = [google_kms_crypto_key.pci_dek_kek]
}
