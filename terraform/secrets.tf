# ── Secret Manager — Secret Containers ───────────────────────────────────────
# Terraform creates the secret "shells" here.
# Secret VALUES are NOT set in Terraform — they come from actual provisioned
# infrastructure (Kafka bootstrap address, Schema Registry URL, etc.)
# and are entered manually or via a separate secrets pipeline.
#
# To set a value after apply:
#   gcloud secrets versions add <secret-name> --data-file=- <<< "your-value"

locals {
  # Using Step 05a (GCP native Schema Registry) — 8 secrets, not 10.
  # schema-registry-api-key and schema-registry-api-secret are not needed.
  secret_names = [
    "kafka-bootstrap-servers",  # filled after Kafka cluster is Active (Step 04)
    "schema-registry-url",      # filled after Schema Registry is Active (Step 05a)
    "snowflake-account",        # filled after Snowflake setup (Step 10)
    "snowflake-user",           # DATAFLOW_USER
    "snowflake-password",       # set during CREATE USER in Step 10
    "snowflake-database",       # POC_DB
    "snowflake-schema",         # KAFKA_INGEST
    "snowflake-warehouse",      # POC_WH
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
