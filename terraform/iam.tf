# ── Service Accounts ──────────────────────────────────────────────────────────

# SA for the GCE VM running the Kafka producer
resource "google_service_account" "vm_producer_sa" {
  account_id   = "vm-producer-sa"
  display_name = "VM Producer Service Account"
  description  = "Attached to poc-dev-vm; used by producer.py and consumer.py"
  project      = var.project_id

  depends_on = [google_project_service.apis]
}

# SA for the Dataflow pipeline and Managed Kafka Connect cluster
resource "google_service_account" "dataflow_pipeline_sa" {
  account_id   = "dataflow-pipeline-sa"
  display_name = "Dataflow Pipeline Service Account"
  description  = "Used by the Kafka Connect GCS Sink connector and Dataflow jobs"
  project      = var.project_id

  depends_on = [google_project_service.apis]
}

# ── vm-producer-sa roles ───────────────────────────────────────────────────────

# Connect to and produce/consume messages from the Kafka cluster
resource "google_project_iam_member" "vm_producer_kafka_client" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.vm_producer_sa.email}"
}

# Read secrets (bootstrap servers, schema registry URL) from Secret Manager
resource "google_project_iam_member" "vm_producer_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.vm_producer_sa.email}"
}

# Read and write schemas to the GCP Managed Schema Registry (Step 05a)
resource "google_project_iam_member" "vm_producer_schema_registry_editor" {
  project = var.project_id
  role    = "roles/managedkafka.schemaRegistryEditor"
  member  = "serviceAccount:${google_service_account.vm_producer_sa.email}"
}

# ── dataflow-pipeline-sa roles ────────────────────────────────────────────────

# Allows Dataflow to provision and manage worker VMs
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_pipeline_sa.email}"
}

# Consume messages from the Kafka topic
resource "google_project_iam_member" "dataflow_kafka_client" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.dataflow_pipeline_sa.email}"
}

# Read secrets from Secret Manager
resource "google_project_iam_member" "dataflow_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.dataflow_pipeline_sa.email}"
}

# Write files to the GCS landing bucket
resource "google_project_iam_member" "dataflow_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_pipeline_sa.email}"
}

# Read schemas from the GCP Managed Schema Registry (for the GCS Sink connector)
resource "google_project_iam_member" "dataflow_schema_registry_editor" {
  project = var.project_id
  role    = "roles/managedkafka.schemaRegistryEditor"
  member  = "serviceAccount:${google_service_account.dataflow_pipeline_sa.email}"
}
