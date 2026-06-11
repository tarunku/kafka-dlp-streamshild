# Values printed to the terminal after terraform apply.
# Use these to fill in secret values and configure downstream services.

output "vm_producer_sa_email" {
  description = "Email of the VM producer service account — attach to poc-dev-vm"
  value       = google_service_account.vm_producer_sa.email
}

output "dataflow_pipeline_sa_email" {
  description = "Email of the Dataflow service account — used by Kafka Connect cluster"
  value       = google_service_account.dataflow_pipeline_sa.email
}

output "kafka_cluster_id" {
  description = "Managed Kafka cluster ID"
  value       = google_managed_kafka_cluster.poc_cluster.cluster_id
}

output "kafka_bootstrap_address" {
  description = "Bootstrap server address — copy this into the kafka-bootstrap-servers secret"
  value       = "bootstrap.${google_managed_kafka_cluster.poc_cluster.cluster_id}.${var.region}.managedkafka.${var.project_id}.cloud.goog:9092"
}

output "gcs_landing_bucket" {
  description = "GCS bucket name where Kafka events land"
  value       = google_storage_bucket.gcs_landing.name
}

output "pubsub_subscription" {
  description = "Pub/Sub subscription ID — used in the Snowflake notification integration"
  value       = google_pubsub_subscription.gcs_notify_sub.id
}

output "poc_dev_vm_name" {
  description = "GCE VM name — use with: gcloud compute ssh poc-dev-vm --tunnel-through-iap"
  value       = google_compute_instance.poc_dev_vm.name
}

output "poc_dev_vm_zone" {
  description = "Zone of the GCE VM"
  value       = google_compute_instance.poc_dev_vm.zone
}

output "kms_pii_key_name" {
  description = "Full resource name of the PII KMS key — auto-stored in dlp-kms-pii-key-name secret"
  value       = google_kms_crypto_key.pii_dek_kek.id
}

output "kms_pci_key_name" {
  description = "Full resource name of the PCI KMS key — auto-stored in dlp-kms-pci-key-name secret"
  value       = google_kms_crypto_key.pci_dek_kek.id
}

output "schema_registry_url" {
  description = "Schema Registry URL — copy this into the schema-registry-url secret"
  value       = "https://managedkafka.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/schemaRegistries/poc_schema_registry"
}

output "next_steps" {
  description = "Manual steps required after terraform apply"
  value       = <<-EOT

    ── Next steps after apply ────────────────────────────────────────
    1. Copy the kafka_bootstrap_address output into Secret Manager:
         gcloud secrets versions add kafka-bootstrap-servers \
           --data-file=- --project=${var.project_id} \
           <<< "bootstrap.poc-kafka-cluster.${var.region}.managedkafka.${var.project_id}.cloud.goog:9092"

    2. Copy the schema_registry_url output into Secret Manager:
         gcloud secrets versions add schema-registry-url \
           --data-file=- --project=${var.project_id} \
           <<< "https://managedkafka.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/schemaRegistries/poc_schema_registry"

       NOTE: dlp-kms-pii-key-name and dlp-kms-pci-key-name are auto-populated
       by Terraform — no manual action needed for those two secrets.

    3. SSH into the VM and run the one-time crypto setup:
         gcloud compute ssh poc-dev-vm \
           --project=${var.project_id} \
           --zone=${var.zone} \
           --tunnel-through-iap
         cd kafka-poc/streamshield && source venv/bin/activate
         python3 examples/generate_wrapped_dek.py   # wraps DEKs into Secret Manager
         python3 examples/register_schema.py         # embeds DEKs in Avro schema

    4. Run the examples:
         python3 examples/prescription_producer.py
         python3 examples/tokenized_consumer.py
         python3 examples/detokenized_consumer.py

    5. Complete Snowflake setup (Step 10) and update remaining snowflake-* secrets.
    ──────────────────────────────────────────────────────────────────
  EOT
}
