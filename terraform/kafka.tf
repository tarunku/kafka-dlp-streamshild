# ── Managed Kafka Cluster ─────────────────────────────────────────────────────
# Uses google-beta provider — Managed Kafka is not yet in the stable provider.

resource "google_managed_kafka_cluster" "poc_cluster" {
  provider   = google-beta
  cluster_id = "poc-kafka-cluster"
  location   = var.region
  project    = var.project_id

  capacity_config {
    vcpu_count   = 3      # minimum per broker
    memory_bytes = 3221225472  # 3 GB per broker
  }

  gcp_config {
    access_config {
      network_configs {
        # Full resource URL required — Terraform builds this from your subnet
        subnet = "projects/${var.project_id}/regions/${var.region}/subnetworks/${google_compute_subnetwork.poc_subnet.name}"
      }
    }
  }

  # Cost warning: ~$1.50-2.00/hr while running. Delete when not testing.
  labels = {
    environment = "poc"
    managed-by  = "terraform"
  }

  depends_on = [
    google_project_service.apis,
    google_compute_subnetwork.poc_subnet,
  ]
}


# ── GCP Managed Schema Registry ───────────────────────────────────────────────
# google_managed_kafka_schema_registry does not exist in hashicorp/google-beta yet.
# Fall back to gcloud (same pattern as null_resource.connect_cluster above).

resource "null_resource" "schema_registry" {
  provisioner "local-exec" {
    command = <<-EOT
      gcloud alpha managed-kafka schema-registries create poc_schema_registry \
        --location=${var.region} \
        --project=${var.project_id} \
        --quiet || echo "Schema registry may already exist, skipping."
    EOT
  }

  depends_on = [google_managed_kafka_cluster.poc_cluster]
}

# ── Managed Kafka Connect Cluster ─────────────────────────────────────────────
# NOTE: google_managed_kafka_connect_cluster may not exist in the provider yet.
# This null_resource falls back to gcloud CLI if the Terraform resource is unavailable.
# Remove this block and use the google_managed_kafka_connect_cluster resource
# once it becomes available in google-beta.

resource "null_resource" "connect_cluster" {
  provisioner "local-exec" {
    command = <<-EOT
      gcloud managed-kafka connect-clusters create poc-connect-cluster \
        --location=${var.region} \
        --kafka-cluster=${google_managed_kafka_cluster.poc_cluster.cluster_id} \
        --primary-subnet=projects/${var.project_id}/regions/${var.region}/subnetworks/${google_compute_subnetwork.poc_subnet.name} \
        --cpu=3 \
        --memory=3GiB \
        --project=${var.project_id} \
        --quiet || echo "Connect cluster may already exist, skipping."
    EOT
  }

  depends_on = [
    google_managed_kafka_cluster.poc_cluster,
    google_service_account.dataflow_pipeline_sa,
    google_project_iam_member.dataflow_storage_admin,
  ]
}

# GCS Sink Connector is created manually after terraform apply.
# Topics must exist before the connector is deployed.
# See APPLY-GUIDE.md Phase 3 for the gcloud commands.
