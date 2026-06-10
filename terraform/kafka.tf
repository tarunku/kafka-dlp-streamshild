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

# ── Kafka Topic ───────────────────────────────────────────────────────────────

resource "google_managed_kafka_topic" "raw_events" {
  provider           = google-beta
  topic_id           = "raw-events"
  cluster            = google_managed_kafka_cluster.poc_cluster.cluster_id
  location           = var.region
  project            = var.project_id
  partition_count    = 3
  replication_factor = 3

  configs = {
    "retention.ms"   = "604800000"  # 7 days in milliseconds
    "cleanup.policy" = "delete"
  }
}

# ── GCP Managed Schema Registry ───────────────────────────────────────────────
# NOTE: google_managed_kafka_schema_registry is a Preview-stage resource.
# If terraform plan errors with "resource type not found", comment this block
# out and create the schema registry manually via gcloud (see APPLY-GUIDE.md).

resource "google_managed_kafka_schema_registry" "poc_registry" {
  provider    = google-beta
  registry_id = "poc-schema-registry"
  location    = var.region
  project     = var.project_id

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
        --vpc-config=network=projects/${var.project_id}/global/networks/${google_compute_network.poc_vpc.name},subnetwork=projects/${var.project_id}/regions/${var.region}/subnetworks/${google_compute_subnetwork.poc_subnet.name} \
        --gcp-service-account=${google_service_account.dataflow_pipeline_sa.email} \
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

# ── GCS Sink Connector ────────────────────────────────────────────────────────
# Deploys the connector via REST API (same call used in guide 11a).
# Runs after the connect cluster is active and the GCS bucket exists.

resource "null_resource" "gcs_sink_connector" {
  provisioner "local-exec" {
    command = <<-EOT
      # Fetch Schema Registry URL from Secret Manager
      SR_URL=$(gcloud secrets versions access latest \
        --secret=schema-registry-url \
        --project=${var.project_id} 2>/dev/null || echo "PLACEHOLDER")

      curl -s -X POST \
        -H "Authorization: Bearer $(gcloud auth print-access-token)" \
        -H "Content-Type: application/json" \
        "https://managedkafka.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/connectClusters/poc-connect-cluster/connectors?connectorId=gcs-sink-order-events" \
        -d "{
          \"configs\": {
            \"connector.class\": \"io.confluent.connect.gcs.GcsSinkConnector\",
            \"tasks.max\": \"1\",
            \"topics\": \"raw-events\",
            \"gcs.bucket.name\": \"${google_storage_bucket.gcs_landing.name}\",
            \"gcs.credentials.default\": \"true\",
            \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
            \"value.converter\": \"io.confluent.connect.avro.AvroConverter\",
            \"value.converter.schema.registry.url\": \"$SR_URL\",
            \"value.converter.schemas.enable\": \"false\",
            \"format.class\": \"io.confluent.connect.gcs.format.json.JsonFormat\",
            \"file.name.prefix\": \"order-events-\",
            \"flush.size\": \"10\",
            \"rotate.interval.ms\": \"60000\",
            \"rotate.schedule.interval.ms\": \"120000\",
            \"storage.class\": \"io.confluent.connect.gcs.storage.GcsStorage\",
            \"locale\": \"en_US\",
            \"timezone\": \"UTC\"
          }
        }" | python3 -m json.tool
    EOT
  }

  depends_on = [
    null_resource.connect_cluster,
    google_storage_bucket.gcs_landing,
    google_managed_kafka_schema_registry.poc_registry,
  ]
}
