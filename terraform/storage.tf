# ── GCS Landing Bucket ────────────────────────────────────────────────────────
# Kafka events land here as JSON files before Snowpipe picks them up.

resource "google_storage_bucket" "gcs_landing" {
  name          = "kafka-poc-gcs-landing-${var.project_id}"
  location      = var.region
  project       = var.project_id
  force_destroy = true  # allows terraform destroy to delete the bucket even if it has files

  uniform_bucket_level_access = true

  labels = {
    environment = "poc"
    managed-by  = "terraform"
  }

  depends_on = [google_project_service.apis]
}

# Grant dataflow-pipeline-sa write access to the landing bucket
resource "google_storage_bucket_iam_member" "dataflow_bucket_writer" {
  bucket = google_storage_bucket.gcs_landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_pipeline_sa.email}"
}

# ── Pub/Sub Topic ─────────────────────────────────────────────────────────────
# GCS publishes an OBJECT_FINALIZE event here whenever a new file lands.
# Snowpipe subscribes to the subscription below and triggers a COPY automatically.

resource "google_pubsub_topic" "gcs_notify" {
  name    = "kafka-poc-gcs-notify"
  project = var.project_id

  labels = {
    managed-by = "terraform"
  }

  depends_on = [google_project_service.apis]
}

# ── Pub/Sub Subscription ──────────────────────────────────────────────────────

resource "google_pubsub_subscription" "gcs_notify_sub" {
  name    = "kafka-poc-gcs-notify-sub"
  topic   = google_pubsub_topic.gcs_notify.id
  project = var.project_id

  # Messages not acknowledged within 600s are re-delivered
  ack_deadline_seconds = 600

  labels = {
    managed-by = "terraform"
  }
}

# ── GCS Bucket Notification ───────────────────────────────────────────────────
# Tells GCS to publish an event to the Pub/Sub topic when a file write completes.

resource "google_storage_notification" "gcs_to_pubsub" {
  bucket         = google_storage_bucket.gcs_landing.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.gcs_notify.id
  event_types    = ["OBJECT_FINALIZE"]  # fires when a file is fully written

  depends_on = [google_pubsub_topic_iam_member.gcs_sa_publisher]
}

# ── Grant GCS permission to publish to the Pub/Sub topic ─────────────────────
# GCS uses a Google-managed service agent to publish notifications.
# We must give it Pub/Sub Publisher on the topic.

data "google_storage_project_service_account" "gcs_sa" {
  project = var.project_id
}

resource "google_pubsub_topic_iam_member" "gcs_sa_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.gcs_notify.id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_sa.email_address}"
}
