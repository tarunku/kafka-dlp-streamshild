# Enable all GCP APIs the pipeline depends on.
# disable_on_destroy = false means Terraform will NOT disable the API
# when you run terraform destroy — safer for shared projects.

resource "google_project_service" "apis" {
  for_each = toset([
    "managedkafka.googleapis.com",      # GCP Managed Kafka
    "compute.googleapis.com",           # GCE VM, VPC, Firewall
    "dataflow.googleapis.com",          # Dataflow (future use)
    "secretmanager.googleapis.com",     # Secret Manager
    "dlp.googleapis.com",               # Cloud DLP (Phase 2)
    "cloudresourcemanager.googleapis.com", # Required by Terraform IAM calls
    "pubsub.googleapis.com",            # Pub/Sub notifications
    "storage.googleapis.com",           # Cloud Storage
    "iam.googleapis.com",               # IAM service accounts
    "iap.googleapis.com",               # Cloud IAP for SSH tunneling
    "cloudkms.googleapis.com",          # Cloud KMS — wraps/unwraps DLP DEKs
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
