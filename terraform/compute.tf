# ── GCE Developer VM ──────────────────────────────────────────────────────────
# This VM is used to run producer.py and consumer.py.
# No external IP — connect via Cloud IAP tunnel (see APPLY-GUIDE.md).

resource "google_compute_instance" "poc_dev_vm" {
  name         = "poc-dev-vm"
  machine_type = var.vm_machine_type
  zone         = var.zone
  project      = var.project_id

  # The dev-vm tag links this VM to the SSH firewall rules in networking.tf
  tags = ["dev-vm"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20  # GB
      type  = "pd-standard"
    }
  }

  network_interface {
    network    = google_compute_network.poc_vpc.id
    subnetwork = google_compute_subnetwork.poc_subnet.id
    # No access_config block = no external IP
    # Connection is via IAP tunnel: gcloud compute ssh --tunnel-through-iap
  }

  # Attach the vm-producer-sa service account.
  # The VM automatically uses this identity for all GCP API calls —
  # no JSON key file needed.
  service_account {
    email  = google_service_account.vm_producer_sa.email
    scopes = ["cloud-platform"]
  }

  # Install Python on first boot so the VM is ready for producer/consumer scripts
  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update -y
    apt-get install -y python3 python3-pip python3-venv python3.11-venv git
    mkdir -p /home/$(id -un 1000)/kafka-poc
  EOF

  labels = {
    environment = "poc"
    managed-by  = "terraform"
  }

  depends_on = [
    google_compute_subnetwork.poc_subnet,
    google_service_account.vm_producer_sa,
    google_project_service.apis,
  ]
}
