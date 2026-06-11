# ── VPC Network ───────────────────────────────────────────────────────────────

resource "google_compute_network" "poc_vpc" {
  name                    = "poc-vpc"
  auto_create_subnetworks = false  # custom mode — only create subnets we define
  routing_mode            = "REGIONAL"
  description             = "POC custom VPC for Kafka streaming pipeline"

  depends_on = [google_project_service.apis]
}

# ── Subnet ────────────────────────────────────────────────────────────────────

resource "google_compute_subnetwork" "poc_subnet" {
  name                     = "poc-subnet"
  ip_cidr_range            = "10.0.0.0/22"
  region                   = var.region
  network                  = google_compute_network.poc_vpc.id
  private_ip_google_access = true  # allows VMs without public IPs to reach Google APIs
}

# ── Firewall Rules ─────────────────────────────────────────────────────────────

# Rule 1: Allow SSH from your laptop only
resource "google_compute_firewall" "allow_ssh_laptop" {
  name      = "allow-ssh-from-laptop"
  network   = google_compute_network.poc_vpc.name
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [var.laptop_ip_cidr]
  target_tags   = ["dev-vm"]  # only affects VMs tagged with dev-vm
}

# Rule 2: Allow SSH via Cloud IAP (GCP browser terminal + gcloud compute ssh)
# 35.235.240.0/20 is Google's fixed IP range for IAP — never changes
resource "google_compute_firewall" "allow_iap_ssh" {
  name      = "allow-iap-ssh"
  network   = google_compute_network.poc_vpc.name
  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["dev-vm"]
}

# Rule 3: Allow all traffic between resources inside the subnet
# Needed for VM <-> Kafka broker communication
resource "google_compute_firewall" "allow_internal" {
  name      = "allow-internal"
  network   = google_compute_network.poc_vpc.name
  direction = "INGRESS"

  allow {
    protocol = "all"
  }

  source_ranges = ["10.0.0.0/22"]
}

# Rule 4: Allow outbound HTTPS — needed for Confluent Schema Registry,
# Snowflake, and Google API calls from the VM
resource "google_compute_firewall" "allow_egress_https" {
  name      = "allow-egress-https"
  network   = google_compute_network.poc_vpc.name
  direction = "EGRESS"

  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  destination_ranges = ["0.0.0.0/0"]
}
