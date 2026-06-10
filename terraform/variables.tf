variable "project_id" {
  description = "GCP project ID of the test project"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the GCE VM"
  type        = string
  default     = "us-central1-a"
}

variable "laptop_ip_cidr" {
  description = "Your laptop's public IP in CIDR notation (e.g. 203.0.113.45/32). Used for the SSH firewall rule. Find yours at https://whatismyip.com"
  type        = string
}

variable "vm_machine_type" {
  description = "Machine type for the GCE developer VM"
  type        = string
  default     = "e2-medium"
}

variable "kafka_broker_count" {
  description = "Number of Kafka brokers. Minimum 3 for replication factor 3"
  type        = number
  default     = 3
}
