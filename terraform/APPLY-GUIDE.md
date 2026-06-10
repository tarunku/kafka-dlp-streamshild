# Terraform Apply Guide — GCP Kafka POC

Step-by-step instructions to run these Terraform templates from the GCE VM.

---

## Required Permissions

### Who should run Terraform?

**The person running Terraform needs Project Owner on the test GCP project.**

| Role | Where | Why |
|---|---|---|
| `roles/owner` | Test GCP project | Covers creating VPC, IAM, Kafka, GCS, Pub/Sub, Secrets, VM |
| `roles/resourcemanager.projectCreator` | GCP Organisation level | Only needed if YOU are creating the project itself |
| `roles/billing.user` | Billing account | Only needed if attaching a billing account to the project |

> **Project Owner vs Organisation Admin:**
> - **Project Owner** — runs day-to-day Terraform. This is the right role for a developer on a test project.
> - **Organisation Admin** — only needed to create new projects under the org. Once the project exists and billing is linked, Project Owner is sufficient.
>
> You do NOT need to be an Organisation Admin to run these templates. Ask your org admin to create the project and add you as Project Owner.

---

## Prerequisites on the GCE VM

These are one-time steps. If you already did them, skip ahead to **Step 3**.

### Step 1 — SSH into the GCE VM

```bash
gcloud compute ssh poc-dev-vm \
  --project=YOUR_TEST_PROJECT_ID \
  --zone=us-central1-a \
  --tunnel-through-iap
```

### Step 2 — Install Terraform on the VM

```bash
# Add the HashiCorp package repository
sudo apt-get update -y
sudo apt-get install -y gnupg software-properties-common curl

curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp.gpg

echo "deb [signed-by=/usr/share/keyrings/hashicorp.gpg] \
  https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
  sudo tee /etc/apt/sources.list.d/hashicorp.list

sudo apt-get update -y
sudo apt-get install -y terraform

# Verify installation
terraform -version
```

Expected output: `Terraform v1.x.x`

---

## Applying the Templates

### Step 3 — Copy the Terraform files to the VM

**Option A — if the repo is on GitHub:**
```bash
git clone https://github.com/YOUR_ORG/vetsource.git
cd vetsource/terraform
```

**Option B — copy files manually from your laptop:**
```bash
# Run this on your LAPTOP, not the VM
gcloud compute scp --recurse ./terraform poc-dev-vm:~/terraform \
  --project=YOUR_TEST_PROJECT_ID \
  --zone=us-central1-a \
  --tunnel-through-iap
```

Then on the VM:
```bash
cd ~/terraform
```

### Step 4 — Authenticate to GCP

The VM's attached service account (`vm-producer-sa`) does not have enough
permissions to create infrastructure. You need to log in with your personal
GCP account that has Project Owner on the test project.

```bash
gcloud auth application-default login
```

This prints a URL — open it in your laptop's browser, log in with your
Google account, and paste the auth code back into the terminal.

Confirm authentication worked:
```bash
gcloud auth application-default print-access-token
```

If a token prints, you are authenticated.

### Step 5 — Create your variables file

```bash
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars   # or use vim
```

Fill in the required values:

```hcl
project_id     = "your-test-project-id"   # the test project ID
region         = "us-central1"
zone           = "us-central1-a"
laptop_ip_cidr = "203.0.113.45/32"        # your laptop public IP + /32
```

> **Find your laptop IP:** On your laptop, search "what is my ip" in a browser.
> Then add `/32` at the end. Example: `203.0.113.45/32`

### Step 6 — Initialise Terraform

Downloads the GCP provider plugins. Run once, or whenever providers change.

```bash
terraform init
```

Expected output:
```
Terraform has been successfully initialized!
```

### Step 7 — Preview what will be created

```bash
terraform plan
```

Terraform shows every resource it will create with a `+` prefix.
Nothing is touched yet — this is a dry run.

Read through the output and confirm:
- `+` means will be created
- No `-` (destroy) lines should appear on a fresh run

### Step 8 — Apply (create all resources)

```bash
terraform apply
```

Terraform will show the plan again and ask:
```
Do you want to perform these actions? Enter a value:
```

Type `yes` and press Enter.

> **Time estimate:**
> - VPC, IAM, GCS, Pub/Sub, Secrets: ~1–2 minutes
> - GCE VM: ~2–3 minutes
> - Managed Kafka cluster: ~10–15 minutes (longest step)
> - Schema Registry: ~1–2 minutes
> - Kafka Connect cluster: ~3–5 minutes

Total: approximately **20–25 minutes** end to end.

### Step 9 — Review the outputs

After apply completes, Terraform prints the outputs defined in `outputs.tf`:

```
Outputs:

kafka_bootstrap_address = "bootstrap.poc-kafka-cluster.us-central1..."
gcs_landing_bucket      = "kafka-poc-gcs-landing-your-project"
pubsub_subscription     = "projects/.../subscriptions/kafka-poc-gcs-notify-sub"
poc_dev_vm_name         = "poc-dev-vm"
next_steps              = ...
```

Copy the `kafka_bootstrap_address` — you will need it in the next step.

---

## Post-Apply: Fill In Secret Values

Terraform creates the secret containers but not the values. Fill them in now.

### kafka-bootstrap-servers

```bash
gcloud secrets versions add kafka-bootstrap-servers \
  --project=YOUR_TEST_PROJECT_ID \
  --data-file=- <<< "PASTE_BOOTSTRAP_ADDRESS_FROM_OUTPUT"
```

### schema-registry-url

Get the URL from the GCP Console:
**Managed Kafka > Schema Registries > poc-schema-registry > Endpoint**

```bash
gcloud secrets versions add schema-registry-url \
  --project=YOUR_TEST_PROJECT_ID \
  --data-file=- <<< "https://managedkafka.googleapis.com/v1main/projects/YOUR_TEST_PROJECT_ID/locations/us-central1/schemaRegistries/poc-schema-registry"
```

---

## Verify Resources Were Created

Open the GCP Console and confirm:

- [ ] **VPC Network** → `poc-vpc` exists with subnet `poc-subnet`
- [ ] **Firewall** → 4 rules: `allow-ssh-from-laptop`, `allow-iap-ssh`, `allow-internal`, `allow-egress-https`
- [ ] **IAM** → `vm-producer-sa` and `dataflow-pipeline-sa` visible in Service Accounts
- [ ] **Managed Kafka** → `poc-kafka-cluster` status = Active
- [ ] **Managed Kafka** → topic `raw-events` with 3 partitions
- [ ] **Managed Kafka** → Schema Registry `poc-schema-registry` status = Active
- [ ] **Managed Kafka** → Connect cluster `poc-connect-cluster` status = Active
- [ ] **Compute Engine** → `poc-dev-vm` running
- [ ] **Cloud Storage** → `kafka-poc-gcs-landing-<project>` bucket exists
- [ ] **Pub/Sub** → topic `kafka-poc-gcs-notify` and subscription `kafka-poc-gcs-notify-sub`
- [ ] **Secret Manager** → 8 secrets listed

---

## Tearing Down (Destroy Everything)

When testing is complete, destroy all resources to stop billing:

```bash
terraform destroy
```

Type `yes` when prompted.

> **Note:** The Managed Kafka cluster costs ~$1.50–2.00/hr. Always run
> `terraform destroy` when you are done testing. The GCS bucket will also
> be deleted (`force_destroy = true` is set in storage.tf).

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Error: googleapi: Error 403: Required 'compute.networks.create' permission` | Your account lacks Project Owner on the test project | Ask org admin to add you as Project Owner |
| `Error: resource type "google_managed_kafka_schema_registry" not found` | Schema Registry resource not yet in google-beta provider | Comment out that resource in kafka.tf and create it manually via gcloud |
| `Error authenticating: could not find default credentials` | Not authenticated on the VM | Re-run `gcloud auth application-default login` |
| `terraform init` fails — can't download providers | VM has no internet access | Confirm `allow-egress-https` firewall rule exists and Private Google Access is On for the subnet |
| Kafka cluster stuck in `CREATING` for >20 min | Transient GCP issue | Run `terraform apply` again — it will resume where it left off |
