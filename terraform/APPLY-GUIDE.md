# Terraform Apply Guide — `terraform-testing-498903`

Complete step-by-step instructions to provision the StreamShield infrastructure
from `poc-dev-vm-2` (vetsource-496203) and run `streamshield/examples` end-to-end
in the new project.

---

## Overview

```
PHASE 1  Install Terraform on poc-dev-vm-2 and authenticate
PHASE 2  Run Terraform → provisions all GCP resources in terraform-testing-498903  (~20–25 min)
PHASE 3  Create Kafka topics + GCS Sink Connector manually
PHASE 4  Fill 2 secrets manually (bootstrap servers + schema registry URL)
PHASE 5  Copy SDK code to the new VM and run generate_wrapped_dek.py  (one-time)
PHASE 6  Run register_schema.py — registers Avro schema with DLP metadata  (one-time)
PHASE 7  Run examples — producer, tokenized consumer, detokenized consumer
```

**Where each phase runs:**

| Phase | Runs on |
|---|---|
| 1 – 4 | `poc-dev-vm-2` in `vetsource-496203` |
| 5 – 7 | `poc-dev-vm` in `terraform-testing-498903` (created by Terraform in Phase 2) |

---

## Required Permissions

| Role | Where | Why |
|---|---|---|
| `roles/owner` | `terraform-testing-498903` | Creates VPC, IAM, Kafka, KMS, GCS, Pub/Sub, Secrets, VM |

Confirm you have it before starting:
```bash
gcloud projects get-iam-policy terraform-testing-498903 \
  --flatten=bindings \
  --filter=bindings.members:tarunkumar@fusionleap.io \
  --format="table(bindings.role)"
```

You should see `roles/owner` in the output.

---

## PHASE 1 — Set Up Terraform on `poc-dev-vm-2`

#### 1-1. SSH into poc-dev-vm-2

Run this from your current terminal (wherever `gcloud` is available):

```bash
gcloud compute ssh poc-dev-vm-2 \
  --project=vetsource-496203 \
  --zone=us-central1-a \
  --tunnel-through-iap
```

All remaining Phase 1–3 commands run **inside this SSH session**.

#### 1-2. Install Terraform

```bash
sudo apt-get update -y
sudo apt-get install -y gnupg software-properties-common curl

curl -fsSL https://apt.releases.hashicorp.com/gpg | \
  sudo gpg --dearmor -o /usr/share/keyrings/hashicorp.gpg

echo "deb [signed-by=/usr/share/keyrings/hashicorp.gpg] \
  https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
  sudo tee /etc/apt/sources.list.d/hashicorp.list

sudo apt-get update -y
sudo apt-get install -y terraform
```

Verify (must be ≥ 1.5):
```bash
terraform -version
# Expected: Terraform v1.x.x
```

#### 1-3. Authenticate with your personal GCP account

The VM's attached service account (`vm-producer-sa`) has roles only in `vetsource-496203`
and cannot create resources in `terraform-testing-498903`. You must log in with your
personal account that has Project Owner on the new project.

```bash
gcloud auth application-default login
```

This prints a URL. Open it in **your browser**, log in with `tarunkumar@fusionleap.io`,
and paste the authorisation code back into the terminal.

Verify authentication worked:
```bash
gcloud auth application-default print-access-token
# Should print a long token string (not an error)
```

Also set your active account for `gcloud` commands:
```bash
gcloud config set account tarunkumar@fusionleap.io
```

---

## PHASE 2 — Run Terraform

All commands in this phase run on **`poc-dev-vm-2`** inside the `~/kafka-poc/terraform` directory.

#### 2-1. Navigate to the Terraform directory

The repo is already on this VM at:

```bash
cd /home/tarunkumar_fusionleap_io/kafka-stramshild/kafka-dlp-streamshild/terraform
```

#### 2-2. Find the VM's external IP for the firewall rule

The `laptop_ip_cidr` variable controls which IP can SSH directly to the new VM.
Since you will always use IAP tunnelling (no direct SSH), you can use any placeholder.
If you want to allow direct SSH from this VM's IP in future:

```bash
# Get the external IP of poc-dev-vm-2 (will be empty if no external IP)
curl -s https://ifconfig.me && echo
```

Use the returned IP as `X.X.X.X/32`, or use `35.235.240.0/20` (the IAP range, which
is already covered by a separate firewall rule — fine as a placeholder here).

#### 2-3. Create your variables file

```bash
cp terraform.tfvars.example terraform.tfvars
nano terraform.tfvars
```

Fill in exactly:
```hcl
project_id      = "terraform-testing-498903"
region          = "us-central1"
zone            = "us-central1-a"
laptop_ip_cidr  = "35.235.240.0/20"   # IAP range — used as placeholder; IAP SSH rule already covers SSH access
vm_machine_type = "e2-medium"
kafka_broker_count = 3
```

Save and exit (`Ctrl+O`, `Enter`, `Ctrl+X` in nano).

> `terraform.tfvars` is in `.gitignore` and will never be committed.

#### 2-4. Initialise Terraform

Downloads provider plugins. Run once per new working directory.

```bash
terraform init
```

Expected output:
```
Terraform has been successfully initialized!
```

If this fails with a network error, confirm Private Google Access is enabled on
`poc-subnet` in `vetsource-496203` (it should be — it was set up in the original POC).

#### 2-5. Preview — dry run

```bash
terraform plan
```

You should see **~40+ resources** marked with `+`. No `-` (destroy) lines on a fresh project.

Confirm these key resources appear in the plan:

```
+ google_kms_key_ring.dlp_ring
+ google_kms_crypto_key.pii_dek_kek
+ google_kms_crypto_key.pci_dek_kek
+ google_kms_crypto_key_iam_member.vm_producer_pii_kms
+ google_kms_crypto_key_iam_member.vm_producer_pci_kms
+ google_managed_kafka_cluster.poc_cluster
+ google_managed_kafka_topic.prescription_events
+ google_managed_kafka_topic.raw_events
+ null_resource.schema_registry
+ google_secret_manager_secret_version.dlp_kms_pii_key_name
+ google_secret_manager_secret_version.dlp_kms_pci_key_name
+ google_compute_instance.poc_dev_vm
+ google_project_iam_member.vm_producer_dlp_user
```

The schema registry is provisioned via `null_resource.schema_registry` (gcloud fallback)
because `google_managed_kafka_schema_registry` is not yet in the Terraform provider.

#### 2-6. Apply — create all resources

```bash
terraform apply
```

Type `yes` when prompted.

**Time estimates:**

| Resource group | Approximate time |
|---|---|
| VPC, IAM, GCS, Pub/Sub, KMS, Secrets | ~2–3 min |
| GCE VM | ~2–3 min |
| Managed Kafka cluster | ~10–15 min ← slowest step |
| Schema Registry | ~2–3 min |
| Kafka Connect cluster | ~3–5 min |
| **Total** | **~20–25 min** |

Leave the terminal open. Do not interrupt the apply.

#### 2-7. Review the outputs

After apply finishes, copy these two values — you need them in Phase 4:

```
Outputs:

kafka_bootstrap_address = "bootstrap.poc-kafka-cluster.us-central1.managedkafka.terraform-testing-498903.cloud.goog:9092"
schema_registry_url     = "https://managedkafka.googleapis.com/v1/projects/terraform-testing-498903/locations/us-central1/schemaRegistries/poc_schema_registry"
kms_pii_key_name        = "projects/terraform-testing-498903/locations/global/keyRings/dlp-kms-ring/cryptoKeys/pii-dek-kek"
kms_pci_key_name        = "projects/terraform-testing-498903/locations/global/keyRings/dlp-kms-ring/cryptoKeys/pci-dek-kek"
gcs_landing_bucket      = "kafka-poc-gcs-landing-terraform-testing-498903"
poc_dev_vm_name         = "poc-dev-vm"
```

> `kms_pii_key_name` and `kms_pci_key_name` are **already written to Secret Manager**
> by Terraform. You do not need to fill those in manually.

---

## PHASE 3 — Create Kafka Topics and GCS Sink Connector

Still running on **`poc-dev-vm-2`** in `vetsource-496203`.

Kafka topics and the GCS Sink Connector are created manually — the google-beta Terraform
provider has a bug with topic resources, and the connector depends on topics existing first.

#### 3-1. Wait for the Connect cluster to be Active

```bash
gcloud managed-kafka connect-clusters describe poc-connect-cluster \
  --location=us-central1 \
  --project=terraform-testing-498903 \
  --format="value(state)"
```

Re-run until it shows `ACTIVE` (takes ~3–5 min after Terraform finishes).

#### 3-2. Create Kafka topics

```bash
gcloud managed-kafka topics create prescription-events \
  --cluster=poc-kafka-cluster \
  --location=us-central1 \
  --partitions=3 \
  --replication-factor=3 \
  --configs=retention.ms=604800000,cleanup.policy=delete \
  --project=terraform-testing-498903

gcloud managed-kafka topics create raw-events \
  --cluster=poc-kafka-cluster \
  --location=us-central1 \
  --partitions=3 \
  --replication-factor=3 \
  --configs=retention.ms=604800000,cleanup.policy=delete \
  --project=terraform-testing-498903
```

#### 3-3. Deploy the GCS Sink Connector

```bash
SR_URL="https://managedkafka.googleapis.com/v1/projects/terraform-testing-498903/locations/us-central1/schemaRegistries/poc_schema_registry"

curl -s -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  "https://managedkafka.googleapis.com/v1/projects/terraform-testing-498903/locations/us-central1/connectClusters/poc-connect-cluster/connectors?connectorId=gcs-sink-order-events" \
  -d "{
    \"configs\": {
      \"connector.class\": \"io.confluent.connect.gcs.GcsSinkConnector\",
      \"tasks.max\": \"1\",
      \"topics\": \"raw-events\",
      \"gcs.bucket.name\": \"kafka-poc-gcs-landing-terraform-testing-498903\",
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
```

A successful response shows the connector config echoed back with no `error` key.

---

## PHASE 4 — Fill 2 Secrets Manually

Still running on **`poc-dev-vm-2`** in `vetsource-496203`.

Terraform creates 12 secret containers but only auto-populates the 2 KMS key name secrets.
Fill in the two infrastructure-dependent secrets now.

#### 3-1. kafka-bootstrap-servers

```bash
gcloud secrets versions add kafka-bootstrap-servers \
  --project=terraform-testing-498903 \
  --data-file=- \
  <<< "bootstrap.poc-kafka-cluster.us-central1.managedkafka.terraform-testing-498903.cloud.goog:9092"
```

#### 3-2. schema-registry-url

```bash
gcloud secrets versions add schema-registry-url \
  --project=terraform-testing-498903 \
  --data-file=- \
  <<< "https://managedkafka.googleapis.com/v1/projects/terraform-testing-498903/locations/us-central1/schemaRegistries/poc_schema_registry"
```

#### 3-3. Verify all 4 required secrets have values

```bash
for secret in kafka-bootstrap-servers schema-registry-url dlp-kms-pii-key-name dlp-kms-pci-key-name; do
  echo -n "$secret: "
  gcloud secrets versions access latest \
    --secret=$secret \
    --project=terraform-testing-498903
  echo
done
```

All four should print values. The two KMS secrets will show full resource paths like
`projects/terraform-testing-498903/locations/global/keyRings/dlp-kms-ring/cryptoKeys/pii-dek-kek`.

> **Snowflake secrets** (`snowflake-account`, `snowflake-user`, etc.) can remain empty
> for now — they are only needed for the GCS → Snowflake pipeline, not the SDK examples.

---

## PHASE 5 — Copy Code to New VM and Generate Wrapped DEKs

From this phase onward, all commands run on **`poc-dev-vm`** in `terraform-testing-498903`.

#### 5-1. Copy the StreamShield repo from poc-dev-vm-2 to the new VM

Run this on **`poc-dev-vm-2`** (your current session):

```bash
# First, copy from poc-dev-vm-2 (vetsource) to poc-dev-vm (terraform-testing)
# using gcloud compute scp with IAP tunnelling

gcloud compute scp --recurse \
  /home/tarunkumar_fusionleap_io/kafka-stramshild/kafka-dlp-streamshild/streamshield \
  poc-dev-vm:~/kafka-poc/streamshield \
  --project=terraform-testing-498903 \
  --zone=us-central1-a \
  --tunnel-through-iap
```

Expected: files copy silently. If it fails, see Troubleshooting.

#### 5-2. SSH into the new VM

```bash
gcloud compute ssh poc-dev-vm \
  --project=terraform-testing-498903 \
  --zone=us-central1-a \
  --tunnel-through-iap
```

All remaining Phase 5–7 commands run **inside this new SSH session**.

#### 5-3. Verify Python is installed

The VM startup script ran `apt-get install python3 python3-pip python3-venv` on boot.

```bash
python3 --version
# Expected: Python 3.x.x
```

If Python is not found (startup script may still be running):
```bash
sudo apt-get update -y && sudo apt-get install -y python3 python3-pip python3.11-venv
```

#### 5-4. Set up the Python virtual environment

```bash
cd ~/kafka-poc/streamshield
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
```

Verify the SDK installed correctly:
```bash
python3 -c "import streamshield; print('StreamShield import OK')"
```

#### 5-5. Update the project ID in the setup scripts

`generate_wrapped_dek.py` and `register_schema.py` have `PROJECT_ID` hardcoded to
`vetsource-496203`. Replace it for the new project:

```bash
sed -i 's/vetsource-496203/terraform-testing-498903/g' \
  examples/generate_wrapped_dek.py \
  examples/register_schema.py \
  examples/prescription_producer.py \
  examples/tokenized_consumer.py \
  examples/detokenized_consumer.py \
  examples/streamshield-config.yaml
```

Verify:
```bash
grep -r "vetsource-496203" examples/
# Should return no output — all references replaced
```

#### 5-6. Run generate_wrapped_dek.py

Generates two random AES-256 keys in memory, wraps them with Cloud KMS,
and stores the wrapped ciphertext in Secret Manager. The plaintext keys
never leave process memory.

```bash
python3 examples/generate_wrapped_dek.py
```

Expected output:
```
Loading KMS key names from Secret Manager...
  PII key: projects/terraform-testing-498903/locations/global/keyRings/dlp-kms-ring/cryptoKeys/pii-dek-kek
  PCI key: projects/terraform-testing-498903/locations/global/keyRings/dlp-kms-ring/cryptoKeys/pci-dek-kek

Generating PII domain wrapped DEK...
  PII wrapped DEK (base64, first 40 chars): CiQA...
  Stored new version of secret: dlp-pii-wrapped-dek

Generating PCI-DSS domain wrapped DEK...
  PCI wrapped DEK (base64, first 40 chars): CiQA...
  Stored new version of secret: dlp-pci-wrapped-dek

Done. Wrapped DEKs stored in Secret Manager.
Next step: python3 examples/register_schema.py

IMPORTANT: Do NOT run this script again unless you are rotating keys and
are prepared to re-tokenize all existing data in the Kafka topic.
```

---

## PHASE 6 — Register the Avro Schema

Still on **`poc-dev-vm`** in `terraform-testing-498903`, with venv active.

Registers the `PrescriptionOrder` Avro schema with the KMS key names and
wrapped DEKs embedded in its metadata. This is a one-time operation.

#### 6-1. Run register_schema.py

```bash
python3 examples/register_schema.py
```

Expected output:
```
Loading KMS keys and wrapped DEKs from Secret Manager...
  PII KMS key : projects/terraform-testing-498903/.../pii-dek-kek
  PCI KMS key : projects/terraform-testing-498903/.../pci-dek-kek

Registering schema under subject 'prescription-events-value'...
Schema registered — ID: 1, version: 1
Subject: prescription-events-value

Next step: run prescription_producer.py to publish tokenized events.
```

#### 6-2. Verify the schema was registered

```bash
SR_URL=$(gcloud secrets versions access latest \
  --secret=schema-registry-url \
  --project=terraform-testing-498903)

TOKEN=$(gcloud auth print-access-token)

curl -s -H "Authorization: Bearer $TOKEN" "$SR_URL/subjects"
# Expected: ["prescription-events-value"]
```

---

## PHASE 7 — Run the StreamShield Examples

Still on **`poc-dev-vm`** in `terraform-testing-498903`, with venv active.

#### 7-1. Run the producer

Produces 5 prescription orders. Sensitive fields (`owner_name`, `owner_email`,
`owner_payment_card`, `pet_name`) are tokenized by Cloud DLP before reaching Kafka.

```bash
python3 examples/prescription_producer.py
```

Expected output:
```
Producing 5 prescription orders to 'prescription-events'...
────────────────────────────────────────────────────────────

Message 1: RX-4F2A1B3C
  Plaintext  owner_name  : Sarah Mitchell
  Plaintext  owner_email : sarah@example.com
  Plaintext  card        : 4111111111111111
  Queued: topic=prescription-events
...
────────────────────────────────────────────────────────────
All 5 messages delivered.
```

#### 7-2. Run the tokenized consumer

Reads from `prescription-events` **without** DLP access. Sensitive fields appear as
opaque tokens — this is what any downstream subscriber without KMS access would see.

```bash
python3 examples/tokenized_consumer.py
```

Expected output:
```
Subscribed to 'prescription-events' as group 'streamshield-tokenized-consumer'.
Printing TOKENIZED data (no DLP access — tokens visible as-is).
Press Ctrl+C to stop.

Message  partition=0 offset=0
───────────────────────────────────────────────────────────────────
    order_id                : RX-4F2A1B3C
    medication              : Carprofen 25mg
    quantity                : 30
  🔒  owner_name              : VETSOURCE_PII_TOKEN(AQIDBAUGBwg...)  [reversible token]
  🔒  owner_email             : VETSOURCE_PII_TOKEN(...)              [reversible token]
  🔐  owner_phone             : CryptoHash(...)                       [irreversible hash]
  🔒  owner_payment_card      : VETSOURCE_PCI_TOKEN(4111...)          [reversible token]
```

The consumer stops automatically after 30 seconds of no new messages.

#### 7-3. Run the detokenized consumer

Reads from `prescription-events` **with** DLP access. Calls `DLP.reidentifyContent`
to restore plaintext for all reversible fields. Irreversible fields (`owner_phone`,
hashed with `CryptoHashConfig`) remain as hashes — the original value is unrecoverable
by design.

```bash
python3 examples/detokenized_consumer.py
```

Expected output:
```
Subscribed to 'prescription-events' as group 'streamshield-detokenized-consumer'.
De-tokenizing sensitive fields via Cloud DLP (requires KMS cryptoKeyDecrypter).
Press Ctrl+C to stop.

Message  partition=0 offset=0
  Field                     Value                                     Note
  ────────────────────────  ────────────────────────────────────────  ──────────────────────────
  order_id                  RX-4F2A1B3C
  medication                Carprofen 25mg
  quantity                  30
  owner_name                Sarah Mitchell                            [de-tokenized — PII]
  owner_email               sarah@example.com                        [de-tokenized — PII]
  owner_phone               CryptoHash(...)                          [hash — irreversible]
  owner_payment_card        4111111111111111                          [de-tokenized — PCI-DSS]
```

---

## Verification Checklist

Run through this after `terraform apply` completes to confirm every resource was
created before proceeding to Phase 3 (manual topics + connector).

### Networking
- [ ] `poc-vpc` VPC with custom subnet `poc-subnet` (`10.0.0.0/24`)
- [ ] 4 firewall rules: `allow-ssh-from-laptop`, `allow-iap-ssh`, `allow-internal`, `allow-egress-https`

### Kafka
- [ ] Cluster `poc-kafka-cluster` — State: **Active**
- [ ] Topic `prescription-events` — 3 partitions, RF 3 *(created in Phase 3)*
- [ ] Topic `raw-events` — 3 partitions, RF 3 *(created in Phase 3)*
- [ ] Schema Registry `poc_schema_registry` — State: **Active**
- [ ] Connect cluster `poc-connect-cluster` — State: **Active**

### KMS
- [ ] Keyring `dlp-kms-ring` in `global` location
- [ ] Key `pii-dek-kek` — Purpose: ENCRYPT_DECRYPT, State: Enabled
- [ ] Key `pci-dek-kek` — Purpose: ENCRYPT_DECRYPT, State: Enabled

### IAM
- [ ] `vm-producer-sa` has: `managedkafka.client`, `managedkafka.schemaRegistryEditor`, `secretmanager.secretAccessor`, `dlp.user`, `cloudkms.cryptoKeyEncrypterDecrypter` on both keys
- [ ] `dataflow-pipeline-sa` has: `dataflow.worker`, `managedkafka.client`, `managedkafka.schemaRegistryEditor`, `secretmanager.secretAccessor`, `storage.objectAdmin`

### Compute
- [ ] VM `poc-dev-vm` — Status: **Running**, no external IP

### Storage & Messaging
- [ ] GCS bucket `kafka-poc-gcs-landing-terraform-testing-498903`
- [ ] Pub/Sub topic `kafka-poc-gcs-notify` + subscription `kafka-poc-gcs-notify-sub`

### Secrets (12 total — check after Phase 4 and 5)
- [ ] `kafka-bootstrap-servers` — filled (Phase 4)
- [ ] `schema-registry-url` — filled (Phase 4)
- [ ] `dlp-kms-pii-key-name` — filled (auto by Terraform)
- [ ] `dlp-kms-pci-key-name` — filled (auto by Terraform)
- [ ] `dlp-pii-wrapped-dek` — filled (Phase 5)
- [ ] `dlp-pci-wrapped-dek` — filled (Phase 5)
- [ ] `snowflake-*` (6 secrets) — empty shells; fill only when setting up Snowflake

Quick check command:
```bash
for s in kafka-bootstrap-servers schema-registry-url dlp-kms-pii-key-name dlp-kms-pci-key-name dlp-pii-wrapped-dek dlp-pci-wrapped-dek; do
  state=$(gcloud secrets versions list $s --project=terraform-testing-498903 --format="value(state)" 2>/dev/null | head -1)
  echo "$s: ${state:-MISSING}"
done
```

All six should print `enabled`.

---

## Tearing Down

When testing is complete, run from **`poc-dev-vm-2`** in the `terraform` directory:

```bash
terraform destroy
```

Type `yes` when prompted.

> **Cost warning:** The Managed Kafka cluster costs ~$1.50–2.00/hr. Always destroy
> when done. The GCS bucket is removed automatically (`force_destroy = true`).
> KMS keys cannot be immediately deleted — GCP schedules them for destruction
> after 24 hours minimum, but they have no ongoing cost.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Error 403: Required 'compute.networks.create' permission` | Not Project Owner on `terraform-testing-498903` | Verify with the permissions check command at the top of this guide |
| `Error authenticating: could not find default credentials` | ADC not set on poc-dev-vm-2 | Re-run `gcloud auth application-default login` |
| Schema Registry still shows `CREATING` after apply | `gcloud alpha` command may have timed out | Run manually: `gcloud alpha managed-kafka schema-registries create poc_schema_registry --location=${region} --project=${project_id}` |
| `terraform init` fails — cannot download providers | No outbound HTTPS from poc-dev-vm-2 | Private Google Access is enabled on poc-subnet, so this should not happen. Check: `gcloud compute networks subnets describe poc-subnet --region=us-central1 --project=vetsource-496203 --format="value(privateIpGoogleAccess)"` |
| Kafka cluster stuck in `CREATING` for >20 min | Transient GCP issue | Run `terraform apply` again — it resumes from where it left off |
| `gcloud compute scp` fails in Phase 5-1 | IAP not enabled or quota | Confirm IAP API is enabled: `gcloud services list --enabled --project=terraform-testing-498903 \| grep iap` |
| `generate_wrapped_dek.py` — `PERMISSION_DENIED` on KMS | `vm-producer-sa` missing KMS role | Check: `gcloud kms keys get-iam-policy pii-dek-kek --keyring=dlp-kms-ring --location=global --project=terraform-testing-498903` — should show `cryptoKeyEncrypterDecrypter` for `vm-producer-sa` |
| `register_schema.py` — `SchemaNotFoundError` or connection error | Wrong Schema Registry URL | Check `schema-registry-url` secret value matches the `schema_registry_url` Terraform output exactly |
| Producer — `TopicNotFoundError: prescription-events` | Topic not created | Verify: `gcloud managed-kafka topics list poc-kafka-cluster --project=terraform-testing-498903 --location=us-central1` |
| Producer — `dlp.user` permission denied | IAM binding not applied | Verify: `gcloud projects get-iam-policy terraform-testing-498903 --flatten=bindings --filter=bindings.role:roles/dlp.user` |
| Consumer — tokens not detokenizing | Wrong project_id still set | Re-run the `sed` command in step 5-5 and verify with `grep vetsource examples/*.py` |
