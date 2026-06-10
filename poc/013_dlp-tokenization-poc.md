# Step 13 — GCP Cloud DLP Tokenization POC

This guide walks through the end-to-end setup of a tokenization pipeline for prescription events using Google Cloud DLP. Sensitive fields (PII and PCI-DSS) are replaced with tokens before being published to Kafka. The schema itself carries all the key references a consumer needs — no external config lookup is required.

**Prerequisite:** Steps 01–09 complete (project, VPC, IAM, Kafka cluster, Schema Registry, GCE VM, Kafka topics, Python environment). The GCE VM (`poc-dev-vm`) is the only environment where these scripts run.

---

## What This POC Covers

| Script | Purpose |
|--------|---------|
| `generate_wrapped_dek.py` | One-time: generate DEKs, wrap with KMS, store in Secret Manager |
| `register_schema.py` | One-time: embed key material into Avro schema, register with Schema Registry |
| `producer.py` | Tokenizes 5 prescription events via DLP, publishes Avro to Kafka |
| `consumer_tokenized.py` | Consumes and prints tokens as-is (unauthorized subscriber view) |
| `consumer_detokenized.py` | Consumes, calls DLP reidentify, prints original values |

---

## Architecture Summary

```
PrescriptionOrder (plaintext)
        │
        ▼
  Cloud DLP  ──── deidentifyContent
   (producer)         per-field method:
                      owner_name/email/pet_name → AES-SIV (CryptoDeterministicConfig)
                      owner_phone               → SHA-256 hash (CryptoHashConfig, irreversible)
                      owner_payment_card        → FPE/NUMERIC (CryptoReplaceFfxFpeConfig)
        │
        ▼
  Tokenized record  ──Avro wire format──▶  Kafka: prescription-events
                                                    │
                      ┌─────────────────────────────┴──────────────────────────────┐
                      ▼                                                             ▼
           consumer_tokenized.py                                    consumer_detokenized.py
           (no DLP/KMS access)                               (has roles/cloudkms.cryptoKeyDecrypter)
           prints opaque tokens                              calls DLP reidentifyContent
                                                             prints original plaintext values
```

**Key design principle:** The Avro schema registered in Schema Registry contains:
- `token.kms-key` — KMS key resource name (PII domain)
- `token.wrapped-dek` — base64-encoded KMS-wrapped AES-256 key (PII domain)
- `token.pci-kms-key` and `token.pci-wrapped-dek` — same for PCI-DSS domain
- Per-field `token.method`, `token.reversible`, `token.sensitivity`

A consumer that fetches the schema has everything it needs to call DLP independently. No config files, no hardcoded constants.

---

## Section 1: Enable Required APIs

In the GCP Console, navigate to **APIs & Services → Enable APIs** and enable:

| API | Purpose |
|-----|---------|
| Cloud DLP API (`dlp.googleapis.com`) | Tokenization and de-tokenization |
| Cloud KMS API (`cloudkms.googleapis.com`) | Key wrapping / DEK management |

```bash
# Or enable via gcloud (run from Cloud Shell or the GCE VM after authenticating)
gcloud services enable dlp.googleapis.com cloudkms.googleapis.com \
  --project=vetsource-496203
```

---

## Section 2: Cloud KMS Setup

KMS provides **envelope encryption**: a short-lived AES-256 data encryption key (DEK) does the actual encryption; KMS wraps (encrypts) that DEK. The plaintext DEK never leaves DLP — KMS unwraps it on demand inside DLP's trust boundary.

### 2.1 Create the Key Ring

1. In the GCP Console, go to **Security → Key Management**.
2. Click **Create Key Ring**.
3. Fill in:
   - **Key ring name:** `vetsource-dlp`
   - **Location:** `global`
4. Click **Create**.

> **Why `global`?** Cloud DLP's `deidentify_content` endpoint is global. The KMS key must be in `global` for DLP to unwrap it in-band. If you restrict DLP to a regional endpoint (e.g., `us-central1`), switch this to `us-central1`.

### 2.2 Create the PII Domain Key

1. Click **+ Create Key** inside the `vetsource-dlp` key ring.
2. Fill in:
   - **Key name:** `pii-dek-kek`
   - **Purpose:** Symmetric encrypt/decrypt
   - **Protection level:** Software
   - **Rotation period:** 1 year (adjust to your compliance policy)
3. Click **Create**.

### 2.3 Create the PCI-DSS Domain Key

Repeat the same steps with:
- **Key name:** `pci-dek-kek`
- All other settings identical to `pii-dek-kek`

> **Why two keys?** Compliance tier isolation. PCI-DSS has a faster rotation requirement than generic PII. Audit queries like "who decrypted card data in the last 30 days" are a single KMS log query on `pci-dek-kek`, not a mixed-topic search.

---

## Section 3: IAM Roles for DLP

### 3.1 Grant KMS access to the VM's service account

The GCE VM runs the producer and the detokenizing consumer. Both need to call DLP, which in turn needs to unwrap the DEKs via KMS.

1. Go to **Security → Key Management → Key Rings → vetsource-dlp**.
2. Click on the `pii-dek-kek` key.
3. On the **Permissions** tab, click **Grant Access**.
4. In **New principals**, enter: `vm-producer-sa@vetsource-496203.iam.gserviceaccount.com`
5. Assign role: **Cloud KMS CryptoKey Encrypter/Decrypter** (`roles/cloudkms.cryptoKeyEncrypterDecrypter`)
6. Click **Save**.

Repeat steps 2–6 for the `pci-dek-kek` key.

> **Why Encrypter/Decrypter and not just Decrypter?** `generate_wrapped_dek.py` calls `kms.encrypt` to create the wrapped DEK. After setup is complete, the VM only needs Decrypter (via DLP) for tokenization and de-tokenization. If you want least-privilege after initial setup, change the role on `pii-dek-kek` and `pci-dek-kek` to **Cloud KMS CryptoKey Decrypter** (`roles/cloudkms.cryptoKeyDecrypter`) once `generate_wrapped_dek.py` has been run.

### 3.2 Grant DLP roles to the VM's service account

1. Go to **IAM & Admin → IAM**.
2. Find `vm-producer-sa` and click the pencil (Edit) icon.
3. Click **+ Add Another Role** and add:

| Role | IAM identifier | Purpose |
|------|---------------|---------|
| DLP User | `roles/dlp.user` | Grants access to call `deidentifyContent` and `reidentifyContent` |

4. Click **Save**.

> **What `roles/dlp.user` covers:** This is the correct role for invoking DLP transformation APIs (`deidentifyContent`, `reidentifyContent`, `inspectContent`). It does not grant access to administer DLP job triggers or stored templates — that requires `roles/dlp.admin`. There are no separate `deidentify` / `reidentify` split roles in Cloud DLP; access to both directions is gated by the single `dlp.user` role combined with the KMS IAM check (a caller without KMS Decrypter receives `PermissionDenied` from DLP even if they have `dlp.user`).

> **Least-privilege note:** In production, split the producer and consumer into separate service accounts. Both get `roles/dlp.user`, but the producer SA gets only KMS Encrypter on the domain keys (cannot decrypt), while the consumer SA gets only KMS Decrypter (cannot re-encrypt). This ensures the producer can never reverse its own tokens.

---

## Section 4: Store Secrets in Secret Manager

Create the following secrets in **Security → Secret Manager**. For each:
- Click **+ Create Secret**
- Set the **Name** exactly as shown
- Paste the value
- Click **Create Secret**

After the secrets are created, `generate_wrapped_dek.py` will add the wrapped DEK values automatically. You only need to create the name/value secrets for the KMS key names now.

| Secret name | Value to set now | Notes |
|-------------|-----------------|-------|
| `dlp-kms-pii-key-name` | `projects/vetsource-496203/locations/global/keyRings/vetsource-dlp/cryptoKeys/pii-dek-kek` | Full KMS resource name |
| `dlp-kms-pci-key-name` | `projects/vetsource-496203/locations/global/keyRings/vetsource-dlp/cryptoKeys/pci-dek-kek` | Full KMS resource name |
| `dlp-pii-wrapped-dek` | _(create secret with empty placeholder — `generate_wrapped_dek.py` will add the real value)_ | Base64 wrapped AES-256 key |
| `dlp-pci-wrapped-dek` | _(create secret with empty placeholder — `generate_wrapped_dek.py` will add the real value)_ | Base64 wrapped AES-256 key |

> **Creating with a placeholder:** When creating `dlp-pii-wrapped-dek` and `dlp-pci-wrapped-dek`, you can set the initial secret value to `placeholder`. `generate_wrapped_dek.py` adds a new version with the real value — Secret Manager always serves the latest version.

---

## Section 5: Create the Kafka Topic

The DLP POC uses a dedicated topic to keep prescription events separate from the existing `raw-events` topic.

In the GCP Console, go to **Managed Kafka → Clusters → poc-kafka-cluster → Topics**:
- Click **Create Topic**
- **Name:** `prescription-events`
- **Partitions:** 3
- **Retention:** 7 days
- Click **Create**

Or from the GCE VM:
```bash
# If kafka-topics.sh is available on the VM
kafka-topics.sh --bootstrap-server $BOOTSTRAP_SERVERS \
  --command-config /tmp/client.properties \
  --create --topic prescription-events --partitions 3 --replication-factor 3
```

---

## Section 6: Python Environment & Dependencies

SSH into `poc-dev-vm` via VSCode Remote SSH. All commands below run on the VM.

### 6.1 Project directory

```bash
mkdir -p ~/kafka-poc-dlp
cd ~/kafka-poc-dlp
python3 -m venv venv
source venv/bin/activate
```

### 6.2 Install dependencies

```bash
pip install \
  confluent-kafka \
  fastavro \
  google-cloud-dlp \
  google-cloud-kms \
  google-cloud-secret-manager \
  google-auth \
  requests
```

### 6.3 Copy scripts to the VM

Copy all files from `Scripts-dlp/` to `~/kafka-poc-dlp/` on the VM:

```bash
# From your local machine (adjust the path to match your local repo checkout)
scp /path/to/Scripts-dlp/*.py poc-dev-vm:~/kafka-poc-dlp/
```

Or use VSCode Remote SSH to drag files into the `~/kafka-poc-dlp/` directory.

### 6.4 Verify imports

```bash
cd ~/kafka-poc-dlp && source venv/bin/activate
python3 -c "from google.cloud import dlp_v2, kms, secretmanager; print('All imports OK')"
python3 -c "import confluent_kafka, fastavro; print('Kafka + Avro OK')"
```

---

## Section 7: Generate Wrapped DEKs (one-time setup)

```bash
cd ~/kafka-poc-dlp && source venv/bin/activate
python3 generate_wrapped_dek.py
```

**Expected output:**
```
Loading KMS key names from Secret Manager...
  PII key: projects/vetsource-496203/locations/global/keyRings/vetsource-dlp/cryptoKeys/pii-dek-kek
  PCI key: projects/vetsource-496203/locations/global/keyRings/vetsource-dlp/cryptoKeys/pci-dek-kek

Generating PII domain wrapped DEK...
  PII wrapped DEK (base64, first 40 chars): CiQAjpTsmQzGl7v9kNpRx...
  Stored new version of secret: dlp-pii-wrapped-dek

Generating PCI-DSS domain wrapped DEK...
  PCI wrapped DEK (base64, first 40 chars): CiQAjpTsmZp8nLwKqMoSy...
  Stored new version of secret: dlp-pci-wrapped-dek

Done. Wrapped DEKs stored in Secret Manager.
```

> **Run this only once.** The wrapped DEK is embedded permanently in the Avro schema at registration time. Rotating it requires re-registration of the schema and re-tokenization of all existing messages.

---

## Section 8: Register the Schema

```bash
python3 register_schema.py
```

**Expected output:**
```
Loading credentials and key material from Secret Manager...
  Loaded schema-registry-url
  Loaded PII KMS key:  projects/vetsource-496203/.../pii-dek-kek
  Loaded PCI KMS key:  projects/vetsource-496203/.../pci-dek-kek
  Loaded wrapped DEKs (PII + PCI)

Registering schema under subject 'prescription-events-value'...
Schema registered — ID: 2
Subject:              prescription-events-value
Registry:             https://...

Verifying round-trip fetch from registry...
  Fetched schema fields: ['order_id', 'medication', 'quantity', 'order_date', 'is_refill', 'owner_name', ...]
  Tokenized fields:      ['owner_name', 'owner_email', 'pet_name', 'owner_phone', 'owner_payment_card']
  token.kms-key present: True
  token.wrapped-dek present: True

Schema registration complete.
```

Verify in the GCP Console: **Managed Kafka → Schema Registry → Subjects** — you should see `prescription-events-value` with one version.

---

## Section 9: Run the Producer

```bash
python3 producer.py
```

**Expected output (one block per message):**
```
Loading credentials from Secret Manager...
Credentials loaded.
Schema fetched from registry — subject: prescription-events-value, ID: 2

Producing 5 messages to topic 'prescription-events'...

────────────────────────────────────────────────────────────
Message 1: order_id=RX-3F7A1C2E
  Plaintext  owner_name  : Sarah Mitchell
  Plaintext  owner_email : sarah@example.com
  Plaintext  owner_phone : +1-555-0142
  Plaintext  card        : 4111111111111111
  Plaintext  pet_name    : Biscuit
  Tokenized  owner_name  : VETSOURCE_PII_TOKEN(14):aB3xKpQ7mZnRwY...
  Tokenized  owner_phone : VETSOURCE_PII_TOKEN(12):kJ9sUeYfOiNgWm...
  Tokenized  card        : 5412753489210033  ← format-preserved
  Delivered: order_id=RX-3F7A1C2E  → prescription-events [partition=1, offset=0]
...
Flushing producer — waiting for all acks...
All messages delivered.
```

Key things to observe:
- **owner_name / owner_email / pet_name:** `VETSOURCE_PII_TOKEN(...)` prefix — reversible AES-SIV token
- **owner_phone:** `VETSOURCE_PII_TOKEN(...)` prefix — hash token (same prefix format, irreversible)
- **owner_payment_card:** A valid-looking 16-digit number — FPE, format preserved

---

## Section 10: Run the Tokenized Consumer

Open a second terminal on the VM. This consumer simulates a subscriber **without** KMS access.

```bash
cd ~/kafka-poc-dlp && source venv/bin/activate
python3 consumer_tokenized.py
```

**Expected output:**
```
Subscribed to 'prescription-events' as group 'dlp-tokenized-consumer-group'.
Printing TOKENIZED data as-is (no de-tokenization).

Message #1
  Partition / Offset : 1 / 0

       order_id              : RX-3F7A1C2E
       medication            : Carprofen 25mg
       quantity              : 30
       order_date            : 2026-05-27
       is_refill             : False
  🔒   owner_name            : VETSOURCE_PII_TOKEN(14):aB3xKp...  [reversible token]
  🔒   owner_email           : VETSOURCE_PII_TOKEN(20):cF7hMn...  [reversible token]
  🔐   owner_phone           : VETSOURCE_PII_TOKEN(12):kJ9sUe...  [irreversible hash]
  🔒   owner_payment_card    : 5412753489210033  [FPE token — format preserved]
  🔒   pet_name              : VETSOURCE_PII_TOKEN(7):dG2jLr...   [reversible token]
```

The 🔒 icon indicates a reversible token. 🔐 indicates an irreversible hash. The original values are completely inaccessible without KMS authorization.

---

## Section 11: Run the De-tokenizing Consumer

This consumer calls DLP `reidentifyContent`. It requires `roles/cloudkms.cryptoKeyDecrypter` on both domain keys — which `vm-producer-sa` already has from Section 3.

```bash
python3 consumer_detokenized.py
```

**Expected output:**
```
Subscribed to 'prescription-events' as group 'dlp-detokenized-consumer-group'.
De-tokenizing sensitive fields via Cloud DLP.

Message #1
  Partition / Offset : 1 / 0

  Field                   Value                                     Note
  ──────────────────────  ────────────────────────────────────────  ────────────────────
  order_id                RX-3F7A1C2E
  medication              Carprofen 25mg
  quantity                30
  order_date              2026-05-27
  is_refill               False
  owner_name              Sarah Mitchell                            [de-tokenized — PII]
  owner_email             sarah@example.com                         [de-tokenized — PII]
  pet_name                Biscuit                                   [de-tokenized — PII]
  owner_phone             VETSOURCE_PII_TOKEN(12):kJ9sUe...         [hash — irreversible, original unrecoverable]
  owner_payment_card      4111111111111111                          [de-tokenized — PCI-DSS]
```

Notice:
- `owner_name`, `owner_email`, `pet_name`, `owner_payment_card` — fully restored to original plaintext
- `owner_phone` — remains as token (hash is permanent; original cannot be recovered even with the key)

---

## Section 12: Validation Checklist

- [ ] `generate_wrapped_dek.py` ran without errors; two new secret versions appear in Secret Manager
- [ ] `register_schema.py` completed; `prescription-events-value` appears in Schema Registry with `token.kms-key` and `token.wrapped-dek` embedded
- [ ] `producer.py` produced 5 messages; `owner_payment_card` token is 16 digits (FPE format preserved)
- [ ] `consumer_tokenized.py` received 5 messages; all sensitive fields show opaque tokens
- [ ] `consumer_detokenized.py` received 5 messages; `owner_name`, `owner_email`, `pet_name`, `owner_payment_card` match the original plaintext printed by the producer; `owner_phone` remains as token

---

## Troubleshooting

**`PermissionDenied` from DLP (producer or detokenizing consumer)**
- The service account lacks `roles/cloudkms.cryptoKeyEncrypterDecrypter` (for `generate_wrapped_dek.py`) or `roles/cloudkms.cryptoKeyDecrypter` (for DLP during tokenization/de-tokenization).
- Verify in **IAM & Admin → IAM**: search for `vm-producer-sa`, confirm the KMS roles on both keys.
- Also confirm `roles/dlp.user` is present on `vm-producer-sa`.

**`google.api_core.exceptions.InvalidArgument` from DLP**
- The wrapped DEK bytes or KMS key name is incorrect. Verify the base64 in `dlp-pii-wrapped-dek` / `dlp-pci-wrapped-dek` was generated by the same KMS key referenced in `dlp-kms-pii-key-name` / `dlp-kms-pci-key-name`.
- Regenerate with `generate_wrapped_dek.py` and re-register the schema if needed.

**Schema Registry 401 Unauthorized**
- Bearer token has expired. The scripts refresh tokens at startup, but if the script runs for many hours, call `get_gcp_bearer_token()` again and retry.
- Confirm `vm-producer-sa` has the `roles/managedkafka.schemaRegistryEditor` role on the Kafka cluster.

**De-tokenized card number does not match original**
- FPE with `common_alphabet=NUMERIC` operates on the digit string without spaces. Ensure the producer strips spaces from card numbers before tokenizing, and the consumer interprets the de-tokenized value the same way.

**`owner_phone` not restored after de-tokenization**
- This is correct and expected. `CryptoHashConfig` is a one-way operation. The original phone number cannot be recovered under any circumstances. This is intentional — see `token.reversible = "false"` in the schema.

**`fastavro.write.UnknownType` error**
- Ensure you are calling `fastavro.parse_schema(raw_schema)` on the raw schema dict before writing/reading Avro. The `logicalType` and `token.*` fields on field definitions are ignored by fastavro during serialization.

---

## File Reference

All scripts live in `Architecture/setup-guides/Scripts-dlp/` in this repository.

```
Scripts-dlp/
├── utils.py                  # Secret Manager, GCP auth, Kafka config helpers
├── schema.py                 # PrescriptionOrder schema builder with token metadata
├── dlp_utils.py              # tokenize_record() and detokenize_record() — schema-driven
├── generate_wrapped_dek.py   # One-time: create + KMS-wrap DEKs → Secret Manager
├── register_schema.py        # One-time: embed key material into schema → Schema Registry
├── producer.py               # Tokenizing Avro producer
├── consumer_tokenized.py     # Read-only consumer (no DLP access required)
└── consumer_detokenized.py   # Authorized consumer — reverses tokens via DLP
```

---

## What's Next

- **Phase 2 — Key rotation:** Rotate `pii-dek-kek` and `pci-dek-kek` in KMS, generate new wrapped DEKs, register a new schema version, and verify consumers pick up the new schema revision automatically.
- **Phase 3 — Separate service accounts:** Split the producer SA (`roles/dlp.user` + KMS Encrypter only) from the consumer SA (`roles/dlp.user` + KMS Decrypter only). Both have `dlp.user`, but the KMS key permissions enforce the asymmetry — the producer SA cannot decrypt its own tokens.
- **Phase 4 — DLP inspection template:** Replace inline `custom_info_types` in `reidentify_content` with a stored Cloud DLP inspection template. Centralises the surrogate type config and simplifies the consumer code.
