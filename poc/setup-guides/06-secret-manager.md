# Step 06 — Google Secret Manager: Store All Credentials

## Overview

**Google Secret Manager** is GCP's secure vault for storing sensitive values — passwords, API keys, connection strings, and other credentials. Think of it as a locked safe that only specific, authorized identities can open.

**Why not just put credentials in code or config files?**
- Code is often shared, committed to Git, or visible to multiple people
- Config files can accidentally be exposed or checked in
- Secret Manager provides audit logs of every access, automatic versioning, and fine-grained IAM-based access control

**How this architecture uses it:**
- Your GCE VM (running as `vm-producer-sa`) reads the Kafka and Schema Registry credentials from Secret Manager at startup
- Your Dataflow pipeline (running as `dataflow-pipeline-sa`) reads credentials from Secret Manager at job launch time
- No credentials ever appear in source code, environment variables, or command-line arguments

---

## 1. Navigate to Secret Manager

1. In the GCP Console, click the **search bar** at the top.
2. Type `Secret Manager` and select **Secret Manager** from the results.
3. Alternatively, open the **Navigation Menu** (☰) > **Security** > **Secret Manager**.
4. If you see a prompt to enable the API, click **Enable** and wait 1–2 minutes before continuing.

---

## 2. Create Secrets One by One

You will create 10 secrets total. The process is identical for each one. Follow these steps for every secret in the table below.

### Steps to create each secret:

1. Click **Create Secret** (blue button at the top of the Secret Manager page).
2. In the **Name** field, enter the exact secret name from the table (copy-paste to avoid typos — secret names are case-sensitive).
3. In the **Secret value** field, paste the corresponding value.
4. Under **Replication policy**, select **Automatic** (this is usually the default).
5. Leave all other settings (rotation, expiration, labels, annotations) at their defaults.
6. Click **Create Secret**.
7. You will be taken to the secret's detail page. Confirm it shows **1 version** with status **Enabled**.
8. Click the **back arrow** or navigate back to the Secret Manager main list.
9. Repeat for the next secret.

---

### Secrets to create:

| Secret Name | Value to paste | Where it comes from |
|---|---|---|
| `kafka-bootstrap-servers` | The full bootstrap address from Step 04 (e.g., `bootstrap.poc-kafka-cluster.us-central1.managedkafka.kafka-poc.cloud.goog:9092`) | GCP Managed Kafka — Step 04 |
| `schema-registry-url` | The HTTPS endpoint URL from Step 05 (e.g., `https://psrc-xxxxx.us-central1.gcp.confluent.cloud`) | Confluent Cloud — Step 05 |
| `schema-registry-api-key` | The API Key from Step 05 (e.g., `ABCDEF123456`) | Confluent Cloud — Step 05 |
| `schema-registry-api-secret` | The API Secret from Step 05 (the long random string) | Confluent Cloud — Step 05 |
| `snowflake-account` | Your Snowflake account identifier | Step 10 — fill in later |
| `snowflake-user` | Your Snowflake username | Step 10 — fill in later |
| `snowflake-password` | Your Snowflake password | Step 10 — fill in later |
| `snowflake-database` | `POC_DB` | Step 10 — fill in later |
| `snowflake-schema` | `KAFKA_INGEST` | Step 10 — fill in later |
| `snowflake-warehouse` | `POC_WH` | Step 10 — fill in later |

> **Note on Snowflake secrets:** Create all 10 secrets now even if you do not have the Snowflake values yet. For `snowflake-account`, `snowflake-user`, and `snowflake-password`, you can enter a placeholder value like `PLACEHOLDER` for now. After completing Step 10, update each one using the process below.

### How to update a secret value later:

1. In Secret Manager, click the secret name you want to update.
2. Click **New Version** (top of the page).
3. Paste the real value into the **Secret value** field.
4. Click **Add New Version**.
5. The new version becomes active automatically. The old placeholder version is retained but inactive.

---

## 3. Grant Access to Service Accounts

By default, a service account has no access to any secrets — even secrets in the same project. You must explicitly grant the `Secret Manager Secret Accessor` role to each service account that needs to read secrets.

You need to grant access to two service accounts:
- `vm-producer-sa` — used by the GCE VM running the Kafka producer
- `dataflow-pipeline-sa` — used by the Dataflow pipeline

> **Already done?** If you already assigned the `Secret Manager Secret Accessor` role to these service accounts in Step 03 (IAM setup), you can skip this section — the access is already in place.

### Steps to grant access via IAM:

1. In the GCP Console, open the **Navigation Menu** (☰) > **IAM & Admin** > **IAM**.
2. You will see a list of principals (users, groups, service accounts) that have roles in the project.
3. Find **`vm-producer-sa`** in the list. It will appear as something like `vm-producer-sa@kafka-poc.iam.gserviceaccount.com`.

   > **Tip:** Use your browser's find-in-page (Ctrl+F / Cmd+F) and search for `vm-producer-sa` to locate it quickly in a long list.

4. Click the **pencil icon** (Edit principal) on the right side of that row.
5. In the edit panel that opens on the right, click **Add Another Role**.
6. In the role search box, type `Secret Manager Secret Accessor`.
7. Select **Secret Manager Secret Accessor** from the dropdown.
8. Click **Save**.
9. Repeat steps 3–8 for **`dataflow-pipeline-sa`**.

### Alternative: Grant access per secret (more fine-grained)

If you prefer to grant access only to specific secrets rather than all secrets in the project:

1. In Secret Manager, click the **checkbox** next to a secret name.
2. In the right panel (or top info bar), click **Show Info Panel** and then **Add Principal**.
3. Under **New principals**, type the service account email address.
4. Under **Role**, search for and select **Secret Manager Secret Accessor**.
5. Click **Save**.
6. Repeat for each secret and each service account.

> **Recommendation for this POC:** Use the IAM-level grant (project-wide) to keep things simple. Fine-grained per-secret access is better for production environments.

---

## 4. Verify Secrets

Before finishing this step, confirm everything is set up correctly.

1. Go to the **Secret Manager** main page (Navigation Menu > Security > Secret Manager).
2. Confirm you see all **10 secrets** listed:
   - `kafka-bootstrap-servers`
   - `schema-registry-url`
   - `schema-registry-api-key`
   - `schema-registry-api-secret`
   - `snowflake-account`
   - `snowflake-user`
   - `snowflake-password`
   - `snowflake-database`
   - `snowflake-schema`
   - `snowflake-warehouse`
3. Each secret should show:
   - **Status:** Enabled
   - **Versions:** 1 (or more if you updated a placeholder)
4. Secrets that still have placeholder values are fine at this stage — they are created and accessible, and you will update them after Step 10.

### Verify service account access (optional but recommended):

1. Click on any secret (e.g., `kafka-bootstrap-servers`).
2. Click the **Permissions** tab.
3. Confirm that `vm-producer-sa` and `dataflow-pipeline-sa` appear with the **Secret Manager Secret Accessor** role — either directly on the secret or inherited from the project IAM policy.

---

## What's Next

You now have a centralized, secure credential store. All 10 secrets are created, and both service accounts have read access.

At runtime:
- The GCE VM producer will call Secret Manager to retrieve `kafka-bootstrap-servers`, `schema-registry-url`, `schema-registry-api-key`, and `schema-registry-api-secret`
- The Dataflow pipeline will call Secret Manager to retrieve those same values plus all `snowflake-*` secrets

Next: **Step 07 — GCE VM Setup (Kafka Producer)**

In Step 07 you will create the GCE virtual machine that will run as `vm-producer-sa`, connect to GCP Managed Kafka, and produce test messages using the Avro schemas validated by your Schema Registry.
