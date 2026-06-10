# Step 03 — IAM Service Accounts & Permissions

In this step you create two service accounts — one for the GCE VM running the Kafka producer, and one for the Dataflow pipeline. Getting IAM right now means your services can authenticate securely to other GCP resources without any manual credential management.

---

## What Is a Service Account?

A service account is like a robot user for a GCP service. It is:

- **Not a human login** — it has no password and is not tied to anyone's personal Google account
- **Attached to a resource** (like a VM or a Dataflow job) — the resource automatically authenticates as that service account
- **Granted only the roles it needs** — following the principle of least privilege

Think of it this way: when your GCE VM needs to write a Kafka message or read a secret, it doesn't use your personal credentials. It uses the service account you attached to it — like a badge that says "this machine is allowed to do X and Y."

---

## Section 1: Create Service Account for the GCE VM (Producer)

This service account will be attached to your GCE VM. It gives the VM permission to produce messages to Kafka and to read credentials from Secret Manager.

1. In the left navigation, click **IAM & Admin**, then click **Service Accounts**.

2. Click **+ Create Service Account** at the top of the page.

3. Fill in Step 1 — Service account details:
   - **Service account name:** `vm-producer-sa`
   - **Service account ID:** This auto-fills as `vm-producer-sa` — leave it as-is
   - **Service account description:** `Service account for the GCE VM running the Kafka producer`

4. Click **Create and Continue**.

5. You are now on Step 2 — Grant this service account access to the project. Add the following two roles, one at a time:

   **Role 1:**
   - Click the **Role** dropdown
   - Search for `Managed Kafka Client`
   - Select **Managed Kafka Client** from the results

   **Role 2:**
   - Click **+ Add Another Role**
   - Search for `Secret Manager Secret Accessor`
   - Select **Secret Manager Secret Accessor** from the results

   > **What these roles do:**
   > - `Managed Kafka Client` — allows the VM to connect to and produce/consume from the Kafka cluster
   > - `Secret Manager Secret Accessor` — allows the VM to read secrets (e.g., Confluent Schema Registry credentials) stored in Secret Manager

6. Click **Continue**.

7. Step 3 is optional (granting other users access to this service account). Leave it blank.

8. Click **Done**.

You will be returned to the Service Accounts list and should see `vm-producer-sa` listed.

---

## Section 2: Create Service Account for the Dataflow Pipeline

This service account will be used by your Dataflow job to read from Kafka and write to Snowflake. It needs a broader set of permissions than the VM service account.

1. On the **Service Accounts** page, click **+ Create Service Account** again.

2. Fill in Step 1 — Service account details:
   - **Service account name:** `dataflow-pipeline-sa`
   - **Service account ID:** This auto-fills as `dataflow-pipeline-sa` — leave it as-is
   - **Service account description:** `Service account for the Dataflow Kafka-to-Snowflake pipeline`

3. Click **Create and Continue**.

4. On Step 2, add the following four roles, one at a time using **+ Add Another Role** for each additional role:

   **Role 1:**
   - Search for `Dataflow Worker`
   - Select **Dataflow Worker**

   > `Dataflow Worker` — allows Dataflow to spawn and manage worker VMs that execute the pipeline

   **Role 2:**
   - Click **+ Add Another Role**
   - Search for `Managed Kafka Client`
   - Select **Managed Kafka Client**

   > `Managed Kafka Client` — allows the Dataflow workers to consume messages from the Kafka topic

   **Role 3:**
   - Click **+ Add Another Role**
   - Search for `Secret Manager Secret Accessor`
   - Select **Secret Manager Secret Accessor**

   > `Secret Manager Secret Accessor` — allows Dataflow workers to read Snowflake connection credentials and other secrets at runtime

   **Role 4:**
   - Click **+ Add Another Role**
   - Search for `Storage Object Admin`
   - Select **Storage Object Admin**

   > `Storage Object Admin` — Dataflow requires a Cloud Storage bucket for temporary files and staging during job execution. This role gives the service account read/write access to those GCS buckets.

5. Click **Continue**.

6. Leave Step 3 blank.

7. Click **Done**.

You should now see both `vm-producer-sa` and `dataflow-pipeline-sa` in the Service Accounts list.

---

## Section 3: Verify the Service Accounts

1. Go to **IAM & Admin** > **Service Accounts**.

2. Confirm both service accounts appear in the list:
   - [ ] `vm-producer-sa@kafka-poc-XXXXXX.iam.gserviceaccount.com`
   - [ ] `dataflow-pipeline-sa@kafka-poc-XXXXXX.iam.gserviceaccount.com`

   > The long email-style identifier is the service account's unique ID. You will use this when attaching the service account to your VM and Dataflow job in later steps.

3. Click on `vm-producer-sa` to open its details.
   - Click the **Permissions** tab
   - Verify it shows the roles: **Managed Kafka Client** and **Secret Manager Secret Accessor**

4. Click the back arrow, then click on `dataflow-pipeline-sa`.
   - Click the **Permissions** tab
   - Verify it shows: **Dataflow Worker**, **Managed Kafka Client**, **Secret Manager Secret Accessor**, and **Storage Object Admin**

> **Tip:** If a role is missing, click **Edit** on the service account, go back to Step 2, and add the missing role using **+ Add Another Role**.

---

## Section 4: Important — Do NOT Download JSON Key Files

GCP gives you the option to create and download a JSON key file for a service account. **Do not do this.**

Here is why:

- A JSON key file is a long-lived credential that never expires automatically
- If it leaks (accidentally committed to GitHub, left in a public S3 bucket, etc.), anyone can use it to access your GCP project
- It bypasses the audit trail — GCP cannot tell you which person or system used the key

**The correct approach for this POC:**

- **GCE VM:** The service account will be attached directly to the VM at creation time (in Step 04). The VM automatically receives a short-lived token from the GCP metadata server — no key file needed.
- **Dataflow job:** The service account will be specified in the Dataflow job configuration when you launch the job. Dataflow handles authentication internally — no key file needed.

> **Rule of thumb:** If you ever find yourself on the **Keys** tab of a service account thinking about clicking **Add Key** — stop and ask whether there is a better way. For GCE VMs and Dataflow, there always is.

---

## What's Next

Service accounts are configured. Move on to **Step 04 — GCE VM Setup** to create the virtual machine, attach the `vm-producer-sa` service account to it, and configure SSH access via VSCode Remote SSH.
