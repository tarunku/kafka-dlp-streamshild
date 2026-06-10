# Step 11 — Kafka to Snowflake via GCS: Two Approaches

## Why Not a Direct "Kafka to Snowflake" Template?

> **Important context before you begin.**
>
> A direct "Kafka to Snowflake" path via Google Cloud Dataflow or GCP Managed Kafka Connect is not available for either of the following reasons:
>
> - **Dataflow Flex Templates** target GCP-native sinks (BigQuery, GCS, Pub/Sub). Snowflake is a third-party destination — there is no first-party Dataflow template for it, and none is planned.
> - **GCP Managed Kafka Connect** supports only pre-packaged connector plugins. The official Snowflake Kafka Connector requires uploading a custom JAR to the Connect cluster — this is blocked by design in the managed service.
>
> The correct architecture routes data through **Google Cloud Storage (GCS) as a staging area**, then uses **Snowpipe Auto-Ingest** to pull files from GCS into Snowflake automatically. Both approaches below follow this pattern.

---

## Architecture Overview

Two equivalent paths achieve the same result. Choose one based on your setup and preference.

**Approach A — Managed Kafka Connect GCS Sink (Recommended)**

```
GCP Managed Kafka
      │
      │  GCS Sink Connector  (pre-packaged — GCP Managed Kafka Connect)
      ▼
Google Cloud Storage  (kafka-poc-gcs-landing)
      │
      │  GCS Pub/Sub Object Notification  →  Snowpipe Auto-Ingest
      ▼
Snowflake  POC_DB.KAFKA_INGEST.ORDER_EVENTS
```

**Approach B — Google Cloud Dataflow (Kafka to Cloud Storage)**

```
GCP Managed Kafka
      │
      │  "Kafka to Cloud Storage" Dataflow Template  (first-party GCP)
      ▼
Google Cloud Storage  (kafka-poc-gcs-landing)
      │
      │  GCS Pub/Sub Object Notification  →  Snowpipe Auto-Ingest
      ▼
Snowflake  POC_DB.KAFKA_INGEST.ORDER_EVENTS
```

Both approaches write event files into the same GCS landing bucket and rely on identical Snowpipe configuration downstream.

> **Recommendation:** Use **Approach A** for this POC. It requires fewer managed resources — the GCS Sink Connector is pre-packaged, no custom code is needed, and there is no Dataflow job to monitor. Choose Approach B if your team is already invested in Dataflow-based pipeline observability.

---

## Before You Begin

These steps assume Steps 01–10 are complete:
- GCP project, networking, and service accounts are configured.
- GCP Managed Kafka cluster and topics exist; the `raw-events` topic is active.
- Secret Manager contains: `kafka-bootstrap-servers`, `schema-registry-url`, `schema-registry-api-key`, `schema-registry-api-secret`.
- Snowflake `POC_DB.KAFKA_INGEST.ORDER_EVENTS` table exists (created in Step 10).
- `dataflow-pipeline-sa` has `Storage Object Admin` at project level (granted in Step 03) — this covers any GCS bucket you create.

---

## Part 1 — Create the GCS Landing Bucket (Both Approaches)

This bucket is the staging area where Kafka event files land before Snowpipe picks them up.

1. Navigate to **Cloud Storage > Buckets** using the top search bar.
2. Click **"+ Create"**.
3. Fill in:
   - **Name**: `kafka-poc-gcs-landing`

     > Bucket names are globally unique across all GCP projects. If this name is taken, append a short suffix such as `-01`. Note the exact name — you need it in multiple places throughout this guide.

   - **Location type**: `Region`
   - **Region**: `us-central1`
   - **Storage class**: `Standard`
   - **Access control**: `Uniform`
4. Leave all other settings at defaults.
5. Click **"Create"**.
6. Confirm the bucket appears in the list with region `us-central1`.

---

## Part 2A — Configure the GCS Sink Connector (Approach A only)

Skip to **Part 2B** if you chose Approach B.

### 2A-1: Open Your Managed Kafka Connect Cluster

1. Navigate to **Managed Kafka** using the top search bar.
2. In the left nav, click **Connect clusters**.
3. Click on your existing Connect cluster.

   > If you have not yet created a Connect cluster, click **"+ Create Connect Cluster"**, select the same region (`us-central1`), and attach `dataflow-pipeline-sa` as the service account. The cluster will take a few minutes to provision.

### 2A-2: Create the GCS Sink Connector

1. Click **"+ Create Connector"**.
2. In the connector type list, find and select **"Google Cloud Storage Sink"** (also listed as `GcsSinkConnector`).
3. The console shows a JSON configuration editor. Paste the following and replace the three placeholder values with real values from Secret Manager (open Secret Manager in a second tab):

```json
{
  "name": "gcs-sink-order-events",
  "connector.class": "io.confluent.connect.gcs.GcsSinkConnector",
  "tasks.max": "1",
  "topics": "raw-events",
  "gcs.bucket.name": "kafka-poc-gcs-landing",
  "gcs.part.size": "5242880",
  "flush.size": "100",
  "rotate.interval.ms": "60000",
  "format.class": "io.confluent.connect.gcs.format.json.JsonFormat",
  "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
  "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
  "locale": "en_US",
  "timezone": "UTC",
  "timestamp.extractor": "Wallclock",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "PASTE_VALUE_FROM_schema-registry-url",
  "value.converter.basic.auth.credentials.source": "USER_INFO",
  "value.converter.basic.auth.user.info": "PASTE_KEY_FROM_schema-registry-api-key:PASTE_SECRET_FROM_schema-registry-api-secret"
}
```

   | Placeholder | Secret Manager secret to copy |
   |---|---|
   | `PASTE_VALUE_FROM_schema-registry-url` | `schema-registry-url` |
   | `PASTE_KEY_FROM_schema-registry-api-key` | `schema-registry-api-key` |
   | `PASTE_SECRET_FROM_schema-registry-api-secret` | `schema-registry-api-secret` |

   > **What these settings do:**
   > - `value.converter = AvroConverter` — reads Avro-encoded Kafka messages and deserializes them using the Schema Registry before writing
   > - `format.class = JsonFormat` — writes each deserialized record as one JSON object per line into GCS files
   > - `flush.size = 100` — commits a new file to GCS after 100 records accumulate
   > - `rotate.interval.ms = 60000` — also commits a file every 60 seconds even if fewer than 100 records have arrived, preventing stale data at low volume
   > - `path.format` — organizes output files into time-partitioned directories (`year=.../month=.../day=.../hour=...`)

4. Click **"Create"** (or **"Submit"** depending on console version).
5. The connector status transitions from **Provisioning** to **Running** within 2–3 minutes.

### 2A-3: Confirm Files Land in GCS

1. Navigate to **Cloud Storage > Buckets > kafka-poc-gcs-landing**.
2. Confirm the Kafka producer is running (see Step 09). After 60–90 seconds, subdirectories should appear under the path structure:
   `year=YYYY/month=MM/day=dd/hour=HH/raw-events+0+0000000000.json`
3. Click one of the `.json` files and use **"View file"** to confirm it contains JSON records with `order_id`, `customer_id`, `product_id`, `amount`, `currency`, `timestamp`, and `status` fields.

> **If no files appear after 5 minutes:** Open the connector **Logs** tab in the Managed Kafka Connect UI. Common issues: the Connect cluster service account is missing `Storage Object Admin` on the bucket, or the Schema Registry credentials are incorrect.

---

## Part 2B — Launch the Dataflow Job (Approach B only)

Skip to **Part 3** if you chose Approach A.

### 2B-1: Create a Dataflow Staging Bucket

Dataflow needs a staging bucket for temporary pipeline files separate from the landing bucket.

1. Navigate to **Cloud Storage > Buckets** and click **"+ Create"**.
2. Fill in:
   - **Name**: `kafka-poc-dataflow-temp-kafka-poc`
   - **Location type**: `Region`
   - **Region**: `us-central1`
   - **Storage class**: `Standard`
   - **Access control**: `Uniform`
3. Click **"Create"**.

### 2B-2: Launch the Job

1. Navigate to **Dataflow > Jobs** using the top search bar.
2. Click **"+ Create Job From Template"**.
3. Fill in **Job details**:
   - **Job name**: `kafka-to-gcs-poc`
   - **Region**: `us-central1`
4. In the **Dataflow template** dropdown, search for `Kafka to Cloud Storage` and select **"Apache Kafka to Cloud Storage"** from the results.
5. Fill in the **Required parameters**:

   | Parameter | Value |
   |---|---|
   | **Kafka Bootstrap Servers** | Paste from Secret Manager: `kafka-bootstrap-servers` |
   | **Kafka Input Topics** | `raw-events` |
   | **Output file directory in Cloud Storage** | `gs://kafka-poc-gcs-landing/dataflow/` |
   | **Output filename prefix** | `order-events` |
   | **Window duration** | `1m` |

   > **Window duration:** Dataflow batches incoming records into time-bounded windows and writes each window as one or more GCS files. `1m` gives Snowpipe near-real-time ingestion at low file overhead. Increase to `5m` for larger files with fewer Snowpipe invocations.

6. Scroll to **Optional parameters / Networking**:
   - **Network**: `poc-vpc`
   - **Subnetwork**: `regions/us-central1/subnetworks/poc-subnet`
   - **Worker IP address configuration**: `Private`

7. Fill in **Pipeline configuration**:
   - **Service account email**: `dataflow-pipeline-sa@vetsource-496203.iam.gserviceaccount.com`
   - **Machine type**: `n1-standard-2`
   - **Maximum number of workers**: `1`
   - **Temporary location**: `gs://kafka-poc-dataflow-temp-kafka-poc/temp`

8. Click **"Run Job"**.
9. The job status transitions from **Starting** → **Running** in 3–5 minutes.

> **On Avro encoding:** The "Apache Kafka to Cloud Storage" template writes Kafka message payloads to GCS files. Because messages are Avro-encoded at the source, the output files contain Avro-formatted data. The Snowpipe setup in Part 4 uses an Avro file format to load them correctly — no manual schema mapping is needed.

### 2B-3: Confirm Files Land in GCS

1. Navigate to **Cloud Storage > Buckets > kafka-poc-gcs-landing > dataflow/**.
2. After 2–3 minutes (one window duration plus startup), files named `order-events-SHARD-WINDOW.avro` should appear.
3. Confirm the Dataflow job graph shows records flowing: check **Metrics** → **Elements added** is non-zero.

---

## Part 3 — Set Up GCS Pub/Sub Notifications (Both Approaches)

Snowpipe Auto-Ingest works by subscribing to a GCS event stream. When a new file lands in the bucket, GCS publishes an event to a Pub/Sub topic; Snowpipe polls the subscription and triggers a COPY automatically.

### 3-1: Create a Pub/Sub Topic

1. Navigate to **Pub/Sub** using the top search bar.
2. Click **"+ Create Topic"**.
3. Fill in:
   - **Topic ID**: `kafka-poc-gcs-notify`
   - Leave **"Add a default subscription"** checked
4. Click **"Create"**.
5. In the **Subscriptions** tab, confirm a subscription named `kafka-poc-gcs-notify-sub` was created automatically.

### 3-2: Add a GCS Bucket Notification

> **Note:** The Notifications tab has been removed from the GCP Cloud Storage bucket UI. Use Cloud Shell or a local terminal authenticated to the project instead.

1. Open **Cloud Shell** (top-right toolbar icon in the GCP Console, `>_`), or run locally if `gcloud` is configured.
2. Run:

```bash
gcloud storage buckets notifications create gs://kafka-poc-gcs-landing \
  --topic=projects/vetsource-496203/topics/kafka-poc-gcs-notify \
  --event-types=OBJECT_FINALIZE
```

   > `OBJECT_FINALIZE` fires when a file is fully written to GCS — the correct event for Snowpipe. Do not add other event types.

3. Confirm the notification was created:

```bash
gcloud storage buckets notifications list gs://kafka-poc-gcs-landing
```

   The output should show a row containing `OBJECT_FINALIZE` and the `kafka-poc-gcs-notify` topic.

### 3-3: Grant GCS the Right to Publish to Pub/Sub

GCS uses a Google-managed service account to publish notifications. You must give it `Pub/Sub Publisher` on the topic.

1. In the **GCP Console**, navigate to **Cloud Storage > Settings** (gear icon in the left nav).
2. Note the **Cloud Storage Service Account** field. It shows an SA in the form:
   `service-NNNNNNNNNNN@gs-project-accounts.iam.gserviceaccount.com`

   > This is a Google-owned SA used internally by GCS — it is different from `dataflow-pipeline-sa`.

3. Navigate to **Pub/Sub > Topics** and click `kafka-poc-gcs-notify`.
4. Click the **Permissions** tab, then **"Grant Access"**.
5. In **"New principals"**, paste the GCS service account email from step 2.
6. In **"Select a role"**, choose **Pub/Sub Publisher**.
7. Click **"Save"**.

---

## Part 4 — Configure Snowpipe Auto-Ingest in Snowflake (Both Approaches)

### 4-1: Create a GCS Storage Integration

A Storage Integration lets Snowflake access GCS files without embedding GCP credentials in the stage definition.

1. In **Snowsight**, open a new worksheet and set context:
   - **Role**: `ACCOUNTADMIN`
   - **Warehouse**: `POC_WH`

2. Run:

```sql
CREATE STORAGE INTEGRATION IF NOT EXISTS gcs_poc_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://kafka-poc-gcs-landing/');
```

3. Run the following and note the output:

```sql
DESC INTEGRATION gcs_poc_int;
```

4. Find the row where `property = STORAGE_GCS_SERVICE_ACCOUNT`. Copy the value — it looks like:
   `service-account@snowflake-gcs.iam.gserviceaccount.com`

   **Keep this value.** You need it in the next step.

### 4-2: Grant Snowflake's Storage SA Access to the GCS Bucket

1. In the **GCP Console**, navigate to **Cloud Storage > Buckets > kafka-poc-gcs-landing**.
2. Click the **Permissions** tab, then **"Grant Access"**.
3. In **"New principals"**, paste the Snowflake GCS service account from the previous step.
4. Add the following two roles (click **"+ Add Another Role"** for the second):
   - **Storage Object Viewer**
   - **Storage Legacy Bucket Reader**
5. Click **"Save"**.

> **Why two roles?** `Storage Object Viewer` allows Snowflake to read file content. `Storage Legacy Bucket Reader` allows Snowflake to list objects in the bucket. Both are required for COPY INTO to locate and load files.

### 4-3: Create a Notification Integration

1. Back in **Snowsight** (still using `ACCOUNTADMIN`), run:

```sql
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS gcs_notification_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/vetsource-496203/subscriptions/kafka-poc-gcs-notify-sub';
```

   > Replace `vetsource-496203` with your actual GCP project ID if it differs.

2. Retrieve the Snowflake Pub/Sub service account:

```sql
DESC INTEGRATION gcs_notification_int;
```

3. Find the row where `property = GCP_PUBSUB_SERVICE_ACCOUNT`. Copy that value.

### 4-4: Grant Snowflake's Notification SA Access to Pub/Sub

Use the **CLI** for all three grants below — the GCP Console UI is unreliable for subscription-level IAM bindings. Replace the SA email with the value from `GCP_PUBSUB_SERVICE_ACCOUNT` in the previous step.

```bash
# 1. Allow Snowflake to pull messages from the subscription
gcloud pubsub subscriptions add-iam-policy-binding kafka-poc-gcs-notify-sub \
  --project=vetsource-496203 \
  --member="serviceAccount:kkrc30000@gcpuscentral1-1dfa.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

# 2. Allow Snowflake to read subscription metadata (required for pipe monitoring)
gcloud pubsub subscriptions add-iam-policy-binding kafka-poc-gcs-notify-sub \
  --project=vetsource-496203 \
  --member="serviceAccount:kkrc30000@gcpuscentral1-1dfa.iam.gserviceaccount.com" \
  --role="roles/pubsub.viewer"

# 3. Allow Snowflake to query Cloud Monitoring APIs (required for Snowpipe health checks)
gcloud projects add-iam-policy-binding vetsource-496203 \
  --member="serviceAccount:kkrc30000@gcpuscentral1-1dfa.iam.gserviceaccount.com" \
  --role="roles/monitoring.viewer"
```

> **Why three roles?** `pubsub.subscriber` lets Snowflake consume messages. `pubsub.viewer` lets it read subscription state. `monitoring.viewer` at the project level is required for Snowpipe's internal health-check mechanism — without it, `CREATE PIPE` fails with `PERMISSION_DENIED` even when the Pub/Sub grants are correct.

Confirm all bindings are in place:

```bash
gcloud pubsub subscriptions get-iam-policy kafka-poc-gcs-notify-sub \
  --project=vetsource-496203
```

### 4-5: Create File Format, Stage, and Snowpipe

In **Snowsight**, switch to `SYSADMIN` and run the block that matches your chosen approach.

**Approach A — JSON files from GCS Sink Connector:**

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE POC_WH;

CREATE FILE FORMAT IF NOT EXISTS poc_db.kafka_ingest.json_ndjson
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE;

CREATE STAGE IF NOT EXISTS poc_db.kafka_ingest.gcs_stage
  URL = 'gcs://kafka-poc-gcs-landing/'
  STORAGE_INTEGRATION = gcs_poc_int
  FILE_FORMAT = poc_db.kafka_ingest.json_ndjson;

CREATE PIPE IF NOT EXISTS poc_db.kafka_ingest.order_events_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'GCS_NOTIFICATION_INT'
  AS
  COPY INTO poc_db.kafka_ingest.order_events (
    order_id, customer_id, product_id, amount, currency, timestamp, status
  )
  FROM (
    SELECT
      $1:order_id::VARCHAR,
      $1:customer_id::VARCHAR,
      $1:product_id::VARCHAR,
      $1:amount::FLOAT,
      $1:currency::VARCHAR,
      $1:timestamp::NUMBER,
      $1:status::VARCHAR
    FROM @poc_db.kafka_ingest.gcs_stage
  );
```

**Approach B — Avro files from Dataflow:**

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE POC_WH;

CREATE FILE FORMAT IF NOT EXISTS poc_db.kafka_ingest.avro_fmt
  TYPE = 'AVRO';

CREATE STAGE IF NOT EXISTS poc_db.kafka_ingest.gcs_stage
  URL = 'gcs://kafka-poc-gcs-landing/dataflow/'
  STORAGE_INTEGRATION = gcs_poc_int
  FILE_FORMAT = poc_db.kafka_ingest.avro_fmt;

CREATE PIPE IF NOT EXISTS poc_db.kafka_ingest.order_events_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 'GCS_NOTIFICATION_INT'
  AS
  COPY INTO poc_db.kafka_ingest.order_events (
    order_id, customer_id, product_id, amount, currency, timestamp, status
  )
  FROM (
    SELECT
      $1:order_id::VARCHAR,
      $1:customer_id::VARCHAR,
      $1:product_id::VARCHAR,
      $1:amount::FLOAT,
      $1:currency::VARCHAR,
      $1:timestamp::NUMBER,
      $1:status::VARCHAR
    FROM @poc_db.kafka_ingest.gcs_stage
  );
```

> **`$1` notation:** In a COPY INTO SELECT from a stage, `$1` refers to the entire parsed record. For JSON, `$1:field_name` extracts a top-level key. For Avro, Snowflake infers the schema from the file and exposes fields the same way.

### 4-6: Grant the Pipe Owner Role INSERT on the Target Table

Snowpipe loads data using the role that owns the pipe. `SYSADMIN` created the pipe above, so grant it INSERT on the table:

```sql
USE ROLE ACCOUNTADMIN;
GRANT INSERT, SELECT ON TABLE poc_db.kafka_ingest.order_events TO ROLE SYSADMIN;
```

---

## Part 5 — Verify the End-to-End Pipeline

### 5-1: Check Pipe Status

```sql
USE ROLE SYSADMIN;
SELECT SYSTEM$PIPE_STATUS('poc_db.kafka_ingest.order_events_pipe');
```

A healthy pipe returns a JSON object with `"executionState": "RUNNING"` and `"pendingFileCount"` at zero or a small positive number (files currently being processed).

### 5-2: Confirm Rows Are Arriving

Wait 2–3 minutes after files start appearing in GCS, then run:

```sql
SELECT COUNT(*), MAX(ingested_at)
FROM poc_db.kafka_ingest.order_events;
```

The row count should increase each time you run this query as long as the producer is publishing events.

### 5-3: Inspect Recent Load History

```sql
SELECT file_name, status, row_count, error_count, last_load_time
FROM TABLE(information_schema.copy_history(
  table_name => 'ORDER_EVENTS',
  start_time => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC
LIMIT 20;
```

Each row represents one GCS file Snowpipe attempted to load. `status = LOADED` and `error_count = 0` is the expected state.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No files in GCS after 5 min (Approach A) | Connect cluster SA lacks Storage Object Admin on landing bucket | Verify Connect cluster SA is `dataflow-pipeline-sa`; grant it Storage Object Admin on `kafka-poc-gcs-landing` |
| Connector shows **Failed** state | Incorrect Schema Registry credentials | Re-open connector config, re-copy credentials from Secret Manager — watch for stray spaces |
| No files in GCS after 5 min (Approach B) | Dataflow job stuck in **Starting** | Check Dataflow job logs; verify `poc-vpc` firewall allows egress on port 443 |
| Pipe status shows `PAUSED` | Notification integration cannot reach Pub/Sub subscription | Re-run `DESC INTEGRATION gcs_notification_int;`, verify the Snowflake Pub/Sub SA has Pub/Sub Subscriber on `kafka-poc-gcs-notify-sub` |
| `COPY INTO` errors in copy_history | Column type mismatch or missing field | Open a raw GCS file and confirm JSON field names exactly match those in the COPY SELECT (case-sensitive) |
| 0 rows despite `LOADED` files | Stage URL prefix does not match actual file path | Confirm the stage `URL` matches the directory where files are written; run `LIST @poc_db.kafka_ingest.gcs_stage;` to check what Snowflake sees |

---

## What's Next

The pipeline is running: Kafka events flow into GCS files, Snowpipe ingests them automatically, and rows appear in `POC_DB.KAFKA_INGEST.ORDER_EVENTS`. The next step is **Step 12 — End-to-End Validation**, where you run a load test with the producer, watch row counts climb in real time, and confirm ingestion latency meets POC requirements.
