# Step 11a — Kafka to GCS via Managed Kafka Connect GCS Sink (Approach A)

## Overview

This guide implements **Approach A** of the Kafka → GCS → Snowflake pipeline using **GCP Managed Kafka Connect** with the pre-packaged **GCS Sink Connector**.

`producer.py` is **not modified** — it continues to send Avro-encoded messages in the Confluent wire format. The connector is configured with `io.confluent.connect.avro.AvroConverter`, which reads the schema from the GCP Managed Schema Registry using `dataflow-pipeline-sa`'s workload identity, deserializes each message into a struct, and writes clean JSON lines into GCS. Snowpipe picks up each file automatically and loads rows into Snowflake.

> **Why Approach A over Approach B?** No custom pipeline code, no Dataflow job to monitor or tune, no separate staging bucket. The GCS Sink Connector is pre-packaged in GCP Managed Kafka Connect and operates entirely within your VPC. Snowpipe latency is typically under 60 seconds end-to-end.

---

## Architecture (Approach A)

```
GCE VM — producer.py
      │  Avro (Confluent wire format) over SASL_SSL
      ▼
GCP Managed Kafka — raw-events topic
      │
      │  GCS Sink Connector (AvroConverter → JsonFormat)
      │  reads Avro → deserializes via Schema Registry → writes JSON files
      ▼
GCS — kafka-poc-gcs-landing/
      order-events-raw-events-0-000000000000.json
      │
      │  OBJECT_FINALIZE event → Pub/Sub topic: kafka-poc-gcs-notify
      ▼
Snowpipe Auto-Ingest  (GCS notification integration)
      │
      ▼
Snowflake — POC_DB.KAFKA_INGEST.ORDER_EVENTS
```

---

## Prerequisites

Complete all of the following before starting this guide:

- [ ] **Step 01–03** — GCP project, VPC, and service accounts are in place
- [ ] **Step 04** — `poc-kafka-cluster` is Active; bootstrap server address is stored in Secret Manager
- [ ] **Step 05a** — GCP Managed Schema Registry (`poc-schema-registry`) is Active; URL stored in Secret Manager as `schema-registry-url`
- [ ] **Step 06** — Secret Manager secrets exist; both service accounts have `Secret Manager Secret Accessor`
- [ ] **Step 07** — `poc-dev-vm` is running; Python virtualenv is configured
- [ ] **Step 08** — `raw-events` topic exists (3 partitions, 7-day retention)
- [ ] **Step 09** — `producer.py` runs successfully and publishes Avro messages
- [ ] **Step 10** — Snowflake `POC_DB.KAFKA_INGEST.ORDER_EVENTS` table exists

---

## Part 1 — Verify IAM for `dataflow-pipeline-sa`

All commands in this guide run in **Cloud Shell**. The Connect cluster runs all connector tasks as `dataflow-pipeline-sa` — confirm its roles before continuing.

```bash
gcloud projects get-iam-policy vetsource-496203 \
  --flatten="bindings[].members" \
  --filter="bindings.members:dataflow-pipeline-sa@vetsource-496203.iam.gserviceaccount.com" \
  --format="table(bindings.role)"
```

Required roles:

| Role | Purpose |
|---|---|
| `roles/managedkafka.client` | Consume from `raw-events` |
| `roles/storage.objectAdmin` | Write files to the GCS landing bucket |
| `roles/secretmanager.secretAccessor` | Read secrets at runtime |
| `roles/managedkafka.schemaRegistryEditor` | Read schemas from `poc-schema-registry` |

If any role is missing, grant it:

```bash
# Example — grant Storage Object Admin if missing
gcloud projects add-iam-policy-binding vetsource-496203 \
  --member="serviceAccount:dataflow-pipeline-sa@vetsource-496203.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

---

## Part 2 — Create the GCS Landing Bucket

```bash
gcloud storage buckets create gs://kafka-poc-gcs-landing \
  --location=us-central1 \
  --uniform-bucket-level-access

# Grant the Connect cluster's service account write access
gcloud storage buckets add-iam-policy-binding gs://kafka-poc-gcs-landing \
  --member="serviceAccount:dataflow-pipeline-sa@vetsource-496203.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Verify
gcloud storage ls gs://
```

> Skip this part if the bucket already exists. Re-run the `add-iam-policy-binding` command regardless — it is idempotent.

---

## Part 3 — Create the Managed Kafka Connect Cluster

```bash
gcloud managed-kafka connect-clusters create poc-connect-cluster \
  --location=us-central1 \
  --kafka-cluster=poc-kafka-cluster \
  --vpc-config=network=projects/vetsource-496203/global/networks/poc-vpc,subnetwork=projects/vetsource-496203/regions/us-central1/subnetworks/poc-subnet \
  --gcp-service-account=dataflow-pipeline-sa@vetsource-496203.iam.gserviceaccount.com
```

> **Subnet IP range error:** If creation fails with `The worker subnet must have at least 1024 IP addresses`, expand `poc-subnet` from `/24` to `/22` in the GCP Console under **VPC Network > VPC Networks > poc-vpc > Subnets > poc-subnet > Edit**, then re-run the command.

Poll until `state: ACTIVE` (typically 3–5 minutes):

```bash
gcloud managed-kafka connect-clusters describe poc-connect-cluster \
  --location=us-central1
```

---

## Part 4 — Deploy the GCS Sink Connector

The connector uses `io.confluent.connect.avro.AvroConverter` to deserialize Avro messages from `raw-events`. It fetches the schema from the GCP Managed Schema Registry using `dataflow-pipeline-sa`'s workload identity — no credentials need to be embedded in the config. The output format is `JsonFormat`, which writes newline-delimited JSON files that Snowpipe can parse directly.

### 4-1: Build the connector config

```bash
# Fetch Schema Registry URL from Secret Manager
SR_URL=$(gcloud secrets versions access latest \
  --secret=schema-registry-url \
  --project=vetsource-496203)

cat > /tmp/connector.json <<EOF
{
  "configs": {
    "connector.class":                        "io.confluent.connect.gcs.GcsSinkConnector",
    "tasks.max":                              "1",
    "topics":                                 "raw-events",
    "gcs.bucket.name":                        "kafka-poc-gcs-landing",
    "gcs.credentials.default":               "true",
    "key.converter":                          "org.apache.kafka.connect.storage.StringConverter",
    "value.converter":                        "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url":   "${SR_URL}",
    "value.converter.schemas.enable":        "false",
    "format.class":                           "io.confluent.connect.gcs.format.json.JsonFormat",
    "file.name.prefix":                       "order-events-",
    "flush.size":                             "10",
    "rotate.interval.ms":                     "60000",
    "rotate.schedule.interval.ms":            "120000",
    "storage.class":                          "io.confluent.connect.gcs.storage.GcsStorage",
    "locale":                                 "en_US",
    "timezone":                               "UTC"
  }
}
EOF
```

> **`value.converter.schemas.enable = false`** — disables the Confluent schema envelope wrapper (`{"schema": ..., "payload": ...}`) so each GCS line contains the raw JSON object. This is what the Snowpipe `$1:field_name` expressions expect.

> **`flush.size = 10`** — the connector writes a new file after every 10 records. Combined with `rotate.interval.ms = 60000`, a file is also written after 60 seconds even if fewer than 10 records have arrived.

### 4-2: Deploy

```bash
curl -s -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  "https://managedkafka.googleapis.com/v1/projects/vetsource-496203/locations/us-central1/connectClusters/poc-connect-cluster/connectors?connectorId=gcs-sink-order-events" \
  -d @/tmp/connector.json | python3 -m json.tool
```

### 4-3: Confirm RUNNING

```bash
# Poll until state: RUNNING (2-3 min)
gcloud managed-kafka connect-clusters connectors describe gcs-sink-order-events \
  --connect-cluster=poc-connect-cluster \
  --location=us-central1
```

If the state shows `FAILED`, check the Logs tab in the GCP Console under **Managed Kafka > Connect clusters > poc-connect-cluster > gcs-sink-order-events**. Common causes are listed in the Troubleshooting section.

### 4-4: Confirm files land in GCS

Run the producer on `poc-dev-vm`:

```bash
cd ~/kafka-poc && source venv/bin/activate
python3 producer.py
```

Wait 60–90 seconds, then check the bucket:

```bash
gcloud storage ls gs://kafka-poc-gcs-landing/
```

One or more `.json` files should appear, named like `order-events-raw-events-0-000000000000.json`.

Inspect the content to confirm clean JSON (not binary):

```bash
gcloud storage cat gs://kafka-poc-gcs-landing/$(gcloud storage ls gs://kafka-poc-gcs-landing/ | head -1 | xargs basename)
```

Expected output — one JSON object per line:

```
{"order_id":"3f7a1c2e-...","customer_id":"cust-A1","product_id":"prod-042","amount":149.99,"currency":"USD","timestamp":1748185200000,"status":"CREATED"}
{"order_id":"7b2d4f8a-...","customer_id":"cust-C3","product_id":"prod-001","amount":29.99,"currency":"EUR","timestamp":1748185200100,"status":"SHIPPED"}
```

> **No files after 5 minutes?** The connector reads from the latest offset by default. If the producer ran before the connector started, those messages were missed. Run `producer.py` again — new messages are picked up immediately.

---

## Part 5 — Set Up GCS Pub/Sub Notifications

Snowpipe Auto-Ingest is event-driven: GCS publishes a notification to Pub/Sub when each file write completes, and Snowpipe triggers a COPY immediately.

### 5-1: Create Pub/Sub topic and subscription

```bash
gcloud pubsub topics create kafka-poc-gcs-notify \
  --project=vetsource-496203

gcloud pubsub subscriptions create kafka-poc-gcs-notify-sub \
  --topic=kafka-poc-gcs-notify \
  --project=vetsource-496203
```

### 5-2: Attach GCS bucket notification

```bash
gcloud storage buckets notifications create gs://kafka-poc-gcs-landing \
  --topic=projects/vetsource-496203/topics/kafka-poc-gcs-notify \
  --event-types=OBJECT_FINALIZE

# Verify
gcloud storage buckets notifications list gs://kafka-poc-gcs-landing
```

> `OBJECT_FINALIZE` fires when a file write completes — the correct trigger for Snowpipe. Do not add other event types.

### 5-3: Grant GCS permission to publish to Pub/Sub

```bash
GCS_SA=$(gcloud storage service-agent --project=vetsource-496203)

gcloud pubsub topics add-iam-policy-binding kafka-poc-gcs-notify \
  --member="serviceAccount:${GCS_SA}" \
  --role="roles/pubsub.publisher" \
  --project=vetsource-496203
```

---

## Part 6 — Configure Snowpipe Auto-Ingest in Snowflake

All SQL in this part runs in **Snowsight**. Open a new worksheet before beginning.

### 6-1: Create a GCS Storage Integration

Run as `ACCOUNTADMIN`:

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE POC_WH;

CREATE STORAGE INTEGRATION IF NOT EXISTS gcs_poc_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://kafka-poc-gcs-landing/');
```

Retrieve Snowflake's GCS service account identity:

```sql
DESC INTEGRATION gcs_poc_int;
```

Find the row where `property = STORAGE_GCS_SERVICE_ACCOUNT`. Copy the value — it looks like:
`service-account@snowflake-gcs.iam.gserviceaccount.com`

**Keep this value — you need it in the next step.**

### 6-2: Grant Snowflake read access to the GCS bucket

```bash
# Replace with the STORAGE_GCS_SERVICE_ACCOUNT value from DESC INTEGRATION above
SNOWFLAKE_GCS_SA="REPLACE_ME@snowflake-gcs.iam.gserviceaccount.com"

gcloud storage buckets add-iam-policy-binding gs://kafka-poc-gcs-landing \
  --member="serviceAccount:${SNOWFLAKE_GCS_SA}" \
  --role="roles/storage.objectViewer"

gcloud storage buckets add-iam-policy-binding gs://kafka-poc-gcs-landing \
  --member="serviceAccount:${SNOWFLAKE_GCS_SA}" \
  --role="roles/storage.legacyBucketReader"
```

> **Why two roles?** `storage.objectViewer` lets Snowflake read file content. `storage.legacyBucketReader` lets Snowflake list objects in the bucket. Both are required for COPY INTO to locate and load files.

### 6-3: Create a Notification Integration

Back in Snowsight (still as `ACCOUNTADMIN`):

```sql
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS gcs_notification_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/vetsource-496203/subscriptions/kafka-poc-gcs-notify-sub';
```

Retrieve Snowflake's Pub/Sub service account:

```sql
DESC INTEGRATION gcs_notification_int;
```

Find the row where `property = GCP_PUBSUB_SERVICE_ACCOUNT`. Copy that value.

### 6-4: Grant Snowflake access to the Pub/Sub subscription

```bash
# Replace with the GCP_PUBSUB_SERVICE_ACCOUNT value from DESC INTEGRATION above
SNOWFLAKE_PUBSUB_SA="REPLACE_ME@..."

# Allow Snowflake to pull messages from the subscription
gcloud pubsub subscriptions add-iam-policy-binding kafka-poc-gcs-notify-sub \
  --project=vetsource-496203 \
  --member="serviceAccount:${SNOWFLAKE_PUBSUB_SA}" \
  --role="roles/pubsub.subscriber"

# Allow Snowflake to read subscription metadata (required for pipe monitoring)
gcloud pubsub subscriptions add-iam-policy-binding kafka-poc-gcs-notify-sub \
  --project=vetsource-496203 \
  --member="serviceAccount:${SNOWFLAKE_PUBSUB_SA}" \
  --role="roles/pubsub.viewer"

# Allow Snowflake to query Cloud Monitoring APIs (required for Snowpipe health checks)
gcloud projects add-iam-policy-binding vetsource-496203 \
  --member="serviceAccount:${SNOWFLAKE_PUBSUB_SA}" \
  --role="roles/monitoring.viewer"

# Verify all bindings
gcloud pubsub subscriptions get-iam-policy kafka-poc-gcs-notify-sub \
  --project=vetsource-496203
```

### 6-5: Create file format, stage, and pipe

Switch to `SYSADMIN` and run:

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE POC_WH;

-- JSON file format — one JSON object per line (NDJSON, no outer array)
CREATE FILE FORMAT IF NOT EXISTS poc_db.kafka_ingest.json_ndjson
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE;

-- External stage pointing at the landing bucket root
CREATE STAGE IF NOT EXISTS poc_db.kafka_ingest.gcs_stage
  URL = 'gcs://kafka-poc-gcs-landing/'
  STORAGE_INTEGRATION = gcs_poc_int
  FILE_FORMAT = poc_db.kafka_ingest.json_ndjson;

-- Snowpipe — triggered by Pub/Sub events, loads JSON into ORDER_EVENTS
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

> **`$1` notation:** In a COPY INTO SELECT from a stage, `$1` refers to the entire parsed JSON record. `$1:field_name` extracts a top-level key by name. Field names are case-sensitive and must exactly match the JSON keys written by the connector — i.e., `order_id`, not `orderId` or `ORDER_ID`.

> **`INGESTED_AT` column:** The `ORDER_EVENTS` table has an `INGESTED_AT` column with `DEFAULT CURRENT_TIMESTAMP()`. Snowpipe populates it automatically — you do not need to include it in the COPY SELECT.

### 6-6: Grant the pipe owner INSERT on the target table

```sql
USE ROLE ACCOUNTADMIN;
GRANT INSERT, SELECT ON TABLE poc_db.kafka_ingest.order_events TO ROLE SYSADMIN;
```

---

## Part 7 — Verify the End-to-End Pipeline

### 7-1: Check pipe status

```sql
USE ROLE SYSADMIN;
SELECT SYSTEM$PIPE_STATUS('poc_db.kafka_ingest.order_events_pipe');
```

A healthy pipe returns JSON containing:
- `"executionState": "RUNNING"`
- `"pendingFileCount": 0` (or a small positive number if files are actively being processed)

If `executionState` is `PAUSED` or `STOPPED`, re-run the three `gcloud` commands in Step 6-4.

### 7-2: List what the stage can see

```sql
LIST @poc_db.kafka_ingest.gcs_stage;
```

This shows every file Snowflake can see in the bucket. If files are in GCS but not visible here, the stage URL or bucket permissions are misconfigured.

### 7-3: Check row counts

Wait 2–3 minutes after the first files appear in GCS, then run:

```sql
SELECT COUNT(*), MAX(ingested_at)
FROM poc_db.kafka_ingest.order_events;
```

Run the producer again if no rows appear:

```bash
cd ~/kafka-poc && source venv/bin/activate
python3 producer.py
```

The row count should increase each time the producer runs. `MAX(ingested_at)` should be within the last few minutes.

### 7-4: Inspect load history

```sql
SELECT file_name, status, row_count, error_count, last_load_time
FROM TABLE(information_schema.copy_history(
  table_name => 'ORDER_EVENTS',
  start_time => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC LIMIT 20;
```

Each row represents one GCS file Snowpipe attempted to load. `status = LOADED` and `error_count = 0` is the expected state. Any row with `status = LOAD_FAILED` will include an `error_message` column explaining the failure.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Connector state shows `FAILED` immediately | `dataflow-pipeline-sa` lacks `storage.objectAdmin` on the bucket | Re-run the `add-iam-policy-binding` in Part 2; check connector Logs tab for the specific error |
| Connector state shows `FAILED` with schema registry error | `dataflow-pipeline-sa` lacks `managedkafka.schemaRegistryEditor` | Grant the role as shown in Part 1; restart the connector |
| No files in GCS after 5 min | Producer ran before the connector started; connector reads from latest offset | Run `producer.py` again — the connector picks up new messages immediately |
| GCS files contain binary garbage | An old run of the connector used `ByteArrayConverter` | Delete the garbled files manually; confirm `value.converter = io.confluent.connect.avro.AvroConverter` in the connector config; run `producer.py` again |
| Connector task shows `FAILED` in Tasks tab | Transient error | Restart via Console: Managed Kafka → Connect clusters → gcs-sink-order-events → Restart task; check Logs for root cause |
| Pipe status `PAUSED` | Snowflake Pub/Sub SA lacks Subscriber or Viewer on the subscription | Re-run the three `gcloud` commands in Step 6-4 |
| `LOAD_FAILED` in copy history | JSON field name mismatch between GCS file and COPY SELECT | Run `gcloud storage cat` on a raw GCS file; check exact key names against the `$1:field_name` expressions in the pipe definition |
| 0 rows despite `LOADED` files | Stage URL prefix does not match actual GCS file path | Run `LIST @poc_db.kafka_ingest.gcs_stage;` to verify what Snowflake can see |
| `PERMISSION_DENIED` on CREATE PIPE | `monitoring.viewer` not granted to the Snowflake Pub/Sub SA | Re-run the third `gcloud projects add-iam-policy-binding` command in Step 6-4 |

---

## What's Next

The Approach A pipeline is running:

- **`producer.py`** publishes Avro `OrderEvent` records to `raw-events` (unchanged)
- **Connector** deserializes each record via GCP Schema Registry and writes JSON files into `kafka-poc-gcs-landing/`
- **Pub/Sub** sends an `OBJECT_FINALIZE` event for each completed file
- **Snowpipe** loads each file into `POC_DB.KAFKA_INGEST.ORDER_EVENTS` within ~60 seconds of the file landing

Next: **[Step 12 — End-to-End Validation](./12-end-to-end-validation.md)**

In Step 12 you run a sustained producer load, watch row counts climb in real time in Snowflake, measure ingestion latency, and confirm the pipeline meets POC acceptance criteria.
